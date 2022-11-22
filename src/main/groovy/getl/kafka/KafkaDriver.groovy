package getl.kafka

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ConnectionError
import getl.exception.DatasetError
import getl.exception.NotSupportError
import getl.exception.RequiredParameterError
import getl.json.JSONConnection
import getl.json.JSONDataset
import getl.tfs.TFS
import getl.utils.FileUtils
import getl.utils.ListUtils
import getl.utils.MapUtils
import getl.utils.StringUtils
import groovy.json.JsonGenerator
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Duration

@InheritConstructors
class KafkaDriver extends Driver {
    @Override
    protected void registerParameters() {
        super.registerParameters()
        methodParams.register('eachRow', ['fields', 'filter', 'readDuration', 'offsetForRegister', 'limit', 'maxPollRecords'])
    }

    @Override
    List<Support> supported() {
        return [Support.EACHROW, Support.WRITE, Support.DATE, Support.TIME, Support.AUTOLOADSCHEMA]
    }

    @Override
    List<Operation> operations() {
        return [Operation.INSERT]
    }

    @Override
    Boolean isConnected() {
        throw new NotSupportError(connection, 'connect')
    }

    /** Current Kafka connection object  */
    KafkaConnection getCurrentKafkaConnection() { connection as KafkaConnection }

    @Override
    void connect() {
        throw new NotSupportError(connection, 'connect')
    }

    @Override
    void disconnect() {
        throw new NotSupportError(connection, 'disconnect')
    }

    @Override
    List<Object> retrieveObjects(Map params, Closure<Boolean> filter) {
        throw new NotSupportError(connection, 'retrieveObject')
    }

    @Override
    List<Field> fields(Dataset dataset) {
        throw new NotSupportError(connection, 'read fields')
    }

    @Override
    void startTran(Boolean useSqlOperator = false) {
        throw new NotSupportError(connection, 'start transaction')
    }

    @Override
    void commitTran(Boolean useSqlOperator = false) {
        throw new NotSupportError(connection, 'commit transaction')
    }

    @Override
    void rollbackTran(Boolean useSqlOperator = false) {
        throw new NotSupportError(connection, 'rollback transaction')
    }

    @Override
    void createDataset(Dataset dataset, Map params) {
        throw new NotSupportError(connection, 'create dataset')
    }

    void checkKafkaParams(KafkaDataset ds = null) {
        if (ds != null && ds.kafkaTopic == null)
            throw new RequiredParameterError(ds, 'kafkaTopic')

        def con = currentKafkaConnection
        if (con.bootstrapServers == null)
            throw new RequiredParameterError(ds, 'bootstrapServers')
        if (con.groupId == null)
            throw new RequiredParameterError(ds, 'groupId')
    }

    @SuppressWarnings(['UnnecessaryQualifiedReference'])
    @Override
    Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
        def ds = dataset as KafkaDataset
        checkKafkaParams(ds)

        if (ds.field.isEmpty())
            throw new DatasetError(ds, '#dataset.non_fields')

        def con = currentKafkaConnection
        def topicName = ds.kafkaTopic

        def fields = [] as List<String>
        if (prepareCode != null) {
            prepareCode.call(fields)
        } else if (params.fields != null)
            fields = ListUtils.ToList(params.fields) as List<String>

        def keyName = ds.keyName
        def dur = (params.readDuration != null)?Duration.ofSeconds(params.readDuration as Long):Duration.ofMillis(Long.MAX_VALUE)
        def limit = (params.limit as Long)?:0L
        def maxPoolRecords = (params.maxPollRecords as Integer)?:10000
        def offsetForRegister = (params.offsetForRegister as String)?:KafkaDataset.offsetForRegisterLatest

        def props = new Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, con.bootstrapServers)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, con.groupId)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetForRegister.toLowerCase())
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.name)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.name)
        if (limit != 0 && limit < maxPoolRecords)
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, limit)
        else
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoolRecords)
        props.putAll(con.connectProperties)

        def res = 0L
        KafkaConsumer<String, String> kafkaConsumer
        try {
            kafkaConsumer = new KafkaConsumer<String, String>(props)
        }
        catch (Exception e) {
            connection.logger.severe("Can't connect to Kafka servers", e)
            connection.logger.dump(e, 'KafkaDriver', dataset.toString(), MapUtils.ToJson(props))
            throw e
        }

        def jsonDirName = "${TFS.storage.currentPath()}/${FileUtils.UniqueFileName()}"
        def jsonDirFile = new File(jsonDirName)
        FileUtils.ValidPath(jsonDirFile, true)
        def jsonName = "$jsonDirFile/kafka."
        def jsonFile = new File(jsonName + StringUtils.AddLedZeroStr(1, 6) + '.json')
        jsonFile.deleteOnExit()

        try {
            def topicParts = kafkaConsumer.partitionsFor(topicName).collect { new TopicPartition(topicName, it.partition()) }
            kafkaConsumer.assign(topicParts)
            def endOffs = kafkaConsumer.endOffsets(topicParts)

            def countRows = 0L
            def curRows = 0L
            def countPortions = 1
            def isStart = true

            def writer = jsonFile.newWriter()
            try {
                writer.append('[')
                while (endOffs.any {kafkaConsumer.position(it.key) < it.value }) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(dur)

                    if (curRows >= maxPoolRecords) {
                        writer.append(']')
                        writer.close()

                        curRows = 0
                        countPortions++
                        isStart = true

                        jsonFile = new File(jsonName + StringUtils.AddLedZeroStr(countPortions, 6) + '.json')
                        jsonFile.deleteOnExit()
                        writer = jsonFile.newWriter()
                        writer.append('[')
                    }

                    for (org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record : records) {
                        def recKey = record.key()
                        if (keyName == null || keyName == recKey) {
                            def recValue = record.value() as String
                            if (recValue?.length() > 0) {
                                if (isStart)
                                    isStart = false
                                else
                                    writer.append(',')
                                writer.append(recValue)
                                curRows++
                                countRows++
                            }
                        }
                    }

                    if (limit > 0 && countRows >= limit)
                        break
                }
                writer.append(']')
            }
            finally {
                writer.close()
                if (curRows == 0) {
                    jsonFile.delete()
                    countPortions--
                }
            }

            if (countPortions > 0) {
                def jsonCon = new JSONConnection(path: jsonDirName)
                def json = new JSONDataset()
                json.tap {
                    useConnection jsonCon
                    extension = 'json'
                    field = (!fields.isEmpty()) ? ds.getFields(fields) : ds.field
                    rootNode = '.'
                    dataNode = ds.dataNode
                    formatDate = ds.formatDate()
                    formatTime = ds.formatTime()
                    formatDateTime = ds.formatDateTime()
                    formatTimestampWithTz = ds.formatTimestampWithTz()
                    uniFormatDateTime = ds.uniFormatDateTime()
                    formatBoolean = ds.formatBoolean()
                    decimalSeparator = ds.decimalSeparator()
                    groupSeparator = ds.groupSeparator()
                    readOpts.onFilter = (params.filter as Closure<Boolean>)?:ds.readOpts.onFilter
                }
                (1..countPortions).each { num ->
                    json.tap {
                        fileName = 'kafka.' + StringUtils.AddLedZeroStr(num, 6)
                        eachRow(code)
                        res += readRows
                        drop()
                    }
                }
            }

            kafkaConsumer.commitSync()
        }
        finally {
            kafkaConsumer.close()
        }
        return res
    }

    class WriterParams {
        KafkaProducer<String, String> kafkaProducer
        JsonGenerator jsonGen
        String kafkaTopic
        String keyName
    }

    @Override
    void openWrite(Dataset dataset, Map params, Closure prepareCode) {
        def ds = dataset as KafkaDataset
        checkKafkaParams(ds)

        def con = currentKafkaConnection
        def topicName = ds.kafkaTopic
        if (ds.autoCreateTopic())
            createTopic(topicName, 1, 1.shortValue(), true)

        if (prepareCode != null)
            prepareCode.call([]) as ArrayList

        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, con.bootstrapServers)
        props.put(ProducerConfig.ACKS_CONFIG, 'all')
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1)
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.putAll(con.connectProperties)

        def wp = new WriterParams()
        wp.kafkaTopic = ds.kafkaTopic
        wp.keyName = ds.keyName
        wp.kafkaProducer = new KafkaProducer<String, String>(props)
        def format = ds.uniFormatDateTime()?:ds.formatTimestampWithTz()
        wp.jsonGen = new JsonGenerator.Options().dateFormat(format, Locale.default).timezone(TimeZone.default.getID()).build()
        ds._driver_params = wp
    }

    @Override
    @CompileStatic
    void write(Dataset dataset, Map row) {
        def ds = dataset as KafkaDataset
        def wp = ds._driver_params as WriterParams

        def json = wp.jsonGen.toJson(row)
        def record = (ds.keyName != null)?new ProducerRecord<String, String>(wp.kafkaTopic, wp.keyName, json): new ProducerRecord<String, String>(wp.kafkaTopic, json)

        wp.kafkaProducer.send(record, new Callback() {
            @Override
            void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null)
                    throw exception
            }
        })
    }

    @Override
    void doneWrite(Dataset dataset) {
        def wp = dataset._driver_params as WriterParams
        wp.kafkaProducer.flush()
    }

    @Override
    void closeWrite(Dataset dataset) {
        def wp = dataset._driver_params as WriterParams
        wp.kafkaProducer.close()
        dataset._driver_params = null
    }

    @Override
    void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
        throw new NotSupportError(connection, 'bulk load file')
    }

    @Override
    void clearDataset(Dataset dataset, Map params) {
        throw new NotSupportError(connection, 'clear dataset')
    }

    @Override
    Long executeCommand(String command, Map params) {
        throw new NotSupportError(connection, 'execute command')
    }

    @Override
    Long getSequence(String sequenceName) {
        throw new NotSupportError(connection, 'sequence')
    }

    AdminClient createAdminClient() {
        checkKafkaParams()

        def con = currentKafkaConnection

        def props = new Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, con.bootstrapServers)
        def res = AdminClient.create(props)

        return res
    }

    List<String> listTopics(AdminClient adminClient = null) {
        def res = [] as List<String>
        def admin = adminClient?:createAdminClient()
        try {
            admin.listTopics().listings().get().each {topic ->
                res.add(topic.name())
            }
        }
        finally {
            if (adminClient == null)
                admin.close()
        }

        return res
    }

    Boolean existsTopic(String topicName, AdminClient adminClient = null) {
        if (topicName == null)
            throw new RequiredParameterError(connection, 'topicName', 'existsTopic')

        def admin = adminClient?:createAdminClient()
        Boolean res = false
        try {
            res = (listTopics(admin).indexOf(topicName) != -1)
        }
        finally {
            if (adminClient == null)
                admin.close()
        }

        return res
    }

    void createTopic(String topicName, Integer numPartitions, Short replicationFactor, Boolean ifNotExists, AdminClient adminClient = null) {
        if (topicName == null)
            throw new RequiredParameterError(connection, 'topicName', 'createTopic')

        def admin = adminClient?:createAdminClient()
        try {
            def exists = existsTopic(topicName, admin)
            if (exists && !ifNotExists)
                throw new ConnectionError(connection, '#kafka.topic_already', [topic: topicName])

            if (!exists) {
                def topic = new NewTopic(topicName, numPartitions, replicationFactor)
                admin.createTopics(Collections.singleton(topic))
            }
        }
        finally {
            if (adminClient == null)
                admin.close()
        }
    }

    void dropTopic(String topicName, Boolean ifExists, AdminClient adminClient = null) {
        if (topicName == null)
            throw new RequiredParameterError(connection, 'topicName', 'dropTopic')

        def admin = adminClient?:createAdminClient()
        try {
            def exists = existsTopic(topicName, admin)
            if (!exists && !ifExists)
                throw new ConnectionError(connection, '#kafka.topic_not_found', [topic: topicName])

            if (exists) {
                admin.deleteTopics(Collections.singleton(topicName))
            }
        }
        finally {
            if (adminClient == null)
                admin.close()
        }
    }
}