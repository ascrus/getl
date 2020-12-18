package getl.kafka

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.json.JSONConnection
import getl.json.JSONDataset
import getl.tfs.TFS
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.MapUtils
import getl.utils.StringUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import java.time.Duration

class KafkaDriver extends Driver {
    @Override
    List<Support> supported() {
        return [Support.EACHROW, Support.WRITE, Support.DATE, Support.TIME, Support.TRANSACTIONAL]
    }

    @Override
    List<Operation> operations() {
        return [Operation.INSERT]
    }

    @Override
    Boolean isConnected() {
        throw new ExceptionGETL('Not supported!')
    }

    /** Current Kafka connection object  */
    KafkaConnection getCurrentKafkaConnection() { connection as KafkaConnection }

    @Override
    void connect() {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void disconnect() {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    List<Object> retrieveObjects(Map params, Closure<Boolean> filter) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    List<Field> fields(Dataset dataset) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void startTran() {
        kafkaProducer.beginTransaction()
    }

    @Override
    void commitTran() {
        kafkaProducer.commitTransaction()
    }

    @Override
    void rollbackTran() {
        kafkaProducer.abortTransaction()
    }

    @Override
    void createDataset(Dataset dataset, Map params) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
        def ds = dataset as KafkaDataset
        def topicName = ds.kafkaTopic
        if (topicName == null)
            throw new ExceptionGETL('In the dataset, you must specify the name of the topic Kafka!')

        if (ds.field.isEmpty())
            throw new ExceptionGETL("Required fields description with dataset!")

        def con = currentKafkaConnection
        if (con.bootstrapServers == null)
            throw new ExceptionGETL('No servers are specified to connect to in the "bootstrapServers"!')
        if (con.groupId == null)
            throw new ExceptionGETL('Group id required!')

        def fields = [] as List<String>
        if (prepareCode != null) {
            prepareCode.call(fields)
        } else if (params.fields != null)
            fields = params.fields as List<String>

        def ro = ds.readOpts
        def keyName = ro.keyName
        def dur = Duration.ofMillis(ro.readDuration?:(Long.MAX_VALUE))
        def limit = ro.limit
        def maxPoolRecords = ro.maxPollRecords?:10000

        def props = new Properties()
        props.putAll(con.connectProperties)
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, con.bootstrapServers)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, con.groupId)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'latest')
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.name)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.name)
        if (limit != null)
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, limit)
        else
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoolRecords)

        def res = 0L
        KafkaConsumer<String, String> kafkaConsumer
        try {
            kafkaConsumer = new KafkaConsumer<String, String>(props)
        }
        catch (Exception e) {
            Logs.Severe("Can't connect to Kafka servers: ${e.message}")
            Logs.Dump(e, 'KafkaDriver', ds.toString(), MapUtils.ToJson(props))
            throw e
        }

        def jsonDirName = "${TFS.systemPath}/${FileUtils.UniqueFileName()}"
        def jsonDirFile = new File(jsonDirName)
        FileUtils.ValidPath(jsonDirFile, true)
        def jsonName = "$jsonDirFile/kafka."
        def jsonFile = new File(jsonName + StringUtils.AddLedZeroStr(1, 6) + '.json')
        jsonFile.deleteOnExit()

        try {
            def topicParts = kafkaConsumer.partitionsFor(topicName).collect { new TopicPartition(topicName, it.partition()) }
            kafkaConsumer.assign(topicParts)
            def endOffs = kafkaConsumer.endOffsets(topicParts)

            def curRows = 0L
            def countPortions = 1
            def isStart = true

            def writer = jsonFile.newWriter()
            try {
                writer.append('[')
                while (endOffs.any { kafkaConsumer.position(it.key) < it.value }) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(dur)

                    if (curRows >= 10000) {
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
                            }
                        }
                    }

                    if (limit != null)
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

            def jsonCon = new JSONConnection(path: jsonDirName)
            def json = new JSONDataset()
            json.with {
                useConnection jsonCon
                extension = 'json'
                field = (!fields.isEmpty())?ds.getFields(fields):ds.field
                rootNode = '.'
                dataNode = ds.dataNode
            }
            (1..countPortions).each {num ->
                json.with {
                    fileName = 'kafka.' + StringUtils.AddLedZeroStr(num, 6)
                    eachRow(code)
                    res += readRows
                    drop()
                }
            }

            kafkaConsumer.commitSync()
        }
        finally {
            kafkaConsumer.unsubscribe()
            kafkaConsumer.close()
        }
        return res
    }

    /** Kafka producer for write rows */
    private KafkaProducer<String, String> kafkaProducer

    @Override
    void openWrite(Dataset dataset, Map params, Closure prepareCode) {
        def con = currentKafkaConnection
        if (con.bootstrapServers == null)
            throw new ExceptionGETL('No servers are specified to connect to in the "bootstrapServers"!')
        if (con.groupId == null)
            throw new ExceptionGETL('Group id required!')

        def props = new Properties()
        props.putAll(con.connectProperties)
        props.put('bootstrap.servers', con.bootstrapServers)
        props.put("acks", "all")
        props.put("retries", 0)
        props.put("batch.size", 16384)
        props.put("linger.ms", 1)
        props.put("buffer.memory", 33554432)
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        kafkaProducer = new KafkaProducer<String, String>(props)

        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void write(Dataset dataset, Map row) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void doneWrite(Dataset dataset) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void closeWrite(Dataset dataset) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void clearDataset(Dataset dataset, Map params) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    Long executeCommand(String command, Map params) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    Long getSequence(String sequenceName) {
        throw new ExceptionGETL('Not supported!')
    }
}