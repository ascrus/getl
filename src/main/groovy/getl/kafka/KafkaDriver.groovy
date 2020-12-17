package getl.kafka

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.GenerationUtils
import groovy.json.JsonSlurper
import org.apache.kafka.clients.consumer.ConsumerRecord
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

        def props = new Properties()
        props.putAll(con.connectProperties)
        props.put('bootstrap.servers', con.bootstrapServers)
        props.put('group.id', con.groupId)
        props.put('key.deserializer', 'org.apache.kafka.common.serialization.StringDeserializer')
        props.put('value.deserializer', 'org.apache.kafka.common.serialization.StringDeserializer')
        if (limit != null)
            props.put('max.poll.records', limit)
        else
            props.put('max.poll.records', Integer.MAX_VALUE)

        def cl = generateParser(ds, fields)

        def res = 0L
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props)
        try {
            def topicParts = kafkaConsumer.partitionsFor(topicName).collect { new TopicPartition(topicName, it.partition()) }
            kafkaConsumer.assign(topicParts)
            def endOffs = kafkaConsumer.endOffsets(topicParts)
            if (endOffs.any { kafkaConsumer.position(it.key) < it.value }) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(dur)
                def json = new JsonSlurper()
                for (ConsumerRecord<String, String> rec : records) {
                    def recKey = rec.key()
                    if (keyName == null || keyName == recKey) {
                        def recValue = rec.value() as String
                        if (recValue?.length() > 0) {
                            def data = json.parseText(recValue)
                            def row = cl.call(data)
                            code.call(row)
                            res++
                        }
                    }
                }
            }
            kafkaConsumer.commitSync()
            kafkaConsumer.unsubscribe()
        }
        finally {
            kafkaConsumer.close()
        }
        return res
    }

    /**
     * Generate Json parser
     * @param dataset source dataset
     * @param listFields list of parsed fields
     * @return
     */
    private Closure<Map<String, Object>> generateParser(KafkaDataset dataset, List<String> listFields) {
        def rootNode = dataset.rootNode

        StringBuilder sb = new StringBuilder()
        sb << '{ Map rec ->\n'
        sb << '  def row = [:] as Map<String, Object>\n'
        dataset.field.each { Field d ->
            if (listFields.isEmpty() || listFields.find { it.toLowerCase() == d.name.toLowerCase() }) {
                Field s = d.copy()
                if (s.type in [Field.Type.DATETIME, Field.Type.DATE, Field.Type.TIME, Field.Type.TIMESTAMP_WITH_TIMEZONE])
                    s.type = Field.Type.STRING

                def fn = d.alias?:(((rootNode != null)?"${rootNode}.":'') + d.name)
                String path = GenerationUtils.ProcessAlias(fn, true)

                sb << "  row.put('${d.name.toLowerCase()}', "
                sb << GenerationUtils.GenerateConvertValue(d, s, d.format?:'yyyy-MM-dd\'T\'HH:mm:ss', "rec.${path}", false)
                sb << ')\n'
            }
        }
        sb << '  return row\n'
        sb << '}\n'
//        println sb.toString()

        Closure<Map<String, Object>> res

        def script = sb.toString()
        def hash = script.hashCode()
        def driverParams = dataset._driver_params as Map<String, Object>
        if (((driverParams.hash_code_read as Integer)?:0) != hash) {
            res = GenerationUtils.EvalGroovyClosure(script) as Closure<Map<String, Object>>
            driverParams.code_read = res
            driverParams.hash_code_read = hash
        }
        else {
            res = driverParams.code_read as Closure<Map<String, Object>>
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
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
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