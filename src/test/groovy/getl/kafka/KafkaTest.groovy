package getl.kafka

import getl.files.SFTPManager
import getl.lang.Getl
import getl.stat.ProcessTime
import getl.test.GetlTest
import getl.utils.Config
import org.junit.Before
import org.junit.Test

class KafkaTest extends GetlTest {
    private KafkaConnection con
    private SFTPManager man
    private KafkaDataset ds = new KafkaDataset()

    static final def divPortions = 10

    @Before
    void init() {
        con = null

        if (!new File('tests/kafka/kafka.conf'))
            return

        Config.LoadConfig(fileName: 'tests/kafka/kafka.conf')
        con = new KafkaConnection(config: 'kafka')

        Config.LoadConfig(fileName: 'tests/filemanager/sftp.conf')
        man = new SFTPManager(config: 'test_sftp_filemanager')

        ds.with {
            useConnection con
            kafkaTopic = 'test1'
            dataNode = 'data'
            uniFormatDateTime = 'yyyy-MM-dd\'T\'HH:mm:ss'

            field('ID') { type = bigintFieldType }
            field('SHIPMENT_ID') { type = bigintFieldType }
            field('WEIGHT') { type = numericFieldType; length = 12; precision = 3 }
            field('LENGTH') { type = integerFieldType }
            field('WIDTH') { type = integerFieldType }
            field('HEIGHT') { type = integerFieldType }
            field('SELF_DELIVERY') { type = integerFieldType }
            field('ACCOUNTING_WEIGHT') { type = numericFieldType; length = 12; precision = 3 }
            field('STATE_ID') { type = bigintFieldType }
            field('DEPARTMENT_ID') { type = bigintFieldType }
            field('STATETRANSITMOMENT') { type = datetimeFieldType }
            field('IN_SPS') { type = integerFieldType }
            field('LAST_EVENT_STATE_ID') { type = bigintFieldType }

            field('_meta_operation') { alias = 'meta.op' }
            field('_meta_table') { alias = 'meta.table' }
            field('_meta_key_ID') { type = bigintFieldType; alias = 'key.ID' }
        }
    }

    @Test
    void testConsumer() {
        man.connect()
        new ProcessTime(name: 'Generate kafka records', objectName: 'file', debug: true).run {
            (1..divPortions).each {
                def sb1 = new StringBuilder()
                def sb2 = new StringBuilder()
                def resCommand = man.command('/opt/kafka/kafka_2.13-2.6.0/bin/kafka-console-producer.sh --bootstrap-server stand2.easydata.ru:9092 --topic test1 < /data/sftp/getl/ARG_PARCEL.txt', sb1, sb2)
                if (resCommand != 0) {
                    println sb1.toString()
                    println sb2.toString()
                    assertEquals(0, resCommand)
                }
            }

            return divPortions
        }
        man.disconnect()

        new ProcessTime(name: 'Load Kafka rows', debug: true).run {
            ds.eachRow { row ->
                assertEquals('ARGIS.ARG_PARCEL', row._meta_table)
                assertNotNull(row._meta_operation)
                if (row._meta_operation == 'ins')
                    assertNotNull(row.id)
                else
                    assertNotNull(row._meta_key_id)
            }
            return ds.readRows
        }
        assertEquals(1954 * divPortions, ds.readRows)
    }

    @Test
    void testProducer() {
        Getl.Dsl {
            options {processTimeDebug = true }

            def jsonFile = json {
                fileName = 'resource:/kafka/kafka.json'
                rootNode = '.'
                dataNode = 'data'
                uniFormatDateTime = ds.uniFormatDateTime
                field = ds.field
            }

            def tmpFile = csvTemp {
                field = ds.field
                append = true
            }

            profile('Generate temp csv file') {prof ->
                (1..divPortions).each {
                    prof.countRow += etl.copyRows(jsonFile, tmpFile).countRow
                }
            }

            def dw = kafka {
                useConnection con
                kafkaTopic = 'test2'
                field = ds.field
            }

            profile('Send temp csv file to Kafka topic') {
                countRow = etl.copyRows(tmpFile, dw).countRow
            }
        }
    }
}