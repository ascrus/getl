package getl.kafka

import getl.files.SFTPManager
import getl.lang.Getl
import getl.stat.ProcessTime
import getl.test.GetlTest
import getl.utils.Config
import getl.utils.Logs
import org.junit.Before
import org.junit.Ignore
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

        Logs.global.logFileName = '{GETL_TEST}/logs/kafka.{date}.log'

        ds.with {
            useConnection con
            kafkaTopic = 'test1'
            dataNode = 'data'
            uniFormatDateTime = 'yyyy-MM-dd\'T\'HH:mm:ss'
            autoCreateTopic = true

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

            field('_meta_operation') { alias = '#root.meta.op' }
            field('_meta_table') { alias = '#root.meta.table' }
            field('_meta_key_ID') { type = bigintFieldType; alias = '#root.key.ID' }
        }
    }

    @Test
    void testWriteAndRead() {
        if (con == null)
            return

        try {
            con.connect()
        }
        catch (Exception ignored) {
            Logs.Warning('Kafka not ready!')
            return
        }

        Getl.Dsl {
            con.dropTopic('test-getl', true)
            con.createTopic('test-getl', 1, 1, true)

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
                useConnection (con.cloneConnection() as KafkaConnection).tap { groupId = 'test-getl-writer' }
                kafkaTopic = 'test-getl'
                field = ds.field
            }

            def countWrites = profile('Copy temp csv file to Kafka topic') {
                countRow = etl.copyRows(tmpFile, dw).countRow
            }.countRow
            assertEquals(tmpFile.readRows, countWrites)

            def tmpFileClone = csvTemp {
                field = ds.field
            }

            def dr = kafka {
                useConnection (con.cloneConnection() as KafkaConnection).tap { groupId = 'test-getl-reader' }
                kafkaTopic = 'test-getl'
                field = ds.field
                readOpts.offsetForRegister = offsetForRegisterEarliest
            }

            def countReads = profile('Copy Kafka topic to temp csv file') {
                countRow = etl.copyRows(dr, tmpFileClone).countRow
            }.countRow
            assertEquals(countWrites, countReads)
        }
    }
}