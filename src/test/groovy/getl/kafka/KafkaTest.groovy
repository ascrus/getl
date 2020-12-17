package getl.kafka

import getl.test.GetlTest
import getl.utils.Config
import org.junit.Before
import org.junit.Test

class KafkaTest extends GetlTest {
    private KafkaConnection con

    @Before
    void init() {
        con = null

        if (!new File('tests/kafka/kafka.conf'))
            return

        Config.LoadConfig(fileName: 'tests/kafka/kafka.conf')
        con = new KafkaConnection(config: 'kafka')
    }

    @Test
    void testConsumer() {
        KafkaDataset ds = new KafkaDataset(connection: con, kafkaTopic: 'test1', rootNode: 'data')
        ds.with {
            field('ID') { type = bigintFieldType }
            field('SHIPMENT_ID') { type = bigintFieldType }
            field('WEIGHT') { type = integerFieldType }
            field('LENGTH') { type = integerFieldType }
            field('WIDTH') { type = integerFieldType }
            field('HEIGHT') { type = integerFieldType }
            field('SELF_DELIVERY') { type = booleanFieldType }
            field('ACCOUNTING_WEIGHT') { type = integerFieldType }
            field('STATE_ID') { type = bigintFieldType }
            field('DEPARTMENT_ID') { type = bigintFieldType }
            field('STATETRANSITMOMENT') { type = datetimeFieldType }

            field('_meta_operation') { alias = 'meta.op' }
            field('_meta_table') { alias = 'meta.table' }
            field('_meta_key_ID') { type = bigintFieldType; alias = 'key.ID' }
        }
        ds.eachRow {println it }
    }
}