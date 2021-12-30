package getl.data

import getl.test.GetlTest
import org.junit.Test

class TestDataset extends GetlTest {
    @Test
    void testCompareFields() {
        def ds1 = new Dataset().tap {
            field('id') { type = integerFieldType; isKey = true; checkValue = 'id > 0' }
            field('name') { length = 50; isNull = false }
            field('dt') { type = datetimeFieldType; isNull = false; defaultValue = 'Now()' }
            field('value') { type = numericFieldType; length = 12; precision = 2 }
            field('year') { type = integerFieldType; compute = 'Year(dt)' }
        }

        def ds2 = new Dataset().tap {
            field = ds1.field
        }

        assertEquals(0, ds1.compareFields(ds2.field).size())

        ds1.field('value').isNull = false
        assertEquals([value: Dataset.EqualFieldStatus.CHANGED], ds1.compareFields(ds2.field))
        assertEquals(0, ds1.compareFields(ds2.field, true).size())

        ds1.field = ds2.field
        ds1.field('name').length = 40
        assertEquals([name: Dataset.EqualFieldStatus.CHANGED], ds1.compareFields(ds2.field))
        assertEquals(0, ds1.compareFields(ds2.field, true).size())

        ds1.removeField('name')
        assertEquals([name: Dataset.EqualFieldStatus.DELETED], ds1.compareFields(ds2.field))

        ds1.field = ds2.field
        ds1.field('description') { type = textFieldType }
        assertEquals([description: Dataset.EqualFieldStatus.ADDED], ds1.compareFields(ds2.field))
    }
}
