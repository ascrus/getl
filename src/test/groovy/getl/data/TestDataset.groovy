package getl.data

import getl.test.GetlTest
import getl.utils.ListUtils
import getl.utils.MapUtils
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

    @Test
    void testCheckRow() {
        new Dataset().tap {
            field('id') { type = integerFieldType; isKey = true; length = 3 }
            field('name') {isNull = false; length = 10 }
            field('value') { type = numericFieldType; length = 5; precision = 2 }
            field('dt') { type = datetimeFieldType }

            def res1 = checkRowByFields([id: 1234, name: '12345', value: 123.45], true, true)
            //println res1
            assertTrue(res1.isEmpty())

            def res2 = checkRowByFields([value: 123.456], true, true)
            //println res2
            assertEquals(2, res2.size())
            assertEquals(['id', 'name'], res2.notnull)
            assertEquals(['value'], res2.length)
        }
    }

    @Test
    void testDetectChangeFields() {
        def ds1 = new Dataset().tap {
            field('id') { type = integerFieldType; isKey = true; length = 3 }
            field('name') {isNull = false; length = 10 }
            field('value') { type = numericFieldType; length = 5; precision = 2 }
            field('dt') { type = datetimeFieldType }
        }

        def ds2 = new Dataset().tap {
            field = ds1.field
        }
        assertTrue(Dataset.DetectChangeFields(ds1, ds2.field).isEmpty())

        ds1.removeField('dt')
        def res = Dataset.DetectChangeFields(ds1, ds2.field)
        assertEquals('[field_not_found:[comparedField: "dt DATETIME", datasetField: <none>]]', res.toString())

        ds1.field = ds2.field
        ds2.fieldByName('value').type = Field.doubleFieldType
        res = Dataset.DetectChangeFields(ds1, ds2.field, true)
        assertTrue(res.isEmpty())
        res = Dataset.DetectChangeFields(ds1, ds2.field, false)
        assertEquals('[field_type_not_compatible:[comparedField: "value DOUBLE", datasetField: "value NUMERIC(5, 2)"]]', res.toString())

        ds2.field = ds1.field
        ds2.fieldByName('name').length = 20
        res = Dataset.DetectChangeFields(ds1, ds2.field)
        assertEquals('[field_length_not_compatible:[comparedField: "name STRING(20) NOT NULL", datasetField: "name STRING(10) NOT NULL"]]', res.toString())

        ds2.field = ds1.field
        ds2.fieldByName('value').precision = 5
        res = Dataset.DetectChangeFields(ds1, ds2.field)
        assertEquals('[field_length_not_compatible:[comparedField: "value NUMERIC(5, 5)", datasetField: "value NUMERIC(5, 2)"]]', res.toString())

        ds2.field = ds1.field
        ds1.fieldByName('name').isNull = true
        res = Dataset.DetectChangeFields(ds1, ds2.field)
        assertEquals('[field_null_not_compatible:[comparedField: "name STRING(10) NOT NULL", datasetField: "name STRING(10)"]]', res.toString())

        ds1.field = ds2.field
        ds1.fieldByName('id').isKey = false
        res = Dataset.DetectChangeFields(ds1, ds2.field)
        assertEquals('[field_key_not_compatible:[comparedField: "id INTEGER KEY", datasetField: "id INTEGER NOT NULL"]]', res.toString())

        ds1.field = ds2.field
        ds2.removeField('dt')
        res = Dataset.DetectChangeFields(ds1, ds2.field, true, true, true, false, true)
        assertEquals('[unnecessary_field:[comparedField: <none>, datasetField: "dt DATETIME"]]', res.toString())
    }
}