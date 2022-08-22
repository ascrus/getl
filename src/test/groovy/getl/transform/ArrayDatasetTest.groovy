package getl.transform

import getl.data.Field
import getl.lang.Getl
import getl.test.TestDsl
import org.junit.Test

class ArrayDatasetTest extends TestDsl {
    @Test
    void testWork() {
        Getl.Dsl {
            def ds = arrayDataset('test1', true) {
                assertNotNull(fieldByName('value'))
                field('value') { type = integerFieldType }
                shouldFail { field('new_field') }
                shouldFail { removeField('new_field') }
                shouldFail { field.clear() }
                shouldFail { it.field = [] }
                assertEquals(0, countRow())

                localDatasetData = [1,2,3,4,5]
                assertEquals(5, countRow())
                assertEquals([1,2,3,4,5], rows().collect { it.value as Integer })
            }

            attachToArray([0], arrayDataset('test1'))
            assertEquals([0], ds.rows().collect { it.value as Integer })

            attachToArray([1, 2, 3], 'test2') { name = 'num'; type = integerFieldType }
            arrayDataset('test2') {
                assertEquals('num', field[0].name)
                assertEquals(Field.integerFieldType, field[0].type)
                assertEquals(3, countRow())
                assertEquals([1,2,3], rows().collect { it.num as Integer })
            }

            cloneDataset('test3', arrayDataset('test2'))
            arrayDataset('test3') {
                assertEquals('num', field[0].name)
                assertEquals(Field.integerFieldType, field[0].type)
                assertEquals(0, countRow())
            }
        }
    }
}