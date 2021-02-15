package getl.models

import getl.data.Field
import getl.test.TestRepository
import org.junit.Test
import static getl.test.TestRunner.Dsl

class setOfTablesTest extends TestRepository {
    @Test
    void testSetOfTables() {
        Dsl {
            models.setOfTables('list1', true) {
                useSourceConnection embeddedConnection('h2:con')
                table('h2:table1') {
                    assertEquals('h2:table1', sourceTableName)
                    assertEquals('public', sourceTable.schemaName)
                    assertEquals('getl_table1', sourceTable.tableName)
                    assertEquals(Field.integerFieldType, sourceTable.field('id').type)
                }

                assertEquals(['h2:table1'], findModelDatasets())
                assertEquals(['h2:table1'], findModelDatasets(['h2:*']))
                assertEquals(['h2:table1'], findModelDatasets(['h2:*1']))
                assertTrue(findModelDatasets(null, ['h2:*']).isEmpty())
                assertEquals(['h2:table1'], findModelDatasets(null, ['h2:*2']))
            }
        }
    }
}