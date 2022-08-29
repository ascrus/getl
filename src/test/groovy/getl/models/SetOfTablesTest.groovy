package getl.models

import getl.data.Field
import getl.test.TestRepository
import org.junit.Test
import static getl.test.TestRunner.Dsl

class SetOfTablesTest extends TestRepository {
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
                cloneDataset('h2:table2', embeddedTable('h2:table1'))
                table('h2:table2') {
                    map.name = 'Upper(name)'
                }
                assertEquals('Upper(name)', table('h2:table2').map.name)

                assertEquals(['h2:table1', 'h2:table2'], findModelObjects())
                assertEquals(['h2:table1', 'h2:table2'], findModelObjects(['h2:*']))
                assertEquals(['h2:table1'], findModelObjects(['h2:*1']))
                assertTrue(findModelObjects(null, ['h2:*']).isEmpty())
                assertEquals(['h2:table1'], findModelObjects(null, ['h2:*2']))
                assertEquals(['h2:table1'], findModelObjects(['h2:*'], ['h2:*2']))

                assertTrue(isObjectInModel('h2:table1'))
            }
        }
    }
}