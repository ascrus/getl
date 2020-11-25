package getl.models

import getl.test.TestRepository
import org.junit.Test
import static getl.lang.Getl.Dsl

class ReferenceVerticaTablesTest extends TestRepository {
    @Test
    void testModel() {
        Dsl {
            verticaTable('ver:table1') {
                drop(ifExists: true)
                create()
                etl.rowsTo {
                    writeRow {add ->
                        (1..3).each {
                            add id: it, name: "name $it", dt: new Date()
                        }
                    }
                }
            }

            models.referenceVerticaTables('model1', true) {
                useReferenceConnection 'ver:con'
                referenceSchemaName = '_reference_getl_model1'

                referenceFromTable('ver:table1') { allowCopy = true }

                dropReferenceTables()
                createReferenceTables(false, true)
                copyFromSourceTables(true)

                referenceFromTable('ver:table1') {
                    assertTrue(referenceTable.exists)
                    assertEquals(3, referenceTable.countRow())
                    workTable.truncate()
                    assertEquals(1, fill())
                    assertEquals(3, workTable.countRow())
                }
            }
        }
    }
}