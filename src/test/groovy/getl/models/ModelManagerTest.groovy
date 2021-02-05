package getl.models

import getl.test.TestRepository
import org.junit.Before

import static getl.test.TestRunner.Dsl
import org.junit.Test

class ModelManagerTest extends TestRepository {
    @Test
    void testRegistration() {
        Dsl {
            def origMod = models.referenceVerticaTables('test:model1', true) {
                useReferenceConnection verticaConnection('ver:con')
                referenceSchemaName = '_test_reference'
                modelVars.test = 1

                referenceFromTable('ver:table1') {
                    objectVars.test = '2'
                }
            }
            assertEquals(['test:model1'], models.listReferenceVerticaTables('test:*'))

            thread {
                runMany(3) {
                    def mod = models.referenceVerticaTables('test:model1')
                    assertEquals(verticaConnection('ver:con'), mod.referenceConnection)
                    assertEquals('_test_reference', mod.referenceSchemaName)
                    assertEquals(1, mod.modelVars.test)
                    assertEquals('2', mod.referenceFromTable('ver:table1').objectVars.test)

                    assertNotEquals(origMod, mod)
                }
            }

            forGroup 'test'
            models.referenceVerticaTables('model2', true) {
                forGroup 'ver'
                useReferenceConnection verticaConnection('con')
                referenceSchemaName = '_test_reference'

                referenceFromTable('table1')
            }

            assertEquals(['test:model1', 'test:model2'].sort(), models.listReferenceVerticaTables('*:*').sort())
        }
    }
}