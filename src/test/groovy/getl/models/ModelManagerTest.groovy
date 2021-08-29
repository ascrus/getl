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
                modelAttrs.test = 'a'
                modelVars.test = 1

                referenceFromTable('ver:table1') {
                    attrs.test = 'b'
                    objectVars.test = 2
                }
            }
            assertEquals(['test:model1'], models.listReferenceVerticaTables('test:*'))

            thread {
                runMany(3) {
                    def mod = models.referenceVerticaTables('test:model1')
                    assertNotEquals(origMod, mod)
                    mod.tap {
                        assertEquals(verticaConnection('ver:con'), referenceConnection)
                        assertEquals('_test_reference', referenceSchemaName)
                        assertEquals(1, modelVars.test)
                        assertEquals(2, referenceFromTable('ver:table1').objectVars.test)
                        assertEquals(2, referenceFromTable('ver:table1').variable('test'))
                        assertEquals('a', modelAttrs.test)
                        assertEquals('b', referenceFromTable('ver:table1').attrs.test)
                        assertEquals('b', referenceFromTable('ver:table1').attribute('test'))
                    }
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

            def cloneMod = models.referenceVerticaTables('test:model1').clone() as ReferenceVerticaTables
            cloneMod.tap {
                modelAttrs.test = 'c'
                modelVars.test = 3

                referenceFromTable('ver:table1') {
                    attrs.test = 'd'
                    objectVars.test = 4
                }

                assertEquals(3, modelVars.test)
                assertEquals(4, referenceFromTable('ver:table1').objectVars.test)
                assertEquals(4, referenceFromTable('ver:table1').variable('test'))
                assertEquals('c', modelAttrs.test)
                assertEquals('d', referenceFromTable('ver:table1').attrs.test)
                assertEquals('d', referenceFromTable('ver:table1').attribute('test'))
            }

            origMod.tap {
                assertEquals(verticaConnection('ver:con'), referenceConnection)
                assertEquals('_test_reference', referenceSchemaName)
                assertEquals(1, modelVars.test)
                assertEquals(2, referenceFromTable('ver:table1').objectVars.test)
                assertEquals(2, referenceFromTable('ver:table1').variable('test'))
                assertEquals('a', modelAttrs.test)
                assertEquals('b', referenceFromTable('ver:table1').attrs.test)
                assertEquals('b', referenceFromTable('ver:table1').attribute('test'))
            }
        }
    }
}