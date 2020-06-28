package getl.models

import getl.test.TestRepository
import org.junit.Before

import static getl.test.TestRunner.Dsl
import org.junit.Test

class ModelManagerTest extends TestRepository {
    @Test
    void testRegistration() {
        Dsl {
            models.referenceVerticaTables('test:model1', true) {
                useReferenceConnection verticaConnection('ver:con')
                referenceSchemaName = '_test_reference'

                referenceFromTable('ver:table1')
            }
            assertEquals(['test:model1'], models.listReferenceVerticaTables('test:*'))

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