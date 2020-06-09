package getl.models

import getl.lang.Getl
import getl.test.GetlDslTest
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class ReferenceFilesTest extends GetlDslTest {
    @Test
    void testFill() {
        Getl.Dsl {
            resourceFiles('test', true) {

            }

            models.referenceFiles('test', true) {

            }
        }
    }
}
