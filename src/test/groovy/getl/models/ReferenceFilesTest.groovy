package getl.models

import getl.test.TestRepository
import groovy.transform.InheritConstructors
import org.junit.Test
import static getl.test.TestRunner.Dsl

@InheritConstructors
class ReferenceFilesTest extends TestRepository {
    @Test
    void testFill() {
        Dsl {
            def f = files {
                useConfig 'files1'
                if (existsDirectory('reference'))
                    removeDir('reference', true)

                createDir 'reference'
            }

            models.referenceFiles('files') {
                fill()
            }

            f.with {
                assertTrue(existsFile('reference/тест.txt'))
                assertEquals('12345', new File(rootPath + '/reference/тест.txt').text)
            }
        }
    }
}