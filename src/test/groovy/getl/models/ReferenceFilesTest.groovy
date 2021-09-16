package getl.models

import getl.test.TestRepository
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import org.junit.Test
import static getl.test.TestRunner.Dsl

@InheritConstructors
class ReferenceFilesTest extends TestRepository {
    static void fill(boolean local) {
        Dsl {
            def f = files {
                rootPath = '{GETL_TEST}/getl.dsl'
                connect()
                cleanDir()
            }

            models.referenceFiles('files') {
                localUnpack = local
                fill()
            }

            f.tap {
                assertTrue(existsFile('reference/тест.txt'))
                assertEquals('12345', new File(currentRootPath + '/reference/тест.txt').text)

                assertTrue(existsFile('reference1/тест.txt'))
                assertEquals('12345', new File(currentRootPath + '/reference1/тест.txt').text)
            }
        }
    }

    @Test
    void remoteUnpack() {
        fill(false)
    }

    @Test
    void localUnpack() {
        fill(true)
    }
}