package getl.models

import getl.test.TestRepository
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import org.junit.Test
import static getl.test.TestRunner.Dsl

@InheritConstructors
class ReferenceFilesTest extends TestRepository {
    @Test
    void testFill() {
        Dsl {
            def f = files {
                rootPath = '{GETL_TEST}/getl.dsl'
                createRootPath = true
                if (FileUtils.ExistsFile(currentRootPath))
                    FileUtils.DeleteFolder(currentRootPath, true)
                connect()
                createDir('reference')
            }

            models.referenceFiles('files') {
                fill()
            }

            f.with {
                assertTrue(existsFile('reference/тест.txt'))
                assertEquals('12345', new File(currentRootPath + '/reference/тест.txt').text)
            }
        }
    }
}