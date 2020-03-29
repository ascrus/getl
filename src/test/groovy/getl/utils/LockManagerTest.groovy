package getl.utils

import getl.lang.Getl
import getl.tfs.TFS
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class LockManagerTest extends getl.test.GetlTest {
    @Test
    void testScheduleLocks() {
        Getl.Dsl(this) {
            def man = new LockManager(true, 3000)
            def counter = new SynchronizeObject()
            thread {
                useList (1..100)
                abortOnError = true
                def file = new File("${TFS.systemPath}/${FileUtils.UniqueFileName()}")
                file.deleteOnExit()
                def fileName = file.path
                run(50) {
                    def f = new File(fileName)
                    man.lockObject(f.path) {
                        if (!f.exists()) {
                            f.text = '12345'
                            counter.nextCount()
                        }
                        else
                            assertEquals('12345', f.text)
                    }
                }
            }
            assertEquals(1, counter.count)
            assertFalse(man.isEmpty())
            man.lockLife = 1
            pause 500
            assertTrue(man.isEmpty())
        }
    }
}