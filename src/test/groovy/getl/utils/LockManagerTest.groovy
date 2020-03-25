package getl.utils

import getl.lang.Getl
import getl.test.GetlTest
import getl.tfs.TFS
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class LockManagerTest extends GetlTest {
    @Test
    void testScheduleLocks() {
        Getl.Dsl(this) {
            def man = new LockManager(true, 1)
            def counter = new SynchronizeObject()
            thread {
                useList (1..100)
                run(10) {
                    def f = new File("${TFS.systemPath}/test_file.lock")
                    man.lockObject(f.path) {
                        if (!f.exists()) {
                            f.deleteOnExit()
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
            pause 1500
            assertTrue(man.isEmpty())
        }
    }
}