package getl.lang

import getl.test.TestDsl
import org.junit.Test

class CallDslTest extends TestDsl {
    def i1 = 100
    public i2 = 200
    protected i3 = 300
    private i4 = 400

    @Test
    void testCallDsl() {
        Getl.Dsl { main ->
            assertEquals(100, i1)
            assertEquals(200, i2)
            assertEquals(300, i3)
            assertEquals(400, i4)

            assertTrue(main.unitTestMode)
            assertTrue(unitTestMode)
            main.unitTestMode = false
            assertFalse(main.unitTestMode)
            assertFalse(unitTestMode)

            unitTestMode = true
            assertTrue(main.unitTestMode)
            assertTrue(unitTestMode)
            unitTestMode = false
            assertFalse(main.unitTestMode)
            assertFalse(unitTestMode)

            unitTestMode = true
            assertTrue(unitTestMode)
            assertTrue(main.unitTestMode)
            def m = models.workflow {
                start {
                    initCode = '''
assert unitTestMode == modelVars.test; 
ifUnitTestMode { modelVars.res = true }
ifRunAppMode { modelVars.res = false }'''
                }
                modelVars.test = unitTestMode
                assertTrue(modelVars.test as Boolean)
            }
            m.execute()
            assertTrue(m.modelVars.res as Boolean)

            unitTestMode = false
            assertFalse(unitTestMode)
            assertFalse(main.unitTestMode)
            m.modelVars.test = unitTestMode
            assertFalse(m.modelVars.test as Boolean)
            m.execute()
            assertFalse(m.modelVars.res as Boolean)
        }
    }
}