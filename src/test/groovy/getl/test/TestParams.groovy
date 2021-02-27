package getl.test

import org.junit.Test

class TestParams extends GetlDslTest {
    @Test
    void testParameters() {
        TestVars.each { k, v ->
            println "$k: $v"
        }
    }
}
