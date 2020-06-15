package getl.test

import getl.lang.Getl
import org.junit.Test

class TestAnnotations extends GetlDslTest {
    @Test
    @Config(env='dev')
    void testConfigDev() {
        Getl.Dsl {
            assertEquals('dev', configuration.environment)
        }
    }

    @Test
    @Config(env='prod')
    void testConfigProd() {
        Getl.Dsl {
            assertEquals('prod', configuration.environment)
        }
    }

    @Test
    void testConfigNon() {
        Getl.Dsl {
            assertEquals('dev', configuration.environment)
        }
    }
}