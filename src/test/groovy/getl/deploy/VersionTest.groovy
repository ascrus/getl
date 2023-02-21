package getl.deploy

import getl.test.GetlTest
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class VersionTest extends GetlTest {
    @Test
    void initTest() {
        assertNotNull(Version.instance.version)
        assertNotNull(Version.instance.versionNum)
        assertNotNull(Version.instance.years)
        assertTrue(Version.instance.IsCompatibility(4.1800))
        assertFalse(Version.instance.IsCompatibility(4.0000))
        Version.instance.sayInfo(true)
    }
}