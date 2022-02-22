package getl.deploy

import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class VersionTest extends getl.test.GetlTest {
    @Test
    void initTest() {
        assertNotNull(Version.instance.version)
        assertNotNull(Version.instance.versionNum)
        assertNotNull(Version.instance.years)
        assertTrue(Version.instance.IsCompatibility(4.0300))
        assertFalse(Version.instance.IsCompatibility(4.0205))
        Version.instance.sayInfo(true)
    }
}