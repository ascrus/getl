package getl.deploy

import getl.test.GetlTest
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
public class VersionTest extends GetlTest {
    @Test
    void initTest() {
        assertNotNull(Version.version)
        assertNotNull(Version.versionNum)
        assertNotNull(Version.years)
        assertTrue(Version.IsCompatibility(4.0300))
        assertFalse(Version.IsCompatibility(4.0205))
        Version.SayInfo(true)
    }
}