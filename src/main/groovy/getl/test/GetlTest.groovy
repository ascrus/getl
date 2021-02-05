package getl.test

import getl.config.ConfigFiles
import getl.config.ConfigManager
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.MapUtils
import groovy.test.GroovyAssert
import groovy.time.Duration
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass

import java.sql.Time

/**
 * Getl functional testing base class
 * @author Alexsey Konstantinov
 *
 */
class GetlTest extends GroovyAssert {
    /** Configuration manager class to use */
    protected Class<ConfigManager> useConfigManager() { ConfigFiles }

    /** Install the required configuration manager */
    protected InstallConfigManager() {
        if (!useConfigManager().isInstance(Config.configClassManager))
            Config.configClassManager = useConfigManager().newInstance() as ConfigManager
    }

    @BeforeClass
    static void InitTestClass() {
        Config.ReInit()
        Logs.Init()
        FileUtils.ListResourcePath.clear()
    }

    @AfterClass
    static void DoneTestClass() {
        Logs.Done()
        FileUtils.ListResourcePath.clear()
    }

    @Before
    void beforeTest() {
        InstallConfigManager()
        //noinspection UnnecessaryQualifiedReference
        org.junit.Assume.assumeTrue(allowTests())
    }

    /** Allow to run tests */
    Boolean allowTests() { true }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param message message text
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(String message, Duration expected, Duration actual) {
        if (expected == null && actual == null)
            return

        assertEquals(message, expected.toString(), actual.toString())
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(Duration expected, Duration actual) {
        assertEquals(null as String, expected, actual)
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param message message text
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(String message, Map expected, Map actual) {
        if (expected == null && actual == null)
            return
        if ((expected == null && actual != null) || (expected != null && actual == null))
            throw new AssertionError('Parameters do not match, one of them is null!')

        def res = MapUtils.CompareMap(expected, actual)
        if (!res.isEmpty())
            throw new AssertionError('maps difference: ' + MapUtils.ToJson(res))
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(Map expected, Map actual) {
        assertEquals(null as String, expected, actual)
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param message message text
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(String message, Time expected, Time actual) {
        if (expected == null && actual == null)
            return

        assertEquals(message, expected.toString(), actual.toString())
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(Time expected, Time actual) {
        assertEquals(null, expected, actual)
    }
}