package getl.test

import getl.config.ConfigFiles
import getl.config.ConfigManager
import getl.lang.Getl
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
import java.util.concurrent.ConcurrentHashMap

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
            Config.configClassManager = useConfigManager().getConstructor().newInstance() as ConfigManager
    }

    /** Variables for testing from JVM environment */
    static public final Map<String, String> TestVars = new ConcurrentHashMap<String, String>()

    /** Preparing variables for testing from JVM environment */
    static private PrepareTestVars() {
        System.properties.each { k, v ->
            if ((k as String).matches('getl[-]vars[.].+')) {
                def name = (k as String).substring(10)
                def value = v as String
                if (value?.length() > 0)
                    TestVars.put(name, value)
            }
        }
    }

    @BeforeClass
    static void InitTestClass() {
        Getl.CleanGetl(false)
        Config.ReInit()
        Config.RegisterChangeManagerEvent { oldManager, newManager ->
            newManager.mergeConfig(oldManager.content)
        }
        Logs.Init()
        FileUtils.ListResourcePath.clear()
        PrepareTestVars()
    }

    @AfterClass
    static void DoneTestClass() {
        Getl.CleanGetl(false)
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
        assert !((expected == null && actual != null) || (expected != null && actual == null)),
                'Parameters do not match, one of them is null!'

        def res = MapUtils.CompareMap(expected, actual)
        assert res.isEmpty(), "${message?:'Maps difference'}: " + MapUtils.ToJson(res)
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

    static Boolean IsJava8() {
        System.getProperty("java.version").matches('1[.]8[.].+')
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(GString expected, String actual) {
        assertEquals(null as String, expected.toString(), actual)
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param message message text
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(String message, GString expected, String actual) {
        assertEquals(message, expected.toString(), actual)
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(String expected, String actual) {
        assertEquals(null as String, expected.toString(), actual)
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param message message text
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(String message, String expected, String actual) {
        assertEquals(message, expected as Object, actual as Object)
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(String expected, GString actual) {
        assertEquals(null as String, expected, actual.toString())
    }

    /**
     * Asserts that two objects are equal. If they are not, an AssertionError without a message is thrown. If expected and actual are null, they are considered equal.
     * @param message message text
     * @param expected expected value
     * @param actual the value to check against expected
     */
    static void assertEquals(String message, String expected, GString actual) {
        assertEquals(message, expected, actual.toString())
    }
}