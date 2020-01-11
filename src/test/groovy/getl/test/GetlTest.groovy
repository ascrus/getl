package getl.test

import getl.lang.Getl
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs
import groovy.transform.InheritConstructors
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@InheritConstructors
@RunWith(JUnit4.class)
abstract class GetlTest extends GroovyTestCase {
    @BeforeClass
    static void InitTestClass() {
        Getl.CleanGetl()
        Config.ReInit()
        Logs.Init()
        FileUtils.ListResourcePath.clear()
    }

    @AfterClass
    static void DoneTestClass() {
        Config.ReInit()
        Logs.Done()
        FileUtils.ListResourcePath.clear()
    }

    @Before
    void beforeTest() {
        org.junit.Assume.assumeTrue(allowTests());
    }

    /** Allow to run tests */
    boolean allowTests() { true }
}