package getl.test

import getl.utils.Config
import getl.utils.Logs
import groovy.transform.InheritConstructors

@InheritConstructors
abstract class GetlTest extends GroovyTestCase {
    @Override
    void setUp() {
        super.setUp()
        Logs.Init()
    }

    @Override
    void tearDown() {
        super.tearDown()
        Config.ReInit()
    }

    @Override
    protected void runTest() {
        if (allowTests()) super.runTest()
    }

    /** Allow to run tests */
    boolean allowTests() { true }
}