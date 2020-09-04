package getl.test

import getl.config.ConfigManager
import getl.config.ConfigSlurper
import getl.lang.Getl
import groovy.transform.InheritConstructors
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.rules.TestRule
import org.junit.rules.TestWatcher
import org.junit.runner.Description

/**
 * Dsl language functional testing base class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class GetlDslTest extends GetlTest {
    /** Environment for running method */
    protected String configEnvironment = 'dev'

    @Override
    protected Class<ConfigManager> useConfigManager() { ConfigSlurper }

    /** Use this initialization class at application startup if it is not explicitly specified */
    Class<Getl> useInitClass() { null }
    /** Run initialization only once */
    Boolean onceRunInitClass() { false }
    /** Used class for Getl */
    Class<Getl> useGetlClass() { Getl }

    @Rule
    public TestRule watcher = new TestWatcher() {
        @Override
        protected void starting(Description description) {
            def a = description.getAnnotation(getl.test.Config)
            if (a != null)
                configEnvironment = a.env()
            else
                configEnvironment = 'dev'
        }
    }

    @BeforeClass
    static void InitDslTestClass() {
        Getl.CleanGetl()
    }

    @AfterClass
    static void DoneDslTestClass() {
        Getl.CleanGetl()
    }

    /** Status init script */
    private Boolean initWasRun = false

    /** Clean Getl on every test */
    protected Boolean cleanGetlBeforeTest() { true }

    @Before
    void beforeDslTest() {
        if (cleanGetlBeforeTest()) {
            Getl.CleanGetl(false)
            initWasRun = false
        }

        if (!Getl.GetlInstanceCreated()) {
            def eng = useGetlClass().newInstance()
            Getl.GetlSetInstance(eng)
        }

        Getl.Dsl(this) {
            if (configuration().environment != configEnvironment)
                configuration().environment = configEnvironment
        }

        def initClass = useInitClass()
        if (initClass != null && (!this.onceRunInitClass() || !initWasRun)) {
            Getl.Dsl(this) {
                callScript initClass
            }
            initWasRun = true
        }
    }
}