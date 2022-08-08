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
    protected GetlDslTest() {
        super()
        readGetlTestProperties()
    }

    /** Read unit test properties from file and system variables */
    private void readGetlTestProperties() {
        def isSysEnvExists = System.properties.containsKey('getl-test-env')
        if (isSysEnvExists) {
            getlDefaultConfigEnvironment = System.properties.get('getl-test-env')
        }
        else
            getlDefaultConfigEnvironment = 'dev'

        def propFile = new File('getl-test-properties.conf')
        if (!propFile.exists()) {
            getlTestConfigProperties = new HashMap<String, Object>()
            return
        }

        getlTestConfigProperties = ConfigSlurper.LoadConfigFile(file: propFile)
        if (!isSysEnvExists && getlTestConfigProperties.containsKey('defaultEnv'))
            getlDefaultConfigEnvironment = getlTestConfigProperties.defaultEnv as String
    }

    /** Additional properties for test methods */
    private Map<String, Object> getlTestConfigProperties
    /** Additional properties for test methods */
    protected Map<String, Object> getGetlTestConfigProperties() { getlTestConfigProperties }

    /** Default configuration environment */
    private String getlDefaultConfigEnvironment
    /** Default configuration environment */
    protected String getGetlDefaultConfigEnvironment() { getlDefaultConfigEnvironment }

    /** Environment for test methods */
    protected String configEnvironment

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
            //noinspection UnnecessaryQualifiedReference
            def a = description.getAnnotation(getl.test.Config)
            if (a != null)
                configEnvironment = a.env()
            else
                configEnvironment = getlDefaultConfigEnvironment
        }
    }

    /** Status init script */
    private Boolean initWasRun = false

    /** Clean Getl on every test */
    protected Boolean cleanGetlBeforeTest() { true }

    /** Auto loading the project configuration file getl-properties.conf */
    protected Boolean autoLoadProperties() { true }

    @Before
    void beforeDslTest() {
        if (cleanGetlBeforeTest()) {
            Getl.CleanGetl(false)
            initWasRun = false
        }

        if (!Getl.GetlInstanceCreated()) {
            def eng = useGetlClass().getConstructor().newInstance()
            Getl.GetlSetInstance(eng)
            eng.options.autoInitFromConfig = autoLoadProperties()
        }

        Getl.GetlInstance().tap {
            if (configuration.environment != configEnvironment)
                configuration.environment = configEnvironment
            if (configuration.environment != 'prod')
                enableUnitTestMode()
        }

        if ((!this.onceRunInitClass() || !initWasRun)) {
            def initClasses = [] as List<Class<Script>>
            def initClass = useInitClass()
            if (initClass != null)
                initClasses.add(initClass)
            Getl.GetlInstance().with {
                _initGetlProperties(initClasses, null, false, true, this.getClass().name)
            }
            initWasRun = true
        }
    }
}