package getl.lang

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.clickhouse.ClickHouseConnection
import getl.clickhouse.ClickHouseTable
import getl.config.*
import getl.csv.*
import getl.data.*
import getl.data.sub.AttachData
import getl.db2.*
import getl.dbf.*
import getl.deploy.Version
import getl.driver.Driver
import getl.excel.*
import getl.exception.*
import getl.files.*
import getl.firebird.*
import getl.h2.*
import getl.hive.*
import getl.impala.*
import getl.jdbc.*
import getl.json.*
import getl.kafka.*
import getl.lang.opts.*
import getl.lang.sub.*
import getl.models.*
import getl.models.sub.*
import getl.mssql.*
import getl.mysql.*
import getl.netezza.*
import getl.netsuite.*
import getl.oracle.*
import getl.postgresql.*
import getl.proc.*
import getl.proc.sub.*
import getl.salesforce.*
import getl.sap.HanaConnection
import getl.sap.HanaTable
import getl.sqlite.SQLiteConnection
import getl.sqlite.SQLiteTable
import getl.stat.*
import getl.test.GetlTest
import getl.tfs.*
import getl.transform.ArrayDataset
import getl.transform.ArrayDatasetConnection
import getl.utils.*
import getl.utils.sub.*
import getl.vertica.*
import getl.xml.*
import getl.yaml.*
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import java.sql.Time
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level

/**
 * Getl language script
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings(['GrMethodMayBeStatic', 'unused'])
class Getl extends Script {
    Getl() {
        super()
        initGetlParams()
    }

    Getl(Binding binding) {
        super(binding)
        initGetlParams()
    }

    static void main(String[] args) {
        Main(args.toList(), true)
    }

    static private void Help() {
        Version.instance.sayInfo(false)
        println """
# The syntax for running a specified Getl Dsl script (with @BaseScript directive):
* java getl.lang.Getl workflow=<workflow model name> [<base arguments>] [<workflow arguments>]
* java getl.lang.Getl runclass=<script class name> [<base arguments>] [<run class arguments>]

# The syntax for running a script that inherits from the Getl class is:
* java <main class name> [<arguments>] [<main class arguments>]
  
# List of possible base arguments:
* unittest=true|false
    set the flag that unit tests are launched
* environment=dev|test|prod 
    use the specified configuration environment (the default is "prod")
* vars.name=<value>
    set the value for the script field with the specified name, which is marked 
    with the "@Field" directive in the code

# List of possible workflow arguments:
* workflow=<model name>
    the name of the workflow model to run (required parameter)
* include_steps=<list of step names>
    run only the specified steps
* exclude_steps=<list of step names>
    do not run the specified steps

# List of possible run class arguments:
* initclass=<class name> 
    class name of the initialization script 
    that runs before the main script runs (required parameter)
* runclass=<class name>
    the name of the main class of the script
    
# List of possible main class arguments:
* config.path=<directory>
    path to load configuration files
* config.filename=config1.groovy;config2.groovy
    a semicolon separated list of configuration file names is allowed to indicate 
    the full path or relative to the one specified in "config.path"
      
Examples:
  java getl.lang.Getl workflow=test:workflow1
  java getl.lang.Getl runclass=com.comp.MainScript vars.message="Hello World!"
  java my.Class1 config.path=dir1 config.filename=config1.conf;config2.conf
"""
    }

    /**
     * Launch Getl Dsl script in module runtime
     * @param args
     */
    static void Module(List args) {
        Main(args, false)
    }

    /**
     * Run the current class inherited from the Getl as an application<br>
     * P.S. Locate the execution script in the "run" method
     * @param args
     */
    static void Application(Class scriptClass, def args) {
        if (!Getl.isAssignableFrom(scriptClass))
            throw new DslError('#dsl.invalid_class', [className: scriptClass.name])

        if (!(args instanceof List)) {
            if (args instanceof String[])
                args = args.toList() as List<String>
            else if (args instanceof String || args instanceof GString)
                args = [(args as Object).toString()] as List<String>
            else
                throw new DslError('#dsl.invalid_instance_args', [className: args.getClass().name])
        }

        def runclass = scriptClass.name
        Main(args + ["runclass=$runclass"])
    }

    /** Use this initialization class at application startup if it is not explicitly specified */
    protected Class<Script> useInitClass() { null }

    /**
     * Prepare script before execution from Groovy
     */
    protected void groovyStarter() {
        def cmdArgs = (binding.hasVariable('args'))?binding.getVariable('args') as String[]:([] as String[])
        def jobArgs = MapUtils.ProcessArguments(cmdArgs)

        def p = new ParamMethodValidator()
        p.register('main', ['config', 'environment', 'unittest', 'vars', 'getlprop'])
        p.register('main.config', ['path', 'filename'])
        p.validation('main', jobArgs)

        //def isTestMode = BoolUtils.IsValue(jobArgs.unittest)

        GetlSetInstance(this)
        setGetlSystemParameter('mainClass', getClass().name)
        setGetlSystemParameter('groovyConsole', true)

        if (jobArgs.environment == null) {
            def propPath = new File('.')
            def propFile = new File('getl-test-properties.conf')
            def path = FileUtils.ConvertToUnixPath(propPath.absolutePath)
            if (!propFile.exists() && (path.matches('.+/src/main/[.]') || path.matches('.+/src/test/[.]'))) {
                propPath = new File(StringUtils.LeftStr(path, path.length() - 11))
                propFile = new File(propPath.path + '/' + 'getl-test-properties.conf')
            }

            String env = 'dev'
            if (propFile.exists()) {
                def cp = ConfigSlurper.LoadConfigFile(file: propFile)
                if (cp.containsKey('defaultEnv'))
                    env = cp.defaultEnv as String
            }
            configuration.environment = env
        }

        if (configuration.environment != 'prod')
            enableUnitTestMode()

        _initGetlProperties(null, jobArgs.getlprop as Map<String, Object>, true)
        logInfo("### Start script ${getClass().name}")

        if (jobArgs.vars == null)
            jobArgs.vars = new HashMap()
        def vars = jobArgs.vars as Map
        configuration.manager.init(jobArgs)
        configuration.manager.setVars(vars)

        if (vars != null && !vars.isEmpty())
            _fillFieldFromVars(this, vars, true, true)

        setGetlSystemParameter('runMode', 'shell')
    }

    /**
     * Launch Getl Dsl script<br><br>
     * <i>List of argument (use format name=value):</i><br>
     * <ul>
     * <li>initclass: class name of the initialization script that runs before the main script runs</li>
     * <li>runclass: the name of the main class of the script</li>
     * <li>workflow: the name of the workflow model to run</li>
     * <li>include_steps: run only the specified steps</li>
     * <li>exclude_steps: do not run the specified steps</li>
     * <li>unittest: set the flag that unit tests are launched</li>
     * <li>environment: use the specified configuration environment (the default is "prod")</li>
     * <li>config.path: path to load configuration files</li>
     * <li>config.filename: a comma separated list of configuration file names is allowed to indicate the full path or relative to the one specified in "config.path"</li>
     * <li>vars.name: set the value for the script field with the specified name, which is marked with the "@Field" directive in the code</li>
     * </ul>
     * @param args startup arguments
     * @param isApp run as application or module
     */
    static void Main(List args, Boolean isApp = true, Class<Getl> launcher = null) {
        if (isApp) {
            def a = ((args?:[]) as List<String>)*.trim()
            a = a*.toLowerCase()
            if (a.isEmpty() || ('-help' in a) || ('-h') in a || ('-?') in a) {
                Help()
                return
            }
        }

        CleanGetl()

        def job = new Job() {
            static ParamMethodValidator allowArgs = {
                def p = new ParamMethodValidator()
                //noinspection SpellCheckingInspection
                p.register('main', ['config', 'environment', 'initclass', 'runclass',
                                    'workflow', 'workflowfile', 'include_steps', 'exclude_steps',
                                    'unittest', 'vars', 'getlprop', 'loadproperties'])
                p.register('main.config', ['path', 'filename'])
                return p
            }.call()

            public Boolean isMain
            private String className
            private Class runClass
            private String workflowName
            private String workflowFileName
            private List<String> workflow_include_steps
            private List<String> workflow_exclude_steps
            private Boolean loadProperties
            private Getl eng

            @Override
            protected void doRun() {
                super.doRun()
                StackTraceElement[] stack = Thread.currentThread().getStackTrace()
                def obj = stack[stack.length - 1]
                if (obj.getClassName() == 'getl.lang.Getl' && this.isMain) {
                    dslCreator.logFiner("System exit ${exitCode?:0}")
                    System.exit(exitCode?:0)
                }
            }

            @Override
            void init() {
                super.init()
                allowArgs.validation('main', jobArgs)
                className = jobArgs.runclass as String

                workflowName = jobArgs.workflow as String
                workflowFileName = jobArgs.workflowfile as String
                if (jobArgs.include_steps != null) {
                    if (workflowName == null)
                        throw new DslError('#dsl.invalid_job_start_parameter', [param: 'include_steps'])
                    workflow_include_steps = ConvertUtils.String2List(jobArgs.include_steps as String)
                }
                if (jobArgs.exclude_steps != null) {
                    if (workflowName == null)
                        throw new DslError('#dsl.invalid_job_start_parameter', [parameter: 'exclude_steps'])
                    workflow_exclude_steps = ConvertUtils.String2List(jobArgs.exclude_steps as String)
                }

                loadProperties = BoolUtils.IsValue(jobArgs.loadproperties, true)

                if (className == null && workflowName == null && workflowFileName == null)
                    throw new DslError('#dsl.non_main_operator')

                if (className != null && (workflowName != null || workflowFileName != null))
                    throw new DslError('#dsl.invalid_main_operator')

                if (className != null) {
                    try {
                        runClass = Class.forName(className)
                    }
                    catch (Throwable e) {
                        Logs.Severe("Class \"$className\" not found", e)
                        throw e
                    }

                    if (!Getl.isAssignableFrom(runClass))
                        throw new DslError('#dsl.invalid_class')
                }
                else {
                    runClass = launcher?:Getl
                }

                eng = runClass.getConstructor().newInstance() as Getl
                eng.configuration.manager.init(jobArgs)
                if (jobArgs.vars != null)
                    eng.configVars.putAll(jobArgs.vars as Map)
                dslCreator = eng
            }

            @Override
            void process() {
                def isTestMode = BoolUtils.IsValue(jobArgs.unittest)

                GetlSetInstance(eng)
                eng.setGetlSystemParameter('mainClass', runClass.name)
                eng.setUnitTestMode(isTestMode)

                def initClasses = [] as List<Class<Script>>

                if (eng.useInitClass() != null)
                    initClasses << eng.useInitClass()

                def initClassName = (jobArgs.initclass as String)
                if (initClassName != null) {
                    eng.setGetlSystemParameter('initClass', initClassName)

                    try {
                        def initClass = Class.forName(initClassName)
                        if (!Script.isAssignableFrom(initClass))
                            throw new DslError('#dsl.invalid_script', [className: initClassName])
                        initClasses << (initClass as Class<Script>)
                    }
                    catch (Throwable e) {
                        Logs.Severe("Class \"$initClassName\" not found", e)
                        throw e
                    }
                }

                eng.setGetlSystemParameter('runMode', 'init')
                if (className == null)
                    eng.setGetlSystemParameter('workflow', workflowName?:FileUtils.FilenameWithoutExtension(FileUtils.FileName(workflowFileName)))
                eng._initGetlProperties(initClasses, jobArgs.getlprop as Map<String, Object>, false, loadProperties)
                if (className != null)
                    eng.logInfo("### Start script ${eng.getClass().name}")
                else if (workflowFileName != null)
                    eng.logInfo("### Start workflow ${(workflowName != null)?"\"$workflowName\" ":''}from file \"$workflowFileName\"")

                else
                    eng.logInfo("### Start workflow \"$workflowName\"")

                def isJobError = false
                try {
                    if (className != null) {
                        eng.setGetlSystemParameter('runMode', 'class')
                        eng.runGroovyInstance(eng, true, eng.configuration.manager.vars)
                    }
                    else if (workflowFileName != null) {
                        eng.setGetlSystemParameter('runMode', 'workflow')
                        def workflow = eng.models.workflow((workflowName != null)?
                                "##${workflowName.replace(':', '_')}##":'##workflow##', true)
                        eng.repositoryStorageManager {
                            readObjectFromFile(repository(RepositoryWorkflows), workflowFileName, null, workflow)
                        }
                        workflow.execute(eng.configuration.manager.vars, workflow_include_steps, workflow_exclude_steps)
                    }
                    else {
                        eng.setGetlSystemParameter('runMode', 'workflow')
                        eng.models.workflow(workflowName).execute(eng.configuration.manager.vars, workflow_include_steps, workflow_exclude_steps)
                    }
                }
                catch (AbortDsl e) {
                    if (e.typeCode == AbortDsl.STOP_APP) {
                        if (e.message != null)
                            eng.logWarn(e.message)
                        if (e.exitCode != null)
                            exitCode = e.exitCode
                    }
                    else {
                        isJobError = true
                        throw e
                    }
                }
                catch (Throwable e) {
                    isJobError = true
                    throw e
                }
                finally {
                    eng.setGetlSystemParameter('runMode', 'stop')
                    eng.repositoryStorageManager.clearRepositories()

                    if (className != null) {
                        if (!isJobError)
                            eng.logInfo("### Job ${eng.getClass().name} completed successfully.")
                        else
                            eng.logError("### Job ${eng.getClass().name} completed with errors!")
                    }
                    else if (workflowName != null) {
                        if (!isJobError)
                            eng.logInfo("### Workflow $workflowName completed successfully.")
                        else
                            eng.logError("### Workflow $workflowName completed with errors!")
                    }
                    else {
                        if (!isJobError)
                            eng.logInfo("### Workflow file \"$workflowFileName\" completed successfully.")
                        else
                            eng.logError("### Workflow file \"$workflowFileName\" completed with errors!")
                    }
                }
            }
        }
        job.isMain = isApp
        job.run(args)
    }

    /**
     * Initialize getl instance properties before starting it
     * @param listInitClass list of initialization classes that should be executed before starting
     */
    protected void _prepareGetlProperties(List<Class<Script>> initClasses, Map<String, Object> extProp,
                                          Boolean loadProperties, String unitClassName = null) {
        def instance = this
        if (extProp == null)
            extProp = new HashMap<String, Object>()

        File configFile
        String configFilePath

        options {
            if (autoInitFromConfig) {
                if (loadProperties) {
                    configFile = loadProjectProperties(instance.configuration.environment, extProp.filename as String)
                    if (configFile != null)
                        configFilePath = FileUtils.PathFromFile(configFile.canonicalPath)
                }
                if (!extProp?.isEmpty())
                    MapUtils.MergeMap(options.getlConfigProperties,
                            MapUtils.CleanMap(extProp, ['filepath']) as Map<String, Object>, true, false)

                logFinest("Processing project configuration for \"${(configuration.environment)?:'prod'}\" environment ...")
                if (this.unitTestMode)
                    logWarn('Used to work in unit testing mode!')

                def procs = new HashMap<String, Closure>()
                procs.logging = { Map<String, Object> en ->
                    if (en.logConsoleLevel != null) {
                        logging.logConsoleLevel = Logs.ObjectToLevel(en.logConsoleLevel)
                        logFine("Logging to console is done starting from level ${logging.logConsoleLevel}")
                    }

                    if (en.logFileLevel != null) {
                        logging.logFileLevel = Logs.ObjectToLevel(en.logFileLevel)
                        logFine("Logging to file is done starting from level ${logging.logFileLevel}")
                    }

                    if (en.logFileName != null) {
                        String procName
                        String env = instance.configuration.environment
                        if (unitClassName != null) {
                            procName = "unittest_$unitClassName"
                            if (env == null)
                                env = 'dev'
                        }
                        else if (getGetlSystemParameter('workflow') != null) {
                            def wf = (getGetlSystemParameter('workflow') as String).replace(':', '-')
                            procName = "workflow_$wf"
                        }
                        else
                            procName = "script_${instance.getClass().name}"

                        logging.logFileName = StringUtils.EvalMacroString(en.logFileName as String,
                                [env: env?:'prod', process: procName], false)
                        logFine("Logging the process \"${instance.getClass().name}\" to file " +
                                "\"${FileUtils.TransformFilePath(logging.logFileName, false, this)}\"")
                    }

                    if (en.printStackTraceError != null)
                        logging.logPrintStackTraceError = BoolUtils.IsValue(en.printStackTraceError)

                    if (en.printConfigMessage != null)
                        logging.logPrintConfigMessage = BoolUtils.IsValue(en.printConfigMessage)

                    if (en.printErrorToConsole != null)
                        logging.printErrorToConsole = BoolUtils.IsValue(en.printErrorToConsole)

                    if (en.jdbcLogPath != null) {
                        jdbcConnectionLoggingPath = StringUtils.EvalMacroString(en.jdbcLogPath as String,
                                [env: instance.configuration.environment?:'prod', process: instance.getClass().name], false)
                        logFine("Logging jdbc connections to path \"${FileUtils.TransformFilePath(jdbcConnectionLoggingPath, false, this)}\"")
                    }

                    if (en.filesLogPath != null) {
                        fileManagerLoggingPath = StringUtils.EvalMacroString(en.filesLogPath as String,
                                [env: instance.configuration.environment?:'prod', process: instance.getClass().name], false)
                        logFine("Logging file managers to path \"${FileUtils.TransformFilePath(fileManagerLoggingPath, false, this)}\"")
                    }

                    if (en.tempDBLogFileName != null) {
                        tempDBSQLHistoryFile = StringUtils.EvalMacroString(en.tempDBLogFileName as String,
                                [env: instance.configuration.environment?:'prod', process: instance.getClass().name], false)
                        logFine("Logging of ebmedded database SQL commands to a file \"${FileUtils.TransformFilePath(tempDBSQLHistoryFile, false, this)}\"")
                    }

                    if (en.sqlEchoLevel != null) {
                        logging.sqlEchoLogLevel = Logs.ObjectToLevel(en.sqlEchoLevel)
                        if (logging.sqlEchoLogLevel)
                            logFine("SQL command echo is logged with level ${logging.sqlEchoLogLevel}")
                    }
                }
                procs.repository = { Map<String, Object> en ->
                    instance.repositoryStorageManager {
                        if (en.encryptKey != null) {
                            def password = en.encryptKey as String
                            if (password.length() > 2 && password[0] == '#' && password[password.length() - 1] == '#') {
                                password = StringUtils.Decrypt(password.substring(1, password.length() - 1),
                                        new String(RepositoryObjects._storage_key))
                            }

                            storagePassword = password
                            logFine('Repository encryption mode: enabled')
                        }

                        if (en.autoLoadFromStorage != null)
                            autoLoadFromStorage = BoolUtils.IsValue(en.autoLoadFromStorage)
                        else
                            autoLoadFromStorage = (storagePath != null)

                        if (en.autoLoadForList != null)
                            autoLoadForList = BoolUtils.IsValue(en.autoLoadForList)

                        if (en.savingStoryDataset != null) {
                            def storyDatasetFilePath = new File(FileUtils.TransformFilePath(en.savingStoryDataset as String, this)).canonicalPath
                            def storyDatasetFile = new File(storyDatasetFilePath)
                            if (storyDatasetFile.parentFile == null || !storyDatasetFile.parentFile.exists())
                                throw new DslError(this, '#dsl.invalid_story_path', [path: storyDatasetFile.parent])
                            def csvCon = new CSVConnection(dslCreator: this, path: storyDatasetFile.parent)
                            def csvDataset = new CSVDataset(dslCreator: this, connection: csvCon, fileName: storyDatasetFile.name, header: true,
                                    fieldDelimiter: ',', codePage: 'utf-8', escaped: false)
                            savingStoryDataset = csvDataset
                            logFine("The history of saving repository objects is written to file ${savingStoryDataset.fullFileName()}")
                        }

                        if (en.path != null) {
                            def sp = en.path as String
                            if (sp == '.')
                                it.storagePath = configFilePath
                            else //noinspection RegExpSimplifiable
                            if (sp.matches('[.][/].+'))
                                it.storagePath = configFilePath + sp.substring(1)
                            else
                                it.storagePath = sp

                            autoLoadFromStorage = true
                            logFine("Path to repository objects: ${storagePath()}")
                        }

                        if (en.libs != null) {
                            librariesDirName = en.libs as String
                            logFine("Using libraries from repository directory \"$librariesDirName\"")
                            buildLibrariesClassLoader()
                        }
                    }
                }
                procs.engine = { Map<String, Object> en ->
                    if (en.language != null)
                        language = en.language as String
                    else
                        _onChangeLanguage()

                    logFine("Using \"$language\" language in messages and errors")

                    def ic = en.initClass as String
                    if (ic != null) {
                        try {
                            def initClass = Class.forName(ic)
                            if (!Script.isAssignableFrom(initClass))
                                throw new DslError(this, '#dsl.invalid_script', [className: ic])
                            initClasses << (initClass as Class<Script>)
                            logFine("Initialization class \"$ic\" is used")
                        }
                        catch (Throwable e) {
                            logError("Init class \"$ic\" not found!", e)
                            throw e
                        }
                    }

                    if (en.useThreadModelCloning != null) {
                        useThreadModelCloning = BoolUtils.IsValue(en.useThreadModelCloning, true)
                        if (useThreadModelCloning)
                            logFine("Model of cloning objects in threads is used")
                    }

                    if (en.controlDataset != null) {
                        processControlDataset = instance.dataset(en.controlDataset as String)
                        logFine("Process start control uses dataset \"$processControlDataset\"")
                        if (en.controlStart != null) {
                            checkProcessOnStart = BoolUtils.IsValue(en.controlStart, true)
                            if (checkProcessOnStart)
                                logFine("Process startup is checked in the process checklist")
                        }
                        if (en.controlThreads != null) {
                            checkProcessForThreads = BoolUtils.IsValue(en.controlThreads)
                            if (checkProcessForThreads)
                                logFine("Running processes in threads is checked in the process checklist")
                        }
                        if (en.controlLogin != null)
                            processControlLogin = en.controlLogin as String
                    }
                }
                procs.profile = { Map<String, Object> en ->
                    if (en.enabled != null) {
                        processTimeTracing = BoolUtils.IsValue(en.enabled, false)
                        if (processTimeTracing)
                            logFine("Enabled output of profiling results to the log")

                        if (en.level != null) {
                            processTimeLevelLog = Logs.ObjectToLevel(en.level)
                            if (processTimeLevelLog && processTimeTracing)
                                logFine("Output profiling messages with level $processTimeLevelLog")
                        }

                        if (en.debug != null) {
                            processTimeDebug = BoolUtils.IsValue(en.debug)
                            if (processTimeDebug && processTimeTracing)
                                logFine('Profiling the start of process commands')
                        }
                    }
                }
                procs.sqlscripter = { Map<String, Object> en ->
                    if (en.debug != null) {
                        sqlScripterDebug = BoolUtils.IsValue(en.debug)
                        if (sqlScripterDebug)
                            logFine('Enabled logging SQL scripter commands')
                    }
                }
                procs.project = { Map<String, Object> en ->
                    def notFounds = [] as List<String>
                    (en.needEnvironments as List<String>)?.each {env ->
                        if (Config.SystemProps().get(env) == null)
                            notFounds << env
                    }
                    if (!notFounds.isEmpty())
                        throw new DslError(this, '#dsl.env_vars_not_found', [variables: notFounds.join(', ')])

                    if (en.configFileName != null) {
                        def configFileName = en.configFileName as String
                        def m = ConfigSlurper.LoadConfigFile(
                                file: new File(FileUtils.TransformFilePath(configFileName, this)),
                                codePage: 'utf-8', configVars: configVars, owner: this)
                        projectConfigParams.putAll(m)
                    }
                }

                if (getlConfigProperties.engine == null)
                    getlConfigProperties.engine = [:]

                MapUtils.ProcessSections(getlConfigProperties, procs, ['engine', 'logging', 'project', 'repository', 'profile'])
            }
        }
    }

    /** Language change handling event */
    void _onChangeLanguage() { }

    /** Log file name change handling event */
    void _onChangeLogFileName() { }

    /** Log file name change handling event */
    void _onChangeRepositoryPath() { }

    void _initGetlProperties(List<Class<Script>> listInitClass = null, Map<String, Object> extProp,
                             Boolean startAsGroovy = false, Boolean loadProperties = true, String unitClassName = null) {
        def initClasses = [] as List<Class<Script>>
        if (listInitClass != null)
            initClasses.addAll(listInitClass)

        _prepareGetlProperties(initClasses, extProp, loadProperties, unitClassName)

        if (!initClasses.isEmpty() && !startAsGroovy) {
            setGetlSystemParameter('isInitMode', true)
            try {
                initClasses.each { initClass ->
                    runGroovyClass(initClass, true)
                }
            }
            finally {
                setGetlSystemParameter('isInitMode', false)
            }
        }

        if (options.projectConfigParams.project != null) {
            logFine("### Start project \"${options.projectConfigParams.project}\", " +
                    "version ${options.projectConfigParams.version?:'1.0'}, " +
                    "created ${options.projectConfigParams.year?:DateUtils.PartOfDate('YEAR', new Date()).toString()} " +
                    "by \"${options.projectConfigParams.company?:'My company'}\"")
        }

        InitServices.each { code ->
            code.call(this)
        }
    }

    /** List of initialization service */
    static private final InitServices = [] as List<Closure>
    /** Add service code on initialization Getl instance */
    static void InitServiceAdd(@ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        if (cl == null)
            throw new DslError('#params.required', [param: 'closure code', detail: 'InitServiceAdd'])
        if (_getl == null || _getl.getGetlSystemParameter('runMode') == 'init') {
            if (InitServices.indexOf(cl) != -1)
                throw new DslError('#dsl.duplicate_service')
            InitServices.add(cl)
        }
        else if (_getl != null)
            cl.call(_getl)
    }

    /** Quit DSL Application */
    void appRunSTOP(String message = null, Integer exitCode = null) {
        if (message != null)
            throw new AbortDsl(AbortDsl.STOP_APP, exitCode?:0, message)
        else
            throw new AbortDsl(AbortDsl.STOP_APP, exitCode?:0)
    }

    /** Quit DSL Application */
    void appRunSTOP(Integer exitCode) {
        throw new AbortDsl(AbortDsl.STOP_APP, exitCode?:0)
    }

    /** Stop code execution of the current class */
    void classRunSTOP(String message = null, Integer exitCode = null) {
        if (message != null)
            throw new AbortDsl(AbortDsl.STOP_CLASS, exitCode?:0, message)
        else
            throw new AbortDsl(AbortDsl.STOP_CLASS, exitCode?:0)
    }

    /** Stop code execution of the current class */
    void classRunSTOP(Integer exitCode) {
        throw new AbortDsl(AbortDsl.STOP_CLASS, exitCode?:0)
    }

    /** Abort execution with the specified error */
    void abortWithError(String message) {
        throw new AbortDsl(message)
    }

    /** The name of the main class of the process */
    String getGetlMainClassName() { getGetlSystemParameter('mainClass') as String }

    /** The name of the process initializing class */
    String getGetlInitClassName() { getGetlSystemParameter('initClass') as String }

    /** Main Getl instance */
    Getl getGetlMainInstance() { getGetlSystemParameter('mainInstance') as Getl }

    /** Checking process permission */
    @Synchronized
    Boolean allowProcess(String processName, Boolean throwError = false) {
        def res = true

        if (_langOpts.processControlDataset != null) {
            if (processName == null)
                processName = getGetlMainClassName()?:this.getClass().name
            if (processName == null)
                throw new DslError(this, '#params.required', [param: 'processName', detail: 'allowProcess'])

            if (_langOpts.processControlDataset instanceof TableDataset) {
                def table = (_langOpts.processControlDataset as TableDataset).cloneDatasetConnection() as TableDataset
                try {
                    if (_langOpts.processControlLogin != null)
                        table.currentJDBCConnection.useLogin(_langOpts.processControlLogin)

                    def row = sqlQueryRow(table.currentJDBCConnection,
                            "SELECT enabled FROM ${table.fullNameDataset()} WHERE name = '$processName'")
                    if (row != null && !row.isEmpty())
                        res = BoolUtils.IsValue(row.enabled)
                }
                finally {
                    table.currentJDBCConnection.connected = false
                }
            } else {
                def ds = _langOpts.processControlDataset
                Map row = null
                ds.eachRow {
                    if (it.name == processName) {
                        row = it
                        //noinspection UnnecessaryQualifiedReference
                        directive = Closure.DONE
                    }
                }
                if (row != null)
                    res = BoolUtils.IsValue(row.enabled)
            }

            if (!res) {
                if (!throwError)
                    Logs.Warning(this, '#dsl.need_abort_work', [process: processName])
                else
                    throw new DslError(this, '#dsl.need_abort_work', [process: processName])
            }
        }

        return res
    }

    /** Checking process permission */
    Boolean allowProcess(Boolean throwError = false) {
        allowProcess(null, throwError)
    }

    /** Init Getl instance */
    protected void initGetlParams() {
        _langOpts = new LangSpec(this)
        _configOpts = new ConfigSpec(this)
        _logOpts = new LogSpec(this)
        _repositoryFilter = new RepositoryFilter(this)
        _repositoryStorageManager = new RepositoryStorageManager(this)
        _etl = new EtlSpec(this)
        _models = new ModelSpec(this)
        _fileman = new FilemanSpec(this)

        _params.executedClasses = new SynchronizeObject()
        _params.langOpts = _langOpts
        _params.configOpts = _configOpts
        _params.logOpts = _logOpts
        _params.repositoryFilter = _repositoryFilter
        _params.repositoryStorageManager = _repositoryStorageManager
        _params.etl = _etl
        _params.models = _models
        _params.fileman = _fileman

        Version.instance.sayInfo(true, this)

        if (!IsCurrentProcessInThread() && getGetlSystemParameter('mainClass') == null &&
                (MainClassName() in ['org.codehaus.groovy.tools.GroovyStarter', 'com.intellij.rt.execution.CommandLineWrapper'/*, 'java.lang.Thread'*/] ||
                        LaunchedFromGroovyConsole))
            groovyStarter()
    }

    /** Indicates that the script is being run from the Groovy launcher */
    static public Boolean LaunchedFromGroovyConsole = false

    static String MainClassName() {
        def trace = Thread.currentThread().getStackTrace()
        if (trace.length > 0) {
            return trace[trace.length - 1].getClassName()
        }
        return "Unknown"
    }

    @Override
    Object run() {
        return this
    }

    /** Instance of GETL DSL */
    static private Getl _getl

    /** The object is a static instance */
    private Boolean _getlInstance = false

    /** Set current Getl instance */
    protected _setGetlInstance() {
        if (_getl == this)
            return

        if (_getl != null)
            _getl._getlInstance = false

        _getl = this
        _getl._getlInstance = true
        if (getlMainInstance != null) {
            Config.configClassManager = getlMainInstance.configuration.manager
            if (Logs.global == null)
                Logs.global = getlMainInstance.logging.manager
        }
    }

    /** Get current Getl instance */
    static Getl GetlInstance() { return _getl }

    /** Set current Getl instance */
    static void GetlSetInstance(Getl instance) {
        if (instance == null)
            throw new DslError('#params.required', [param: 'instance', detail: 'GetlSetInstance'])

        instance.setGetlSystemParameter('mainInstance', instance)
        instance._setGetlInstance()
    }

    /** check that Getl instance is created */
    static Boolean GetlInstanceCreated() { _getl != null }

    /* Owner object for instance DSL */
    private Object _ownerObject
    /* Owner object for instance DSL */
    Object getGetlOwnerObject() { _ownerObject }

    /** Run DSL script on getl share object */
    static Object Dsl(def ownerObject, Map parameters,
                    @DelegatesTo(Getl)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        if (_getl != null) {
            if (!_getl._getlInstance)
                throw new DslError(_getl, '#dsl.deny_dsl_when_init')
        }
        else {
            GetlSetInstance(new Getl())
        }

        _getl.runDsl(ownerObject, cl)
    }

    /** Run DSL script on getl share object */
    static Object Dsl(def thisObject,
                    @DelegatesTo(Getl)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        Dsl(thisObject, null, cl)
    }

    /** Run DSL script on getl share object */
    static Object Dsl(@DelegatesTo(Getl)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        Dsl(null, null, cl)
    }

    /** Run DSL script on getl share object */
    static Object Dsl() {
        Dsl(null, null, null)
    }

    /** Clean Getl instance */
    static void CleanGetl(Boolean softClean = false) {
        if (_getl != null)
            _getl.repositoryStorageManager.clearRepositories()

        if (softClean && _getl != null) {
            _getl = _getl.getClass().getConstructor().newInstance()
            _getl._getlInstance = true
            _getl.setGetlSystemParameter('mainInstance', _getl)
        }
        else {
            _getl = null
        }
    }

    /** Unit test mode */
    Boolean getUnitTestMode() { BoolUtils.IsValue(getGetlSystemParameter('unitTestMode')) }
    /** Unit test mode */
    @Synchronized
    void setUnitTestMode(Boolean value) {
        if (value == true && configuration.environment == 'prod')
            throw new DslError(this, '#dsl.deny_unittest_on_prod!')
        setGetlSystemParameter('unitTestMode', value)
    }
    /** Enable unit test mode */
    void enableUnitTestMode() { setUnitTestMode(true) }
    /** Disable unit test mode */
    void disableUnitTestMode() { setUnitTestMode(false) }

    /** Run DSL script */
    Object runDsl(def ownerObject,
                  @DelegatesTo(Getl)
                  @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        Object res = null

        def oldOwnerObject = _ownerObject
        try {
            if (ownerObject != null) {
                _ownerObject = ownerObject
                /*if (ownerObject instanceof GroovyTestCase || ownerObject instanceof GroovyAssert)
                    enableUnitTestMode()*/
            }

            if (cl != null) {
                res = this.with(cl)
            }
        }
        finally {
            _ownerObject = oldOwnerObject
        }

        return res
    }

    /** Run DSL script */
    Object runDsl(@DelegatesTo(Getl)
                @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        runDsl(null, cl)
    }

    /** Engine parameters */
    protected final Map<String, Object> _params = new ConcurrentHashMap<String, Object>()
    /** Get engine parameter */
    Object getGetlSystemParameter(String key) { _params.get(key) }
    /** Set engine parameter */
    protected setGetlSystemParameter(String key, Object value) {
        _params.put(key, value)
    }

    /** Managing repository object storage instance */
    protected RepositoryStorageManager _repositoryStorageManager
    /** Managing repository object storage */
    RepositoryStorageManager getRepositoryStorageManager() { _repositoryStorageManager }

    /** Managing repository object storage */
    RepositoryStorageManager repositoryStorageManager(@DelegatesTo(RepositoryStorageManager)
                                               @ClosureParams(value = SimpleType, options = ['getl.lang.sub.RepositoryStorageManager'])
                                                       Closure cl) {
        runClosure(_repositoryStorageManager, cl)
        return _repositoryStorageManager
    }

    /** Running init script */
    Boolean getIsInitMode() { BoolUtils.IsValue(getGetlSystemParameter('isInitMode')) }

    /** Set language parameters */
    protected void importGetlParams(Map importParams) {
        _params.putAll(MapUtils.Copy(importParams, ['langOpts', 'configOpts', 'logOpts']))

        def isThread = IsCurrentProcessInThread(true)

        _langOpts.importParams((importParams.langOpts as LangSpec).params, !isThread)
        _configOpts.importParams((importParams.configOpts as ConfigSpec).params, true)
        _logOpts.importParams((importParams.logOpts as LogSpec).params, !isThread)

        //_repositoryFilter = _params.repositoryFilter as RepositoryFilter
        //_repositoryFilter.importParams((_params.repositoryFilter as RepositoryFilter).params, false)
        _repositoryStorageManager = _params.repositoryStorageManager as RepositoryStorageManager
        _etl = _params.etl as EtlSpec
        _models = _params.models as ModelSpec
        _fileman = _params.fileman as FilemanSpec
    }

    /** Fix start process */
    ProcessTime startProcess(String name, String objName = null) {
        new ProcessTime(dslCreator: this, name: name, objectName: objName,
                logLevel: (_langOpts.processTimeTracing) ? _langOpts.processTimeLevelLog : Level.OFF,
                debug: _langOpts.processTimeDebug)
    }

    /** Fix finish process */
    void finishProcess(ProcessTime pt, Long countRow = null) {
        if (pt != null) pt.finish(countRow)
    }

    /** GETL DSL options */
    ProfileSpec profile(String name, String objName = null,
                 @DelegatesTo(ProfileSpec)
                 @ClosureParams(value = SimpleType, options = ['getl.lang.opts.ProfileSpec']) Closure cl) {
        def parent = new ProfileSpec(this, name, objName, true)
        parent.startProfile()
        runClosure(parent, cl)
        parent.finishProfile()

        return parent
    }

    /** list of executed script classes and call parameters */
    protected SynchronizeObject getExecutedClasses() { getGetlSystemParameter('executedClasses') as SynchronizeObject }

    /** Repository object filtering manager */
    private RepositoryFilter _repositoryFilter
    /** Repository object filtering manager */
    RepositoryFilter getRepositoryFilter() { _repositoryFilter }
    /** Specified filter when searching for objects */
    String getFilteringGroup() { _repositoryFilter.filteringGroup }

    /** Use the specified filter by group name when searching for objects */
    @Synchronized('_repositoryFilter')
    void forGroup(String group) {
        if (group == null || group.trim().length() == 0)
            throw new DslError(this, '#params.required', [param: group, detail: 'forGroup'])
        if (IsCurrentProcessInThread(true))
            throw new DslError(this, '#dsl.deny_filter_group_threads')

        _repositoryFilter.filteringGroup = group.trim().toLowerCase()
    }

    /** Reset filter to search for objects */
    @Synchronized('_repositoryFilter')
    void clearGroupFilter() {
        if (IsCurrentProcessInThread(true))
            throw new DslError(this, '#dsl.deny_filter_group_threads')

        _repositoryFilter.clearGroupFilter()
    }

    /** Repository object name */
    String repObjectName(String name, Boolean needObjectName = true) {
        def names = _repositoryFilter.parseName(name, false)
        if (needObjectName && names.objectName == null)
            throw new DslError(this, '#dsl.invalid_object_name_format', [repname: name])

        return names.name
    }

    /** Parsing the name of an object from the repository into a group and the name itself */
    ParseObjectName parseName(String name, Boolean isMaskName = false) {
        _repositoryFilter.parseName(name, isMaskName)
    }

    /** Replace the group name with the one specified for the object name */
    String replaceGroupName(String name, String newGroupName) {
        def objName = parseName(name)
        objName.groupName = newGroupName
        return objName.name
    }

    /**
     * Read text from specified file
     * @param fileName file path
     * @param codePage encoding text (default UTF-8)
     * @return text from file
     */
    String textFromFile(String fileName, String codePage = 'UTF-8') {
        def path = FileUtils.TransformFilePath(fileName, this)
        def file = new File(path)
        if (!file.exists() || !file.isFile())
            throw new DslError(this, "#io.file.not_found", [path: fileName, type: 'Text'])

        return file.getText(codePage ?: 'UTF-8')
    }

    /** Generate unique name for temporary repository object */
    static String GenerateTempName() { '#' + StringUtils.RandomStr() }

    /**
     * Return list of repository connections for specified mask, classes and filter
     * @param mask filter mask (use Path expression syntax)
     * @param connectionClasses connection class list
     * @param filter object filtering code
     * @return list of connection names according to specified conditions
     */
    List<String> listConnections(String mask = null, List connectionClasses = null,
                                 @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Connection'])
                                         Closure<Boolean> filter = null) {
        _repositoryStorageManager.repository(RepositoryConnections).list(mask, connectionClasses, true, filter)
    }

    /**
     * Return list of repository connection objects for specified list of name
     * @param names list of name connections
     * @return list of connection objects
     */
    List<Connection> connectionObjects(List<String> names) {
        if (names == null) return null
        def res = [] as List<Connection>
        names.each { connectionName ->
            res << connection(connectionName)
        }
        return res
    }

    /**
     * Return list of repository connections for specified classes
     * @param connectionClasses connection class list
     * @param filter object filtering code
     * @return list of connection names according to specified conditions
     */
    List<String> listConnections(List connectionClasses,
                                 @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Connection'])
                                         Closure<Boolean> filter = null) {
        listConnections(null, connectionClasses, filter)
    }

    /**
     * Return list of repository connections for specified filter
     * @param filter object filtering code
     * @return list of connection names according to specified conditions
     */
    List<String> listConnections(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Connection'])
                                         Closure<Boolean> filter) {
        listConnections(null, null, filter)
    }

    /**
     * Process repository connections for specified mask and class
     * @param mask filter mask (use Path expression syntax)
     * @param connectionClasses connection class list
     * @param cl processing code
     */
    void processConnections(String mask, List connectionClasses,
                            @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        _repositoryStorageManager.repository(RepositoryConnections).processObjects(mask, connectionClasses, cl)
    }

    /**
     * Process repository connections for specified mask
     * @param mask filter mask (use Path expression syntax)
     * @param cl processing code
     */
    void processConnections(String mask,
                            @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processConnections(mask, null, cl)
    }

    /**
     * Process repository connections for specified class
     * @param connectionClasses connection class list
     * @param cl processing code
     */
    void processConnections(List connectionClasses,
                            @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processConnections(null, connectionClasses, cl)
    }

    /**
     * Process all repository connections
     * @param cl processing code
     */
    void processConnections(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processConnections(null, null, cl)
    }

    /**
     * Return list of repository jdbc connections for specified mask and filter
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of jdbc connection names according to specified conditions
     */
    List<String> listJdbcConnections(String mask,
                                     @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.JDBCConnection'])
                                             Closure<Boolean> filter = null) {
        listConnections(mask, (_repositoryStorageManager.repository(RepositoryConnections) as RepositoryConnections).listJdbcClasses, filter)
    }

    /**
     * Return list of repository jdbc connections for specified mask and filter
     * @param filter object filtering code
     * @return list of jdbc connection names according to specified conditions
     */
    List<String> listJdbcConnections(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.JDBCConnection'])
                                             Closure<Boolean> filter = null) {
        listConnections(null, (_repositoryStorageManager.repository(RepositoryConnections) as RepositoryConnections).listJdbcClasses, filter)
    }

    /**
     * Return list of repository jdbc connection objects for specified list of name
     * @param names list of name jdbc connections
     * @return list of connection objects
     */
    List<JDBCConnection> jdbcConnectionObjects(List<String> names) {
        if (names == null) return null
        def res = [] as List<JDBCConnection>
        names.each { connectionName ->
            res << jdbcConnection(connectionName)
        }
        return res
    }

    /**
     * Process repository jdbc connections for specified mask
     * @param mask filter mask (use Path expression syntax)
     * @param cl processing code
     */
    void processJdbcConnections(String mask,
                                @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processConnections(mask, (_repositoryStorageManager.repository(RepositoryConnections) as RepositoryConnections).listJdbcClasses, cl)
    }

    /**
     * Process repository all jdbc connections
     * @param cl processing code
     */
    void processJdbcConnections(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processJdbcConnections(null, cl)
    }

    /**
     * Search for an object in the repository
     * @param obj connection
     * @return name of the object in the repository or null if not found
     */
    String findConnection(Connection obj) {
        _repositoryStorageManager.repository(RepositoryConnections).find(obj)
    }

    /**
     * Find a connection by name
     * @param name connection name
     * @return found connection object or null if not found
     */
    Connection findConnection(String name) {
        (_repositoryStorageManager.repository(RepositoryConnections) as RepositoryConnections).find(name)
    }

    /** Register connection in repository */
    Connection registerConnection(String connectionClassName, String name, Boolean registration = false, Boolean cloneInThread = true) {
        (_repositoryStorageManager.repository(RepositoryConnections) as RepositoryConnections).register(this, connectionClassName, name, registration, cloneInThread)
    }

    /**
     * Register connection object in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    Connection registerConnectionObject(Connection obj, String name = null, Boolean validExist = true,
                                        Boolean encryptPasswords = true) {
        (_repositoryStorageManager.repository(RepositoryConnections) as RepositoryConnections).registerObject(this, obj,
                name, validExist, encryptPasswords)
    }

    /**
     * Unregister connections by a given mask or a list of their classes
     * @param mask mask of objects (in Path format)
     * @param connectionClasses list of processed connection classes
     * @param filter filter for detect connections to unregister
     */
    void unregisterConnection(String mask = null, List connectionClasses = null,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Connection'])
                                      Closure<Boolean> filter = null) {
        _repositoryStorageManager.repository(RepositoryConnections).unregister(mask, connectionClasses, filter)
    }

    /**
     * Register connections from storage configuration files to repository
     * @param mask connection name mask
     * @param env environment
     * @param ignoreExists don't load existing ones (default true)
     * @return number of registered connections
     */
    Integer registerConnectionsFromStorage(String mask, String env, Boolean ignoreExists = true) {
        return repositoryStorageManager.loadRepository(RepositoryConnections, mask, env, ignoreExists)
    }

    /**
     * Register connections from storage configuration files to repository
     * @param mask connection name mask
     * @param ignoreExists don't load existing ones (default true)
     * @return number of registered connections
     */
    Integer registerConnectionsFromStorage(String mask = null, Boolean ignoreExists = true) {
        return repositoryStorageManager.loadRepository(RepositoryConnections, mask, null, ignoreExists)
    }

    /**
     * Register connections from storage configuration files to repository
     * @param ignoreExists don't load existing ones
     * @return number of registered connections
     */
    Integer registerConnectionsFromStorage(Boolean ignoreExists) {
        return repositoryStorageManager.loadRepository(RepositoryConnections, null, null, ignoreExists)
    }

    /**
     * Return list of repository name datasets for specified mask, class and filter
     * @param mask filter mask (use Path expression syntax)
     * @param datasetClasses dataset class list
     * @param filter object filtering code
     * @return list of dataset names according to specified conditions
     */
    List<String> listDatasets(String mask = null, List datasetClasses = null,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Dataset'])
                                      Closure<Boolean> filter = null) {
        _repositoryStorageManager.repository(RepositoryDatasets).list(mask, datasetClasses, true, filter)
    }

    /**
     * Return list of repository name datasets for specified class and filter
     * @param datasetClasses dataset class list
     * @param filter object filtering code
     * @return list of dataset names according to specified conditions
     */
    List<String> listDatasets(List datasetClasses,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Dataset'])
                                      Closure<Boolean> filter = null) {
        listDatasets(null, datasetClasses, filter)
    }

    /**
     * Return list of repository name datasets for specified filter
     * @param filter object filtering code
     * @return list of dataset names according to specified conditions
     */
    List<String> listDatasets(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Dataset'])
                                      Closure<Boolean> filter) {
        listDatasets(null, null, filter)
    }

    /**
     * Return list of repository dataset objects for specified list of name
     * @param names list of name datasets
     * @return list of dataset objects
     */
    List<Dataset> datasetObjects(List<String> names) {
        if (names == null) return null
        def res = [] as List<Dataset>
        names.each { tableName ->
            res << this.dataset(tableName)
        }
        return res
    }

    /**
     * Process repository datasets for specified mask and class
     * @param mask filter mask (use Path expression syntax)
     * @param datasetClasses dataset class list
     * @param cl processing code
     */
    void processDatasets(String mask, List datasetClasses,
                         @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        _repositoryStorageManager.repository(RepositoryDatasets).processObjects(mask, datasetClasses, cl)
    }

    /**
     * Process repository datasets for specified mask
     * @param mask filter mask (use Path expression syntax)
     * @param cl processing code
     */
    void processDatasets(String mask,
                         @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processDatasets(mask, null, cl)
    }

    /**
     * Process repository datasets for specified mask
     * @param datasetClasses dataset class list
     * @param cl processing code
     */
    void processDatasets(List datasetClasses,
                         @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processDatasets(null, datasetClasses, cl)
    }

    /**
     * Process all repository datasets
     * @param cl processing code
     */
    void processDatasets(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processDatasets(null, null, cl)
    }

    /**
     * Return a list of objects associated with the names of two groups
     * @param sourceList List of name source objects
     * @param destGroup List of name destination objects
     * @param filter filtering objects
     * @return list of names of related objects
     */
    List<ExecutorListElement> linkDatasets(List sourceList, List destList,
                                           @ClosureParams(value = SimpleType, options = ['java.lang.String'])
                                                   Closure<Boolean> filter = null) {
        (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).linkDatasets(sourceList, destList, filter)
    }

    /**
     * Return a list of objects associated with the names of two groups
     * @param sourceGroup name of the group of source objects
     * @param destGroup name of the group of destination objects
     * @param filter filtering objects
     * @return list of names of related objects
     */
    List<ExecutorListElement> linkDatasets(String sourceGroup, String destGroup,
                                           @ClosureParams(value = SimpleType, options = ['java.lang.String'])
                                                   Closure<Boolean> filter = null) {
        if (sourceGroup == null)
            throw new DslError(this, '#params.required', [param: 'sourceGroup', detail: 'linkDatasets'])
        if (destGroup == null)
            throw new DslError(this, '#params.required', [param: 'destGroup', detail: 'linkDatasets'])

        linkDatasets(listDatasets(sourceGroup + ':*'), listDatasets(destGroup + ':*'), filter)
    }

    /**
     * Return list of repository jdbc tables for specified mask and filter
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of jdbc table names according to specified conditions
     */
    List<String> listJdbcTables(String mask = null,
                                @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.TableDataset'])
                                        Closure<Boolean> filter = null) {
        listDatasets(mask, (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).listJdbcClasses, filter)
    }

    /**
     * Return list of repository jdbc tables for specified filter
     * @param filter object filtering code
     * @return list of jdbc table names according to specified conditions
     */
    List<String> listJdbcTables(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.TableDataset'])
                                        Closure<Boolean> filter) {
        listJdbcConnections(null, filter)
    }

    /**
     * Return list of repository table objects for specified list of name
     * @param names list of name tables
     * @return list of table objects
     */
    List<TableDataset> tableObjects(List<String> names) {
        if (names == null) return null
        def res = [] as List<TableDataset>
        names.each { tableName ->
            res << (this.dataset(tableName) as TableDataset)
        }
        return res
    }

    /**
     * Process repository tables for specified mask
     * @param mask filter mask (use Path expression syntax)
     * @param cl processing code
     */
    void processJdbcTables(String mask,
                           @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processDatasets(mask, (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).listJdbcClasses, cl)
    }

    /**
     * Process all repository tables
     * @param cl processing code
     */
    void processJdbcTables(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processJdbcTables(null, cl)
    }

    /** Set default connection for use in datasets */
    protected void setDefaultConnection(String datasetClassName, Dataset ds) {
        if (datasetClassName == null)
            throw new DslError(this, '#params.required', [param: 'datasetClassName', detail: 'setDefaultConnection'])
        if (!(datasetClassName in (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).listClasses))
            throw new DslError(this, '#dsl.invalid_dataset', [className: datasetClassName])

        if (ds instanceof JDBCDataset) {
            def con = defaultJdbcConnection(datasetClassName)
            if (con != null) ds.connection = con
        } else if (ds instanceof FileDataset) {
            def con = defaultFileConnection(datasetClassName)
            if (con != null) ds.connection = con
        } else {
            def con = defaultOtherConnection(datasetClassName)
            if (con != null) ds.connection = con
        }
    }

    /** Last used JDBC default connection */
    private JDBCConnection _lastJDBCDefaultConnection
    private final Object _lockLastJDBCDefaultConnection = new Object()

    /** Last used JDBC default connection */
    JDBCConnection getLastJdbcDefaultConnection() {
        JDBCConnection res
        synchronized (_lockLastJDBCDefaultConnection) {
            res = _lastJDBCDefaultConnection
        }
        return res
    }
    /** Last used JDBC default connection */
    void setLastJdbcDefaultConnection(JDBCConnection value) {
        synchronized (_lockLastJDBCDefaultConnection) {
            _lastJDBCDefaultConnection = value
        }
    }

    /** Default JDBC connection for datasets */
    private final Map<String, JDBCConnection> _defaultJDBCConnection = new HashMap<String, JDBCConnection>()

    /** Default JDBC connection for datasets */
    JDBCConnection defaultJdbcConnection(String datasetClassName = null) {
        JDBCConnection res
        if (datasetClassName == null)
            res = lastJdbcDefaultConnection
        else {
            if (!(datasetClassName in (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).listJdbcClasses))
                throw new DslError(this, '#dsl.invalid_jdbc_dataset', [className: datasetClassName])

            synchronized (_lockLastJDBCDefaultConnection) {
                res = _defaultJDBCConnection.get(datasetClassName)
                if (res == null && lastJdbcDefaultConnection != null && datasetClassName == RepositoryDatasets.QUERYDATASET)
                    res = lastJdbcDefaultConnection
            }
        }

        if (_langOpts.useThreadModelCloning && IsCurrentProcessInThread(false)) {
            def thread = Thread.currentThread() as ExecutorThread
            res = thread.registerCloneObject(_repositoryStorageManager.repository(RepositoryConnections).nameCloneCollection, res,
                    {
                        def c = (it as Connection).cloneConnection()
                        c.sysParams.dslNameObject = res.dslNameObject
                        return c
                    }
            ) as JDBCConnection
        }

        return res
    }

    /** Use specified JDBC connection as default */
    JDBCConnection useJdbcConnection(String datasetClassName, JDBCConnection value) {
        synchronized (_lockLastJDBCDefaultConnection) {
            if (datasetClassName != null) {
                if (!(datasetClassName in (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).listJdbcClasses))
                    throw new DslError(this, '#dsl.invalid_jdbc_dataset', [className: datasetClassName])
                _defaultJDBCConnection.put(datasetClassName, value)
            }
            setLastJdbcDefaultConnection(value)
        }

        return value
    }

    /** Last used file default connection */
    private FileConnection _lastFileDefaultConnection
    private final Object _lockLastFileDefaultConnection = new Object()

    /** Last used file default connection */
    FileConnection getLastFileDefaultConnection() {
        FileConnection res
        synchronized (_lockLastFileDefaultConnection) {
            res = _lastFileDefaultConnection
        }
        return res
    }
    /** Last used file default connection */
    void setLastFileDefaultConnection(FileConnection value) {
        synchronized (_lockLastFileDefaultConnection) {
            _lastFileDefaultConnection = value
        }
    }

    private final Map<String, FileConnection> _defaultFileConnection = new HashMap<String, FileConnection>()

    /** Default file connection for datasets */
    FileConnection defaultFileConnection(String datasetClassName = null) {
        FileConnection res
        synchronized (_lockLastFileDefaultConnection) {
            if (datasetClassName == null)
                res = lastFileDefaultConnection
            else {
                if (!(datasetClassName in (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).listFileClasses))
                    throw new DslError(this, '#dsl.invalid_file_dataset', [className: datasetClassName])

                res = _defaultFileConnection.get(datasetClassName)
            }
        }

        if (_langOpts.useThreadModelCloning && IsCurrentProcessInThread(false)) {
            def thread = Thread.currentThread() as ExecutorThread
            res = thread.registerCloneObject(_repositoryStorageManager.repository(RepositoryConnections).nameCloneCollection, res,
                    {
                        def c = (it as Connection).cloneConnection()
                        c.sysParams.dslNameObject = res.dslNameObject
                        return c
                    }
            ) as FileConnection
        }

        return res
    }

    /** Use specified file connection as default */
    FileConnection useFileConnection(String datasetClassName, FileConnection value) {
        synchronized (_lockLastFileDefaultConnection) {
            if (datasetClassName != null) {
                if (!(datasetClassName in (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).listFileClasses))
                    throw new DslError(this, '#dsl.invalid_file_dataset', [className: datasetClassName])

                _defaultFileConnection.put(datasetClassName, value)
            }
            setLastFileDefaultConnection(value)
        }

        return value
    }

    /** Last used other type default connection */
    private Connection _lastOtherDefaultConnection
    private final Object _lockLastOtherDefaultConnection = new Object()

    /** Last used other type default connection */
    Connection getLastOtherDefaultConnection() {
        Connection res
        synchronized (_lockLastOtherDefaultConnection) {
            res = _lastOtherDefaultConnection
        }
        return res
    }
    /** Last used other type default connection */
    void setLastOtherDefaultConnection(Connection value) {
        synchronized (_lockLastOtherDefaultConnection) {
            _lastOtherDefaultConnection = value
        }
    }

    private final Map<String, Connection> _defaultOtherConnection = new HashMap<String, Connection>()

    /** Default other type connection for datasets */
    Connection defaultOtherConnection(String datasetClassName = null) {
        Connection res
        synchronized (_lockLastOtherDefaultConnection) {
            if (datasetClassName == null)
                res = lastOtherDefaultConnection
            else {
                if (!(datasetClassName in (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).listOtherClasses))
                    throw new DslError(this, '#dsl.invalid_other_dataset', [className: datasetClassName])

                res = _defaultOtherConnection.get(datasetClassName)
            }
        }

        if (_langOpts.useThreadModelCloning && IsCurrentProcessInThread(false)) {
            def thread = Thread.currentThread() as ExecutorThread
            res = thread.registerCloneObject(_repositoryStorageManager.repository(RepositoryConnections).nameCloneCollection, res,
                    {
                        def c = (it as Connection).cloneConnection()
                        c.sysParams.dslNameObject = res.dslNameObject
                        return c
                    }
            ) as Connection
        }

        return res
    }

    /** Use specified other type connection as default */
    Connection useOtherConnection(String datasetClassName, Connection value) {
        synchronized (_lockLastOtherDefaultConnection) {
            if (datasetClassName != null) {
                if (!(datasetClassName in (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).listOtherClasses))
                    throw new DslError(this, '#dsl.invalid_other_dataset', [className: datasetClassName])

                _defaultOtherConnection.put(datasetClassName, value)
            }
            setLastOtherDefaultConnection(value)
        }

        return value
    }

    /**
     * Search for an object in the repository
     * @param obj dataset
     * @return name of the object in the repository or null if not found
     */
    String findDataset(Dataset obj) {
        (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).find(obj)
    }

    /**
     * Find a dataset by name
     * @param name dataset name
     * @return found dataset object or null if not found
     */
    Dataset findDataset(String name) {
        (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).find(name)
    }

    /** Register dataset in repository */
    Dataset registerDataset(Connection connection, String datasetClassName, String name, Boolean registration = false,
                                      Connection defaultConnection = null, Class<Connection> classConnection = null, Closure cl = null) {
        (_repositoryStorageManager.repository(RepositoryDatasets) as RepositoryDatasets).register(this, connection,
                datasetClassName, name, registration, defaultConnection, classConnection, cl)
    }

    /**
     * Register dataset in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    Dataset registerDatasetObject(Dataset obj, String name = null, Boolean validExist = true) {
        (_repositoryStorageManager.repository(RepositoryDatasets)).registerObject(this, obj, name, validExist) as Dataset
    }

    /**
     * Remove dataset from repository
     * @param mask filter mask (use Path expression syntax)
     * @param datasetClasses dataset class list
     * @param filter object filtering code
     */
    void unregisterDataset(String mask = null, List datasetClasses = null,
                           @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Dataset'])
                                   Closure<Boolean> filter = null) {
        _repositoryStorageManager.repository(RepositoryDatasets).unregister(mask, datasetClasses, filter)
    }

    /**
     * Register datasets from storage configuration files to repository
     * @param mask connection name mask
     * @param env environment
     * @param ignoreExists don't load existing ones (default true)
     * @return number of registered datasets
     */
    Integer registerDatasetsFromStorage(String mask, String env, Boolean ignoreExists = true) {
        return repositoryStorageManager.loadRepository(RepositoryDatasets, mask, env, ignoreExists)
    }

    /**
     * Register datasets from storage configuration files to repository
     * @param mask connection name mask
     * @param ignoreExists don't load existing ones (default true)
     * @return number of registered datasets
     */
    Integer registerDatasetsFromStorage(String mask = null, Boolean ignoreExists = true) {
        return repositoryStorageManager.loadRepository(RepositoryDatasets, mask, null, ignoreExists)
    }

    /**
     * Register datasets from storage configuration files to repository
     * @param ignoreExists don't load existing ones
     * @return number of registered datasets
     */
    Integer registerDatasetsFromStorage(Boolean ignoreExists) {
        return repositoryStorageManager.loadRepository(RepositoryDatasets, null, null, ignoreExists)
    }

    /**
     * Return list of repository history point manager
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of history point manager names according to specified conditions
     */
    List<String> listHistorypoints(String mask = null,
                                   @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.HistoryPointManager'])
                                           Closure<Boolean> filter = null) {
        _repositoryStorageManager.repository(RepositoryHistorypoints).list(mask, null, true, filter)
    }

    /**
     * Return list of repository history point manager
     * @param filter object filtering code
     * @return list of history point manager names according to specified conditions
     */
    List<String> listHistorypoints(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.HistoryPointManager'])
                                            Closure<Boolean> filter) {
        listHistorypoints(null, filter)
    }

    /**
     * Process repository history point managers for specified mask and filter
     * @param mask filter mask (use Path expression syntax)
     * @param cl process code
     */
    void processHistorypoints(String mask,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        _repositoryStorageManager.repository(RepositoryHistorypoints).processObjects(mask, null, cl)
    }

    /**
     * Process repository history point managers for specified mask and filter
     * @param cl process code
     */
    void processHistorypoints(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processHistorypoints(null, cl)
    }

    /**
     * Search for an object in the repository
     * @param obj history point manager
     * @return name of the object in the repository or null if not found
     */
    String findHistorypoint(HistoryPointManager obj) {
        _repositoryStorageManager.repository(RepositoryHistorypoints).find(obj)
    }

    /**
     * Find a history point manager by name
     * @param name history point manager name
     * @return found history point manager object or null if not found
     */
    HistoryPointManager findHistorypoint(String name) {
        (_repositoryStorageManager.repository(RepositoryHistorypoints) as RepositoryHistorypoints).find(name) as HistoryPointManager
    }

    /**
     * Register history point in repository
     * @param connection used connection
     * @param name name in repository
     * @param registration registering in repository
     * @param cl user code
     * @return history point manager object
     */
    HistoryPointManager registerHistoryPoint(String name,  Boolean registration = false) {
        (_repositoryStorageManager.repository(RepositoryHistorypoints) as RepositoryHistorypoints).register(this,
                RepositoryHistorypoints.HISTORYPOINTMANAGER, name, registration, true) as HistoryPointManager
    }

    /**
     * Register history point in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    HistoryPointManager registerHistoryPointObject(HistoryPointManager obj, String name = null, Boolean validExist = true) {
        (_repositoryStorageManager.repository(RepositoryHistorypoints) as RepositoryHistorypoints).registerObject(this,
                obj, name, validExist) as HistoryPointManager
    }

    /**
     * Unregister history point manager
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     */
    void unregisterHistorypoint(String mask = null,
                                @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.HistoryPointManager'])
                                        Closure<Boolean> filter = null) {
        _repositoryStorageManager.repository(RepositoryHistorypoints).unregister(mask, null, filter)
    }

    /**
     * Register history point managers from storage configuration files to repository
     * @param mask history point manager name mask
     * @param env environment
     * @param ignoreExists don't load existing ones (default true)
     * @return number of registered history point managers
     */
    Integer registerHistorypointsFromStorage(String mask, String env, Boolean ignoreExists = true) {
        return repositoryStorageManager.loadRepository(RepositoryHistorypoints, mask, env, ignoreExists)
    }

    /**
     * Register history point managers from storage configuration files to repository
     * @param mask history point manager name mask
     * @param ignoreExists don't load existing ones (default true)
     * @return number of registered history point managers
     */
    Integer registerHistorypointsFromStorage(String mask = null, Boolean ignoreExists = true) {
        return repositoryStorageManager.loadRepository(RepositoryHistorypoints, mask, null, ignoreExists)
    }

    /**
     * Register history point managers from storage configuration files to repository
     * @param ignoreExists don't load existing ones
     * @return number of registered history point managers
     */
    Integer registerHistorypointsFromStorage(Boolean ignoreExists) {
        return repositoryStorageManager.loadRepository(RepositoryHistorypoints, null, null, ignoreExists)
    }

    /**
     * Return list of repository sequences
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of sequences names according to specified conditions
     */
    List<String> listSequences(String mask = null,
                               @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.Sequence'])
                                       Closure<Boolean> filter = null) {
        _repositoryStorageManager.repository(RepositorySequences).list(mask, null, true, filter)
    }

    /**
     * Return list of repository sequences
     * @param filter object filtering code
     * @return list of sequences names according to specified conditions
     */
    List<String> listSequences(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.Sequence'])
                                       Closure<Boolean> filter) {
        listSequences(null, filter)
    }

    /**
     * Process repository sequences for specified mask and filter
     * @param mask filter mask (use Path expression syntax)
     * @param cl process code
     */
    void processSequences(String mask,
                          @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        _repositoryStorageManager.repository(RepositorySequences).processObjects(mask, null, cl)
    }

    /**
     * Process all repository sequences
     * @param cl process code
     */
    void processSequences(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processSequences(null, cl)
    }

    /**
     * Search for an object in the repository
     * @param obj sequence
     * @return name of the object in the repository or null if not found
     */
    String findSequence(Sequence obj) {
        _repositoryStorageManager.repository(RepositorySequences).find(obj)
    }

    /**
     * Find a sequences by name
     * @param name sequences name
     * @return found sequences object or null if not found
     */
    Sequence findSequence(String name) {
        (_repositoryStorageManager.repository(RepositorySequences) as RepositorySequences).find(name)
    }

    /**
     * Register sequence in repository
     * @param connection used connection
     * @param name name in repository
     * @param registration registering in repository
     * @param cl user code
     * @return sequence object
     */
    Sequence registerSequence(Connection connection, String name, Boolean registration = false,
                                        Closure cl = null) {
        (_repositoryStorageManager.repository(RepositorySequences) as RepositorySequences).register(this, connection,
                RepositorySequences.SEQUENCE, name, registration, defaultJdbcConnection(RepositoryDatasets.QUERYDATASET), JDBCConnection, cl)
    }

    /**
     * Register sequence in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    Sequence registerSequenceObject(Sequence obj, String name = null, Boolean validExist = true) {
        (_repositoryStorageManager.repository(RepositorySequences) as RepositorySequences).registerObject(this, obj, name, validExist) as Sequence
    }

    /**
     * Unregister sequence
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     */
    void unregisterSequence(String mask = null,
                            @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.Sequence'])
                                    Closure<Boolean> filter = null) {
        _repositoryStorageManager.repository(RepositorySequences).unregister(mask, null, filter)
    }

    /**
     * Register sequences from storage configuration files to repository
     * @param mask sequence name mask
     * @param env environment
     * @param ignoreExists don't load existing ones (default true)
     * @return number of registered sequences
     */
    Integer registerSequencesFromStorage(String mask, String env, Boolean ignoreExists = true) {
        return repositoryStorageManager.loadRepository(RepositorySequences, mask, env, ignoreExists)
    }

    /**
     * Register sequences from storage configuration files to repository
     * @param mask sequence name mask
     * @param ignoreExists don't load existing ones (default true)
     * @return number of registered sequences
     */
    Integer registerSequencesFromStorage(String mask = null, Boolean ignoreExists = true) {
        return repositoryStorageManager.loadRepository(RepositorySequences, mask, null, ignoreExists)
    }

    /**
     * Register sequences from storage configuration files to repository
     * @param ignoreExists don't load existing ones
     * @return number of registered sequences
     */
    Integer registerSequencesFromStorage(Boolean ignoreExists) {
        return repositoryStorageManager.loadRepository(RepositorySequences, null, null, ignoreExists)
    }

    /**
     * Return list of repository file managers for specified mask, class and filter
     * @param mask filter mask (use Path expression syntax)
     * @param filemanagerClasses file manager class list
     * @param filter object filtering code
     * @return list of file manager names according to specified conditions
     */
    List<String> listFilemanagers(String mask = null, List filemanagerClasses = null,
                                  @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.files.Manager'])
                                          Closure<Boolean> filter = null) {
        _repositoryStorageManager.repository(RepositoryFilemanagers).list(mask, filemanagerClasses, true, filter)
    }

    /**
     * Return list of repository file managers for specified class
     * @param filemanagerClasses file manager class list
     * @param filter object filtering code
     * @return list of file manager names according to specified conditions
     */
    List<String> listFilemanagers(List filemanagerClasses,
                                  @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.files.Manager'])
                                          Closure<Boolean> filter = null) {
        listFilemanagers(null, filemanagerClasses, filter)
    }

    /**
     * Return list of repository file managers for specified class
     * @param filter object filtering code
     * @return list of file manager names according to specified conditions
     */
    List<String> listFilemanagers(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.files.Manager'])
                                          Closure<Boolean> filter) {
        listFilemanagers(null, null, filter)
    }

    /**
     * Return list of repository file managers objects for specified list of name
     * @param names list of name file managers
     * @return list of file manager objects
     */
    List<Manager> filemanagerObjects(List<String> names) {
        if (names == null) return null
        def res = [] as List<Manager>
        names.each { managerName ->
            res << filemanager(managerName)
        }
        return res
    }

    /**
     * Process repository file managers for specified mask and classes
     * @param mask filter mask (use Path expression syntax)
     * @param filemanagerClasses file manager class list
     * @param cl process code
     */
    void processFilemanagers(String mask, List filemanagerClasses,
                             @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        _repositoryStorageManager.repository(RepositoryFilemanagers).processObjects(mask, filemanagerClasses, cl)
    }

    /**
     * Process repository file managers for specified mask
     * @param mask filter mask (use Path expression syntax)
     * @param cl processing code
     */
    void processFilemanagers(String mask,
                             @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processFilemanagers(mask, null, cl)
    }

    /**
     * Process repository file managers for specified classes
     * @param filemanagerClasses file manager class list
     * @param cl processing code
     */
    void processFilemanagers(List filemanagerClasses,
                             @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processFilemanagers(null, filemanagerClasses, cl)
    }

    /**
     * Process all repository file managers
     * @param cl processing code
     */
    void processFilemanagers(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processFilemanagers(null, null, cl)
    }

    /**
     * Search for an object in the repository
     * @param obj file manager
     * @return name of the object in the repository or null if not found
     */
    String findFilemanager(Manager obj) {
        _repositoryStorageManager.repository(RepositoryFilemanagers).find(obj)
    }

    /**
     * Find a file manager by name
     * @param name file manager name
     * @return found file manager object or null if not found
     */
    Manager findFilemanager(String name) {
        (_repositoryStorageManager.repository(RepositoryFilemanagers) as RepositoryFilemanagers).find(name)
    }

    /** Register file manager in repository */
    Manager registerFileManager(String fileManagerClassName, String name,
                                          Boolean registration = false, Boolean cloneInThread = true) {
        (_repositoryStorageManager.repository(RepositoryFilemanagers) as RepositoryFilemanagers).register(this, fileManagerClassName, name, registration, cloneInThread)
    }

    /**
     * Register file manager object in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    Manager registerFileManagerObject(Manager obj, String name = null, Boolean validExist = true,
                                      Boolean encryptPasswords = true) {
        (_repositoryStorageManager.repository(RepositoryFilemanagers) as RepositoryFilemanagers).registerObject(this,
                obj, name, validExist, encryptPasswords)
    }

    /**
     * Unregister file manager by a given mask or a list of their classes
     * @param mask mask of objects (in Path format)
     * @param connectionClasses list of processed file managers classes
     * @param filter filter for detect connections to unregister
     */
    void unregisterFilemanager(String mask = null, List filemanagerClasses = null,
                               @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.files.Manager'])
                                       Closure<Boolean> filter = null) {
        _repositoryStorageManager.repository(RepositoryFilemanagers).unregister(mask, filemanagerClasses, filter)
    }

    /**
     * Register file managers from storage configuration files to repository
     * @param mask file manager name mask
     * @param env environment
     * @param ignoreExists don't load existing ones (default true)
     * @return number of registered file managers
     */
    Integer registerFilemanagersFromStorage(String mask, String env, Boolean ignoreExists = true) {
        return repositoryStorageManager.loadRepository(RepositoryFilemanagers, mask, env, ignoreExists)
    }

    /**
     * Register file managers from storage configuration files to repository
     * @param mask file manager name mask
     * @param ignoreExists don't load existing ones (default true)
     * @return number of registered file managers
     */
    Integer registerFilemanagersFromStorage(String mask = null, Boolean ignoreExists = true) {
        return repositoryStorageManager.loadRepository(RepositoryFilemanagers, mask, null, ignoreExists)
    }

    /**
     * Register file managers from storage configuration files to repository
     * @param ignoreExists don't load existing ones
     * @return number of registered file managers
     */
    Integer registerFilemanagersFromStorage(Boolean ignoreExists) {
        return repositoryStorageManager.loadRepository(RepositoryFilemanagers, null, null, ignoreExists)
    }

    /**
     * Load and run groovy script file
     * @param fileName script file path
     * @param runOnce do not execute if previously executed
     * @param vars set values for public script fields declared
     * @param extVars extended variables
     * @param events script events code
     * @return exit code
     */
    Map<String, Object> runGroovyScriptFile(String fileName, Boolean runOnce, Map vars = new HashMap(), Map extVars = null, ScriptEvents events = null) {
        File sourceFile = new File(fileName)
        def groovyClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(sourceFile)
        return runGroovyClass(groovyClass, runOnce, vars, extVars, events)
    }

    /**
     * Load and run groovy script file
     * @param fileName script file path
     * @param vars set values for public script fields declared
     * @param extVars extended variables
     * @param events script events code
     * @return exit code
     */
    Map<String, Object> runGroovyScriptFile(String fileName, Map vars = new HashMap(), Map extVars = null, ScriptEvents events) {
        runGroovyScriptFile(fileName, false, vars, extVars, events)
    }

    /**
     * Load and run groovy script file
     * @param fileName script file path
     * @param runOnce do not execute if previously executed
     * @param vars set values for public script fields declared
     * @return exit code
     */
    Map<String, Object> runGroovyScriptFile(String fileName, Boolean runOnce, Closure vars) {
        File sourceFile = new File(fileName)
        def groovyClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(sourceFile)
        return runGroovyClass(groovyClass, runOnce, vars)
    }

    /**
     * Load and run groovy script file
     * @param fileName script file path
     * @param runOnce do not execute if previously executed
     * @param vars set values for public script fields declared
     * @return exit code
     */
    Map<String, Object> runGroovyScriptFile(String fileName, Closure vars) {
        runGroovyScriptFile(fileName, false, vars)
    }

    /**
     * Load and run groovy script file
     * @param fileName script file path
     * @param runOnce do not execute if previously executed
     * @param configSection set values for public script fields declared from the specified configuration section
     * @return exit code
     */
    Map<String, Object> runGroovyScriptFile(String fileName, Boolean runOnce, String configSection) {
        def sectParams = configuration.manager.findSection(configSection)
        if (sectParams == null)
            throw new DslError(this, '#config.section_not_found', [section: configSection])

        return runGroovyScriptFile(fileName, runOnce, sectParams)
    }

    /**
     * Load and run groovy script file
     * @param fileName script file path
     * @param configSection set values for public script fields declared from the specified configuration section
     * @return exit code
     */
    Map<String, Object> runGroovyScriptFile(String fileName, String configSection) {
        runGroovyScriptFile(fileName, false, configSection)
    }

    private final instanceBindingName = '_main_getl'

    /**
     * Run groovy script class
     * @param groovyClass groovy script class
     * @param runOnce do not execute if previously executed
     * @param vars set values for public script fields declared
     * @param extVars extended variables
     * @param events script events code
     * @return exit code
     */
    Map<String, Object> runGroovyClass(Class groovyClass, Boolean runOnce, Map vars = new HashMap(), Map extVars = null, ScriptEvents events = null) {
        def className = groovyClass.name
        def previouslyRun = (executedClasses.indexOfListItem(className) != -1)
        if (previouslyRun && BoolUtils.IsValue(runOnce))
            return [exitCode: 0]

        Script script
        synchronized (_lockMainThread) {
            script = groovyClass.getConstructor().newInstance() as Script
        }
        def res = runGroovyInstance(script, true, vars, extVars, events)

        if (!previouslyRun)
            executedClasses.addToList(className)

        return res
    }

    static private final Object _lockMainThread = new Object()

    /** Set thread to work in main thread */
    private void switchThreadToMain(Boolean workInMain) {
        if (!(Thread.currentThread() instanceof ExecutorThread))
            return

        def thread = Thread.currentThread() as ExecutorThread
        thread.params.workInMain = workInMain
    }

    /** Extended script variables */
    private final Map scriptExtendedVars = new HashMap()
    /** Extended script variables */
    @JsonIgnore
    @Synchronized('scriptExtendedVars')
    Map getScriptExtendedVars() { this.scriptExtendedVars }

    /** Script events code */
    private final ScriptEvents scriptEvents = new ScriptEvents()
    @JsonIgnore
    @Synchronized('scriptEvents')
    /** Script events code */
    ScriptEvents getScriptEvents() { this.scriptEvents }

    /**
     * Run groovy script object
     * @param script groovy script object
     * @param runScript run script or only use in current Getl object
     * @param vars set values for public script fields declared
     * @param extVars extended variables
     * @param events script events code
     * @return exitCode and result
     */
    protected Map<String, Object> runGroovyInstance(Script script, Boolean runScript = true, Map vars = new HashMap(), Map extVars = null, ScriptEvents events = null) {
        def exitCode = 0
        def result = null

        /*if (runScript)
            _repositoryFilter.pushOptions(true)*/

        def isGetlScript = (script instanceof Getl)
        try {
            if (isGetlScript) {
                def scriptGetl = script as Getl

                synchronized (_lockMainThread) {
                    if (extVars != null)
                        scriptGetl.scriptExtendedVars.putAll(extVars)

                    if (events != null)
                        scriptGetl.scriptEvents.putAll(events)

                    if (scriptGetl != getlMainInstance)
                        scriptGetl.importGetlParams(_params)

                    if (runScript)
                        scriptGetl._setGetlInstance()

                    switchThreadToMain(true)
                    try {
                        _doInitMethod(scriptGetl)
                        if (vars != null && !vars.isEmpty()) {
                            _fillFieldFromVars(scriptGetl, vars)
                        }
                        _doCheckMethod(scriptGetl)
                    }
                    catch (Throwable e) {
                        logError("Script \"${script.getClass().name}\" initialization error", e)
                        logging.manager.exception(e)
                        throw e
                    }
                    finally {
                        switchThreadToMain(false)
                    }
                }
            } else if (vars != null && !vars.isEmpty()) {
                script.binding = new Binding(vars)
            }

            if (runScript) {
                if (isInitMode)
                    logInfo("### Start script ${script.getClass().name}")
                def pt = startProcess("Execution groovy script ${script.getClass().name}", 'class')
                try {
                    if (isGetlScript)
                        (script as Getl).prepare()

                    if (script instanceof RepositorySave)
                        (script as RepositorySave)._initRepositorySave()

                    result = script.run()

                    if (script instanceof RepositorySave)
                        (script as RepositorySave)._processRepositorySave()
                }
                catch (AbortDsl e) {
                    if (e.typeCode == AbortDsl.STOP_CLASS) {
                        if (e.message != null)
                            logInfo(e.message)
                        if (e.exitCode != null)
                            exitCode = e.exitCode
                    } else {
                        throw e
                    }
                }
                catch (Throwable e) {
                    logError("Script \"${script.getClass().name}\" execution error", e)
                    logging.manager.exception(e)
                    try {
                        synchronized (_lockMainThread) {
                            switchThreadToMain(true)
                            try {
                                _doErrorMethod(script, e)
                            }
                            finally {
                                switchThreadToMain(false)
                            }
                        }
                    }
                    catch (Exception err) {
                        logError("An error occurred while processing the error for script \"${script.getClass().name}\"", err)
                        logging.manager.exception(err)
                    }
                    finally {
                        throw e
                    }
                }
                finally {
                    synchronized (_lockMainThread) {
                        switchThreadToMain(true)
                        try {
                            _doDoneMethod(script)
                        }
                        finally {
                            switchThreadToMain(false)
                        }
                    }
                }
                pt.finish()
                if (isInitMode)
                    logInfo("### Finish script ${script.getClass().name}")
            }
        }
        finally {
            if (runScript) {
                if (isGetlScript)
                    releaseTemporaryObjects(script as Getl)

                this._setGetlInstance()
                /*_repositoryFilter.pullOptions()*/
            }
        }

        return [exitCode: exitCode, result: result]
    }

    /** Prepare a script before running */
    protected void prepare() { }

    /** Release temporary objects by name mask "#*" */
    void releaseTemporaryObjects(Getl creator) {
        _repositoryStorageManager.listRepositories.each { name ->
            def rep = _repositoryStorageManager.repository(name)
            rep.releaseTemporary(creator)
        }
    }

    /**
     * Run groovy script class
     * @param groovyClass groovy script class
     * @param vars set values for public script fields declared
     * @param extVars extended variables
     * @param events script events code
     * @return exitCode and result
     */
    Map<String, Object> runGroovyClass(Class groovyClass, Map vars = new HashMap(), Map extVars = null, ScriptEvents events = null) {
        runGroovyClass(groovyClass, false, vars, extVars, events)
    }

    /**
     * Run groovy script class
     * @param groovyClass groovy script class
     * @param runOnce do not execute if previously executed
     * @param clVars set values for public script fields declared
     * @return exitCode and result
     */
    Map<String, Object> runGroovyClass(Class groovyClass, Boolean runOnce, Closure clVars) {
        def cfg = new groovy.util.ConfigSlurper()
        def map = cfg.parse(new ClosureScript(closure: clVars))
        return runGroovyClass(groovyClass, runOnce, MapUtils.ConfigObject2Map(map))
    }

    /**
     * Run groovy script class
     * @param groovyClass groovy script class
     * @param clVars set values for public script fields declared
     * @return exitCode and result
     */
    Map<String, Object> runGroovyClass(Class groovyClass, Closure clVars) {
        runGroovyClass(groovyClass, false, clVars)
    }

    /**
     * Run groovy script class
     * @param groovyClass groovy script class
     * @param runOnce do not execute if previously executed
     * @param configSection set values for public script fields declared from the specified configuration section
     * @return exitCode and result
     */
    Map<String, Object> runGroovyClass(Class groovyClass, Boolean runOnce, String configSection) {
        def sectParams = configuration.manager.findSection(configSection)
        if (sectParams == null)
            throw new DslError(this, '#config.section_not_found', [section: configSection])

        return runGroovyClass(groovyClass, runOnce, sectParams)
    }

    /**
     * Run groovy script class
     * @param groovyClass groovy script class
     * @param configSection set values for public script fields declared from the specified configuration section
     * @return exitCode and result
     */
    Map<String, Object> runGroovyClass(Class groovyClass, String configSection) {
        runGroovyClass(groovyClass, false, configSection)
    }

    /**
     * Call the listed Getl scripts, if they have not already been run previously
     * <br><br>Example:
     * <br>callScripts project.processed.GetlScript1, project.processed.GetlScript2
     * @param scripts list of Getl scripts to run
     * @return list of exit code and result
     */
    List<Map<String, Object>> callScripts(Class<Getl>... scriptClasses) {
        def res = [] as List<Map<String, Object>>
        scriptClasses.each { script ->
            res.add(runGroovyClass(script, true))
        }

        return res
    }

    /**
     * Call Getl script
     * @param scriptClass Getl script class
     * @param runOnce do not execute if previously executed
     * @param vars set values for public script fields declared
     * @param extVars extended variables
     * @param events script events code
     * @return exitCode and result
     */
    Map<String, Object> callScript(Class<Getl> scriptClass, Boolean runOnce, Map vars = new HashMap(), Map extVars = null, ScriptEvents events = null) {
        return runGroovyClass(scriptClass, runOnce, vars, extVars, events)
    }

    /**
     * Call Getl script
     * @param scriptClass Getl script class
     * @param vars set values for public script fields declared
     * @param extVars extended variables
     * @param events script events code
     * @return exitCode and result
     */
    Map<String, Object> callScript(Class<Getl> scriptClass, Map vars = new HashMap(), Map extVars = null, ScriptEvents events = null) {
        return runGroovyClass(scriptClass, false, vars, extVars, events)
    }

    /**
     * Call Getl script
     * @param scriptClass Getl script class
     * @param runOnce do not execute if previously executed
     * @param clVars set values for script fields declared as "@Field"
     * @return exitCode and result
     */
    Map<String, Object> callScript(Class<Getl> scriptClass, Boolean runOnce, Closure clVars) {
        return runGroovyClass(scriptClass, runOnce, clVars)
    }

    /**
     * Call Getl script
     * @param scriptClass Getl script class
     * @param clVars set values for public script fields declared
     * @return exitCode and result
     */
    Map<String, Object> callScript(Class<Getl> scriptClass, Closure clVars) {
        return runGroovyClass(scriptClass, false, clVars)
    }

    /**
     * Call Getl script
     * @param groovyClass groovy script class
     * @param runOnce do not execute if previously executed
     * @param configSection set values for script fields declared as "@Field" from the specified configuration section
     * @return exitCode and result
     */
    Map<String, Object> callScript(Class<Getl> scriptClass, Boolean runOnce, String configSection) {
        return runGroovyClass(scriptClass, runOnce, configSection)
    }

    /**
     * Call Getl script
     * @param groovyClass groovy script class
     * @param configSection set values for public script fields declared from the specified configuration section
     * @return exitCode and result
     */
    Map<String, Object> callScript(Class<Getl> scriptClass, String configSection) {
        return runGroovyClass(scriptClass, false, configSection)
    }

    /**
     * Call Getl instance script
     * @param script Getl script instance
     * @param runOnce do not execute if previously executed
     * @return exitCode and result
     */
    Map<String, Object> callScript(Getl script, Boolean runOnce = false) {
        def className = script.getClass().name
        def previouslyRun = (executedClasses.indexOfListItem(className) != -1)
        if (previouslyRun && BoolUtils.IsValue(runOnce))
            return [exitCode: 0]

        def res = runGroovyInstance(script, true)

        if (!previouslyRun)
            executedClasses.addToList(className)

        return res
    }

    private Boolean _usedMode = false
    /** Script used in other script */
    Boolean getUsedMode() { _usedMode }
    /** Script used in other script */
    @Synchronized
    protected void setUsedMode(Boolean value) {
        _usedMode = value
    }

    /**
     * Connect the use of the script class to work in the current script
     * @param scriptClass script class to use
     * @param vars set values for public script fields declared
     * @param extVars extended variables
     * @param events script events code
     * @return
     */
    Getl useScript(Class<Getl> scriptClass, Map vars = new HashMap(), Map extVars = null, ScriptEvents events = null) {
        def script = scriptClass.getConstructor().newInstance() as Getl
        return useScript(script, vars, extVars, events)
    }

    /**
     * Connect the use of the script to work in the current script
     * @param script script to use
     * @param vars set values for public script fields declared
     * @param extVars extended variables
     * @param events script events code additional variables for the script
     * @return
     */
    Getl useScript(Getl script, Map vars = new HashMap(), Map extVars = null, ScriptEvents events = null) {
        script.setUsedMode(true)
        runGroovyInstance(script, false, vars, extVars, events)
        return script
    }

    /** Init script method */
    void init() { }

    /**
     *  Call script init method before execute script
     */
    protected void _doInitMethod(Script script) {
        if (script instanceof Getl)
            (script as Getl).init()
        else {
            def m = script.getClass().methods.find { it.name == 'init' }
            if (m != null)
                script.invokeMethod('init', null)
        }
    }

    /** Check script method */
    void check() { }

    /**
     *  Call script check method after setting field values
     */
    protected void _doCheckMethod(Script script) {
        if (script instanceof Getl)
            (script as Getl).check()
        else {
            def m = script.getClass().methods.find { it.name == 'check' }
            if (m != null)
                script.invokeMethod('check', null)
        }
    }

    /** Done script method */
    void done() { }

    /**
     *  Call script done method before execute script
     */
    protected void _doDoneMethod(Script script) {
        if (script instanceof Getl)
            (script as Getl).done()
        else {
            def m = script.getClass().methods.find { it.name == 'done' }
            if (m != null)
                script.invokeMethod('done', null)
        }
    }

    /** Error script method */
    void error(Throwable e) { }

    /**
     *  Call script error method before execute script
     */
    protected void _doErrorMethod(Script script, Throwable e) {
        if (script instanceof Getl)
            (script as Getl).error(e)
        else {
            def m = script.getClass().methods.find { it.name == 'error' }
            if (m != null)
                script.invokeMethod('error', e)
        }
    }

    /**
     * Fill script field property from external arguments
     * @param script groove script object
     * @param vars vars set values for script fields declared as "@Field"
     * @param validExist check for the existence of fields in the script
     */
    void _fillFieldFromVars(Script script, Map vars, Boolean validExist = true, Boolean startGroovy = false) {
        vars.each { key, value ->
            MetaProperty prop = script.hasProperty(key as String)
            if (prop == null) {
                if (validExist)
                    throw new DslError(this, '#dsl.invalid_script_property', [prop: key])
                else
                    return
            }

            if (value != null) {
                if (script instanceof Getl && (value instanceof String || value instanceof GString)) {
                    def v = (value as Object).toString()
                    if (v.indexOf('{') != -1)
                        value = StringUtils.EvalMacroString(v,
                                [environment: configuration.environment, unitmode: unitTestMode] + (script as Getl).scriptExtendedVars, false)
                }

                switch (prop.type) {
                    case Character:
                        if (!(value instanceof Character))
                            value = value.toString().toCharacter()

                        break
                    case String:
                        if (value instanceof GetlRepository)
                            value = (value as GetlRepository).dslNameObject
                        else
                            value = value.toString()

                        break
                    case GString:
                        if (value instanceof GetlRepository)
                            value = (value as GetlRepository).dslNameObject
                        else
                            value = value.toString()

                        break
                    case Short:
                        if (!(value instanceof Short))
                            value = ConvertUtils.Object2Short(value)

                        break
                    case Integer:
                        if (!(value instanceof Integer))
                            value = ConvertUtils.Object2Int(value.toString())

                        break
                    case Long:
                        if (!(value instanceof Long))
                            value = ConvertUtils.Object2Long(value.toString())

                        break
                    case Float:
                        if (!(value instanceof Float))
                            value = ConvertUtils.Object2Float(value.toString())

                        break
                    case Double:
                        if (!(value instanceof Double))
                            value = ConvertUtils.Object2Double(value.toString())

                        break
                    case BigInteger:
                        if (!(value instanceof BigInteger))
                            value = ConvertUtils.Object2BigInteger(value.toString())

                        break
                    case BigDecimal:
                        if (!(value instanceof BigDecimal))
                            value = ConvertUtils.Object2BigDecimal(value.toString())

                        break
                    case Date:
                        if (!(value instanceof Date)) {
                            if (value instanceof String || value instanceof GString) {
                                value = (value as Object).toString()
                                if (value.length() == 10)
                                    value = DateUtils.ParseDate('yyyy-MM-dd', value, false)
                                else if (value.length() == 19)
                                    value = DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss', value, false)
                                else if (value.length() == 23)
                                    value = DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss.SSS', value, false)
                                else
                                    throw new DslError(this, '#dsl.invalid_date_format', [value: value])
                            }
                            else if (value instanceof Number)
                                value = new Date((value as Number).longValue())
                            else
                                throw new DslError(this, '#dsl.invalid_convert_num_to_date', [value: value])
                        }

                        break
                    case Time:
                        if (!(value instanceof Time)) {
                            if (value instanceof String || value instanceof GString) {
                                value = (value as Object).toString()
                                if (value.length() == 8)
                                    value = DateUtils.ParseSQLTime('HH:mm:ss', value, false)
                                else if (value.length() == 12)
                                    value = DateUtils.ParseSQLTime('HH:mm:ss.SSS', value, false)
                                else
                                    throw new DslError(this, '#dsl.invalid_time_format', [value: value])
                            }
                            else if (value instanceof Number)
                                value = new Time((value as Number).longValue())
                            else
                                throw new DslError(this, '#dsl.invalid_convert_num_to_time', [value: value])
                        }

                        break
                    case Boolean:
                        if (!(value instanceof Boolean)) {
                            value = ConvertUtils.Object2Boolean(value)
                        }

                        break
                    case List:
                        if (!(value instanceof List)) {
                            value = ConvertUtils.String2List(value.toString())
                        }

                        break
                    case Map:
                        if (!(value instanceof Map)) {
                            value = ConvertUtils.String2Map(value.toString())
                        }

                        break
                    case Path:
                        if (!(value instanceof Path)) {
                            if (value instanceof String || value instanceof GString)
                                value = new Path((value as Object).toString())
                            else
                                throw new DslError(this, '#dsl.invalid_convert_path', [value: value])
                        }

                        break

                    default:
                        if (script instanceof Getl) {
                            def getl = script as Getl
                            if (Connection.isAssignableFrom(prop.type)) {
                                if (value instanceof String)
                                    value = getl.connection(value as String)
                            } else if (Dataset.isAssignableFrom(prop.type)) {
                                if (value instanceof String)
                                    value = getl.dataset(value as String)
                            } else if (Manager.isAssignableFrom(prop.type)) {
                                if (value instanceof String)
                                    value = getl.filemanager(value as String)
                            } else if (HistoryPointManager.isAssignableFrom(prop.type)) {
                                if (value instanceof String)
                                    value = getl.historypoint(value as String)
                            } else if (Sequence.isAssignableFrom(prop.type)) {
                                if (value instanceof String)
                                    value = getl.sequence(value as String)
                            } else if (BaseModel.isAssignableFrom(prop.type)) {
                                if (MapTables.isAssignableFrom(prop.type)) {
                                    if (value instanceof String)
                                        value = getl.models.mapTables(value as String)
                                } else if (MonitorRules.isAssignableFrom(prop.type)) {
                                    if (value instanceof String)
                                        value = getl.models.monitorRules(value as String)
                                } else if (ReferenceFiles.isAssignableFrom(prop.type)) {
                                    if (value instanceof String)
                                        value = getl.models.referenceFiles(value as String)
                                } else if (ReferenceVerticaTables.isAssignableFrom(prop.type)) {
                                    if (value instanceof String)
                                        value = getl.models.referenceVerticaTables(value as String)
                                } else if (SetOfTables.isAssignableFrom(prop.type)) {
                                    if (value instanceof String)
                                        value = getl.models.setOfTables(value as String)
                                }
                            }
                        }
                }
            }

            try {
                if (!startGroovy)
                    prop.setProperty(script, value)
                else
                    configuration.manager.vars.put(key as String, value)
            }
            catch (Exception e) {
                logError("Can not assign by class ${value.getClass().name} value \"$value\" to property \"$key\" with class \"${prop.type.name}\"", e)
                throw e
            }
        }
    }

    /**
     * Detect delegate object for closure code
     * @param obj analyzed object
     * @param ignore options and return their owner
     * @return found object
     */
    static Object DetectClosureDelegate(Object obj, Boolean ignoreOpts = false) {
        if (obj == null)
            return null

        while (obj instanceof Closure)
            obj = (obj as Closure).delegate

        if (obj != null && ignoreOpts && obj instanceof BaseSpec && !(obj instanceof GetlRepository))
            obj = (obj as BaseSpec).ownerObject

        return obj
    }

    /** Run closure with call parent parameter */
    protected Object runClosure(Object parent, Closure cl) {
        if (cl == null) return null
        return parent.with(cl)
    }

    /** Configuration content */
    Map<String, Object> getConfigContent() { configuration.manager.content }

    /** Configuration variables */
    Map<String, Object> getConfigVars() { configuration.manager.vars }

    /** Init section configuration options */
    Map<String, Object> getConfigInit() { configuration.manager.content.init as Map<String, Object> }

    /** Global section configuration options */
    Map<String, Object> getConfigGlobal() { configuration.manager.content.global as Map<String, Object> }

    /** OS variables */
    Map<String, Object> getSysVars() { Config.SystemProps() }

    /** Execute a synchronized sequence of logging commands */
    void logConsistently(Closure cl) { logging.manager.consistently(cl) }

    /** Write message for specified level to log */
    void logWrite(String level, String message) { logging.manager.write(level, message) }

    /** Write message for specified level to log */
    void logWrite(Level level, String message) { logging.manager.write(level, message) }

    /** Write message as level the INFO to log */
    void logInfo(Object msg) { logging.manager.info(msg?.toString()) }

    /** Write message as level the WARNING to log */
    void logWarn(Object msg, Throwable e = null) { logging.manager.warning(msg?.toString(), e) }

    /** Write message as level the SEVERE to log */
    void logError(Object msg, Throwable e = null) { logging.manager.severe(msg?.toString(), e) }

    /** Write message as level the FINE to log */
    void logFine(Object msg) { logging.manager.fine(msg?.toString()) }

    /** Write message as level the FINER to log */
    void logFiner(Object msg) { logging.manager.finer(msg?.toString()) }

    /** Write message as level the FINEST to log */
    void logFinest(Object msg) { logging.manager.finest(msg?.toString()) }

    /** Write message as level the CONFIG to log */
    void logConfig(Object msg) { logging.manager.config(msg?.toString()) }

    /** System temporary directory */
    static String getSystemTempPath() { TFS.systemPath }

    /** Check the work in a separate from the main thread */
    static Boolean IsCurrentProcessInThread(Boolean checkWorkInMainThread = false) {
        (Thread.currentThread() instanceof ExecutorThread) &&
                (!checkWorkInMainThread || !BoolUtils.IsValue((Thread.currentThread() as ExecutorThread).params.workInMain))
    }

    /** Getl options instance */
    private LangSpec _langOpts

    /** Getl options */
    LangSpec getOptions() { _langOpts }

    /** Getl options */
    LangSpec options(@DelegatesTo(LangSpec)
                     @ClosureParams(value = SimpleType, options = ['getl.lang.opts.LangSpec']) Closure cl = null) {
        def processDataset = _langOpts.processControlDataset
        def checkOnStart = _langOpts.checkProcessOnStart

        runClosure(_langOpts, cl)

        if (_langOpts.processControlDataset != null && _langOpts.checkProcessOnStart) {
            if (processDataset != _langOpts.processControlDataset || !checkOnStart) {
                allowProcess(true)
            }
        }

        return _langOpts
    }

    /** Current configuration manager */
    protected ConfigSlurper getConfigManager() { configuration.manager }

    /** Configuration options instance */
    private ConfigSpec _configOpts

    /** Configuration options */
    ConfigSpec getConfiguration() { _configOpts }

    /** Configuration options */
    ConfigSpec configuration(@DelegatesTo(ConfigSpec)
                             @ClosureParams(value = SimpleType, options = ['getl.lang.opts.ConfigSpec']) Closure cl = null) {
        runClosure(_configOpts, cl)

        return _configOpts
    }

    /** Log options instance */
    private LogSpec _logOpts

    /** Log options */
    LogSpec getLogging() { _logOpts }

    /** Log options */
    LogSpec logging(@DelegatesTo(LogSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.opts.LogSpec']) Closure cl = null) {
        def logFileName = _logOpts.getLogFileName()
        runClosure(_logOpts, cl)
        if (logFileName != _logOpts.getLogFileName())
            logFine("# Start logging to log file ${_logOpts.getLogFileName()}")

        return _logOpts
    }

    /** Etl manager */
    private EtlSpec _etl
    /** Etl manager */
    EtlSpec getEtl() { _etl }

    /** Etl manager */
    EtlSpec etl(@DelegatesTo(EtlSpec)
                @ClosureParams(value = SimpleType, options = ['getl.lang.opts.EtlSpec']) Closure cl = null) {
        runClosure(_etl, cl)

        return _etl
    }

    /** Model manager */
    private ModelSpec _models
    /** Model manager */
    ModelSpec getModels() { _models }

    /** Model manager */
    ModelSpec models(@DelegatesTo(ModelSpec)
                     @ClosureParams(value = SimpleType, options = ['getl.lang.opts.ModelSpec']) Closure cl = null) {
        runClosure(_models, cl)

        return _models
    }

    /**
     * Existing connection from repository
     * @param name name in repository
     * @param cl object processing code
     * @return connection object
     */
    Connection connection(String name,
                          @DelegatesTo(Connection)
                          @ClosureParams(value = SimpleType, options = ['getl.data.Connection']) Closure cl = null) {
        if (name == null)
            throw new DslError(this, '#params.required', [param: 'name', detail: 'connection'])

        def parent = registerConnection(null, name, false) as Connection
        runClosure(parent, cl)

        return parent
    }

    /**
     * Clone connection object
     * @param con original connection to clone
     * @return clone connection
     */
    Connection cloneConnection(Connection con) {
        if (con == null)
            throw new DslError(this, '#params.required', [param: 'connection', detail: 'cloneConnection'])
        return con.cloneConnection(null, this)
    }

    /**
     * Clone connection object and register to repository
     * @param newName repository name for cloned dataset
     * @param con original connection to clone
     * @param cl cloned connection processing code
     * @return clone connection
     */
    Connection cloneConnection(String newName, Connection con,
                               @DelegatesTo(Connection)
                               @ClosureParams(value = SimpleType, options = ['getl.data.Connection']) Closure cl = null) {
        def parent = con.cloneConnection(null, this)
        registerConnectionObject(parent, newName)
        runClosure(parent, cl)

        return parent
    }

    /**
     * Existing dataset from repository
     * @param name name in repository
     * @param cl object processing code
     * @return dataset object
     */
    Dataset dataset(String name,
                    @DelegatesTo(Dataset)
                    @ClosureParams(value = SimpleType, options = ['getl.data.Dataset']) Closure cl = null) {
        if (name == null)
            throw new DslError(this, '#params.required', [param: 'name', detail: 'dataset'])

        def obj = findDataset(name)
        if (obj == null)
            throw new DslError(this, '#dsl.object.not_found', [type: 'Dataset', repname: name])

        def parent = registerDataset(null, obj.getClass().name, name)
        runClosure(parent, cl)

        return parent
    }

    /**
     * Clone dataset object
     * @param dataset original dataset to clone
     * @return cloned dataset
     */
    Dataset cloneDataset(Dataset dataset) {
        if (dataset == null)
            throw new DslError(this, '#params.required', [param: 'dataset', detail: 'cloneDataset'])

        return dataset.cloneDataset(null, null, this)
    }

    /**
     * Clone dataset object
     * @param dataset original dataset to clone
     * @param con used connection for new dataset
     * @return cloned dataset
     */
    Dataset cloneDataset(Dataset dataset, Connection con) {
        if (dataset == null)
            throw new DslError(this, '#params.required', [param: 'dataset', detail: 'cloneDataset'])

        return dataset.cloneDataset(con, null, this)
    }

    /**
     * Clone dataset object
     * @param dataset original dataset to clone
     * @param cloneConnection clone dataset connection
     * @return cloned dataset
     */
    Dataset cloneDataset(Dataset dataset, Boolean cloneConnection) {
        if (dataset == null)
            throw new DslError(this, '#params.required', [param: 'dataset', detail: 'cloneDataset'])

        return (cloneConnection)?dataset.cloneDatasetConnection(null, this):
                dataset.cloneDataset(null, null, this)
    }

    /**
     * Clone dataset object and register in repository
     * @param newName repository name for cloned dataset
     * @param dataset original dataset to clone
     * @param con used connection for new dataset
     * @param cl cloned dataset processing code
     * @return cloned dataset
     */
    Dataset cloneDataset(String newName, Dataset dataset, Connection con = null,
                         @DelegatesTo(Dataset)
                         @ClosureParams(value = SimpleType, options = ['getl.data.Dataset']) Closure cl = null) {
        def parent = cloneDataset(dataset, con as Connection)
        registerDatasetObject(parent, newName)
        runClosure(parent, cl)

        return parent
    }

    /**
     * Clone dataset object and register in repository
     * @param newName repository name for cloned dataset
     * @param dataset original dataset to clone
     * @param cloneConnection clone dataset connection
     * @param cl cloned dataset processing code
     * @return cloned dataset
     */
    Dataset cloneDataset(String newName, Dataset dataset, Boolean cloneConnection,
                         @DelegatesTo(Dataset)
                         @ClosureParams(value = SimpleType, options = ['getl.data.Dataset']) Closure cl = null) {
        def parent = cloneDataset(dataset, cloneConnection)
        registerDatasetObject(parent, newName)
        runClosure(parent, cl)

        return parent
    }

    /**
     * Existing jdbc connection from repository
     * @param name name in repository
     * @param cl object processing code
     * @return jdbc connection object
     */
    JDBCConnection jdbcConnection(String name,
                                  @DelegatesTo(JDBCConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.jdbc.JDBCConnection']) Closure cl = null) {
        if (name == null)
            throw new DslError(this, '#params.required', [param: 'name', detail: 'jdbcConnection'])

        def parent = registerConnection(null, name, false) as Connection
        if (!(parent instanceof JDBCConnection))
            throw new DslError(this, '#dsl.invalid_instance_object', [name: name, type: 'JDBC connection'])

        runClosure(parent, cl)

        return parent
    }

    /**
     * Existing jdbc table from repository
     * @param name name in repository
     * @param cl object processing code
     * @return jdbc table object
     */
    TableDataset jdbcTable(String name,
                           @DelegatesTo(TableDataset)
                           @ClosureParams(value = SimpleType, options = ['getl.jdbc.TableDataset']) Closure cl = null) {
        if (name == null)
            throw new DslError(this, '#params.required', [param: 'name', detail: 'jdbcTable'])

        def obj = findDataset(name)
        if (obj == null)
            throw new DslError(this, '#dsl.object.not_found', [type: 'JDBC table', repname: name])
        if (!(obj instanceof TableDataset))
            throw new DslError(this, '#dsl.invalid_instance_object', [name: name, type: 'JDBC table'])

        def parent = registerDataset(null, obj.getClass().name, name) as TableDataset
        runClosure(parent, cl)

        return parent
    }

    /** JDBC view dataset */
    ViewDataset view(String name = null, Boolean registration = false, JDBCConnection connection = null,
                     @DelegatesTo(ViewDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.ViewDataset']) Closure cl = null) {
        def parent = registerDataset(connection, RepositoryDatasets.VIEWDATASET, name, registration,
                                        defaultJdbcConnection(RepositoryDatasets.QUERYDATASET), JDBCConnection, cl) as ViewDataset
        runClosure(parent, cl)
        return parent
    }

    /** JDBC view dataset */
    ViewDataset view(String name, Boolean registration = false,
                     @DelegatesTo(ViewDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.ViewDataset']) Closure cl) {
        view(name, registration, null, cl)
    }

    /** JDBC view dataset */
    ViewDataset view(@DelegatesTo(ViewDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.ViewDataset']) Closure cl) {
        view(null, false, null, cl)
    }

    /** JDBC view dataset */
    ViewDataset view(JDBCConnection connection,
                     @DelegatesTo(ViewDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.ViewDataset']) Closure cl) {
        view(null, false, connection, cl)
    }

    /** Firebird connection */
    FirebirdConnection firebirdConnection(String name, Boolean registration,
                                          @DelegatesTo(FirebirdConnection)
                                          @ClosureParams(value = SimpleType, options = ['getl.firebird.FirebirdConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.FIREBIRDCONNECTION, name, registration) as FirebirdConnection
        runClosure(parent, cl)

        return parent
    }

    /** Firebird connection */
    FirebirdConnection firebirdConnection(String name,
                                          @DelegatesTo(FirebirdConnection)
                                          @ClosureParams(value = SimpleType, options = ['getl.firebird.FirebirdConnection']) Closure cl = null) {
        firebirdConnection(name, false, cl)
    }

    /** Firebird connection */
    FirebirdConnection firebirdConnection(@DelegatesTo(FirebirdConnection)
                                          @ClosureParams(value = SimpleType, options = ['getl.firebird.FirebirdConnection']) Closure cl) {
        firebirdConnection(null, false, cl)
    }

    /** Firebird current connection */
    FirebirdConnection firebirdConnection() {
        defaultJdbcConnection(RepositoryDatasets.FIREBIRDTABLE) as FirebirdConnection
    }

    /** Use default Firebird connection for new datasets */
    FirebirdConnection useFirebirdConnection(FirebirdConnection connection) {
        useJdbcConnection(RepositoryDatasets.FIREBIRDTABLE, connection) as FirebirdConnection
    }

    /** Firebird table */
    FirebirdTable firebirdTable(String name, Boolean registration,
                                @DelegatesTo(FirebirdTable)
                                @ClosureParams(value = SimpleType, options = ['getl.firebird.FirebirdTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.FIREBIRDTABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.FIREBIRDTABLE), FirebirdConnection, cl) as FirebirdTable
        runClosure(parent, cl)

        return parent
    }

    /** Firebird table */
    FirebirdTable firebirdTable(String name,
                                @DelegatesTo(FirebirdTable)
                                @ClosureParams(value = SimpleType, options = ['getl.firebird.FirebirdTable']) Closure cl = null) {
        firebirdTable(name, false, cl)
    }

    /** Firebird table */
    FirebirdTable firebirdTable(@DelegatesTo(FirebirdTable)
                                @ClosureParams(value = SimpleType, options = ['getl.firebird.FirebirdTable']) Closure cl) {
        firebirdTable(null, false, cl)
    }

    /** H2 connection */
    H2Connection h2Connection(String name, Boolean registration,
                              @DelegatesTo(H2Connection)
                              @ClosureParams(value = SimpleType, options = ['getl.h2.H2Connection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.H2CONNECTION, name, registration) as H2Connection
        runClosure(parent, cl)

        return parent
    }

    /** H2 connection */
    H2Connection h2Connection(String name,
                              @DelegatesTo(H2Connection)
                              @ClosureParams(value = SimpleType, options = ['getl.h2.H2Connection']) Closure cl = null) {
        h2Connection(name, false, cl)
    }

    /** H2 connection */
    H2Connection h2Connection(@DelegatesTo(H2Connection)
                              @ClosureParams(value = SimpleType, options = ['getl.h2.H2Connection']) Closure cl) {
        h2Connection(null, false, cl)
    }

    /** H2 current connection */
    H2Connection h2Connection() {
        defaultJdbcConnection(RepositoryDatasets.H2TABLE) as H2Connection
    }

    /** Use default H2 connection for new datasets */
    H2Connection useH2Connection(H2Connection connection) {
        useJdbcConnection(RepositoryDatasets.H2TABLE, connection) as H2Connection
    }

    /** H2 table */
    H2Table h2Table(String name, Boolean registration,
                    @DelegatesTo(H2Table)
                    @ClosureParams(value = SimpleType, options = ['getl.h2.H2Table']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.H2TABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.H2TABLE), H2Connection, cl) as H2Table
        runClosure(parent, cl)

        return parent
    }

    /** H2 table */
    H2Table h2Table(String name,
                    @DelegatesTo(H2Table)
                    @ClosureParams(value = SimpleType, options = ['getl.h2.H2Table']) Closure cl = null) {
        h2Table(name, false, cl)
    }

    /** H2 table */
    H2Table h2Table(@DelegatesTo(H2Table)
                    @ClosureParams(value = SimpleType, options = ['getl.h2.H2Table']) Closure cl) {
        h2Table(null, false, cl)
    }

    /** ClickHouse connection */
    ClickHouseConnection clickhouseConnection(String name, Boolean registration,
                                              @DelegatesTo(ClickHouseConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.clickhouse.ClickHouseConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.CLICKHOUSECONNECTION, name, registration) as ClickHouseConnection
        runClosure(parent, cl)

        return parent
    }

    /** ClickHouse connection */
    ClickHouseConnection clickhouseConnection(String name,
                                              @DelegatesTo(ClickHouseConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.clickhouse.ClickHouseConnection']) Closure cl = null) {
        clickhouseConnection(name, false, cl)
    }

    /** ClickHouse connection */
    ClickHouseConnection clickhouseConnection(@DelegatesTo(ClickHouseConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.clickhouse.ClickHouseConnection']) Closure cl) {
        clickhouseConnection(null, false, cl)
    }

    /** ClickHouse current connection */
    ClickHouseConnection clickhouseConnection() {
        defaultJdbcConnection(RepositoryDatasets.CLICKHOUSETABLE) as ClickHouseConnection
    }

    /** Use default ClickHouse connection for new datasets */
    ClickHouseConnection useClickHouseConnection(ClickHouseConnection connection) {
        useJdbcConnection(RepositoryDatasets.CLICKHOUSETABLE, connection) as ClickHouseConnection
    }

    /** ClickHouse table */
    ClickHouseTable clickhouseTable(String name, Boolean registration,
                                    @DelegatesTo(ClickHouseTable)
                                    @ClosureParams(value = SimpleType, options = ['getl.clickhouse.ClickHouseTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.CLICKHOUSETABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.CLICKHOUSETABLE), ClickHouseConnection, cl) as ClickHouseTable
        runClosure(parent, cl)

        return parent
    }

    /** ClickHouse table */
    ClickHouseTable clickhouseTable(String name,
                                    @DelegatesTo(ClickHouseTable)
                                    @ClosureParams(value = SimpleType, options = ['getl.clickhouse.ClickHouseTable']) Closure cl = null) {
        clickhouseTable(name, false, cl)
    }

    /** ClickHouse table */
    ClickHouseTable clickhouseTable(@DelegatesTo(ClickHouseTable)
                                    @ClosureParams(value = SimpleType, options = ['getl.clickhouse.ClickHouseTable']) Closure cl) {
        clickhouseTable(null, false, cl)
    }

    /** DB2 connection */
    DB2Connection db2Connection(String name, Boolean registration,
                                @DelegatesTo(DB2Connection)
                                @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Connection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.DB2CONNECTION, name, registration) as DB2Connection
        runClosure(parent, cl)

        return parent

    }

    /** DB2 connection */
    DB2Connection db2Connection(String name,
                                @DelegatesTo(DB2Connection)
                                @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Connection']) Closure cl = null) {
        db2Connection(name, false, cl)
    }

    /** DB2 connection */
    DB2Connection db2Connection(@DelegatesTo(DB2Connection)
                                @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Connection']) Closure cl) {
        db2Connection(null, false, cl)
    }

    /** DB2 current connection */
    DB2Connection db2Connection() {
        defaultJdbcConnection(RepositoryDatasets.DB2TABLE) as DB2Connection
    }

    /** Use default DB2 connection for new datasets */
    DB2Connection useDb2Connection(DB2Connection connection) {
        useJdbcConnection(RepositoryDatasets.DB2TABLE, connection) as DB2Connection
    }

    /** DB2 database table */
    DB2Table db2Table(String name, Boolean registration,
                      @DelegatesTo(DB2Table)
                      @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Table']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.DB2TABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.DB2TABLE), DB2Connection, cl) as DB2Table
        runClosure(parent, cl)

        return parent
    }

    /** DB2 database table */
    DB2Table db2Table(String name,
                      @DelegatesTo(DB2Table)
                      @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Table']) Closure cl = null) {
        db2Table(name, false, cl)
    }

    /** DB2 database table */
    DB2Table db2Table(@DelegatesTo(DB2Table)
                      @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Table']) Closure cl) {
        db2Table(null, false, cl)
    }

    /** Hive connection */
    HiveConnection hiveConnection(String name, Boolean registration,
                                  @DelegatesTo(HiveConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.hive.HiveConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.HIVECONNECTION, name, registration) as HiveConnection
        runClosure(parent, cl)

        return parent
    }

    /** Hive connection */
    HiveConnection hiveConnection(String name,
                                  @DelegatesTo(HiveConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.hive.HiveConnection']) Closure cl = null) {
        hiveConnection(name, false, cl)
    }

    /** Hive connection */
    HiveConnection hiveConnection(@DelegatesTo(HiveConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.hive.HiveConnection']) Closure cl) {
        hiveConnection(null, false, cl)
    }

    /** Hive current connection */
    HiveConnection hiveConnection() {
        defaultJdbcConnection(RepositoryDatasets.HIVETABLE) as HiveConnection
    }

    /** Use default Hive connection for new datasets */
    HiveConnection useHiveConnection(HiveConnection connection) {
        useJdbcConnection(RepositoryDatasets.HIVETABLE, connection) as HiveConnection
    }

    /** Hive table */
    HiveTable hiveTable(String name, Boolean registration,
                        @DelegatesTo(HiveTable)
                        @ClosureParams(value = SimpleType, options = ['getl.hive.HiveTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.HIVETABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.HIVETABLE), HiveConnection, cl) as HiveTable
        runClosure(parent, cl)

        return parent
    }

    /** Hive table */
    HiveTable hiveTable(String name,
                        @DelegatesTo(HiveTable)
                        @ClosureParams(value = SimpleType, options = ['getl.hive.HiveTable']) Closure cl = null) {
        hiveTable(name, false, cl)
    }

    /** Hive table */
    HiveTable hiveTable(@DelegatesTo(HiveTable)
                        @ClosureParams(value = SimpleType, options = ['getl.hive.HiveTable']) Closure cl) {
        hiveTable(null, false, cl)
    }

    /** Hana connection */
    HanaConnection hanaConnection(String name, Boolean registration,
                                  @DelegatesTo(HanaConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.sap.HanaConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.HANACONNECTION, name, registration) as HanaConnection
        runClosure(parent, cl)

        return parent
    }

    /** Hana connection */
    HanaConnection hanaConnection(String name,
                                  @DelegatesTo(HanaConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.sap.HanaConnection']) Closure cl = null) {
        hanaConnection(name, false, cl)
    }

    /** Hana connection */
    HanaConnection hanaConnection(@DelegatesTo(HanaConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.sap.HanaConnection']) Closure cl) {
        hanaConnection(null, false, cl)
    }

    /** Hana current connection */
    HanaConnection hanaConnection() {
        defaultJdbcConnection(RepositoryDatasets.HANATABLE) as HanaConnection
    }

    /** Use default Hana connection for new datasets */
    HanaConnection useHanaConnection(HanaConnection connection) {
        useJdbcConnection(RepositoryDatasets.HANATABLE, connection) as HanaConnection
    }

    /** Hana table */
    HanaTable hanaTable(String name, Boolean registration,
                        @DelegatesTo(HanaTable)
                        @ClosureParams(value = SimpleType, options = ['getl.sap.HanaTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.HANATABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.HANATABLE), HanaConnection, cl) as HanaTable
        runClosure(parent, cl)

        return parent
    }

    /** Hana table */
    HanaTable hanaTable(String name,
                        @DelegatesTo(HanaTable)
                        @ClosureParams(value = SimpleType, options = ['getl.sap.HanaTable']) Closure cl = null) {
        hanaTable(name, false, cl)
    }

    /** Hana table */
    HanaTable hanaTable(@DelegatesTo(HanaTable)
                        @ClosureParams(value = SimpleType, options = ['getl.sap.HanaTable']) Closure cl) {
        hanaTable(null, false, cl)
    }

    /** Impala connection */
    ImpalaConnection impalaConnection(String name, Boolean registration,
                                      @DelegatesTo(ImpalaConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.impala.ImpalaConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.IMPALACONNECTION, name, registration) as ImpalaConnection
        runClosure(parent, cl)

        return parent
    }

    /** Impala connection */
    ImpalaConnection impalaConnection(String name,
                                      @DelegatesTo(ImpalaConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.impala.ImpalaConnection']) Closure cl = null) {
        impalaConnection(name, false, cl)
    }

    /** Impala connection */
    ImpalaConnection impalaConnection(@DelegatesTo(ImpalaConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.impala.ImpalaConnection']) Closure cl) {
        impalaConnection(null, false, cl)
    }

    /** Impala current connection */
    ImpalaConnection impalaConnection() {
        defaultJdbcConnection(RepositoryDatasets.IMPALATABLE) as ImpalaConnection
    }

    /** Use default Impala connection for new datasets */
    ImpalaConnection useImpalaConnection(ImpalaConnection connection) {
        useJdbcConnection(RepositoryDatasets.IMPALATABLE, connection) as ImpalaConnection
    }

    /** Impala table */
    ImpalaTable impalaTable(String name, Boolean registration,
                            @DelegatesTo(ImpalaTable)
                            @ClosureParams(value = SimpleType, options = ['getl.impala.ImpalaTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.IMPALATABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.IMPALATABLE), ImpalaConnection, cl) as ImpalaTable
        runClosure(parent, cl)

        return parent
    }

    /** Impala table */
    ImpalaTable impalaTable(String name,
                            @DelegatesTo(ImpalaTable)
                            @ClosureParams(value = SimpleType, options = ['getl.impala.ImpalaTable']) Closure cl = null) {
        impalaTable(name, false, cl)
    }

    /** Impala table */
    ImpalaTable impalaTable(@DelegatesTo(ImpalaTable)
                            @ClosureParams(value = SimpleType, options = ['getl.impala.ImpalaTable']) Closure cl) {
        impalaTable(null, false, cl)
    }

    /** MSSQL connection */
    MSSQLConnection mssqlConnection(String name, Boolean registration,
                                    @DelegatesTo(MSSQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.MSSQLCONNECTION, name, registration) as MSSQLConnection
        runClosure(parent, cl)

        return parent
    }

    /** MSSQL connection */
    MSSQLConnection mssqlConnection(String name,
                                    @DelegatesTo(MSSQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLConnection']) Closure cl = null) {
        mssqlConnection(name, false, cl)
    }

    /** MSSQL connection */
    MSSQLConnection mssqlConnection(@DelegatesTo(MSSQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLConnection']) Closure cl) {
        mssqlConnection(null, false, cl)
    }

    /** MSSQL current connection */
    MSSQLConnection mssqlConnection() {
        defaultJdbcConnection(RepositoryDatasets.MSSQLTABLE) as MSSQLConnection
    }

    /** Use default MSSQL connection for new datasets */
    MSSQLConnection useMssqlConnection(MSSQLConnection connection) {
        useJdbcConnection(RepositoryDatasets.MSSQLTABLE, connection) as MSSQLConnection
    }

    /** MSSQL database table */
    MSSQLTable mssqlTable(String name, Boolean registration,
                          @DelegatesTo(MSSQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.MSSQLTABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.MSSQLTABLE), MSSQLConnection, cl) as MSSQLTable
        runClosure(parent, cl)

        return parent
    }

    /** MSSQL database table */
    MSSQLTable mssqlTable(String name,
                          @DelegatesTo(MSSQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLTable']) Closure cl = null) {
        mssqlTable(name, false, cl)
    }

    /** MSSQL database table */
    MSSQLTable mssqlTable(@DelegatesTo(MSSQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLTable']) Closure cl) {
        mssqlTable(null, cl)
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(String name, Boolean registration,
                                    @DelegatesTo(MySQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.MYSQLCONNECTION, name, registration) as MySQLConnection
        runClosure(parent, cl)

        return parent
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(String name,
                                    @DelegatesTo(MySQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLConnection']) Closure cl = null) {
        mysqlConnection(name, false, cl)
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(@DelegatesTo(MySQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLConnection']) Closure cl) {
        mysqlConnection(null, false, cl)
    }

    /** MySQL current connection */
    MySQLConnection mysqlConnection() {
        defaultJdbcConnection(RepositoryDatasets.MYSQLTABLE) as MySQLConnection
    }

    /** Use default MySQL connection for new datasets */
    MySQLConnection useMysqlConnection(MySQLConnection connection) {
        useJdbcConnection(RepositoryDatasets.MYSQLTABLE, connection) as MySQLConnection
    }

    /** MySQL database table */
    MySQLTable mysqlTable(String name, Boolean registration,
                          @DelegatesTo(MySQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.MYSQLTABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.MYSQLTABLE), MySQLConnection, cl) as MySQLTable
        runClosure(parent, cl)

        return parent
    }

    /** MySQL database table */
    MySQLTable mysqlTable(String name,
                          @DelegatesTo(MySQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLTable']) Closure cl = null) {
        mysqlTable(name, false, cl)
    }

    /** MySQL database table */
    MySQLTable mysqlTable(@DelegatesTo(MySQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLTable']) Closure cl) {
        mysqlTable(null, false, cl)
    }

    /** Netezza connection */
    NetezzaConnection netezzaConnection(String name, Boolean registration,
                                        @DelegatesTo(NetezzaConnection)
                                        @ClosureParams(value = SimpleType, options = ['getl.netezza.NetezzaConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.NETEZZACONNECTION, name, registration) as NetezzaConnection
        runClosure(parent, cl)

        return parent
    }

    /** Netezza connection */
    NetezzaConnection netezzaConnection(String name,
                                        @DelegatesTo(NetezzaConnection)
                                        @ClosureParams(value = SimpleType, options = ['getl.netezza.NetezzaConnection']) Closure cl = null) {
        netezzaConnection(name, false, cl)
    }

    /** Netezza connection */
    NetezzaConnection netezzaConnection(@DelegatesTo(NetezzaConnection)
                                        @ClosureParams(value = SimpleType, options = ['getl.netezza.NetezzaConnection']) Closure cl) {
        netezzaConnection(null, false, cl)
    }

    /** Netezza current connection */
    NetezzaConnection netezzaConnection() {
        defaultJdbcConnection(RepositoryDatasets.NETEZZATABLE) as NetezzaConnection
    }

    /** Use default Netezza connection for new datasets */
    NetezzaConnection useNetezzaConnection(NetezzaConnection connection) {
        useJdbcConnection(RepositoryDatasets.NETEZZATABLE, connection) as NetezzaConnection
    }

    /** Netezza database table */
    NetezzaTable netezzaTable(String name, Boolean registration,
                              @DelegatesTo(NetezzaTable)
                              @ClosureParams(value = SimpleType, options = ['getl.netezza.NetezzaTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.NETEZZATABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.NETEZZATABLE), NetezzaConnection, cl) as NetezzaTable
        runClosure(parent, cl)

        return parent
    }

    /** Netezza database table */
    NetezzaTable netezzaTable(String name,
                              @DelegatesTo(NetezzaTable)
                              @ClosureParams(value = SimpleType, options = ['getl.netezza.NetezzaTable']) Closure cl = null) {
        netezzaTable(name, false, cl)
    }

    /** Netezza database table */
    NetezzaTable netezzaTable(@DelegatesTo(NetezzaTable)
                              @ClosureParams(value = SimpleType, options = ['getl.netezza.NetezzaTable']) Closure cl) {
        netezzaTable(null, false, cl)
    }

    /** Oracle connection */
    OracleConnection oracleConnection(String name, Boolean registration,
                                      @DelegatesTo(OracleConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.ORACLECONNECTION, name, registration) as OracleConnection
        runClosure(parent, cl)

        return parent
    }

    /** Oracle connection */
    OracleConnection oracleConnection(String name,
                                      @DelegatesTo(OracleConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleConnection']) Closure cl = null) {
        oracleConnection(name, false, cl)
    }

    /** Oracle connection */
    OracleConnection oracleConnection(@DelegatesTo(OracleConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleConnection']) Closure cl) {
        oracleConnection(null, false, cl)
    }

    /** Oracle current connection */
    OracleConnection oracleConnection() {
        defaultJdbcConnection(RepositoryDatasets.ORACLETABLE) as OracleConnection
    }

    /** Use default Oracle connection for new datasets */
    OracleConnection useOracleConnection(OracleConnection connection) {
        useJdbcConnection(RepositoryDatasets.ORACLETABLE, connection) as OracleConnection
    }

    /** Oracle table */
    OracleTable oracleTable(String name, Boolean registration,
                            @DelegatesTo(OracleTable)
                            @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.ORACLETABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.ORACLETABLE), OracleConnection, cl) as OracleTable
        runClosure(parent, cl)

        return parent
    }

    /** Oracle table */
    OracleTable oracleTable(String name,
                            @DelegatesTo(OracleTable)
                            @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleTable']) Closure cl = null) {
        oracleTable(name, false, cl)
    }

    /** Oracle table */
    OracleTable oracleTable(@DelegatesTo(OracleTable)
                            @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleTable']) Closure cl) {
        oracleTable(null, false, cl)
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(String name, Boolean registration,
                                              @DelegatesTo(PostgreSQLConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.POSTGRESQLCONNECTION, name, registration) as PostgreSQLConnection
        runClosure(parent, cl)

        return parent
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(String name,
                                              @DelegatesTo(PostgreSQLConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLConnection']) Closure cl = null) {
        postgresqlConnection(name, false, cl)
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(@DelegatesTo(PostgreSQLConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLConnection']) Closure cl) {
        postgresqlConnection(null, false, cl)
    }

    /** PostgreSQL default connection */
    PostgreSQLConnection postgresqlConnection() {
        defaultJdbcConnection(RepositoryDatasets.POSTGRESQLTABLE) as PostgreSQLConnection
    }

    /** Use default PostgreSQL connection for new datasets */
    PostgreSQLConnection usePostgresqlConnection(PostgreSQLConnection connection) {
        useJdbcConnection(RepositoryDatasets.POSTGRESQLTABLE, connection) as PostgreSQLConnection
    }

    /** PostgreSQL database table */
    PostgreSQLTable postgresqlTable(String name, Boolean registration,
                                    @DelegatesTo(PostgreSQLTable)
                                    @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.POSTGRESQLTABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.POSTGRESQLTABLE), PostgreSQLConnection, cl) as PostgreSQLTable
        runClosure(parent, cl)

        return parent
    }


    /** PostgreSQL database table */
    PostgreSQLTable postgresqlTable(String name,
                                    @DelegatesTo(PostgreSQLTable)
                                    @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLTable']) Closure cl = null) {
        postgresqlTable(name, false, cl)
    }

    /** PostgreSQL database table */
    PostgreSQLTable postgresqlTable(@DelegatesTo(PostgreSQLTable)
                                    @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLTable']) Closure cl) {
        postgresqlTable(null, false, cl)
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(String name, Boolean registration,
                                        @DelegatesTo(VerticaConnection)
                                        @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.VERTICACONNECTION, name, registration) as VerticaConnection
        runClosure(parent, cl)

        return parent
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(String name,
                                        @DelegatesTo(VerticaConnection)
                                        @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaConnection']) Closure cl = null) {
        verticaConnection(name, false, cl)
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(@DelegatesTo(VerticaConnection)
                                        @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaConnection']) Closure cl) {
        verticaConnection(null, false, cl)
    }

    /** Vertica default connection */
    VerticaConnection verticaConnection() {
        defaultJdbcConnection(RepositoryDatasets.VERTICATABLE) as VerticaConnection
    }

    /** Use default Vertica connection for new datasets */
    VerticaConnection useVerticaConnection(VerticaConnection connection) {
        useJdbcConnection(RepositoryDatasets.VERTICATABLE, connection) as VerticaConnection
    }

    /** Vertica table */
    VerticaTable verticaTable(String name, Boolean registration,
                              @DelegatesTo(VerticaTable)
                              @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.VERTICATABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.VERTICATABLE), VerticaConnection, cl) as VerticaTable
        runClosure(parent, cl)

        return parent
    }

    /** Vertica table */
    VerticaTable verticaTable(String name,
                              @DelegatesTo(VerticaTable)
                              @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaTable']) Closure cl = null) {
        verticaTable(name, false, cl)
    }

    /** Vertica table */
    VerticaTable verticaTable(@DelegatesTo(VerticaTable)
                              @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaTable']) Closure cl) {
        verticaTable(null, false, cl)
    }

    /** SQLite connection */
    SQLiteConnection sqliteConnection(String name, Boolean registration,
                                      @DelegatesTo(SQLiteConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.sqlite.SQLiteConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.SQLITECONNECTION, name, registration) as SQLiteConnection
        runClosure(parent, cl)

        return parent
    }

    /** SQLite connection */
    SQLiteConnection sqliteConnection(String name,
                                      @DelegatesTo(SQLiteConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.sqlite.SQLiteConnection']) Closure cl = null) {
        sqliteConnection(name, false, cl)
    }

    /** SQLite connection */
    SQLiteConnection sqliteConnection(@DelegatesTo(SQLiteConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.sqlite.SQLiteConnection']) Closure cl) {
        sqliteConnection(null, false, cl)
    }

    /** SQLite current connection */
    SQLiteConnection sqliteConnection() {
        defaultJdbcConnection(RepositoryDatasets.SQLITETABLE) as SQLiteConnection
    }

    /** Use default SQLite connection for new datasets */
    SQLiteConnection useSQLiteConnection(SQLiteConnection connection) {
        useJdbcConnection(RepositoryDatasets.SQLITETABLE, connection) as SQLiteConnection
    }

    /** SQLite table */
    SQLiteTable sqliteTable(String name, Boolean registration,
                            @DelegatesTo(SQLiteTable)
                            @ClosureParams(value = SimpleType, options = ['getl.sqlite.SQLiteTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.SQLITETABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.SQLITETABLE), SQLiteConnection, cl) as SQLiteTable
        runClosure(parent, cl)

        return parent
    }

    /** SQLite table */
    SQLiteTable sqliteTable(String name,
                            @DelegatesTo(SQLiteTable)
                            @ClosureParams(value = SimpleType, options = ['getl.sqlite.SQLiteTable']) Closure cl = null) {
        sqliteTable(name, false, cl)
    }

    /** SQLite table */
    SQLiteTable sqliteTable(@DelegatesTo(SQLiteTable)
                            @ClosureParams(value = SimpleType, options = ['getl.sqlite.SQLiteTable']) Closure cl) {
        sqliteTable(null, false, cl)
    }

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(String name, Boolean registration,
                                          @DelegatesTo(NetsuiteConnection)
                                          @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.NETSUITECONNECTION, name, registration) as NetsuiteConnection
        runClosure(parent, cl)

        return parent
    }

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(String name,
                                          @DelegatesTo(NetsuiteConnection)
                                          @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteConnection']) Closure cl = null) {
        netsuiteConnection(name, false, cl)
    }

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(@DelegatesTo(NetsuiteConnection)
                                          @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteConnection']) Closure cl) {
        netsuiteConnection(null, false, cl)
    }

    /** NetSuite default connection */
    NetsuiteConnection netsuiteConnection() {
        defaultJdbcConnection(RepositoryDatasets.NETSUITETABLE) as NetsuiteConnection
    }

    /** Use default Netsuite connection for new datasets */
    NetsuiteConnection useNetsuiteConnection(NetsuiteConnection connection) {
        useJdbcConnection(RepositoryDatasets.NETSUITETABLE, connection) as NetsuiteConnection
    }

    /** Netsuite table */
    NetsuiteTable netsuiteTable(String name, Boolean registration,
                                @DelegatesTo(NetsuiteTable)
                                @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.NETSUITETABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.NETSUITETABLE), NetsuiteConnection, cl) as NetsuiteTable
        runClosure(parent, cl)

        return parent
    }

    /** Netsuite table */
    NetsuiteTable netsuiteTable(String name, @DelegatesTo(NetsuiteTable)
    @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteTable']) Closure cl = null) {
        netsuiteTable(name, false, cl)
    }

    /** Netsuite table */
    NetsuiteTable netsuiteTable(@DelegatesTo(NetsuiteTable)
                                @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteTable']) Closure cl) {
        netsuiteTable(null, false, cl)
    }

    /** Temporary database connection */
    TDS embeddedConnection(String name, Boolean registration,
                           @DelegatesTo(TDS)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDS']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.EMBEDDEDCONNECTION, name, registration) as TDS
        runClosure(parent, cl)

        return parent
    }

    /** Temporary database connection */
    TDS embeddedConnection(String name,
                           @DelegatesTo(TDS)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDS']) Closure cl = null) {
        embeddedConnection(name, false, cl)
    }

    /** Temporary database connection */
    TDS embeddedConnection(@DelegatesTo(TDS)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDS']) Closure cl) {
        embeddedConnection().tap(cl)
    }

    /** Temporary database default connection */
    TDS embeddedConnection() { options.defaultEmbeddedConnection }

    /** Use default temporary connection for new datasets */
    TDS useEmbeddedConnection(TDS connection) {
        useJdbcConnection(RepositoryDatasets.EMBEDDEDTABLE, connection) as TDS
    }

    /** Table with temporary database */
    TDSTable embeddedTable(String name, Boolean registration,
                           @DelegatesTo(TDSTable)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.EMBEDDEDTABLE, name, registration,
                defaultJdbcConnection(RepositoryDatasets.EMBEDDEDTABLE), TDS, cl) as TDSTable
        if ((name == null || registration) && parent.connection == null)
            parent.connection = options.defaultEmbeddedConnection
        runClosure(parent, cl)

        return parent
    }

    /** Table with temporary database */
    TDSTable embeddedTable(String name,
                           @DelegatesTo(TDSTable)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl = null) {
        embeddedTable(name, false, cl)
    }

    /** Table with temporary database */
    TDSTable embeddedTable(@DelegatesTo(TDSTable)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl) {
        embeddedTable(null, false, cl)
    }

    /** Table with temporary database */
    TDSTable embeddedTable() {
        embeddedTable(null, false, null)
    }

    /** Table with temporary database */
    TDSTable embeddedTableWithDataset(String name, Dataset sourceDataset,
                                      @DelegatesTo(TDSTable)
                                      @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl = null) {
        if (sourceDataset == null)
            throw new DslError(this, '#params.required', [param: 'sourceDataset', detail: 'embeddedTableWithDataset'])

        if (sourceDataset.field.isEmpty()) {
            sourceDataset.retrieveFields()
            if (sourceDataset.field.isEmpty())
                throw new DatasetError(sourceDataset, '#dataset.non_fields')
        }

        TDSTable parent = new TDSTable(connection: defaultJdbcConnection(RepositoryDatasets.EMBEDDEDTABLE)?:options.defaultEmbeddedConnection)
        parent.field = sourceDataset.field
        parent.resetFieldsTypeName()

        registerDatasetObject(parent, name, true)
        runClosure(parent, cl)

        return parent
    }

    /** Table with temporary database */
    TDSTable embeddedTableWithDataset(Dataset sourceDataset,
                                      @DelegatesTo(TDSTable)
                                      @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl = null) {
        embeddedTableWithDataset(null, sourceDataset, cl)
    }

    /** Use default JDBC connection for query */
    JDBCConnection useQueryConnection(JDBCConnection connection) {
        useJdbcConnection(RepositoryDatasets.QUERYDATASET, connection) as JDBCConnection
    }

    /** JDBC query dataset */
    QueryDataset query(String name, Boolean registration, JDBCConnection connection,
                       @DelegatesTo(QueryDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.QueryDataset']) Closure cl = null) {
        def parent = registerDataset(connection, RepositoryDatasets.QUERYDATASET, name, registration,
                defaultJdbcConnection(RepositoryDatasets.QUERYDATASET), JDBCConnection, cl) as QueryDataset
        runClosure(parent, cl)

        return parent
    }

    /** JDBC query dataset */
    QueryDataset query(String name, Boolean registration,
                       @DelegatesTo(QueryDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.QueryDataset']) Closure cl = null) {
        query(name, registration, null, cl)
    }

    /** JDBC query dataset */
    QueryDataset query(String name,
                       @DelegatesTo(QueryDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.QueryDataset']) Closure cl = null) {
        query(name, false, null, cl)
    }

    /** JDBC query dataset */
    QueryDataset query(@DelegatesTo(QueryDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.QueryDataset']) Closure cl = null) {
        query(null,  false, null, cl)
    }

    /** JDBC query dataset */
    QueryDataset query(JDBCConnection connection,
                       @DelegatesTo(QueryDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.QueryDataset']) Closure cl = null) {
        query(null, false, connection, cl)
    }

    /** JDBC query dataset */
    QueryDataset sqlQuery(JDBCConnection connection, String sql, Map vars = null) {
        def parent = query(connection)
        parent.query = sql
        if (vars != null)
            parent.setAttributes(vars)

        return parent
    }

    /** JDBC query dataset */
    QueryDataset sqlQuery(String sql, Map vars = null) {
        sqlQuery(null, sql, vars)
    }

    /** Return the first row from query */
    Map<String, Object> sqlQueryRow(JDBCConnection connection, String sql, Map vars = null) {
        def query = sqlQuery(connection, sql, vars)
        def rows = query.rows(limit: 1)
        return (!rows.isEmpty()) ? rows[0] : null
    }

    /** Return the first row from query */
    Map<String, Object> sqlQueryRow(String sql, Map vars = null) {
        def query = sqlQuery(sql, vars)
        def rows = query.rows(limit: 1)
        return (!rows.isEmpty()) ? rows[0] : null
    }

    /** CSV connection */
    CSVConnection csvConnection(String name, Boolean registration,
                                @DelegatesTo(CSVConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.csv.CSVConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.CSVCONNECTION, name, registration) as CSVConnection
        runClosure(parent, cl)

        return parent
    }

    /** CSV connection */
    CSVConnection csvConnection(String name,
                                @DelegatesTo(CSVConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.csv.CSVConnection']) Closure cl = null) {
        csvConnection(name, false, cl)
    }

    /** CSV connection */
    CSVConnection csvConnection(@DelegatesTo(CSVConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.csv.CSVConnection']) Closure cl) {
        csvConnection(null, false, cl)
    }

    /** CSV default connection */
    CSVConnection csvConnection() {
        defaultFileConnection(RepositoryDatasets.CSVDATASET) as CSVConnection
    }

    /** Use default CSV connection for new datasets */
    CSVConnection useCsvConnection(CSVConnection connection) {
        useFileConnection(RepositoryDatasets.CSVDATASET, connection) as CSVConnection
    }

    /** CSV delimiter file */
    CSVDataset csv(String name, Boolean registration,
                   @DelegatesTo(CSVDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.CSVDATASET, name, registration,
                defaultFileConnection(RepositoryDatasets.CSVDATASET), CSVConnection, cl) as CSVDataset
        runClosure(parent, cl)

        return parent
    }

    /** CSV delimiter file */
    CSVDataset csv(String name,
                   @DelegatesTo(CSVDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl = null) {
        csv(name, false, cl)
    }

    /** CSV delimiter file */
    CSVDataset csv(@DelegatesTo(CSVDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl) {
        csv(null, false, cl)
    }

    /**
     * Create and register CSV file on the specified dataset
     * @param name repository name
     * @param sourceDataset source dataset
     * @param cl initialization code
     * @return created dataset
     */
    CSVDataset csvWithDataset(String name, Dataset sourceDataset,
                              @DelegatesTo(CSVDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl = null) {
        if (sourceDataset == null)
            throw new DslError(this, '#params.required', [param: 'sourceDataset', detail: 'csvWithDataset'])

        def parent = registerDataset(null, RepositoryDatasets.CSVDATASET, name, true,
                defaultFileConnection(RepositoryDatasets.CSVDATASET), CSVConnection, cl) as CSVDataset

        if (sourceDataset.field.isEmpty()) {
            if (!sourceDataset.connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
                throw new NotSupportError(sourceDataset, 'read fields')

            sourceDataset.retrieveFields()
            if (sourceDataset.field.isEmpty())
                throw new DatasetError(sourceDataset, '#dataset.non_fields')
        }
        parent.field = sourceDataset.field
        parent.resetFieldsTypeName()
        runClosure(parent, cl)

        return parent
    }

    /**
     * Create CSV file on the specified dataset
     * @param sourceDataset source dataset
     * @param cl initialization code
     * @return created dataset
     */
    CSVDataset csvWithDataset(Dataset sourceDataset,
                              @DelegatesTo(CSVDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl = null) {
        csvWithDataset(null, sourceDataset, cl)
    }

    /** DBF connection */
    DBFConnection dbfConnection(String name, Boolean registration,
                                @DelegatesTo(DBFConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.dbf.DBFConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.DBFCONNECTION, name, registration) as DBFConnection
        runClosure(parent, cl)

        return parent
    }

    /** DBF connection */
    DBFConnection dbfConnection(String name,
                                @DelegatesTo(DBFConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.dbf.DBFConnection']) Closure cl = null) {
        dbfConnection(name, false, cl)
    }

    /** DBF connection */
    DBFConnection dbfConnection(@DelegatesTo(DBFConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.dbf.DBFConnection']) Closure cl) {
        dbfConnection(null, false, cl)
    }

    /** DBF default connection */
    DBFConnection dbfConnection() {
        defaultFileConnection(RepositoryDatasets.DBFDATASET) as DBFConnection
    }

    /** Use default DBF connection for new datasets */
    DBFConnection useDbfConnection(DBFConnection connection) {
        useFileConnection(RepositoryDatasets.DBFDATASET, connection) as DBFConnection
    }

    /** DBF file */
    DBFDataset dbf(String name, Boolean registration,
                   @DelegatesTo(DBFDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.dbf.DBFDataset']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.DBFDATASET, name, registration,
                defaultFileConnection(RepositoryDatasets.DBFDATASET), DBFConnection, cl) as DBFDataset
        runClosure(parent, cl)

        return parent
    }

    /** DBF file */
    DBFDataset dbf(String name,
                   @DelegatesTo(DBFDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.dbf.DBFDataset']) Closure cl = null) {
        dbf(name, false, cl)
    }

    /** DBF file */
    DBFDataset dbf(@DelegatesTo(DBFDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.dbf.DBFDataset']) Closure cl) {
        dbf(null, false, cl)
    }

    /**
     * Create and register DBF file on the specified dataset
     * @param name repository name
     * @param sourceDataset source dataset
     * @param cl initialization code
     * @return created dataset
     */
    DBFDataset dbfWithDataset(String name, Dataset sourceDataset,
                              @DelegatesTo(DBFDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.dbf.DBFDataset']) Closure cl = null) {
        if (sourceDataset == null)
            throw new DslError(this, '#params.required', [param: 'sourceDataset', detail: 'dbfWithDataset'])

        def parent = registerDataset(null, RepositoryDatasets.DBFDATASET, name, true,
                defaultFileConnection(RepositoryDatasets.DBFDATASET), DBFConnection, cl) as DBFDataset

        if (sourceDataset.field.isEmpty()) {
            if (!sourceDataset.connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
                throw new NotSupportError(sourceDataset, 'read fields')

            sourceDataset.retrieveFields()
            if (sourceDataset.field.isEmpty())
                throw new DatasetError(sourceDataset, '#dataset.non_fields')
        }
        parent.field = sourceDataset.field
        parent.resetFieldsTypeName()
        runClosure(parent, cl)

        return parent
    }

    /**
     * Create DBF file on the specified dataset
     * @param sourceDataset source dataset
     * @param cl initialization code
     * @return created dataset
     */
    DBFDataset dbfWithDataset(Dataset sourceDataset,
                              @DelegatesTo(DBFDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.dbf.DBFDataset']) Closure cl = null) {
        dbfWithDataset(null, sourceDataset, cl)
    }

    /** Excel connection */
    ExcelConnection excelConnection(String name, Boolean registration,
                                    @DelegatesTo(ExcelConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.EXCELCONNECTION, name, registration) as ExcelConnection
        runClosure(parent, cl)

        return parent
    }

    /** Excel connection */
    ExcelConnection excelConnection(String name,
                                    @DelegatesTo(ExcelConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelConnection']) Closure cl = null) {
        excelConnection(name, false, cl)
    }

    /** Excel connection */
    ExcelConnection excelConnection(@DelegatesTo(ExcelConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelConnection']) Closure cl) {
        excelConnection(null, false, cl)
    }

    /** Excel default connection */
    ExcelConnection excelConnection() {
        defaultFileConnection(RepositoryDatasets.EXCELDATASET) as ExcelConnection
    }

    /** Use default Excel connection for new datasets */
    ExcelConnection useExcelConnection(ExcelConnection connection) {
        useFileConnection(RepositoryDatasets.EXCELDATASET, connection) as ExcelConnection
    }

    /** Excel file */
    ExcelDataset excel(String name, Boolean registration,
                       @DelegatesTo(ExcelDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelDataset']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.EXCELDATASET, name, registration,
                defaultFileConnection(RepositoryDatasets.EXCELDATASET), ExcelConnection, cl) as ExcelDataset
        if (parent.connection == null && (name == null || registration))
            parent.connection = new ExcelConnection()

        runClosure(parent, cl)

        return parent
    }

    /** Excel file */
    ExcelDataset excel(String name,
                       @DelegatesTo(ExcelDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelDataset']) Closure cl = null) {
        excel(name, false, cl)
    }

    /** Excel file */
    ExcelDataset excel(@DelegatesTo(ExcelDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelDataset']) Closure cl) {
        excel(null, false, cl)
    }

    /** JSON connection */
    JSONConnection jsonConnection(String name, Boolean registration,
                                  @DelegatesTo(JSONConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.json.JSONConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.JSONCONNECTION, name, registration) as JSONConnection
        runClosure(parent, cl)

        return parent
    }

    /** JSON connection */
    JSONConnection jsonConnection(String name,
                                  @DelegatesTo(JSONConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.json.JSONConnection']) Closure cl = null) {
        jsonConnection(name, false, cl)
    }

    /** JSON connection */
    JSONConnection jsonConnection(@DelegatesTo(JSONConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.json.JSONConnection']) Closure cl) {
        jsonConnection(null, false, cl)
    }

    /** JSON default connection */
    JSONConnection jsonConnection() {
        defaultFileConnection(RepositoryDatasets.JSONDATASET) as JSONConnection
    }

    /** Use default Json connection for new datasets */
    JSONConnection useJsonConnection(JSONConnection connection) {
        useFileConnection(RepositoryDatasets.JSONDATASET, connection) as JSONConnection
    }

    /** JSON file */
    JSONDataset json(String name, Boolean registration,
                     @DelegatesTo(JSONDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.json.JSONDataset']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.JSONDATASET, name, registration,
                defaultFileConnection(RepositoryDatasets.JSONDATASET), JSONConnection, cl) as JSONDataset
        if (parent.connection == null && (name == null || registration))
            parent.connection = new JSONConnection()

        runClosure(parent, cl)

        return parent
    }

    /** JSON file */
    JSONDataset json(String name,
                     @DelegatesTo(JSONDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.json.JSONDataset']) Closure cl = null) {
        json(name, false, cl)
    }

    /** JSON file */
    JSONDataset json(@DelegatesTo(JSONDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.json.JSONDataset']) Closure cl) {
        json(null, false, cl)
    }

    /** XML connection */
    XMLConnection xmlConnection(String name, Boolean registration,
                                @DelegatesTo(XMLConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.xml.XMLConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.XMLCONNECTION, name, registration) as XMLConnection
        runClosure(parent, cl)

        return parent
    }

    /** XML connection */
    XMLConnection xmlConnection(String name,
                                @DelegatesTo(XMLConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.xml.XMLConnection']) Closure cl = null) {
        xmlConnection(name, false, cl)
    }

    /** XML connection */
    XMLConnection xmlConnection(@DelegatesTo(XMLConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.xml.XMLConnection']) Closure cl) {
        xmlConnection(null, false, cl)
    }

    /** XML default connection */
    XMLConnection xmlConnection() {
        defaultFileConnection(RepositoryDatasets.XMLDATASET) as XMLConnection
    }

    /** Use default XML connection for new datasets */
    XMLConnection useXmlConnection(XMLConnection connection) {
        useFileConnection(RepositoryDatasets.XMLDATASET, connection) as XMLConnection
    }

    /** XML file */
    XMLDataset xml(String name, Boolean registration,
                   @DelegatesTo(XMLDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.xml.XMLDataset']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.XMLDATASET, name, registration,
                defaultFileConnection(RepositoryDatasets.XMLDATASET), XMLConnection, cl) as XMLDataset
        if (parent.connection == null && (name == null || registration))
            parent.connection = new XMLConnection()

        runClosure(parent, cl)

        return parent
    }

    /** XML file */
    XMLDataset xml(String name,
                   @DelegatesTo(XMLDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.xml.XMLDataset']) Closure cl = null) {
        xml(name, false, cl)
    }

    /** XML file */
    XMLDataset xml(@DelegatesTo(XMLDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.xml.XMLDataset']) Closure cl) {
        xml(null, false, cl)
    }

    /** YAML connection */
    YAMLConnection yamlConnection(String name, Boolean registration,
                                  @DelegatesTo(YAMLConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.yaml.YAMLConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.YAMLCONNECTION, name, registration) as YAMLConnection
        runClosure(parent, cl)

        return parent
    }

    /** YAML connection */
    YAMLConnection yamlConnection(String name,
                                  @DelegatesTo(YAMLConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.yaml.YAMLConnection']) Closure cl = null) {
        yamlConnection(name, false, cl)
    }

    /** YAML connection */
    YAMLConnection yamlConnection(@DelegatesTo(YAMLConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.yaml.YAMLConnection']) Closure cl) {
        yamlConnection(null, false, cl)
    }

    /** YAML default connection */
    YAMLConnection yamlConnection() {
        defaultFileConnection(RepositoryDatasets.YAMLDATASET) as YAMLConnection
    }

    /** Use default Yaml connection for new datasets */
    YAMLConnection useYamlConnection(YAMLConnection connection) {
        useFileConnection(RepositoryDatasets.YAMLDATASET, connection) as YAMLConnection
    }

    /** YAML file */
    YAMLDataset yaml(String name, Boolean registration,
                     @DelegatesTo(YAMLDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.yaml.YAMLDataset']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.YAMLDATASET, name, registration,
                defaultFileConnection(RepositoryDatasets.YAMLDATASET), YAMLConnection, cl) as YAMLDataset
        if (parent.connection == null && (name == null || registration))
            parent.connection = new YAMLConnection()

        runClosure(parent, cl)

        return parent
    }

    /** YAML file */
    YAMLDataset yaml(String name,
                     @DelegatesTo(YAMLDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.yaml.YAMLDataset']) Closure cl = null) {
        yaml(name, false, cl)
    }

    /** YAML file */
    YAMLDataset yaml(@DelegatesTo(YAMLDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.yaml.YAMLDataset']) Closure cl) {
        yaml(null, false, cl)
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(String name, Boolean registration,
                                              @DelegatesTo(SalesForceConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.SALESFORCECONNECTION, name, registration) as SalesForceConnection
        runClosure(parent, cl)

        return parent
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(String name,
                                              @DelegatesTo(SalesForceConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceConnection']) Closure cl = null) {
        salesforceConnection(name, false, cl)
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(@DelegatesTo(SalesForceConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceConnection']) Closure cl) {
        salesforceConnection(null, false, cl)
    }

    /** SalesForce default connection */
    SalesForceConnection salesforceConnection() {
        defaultOtherConnection(RepositoryDatasets.SALESFORCEDATASET) as SalesForceConnection
    }

    /** Use default SalesForce connection for new datasets */
    SalesForceConnection useSalesforceConnection(SalesForceConnection connection) {
        useOtherConnection(RepositoryDatasets.SALESFORCEDATASET, connection) as SalesForceConnection
    }

    /** SalesForce table */
    SalesForceDataset salesforce(String name, Boolean registration,
                                 @DelegatesTo(SalesForceDataset)
                                 @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceDataset']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.SALESFORCEDATASET, name, registration,
                defaultOtherConnection(RepositoryDatasets.SALESFORCEDATASET), SalesForceConnection, cl) as SalesForceDataset
        runClosure(parent, cl)

        return parent
    }

    /** SalesForce table */
    SalesForceDataset salesforce(String name,
                                 @DelegatesTo(SalesForceDataset)
                                 @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceDataset']) Closure cl = null) {
        salesforce(name, false, cl)
    }

    /** SalesForce table */
    SalesForceDataset salesforce(@DelegatesTo(SalesForceDataset)
                                 @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceDataset']) Closure cl) {
        salesforce(null, false, cl)
    }

    /** SalesForce query */
    SalesForceQueryDataset salesforceQuery(String name, Boolean registration,
                                           @DelegatesTo(SalesForceQueryDataset)
                                           @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceQueryDataset']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.SALESFORCEQUERYDATASET, name, registration,
                defaultOtherConnection(RepositoryDatasets.SALESFORCEDATASET), SalesForceConnection, cl) as SalesForceQueryDataset
        runClosure(parent, cl)

        return parent
    }

    /** SalesForce query */
    SalesForceQueryDataset salesforceQuery(String name,
                                           @DelegatesTo(SalesForceQueryDataset)
                                           @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceQueryDataset']) Closure cl = null) {
        salesforceQuery(name, false, cl)
    }

    /** SalesForce query */
    SalesForceQueryDataset salesforceQuery(@DelegatesTo(SalesForceQueryDataset)
                                           @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceQueryDataset']) Closure cl) {
        salesforceQuery(null, false, cl)
    }

    /** Kafka connection */
    KafkaConnection kafkaConnection(String name, Boolean registration,
                                    @DelegatesTo(KafkaConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.kafka.KafkaConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.KAFKACONNECTION, name, registration) as KafkaConnection
        runClosure(parent, cl)

        return parent
    }

    /** Kafka connection */
    KafkaConnection kafkaConnection(String name,
                                    @DelegatesTo(KafkaConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.kafka.KafkaConnection']) Closure cl = null) {
        kafkaConnection(name, false, cl)
    }

    /** Kafka connection */
    KafkaConnection kafkaConnection(@DelegatesTo(KafkaConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.kafka.KafkaConnection']) Closure cl) {
        kafkaConnection(null, false, cl)
    }

    /** Kafka default connection */
    KafkaConnection kafkaConnection() {
        defaultOtherConnection(RepositoryDatasets.KAFKADATASET) as KafkaConnection
    }

    /** Use default Kafka connection for new datasets */
    KafkaConnection useKafkaConnection(KafkaConnection connection) {
        useOtherConnection(RepositoryDatasets.KAFKADATASET, connection) as KafkaConnection
    }

    /** Kafka dataset */
    KafkaDataset kafka(String name, Boolean registration,
                       @DelegatesTo(KafkaDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.kafka.KafkaDataset']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.KAFKADATASET, name, registration,
                defaultOtherConnection(RepositoryDatasets.KAFKADATASET), KafkaConnection, cl) as KafkaDataset
        runClosure(parent, cl)

        return parent
    }

    /** Kafka dataset */
    KafkaDataset kafka(String name,
                       @DelegatesTo(KafkaDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.kafka.KafkaDataset']) Closure cl = null) {
        kafka(name, false, cl)
    }

    /** Kafka dataset */
    KafkaDataset kafka(@DelegatesTo(KafkaDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.kafka.KafkaDataset']) Closure cl) {
        kafka(null, false, cl)
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(String name, Boolean registration,
                          @DelegatesTo(TFS)
                          @ClosureParams(value = SimpleType, options = ['getl.tfs.TFS']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.CSVTEMPCONNECTION, name, registration) as TFS
        runClosure(parent, cl)

        return parent
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(String name,
                          @DelegatesTo(TFS)
                          @ClosureParams(value = SimpleType, options = ['getl.tfs.TFS']) Closure cl = null) {
        csvTempConnection(name, false, cl)
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(@DelegatesTo(TFS)
                          @ClosureParams(value = SimpleType, options = ['getl.tfs.TFS']) Closure cl) {
        csvTempConnection(null, false, cl)
    }

    /** Temporary CSV file current connection */
    TFS csvTempConnection() {
        TFS.storage
    }

    /** Use default CSV temporary connection for new datasets */
    TFS useCsvTempConnection(TFS connection = TFS.storage) {
        useFileConnection(RepositoryDatasets.CSVTEMPDATASET, connection) as TFS
    }

    /**
     * Temporary CSV file
     * @param name repository name
     * @param registration register in the repository
     * @param cl initialization code
     * @return dataset
     */
    TFSDataset csvTemp(String name, Boolean registration,
                       @DelegatesTo(TFSDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.CSVTEMPDATASET, name, registration,
                defaultFileConnection(RepositoryDatasets.CSVTEMPDATASET), TFS, cl) as TFSDataset
        if ((name == null || registration) && parent.connection == null)
            parent.connection = TFS.storage
        runClosure(parent, cl)

        return parent
    }

    /**
     * Temporary CSV file
     * @param name repository name
     * @param cl initialization code
     * @return dataset
     */
    TFSDataset csvTemp(String name,
                       @DelegatesTo(TFSDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl = null) {
        csvTemp(name, false, cl)
    }

    /**
     * Temporary CSV file
     * @param cl initialization code
     * @return dataset
     */
    TFSDataset csvTemp(@DelegatesTo(TFSDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl) {
        csvTemp(null, false, cl)
    }

    /**
     * Temporary CSV file
     * @return dataset
     */
    TFSDataset csvTemp() {
        csvTemp(null, false, null)
    }

    /**
     * Create and register temporary CSV file on the specified dataset
     * @param name repository name
     * @param sourceDataset source dataset
     * @param cl initialization code
     * @return created dataset
     */
    TFSDataset csvTempWithDataset(String name, Dataset sourceDataset,
                                  @DelegatesTo(TFSDataset)
                                  @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl = null) {
        if (sourceDataset == null)
            throw new DslError(this, '#params.required', [param: 'sourceDataset', detail: 'csvTempWithDataset'])

        if (sourceDataset.field.isEmpty()) {
            if (!sourceDataset.connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
                throw new NotSupportError(sourceDataset, 'read fields')

            sourceDataset.retrieveFields()
            if (sourceDataset.field.isEmpty())
                throw new DatasetError(sourceDataset, '#dataset.non_fields')
        }

        def parent = registerDataset(null, RepositoryDatasets.CSVTEMPDATASET, name, (name != null),
                defaultFileConnection(RepositoryDatasets.CSVTEMPDATASET), TFS, cl) as TFSDataset
        if (parent.connection == null)
            parent.connection = TFS.storage
        parent.autoSchema = false
        parent.field = sourceDataset.field
        parent.resetFieldsTypeName()
        sourceDataset.prepareCsvTempFile(parent)
        runClosure(parent, cl)

        return parent
    }

    /**
     * Create temporary CSV file on the specified dataset
     * @param sourceDataset source dataset
     * @param cl initialization code
     * @return created dataset
     */
    TFSDataset csvTempWithDataset(Dataset sourceDataset,
                                  @DelegatesTo(TFSDataset)
                                  @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl = null) {
        csvTempWithDataset(null, sourceDataset, cl)
    }

    /** SQL scripter */
    SQLScripter sql(JDBCConnection connection,
                    @DelegatesTo(SQLScripter)
                    @ClosureParams(value = SimpleType, options = ['getl.jdbc.SQLScripter']) Closure cl = null) {
        def parent = new SQLScripter()
        parent.dslCreator = this
        parent.debugMode = BoolUtils.IsValue(options.sqlScripterDebug)
        def owner = DetectClosureDelegate(cl, true)

        if (connection != null)
            parent.connection = connection
        else if (owner instanceof JDBCConnection)
            parent.connection = owner as JDBCConnection
        else if (owner instanceof JDBCDataset)
            parent.connection = (owner as Dataset).connection
        else
            parent.connection = defaultJdbcConnection(RepositoryDatasets.QUERYDATASET)

        parent.logEcho = _logOpts.sqlEchoLogLevel.toString()
        parent.extVars = configVars

        if (owner instanceof GetlRepository) {
            def rep = owner as GetlRepository
            if (rep.dslNameObject != null)
                parent.extVars.put('~dsl_name~', rep.dslNameObject)

            if (owner instanceof Dataset) {
                def ds = owner as Dataset
                if (ds.objectName != null)
                    parent.extVars.put('~short_name~', ds.objectName)
                if (ds.objectFullName != null)
                    parent.extVars.put('~full_name~', ds.objectFullName)

                if (owner instanceof TableDataset) {
                    def tab = owner as TableDataset
                    if (tab.dbName() != null)
                        parent.extVars.put('~db~', tab.dbName())
                    if (tab.schemaName() != null)
                        parent.extVars.put('~schema~', tab.schemaName())
                    if (tab.tableName != null)
                        parent.extVars.put('~table~', tab.tableName)
                }
            }
        }

        def pt = startProcess("Execution SQL script${(parent.connection != null) ? ' on [' + parent.connection + ']' : ''}")
        runClosure(parent, cl)
        finishProcess(pt, parent.rowCount)

        return parent
    }

    /** SQL scripter */
    SQLScripter sql(@DelegatesTo(SQLScripter)
                    @ClosureParams(value = SimpleType, options = ['getl.jdbc.SQLScripter']) Closure cl) {
        sql(null, cl)
    }

    /** Exist file manager */
    Manager filemanager(String name,
                        @DelegatesTo(Manager)
                        @ClosureParams(value = SimpleType, options = ['getl.files.Manager']) Closure cl = null) {
        if (name == null)
            throw new DslError(this, '#params.required', [param: 'name', detail: 'filemanager'])

        def parent = registerFileManager(null, name, false) as Manager

        if (cl != null) {
            def pt = startProcess("Do commands on [$parent]", 'command')
            try {
                runClosure(parent, cl)
            }
            finally {
                if (parent.connected) parent.disconnect()
            }
            pt.name = "Do commands on [$parent]"
            finishProcess(pt)
        }

        return parent
    }

    /**
     * Clone file manager
     * @param man original file manager to clone
     * @return cloned file manager
     */
    Manager cloneFilemanager(Manager man) {
        if (man == null)
            throw new DslError(this, '#params.required', [param: 'manager', detail: 'cloneFilemanager'])

        return man.cloneManager(null, this)
    }

    /**
     * Clone file manager
     * @param newName repository name for cloned file manager
     * @param man original file manager to clone
     * @param cl cloned file manager processing code
     * @return cloned file manager
     */
    Manager cloneFilemanager(String newName, Manager man,
                             @DelegatesTo(Manager)
                             @ClosureParams(value = SimpleType, options = ['getl.files.Manager']) Closure cl = null) {
        def parent = cloneFilemanager(man)
        registerFileManagerObject(parent, newName)
        runClosure(parent, cl)

        return parent
    }

    /** Process local file system */
    FileManager files(String name, Boolean registration,
                      @DelegatesTo(FileManager)
                      @ClosureParams(value = SimpleType, options = ['getl.files.FileManager']) Closure cl = null) {
        def parent = registerFileManager(RepositoryFilemanagers.FILEMANAGER, name, registration) as FileManager
        if (parent.rootPath == null) parent.rootPath = new File('.').canonicalPath
        if (cl != null) {
            def pt = startProcess("Do commands on [$parent]", 'command')
            try {
                runClosure(parent, cl)
            }
            finally {
                if (parent.connected) parent.disconnect()
            }
            pt.name = "Do commands on [$parent]"
            finishProcess(pt)
        }

        return parent
    }

    /** Process local file system */
    FileManager files(String name,
                      @DelegatesTo(FileManager)
                      @ClosureParams(value = SimpleType, options = ['getl.files.FileManager']) Closure cl = null) {
        files(name, false, cl)
    }

    /** Process local file system */
    FileManager files(@DelegatesTo(FileManager)
                      @ClosureParams(value = SimpleType, options = ['getl.files.FileManager']) Closure cl) {
        files(null, false, cl)
    }

    /** Process local file system */
    ResourceManager resourceFiles(String name, Boolean registration,
                      @DelegatesTo(ResourceManager)
                      @ClosureParams(value = SimpleType, options = ['getl.files.ResourceManager']) Closure cl = null) {
        def parent = registerFileManager(RepositoryFilemanagers.RESOURCEMANAGER, name, registration) as ResourceManager
        if (cl != null) {
            try {
                runClosure(parent, cl)
            }
            finally {
                if (parent.connected) parent.disconnect()
            }
        }

        return parent
    }

    /** Process local file system */
    ResourceManager resourceFiles(String name,
                          @DelegatesTo(ResourceManager)
                          @ClosureParams(value = SimpleType, options = ['getl.files.ResourceManager']) Closure cl = null) {
        resourceFiles(name, false, cl)
    }

    /** Process local file system */
    ResourceManager resourceFiles(@DelegatesTo(ResourceManager)
                                  @ClosureParams(value = SimpleType, options = ['getl.files.ResourceManager']) Closure cl) {
        resourceFiles(null, false, cl)
    }

    /** Process ftp file system */
    FTPManager ftp(String name, Boolean registration,
                   @DelegatesTo(FTPManager)
                   @ClosureParams(value = SimpleType, options = ['getl.files.FTPManager']) Closure cl = null) {
        def parent = registerFileManager(RepositoryFilemanagers.FTPMANAGER, name, registration) as FTPManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.currentPath()
        if (cl != null) {
            def pt = startProcess("Do commands on [$parent]", 'command')
            try {
                runClosure(parent, cl)
            }
            finally {
                if (parent.connected) parent.disconnect()
            }
            pt.name = "Do commands on [$parent]"
            finishProcess(pt)
        }

        return parent
    }

    /** Process ftp file system */
    FTPManager ftp(String name,
                   @DelegatesTo(FTPManager)
                   @ClosureParams(value = SimpleType, options = ['getl.files.FTPManager']) Closure cl = null) {
        ftp(name, false, cl)
    }

    /** Process ftp file system */
    FTPManager ftp(@DelegatesTo(FTPManager)
                   @ClosureParams(value = SimpleType, options = ['getl.files.FTPManager']) Closure cl) {
        ftp(null, false, cl)
    }

    /** Process sftp file system */
    SFTPManager sftp(String name, Boolean registration,
                     @DelegatesTo(SFTPManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.SFTPManager']) Closure cl = null) {
        def parent = registerFileManager(RepositoryFilemanagers.SFTPMANAGER, name, registration) as SFTPManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.currentPath()
        if (cl != null) {
            def pt = startProcess("Do commands on [$parent]", 'command')
            try {
                runClosure(parent, cl)
            }
            finally {
                if (parent.connected) parent.disconnect()
            }
            pt.name = "Do commands on [$parent]"
            finishProcess(pt)
        }

        return parent
    }

    /** Process sftp file system */
    SFTPManager sftp(String name,
                     @DelegatesTo(SFTPManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.SFTPManager']) Closure cl = null) {
        sftp(name, false, cl)
    }

    /** Process sftp file system */
    SFTPManager sftp(@DelegatesTo(SFTPManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.SFTPManager']) Closure cl) {
        sftp(null, false, cl)
    }

    /** Process sftp file system */
    HDFSManager hdfs(String name, Boolean registration,
                     @DelegatesTo(HDFSManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.HDFSManager']) Closure cl = null) {
        def parent = registerFileManager(RepositoryFilemanagers.HDFSMANAGER, name, registration) as HDFSManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.currentPath()
        if (cl != null) {
            def pt = startProcess("Do commands on [$parent]", 'command')
            try {
                runClosure(parent, cl)
            }
            finally {
                if (parent.connected) parent.disconnect()
            }
            pt.name = "Do commands on [$parent]"
            finishProcess(pt)
        }

        return parent
    }

    /** Process sftp file system */
    HDFSManager hdfs(String name,
                     @DelegatesTo(HDFSManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.HDFSManager']) Closure cl = null) {
        hdfs(name, false, cl)
    }

    /** Process sftp file system */
    HDFSManager hdfs(@DelegatesTo(HDFSManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.HDFSManager']) Closure cl) {
        hdfs(null, false, cl)
    }

    /** Run code in multithread mode */
    Executor thread(@DelegatesTo(Executor)
                    @ClosureParams(value = SimpleType, options = ['getl.proc.Executor']) Closure cl) {
        def parent = new Executor(abortOnError: true, dslCreator: this)
        parent.dumpErrors = logging.logPrintStackTraceError
        parent.logErrors = logging.logPrintStackTraceError
        parent.debugElementOnError = logging.logPrintStackTraceError

        if (_langOpts.processControlDataset != null && _langOpts.checkProcessForThreads) {
            def allowRun = {
                return allowProcess()
            }
            parent.validAllowRun(allowRun)
        }

        def pt = startProcess('Execution threads', 'thread')
        runClosure(parent, cl)
        finishProcess(pt)

        return parent
    }

    /** Run code in multithread mode */
    EMailer mail(@DelegatesTo(EMailer)
                 @ClosureParams(value = SimpleType, options = ['getl.utils.EMailer']) Closure cl) {
        def parent = new EMailer()
        parent.dslCreator = this
        def pt = startProcess('Mailer', 'command')
        runClosure(parent, cl)
        finishProcess(pt)

        return parent
    }

    /**
     * Processing text file
     * @param file file object or string file name
     * @param cl process code
     * @return text file specification
     */
    FileTextSpec textFile(def file,
                    @DelegatesTo(FileTextSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.opts.FileTextSpec']) Closure cl = null) {
        def parent = new FileTextSpec(this)
        if (file != null) {
            parent.fileName = (file instanceof File) ? ((file as File).path) : file.toString()
        }
        def pt = startProcess("Processing text file${(parent.fileName != null) ? (' "' + parent.filePath() + '"') : ''}", 'byte')
        pt.objectName = 'byte'
        runClosure(parent, cl)
        parent.save()
        pt.name = "Processing text file${(parent.fileName != null) ? (' "' + parent.filePath() + '"') : ''}"
        finishProcess(pt, parent.countBytes)

        return parent
    }

    /** Processing text file */
    FileTextSpec textFile(@DelegatesTo(FileTextSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.opts.FileTextSpec']) Closure cl) {
        textFile(null, cl)
    }

    /** File path parser */
    Path filePath(@DelegatesTo(Path)
                  @ClosureParams(value = SimpleType, options = ['getl.utils.Path']) Closure cl) {
        def parent = new Path()
        parent.dslCreator = this
        runClosure(parent, cl)

        return parent
    }

    /** File path parser */
    Path filePath(String mask) {
        def parent = new Path(mask)
        parent.dslCreator = this
        return parent
    }

    /** File path parser */
    Path filePath(String mask,
                  @DelegatesTo(Path)
                  @ClosureParams(value = SimpleType, options = ['getl.utils.Path']) Closure cl) {
        def parent = new Path(mask)
        parent.dslCreator = this
        runClosure(parent, cl)
        return parent
    }

    /** Incremental history point manager */
    HistoryPointManager historypoint(String name = null, Boolean registration = false,
                                  @DelegatesTo(HistoryPointManager)
                                  @ClosureParams(value = SimpleType, options = ['getl.jdbc.HistoryPointManager']) Closure cl = null) {
        def parent = registerHistoryPoint(name, registration)
        runClosure(parent, cl)

        return parent
    }

    /** Incremental history point manager */
    HistoryPointManager historypoint(String name,
                                     @DelegatesTo(HistoryPointManager)
                                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.HistoryPointManager'])
                                             Closure cl) {
        historypoint(name, false, cl)
    }

                                     /** Incremental history point manager */
    HistoryPointManager historypoint(@DelegatesTo(HistoryPointManager)
                                  @ClosureParams(value = SimpleType, options = ['getl.jdbc.HistoryPointManager'])
                                             Closure cl) {
        historypoint(null, false, cl)
    }

    /**
     * Clone history point manager
     * @param point original history point manager to clone
     * @return cloned history point manager
     */
    HistoryPointManager cloneHistorypoint(HistoryPointManager point) {
        if (point == null)
            throw new DslError(this, '#params.required', [param: 'point', detail: 'cloneHistorypoint'])

        return point.cloneHistoryPointManager(null, null, this) as HistoryPointManager
    }

    /**
     * Clone history point manager
     * @param point original history point manager to clone
     * @return cloned history point manager
     */
    HistoryPointManager cloneHistorypointConnection(HistoryPointManager point) {
        if (point == null)
            throw new DslError(this, '#params.required', [param: 'point', detail: 'cloneHistorypointConnection'])

        return point.cloneHistoryPointManager(point.historyTable?.connection?.cloneConnection() as JDBCConnection,
                null, this) as HistoryPointManager
    }

    /**
     * Clone history point manager
     * @param newName repository name for cloned history point manager
     * @param point original history point manager to clone
     * @param cl cloned history point manager processing code
     * @return cloned history point manager
     */
    HistoryPointManager cloneHistorypoint(String newName, HistoryPointManager point,
                                       @DelegatesTo(HistoryPointManager)
                                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.HistoryPointManager'])
                                               Closure cl = null) {
        def parent = cloneHistorypoint(point)
        registerHistoryPointObject(parent, newName)
        runClosure(parent, cl)

        return parent
    }

    /** Sequence */
    Sequence sequence(String name = null, Boolean registration = false, JDBCConnection connection = null,
                      @DelegatesTo(Sequence)
                          @ClosureParams(value = SimpleType, options = ['getl.jdbc.Sequence']) Closure cl = null) {
        def parent = registerSequence(connection, name, registration, cl)
        runClosure(parent, cl)

        return parent
    }

    /** Sequence */
    Sequence sequence(String name, Boolean registration = false,
                      @DelegatesTo(Sequence)
                      @ClosureParams(value = SimpleType, options = ['getl.jdbc.Sequence']) Closure cl) {
        sequence(name, registration, null, cl)
    }

    /** Sequence */
    Sequence sequence(@DelegatesTo(Sequence)
                      @ClosureParams(value = SimpleType, options = ['getl.jdbc.Sequence']) Closure cl) {
        sequence(null, false, null, cl)
    }

    /**
     * Clone sequence
     * @param seq sequence to clone
     * @param con used connection for new sequence
     * @return clone sequence
     */
    Sequence cloneSequence(Sequence seq, JDBCConnection con = null) {
        if (seq == null)
            throw new DslError(this, '#params.required', [param: 'sequence', detail: 'cloneSequence'])

        return seq.cloneSequence(con, null, this) as Sequence
    }

    /**
     * Clone sequence
     * @param newName repository name for cloned sequence
     * @param seq sequence to clone
     * @param con used connection for new sequence
     * @param cl cloned sequence code
     * @return clone sequence
     */
    Sequence cloneSequence(String newName, Sequence seq, JDBCConnection con = null,
                           @DelegatesTo(Sequence)
                           @ClosureParams(value = SimpleType, options = ['getl.jdbc.Sequence']) Closure cl = null) {
        def parent = cloneSequence(seq, con)
        registerSequenceObject(parent, newName)
        runClosure(parent, cl)

        return parent
    }

    /** File system manager */
    private FilemanSpec _fileman
    /** File system manager */
    FilemanSpec getFileman() { _fileman }

    /** Test case instance */
    private GetlTest _testCase

    /** Run test case code */
    GetlTest testCase(@DelegatesTo(GetlTest)
                            @ClosureParams(value = SimpleType, options = ['getl.test.GetlTest']) Closure cl) {
        def parent = _testCase?:new GetlTest()
        runClosure(parent, cl)
        return parent
    }

    /** Pause current thread process
     * @param timeout how much to wait in ms
     */
    static void pause(Long timeout) {
        def thread = Thread.currentThread()
        synchronized (thread) {
            thread.wait(timeout)
        }
    }

    /**
     * Pause main thread process
     * @param message pause text notification
     */
    @Synchronized
    void pressAnyKey(String message = null) {
        System.in.withReader {
            print (message?:'Press any key to continue ...')
            println it.readLine()
        }
    }

    /**
     * Run code if working in unit test mode
     * @param cl user code
     */
    void ifUnitTestMode(@DelegatesTo(GetlTest)
                        @ClosureParams(value = SimpleType, options = ['getl.test.GetlTest']) Closure cl) {
        ifUnitTestMode(null, cl)
    }

    /**
     * Run code if working in unit test mode in specified environment
     * @param env environment
     * @param cl user code
     */
    void ifUnitTestMode(String env,
                        @DelegatesTo(GetlTest)
                        @ClosureParams(value = SimpleType, options = ['getl.test.GetlTest']) Closure cl) {
        if (this.unitTestMode && (env == null || configuration.environment == env))
            testCase(cl)
    }

    /**
     * Run code if working in application mode
     * @param cl
     */
    void ifRunAppMode(Closure cl) {
        if (!this.unitTestMode)
            cl.call()
    }

    void ifInitMode(Closure cl) {
        if (isInitMode)
            cl.call()
    }

    /** Converting code variables to a map */
    Map<String, Object> toVars(Closure cl) {
        return MapUtils.Closure2Map(configuration.environment?:'prod', cl)
    }

    /**
     * Format with group delimiter
     * @param value number value
     * @return formatted string
     */
    static String Numeric2String(Number value) {
        return StringUtils.WithGroupSeparator(value)
    }

    /**
     * Convert length to byte measurements
     * @param bytes size of bytes
     * @return formatted string
     */
    static String Size2String(Long bytes) {
        return FileUtils.SizeBytes(bytes)
    }

    /**
     * Attach local data for dataset processing
     * @param dataset destination dataset
     * @param data local data
     * @return destination dataset
     */
    Dataset attachToDataset(Object data, Dataset dataset) {
        if (dataset == null)
            throw new DslError(this, '#params.required', [param: 'dataset', detail: 'attachToDataset'])
        if (!(dataset instanceof AttachData))
            throw new DslError(this, '#dsl.invalid_instance_object', [name: dataset.dslNameObject?:dataset.toString(), type: 'attachment dataset'])

        (dataset as AttachData).localDatasetData = data

        return dataset as Dataset
    }

    /**
     * Attach local data for dataset processing
     * @param datasetName destination dataset
     * @param data local data
     * @return destination dataset
     */
    Dataset attachToDataset(Object data, String datasetName) {
        if (datasetName == null)
            throw new DslError(this, '#params.required', [param: 'datasetName', detail: 'attachToDataset'])

        def dataset = dataset(datasetName)
        return attachToDataset(data, dataset)
    }

    /**
     * Attach local data for dataset processing
     * @param dataset destination dataset
     * @param data local data
     * @return destination dataset
     */
    ArrayDataset attachToArray(Iterable data, ArrayDataset dataset) {
        if (dataset == null)
            throw new DslError(this, '#params.required', [param: 'dataset', detail: 'attachToArray'])

        dataset.localDatasetData = data

        return dataset
    }

    /**
     * Attach local data for dataset processing
     * @param datasetName destination dataset, auto create if not exists
     * @param fieldName field name for storage value (default name "value")
     * @param data local data
     * @return destination dataset
     */
    ArrayDataset attachToArray(Iterable data, String datasetName, @DelegatesTo(Field) Closure cl = null) {
        if (datasetName == null)
            throw new DslError(this, '#params.required', [param: 'datasetName', detail: 'attachToArray'])

        def dataset = findDataset(datasetName)
        if (dataset == null)
            dataset = arrayDataset(datasetName, true)
        else if (!(dataset instanceof ArrayDataset))
            throw new DslError(this, '#dsl.invalid_instance_object', [name: datasetName, type: 'array dataset'])

        if (cl != null)
            dataset.field[0].tap(cl)

        return attachToArray(data, dataset as ArrayDataset)
    }

    /** Array dataset */
    ArrayDataset arrayDataset(String name, Boolean registration,
                              @DelegatesTo(ArrayDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.transform.ArrayDataset']) Closure cl = null) {
        def parent = registerDataset(null, RepositoryDatasets.ARRAYDATASET, name, registration,
                defaultOtherConnection(RepositoryDatasets.ARRAYDATASET), ArrayDatasetConnection, cl) as ArrayDataset

        runClosure(parent, cl)

        return parent
    }


    /** Array dataset */
    ArrayDataset arrayDataset(String name,
                              @DelegatesTo(ArrayDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.transform.ArrayDataset']) Closure cl = null) {
        arrayDataset(name, false, cl)
    }

    /** Array dataset */
    ArrayDataset arrayDataset(@DelegatesTo(ArrayDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.transform.ArrayDataset']) Closure cl = null) {
        arrayDataset(null, false, cl)
    }

    /** Array dataset connection */
    ArrayDatasetConnection arrayDatasetConnection(String name, Boolean registration,
                                           @DelegatesTo(ArrayDatasetConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.transform.ArrayDatasetConnection']) Closure cl = null) {
        def parent = registerConnection(RepositoryConnections.ARRAYDATASETCONNECTION, name, registration) as ArrayDatasetConnection
        runClosure(parent, cl)

        return parent
    }

    /** Array dataset connection */
    ArrayDatasetConnection arrayDatasetConnection(String name,
                                    @DelegatesTo(ArrayDatasetConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.transform.ArrayDatasetConnection']) Closure cl = null) {
        arrayDatasetConnection(name, false, cl)
    }

    /** Array dataset connection */
    ArrayDatasetConnection arrayDatasetConnection(@DelegatesTo(ArrayDatasetConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.transform.ArrayDatasetConnection']) Closure cl) {
        arrayDatasetConnection(null, false, cl)
    }

    /** Array dataset default connection */
    ArrayDatasetConnection arrayDatasetConnection() {
        defaultOtherConnection(RepositoryDatasets.ARRAYDATASET) as ArrayDatasetConnection
    }

    /** Use default array dataset connection for new datasets */
    ArrayDatasetConnection useArrayDatasetConnection(ArrayDatasetConnection connection) {
        useOtherConnection(RepositoryDatasets.ARRAYDATASET, connection) as ArrayDatasetConnection
    }
}