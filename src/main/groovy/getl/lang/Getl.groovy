/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.lang

import getl.config.*
import getl.config.opts.*
import getl.csv.*
import getl.data.*
import getl.db2.*
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
import getl.lang.opts.*
import getl.lang.sub.*
import getl.mssql.*
import getl.mysql.*
import getl.netezza.*
import getl.netsuite.*
import getl.oracle.*
import getl.postgresql.*
import getl.proc.*
import getl.proc.opts.*
import getl.proc.sub.*
import getl.salesforce.*
import getl.stat.*
import getl.tfs.*
import getl.utils.*
import getl.utils.sub.*
import getl.vertica.*
import getl.xero.*
import getl.xml.*

import groovy.test.GroovyAssert
import groovy.test.GroovyTestCase
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level

/**
 * Getl language script
 * @author Alexsey Konstantinov
 *
 */
class Getl extends Script {
    protected Getl() {
        super()
        initInstance()
    }

    protected Getl(Binding binding) {
        super(binding)
        initInstance()
    }

    static void main(String[] args) {
        Main(args.toList(), true)
    }

    static private void Help() {
        Version.SayInfo(false)
        println """
The syntax for running a specified Getl Dsl script (with @BaseScript directive):
  java getl.lang.Getl runclass=<script class name> [<arguments>]
  java -jar getl-${Version.version}.jar runclass=<script class name> [<arguments>]

The syntax for running a script that inherits from the Getl class is:
  java <main class name> [<arguments>]

List of possible arguments:
  initclass=<class name> 
    class name of the initialization script that runs before the main script runs
  runclass=<class name>
    the name of the main class of the script
  unittest=true|false
    set the flag that unit tests are launched
  environment=dev|test|prod 
    use the specified configuration environment (the default is "prod")
  config.path=<directory>
    path to load configuration files
  config.filename=config1.groovy;config2.groovy
    a semicolon separated list of configuration file names is allowed to indicate 
    the full path or relative to the one specified in "config.path"
  vars.name=<value>
    set the value for the script field with the specified name, which is marked 
    with the "@Field" directive in the code
      
Examples:
  java getl.lang.Getl runclass=ru.comp.MainScript vars.message="Hello World!"
  java -jar getl-${Version.version}.jar runclass=ru.comp.MainScript config.path=/etc/myconfig config.filename=1.groovy;2.groovy"
  java ru.comp.MainScript initclass=ru.comp.InitScript environment=dev unittest=true
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
            throw new ExceptionDSL("Class $scriptClass is not Getl script!")

        if (args instanceof String[])
            args = args.toList()

        def runclass = scriptClass.name
        Main(args + ["runclass=$runclass"])
    }

    /** Use this initialization class at application startup if it is not explicitly specified */
    protected Class useInitClass() { null }

    /**
     * Launch Getl Dsl script<br><br>
     * <i>List of argument (use format name=value):</i><br>
     * <ul>
     * <li>initclass - class name of the initialization script that runs before the main script runs
     * <li>runclass - the name of the main class of the script
     * <li>unittest - set the flag that unit tests are launched
     * <li>environment - use the specified configuration environment (the default is "prod")
     * <li>config.path - path to load configuration files
     * <li>config.filename - a comma separated list of configuration file names is allowed to indicate the full path or relative to the one specified in "config.path"
     * <li>vars.name - set the value for the script field with the specified name, which is marked with the "@Field" directive in the code
     * <li>
     * </ul>
     * @param args startup arguments
     * @param isApp run as application or module
     */
    static void Main(List args, Boolean isApp = true) {
        if (isApp) {
            def a = ((args?:[]) as List<String>)*.trim()
            a = a*.toLowerCase()
            if (a.isEmpty() || ('-help' in a) || ('-h') in a || ('-?') in a) {
                Help()
                return
            }
        }

        Config.configClassManager = new ConfigSlurper()
        CleanGetl()

        def job = new Job() {
            static ParamMethodValidator allowArgs = {
                def p = new ParamMethodValidator()
                p.register('main', ['config', 'environment', 'initclass', 'runclass', 'unittest', 'vars'])
                p.register('main.config', ['path', 'filename'])
                return p
            }.call()

            public Boolean isMain

            @Override
            protected void doRun() {
                super.doRun()
                StackTraceElement[] stack = Thread.currentThread().getStackTrace()
                def obj = stack[stack.length - 1]
                if (obj.getClassName() == 'getl.lang.Getl' && this.isMain) {
                    Logs.Finest("System exit ${exitCode?:0}")
                    System.exit(exitCode?:0)
                }
            }

            @Override
            void process() {
                allowArgs.validation('main', jobArgs)

                def className = jobArgs.runclass as String
                def isTestMode = BoolUtils.IsValue(jobArgs.unittest)

                if (className == null)
                    throw new ExceptionDSL('Required argument "runclass"!')

                Class runClass
                try {
                    runClass = Class.forName(className)
                }
                catch (Throwable e) {
                    throw new ExceptionDSL("Class \"$className\" not found, error: ${e.message}!")
                }

                Getl eng = GetlInstance(runClass.newInstance() as Getl)
                eng.setGetlSystemParameter('mainClass', runClass.name)
                eng.setUnitTestMode(isTestMode)

                def initClasses = [] as List<Class>

                if (eng.useInitClass() != null)
                    initClasses << eng.useInitClass()

                def initClassName = (jobArgs.initclass as String)
                if (initClassName != null) {
                    eng.setGetlSystemParameter('initClass', initClassName)

                    try {
                        initClasses << Class.forName(initClassName)
                    }
                    catch (Throwable e) {
                        throw new ExceptionDSL("Class \"$initClassName\" not found, error: ${e.message}!")
                    }
                }

                if (!initClasses.isEmpty()) {
                    eng.setGetlSystemParameter('isInitMode', true)
                    try {
                        initClasses.each { initClass ->
                            eng.runGroovyClass(initClass, true)
                        }
                    }
                    finally {
                        eng.setGetlSystemParameter('isInitMode', false)
                    }
                }

                try {
                    eng.runGroovyInstance(eng, Config.vars)
                }
                catch (ExceptionDSL e) {
                    if (e.typeCode == ExceptionDSL.STOP_APP) {
                        if (e.message != null) logInfo(e.message)
                        if (e.exitCode != null) exitCode = e.exitCode
                    }
                    else {
                        throw e
                    }
                }
            }
        }
        job.isMain = isApp
        job.run(args)
    }

    /** Quit DSL Application */
    @SuppressWarnings("GrMethodMayBeStatic")
    void appRunSTOP(String message = null, Integer exitCode = null) {
        if (message != null)
            throw new ExceptionDSL(ExceptionDSL.STOP_APP, exitCode?:0, message)
        else
            throw new ExceptionDSL(ExceptionDSL.STOP_APP, exitCode?:0)
    }

    /** Quit DSL Application */
    @SuppressWarnings("GrMethodMayBeStatic")
    void appRunSTOP(Integer exitCode) {
        throw new ExceptionDSL(ExceptionDSL.STOP_APP, exitCode?:0)
    }

    /** Stop code execution of the current class */
    @SuppressWarnings("GrMethodMayBeStatic")
    void classRunSTOP(String message = null, Integer exitCode = null) {
        if (message != null)
            throw new ExceptionDSL(ExceptionDSL.STOP_CLASS, exitCode?:0, message)
        else
            throw new ExceptionDSL(ExceptionDSL.STOP_CLASS, exitCode?:0)
    }

    /** Stop code execution of the current class */
    @SuppressWarnings("GrMethodMayBeStatic")
    void classRunSTOP(Integer exitCode) {
        throw new ExceptionDSL(ExceptionDSL.STOP_CLASS, exitCode?:0)
    }

    /** Abort execution with the specified error */
    void abortWithError(String message) {
        throw new Exception(message)
    }

    /** The name of the main class of the process */
    String getMainClassName() { _params.mainClass }

    /** The name of the process initializing class */
    String getInitClassName() { _params.initClass }

    static private operationLock = new Object()

    /** Checking process permission */
    @Synchronized
    boolean allowProcess(String processName, Boolean throwError = false) {
        def res = true
        if (_langOpts.processControlDataset != null) {
            if (processName == null) processName = (_params.mainClass)?:this.getClass().name
            if (processName == null)
                throw new ExceptionDSL('Required name for the process being checked!')

            if (_langOpts.processControlDataset instanceof TableDataset) {
                def table = _langOpts.processControlDataset as TableDataset
                def row = sqlQueryRow(table.connection as JDBCConnection, "SELECT enabled FROM ${table.fullNameDataset()} WHERE name = '$processName'")
                if (row != null && !row.isEmpty())
                    res = BoolUtils.IsValue(row.enabled)
            } else {
                def csv = _langOpts.processControlDataset as CSVDataset
                def row = null
                csv.eachRow {
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
                    Logs.Warning("A flag was found to stop the process \"$processName\"!")
                else
                    throw new ExceptionDSL("A flag was found to stop the process \"$processName\"!")
            }
        }

        return res
    }

    /** Checking process permission */
    boolean allowProcess(Boolean throwError = false) {
        allowProcess(null, throwError)
    }

    /** Init Getl instance */
    protected void initInstance() {
        Version.SayInfo()

        _params.executedClasses = new SynchronizeObject()

        _langOpts = new LangSpec(this, this)
        _repositoryConnections = new RepositoryConnections(this)
        _repositoryDatasets = new RepositoryDatasets(this)
        _repositoryHistorypoints = new RepositoryHistorypoints(this)
        _repositorySequences = new RepositorySequences(this)
        _repositoryFilemanagers = new RepositoryFilemanagers(this)

        _params.langOpts =_langOpts
        _params.repositoryConnections = _repositoryConnections
        _params.repositoryDatasets = _repositoryDatasets
        _params.repositoryHistorypoints = _repositoryHistorypoints
        _params.repositorySequences = _repositorySequences
        _params.repositoryFilemanagers = _repositoryFilemanagers
    }

    @Override
    Object run() { this }

    /** Instance of GETL DSL */
    private static Getl _getl

    /** The object is a static instance */
    protected boolean _getlInstance = false

    /** Get Getl instance (created if not exists) */
    static Getl GetlInstance(Getl instance) {
        if (_getl == null) {
            if (instance != null)
                _getl = instance
            else
                _getl = this.newInstance()

            _getl._getlInstance = true
        }

        return _getl
    }

    /** check that Getl instance is created */
    static boolean GetlInstanceCreated() { _getl != null }

    /* Owner object for instance DSL */
    private _ownerObject

    /** Run DSL script on getl share object */
    static Object Dsl(def ownerObject, Map parameters,
                    @DelegatesTo(Getl)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        if (_getl != null && !_getl._getlInstance)
            throw new ExceptionDSL('Cannot be called during Getl object initialization!')

        GetlInstance().runDsl(ownerObject, parameters, cl)
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
    static void CleanGetl(boolean softClean = false) {
        if (softClean && _getl != null) {
            _getl = _getl.getClass().newInstance()
            _getl._getlInstance = true
        }
        else {
            _getl = null
        }
    }

    /** Work in unit test mode */
    Boolean getUnitTestMode() { BoolUtils.IsValue(_params.unitTestMode) }
    /** Work in unit test mode */
    protected void setUnitTestMode(Boolean value) {
        _params.unitTestMode = value
    }

    /** Run DSL script */
    Object runDsl(def ownerObject, Map parameters,
                @DelegatesTo(Getl)
                @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        Object res = null

        if (!(Config.configClassManager instanceof ConfigSlurper))
            Config.configClassManager = new ConfigSlurper()

        def oldOwnerObject = _ownerObject
        try {
            if (ownerObject != null) {
                _ownerObject = ownerObject
                if (ownerObject instanceof GroovyTestCase || ownerObject instanceof GroovyAssert) setUnitTestMode(true)
            }

            if (cl != null) {
                def code = cl.rehydrate(this, this, _ownerObject ?: this)
                code.resolveStrategy = childDelegate
                if (parameters != null) code.properties.putAll(parameters)
                res = code.call(this)
            }
        }
        finally {
            _ownerObject = oldOwnerObject
        }

        return res
    }

    /** Run DSL script */
    Object runDsl(def ownerObject,
                @DelegatesTo(Getl)
                @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        runDsl(ownerObject, null, cl)
    }

    /** Run DSL script */
    Object runDsl(@DelegatesTo(Getl)
                @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        runDsl(null, null, cl)
    }

    /** Owner object for child objects  */
    Object getChildOwnerObject() { this }
    /** This object for child objects */
    Object getChildThisObject() { _ownerObject ?: this }
    /** Delegate method for child objects */
    private static int childDelegate = Closure.DELEGATE_FIRST

    /** Engine parameters */
    private Map<String, Object> _params = new ConcurrentHashMap<String, Object>()
    /** Get engine parameter */
    protected Object getGetlSystemParameter(String key) { _params.get(key) }
    /** Set engine parameter */
    protected setGetlSystemParameter(String key, Object value) {
        _params.put(key, value)
    }

    /** Running init script */
    Boolean getIsInitMode() { BoolUtils.IsValue(_params.isInitMode) }

    /** Set language parameters */
    protected void setGetlParams(Map<String, Object> importParams) {
        _params = importParams

        if (!isInitMode)
            setLangOpts(_params.langOpts as LangSpec)
        else
            _langOpts = _params.langOpts as LangSpec

        _repositoryConnections.objects = (_params.repositoryConnections as RepositoryConnections).objects
        _repositoryDatasets.objects = (_params.repositoryDatasets as RepositoryDatasets).objects
        _repositoryHistorypoints.objects = (_params.repositoryHistorypoints as RepositoryHistorypoints).objects
        _repositorySequences.objects = (_params.repositorySequences as RepositorySequences).objects
        _repositoryFilemanagers.objects = (_params.repositoryFilemanagers as RepositoryFilemanagers).objects
    }

    /** Fix start process */
    ProcessTime startProcess(String name, String objName = null) {
        new ProcessTime(name: name, objectName: objName,
                logLevel: (_langOpts.processTimeTracing) ? _langOpts.processTimeLevelLog : Level.OFF,
                debug: _langOpts.processTimeDebug)
    }

    /** Fix finish process */
    static void finishProcess(ProcessTime pt, Long countRow = null) {
        if (pt != null) pt.finish(countRow)
    }

    /** GETL DSL options */
    void profile(String name, String objName = null,
                 @DelegatesTo(ProfileSpec)
                 @ClosureParams(value = SimpleType, options = ['getl.lang.opts.ProfileSpec']) Closure cl) {
        def stat = new ProfileSpec(this, this, name, objName, true)
        stat.startProfile()
        runClosure(stat, cl)
        stat.finishProfile()
    }


    /** GETL DSL options */
    private LangSpec _langOpts

    /** GETL DSL options */
    LangSpec getLangOpts() { _langOpts }
    /** GETL DSL options */
    void setLangOpts(LangSpec value) {
        _langOpts.params.clear()
        _langOpts.params.putAll(value.params)
    }

    /** list of executed script classes and call parameters */
    protected SynchronizeObject getExecutedClasses() { _params.executedClasses as SynchronizeObject }

    /** Repository object filtering manager */
    private def repositoryFilter = new RepositoryFilter()
    /** Specified filter when searching for objects */
    String getFilteringGroup() { repositoryFilter.filteringGroup }

    /** Use the specified filter by group name when searching for objects */
    void forGroup(String group) {
        if (group == null || group.trim().length() == 0)
            throw new ExceptionDSL('Filter group required!')
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionDSL('Using group filtering within a threads is not allowed!')

        repositoryFilter.filteringGroup = group.trim().toLowerCase()
    }

    /** Reset filter to search for objects */
    void clearGroupFilter() { repositoryFilter.clearGroupFilter() }

    /** Repository object name */
    String repObjectName(String name) {
        repositoryFilter.objectName(name)
    }

    /** Parsing the name of an object from the repository into a group and the name itself */
    ParseObjectName parseName(String name) {
        repositoryFilter.parseName(name)
    }

    /**
     * Read text from specified file
     * @param fileName file path
     * @param codePage encoding text (default UTF-8)
     * @return text from file
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    String textFromFile(String fileName, String codePage = 'UTF-8') {
        def path = FileUtils.ResourceFileName(fileName)
        def file = new File(path)
        if (!file.exists())
            throw new ExceptionDSL("File $fileName not found!")
        if (!file.isFile())
            throw new ExceptionDSL("File $fileName not file!")

        return file.getText(codePage ?: 'UTF-8')
    }

    /** Connections repository */
    private RepositoryConnections _repositoryConnections
    /** Connections repository */
    RepositoryConnections getRepositoryConnections() { _repositoryConnections }

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
        _repositoryConnections.list(mask, connectionClasses, filter)
    }

    /**
     * Return list of reposotory connection objects for specified list of name
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
        _repositoryConnections.processObjects(mask, connectionClasses, cl)
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
    List<String> listJdbcConnections(String mask = null, Closure<Boolean> filter = null) {
        listConnections(mask, _repositoryConnections.listJdbcClasses, filter)
    }

    /**
     * Return list of reposotory jdbc connection objects for specified list of name
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
        processConnections(mask, _repositoryConnections.listJdbcClasses, cl)
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
        _repositoryConnections.find(obj)
    }

    /**
     * Find a connection by name
     * @param name connection name
     * @return found connection object or null if not found
     */
    Connection findConnection(String name) {
        _repositoryConnections.find(name)
    }

    /** Register connection in repository */
    protected Connection registerConnection(String connectionClassName, String name, Boolean registration = false) {
        _repositoryConnections.register(connectionClassName, name, registration)
    }

    /**
     * Register connection object in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    Connection registerConnectionObject(Connection obj, String name = null, Boolean validExist = true) {
        _repositoryConnections.registerObject(obj, name, validExist)
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
        _repositoryConnections.unregister(mask, connectionClasses, filter)
    }

    /** Datasets repository */
    private RepositoryDatasets _repositoryDatasets
    /** Datasets repository */
    RepositoryDatasets getRepositoryDatasets() { _repositoryDatasets }

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
        _repositoryDatasets.list(mask, datasetClasses, filter)
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
     * Return list of reposotory dataset objects for specified list of name
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
        _repositoryDatasets.processObjects(mask, datasetClasses, cl)
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
        _repositoryDatasets.linkDatasets(sourceList, destList, filter)
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
            throw new ExceptionDSL('Required to specify the value of the source group name!')
        if (destGroup == null)
            throw new ExceptionDSL('Required to specify the value of the destination group name!')

        linkDatasets(listDatasets(sourceGroup + ':'), listDatasets(destGroup + ':'), filter)
    }

    /**
     * Return list of repository jdbc tables for specified mask and filter
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of jdbc table names according to specified conditions
     */
    List<String> listJdbcTables(String mask = null, Closure<Boolean> filter = null) {
        listDatasets(mask, _repositoryDatasets.listJdbcClasses, filter)
    }

    /**
     * Return list of repository jdbc tables for specified filter
     * @param filter object filtering code
     * @return list of jdbc table names according to specified conditions
     */
    List<String> listJdbcTables(Closure<Boolean> filter) {
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
        processDatasets(mask, _repositoryDatasets.listJdbcClasses, cl)
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
            throw new ExceptionDSL('Dataset class name cannot be null!')
        if (!(datasetClassName in _repositoryDatasets.listClasses))
            throw new ExceptionDSL("$datasetClassName is not dataset class!")

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
    private final Object lockLastJDBCDefaultConnection = new Object()

    /** Last used JDBC default connection */
    JDBCConnection getLastJdbcDefaultConnection() {
        JDBCConnection res
        synchronized (lockLastJDBCDefaultConnection) {
            res = _lastJDBCDefaultConnection
        }
        return res
    }
    /** Last used JDBC default connection */
    void setLastJdbcDefaultConnection(JDBCConnection value) {
        synchronized (lockLastJDBCDefaultConnection) {
            _lastJDBCDefaultConnection = value
        }
    }

    /** Default JDBC connection for datasets */
    private def _defaultJDBCConnection = new ConcurrentHashMap<String, JDBCConnection>()

    /** Default JDBC connection for datasets */
    JDBCConnection defaultJdbcConnection(String datasetClassName = null) {
        JDBCConnection res
        if (datasetClassName == null)
            res = lastJdbcDefaultConnection
        else {
            if (!(datasetClassName in _repositoryDatasets.listJdbcClasses))
                throw new ExceptionDSL("$datasetClassName is not jdbc dataset class!")

            res = _defaultJDBCConnection.get(datasetClassName)
            if (res == null && lastJdbcDefaultConnection != null && datasetClassName == _repositoryDatasets.QUERYDATASET)
                res = lastJdbcDefaultConnection
        }

        if (_langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
            def thread = Thread.currentThread() as ExecutorThread
            res = thread.registerCloneObject('connections', res,
                    {
                        def c = (it as Connection).cloneConnection()
                        c.sysParams.dslThisObject = childThisObject
                        c.sysParams.dslOwnerObject = childOwnerObject
                        c.sysParams.dslNameObject = res.dslNameObject
                        return c
                    }
            ) as JDBCConnection
        }

        return res
    }

    /** Use specified JDBC connection as default */
    JDBCConnection useJdbcConnection(String datasetClassName, JDBCConnection value) {
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionDSL('Specifying the default connection is not allowed in thread!')

        if (datasetClassName != null) {
            if (!(datasetClassName in repositoryDatasets.listJdbcClasses))
                throw new ExceptionDSL("$datasetClassName is not jdbc dataset class!")
            _defaultJDBCConnection.put(datasetClassName, value)
        }
        setLastJdbcDefaultConnection(value)

        return value
    }

    /** Last used file default connection */
    private FileConnection _lastFileDefaultConnection
    private final Object lockLastFileDefaultConnection = new Object()

    /** Last used file default connection */
    FileConnection getLastFileDefaultConnection() {
        FileConnection res
        synchronized (lockLastFileDefaultConnection) {
            res = _lastFileDefaultConnection
        }
        return res
    }
    /** Last used file default connection */
    void setLastFileDefaultConnection(FileConnection value) {
        synchronized (lockLastFileDefaultConnection) {
            _lastFileDefaultConnection = value
        }
    }

    private def _defaultFileConnection = new ConcurrentHashMap<String, FileConnection>()

    /** Default file connection for datasets */
    FileConnection defaultFileConnection(String datasetClassName = null) {
        FileConnection res
        if (datasetClassName == null)
            res = lastFileDefaultConnection
        else {
            if (!(datasetClassName in _repositoryDatasets.listFileClasses))
                throw new ExceptionDSL("$datasetClassName is not file dataset class!")

            res = _defaultFileConnection.get(datasetClassName)
        }

        if (_langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
            def thread = Thread.currentThread() as ExecutorThread
            res = thread.registerCloneObject('connections', res,
                    {
                        def c = (it as Connection).cloneConnection()
                        c.sysParams.dslThisObject = childThisObject
                        c.sysParams.dslOwnerObject = childOwnerObject
                        c.sysParams.dslNameObject = res.dslNameObject
                        return c
                    }
            ) as FileConnection
        }

        return res
    }

    /** Use specified file connection as default */
    FileConnection useFileConnection(String datasetClassName, FileConnection value) {
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionDSL('Specifying the default connection is not allowed in thread!')

        if (datasetClassName != null) {
            if (!(datasetClassName in _repositoryDatasets.listFileClasses))
                throw new ExceptionDSL("$datasetClassName is not file dataset class!")

            _defaultFileConnection.put(datasetClassName, value)
        }
        setLastFileDefaultConnection(value)

        return value
    }

    /** Last used other type default connection */
    private Connection _lastOtherDefaultConnection
    private final Object lockLastOtherDefaultConnection = new Object()

    /** Last used other type default connection */
    Connection getLastOtherDefaultConnection() {
        Connection res
        synchronized (lockLastOtherDefaultConnection) {
            res = _lastOtherDefaultConnection
        }
        return res
    }
    /** Last used other type default connection */
    void setLastOtherDefaultConnection(Connection value) {
        synchronized (lockLastOtherDefaultConnection) {
            _lastOtherDefaultConnection = value
        }
    }

    private def _defaultOtherConnection = new ConcurrentHashMap<String, Connection>()

    /** Default other type connection for datasets */
    Connection defaultOtherConnection(String datasetClassName = null) {
        Connection res
        if (datasetClassName == null)
            res = lastOtherDefaultConnection
        else {
            if (!(datasetClassName in _repositoryDatasets.listOtherClasses))
                throw new ExceptionDSL("$datasetClassName is not dataset class!")

            res = _defaultOtherConnection.get(datasetClassName)
        }

        if (_langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
            def thread = Thread.currentThread() as ExecutorThread
            res = thread.registerCloneObject('connections', res,
                    {
                        def c = (it as Connection).cloneConnection()
                        c.sysParams.dslThisObject = childThisObject
                        c.sysParams.dslOwnerObject = childOwnerObject
                        c.sysParams.dslNameObject = res.dslNameObject
                        return c
                    }
            ) as Connection
        }

        return res
    }

    /** Use specified other type connection as default */
    Connection useOtherConnection(String datasetClassName, Connection value) {
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionDSL('Specifying the default connection is not allowed in thread!')

        if (datasetClassName != null) {
            if (!(datasetClassName in _repositoryDatasets.listOtherClasses))
                throw new ExceptionDSL("$datasetClassName is not dataset class!")

            _defaultOtherConnection.put(datasetClassName, value)
        }
        setLastOtherDefaultConnection(value)

        return value
    }

    /**
     * Search for an object in the repository
     * @param obj dataset
     * @return name of the object in the repository or null if not found
     */
    String findDataset(Dataset obj) {
        _repositoryDatasets.find(obj)
    }

    /**
     * Find a dataset by name
     * @param name dataset name
     * @return found dataset object or null if not found
     */
    Dataset findDataset(String name) {
        _repositoryDatasets.find(name) as Dataset
    }

    /** Register dataset in repository */
    protected Dataset registerDataset(Connection connection, String datasetClassName, String name, Boolean registration = false,
                                      Connection defaultConnection = null, Class classConnection = null, Closure cl = null) {
        _repositoryDatasets.register(connection, datasetClassName, name, registration, defaultConnection, classConnection, cl)
    }

    /**
     * Register dataset in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    Dataset registerDatasetObject(Dataset obj, String name = null, Boolean validExist = true) {
        _repositoryDatasets.registerObject(obj, name, validExist) as Dataset
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
        _repositoryDatasets.unregister(mask, datasetClasses, filter)
    }

    /** History points repository */
    private RepositoryHistorypoints _repositoryHistorypoints
    /** History points repository */
    RepositoryHistorypoints getRepositoryHistorypoints() { _repositoryHistorypoints }

    /**
     * Return list of repository history point manager
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of history point manager names according to specified conditions
     */
    List<String> listHistorypoints(String mask = null, Closure<Boolean> filter = null) {
        _repositoryHistorypoints.list(mask, null, filter)
    }

    /**
     * Return list of repository history point manager
     * @param filter object filtering code
     * @return list of history point manager names according to specified conditions
     */
    List<String> listHistorypoints(Closure<Boolean> filter) {
        listHistorypoints(null, filter)
    }

    /**
     * Process repository history point managers for specified mask and filter
     * @param mask filter mask (use Path expression syntax)
     * @param cl process code
     */
    void processHistorypoints(String mask,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        _repositoryHistorypoints.processObjects(mask, null, cl)
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
    String findHistorypoint(SavePointManager obj) {
        _repositoryHistorypoints.find(obj)
    }

    /**
     * Find a history point manager by name
     * @param name history point manager name
     * @return found history point manager object or null if not found
     */
    SavePointManager findHistorypoint(String name) {
        _repositoryHistorypoints.find(name) as SavePointManager
    }

    /**
     * Register history point in repository
     * @param name name in repository
     * @registration registering
     */
    protected SavePointManager registerHistoryPoint(JDBCConnection connection, String name,  Boolean registration = false,
                                                    Closure cl = null) {
        _repositoryHistorypoints.register(connection, _repositoryHistorypoints.SAVEPOINTMANAGER, name, registration,
                defaultJdbcConnection(_repositoryDatasets.QUERYDATASET), JDBCConnection, cl)
    }

    /**
     * Register history point in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    SavePointManager registerHistoryPointObject(SavePointManager obj, String name = null, Boolean validExist = true) {
        _repositoryHistorypoints.registerObject(obj, name, validExist) as SavePointManager
    }

    /**
     * Unregister history point manager
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     */
    void unregisterHistorypoint(String mask = null,
                                @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.SavePointManager'])
                                        Closure<Boolean> filter = null) {
        _repositoryHistorypoints.unregister(mask, null, filter)
    }

    /** Sequences repository */
    private RepositorySequences _repositorySequences
    /** Sequences repository */
    RepositorySequences getRepositorySequences() { _repositorySequences }

    /**
     * Return list of repository sequences
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of sequences names according to specified conditions
     */
    List<String> listSequences(String mask = null, Closure<Boolean> filter = null) {
        _repositorySequences.list(mask, null, filter)
    }

    /**
     * Return list of repository sequences
     * @param filter object filtering code
     * @return list of sequences names according to specified conditions
     */
    List<String> listSequences(Closure<Boolean> filter) {
        listSequences(null, filter)
    }

    /**
     * Process repository sequences for specified mask and filter
     * @param mask filter mask (use Path expression syntax)
     * @param cl process code
     */
    void processSequences(String mask,
                          @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        _repositorySequences.processObjects(mask, null, cl)
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
        _repositorySequences.find(obj)
    }

    /**
     * Find a sequences by name
     * @param name sequences name
     * @return found sequences object or null if not found
     */
    Sequence findSequence(String name) {
        _repositorySequences.find(name) as Sequence
    }

    /**
     * Register sequence in repository
     * @param name name in repository
     * @registration registering
     */
    protected Sequence registerSequence(Connection connection, String name, Boolean registration = false,
                                        Closure cl = null) {
        _repositorySequences.register(connection, _repositorySequences.SEQUENCE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.QUERYDATASET), JDBCConnection, cl)
    }

    /**
     * Register sequence in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    Sequence registerSequenceObject(Sequence obj, String name = null, Boolean validExist = true) {
        _repositorySequences.registerObject(obj, name, validExist) as Sequence
    }

    /**
     * Unregister sequence
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     */
    void unregisterSequence(String mask = null,
                            @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.Sequence'])
                                    Closure<Boolean> filter = null) {
        _repositorySequences.unregister(mask, null, filter)
    }

    /** File managers repository */
    private RepositoryFilemanagers _repositoryFilemanagers
    /** File managers repository */
    RepositoryFilemanagers getRepositoryFilemanagers() { _repositoryFilemanagers }

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
        _repositoryFilemanagers.list(mask, filemanagerClasses, filter)
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
     * Return list of reposotory file managers objects for specified list of name
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
        _repositoryFilemanagers.processObjects(mask, filemanagerClasses, cl)
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
        _repositoryFilemanagers.find(obj)
    }

    /**
     * Find a file manager by name
     * @param name file manager name
     * @return found file manager object or null if not found
     */
    Manager findFilemanager(String name) {
        _repositoryFilemanagers.find(name)
    }

    /** Register file manager in repository */
    protected Manager registerFileManager(String fileManagerClassName, String name,
                                          Boolean registration = false) {
        _repositoryFilemanagers.register(fileManagerClassName, name, registration)
    }

    /**
     * Register file manager object in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    Manager registerFileManagerObject(Manager obj, String name = null, Boolean validExist = true) {
        _repositoryFilemanagers.registerObject(obj, name, validExist)
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
        _repositoryFilemanagers.unregister(mask, filemanagerClasses, filter)
    }

    /**
     * Load and run groovy script file
     * <br><br>Example:
     * <br>runGroovyFile './scripts/script1.groovy', false, [param1: 1, param2: 'a', param3: [1,2,3]]
     * @param fileName script file path
     * @param runOnce do not execute if previously executed
     * @param vars set values for script fields declared as "@Field"
     * @return exit code
     */
    Integer runGroovyScriptFile(String fileName, Boolean runOnce, Map vars = [:]) {
        File sourceFile = new File(fileName)
        def groovyClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(sourceFile)
        return runGroovyClass(groovyClass, runOnce, vars)
    }

    /**
     * Load and run groovy script file
     * <br><br>Example:
     * <br>runGroovyFile './scripts/script1.groovy', [param1: 1, param2: 'a', param3: [1,2,3]]
     * @param fileName script file path
     * @param vars set values for script fields declared as "@Field"
     * @return exit code
     */
    Integer runGroovyScriptFile(String fileName, Map vars = [:]) {
        runGroovyScriptFile(fileName, false, vars)
    }

    /**
     * Load and run groovy script file
     * <br><br>Example:
     * <br>runGroovyFile './scripts/script1.groovy', false, { param1 = 1; param2 = 'a'; param3 = [1,2,3] }
     * @param fileName script file path
     * @param runOnce do not execute if previously executed
     * @param vars set values for script fields declared as "@Field"
     * @return exit code
     */
    Integer runGroovyScriptFile(String fileName, Boolean runOnce, Closure vars) {
        File sourceFile = new File(fileName)
        def groovyClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(sourceFile)
        return runGroovyClass(groovyClass, runOnce, vars)
    }

    /**
     * Load and run groovy script file
     * <br><br>Example:
     * <br>runGroovyFile './scripts/script1.groovy', { param1 = 1; param2 = 'a'; param3 = [1,2,3] }
     * @param fileName script file path
     * @param runOnce do not execute if previously executed
     * @param vars set values for script fields declared as "@Field"
     * @return exit code
     */
    Integer runGroovyScriptFile(String fileName, Closure vars) {
        runGroovyScriptFile(fileName, false, vars)
    }

    /**
     * Load and run groovy script file
     * <br><br>Example:
     * <br>runGroovyFile './scripts/script1.groovy', false, 'processes.process1'
     * @param fileName script file path
     * @param runOnce do not execute if previously executed
     * @param configSection set values for script fields declared as "@Field" from the specified configuration section
     * @return exit code
     */
    Integer runGroovyScriptFile(String fileName, Boolean runOnce, String configSection) {
        def sectParams = Config.FindSection(configSection)
        if (sectParams == null)
            throw new ExceptionDSL("Configuration section \"$configSection\" not found!")

        return runGroovyScriptFile(fileName, runOnce, sectParams)
    }

    /**
     * Load and run groovy script file
     * <br><br>Example:
     * <br>runGroovyFile './scripts/script1.groovy', 'processes.process1'
     * @param fileName script file path
     * @param configSection set values for script fields declared as "@Field" from the specified configuration section
     * @return exit code
     */
    Integer runGroovyScriptFile(String fileName, String configSection) {
        runGroovyScriptFile(fileName, false, configSection)
    }

    /**
     * Run groovy script class
     * <br><br>Example:
     * <br>runGroovyClass 'project.processed.Script1', false, [param1: 1, param2: 'a', param3: [1,2,3]]
     * @param groovyClass groovy script class full name
     * @param runOnce do not execute if previously executed
     * @param vars set values for script fields declared as "@Field"
     * @return exit code
     */
    Integer runGroovyClass(Class groovyClass, Boolean runOnce, Map vars = [:]) {
        def className = groovyClass.name
        def previouslyRun = (executedClasses.indexOfListItem(className) != -1)
        if (previouslyRun && BoolUtils.IsValue(runOnce)) return 0

        def script = (GroovyObject)groovyClass.newInstance() as Script
        def res = runGroovyInstance(script, vars)

        if (!previouslyRun) executedClasses.addToList(className)

        return res
    }

    /**
     * Run groovy script object
     * @param script groovy script object
     * @param vars set values for script fields declared as "@Field"
     * @return exit code
     */
    protected Integer runGroovyInstance(Script script, Map vars = [:]) {
        def res = 0

        if (script instanceof Getl) {
            def scriptGetl = script as Getl
            scriptGetl.setGetlParams(_params)
            InitGetlClass(scriptGetl)
            if (vars != null && !vars.isEmpty()) {
                FillFieldFromVars(scriptGetl, vars)
            }
            CheckGetlClass(scriptGetl)
        } else if (vars != null && !vars.isEmpty()) {
            script.binding = new Binding(vars)
        }

        def pt = startProcess("Execution groovy script ${script.getClass().name}", 'class')
        try {
            script.run()
        }
        catch (ExceptionDSL e) {
            if (e.typeCode == ExceptionDSL.STOP_CLASS) {
                if (e.message != null) logInfo(e.message)
                if (e.exitCode != null) res = e.exitCode
            }
            else {
                throw e
            }
        }
        catch (Exception e) {
            try {
                ErrorGetlClass(script, e)
            }
            catch (Exception err) {
                Logs.Exception(err, 'method error', script.getClass().name)
            }
            finally {
                throw e
            }
        }
        finally {
            DoneGetlClass(script)
        }
        pt.finish()

        return res
    }

    /**
     * Run groovy script class
     * <br><br>Example:
     * <br>runGroovyClass 'project.processed.Script1', [param1: 1, param2: 'a', param3: [1,2,3]]
     * @param groovyClass groovy script class full name
     * @param vars set values for script fields declared as "@Field"
     * @return exit code
     */
    Integer runGroovyClass(Class groovyClass, Map vars = [:]) {
        runGroovyClass(groovyClass, false, vars)
    }

    /**
     * Run groovy script class
     * <br><br>Example:
     * <br>runGroovyClass 'project.processed.Script1', false, { param1 = 1; param2 = 'a'; param3 = [1,2,3] }
     * @param groovyClass groovy script class full name
     * @param runOnce do not execute if previously executed
     * @param vars set values for script fields declared as "@Field"
     * @return exit code
     */
    Integer runGroovyClass(Class groovyClass, Boolean runOnce, Closure vars) {
        def cfg = new groovy.util.ConfigSlurper()
        def map = cfg.parse(new ClosureScript(closure: vars))
        return runGroovyClass(groovyClass, runOnce, MapUtils.ConfigObject2Map(map))
    }

    /**
     * Run groovy script class
     * <br><br>Example:
     * <br>runGroovyClass 'project.processed.Script1', { param1 = 1; param2 = 'a'; param3 = [1,2,3] }
     * @param groovyClass groovy script class full name
     * @param vars set values for script fields declared as "@Field"
     */
    Integer runGroovyClass(Class groovyClass, Closure vars) {
        runGroovyClass(groovyClass, false, vars)
    }

    /**
     * Run groovy script class
     * <br><br>Example:
     * <br>runGroovyClass 'project.processed.Script1', false, 'processes.process1'
     * @param groovyClass groovy script class full name
     * @param runOnce do not execute if previously executed
     * @param configSection set values for script fields declared as "@Field" from the specified configuration section
     */
    Integer runGroovyClass(Class groovyClass, Boolean runOnce, String configSection) {
        def sectParams = Config.FindSection(configSection)
        if (sectParams == null)
            throw new ExceptionDSL("Configuration section \"$configSection\" not found!")

        return runGroovyClass(groovyClass, runOnce, sectParams)
    }

    /**
     * Run groovy script class
     * <br><br>Example:
     * <br>runGroovyClass 'project.processed.Script1', 'processes.process1'
     * @param groovyClass groovy script class full name
     * @param configSection set values for script fields declared as "@Field" from the specified configuration section
     */
    Integer runGroovyClass(Class groovyClass, String configSection) {
        runGroovyClass(groovyClass, false, configSection)
    }

    /**
     *  Call script init method before execute script
     */
    static protected void InitGetlClass(Script script) {
        def m = script.getClass().methods.find { it.name == 'init' }
        if (m != null)
            script.invokeMethod('init', null)
    }

    /**
     *  Call script check method after setting field values
     */
    static protected void CheckGetlClass(Script script) {
        def m = script.getClass().methods.find { it.name == 'check' }
        if (m != null)
            script.invokeMethod('check', null)
    }

    /**
     *  Call script done method before execute script
     */
    static protected void DoneGetlClass(Script script) {
        def m = script.getClass().methods.find { it.name == 'done' }
        if (m != null)
            script.invokeMethod('done', null)
    }

    /**
     *  Call script error method before execute script
     */
    static protected void ErrorGetlClass(Script script, Exception e) {
        def m = script.getClass().methods.find { it.name == 'error' }
        if (m != null)
            script.invokeMethod('error', e)
    }

    /**
     * Fill script field property from external arguments
     * @param script groove script object
     * @param vars vars set values for script fields declared as "@Field"
     * @param validExist check for the existence of fields in the script
     */
    static void FillFieldFromVars(Script script, Map vars, Boolean validExist = true) {
        vars.each { key, value ->
            MetaProperty prop = script.hasProperty(key as String)
            if (prop == null) {
                if (validExist)
                    throw new ExceptionDSL("Field \"$key\" not defined in script!")
                else
                    return
            }

            if (value != null)
                switch (prop.type) {
                    case Character:
                        if (!(value instanceof Character))
                            value = value.toString().toCharacter()
                        break
                    case String:
                        if (!(value instanceof String))
                            value = value.toString()
                        break
                    case Short:
                        if (!(value instanceof Short))
                            value = Short.valueOf(value.toString())
                        break
                    case Integer:
                        if (!(value instanceof Integer))
                            value = Integer.valueOf(value.toString())
                        break
                    case Long:
                        if (!(value instanceof Long))
                            value = Long.valueOf(value.toString())
                        break
                    case Float:
                        if (!(value instanceof Float))
                            value = Float.valueOf(value.toString())
                        break
                    case Double:
                        if (!(value instanceof Double))
                            value = Double.valueOf(value.toString())
                        break
                    case BigInteger:
                        if (!(value instanceof BigInteger))
                            value = new BigInteger(value.toString())
                        break
                    case BigDecimal:
                        if (!(value instanceof BigDecimal))
                            value = new BigDecimal(value.toString())
                        break
                    case Date:
                        if (!(value instanceof Date)) {
                            value = value.toString()
                            if (value.length() == 10)
                                value = DateUtils.ParseDate('yyyy-MM-dd', value, false)
                            else
                                value = DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss', value, false)
                        }
                        break
                    case Boolean:
                        if (!(value instanceof Boolean )) {
                            value = (value.toString().toLowerCase() in ['true', '1', 'on'])
                        }
                        break
                    case List:
                        if (!(value instanceof List)) {
                            value = Eval.me(value.toString())
                        }
                        break
                    case Map:
                        if (!(value instanceof Map)) {
                            value = Eval.me(value.toString())
                        }
                        break
                }


            try {
                prop.setProperty(script, value)
            }
            catch (Exception e) {
                throw new ExceptionDSL("Can not assign by class ${value.getClass().name} value \"$value\" to property \"$key\" with class \"${prop.type.name}\", error: ${e.message}")
            }
        }
    }

    /** Detect delegate object for closure code */
    static Object DetectClosureDelegate(Object obj) {
        while (obj instanceof Closure) obj = (obj as Closure).delegate
        return obj
    }

    /** Preparing closure code for specified object */
    static Closure PrepareClosure(def ownerObject, def thisObject, def parent, Closure cl) {
        if (cl == null) return null
        def code = cl.rehydrate(parent, ownerObject, thisObject)
        code.resolveStrategy = childDelegate
        return code
    }

    /** Run closure with call parent parameter */
    static Object RunClosure(Object ownerObject, Object thisObject, Object parent, Closure cl) {
        if (cl == null) return null

        def code = cl.rehydrate(parent, ownerObject, thisObject)
        code.resolveStrategy = childDelegate
        return code.call(parent)
    }

    /** Run closure with call one parameter */
    static Object RunClosure(Object ownerObject, Object thisObject, Object parent, Closure cl, Object param) {
        if (cl == null) return null

        def code = cl.rehydrate(parent, ownerObject, thisObject)
        code.resolveStrategy = childDelegate
        return code.call(param)
    }

    /** Run closure with call two parameters */
    static Object RunClosure(Object ownerObject, Object thisObject, Object parent, Closure cl, Object... params) {
        if (cl == null) return null

        def code = cl.rehydrate(parent, ownerObject, thisObject)
        code.resolveStrategy = childDelegate
        return code.call(params)
    }

    /** Run closure with call parent parameter */
    protected Object runClosure(Object parent, Closure cl) {
        if (cl == null) return null

        def code = cl.rehydrate(parent, childOwnerObject, childThisObject)
        code.resolveStrategy = childDelegate
        return code.call(parent)
    }

    /** Run closure with call one parameter */
    protected Object runClosure(Object parent, Closure cl, Object param) {
        if (cl == null) return null

        def code = cl.rehydrate(parent, childOwnerObject, childThisObject)
        code.resolveStrategy = childDelegate
        return code.call(param)
    }

    /** Run closure with call two parameters */
    protected Object runClosure(Object parent, Closure cl, Object... params) {
        if (cl == null) return null

        def code = cl.rehydrate(parent, childOwnerObject, childThisObject)
        code.resolveStrategy = childDelegate
        return code.call(params)
    }

    /** Configuration content */
    @SuppressWarnings("GrMethodMayBeStatic")
    Map<String, Object> getConfigContent() { Config.content }

    /** Configuration variables */
    @SuppressWarnings("GrMethodMayBeStatic")
    Map<String, Object> getConfigVars() { Config.vars }

    /** Init section configuration options */
    @SuppressWarnings("GrMethodMayBeStatic")
    Map<String, Object> getConfigInit() { Config.content.init as Map<String, Object> }

    /** Global section configuration options */
    @SuppressWarnings("GrMethodMayBeStatic")
    Map<String, Object> getConfigGlobal() { Config.content.global as Map<String, Object> }

    /** OS variables */
    @SuppressWarnings("GrMethodMayBeStatic")
    Map<String, String> getSysVars() { System.getenv() }

    /** Write message for specified level to log */
    static void logWrite(String level, String message) { Logs.Write(level, message) }

    /** Write message for specified level to log */
    static void logWrite(Level level, String message) { Logs.Write(level, message) }

    /** Write message as level the INFO to log */
    static void logInfo(def msg) { Logs.Info(msg.toString()) }

    /** Write message as level the WARNING to log */
    static void logWarn(def msg) { Logs.Warning(msg.toString()) }

    /** Write message as level the SEVERE to log */
    static void logError(def msg) { Logs.Severe(msg.toString()) }

    /** Write message as level the FINE to log */
    static void logFine(def msg) { Logs.Fine(msg.toString()) }

    /** Write message as level the FINER to log */
    static void logFiner(def msg) { Logs.Finer(msg.toString()) }

    /** Write message as level the FINEST to log */
    static void logFinest(def msg) { Logs.Finest(msg.toString()) }

    /** Write message as level the CONFIG to log */
    static void logConfig(def msg) { Logs.Config(msg.toString()) }

    /** System temporary directory */
    static String getSystemTempPath() { TFS.systemPath }

    /** GETL DSL options */
    LangSpec options(@DelegatesTo(LangSpec)
                     @ClosureParams(value = SimpleType, options = ['getl.lang.opts.LangSpec']) Closure cl = null) {
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionDSL('Changing options is not supported in the thread!')

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
    @SuppressWarnings("GrMethodMayBeStatic")
    protected ConfigSlurper getConfigManager() { Config.configClassManager as ConfigSlurper }

    /** Configuration options */
    ConfigSpec configuration(@DelegatesTo(ConfigSpec)
                             @ClosureParams(value = SimpleType, options = ['getl.config.opts.ConfigSpec']) Closure cl = null) {
        if (cl != null && Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionDSL('Changing configuration is not supported in the thread!')

        if (!(Config.configClassManager instanceof ConfigSlurper)) Config.configClassManager = new ConfigSlurper()
        def parent = new ConfigSpec(childOwnerObject, childThisObject)
        parent.runClosure(cl)

        return parent
    }

    /** Log options */
    LogSpec logging(@DelegatesTo(LogSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.opts.LogSpec']) Closure cl = null) {
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionDSL('Changing log file options is not supported in the thread!')

        def parent = new LogSpec(childOwnerObject, childThisObject)
        def logFileName = parent.getLogFileName()
        parent.runClosure(cl)
        if (logFileName != parent.getLogFileName()) {
            Logs.Info("### Getl start logging to log file ${parent.getLogFileName()}")
        }

        return parent
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
            throw new ExceptionDSL('Need connection name value!')

        def parent = registerConnection(null, name, false) as Connection
        runClosure(parent, cl)

        return parent
    }

    /**
     * Clone connection object
     * @param con original connection to clone
     * @return clone connection
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    Connection cloneConnection(Connection con) {
        if (con == null)
            throw new ExceptionDSL('Need object value!')
        return con.cloneConnection()
    }

    /**
     * Clone connection object and register to repository
     * @param newName repository name for cloned dataset
     * @param con original connection to clone
     * @param cl cloned connection processing code
     * @return clone connection
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    Connection cloneConnection(String newName, Connection con,
                               @DelegatesTo(Connection)
                               @ClosureParams(value = SimpleType, options = ['getl.data.Connection']) Closure cl = null) {
        def res = cloneConnection(con)
        registerConnectionObject(res, newName)
        runClosure(res, cl)

        return res
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
            throw new ExceptionDSL('Need table name value!')

        def obj = findDataset(name)
        if (obj == null)
            throw new ExceptionDSL("Dataset \"$name\" not found!")

        def parent = registerDataset(null, obj.getClass().name, name)
        runClosure(parent, cl)

        return parent
    }

    /**
     * Clone dataset object
     * @param dataset original dataset to clone
     * @param con used connection for new dataset
     * @return clone dataset
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    Dataset cloneDataset(Dataset dataset, Connection con = null) {
        if (dataset == null)
            throw new ExceptionDSL('Need object value!')
        return dataset.cloneDataset(con)
    }

    /**
     * Clone dataset object and register in repository
     * @param newName repository name for cloned dataset
     * @param dataset original dataset to clone
     * @param con used connection for new dataset
     * @param cl cloned dataset processing code
     * @return clone dataset
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    Dataset cloneDataset(String newName, Dataset dataset, Connection con = null,
                         @DelegatesTo(Dataset)
                         @ClosureParams(value = SimpleType, options = ['getl.data.Dataset']) Closure cl = null) {
        def res = cloneDataset(dataset, con)
        registerDatasetObject(res, newName)
        runClosure(res, cl)

        return res
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
            throw new ExceptionDSL('Need connection name value!')

        def parent = registerConnection(null, name, false) as Connection
        if (!(parent instanceof JDBCConnection))
            throw new ExceptionDSL("$name is not jdbc connection!")

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
            throw new ExceptionDSL('Need table name value!')

        def obj = findDataset(name)
        if (obj == null)
            throw new ExceptionDSL("Table \"$name\" not found!")
        if (!(obj instanceof TableDataset))
            throw new ExceptionDSL("Dataset \"$name\" is not table!")

        def parent = registerDataset(null, obj.getClass().name, name) as TableDataset
        runClosure(parent, cl)

        return parent
    }

    /** JDBC view dataset */
    ViewDataset view(String name = null, Boolean registration = false, JDBCConnection connection = null,
                     @DelegatesTo(ViewDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.ViewDataset']) Closure cl = null) {
        def parent = registerDataset(connection, _repositoryDatasets.VIEWDATASET, name, registration,
                                        defaultJdbcConnection(_repositoryDatasets.QUERYDATASET), JDBCConnection, cl) as ViewDataset
        runClosure(parent, cl)
        return parent
    }

    /** JDBC view dataset */
    ViewDataset view(String name, Boolean registration = false,
                     @DelegatesTo(ViewDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.ViewDataset']) Closure cl) {
        view(name, false, null, cl)
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
        def parent = registerConnection(_repositoryConnections.FIREBIRDCONNECTION, name, registration) as FirebirdConnection
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
        defaultJdbcConnection(_repositoryDatasets.FIREBIRDTABLE) as FirebirdConnection
    }

    /** Use default Firebird connection for new datasets */
    FirebirdConnection useFirebirdConnection(FirebirdConnection connection) {
        useJdbcConnection(_repositoryDatasets.FIREBIRDTABLE, connection) as FirebirdConnection
    }

    /** Firebird table */
    FirebirdTable firebirdTable(String name, Boolean registration,
                                @DelegatesTo(FirebirdTable)
                                @ClosureParams(value = SimpleType, options = ['getl.firebird.FirebirdTable']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.FIREBIRDTABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.FIREBIRDTABLE), FirebirdConnection, cl) as FirebirdTable
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
        def parent = registerConnection(_repositoryConnections.H2CONNECTION, name, registration) as H2Connection
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
        defaultJdbcConnection(_repositoryDatasets.H2TABLE) as H2Connection
    }

    /** Use default H2 connection for new datasets */
    H2Connection useH2Connection(H2Connection connection) {
        useJdbcConnection(_repositoryDatasets.H2TABLE, connection) as H2Connection
    }

    /** H2 table */
    H2Table h2Table(String name, Boolean registration,
                    @DelegatesTo(H2Table)
                    @ClosureParams(value = SimpleType, options = ['getl.h2.H2Table']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.H2TABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.H2TABLE), H2Connection, cl) as H2Table
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

    /** DB2 connection */
    DB2Connection db2Connection(String name, Boolean registration,
                                @DelegatesTo(DB2Connection)
                                @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Connection']) Closure cl = null) {
        def parent = registerConnection(_repositoryConnections.DB2CONNECTION, name, registration) as DB2Connection
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
        defaultJdbcConnection(_repositoryDatasets.DB2TABLE) as DB2Connection
    }

    /** Use default DB2 connection for new datasets */
    DB2Connection useDb2Connection(DB2Connection connection) {
        useJdbcConnection(_repositoryDatasets.DB2TABLE, connection) as DB2Connection
    }

    /** DB2 database table */
    DB2Table db2Table(String name, Boolean registration,
                      @DelegatesTo(DB2Table)
                      @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Table']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.DB2TABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.DB2TABLE), DB2Connection, cl) as DB2Table
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
        def parent = registerConnection(_repositoryConnections.HIVECONNECTION, name, registration) as HiveConnection
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
        defaultJdbcConnection(_repositoryDatasets.HIVETABLE) as HiveConnection
    }

    /** Use default Hive connection for new datasets */
    HiveConnection useHiveConnection(HiveConnection connection) {
        useJdbcConnection(_repositoryDatasets.HIVETABLE, connection) as HiveConnection
    }

    /** Hive table */
    HiveTable hiveTable(String name, Boolean registration,
                        @DelegatesTo(HiveTable)
                        @ClosureParams(value = SimpleType, options = ['getl.hive.HiveTable']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.HIVETABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.HIVETABLE), HiveConnection, cl) as HiveTable
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

    /** Impala connection */
    ImpalaConnection impalaConnection(String name, Boolean registration,
                                      @DelegatesTo(ImpalaConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.impala.ImpalaConnection']) Closure cl = null) {
        def parent = registerConnection(_repositoryConnections.IMPALACONNECTION, name, registration) as ImpalaConnection
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
        defaultJdbcConnection(_repositoryDatasets.IMPALATABLE) as ImpalaConnection
    }

    /** Use default Impala connection for new datasets */
    ImpalaConnection useImpalaConnection(ImpalaConnection connection) {
        useJdbcConnection(_repositoryDatasets.IMPALATABLE, connection) as ImpalaConnection
    }

    /** Impala table */
    ImpalaTable impalaTable(String name, Boolean registration,
                            @DelegatesTo(ImpalaTable)
                            @ClosureParams(value = SimpleType, options = ['getl.impala.ImpalaTable']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.IMPALATABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.IMPALATABLE), ImpalaConnection, cl) as ImpalaTable
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
        def parent = registerConnection(_repositoryConnections.MSSQLCONNECTION, name, registration) as MSSQLConnection
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
        defaultJdbcConnection(_repositoryDatasets.MSSQLTABLE) as MSSQLConnection
    }

    /** Use default MSSQL connection for new datasets */
    MSSQLConnection useMssqlConnection(MSSQLConnection connection) {
        useJdbcConnection(_repositoryDatasets.MSSQLTABLE, connection) as MSSQLConnection
    }

    /** MSSQL database table */
    MSSQLTable mssqlTable(String name, Boolean registration,
                          @DelegatesTo(MSSQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLTable']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.MSSQLTABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.MSSQLTABLE), MSSQLConnection, cl) as MSSQLTable
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
        def parent = registerConnection(_repositoryConnections.MYSQLCONNECTION, name, registration) as MySQLConnection
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
        defaultJdbcConnection(_repositoryDatasets.MYSQLTABLE) as MySQLConnection
    }

    /** Use default MySQL connection for new datasets */
    MySQLConnection useMysqlConnection(MySQLConnection connection) {
        useJdbcConnection(_repositoryDatasets.MYSQLTABLE, connection) as MySQLConnection
    }

    /** MySQL database table */
    MySQLTable mysqlTable(String name, Boolean registration,
                          @DelegatesTo(MySQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLTable']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.MYSQLTABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.MYSQLTABLE), MySQLConnection, cl) as MySQLTable
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
        def parent = registerConnection(_repositoryConnections.NETEZZACONNECTION, name, registration) as NetezzaConnection
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
        defaultJdbcConnection(_repositoryDatasets.NETEZZATABLE) as NetezzaConnection
    }

    /** Use default Netezza connection for new datasets */
    NetezzaConnection useNetezzaConnection(NetezzaConnection connection) {
        useJdbcConnection(_repositoryDatasets.NETEZZATABLE, connection) as NetezzaConnection
    }

    /** Netezza database table */
    NetezzaTable netezzaTable(String name, Boolean registration,
                              @DelegatesTo(NetezzaTable)
                              @ClosureParams(value = SimpleType, options = ['getl.netezza.NetezzaTable']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.NETEZZATABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.NETEZZATABLE), NetezzaConnection, cl) as NetezzaTable
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
        def parent = registerConnection(_repositoryConnections.ORACLECONNECTION, name, registration) as OracleConnection
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
        defaultJdbcConnection(_repositoryDatasets.ORACLETABLE) as OracleConnection
    }

    /** Use default Oracle connection for new datasets */
    OracleConnection useOracleConnection(OracleConnection connection) {
        useJdbcConnection(_repositoryDatasets.ORACLETABLE, connection) as OracleConnection
    }

    /** Oracle table */
    OracleTable oracleTable(String name, Boolean registration,
                            @DelegatesTo(OracleTable)
                            @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleTable']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.ORACLETABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.ORACLETABLE), OracleConnection, cl) as OracleTable
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
        def parent = registerConnection(_repositoryConnections.POSTGRESQLCONNECTION, name, registration) as PostgreSQLConnection
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
        defaultJdbcConnection(_repositoryDatasets.POSTGRESQLTABLE) as PostgreSQLConnection
    }

    /** Use default PostgreSQL connection for new datasets */
    PostgreSQLConnection usePostgresqlConnection(PostgreSQLConnection connection) {
        useJdbcConnection(_repositoryDatasets.POSTGRESQLTABLE, connection) as PostgreSQLConnection
    }

    /** PostgreSQL database table */
    PostgreSQLTable postgresqlTable(String name, Boolean registration,
                                    @DelegatesTo(PostgreSQLTable)
                                    @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLTable']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.POSTGRESQLTABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.POSTGRESQLTABLE), PostgreSQLConnection, cl) as PostgreSQLTable
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
        def parent = registerConnection(_repositoryConnections.VERTICACONNECTION, name, registration) as VerticaConnection
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
        defaultJdbcConnection(_repositoryDatasets.VERTICATABLE) as VerticaConnection
    }

    /** Use default Vertica connection for new datasets */
    VerticaConnection useVerticaConnection(VerticaConnection connection) {
        useJdbcConnection(_repositoryDatasets.VERTICATABLE, connection) as VerticaConnection
    }

    /** Vertica table */
    VerticaTable verticaTable(String name, Boolean registration,
                              @DelegatesTo(VerticaTable)
                              @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaTable']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.VERTICATABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.VERTICATABLE), VerticaConnection, cl) as VerticaTable
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

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(String name, Boolean registration,
                                          @DelegatesTo(NetsuiteConnection)
                                          @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteConnection']) Closure cl = null) {
        def parent = registerConnection(_repositoryConnections.NETSUITECONNECTION, name, registration) as NetsuiteConnection
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
        defaultJdbcConnection(_repositoryDatasets.NETSUITETABLE) as NetsuiteConnection
    }

    /** Use default Netsuite connection for new datasets */
    NetsuiteConnection useNetsuiteConnection(NetsuiteConnection connection) {
        useJdbcConnection(_repositoryDatasets.NETSUITETABLE, connection) as NetsuiteConnection
    }

    /** Netsuite table */
    NetsuiteTable netsuiteTable(String name, Boolean registration,
                                @DelegatesTo(NetsuiteTable)
                                @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteTable']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.NETSUITETABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.NETSUITETABLE), NetsuiteConnection, cl) as NetsuiteTable
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
        def parent = registerConnection(_repositoryConnections.EMBEDDEDCONNECTION, name, registration) as TDS
        if (parent.sqlHistoryFile == null) parent.sqlHistoryFile = _langOpts.tempDBSQLHistoryFile
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
        embeddedConnection(null, false, cl)
    }

    /** Temporary database default connection */
    @SuppressWarnings("GrMethodMayBeStatic")
    TDS embeddedConnection() {
        TDS.storage
    }

    /** Use default temporary connection for new datasets */
    TDS useEmbeddedConnection(TDS connection = TDS.storage) {
        useJdbcConnection(_repositoryDatasets.EMBEDDEDTABLE, connection) as TDS
    }

    /** Table with temporary database */
    TDSTable embeddedTable(String name, Boolean registration,
                           @DelegatesTo(TDSTable)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.EMBEDDEDTABLE, name, registration,
                defaultJdbcConnection(_repositoryDatasets.EMBEDDEDTABLE), TDS, cl) as TDSTable
        if ((name == null || registration) && parent.connection == null)
            parent.connection = TDS.storage
        if (parent.connection != null && (parent.connection as TDS).sqlHistoryFile == null)
            (parent.connection as TDS).sqlHistoryFile = _langOpts.tempDBSQLHistoryFile
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
    TDSTable embeddedTableWithDataset(String name, Dataset sourceDataset,
                                      @DelegatesTo(TDSTable)
                                      @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl = null) {
        if (sourceDataset == null)
            throw new ExceptionDSL("Source dataset cannot be null!")
        if (sourceDataset.field.isEmpty()) {
            sourceDataset.retrieveFields()
            if (sourceDataset.field.isEmpty()) throw new ExceptionDSL("Required field from dataset $sourceDataset")
        }

        TDSTable parent = new TDSTable(connection: defaultJdbcConnection(_repositoryDatasets.EMBEDDEDTABLE) ?: TDS.storage)
        parent.field = sourceDataset.field
        if ((parent.connection as TDS)?.sqlHistoryFile == null)
            (parent.connection as TDS).sqlHistoryFile = _langOpts.tempDBSQLHistoryFile

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
        useJdbcConnection(_repositoryDatasets.QUERYDATASET, connection) as JDBCConnection
    }

    /** JDBC query dataset */
    QueryDataset query(String name, Boolean registration, JDBCConnection connection,
                       @DelegatesTo(QueryDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.QueryDataset']) Closure cl = null) {
        def parent = registerDataset(connection, _repositoryDatasets.QUERYDATASET, name, registration,
                defaultJdbcConnection(_repositoryDatasets.QUERYDATASET), JDBCConnection, cl) as QueryDataset
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
        if (vars != null) parent.queryParams.putAll(vars)

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
        def parent = registerConnection(_repositoryConnections.CSVCONNECTION, name, registration) as CSVConnection
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
        defaultFileConnection(_repositoryDatasets.CSVDATASET) as CSVConnection
    }

    /** Use default CSV connection for new datasets */
    CSVConnection useCsvConnection(CSVConnection connection) {
        useFileConnection(_repositoryDatasets.CSVDATASET, connection) as CSVConnection
    }

    /** CSV delimiter file */
    CSVDataset csv(String name, Boolean registration,
                   @DelegatesTo(CSVDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.CSVDATASET, name, registration,
                defaultFileConnection(_repositoryDatasets.CSVDATASET), CSVConnection, cl) as CSVDataset
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

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(String name, Dataset sourceDataset, Boolean registration,
                              @DelegatesTo(CSVDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl = null) {
        if (sourceDataset == null)
            throw new ExceptionDSL("Dataset cannot be null!")

        def parent = registerDataset(null, _repositoryDatasets.CSVDATASET, name, registration,
                defaultFileConnection(_repositoryDatasets.CSVDATASET), CSVConnection, cl) as CSVDataset

        if (sourceDataset.field.isEmpty()) {
            if (!sourceDataset.connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
                throw new ExceptionDSL("No fields are specified for dataset $sourceDataset and it supports reading fields from metadata!")

            sourceDataset.retrieveFields()
            if (sourceDataset.field.isEmpty())
                throw new ExceptionDSL("Can not read list of field from dataset $sourceDataset!")
        }
        parent.field = sourceDataset.field
        sourceDataset.prepareCsvTempFile(parent)
        runClosure(parent, cl)

        return parent
    }

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(String name, Dataset sourceDataset,
                              @DelegatesTo(CSVDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl = null) {
        csvWithDataset(name, sourceDataset, true, cl)
    }

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(Dataset sourceDataset,
                              @DelegatesTo(CSVDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl = null) {
        csvWithDataset(null, sourceDataset, true, cl)
    }

    /** Excel connection */
    ExcelConnection excelConnection(String name, Boolean registration,
                                    @DelegatesTo(ExcelConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelConnection']) Closure cl = null) {
        def parent = registerConnection(_repositoryConnections.EXCELCONNECTION, name, registration) as ExcelConnection
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
        defaultFileConnection(_repositoryDatasets.EXCELDATASET) as ExcelConnection
    }

    /** Use default Excel connection for new datasets */
    ExcelConnection useExcelConnection(ExcelConnection connection) {
        useOtherConnection(_repositoryDatasets.EXCELDATASET, connection) as ExcelConnection
    }

    /** Excel file */
    ExcelDataset excel(String name, Boolean registration,
                       @DelegatesTo(ExcelDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelDataset']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.EXCELDATASET, name, registration,
                defaultFileConnection(_repositoryDatasets.EXCELDATASET), ExcelConnection, cl) as ExcelDataset
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
        def parent = registerConnection(_repositoryConnections.JSONCONNECTION, name, registration) as JSONConnection
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
        defaultFileConnection(_repositoryDatasets.JSONDATASET) as JSONConnection
    }

    /** Use default Json connection for new datasets */
    JSONConnection useJsonConnection(JSONConnection connection) {
        useFileConnection(_repositoryDatasets.JSONDATASET, connection) as JSONConnection
    }

    /** JSON file */
    JSONDataset json(String name, Boolean registration,
                     @DelegatesTo(JSONDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.json.JSONDataset']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.JSONDATASET, name, registration,
                defaultFileConnection(_repositoryDatasets.JSONDATASET), JSONConnection, cl) as JSONDataset
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
        def parent = registerConnection(_repositoryConnections.XMLCONNECTION, name, registration) as XMLConnection
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
        defaultFileConnection(_repositoryDatasets.XMLDATASET) as XMLConnection
    }

    /** Use default XML connection for new datasets */
    XMLConnection useXmlConnection(XMLConnection connection) {
        useFileConnection(_repositoryDatasets.XMLDATASET, connection) as XMLConnection
    }

    /** XML file */
    XMLDataset xml(String name, Boolean registration,
                   @DelegatesTo(XMLDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.xml.XMLDataset']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.XMLDATASET, name, registration,
                defaultFileConnection(_repositoryDatasets.XMLDATASET), XMLConnection, cl) as XMLDataset
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

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(String name, Boolean registration,
                                              @DelegatesTo(SalesForceConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceConnection']) Closure cl = null) {
        def parent = registerConnection(_repositoryConnections.SALESFORCECONNECTION, name, registration) as SalesForceConnection
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
        defaultOtherConnection(_repositoryDatasets.SALESFORCEDATASET) as SalesForceConnection
    }

    /** Use default SalesForce connection for new datasets */
    SalesForceConnection useSalesforceConnection(SalesForceConnection connection) {
        useOtherConnection(_repositoryDatasets.SALESFORCEDATASET, connection) as SalesForceConnection
    }

    /** SalesForce table */
    SalesForceDataset salesforce(String name, Boolean registration,
                                 @DelegatesTo(SalesForceDataset)
                                 @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceDataset']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.SALESFORCEDATASET, name, registration,
                defaultOtherConnection(_repositoryDatasets.SALESFORCEDATASET), SalesForceConnection, cl) as SalesForceDataset
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
        def parent = registerDataset(null, _repositoryDatasets.SALESFORCEQUERYDATASET, name, registration,
                defaultOtherConnection(_repositoryDatasets.SALESFORCEDATASET), SalesForceConnection, cl) as SalesForceQueryDataset
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

    /** Xero connection */
    XeroConnection xeroConnection(String name, Boolean registration,
                                  @DelegatesTo(XeroConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.xero.XeroConnection']) Closure cl = null) {
        def parent = registerConnection(_repositoryConnections.XEROCONNECTION, name, registration) as XeroConnection
        runClosure(parent, cl)

        return parent
    }

    /** Xero connection */
    XeroConnection xeroConnection(String name,
                                  @DelegatesTo(XeroConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.xero.XeroConnection']) Closure cl = null) {
        xeroConnection(name, false, cl)
    }

    /** Xero connection */
    XeroConnection xeroConnection(@DelegatesTo(XeroConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.xero.XeroConnection']) Closure cl) {
        xeroConnection(null, false, cl)
    }

    /** Xero default connection */
    XeroConnection xeroConnection() {
        defaultOtherConnection(_repositoryDatasets.XERODATASET) as XeroConnection
    }

    /** Use default Xero connection for new datasets */
    XeroConnection useXeroConnection(XeroConnection connection) {
        useOtherConnection(_repositoryDatasets.XERODATASET, connection) as XeroConnection
    }

    /** Xero table */
    XeroDataset xero(String name, Boolean registration,
                     @DelegatesTo(XeroDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.xero.XeroDataset']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.XERODATASET, name, registration,
                defaultOtherConnection(_repositoryDatasets.XERODATASET), XeroConnection, cl) as XeroDataset
        runClosure(parent, cl)

        return parent
    }

    /** Xero table */
    XeroDataset xero(String name,
                     @DelegatesTo(XeroDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.xero.XeroDataset']) Closure cl = null) {
        xero(name, false, cl)
    }

    /** Xero table */
    XeroDataset xero(@DelegatesTo(XeroDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.xero.XeroDataset']) Closure cl) {
        xero(null, false, cl)
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(String name, Boolean registration,
                          @DelegatesTo(TFS)
                          @ClosureParams(value = SimpleType, options = ['getl.tfs.TFS']) Closure cl = null) {
        def parent = registerConnection(_repositoryConnections.CSVTEMPCONNECTION, name, registration) as TFS
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
    @SuppressWarnings("GrMethodMayBeStatic")
    TFS csvTempConnection() {
        TFS.storage
    }

    /** Use default CSV temporary connection for new datasets */
    TFS useCsvTempConnection(TFS connection = TFS.storage) {
        useFileConnection(_repositoryDatasets.CSVTEMPDATASET, connection) as TFS
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(String name, Boolean registration,
                       @DelegatesTo(TFSDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl = null) {
        def parent = registerDataset(null, _repositoryDatasets.CSVTEMPDATASET, name, registration,
                defaultFileConnection(_repositoryDatasets.CSVTEMPDATASET), TFS, cl) as TFSDataset
        if ((name == null || registration) && parent.connection == null)
            parent.connection = TFS.storage
        runClosure(parent, cl)

        return parent
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(String name,
                       @DelegatesTo(TFSDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl = null) {
        csvTemp(name, false, cl)
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(@DelegatesTo(TFSDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl) {
        csvTemp(null, false, cl)
    }

    /** Temporary CSV file */
    TFSDataset csvTempWithDataset(String name, Dataset sourceDataset,
                                  @DelegatesTo(TFSDataset)
                                  @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl = null) {
        if (sourceDataset == null)
            throw new ExceptionDSL("Dataset cannot be null!")

        if (sourceDataset.field.isEmpty()) {
            if (!sourceDataset.connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
                throw new ExceptionDSL("No fields are specified for dataset $sourceDataset and it supports reading fields from metadata!")

            sourceDataset.retrieveFields()
            if (sourceDataset.field.isEmpty())
                throw new ExceptionDSL("Can not read list of field from dataset $sourceDataset!")
        }

        def parent = registerDataset(null, _repositoryDatasets.CSVTEMPDATASET, name, (name != null),
                defaultFileConnection(_repositoryDatasets.CSVTEMPDATASET), TFS, cl) as TFSDataset
        if (parent.connection == null)
            parent.connection = TFS.storage
        parent.field = sourceDataset.field
        sourceDataset.prepareCsvTempFile(parent)
        runClosure(parent, cl)

        return parent
    }

    /** Temporary CSV file */
    TFSDataset csvTempWithDataset(Dataset sourceDataset,
                                  @DelegatesTo(TFSDataset)
                                  @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl = null) {
        csvTempWithDataset(null, sourceDataset, cl)
    }

    /**
     * Copy rows from source to destination dataset
     * <br>Closure gets two parameters: source and destination datasets
     */
    void copyRows(Dataset source, Dataset destination,
                  @DelegatesTo(FlowCopySpec)
                  @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowCopySpec']) Closure cl = null) {
        if (source == null)
            throw new ExceptionDSL('Source dataset cannot be null!')
        if (destination == null)
            throw new ExceptionDSL('Destination dataset cannot be null!')

        def pt = startProcess("Copy rows from $source to $destination")
        def parent = new FlowCopySpec(childOwnerObject, childThisObject, false, null)
        parent.source = source
        parent.destination = destination
        parent.runClosure(cl)
        if (!parent.isProcessed) parent.copyRow(null)
        finishProcess(pt, parent.countRow)
    }

    /** Write rows to destination dataset */
    void rowsTo(Dataset destination,
                @DelegatesTo(FlowWriteSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteSpec']) Closure cl) {
        if (destination == null)
            throw new ExceptionDSL('Destination dataset cannot be null!')
        if (cl == null)
            throw new ExceptionDSL('Required closure code!')

        def pt = startProcess("Write rows to $destination")
        def parent = new FlowWriteSpec(childOwnerObject, childThisObject, false, null)
        parent.destination = destination
        parent.runClosure(cl)
        finishProcess(pt, parent.countRow)
    }

    /** Write rows to destination dataset */
    void rowsTo(@DelegatesTo(FlowWriteSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteSpec']) Closure cl) {
        if (cl == null)
            throw new ExceptionDSL('Required closure code!')
        def destination = DetectClosureDelegate(cl)
        if (destination == null || !(destination instanceof Dataset))
            throw new ExceptionDSL('Can not detect destination dataset!')

        rowsTo(destination, cl)
    }

    /** Write rows to many destination datasets */
    void rowsToMany(Map destinations,
                    @DelegatesTo(FlowWriteManySpec)
                    @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteManySpec']) Closure cl) {
        if (destinations == null || destinations.isEmpty())
            throw new ExceptionDSL('Destination datasets cannot be null or empty!')
        if (cl == null)
            throw new ExceptionDSL('Required closure code!')

        def destNames = [] as List<String>
        (destinations as Map<String, Dataset>).each { destName, ds -> destNames.add("$destName: ${ds.toString()}".toString()) }
        def pt = startProcess("Write rows to $destNames")
        def parent = new FlowWriteManySpec(childOwnerObject, childThisObject, false, null)
        parent.destinations = destinations
        parent.runClosure(cl)
        finishProcess(pt)
    }

    /** Process rows from source dataset */
    void rowsProcess(Dataset source,
                     @DelegatesTo(FlowProcessSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowProcessSpec']) Closure cl) {
        if (source == null)
            throw new ExceptionDSL('Source dataset cannot be null!')
        if (cl == null)
            throw new ExceptionDSL('Required closure code!')
        def pt = startProcess("Read rows from $source")
        def parent = new FlowProcessSpec(childOwnerObject, childThisObject, false, null)
        parent.source = source
        parent.runClosure(cl)
        finishProcess(pt, parent.countRow)
    }

    /** Process rows from source dataset */
    void rowsProcess(@DelegatesTo(FlowProcessSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowProcessSpec']) Closure cl) {
        if (cl == null)
            throw new ExceptionDSL('Required closure code!')
        def source = DetectClosureDelegate(cl)
        if (source == null || !(source instanceof Dataset))
            throw new ExceptionDSL('Can not detect source dataset!')
        rowsProcess(source, cl)
    }

    /** SQL scripter */
    SQLScripter sql(JDBCConnection connection, /* TODO: Added extended variables with table structure */
                    @DelegatesTo(SQLScripter)
                    @ClosureParams(value = SimpleType, options = ['getl.jdbc.SQLScripter']) Closure cl = null) {
        def parent = new SQLScripter()
        def owner = DetectClosureDelegate(cl)

        if (connection != null)
            parent.connection = connection
        else if (owner instanceof JDBCConnection)
            parent.connection = owner as JDBCConnection
        else
            parent.connection = defaultJdbcConnection(_repositoryDatasets.QUERYDATASET)

        parent.logEcho = _langOpts.sqlEchoLogLevel.toString()
        parent.extVars = configContent
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
            throw new ExceptionDSL('Need file manager name value!')

        def parent = registerFileManager(null, name, false) as Manager
        runClosure(parent, cl)

        return parent
    }

    /**
     * Clone file manager
     * @param man original file manager to clone
     * @return cloned file manager
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    Manager cloneFilemanager(Manager man) {
        if (man == null)
            throw new ExceptionDSL('Need object value!')
        return man.cloneManager()
    }

    /**
     * Clone file manager
     * @param newName repository name for cloned file manager
     * @param man original file manager to clone
     * @param cl cloned file manager processing code
     * @return cloned file manager
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    Manager cloneFilemanager(String newName, Manager man,
                             @DelegatesTo(Manager)
                             @ClosureParams(value = SimpleType, options = ['getl.files.Manager']) Closure cl = null) {
        def res = cloneFilemanager(man)
        registerFileManagerObject(res, newName)
        runClosure(res, cl)

        return res
    }

    /** Process local file system */
    FileManager files(String name, Boolean registration,
                      @DelegatesTo(FileManager)
                      @ClosureParams(value = SimpleType, options = ['getl.files.FileManager']) Closure cl = null) {
        def parent = registerFileManager(_repositoryFilemanagers.FILEMANAGER, name, registration) as FileManager
        if (parent.rootPath == null) parent.rootPath = new File('.').absolutePath
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
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
    FileManager files(String name, @DelegatesTo(FileManager)
    @ClosureParams(value = SimpleType, options = ['getl.files.FileManager']) Closure cl = null) {
        files(name, false, cl)
    }

    /** Process local file system */
    FileManager files(@DelegatesTo(FileManager)
                      @ClosureParams(value = SimpleType, options = ['getl.files.FileManager']) Closure cl) {
        files(null, false, cl)
    }

    /** Process ftp file system */
    FTPManager ftp(String name, Boolean registration,
                   @DelegatesTo(FTPManager)
                   @ClosureParams(value = SimpleType, options = ['getl.files.FTPManager']) Closure cl = null) {
        def parent = registerFileManager(_repositoryFilemanagers.FTPMANAGER, name, registration) as FTPManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
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
        def parent = registerFileManager(_repositoryFilemanagers.SFTPMANAGER, name, registration) as SFTPManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
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
        def parent = registerFileManager(_repositoryFilemanagers.HDFSMANAGER, name, registration) as HDFSManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
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
        def disposeConnections = { Map<String, List<ExecutorThread.CloneObject>> list ->
            (list?.connections as List<ExecutorThread.CloneObject>)?.each { ExecutorThread.CloneObject cloneObject ->
                def con = cloneObject.cloneObject as Connection
                if (con != null && con.driver.isSupport(Driver.Support.CONNECT)) con.connected = false
            }

            (list?.filemanagers as List<ExecutorThread.CloneObject>)?.each { ExecutorThread.CloneObject cloneObject ->
                def man = cloneObject.cloneObject as Manager
                if (man != null && man.connected) man.disconnect()
            }
        }

        def parent = new Executor(abortOnError: true)
        parent.disposeThreadResource(disposeConnections)

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
                    @ClosureParams(value = SimpleType, options = ['getl.lang.opts.FileTextSpec']) Closure cl) {
        def parent = new FileTextSpec(childOwnerObject, childThisObject, false, null)
        if (file != null) {
            parent.fileName = (file instanceof File) ? ((file as File).path) : file.toString()
        }
        def pt = startProcess("Processing text file${(parent.fileName != null) ? (' "' + parent.fileName + '"') : ''}", 'byte')
        pt.objectName = 'byte'
        parent.runClosure(cl)
        parent.save()
        pt.name = "Processing text file${(parent.fileName != null) ? (' "' + parent.fileName + '"') : ''}"
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
        parent.sysParams.dslThisObject = childThisObject
        parent.sysParams.dslOwnerObject = childOwnerObject
        runClosure(parent, cl)

        return parent
    }

    /** File path parser */
    @SuppressWarnings("GrMethodMayBeStatic")
    Path filePath(String mask) {
        new Path(mask: mask)
    }

    /** Incremenal history point manager */
    SavePointManager historypoint(String name = null, Boolean registration = false, JDBCConnection connection = null,
                                  @DelegatesTo(SavePointManager)
                                  @ClosureParams(value = SimpleType, options = ['getl.jdbc.SavePointManager']) Closure cl = null) {
        def parent = registerHistoryPoint(connection, name, registration, cl)
        runClosure(parent, cl)

        return parent
    }

    /** Incremenal history point manager */
    SavePointManager historypoint(String name, Boolean registration = false,
                                  @DelegatesTo(SavePointManager)
                                  @ClosureParams(value = SimpleType, options = ['getl.jdbc.SavePointManager']) Closure cl) {
        historypoint(name, registration, null, cl)
    }

    /** Incremenal history point manager */
    SavePointManager historypoint(@DelegatesTo(SavePointManager)
                                  @ClosureParams(value = SimpleType, options = ['getl.jdbc.SavePointManager']) Closure cl) {
        historypoint(null, false, null, cl)
    }

    /**
     * Clone history point manager
     * @param point original history point manager to clone
     * @param con used connection for new history point manager
     * @return cloned history point manager
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    SavePointManager cloneHistorypoint(SavePointManager point, JDBCConnection con = null) {
        if (point == null)
            throw new ExceptionDSL('Need object value!')
        return point.cloneSavePointManager(con) as SavePointManager
    }

    /**
     * Clone history point manager
     * @param newName repository name for cloned history point manager
     * @param point original history point manager to clone
     * @param con used connection for new history point manager
     * @param cl cloned history point manager processing code
     * @return cloned history point manager
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    SavePointManager cloneHistorypoint(String newName, SavePointManager point, JDBCConnection con = null,
                                       @DelegatesTo(SavePointManager)
                                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.SavePointManager']) Closure cl = null) {
        def res = cloneHistorypoint(point, con)
        registerHistoryPointObject(res, newName)
        runClosure(res, cl)

        return res
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
    @SuppressWarnings("GrMethodMayBeStatic")
    Sequence cloneSequence(Sequence seq, JDBCConnection con = null) {
        if (seq == null)
            throw new ExceptionDSL('Need object value!')
        return seq.cloneSequence(con) as Sequence
    }

    /**
     * Clone sequence
     * @param newName repository name for cloned sequence
     * @param seq sequence to clone
     * @param con used connection for new sequence
     * @param cl cloned sequence code
     * @return clone sequence
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    Sequence cloneSequence(String newName, Sequence seq, JDBCConnection con = null,
                           @DelegatesTo(Sequence)
                           @ClosureParams(value = SimpleType, options = ['getl.jdbc.Sequence']) Closure cl = null) {
        def res = cloneSequence(seq, con)
        registerSequenceObject(res, newName)
        runClosure(res, cl)

        return res
    }

    /**
     * Copying files according to the specified rules between file systems
     * @param source source file manager
     * @param destinations list of destination file manager
     * @param cl process parameter setting code
     * @return file copier instance
     */
    FileCopier fileCopier(Manager source, List<Manager> destinations,
                                  @DelegatesTo(FileCopier)
                          @ClosureParams(value = SimpleType, options = ['getl.proc.FileCopier']) Closure cl) {
        if (source == null)
            throw new ExceptionDSL('Required source file manager!')
        if (destinations == null && destinations.isEmpty())
            throw new ExceptionDSL('Required destination file manager!')

        def parent = new FileCopier()
        parent.sysParams.dslThisObject = childThisObject
        parent.sysParams.dslOwnerObject = childOwnerObject
        parent.source = source
        parent.destinations = destinations

        def ptName = "Copy files from [$source]"
        def pt = startProcess(ptName, 'file')
        try {
            parent.ConnectTo([source] + destinations, 1, 1)
            runClosure(parent, cl)
            parent.process()
        }
        finally {
            parent.DisconnectFrom([source] + destinations)
        }
        finishProcess(pt, parent.countFiles)

        return parent
    }

    /**
     * Copying files according to the specified rules between file systems
     * @param source source file manager
     * @param destination destination file manager
     * @param cl process parameter setting code
     * @return file copier instance
     */
    FileCopier fileCopier(Manager source, Manager destination,
                          @DelegatesTo(FileCopier)
                          @ClosureParams(value = SimpleType, options = ['getl.proc.FileCopier']) Closure cl) {
        fileCopier(source, [destination], cl)
    }

    /**
     * Cleaner files according to the specified rules from source file system
     * @param source source file manager
     * @param cl process parameter setting code
     * @return file cleaner instance
     */
    FileCleaner fileCleaner(Manager source,
                          @DelegatesTo(FileCleaner)
                          @ClosureParams(value = SimpleType, options = ['getl.proc.FileCleaner']) Closure cl) {
        if (source == null)
            throw new ExceptionDSL('Required source file manager!')

        def parent = new FileCleaner()
        parent.sysParams.dslThisObject = childThisObject
        parent.sysParams.dslOwnerObject = childOwnerObject
        parent.source = source

        def ptName = "Remove files from [$source]"
        def pt = startProcess(ptName, 'file')
        try {
            parent.ConnectTo([source], 1, 1)
            runClosure(parent, cl)
            parent.process()
        }
        finally {
            parent.DisconnectFrom([source])
        }
        finishProcess(pt, parent.countFiles)

        return parent
    }

    /**
     * Processing files according to the specified rules from source file system
     * @param source source file manager
     * @param archiveStorage archive file storage
     * @param cl process parameter setting code
     * @return file processing instance
     */
    FileProcessing fileProcessing(Manager source,
                            @DelegatesTo(FileProcessing)
                            @ClosureParams(value = SimpleType, options = ['getl.proc.FileProcessing']) Closure cl) {
        if (source == null)
            throw new ExceptionDSL('Required source file manager!')

        def parent = new FileProcessing()
        parent.sysParams.dslThisObject = childThisObject
        parent.sysParams.dslOwnerObject = childOwnerObject
        parent.source = source

        def ptName = "Processing files from [$source]"
        def pt = startProcess(ptName, 'file')
        try {
            parent.ConnectTo([source], 1, 1)
            runClosure(parent, cl)
            parent.process()
        }
        finally {
            parent.DisconnectFrom([source])
        }
        finishProcess(pt, parent.countFiles)

        return parent
    }

    /** Test case instance */
    private GroovyTestCase _testCase

    /** Run test case code */
    GroovyTestCase testCase(@DelegatesTo(GroovyAssert)
                            @ClosureParams(value = SimpleType, options = ['groovy.test.GroovyTestCase']) Closure cl) {
        def parent = _testCase?:new GroovyTestCase()
        def owner = DetectClosureDelegate(cl)
        RunClosure(owner, childThisObject, parent, cl)
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
     * @param cl
     */
    void ifUnitTestMode(Closure cl) {
        if (unitTestMode)
            runClosure(cl.delegate, cl)
    }

    /**
     * Run code if working in application mode
     * @param cl
     */
    void ifRunAppMode(Closure cl) {
        if (!unitTestMode)
            runClosure(cl.delegate, cl)
    }

    void ifInitMode(Closure cl) {
        if (isInitMode)
            runClosure(cl.delegate, cl)
    }

    /** Сonvert code variables to a map */
    Map<String, Object> toVars(Closure cl) {
        def own = DetectClosureDelegate(cl)
        def code = PrepareClosure(own, childThisObject, own, cl)
        def env = (Config.configClassManager instanceof ConfigSlurper)?((Config.configClassManager as ConfigSlurper).environment):'prod'
        return MapUtils.Closure2Map(env, code)
    }

    /* TODO: add Counter repository object */
    /* TODO: add control process history (complete process elements, analog s_copy_to_impala) */
}