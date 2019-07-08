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
import getl.excel.*
import getl.exception.ExceptionGETL
import getl.files.*
import getl.h2.*
import getl.h2.opts.*
import getl.hive.*
import getl.jdbc.*
import getl.jdbc.opts.*
import getl.json.*
import getl.lang.opts.*
import getl.mssql.*
import getl.mysql.*
import getl.netsuite.*
import getl.oracle.*
import getl.postgresql.*
import getl.proc.*
import getl.proc.opts.*
import getl.salesforce.*
import getl.stat.*
import getl.tfs.*
import getl.utils.*
import getl.vertica.*
import getl.vertica.opts.*
import getl.xero.*
import getl.xml.*
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized

import java.util.concurrent.ConcurrentHashMap

/**
 * Getl language script
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class Getl extends Script {
    protected Getl() {
        super()
        init()
    }

    protected Getl(Binding binding) {
        super(binding)
        init()
    }

    private void init() {
        getl.deploy.Version.SayInfo()
        Config.configClassManager = new ConfigSlurper()

        _params.langOpts = new LangSpec()
        _params.repConnections = new ConcurrentHashMap<String, Map<String, Connection>>()
        _params.repDatasets = new ConcurrentHashMap<String, Map<String, Dataset>>()
        _params.repFileManagers = new ConcurrentHashMap<String, Map<String, Manager>>()

        _params.defaultJDBCConnection = new ConcurrentHashMap<String, Map<String, JDBCConnection>>()
        _params.defaultFileConnection = new ConcurrentHashMap<String, Map<String, FileConnection>>()
        _params.defaultOtherConnection = new ConcurrentHashMap<String, Map<String, FileConnection>>()
    }

    @Override
    Object run() { return this }

    /** Run GETL lang closure */
    static def Dsl(@DelegatesTo(Getl) Closure cl) {
        def parent = new Getl()
        parent.runClosure(parent, cl)
    }

    Map<String, Object> _params = new ConcurrentHashMap<String, Object>()
    /** Set language parameters */
    protected void setGetlParams(Map<String, Object> importParams) { _params = importParams }

    public final String CSVCONNECTION = 'getl.csv.CSVConnection'
    public final String DB2CONNECTION = 'getl.db2.DB2Connection'
    public final String EXCELCONNECTION = 'getl.excel.ExcelConnection'
    public final String H2CONNECTION = 'getl.h2.H2Connection'
    public final String HIVECONNECTION = 'getl.hive.HiveConnection'
    public final String JDBCCONNECTION = 'getl.jdbc.JDBCConnection'
    public final String JSONCONNECTION = 'getl.json.JSONConnection'
    public final String MSSQLCONNECTION = 'getl.mssql.MSSQLConnection'
    public final String MYSQLCONNECTION = 'getl.mysql.MySQLConnection'
    public final String NETSUITECONNECTION = 'getl.netsuite.NetsuiteConnection'
    public final String ORACLECONNECTION = 'getl.oracle.OracleConnection'
    public final String POSTGRESQLCONNECTION = 'getl.postgresql.PostgreSQLConnection'
    public final String SALESFORCECONNECTION = 'getl.salesforce.SalesForceConnection'
    public final String EMBEDDEDCONNECTION = 'getl.tfs.TDS'
    public final String VERTICACONNECTION = 'getl.vertica.VerticaConnection'
    public final String XEROCONNECTION = 'getl.xero.XeroConnection'
    public final String XMLCONNECTION = 'getl.xml.XMLConnection'

    protected final List<String> listConnectionClasses = [
            CSVCONNECTION, DB2CONNECTION, EXCELCONNECTION, H2CONNECTION, HIVECONNECTION, JDBCCONNECTION,
            JSONCONNECTION, MSSQLCONNECTION, MYSQLCONNECTION, NETSUITECONNECTION, ORACLECONNECTION,
            POSTGRESQLCONNECTION, SALESFORCECONNECTION, EMBEDDEDCONNECTION, VERTICACONNECTION, XEROCONNECTION,
            XMLCONNECTION
    ]

    public final String CSVDATASET = 'getl.csv.CSVDataset'
    public final String CSVTEMPDATASET = 'getl.tfs.TFSDataset'
    public final String DB2TABLE = 'getl.db2.DB2Table'
    public final String EXCELDATASET = 'getl.excel.ExcelDataset'
    public final String H2TABLE = 'getl.h2.H2Table'
    public final String HIVETABLE = 'getl.hive.HiveTable'
    public final String JSONDATASET = 'getl.json.JSONDataset'
    public final String MSSQLTABLE = 'getl.mssql.MSSQLTable'
    public final String MYSQLTABLE = 'getl.mysql.MySQLTable'
    public final String NETSUITETABLE = 'getl.netsuite.NetsuiteTable'
    public final String ORACLETABLE = 'getl.oracle.OracleTable'
    public final String QUERYDATASET = 'getl.jdbc.QueryDataset'
    public final String POSTGRESQLTABLE = 'getl.postgresql.PostgreSQLTable'
    public final String SALESFORCEDATASET = 'getl.salesforce.SalesForceDataset'
    public final String TABLEDATASET = 'getl.jdbc.TableDataset'
    public final String EMBEDDEDTABLE = 'getl.tfs.TDSTable'
    public final String VERTICATABLE = 'getl.vertica.VerticaTable'
    public final String XERODATASET = 'getl.xero.XeroDataset'
    public final String XMLDATASET = 'getl.xml.XMLDataset'

    protected final List<String> listDatasetClasses = [
            CSVDATASET, CSVTEMPDATASET, DB2TABLE, EXCELDATASET, H2TABLE, HIVETABLE, JSONDATASET, MSSQLTABLE,
            MYSQLTABLE, NETSUITETABLE, ORACLETABLE, QUERYDATASET, POSTGRESQLTABLE, SALESFORCEDATASET, TABLEDATASET,
            EMBEDDEDTABLE, VERTICATABLE, XERODATASET, XMLDATASET
    ]

    public final String FILEMANAGER = 'getl.files.FileManager'
    public final String FTPMANAGER = 'getl.files.FTPManager'
    public final String HDFSMANAGER = 'getl.files.HDFSManager'
    public final String SFTPMANAGER = 'getl.files.SFTPManager'

    protected final List<String> listFileManagerClasses = [
            FILEMANAGER, FTPMANAGER, HDFSMANAGER, SFTPMANAGER
    ]

    /** Fix start process */
    ProcessTime startProcess(String name) {
        return (langOpts.processTimeTracing)?new ProcessTime(name: name):null
    }

    /** Fix finish process */
    static void finishProcess(ProcessTime pt, Long countRow = null) {
        if (pt != null) pt.finish(countRow)
    }

    /** GETL DSL options */
    void profile(String name, @DelegatesTo(ProfileSpec) Closure cl) {
        def stat = new ProfileSpec(profileName: name)
        stat.startProfile()
        runClosure(stat, cl)
        stat.finishProfile()
    }

    /** GETL DSL options */
    protected LangSpec getLangOpts() { _params.langOpts as LangSpec }

    /** Repository object name */
    protected static String repObjectName(String name) { name.toLowerCase() }

    /** Connections repository */
    protected Map<String, Map<String, Connection>> getConnections() {
        _params.repConnections as Map<String, Map<String, Connection>>
    }

    /** Return list of repository connections for specified class */
    @Synchronized
    List<String> listConnections(String connectionClassName, Closure<Boolean> cl = null) {
        if (connectionClassName ==  null)
            throw new ExceptionGETL('Connection class name cannot be null!')
        if (!(connectionClassName in listConnectionClasses))
            throw new ExceptionGETL("$connectionClassName is not connection class!")

        def res = [] as List<String>
        def sect = connections.get(connectionClassName) as Map<String, Connection>
        if (sect != null && !sect.isEmpty())  {
            sect.each { String name, Connection connection ->
                if (cl == null || BoolUtils.IsValue(cl.call(name, connection))) res << name
            }
        }

        return res
    }

    /** Process repository connections for specified class */
    void processConnections(String connectionClassName, Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Process required closure code!')

        def list = listConnections(connectionClassName)
        list?.each { String name ->
            cl.call(name)
        }
    }

    /** Check connection registration in repository */
    @Synchronized
    protected Boolean isRegisteredConnection(String connectionClassName, String name) {
        if (connectionClassName ==  null)
            throw new ExceptionGETL('Connection class name cannot be null!')
        if (!(connectionClassName in listConnectionClasses))
            throw new ExceptionGETL("$connectionClassName is not connection class!")

        if (name ==  null) return null

        def sect = connections.get(connectionClassName) as Map<String, Connection>
        if (sect == null) return false
        return sect.containsKey(name)
    }

    /** Register connection in repository */
    @Synchronized
    protected Connection registerConnection(String connectionClassName, String name) {
        if (connectionClassName ==  null)
            throw new ExceptionGETL('Connection class name cannot be null!')
        if (!(connectionClassName in listConnectionClasses))
            throw new ExceptionGETL("$connectionClassName is not connection class!")

        if (name == null) {
            return Connection.CreateConnection(connection: connectionClassName) as Connection
        }

        def sect = connections.get(connectionClassName) as Map<String, Connection>
        if (sect == null) {
            sect = new ConcurrentHashMap<String, Connection>()
            connections.put(connectionClassName, sect)
        }

        def repName = repObjectName(name)
        def obj = sect.get(repName)
        if (obj == null) {
            if (langOpts.validRegisterObjects || Thread.currentThread() instanceof ExecutorThread)
                throw new ExceptionGETL("Connection \"$name\" with type \"$connectionClassName\" is not exist!")

            obj = Connection.CreateConnection(connection: connectionClassName) as Connection
            sect.put(repName, obj)
        }

        if (langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
            def thread = Thread.currentThread() as ExecutorThread
            obj = thread.registerCloneObject('connections', obj,
                    { (it as Connection).cloneConnection() } )
        }

        return obj
    }

    /** Register connection object in repository */
    @Synchronized
    void registerConnection(Connection obj, String name, Boolean validExist = true) {
        if (obj == null) throw new ExceptionGETL("Connection object cannot be null!")
        def className = obj.getClass().name
        if (!(className in listConnectionClasses))
            throw new ExceptionGETL("Unknown connection class $className!")

        def sect = connections.get(className) as Map<String, Connection>
        if (sect == null) {
            sect = new ConcurrentHashMap<String, Connection>()
            connections.put(className, sect)
        }

        def repName = repObjectName(name)
        if (validExist && sect.containsKey(repName)) throw new ExceptionGETL("Connection object \"$name\" already registered in repository!")
        sect.put(repName, obj)
    }

    /** Tables repository */
    protected Map<String, Map<String, Dataset>> getDatasets() {
        _params.repDatasets as Map<String, Map<String, Dataset>>
    }

    /** Return list of repository datasets for specified class */
    @Synchronized
    List<String> listDatasets(String datasetClassName, Closure<Boolean> cl = null) {
        if (datasetClassName ==  null)
            throw new ExceptionGETL('Dataset class name cannot be null!')
        if (!(datasetClassName in listDatasetClasses))
            throw new ExceptionGETL("$datasetClassName is not dataset class!")

        def res = [] as List<String>
        def sect = datasets.get(datasetClassName) as Map<String, Dataset>
        if (sect != null && !sect.isEmpty())  {
            sect.each { String name, Dataset dataset ->
                if (cl == null || BoolUtils.IsValue(cl.call(name, dataset))) res << name
            }
        }

        return res
    }

    /** Process repository datasets for specified class */
    void processDatasets(String datasetClassName, Closure<Boolean> cl) {
        if (cl == null)
            throw new ExceptionGETL('Process required closure code!')

        def list = listDatasets(datasetClassName)
        list?.each { String name ->
            cl.call(name)
        }
    }

    /** Return list of repository datasets for specified class connection */
    @Synchronized
    List<String> listDatasetsWithConnection(String connectionClassName, Closure<Boolean> cl = null) {
        if (connectionClassName ==  null)
            throw new ExceptionGETL('Connection class name cannot be null!')
        if (!(connectionClassName in listConnectionClasses))
            throw new ExceptionGETL("$connectionClassName is not dataset class!")

        def res = [] as List<String>
        datasets.each { String sectClassName, Map<String, Dataset> section ->
            section.each { String name, Dataset dataset ->
                if (dataset.connection.getClass().name == connectionClassName) {
                    if (cl == null || BoolUtils.IsValue(cl.call(name, dataset))) res << name
                }
            }
        }

        return res
    }

    /** Process repository datasets for specified class connection */
    void processDatasetsWithConnection(String connectionClassName, Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Process required closure code!')

        def list = listDatasetsWithConnection(connectionClassName)
        list?.each { String name ->
            cl.call(name)
        }
    }

    /** Set default connection for use in datasets */
    @Synchronized
    protected void setDefaultConnection(String datasetClassName, Dataset ds) {
        if (datasetClassName ==  null)
            throw new ExceptionGETL('Dataset class name cannot be null!')
        if (!(datasetClassName in listDatasetClasses))
            throw new ExceptionGETL("$datasetClassName is not dataset class!")

        if (ds instanceof TDSTable || ds instanceof TFSDataset) return

        if (ds instanceof JDBCDataset) {
            def con = defaultJdbcConnection(datasetClassName)
            if (con != null) ds.connection = con
        }
        else if (ds instanceof FileDataset) {
            def con = defaultFileConnection(datasetClassName)
            if (con != null) ds.connection = con
        }
        else {
            def con = defaultOtherConnection(datasetClassName)
            if (con != null) ds.connection = con
        }
    }

    /** Last used JDBC default connection */
    JDBCConnection getLastJdbcDefaultConnection() { _params.lastJDBCDefaultConnection as JDBCConnection }

    /** Default JDBC connection for datasets */
    JDBCConnection defaultJdbcConnection(String datasetClassName = null) {
        JDBCConnection res
        if (datasetClassName == null)
            res = lastJdbcDefaultConnection
        else {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")

            res = (_params.defaultJDBCConnection as Map<String, JDBCConnection>).get(datasetClassName) as JDBCConnection
            if (res == null && lastJdbcDefaultConnection != null && datasetClassName == QUERYDATASET)
                res = lastJdbcDefaultConnection
        }

        return res
    }

    /** Use specified JDBC connection as default */
    void useJdbcConnection(String datasetClassName, JDBCConnection value) {
        if (datasetClassName != null) {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")
            (_params.defaultJDBCConnection as Map<String, JDBCConnection>).put(datasetClassName, value)
        }
        _params.lastJDBCDefaultConnection = value
    }

    /** Use default H2 connection for new datasets */
    void useJdbcConnection(JDBCConnection connection) {
        useJdbcConnection(TABLEDATASET, connection)
    }

    /** Last used file default connection */
    FileConnection getLastFileDefaultConnection() { _params.lastFileDefaultConnection as FileConnection }

    /** Default file connection for datasets */
    FileConnection defaultFileConnection(String datasetClassName = null) {
        FileConnection res
        if (datasetClassName == null)
            res = lastFileDefaultConnection
        else {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")

            res = (_params.defaultFileConnection as Map<String, FileConnection>).get(datasetClassName) as FileConnection
        }

        return res
    }

    /** Use specified file connection as default */
    void useFileConnection(String datasetClassName, FileConnection value) {
        if (datasetClassName != null) {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")

            (_params.defaultFileConnection as Map<String, FileConnection>).put(datasetClassName, value)
        }
        _params.lastFileDefaultConnection = value
    }

    /** Last used other type default connection */
    Connection getLastOtherDefaultConnection() { _params.lastOtherDefaultConnection as Connection }

    /** Default other type connection for datasets */
    Connection defaultOtherConnection(String datasetClassName = null) {
        Connection res
        if (datasetClassName == null)
            res = lastOtherDefaultConnection
        else {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")

            res = (_params.defaultOtherConnection as Map<String, Connection>).get(datasetClassName) as Connection
        }

        return res
    }

    /** Use specified other type connection as default */
    void useOtherConnection(String datasetClassName, Connection value) {
        if (datasetClassName != null) {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")

            (_params.defaultOtherConnection as Map<String, Connection>).put(datasetClassName, value)
        }
        _params.lastOtherDefaultConnection = value
    }

    /** Check dataset registration in repository */
    @Synchronized
    protected Boolean isRegisteredDataset(String datasetClassName, String name) {
        if (datasetClassName ==  null)
            throw new ExceptionGETL('Dataset class name cannot be null!')
        if (!(datasetClassName in listDatasetClasses))
            throw new ExceptionGETL("$datasetClassName is not dataset class!")

        if (name ==  null) return null
        def sect = datasets.get(datasetClassName) as Map<String, Dataset>
        if (sect == null) return false
        return sect.containsKey(name)
    }

    /** Register dataset in repository */
    @Synchronized
    protected Dataset registerDataset(String datasetClassName, String name) {
        if (datasetClassName ==  null)
            throw new ExceptionGETL('Dataset class name cannot be null!')
        if (!(datasetClassName in listDatasetClasses))
            throw new ExceptionGETL("$datasetClassName is not dataset class!")

        Dataset obj
        if (name == null) {
            obj = Dataset.CreateDataset(dataset: datasetClassName) as Dataset
            setDefaultConnection(datasetClassName, obj)
            if (obj.connection != null && langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
                def thread = Thread.currentThread() as ExecutorThread
                obj.connection = thread.registerCloneObject('connections', obj.connection,
                        { (it as Connection).cloneConnection() }) as Connection
            }
        }
        else {
            def sect = datasets.get(datasetClassName) as Map<String, Dataset>
            if (sect == null) {
                sect = new ConcurrentHashMap<String, Dataset>()
                datasets.put(datasetClassName, sect)
            }

            def repName = repObjectName(name)
            obj = sect.get(repName)
            if (obj == null) {
                if (langOpts.validRegisterObjects || Thread.currentThread() instanceof ExecutorThread)
                    throw new ExceptionGETL("Dataset \"$name\" with type \"$datasetClassName\" is not exist!")

                obj = Dataset.CreateDataset(dataset: datasetClassName) as Dataset
                setDefaultConnection(datasetClassName, obj)
                sect.put(repName, obj)
            }

            if (langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
                def thread = Thread.currentThread() as ExecutorThread
                if (obj.connection != null) {
                    def connection = thread.registerCloneObject('connections', obj.connection,
                            { (it as Connection).cloneConnection() }) as Connection
                    obj = thread.registerCloneObject('datasets', obj,
                            { (it as Dataset).cloneDataset(connection) }) as Dataset
                }
                else {
                    obj = thread.registerCloneObject('datasets', obj,
                            { (it as Dataset).cloneDataset() }) as Dataset
                }
            }
        }

        return obj
    }

    /** Register dataset in repository */
    @Synchronized
    void registerDataset(Dataset obj, String name, Boolean validExist = true) {
        if (obj == null) throw new ExceptionGETL("Dataset object cannot be null!")

        def className = obj.getClass().name
        if (!(className in listDatasetClasses))
            throw new ExceptionGETL("Unknown dataset class $className!")

        def sect = datasets.get(className) as Map<String, Dataset>
        if (sect == null) {
            sect = new ConcurrentHashMap<String, Dataset>()
            datasets.put(className, sect)
        }

        def repName = repObjectName(name)
        if (validExist && sect.containsKey(repName)) throw new ExceptionGETL("Dataset object \"$name\" already registered in repository!")
        sect.put(repName, obj)
    }

    /** Create CSV temporary dataset for dataset */
    void createCsvTemp(String name, Dataset dataset) {
        if (name ==  null) throw new ExceptionGETL('Name cannot be null!')
        if (dataset ==  null) throw new ExceptionGETL('Dataset cannot be null!')
        TFSDataset csvTemp = dataset.csvTempFile
        registerDataset(csvTemp, name, true)
    }

    /** File managers repository */
    protected Map<String, Map<String, Manager>> getFileManagers() {
        _params.repFileManagers as Map<String, Map<String, Manager>>

    }

    /** Return list of repository file managers for specified class */
    @Synchronized
    List<String> listFileManagers(String fileManagerClassName, Closure<Boolean> cl = null) {
        if (fileManagerClassName ==  null)
            throw new ExceptionGETL('File manager class name cannot be null!')
        if (!(fileManagerClassName in listFileManagerClasses))
            throw new ExceptionGETL("$fileManagerClassName is not file manager class!")

        def res = [] as List<String>
        def sect = fileManagers.get(fileManagerClassName) as Map<String, Manager>
        if (sect != null && !sect.isEmpty())  {
            sect.each { String name, Manager manager ->
                if (cl == null || BoolUtils.IsValue(cl.call(name, manager))) res << name
            }
        }

        return res
    }

    /** Process repository file managers for specified class */
    void processFileManagers(String fileManagerClassName, Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Process required closure code!')

        def list = listFileManagers(fileManagerClassName)
        list?.each { String name ->
            cl.call(name)
        }
    }

    /** Check dataset registration in repository */
    @Synchronized
    protected Boolean isRegisteredFileManager(String fileManagerClassName, String name) {
        if (fileManagerClassName ==  null)
            throw new ExceptionGETL('File manager class name cannot be null!')
        if (!(fileManagerClassName in listFileManagerClasses))
            throw new ExceptionGETL("$fileManagerClassName is not file manager class!")

        if (name ==  null) return null
        def sect = fileManagers.get(fileManagerClassName) as Map<String, Manager>
        if (sect == null) return false
        return sect.containsKey(name)
    }

    /** Register file manager in repository */
    @Synchronized
    protected Manager registerFileManager(String fileManagerClassName, String name) {
        if (fileManagerClassName ==  null)
            throw new ExceptionGETL('File manager class name cannot be null!')
        if (!(fileManagerClassName in listFileManagerClasses))
            throw new ExceptionGETL("$fileManagerClassName is not file manager class!")

        if (name == null) {
            return FileManager.CreateManager(manager: fileManagerClassName) as Manager
        }

        def sect = fileManagers.get(fileManagerClassName) as Map<String, Manager>
        if (sect == null) {
            sect = new ConcurrentHashMap<String, Manager>()
            fileManagers.put(fileManagerClassName, sect)
        }

        def repName = repObjectName(name)
        def obj = sect.get(repName)
        if (obj == null) {
            if (langOpts.validRegisterObjects || Thread.currentThread() instanceof ExecutorThread)
                throw new ExceptionGETL("File manager \"$name\" with type \"$fileManagerClassName\" is not exist!")

            obj = Manager.CreateManager(connection: fileManagerClassName) as Manager
            sect.put(repName, obj)
        }

        if (langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
            def thread = Thread.currentThread() as ExecutorThread
            obj = thread.registerCloneObject('filemanagers', obj,
                    { (it as FileManager).cloneManager() } )
        }

        return obj
    }

    /** Register file manager object in repository */
    @Synchronized
    void registerFileManager(Manager obj, String name, Boolean validExist = true) {
        if (obj == null) throw new ExceptionGETL("File manager object cannot be null!")
        def className = obj.getClass().name
        if (!(className in listFileManagerClasses))
            throw new ExceptionGETL("$className is not file manager class!")

        def sect = fileManagers.get(className) as Map<String, Manager>
        if (sect == null) {
            sect = new ConcurrentHashMap<String, Manager>()
            fileManagers.put(className, sect)
        }

        def repName = repObjectName(name)
        if (validExist && sect.containsKey(repName)) throw new ExceptionGETL("File manager object \"$name\" already registered in repository!")
        sect.put(repName, obj)
    }

    /** Load and run groovy script file */
    void runGroovyFile(String fileName, Map vars = [:]) {
        File sourceFile = new File(fileName)
        def groovyClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(sourceFile)
        runGroovyClass(groovyClass, vars)
    }

    /** Load and run groovy script by class name */
    void runGroovyScript(String className, Map vars = [:]) {
        def groovyClass = Class.forName(className)
        runGroovyClass(groovyClass, vars)
    }

    /** Script call arguments */
    public Map<String, Object> scriptArgs = [:]

    /** Load and run groovy script by class */
    void runGroovyClass(Class groovyClass, Map<String, Object> vars = [:]) {
        def script = (GroovyObject)groovyClass.newInstance() as Script
        configVars.putAll(vars)
        if (script instanceof Getl) {
            def scriptGetl = script as Getl
            scriptGetl.setGetlParams(_params)
        }
        script.binding = new Binding([scriptArgs: MapUtils.DeepCopy(vars)?:([:] as Map<String, Object>)])
        script.run()
    }

    /** Run closure with call parent parameter */
    protected void runClosure(Object parent, Closure cl) {
        if (cl == null) return
        def code = cl.rehydrate(this, parent, this)
        code.resolveStrategy = Closure.OWNER_FIRST
        code.call(parent)
    }

    /** Run closure with call one parameter */
    protected void runClosure(Object parent, Closure cl, Object param) {
        if (cl == null) return
        def code = cl.rehydrate(this, parent, this)
        code.resolveStrategy = Closure.OWNER_FIRST
        code.call(param)
    }

    /** Run closure with call two parameters */
    protected void runClosure(Object parent, Closure cl, Object param1, Object param2) {
        if (cl == null) return
        def code = cl.rehydrate(this, parent, this)
        code.resolveStrategy = Closure.OWNER_FIRST
        code.call(param1, param2)
    }

    /** Current configuration content */
    static Map<String, Object> getConfigContent() { Config.content }

    /** Current configuration vars */
    static Map<String, Object> getConfigVars() { Config.vars }

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

    /** Current date and time */
    static Date getNow() { DateUtils.Now() }

    /** Random integer value */
    static int getRandomInt() { GenerationUtils.GenerateInt() }

    /** Random integer value */
    static int randomInt(Integer min, Integer max) { GenerationUtils.GenerateInt(min, max) }

    /** Random long value */
    static int getRandomLong() { GenerationUtils.GenerateLong() }

    /** Random date value */
    static Date getRandomDate() { GenerationUtils.GenerateDate() }

    /** Random date value */
    static Date randomDate(Integer days) { GenerationUtils.GenerateDate(days) }

    /** Random timestamp value */
    static Date getRandomDateTime() { GenerationUtils.GenerateDateTime() }

    /** Random date value */
    static Date randomDateTime(int secs) { GenerationUtils.GenerateDateTime(secs) }

    /** Random numeric value */
    static BigDecimal getRandomNumber() { GenerationUtils.GenerateNumeric() }

    /** Random numeric value */
    static BigDecimal randomNumber(int prec) { GenerationUtils.GenerateNumeric(prec) }

    /** Random numeric value */
    static BigDecimal randomNumber(int len, int prec) { GenerationUtils.GenerateNumeric(len, prec) }

    /** Random boolean value */
    static Boolean getRandomBoolean() { GenerationUtils.GenerateBoolean() }

    /** Random double value */
    static Double getRandomDouble() { GenerationUtils.GenerateDouble() }

    /** Random string value */
    static String getRandomString() { GenerationUtils.GenerateString(255) }

    /** Random string value */
    static String randomString(int len) { GenerationUtils.GenerateString(len) }

    /** GETL DSL options */
    LangSpec options(@DelegatesTo(LangSpec) Closure cl = null) {
        runClosure(langOpts, cl)

        return langOpts as LangSpec
    }

    /** Configuration options */
    ConfigSpec configuration(@DelegatesTo(ConfigSpec) Closure cl = null) {
        def parent = new ConfigSpec()
        runClosure(parent, cl)

        return parent
    }

    /** Log options */
    LogSpec logging(@DelegatesTo(LogSpec) Closure cl = null) {
        def parent = new LogSpec()
        runClosure(parent, cl)

        return parent
    }

    /** JDBC connection */
    JDBCConnection jdbcConnection(String name = null, String connectionClassName = null, @DelegatesTo(JDBCConnection) Closure cl = null) {
        def parent = registerConnection(connectionClassName?:JDBCCONNECTION, name) as JDBCConnection
        runClosure(parent, cl)

        return parent
    }

    /** JDBC table */
    TableDataset table(String name, @DelegatesTo(TableDataset) Closure cl = null) {
        def isRegistered = isRegisteredDataset(TABLEDATASET, name)
        def parent = registerDataset(TABLEDATASET, name) as TableDataset
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** JDBC table */
    TableDataset table(@DelegatesTo(TableDataset) Closure cl = null) {
        table(null, cl)
    }

    /** Bulk load CSV file to JDBC table */
    void tableBulkLoad(String name, CSVDataset source, @DelegatesTo(BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, table(name), source, cl)
    }

    /** Bulk load CSV file to JDBC table */
    void tableBulkLoad(String name, @DelegatesTo(BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, table(name), null, cl)
    }

    /** Bulk load csv file to jdbc destination */
    protected void bulkLoadTableDataset(String name, TableDataset dest, CSVDataset source, Closure cl) {
        if (dest == null) throw new ExceptionGETL("Destination dataset cannot be null!")
        def parent = dest.bulkLoadOpts(cl)
        if (source == null) {
            if (name == null) throw new ExceptionGETL("For define the source dataset by default need set name!")
            source = csvTemp(name)
        }

        def pt = startProcess("Bulk load file $source to $dest")
        if (parent.onInit != null) parent.onInit.call()
        parent.prepareParams()
        dest.bulkLoadFile([source: source])
        if (parent.onDone != null) parent.onDone.call()
        finishProcess(pt, dest.updateRows)
    }

    /** H2 database connection */
    H2Connection h2Connection(String name, @DelegatesTo(H2Connection) Closure cl = null) {
        jdbcConnection(name, H2CONNECTION, cl) as H2Connection
    }

    /** H2 database connection */
    H2Connection h2Connection(@DelegatesTo(H2Connection) Closure cl = null) {
        h2Connection(null, cl)
    }

    /** Use default H2 connection for new datasets */
    void useH2Connection(H2Connection connection) {
        useJdbcConnection(H2TABLE, connection)
    }

    /** H2 database table */
    H2Table h2Table(String name, @DelegatesTo(H2Table) Closure cl = null) {
        def isRegistered = isRegisteredDataset(H2TABLE, name)
        def parent = registerDataset(H2TABLE, name) as H2Table
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** H2 database table */
    H2Table h2Table(@DelegatesTo(H2Table) Closure cl = null) {
        h2Table(null, cl)
    }

    /** Bulk load CSV file to H2 database table */
    void h2BulkLoad(String name, CSVDataset source, @DelegatesTo(H2BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, h2Table(name), source, cl)
    }

    /** Bulk load CSV file to H2 database table */
    void h2BulkLoad(String name, @DelegatesTo(H2BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, h2Table(name), null, cl)
    }

    /** DB2 connection */
    DB2Connection db2Connection(String name, @DelegatesTo(DB2Connection) Closure cl = null) {
        jdbcConnection(name, DB2CONNECTION, cl) as DB2Connection
    }

    /** DB2 connection */
    DB2Connection db2Connection(@DelegatesTo(DB2Connection) Closure cl = null) {
        db2Connection(null, cl)
    }

    /** Use default DB2 connection for new datasets */
    void useDb2Connection(DB2Connection connection) {
        useJdbcConnection(DB2TABLE, connection)
    }

    /** DB2 database table */
    DB2Table db2Table(String name, @DelegatesTo(DB2Table) Closure cl = null) {
        def isRegistered = isRegisteredDataset(DB2TABLE, name)
        def parent = registerDataset(DB2TABLE, name) as DB2Table
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** Hive connection */
    HiveConnection hiveConnection(String name, @DelegatesTo(HiveConnection) Closure cl = null) {
        jdbcConnection(name, HIVECONNECTION, cl) as HiveConnection
    }

    /** Hive connection */
    HiveConnection hiveConnection(@DelegatesTo(HiveConnection) Closure cl = null) {
        hiveConnection(null, cl)
    }

    /** Use default Hive connection for new datasets */
    void useHiveConnection(HiveConnection connection) {
        useJdbcConnection(HIVETABLE, connection)
    }

    /** Hive table */
    HiveTable hiveTable(String name, @DelegatesTo(HiveTable) Closure cl = null) {
        def isRegistered = isRegisteredDataset(HIVETABLE, name)
        def parent = registerDataset(HIVETABLE, name) as HiveTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** Hive table */
    HiveTable hiveTable(@DelegatesTo(HiveTable) Closure cl = null) {
        hiveTable(null, cl)
    }

    /** Bulk load CSV file to Hive table */
    void hiveBulkLoad(String name, CSVDataset source, @DelegatesTo(BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, hiveTable(name), source, cl)
    }

    /** Bulk load CSV file to Hive table */
    void hiveBulkLoad(String name, @DelegatesTo(BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, hiveTable(name), null, cl)
    }

    /** MSSQL connection */
    MSSQLConnection mssqlConnection(String name, @DelegatesTo(MSSQLConnection) Closure cl = null) {
        jdbcConnection(name, MSSQLCONNECTION, cl) as MSSQLConnection
    }

    /** MSSQL connection */
    MSSQLConnection mssqlConnection(@DelegatesTo(MSSQLConnection) Closure cl = null) {
        mssqlConnection(null, cl)
    }

    /** Use default MSSQL connection for new datasets */
    void useMssqlConnection(MSSQLConnection connection) {
        useJdbcConnection(MSSQLTABLE, connection)
    }

    /** MSSQL database table */
    MSSQLTable mssqlTable(String name, @DelegatesTo(MSSQLTable) Closure cl = null) {
        def isRegistered = isRegisteredDataset(MSSQLTABLE, name)
        def parent = registerDataset(MSSQLTABLE, name) as MSSQLTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(String name, @DelegatesTo(MySQLConnection) Closure cl = null) {
        jdbcConnection(name, MYSQLCONNECTION, cl) as MySQLConnection
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(@DelegatesTo(MySQLConnection) Closure cl = null) {
        mysqlConnection(null, cl)
    }

    /** Use default MySQL connection for new datasets */
    void useMysqlConnection(MySQLConnection connection) {
        useJdbcConnection(MYSQLTABLE, connection)
    }

    /** MySQL database table */
    MySQLTable mysqlTable(String name, @DelegatesTo(MySQLTable) Closure cl = null) {
        def isRegistered = isRegisteredDataset(MYSQLTABLE, name)
        def parent = registerDataset(MYSQLTABLE, name) as MySQLTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** Oracle connection */
    OracleConnection oracleConnection(String name, @DelegatesTo(OracleConnection) Closure cl = null) {
        jdbcConnection(name, ORACLECONNECTION, cl) as OracleConnection
    }

    /** Oracle connection */
    OracleConnection oracleConnection(@DelegatesTo(OracleConnection) Closure cl = null) {
        oracleConnection(null, cl)
    }

    /** Use default Oracle connection for new datasets */
    void useOracleConnection(OracleConnection connection) {
        useJdbcConnection(ORACLETABLE, connection)
    }

    /** Oracle table */
    OracleTable oracleTable(String name, @DelegatesTo(OracleTable) Closure cl = null) {
        def isRegistered = isRegisteredDataset(ORACLETABLE, name)
        def parent = registerDataset(ORACLETABLE, name) as OracleTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(String name, @DelegatesTo(PostgreSQLConnection) Closure cl = null) {
        jdbcConnection(name, POSTGRESQLCONNECTION, cl) as PostgreSQLConnection
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(@DelegatesTo(PostgreSQLConnection) Closure cl = null) {
        postgresqlConnection(null, cl)
    }

    /** Use default PostgreSQL connection for new datasets */
    void usePostgresqlConnection(PostgreSQLConnection connection) {
        useJdbcConnection(POSTGRESQLTABLE, connection)
    }

    /** MySQL database table */
    PostgreSQLTable postgresqlTable(String name, @DelegatesTo(PostgreSQLTable) Closure cl = null) {
        def isRegistered = isRegisteredDataset(POSTGRESQLTABLE, name)
        def parent = registerDataset(POSTGRESQLTABLE, name) as PostgreSQLTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(String name, @DelegatesTo(VerticaConnection) Closure cl = null) {
        jdbcConnection(name, VERTICACONNECTION, cl) as VerticaConnection
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(@DelegatesTo(VerticaConnection) Closure cl = null) {
        verticaConnection(null, cl)
    }

    /** Use default Vertica connection for new datasets */
    void useVerticaConnection(VerticaConnection connection) {
        useJdbcConnection(VERTICATABLE, connection)
    }

    /** Vertica table */
    VerticaTable verticaTable(String name, @DelegatesTo(VerticaTable) Closure cl = null) {
        def isRegistered = isRegisteredDataset(VERTICATABLE, name)
        def parent = registerDataset(VERTICATABLE, name) as VerticaTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** Vertica table */
    VerticaTable verticaTable(@DelegatesTo(VerticaTable) Closure cl = null) {
        verticaTable(null, cl)
    }

    /** Bulk load CSV file to Vertica table */
    void verticaBulkLoad(String name, CSVDataset source, @DelegatesTo(VerticaBulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, verticaTable(name), source, cl)
    }

    /** Bulk load CSV file to Vertica table */
    void verticaBulkLoad(String name, @DelegatesTo(VerticaBulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, verticaTable(name), null, cl)
    }

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(String name, @DelegatesTo(NetsuiteConnection) Closure cl = null) {
        jdbcConnection(name, NETSUITECONNECTION, cl) as NetsuiteConnection
    }

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(@DelegatesTo(NetsuiteConnection) Closure cl = null) {
        netsuiteConnection(null, cl)
    }

    /** Use default Netsuite connection for new datasets */
    void useNetsuiteConnection(NetsuiteConnection connection) {
        useJdbcConnection(NETSUITETABLE, connection)
    }

    /** Temporary DB connection */
    TDS embeddedConnection(String name, @DelegatesTo(TDS) Closure cl = null) {
        def parent = registerConnection(EMBEDDEDCONNECTION, name) as TDS
        if (parent.sqlHistoryFile == null) parent.sqlHistoryFile = langOpts.tempDBSQLHistoryFile
        runClosure(parent, cl)

        return parent
    }

    /** Temporary DB connection */
    TDS embeddedConnection(@DelegatesTo(TDS) Closure cl = null) {
        embeddedConnection(null, cl)
    }

    /** Table with temporary database */
    TDSTable embeddedTable(String name, @DelegatesTo(TDSTable) Closure cl = null) {
        def isRegistered = isRegisteredDataset(EMBEDDEDTABLE, name)
        def parent = registerDataset(EMBEDDEDTABLE, name) as TDSTable
        if ((parent.connection as TDS).sqlHistoryFile == null)
            (parent.connection as TDS).sqlHistoryFile = langOpts.tempDBSQLHistoryFile

        runClosure(parent, cl)

        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** Table with temporary database */
    TDSTable embeddedTable(@DelegatesTo(TDSTable) Closure cl = null) {
        embeddedTable(null, cl)
    }

    /** Table with temporary database */
    TDSTable embeddedTableWithDataset(String name, Dataset sourceDataset, @DelegatesTo(TDSTable) Closure cl = null) {
        if (sourceDataset == null) throw new ExceptionGETL("Source dataset cannot be null!")
        if (sourceDataset.field.isEmpty()) {
            sourceDataset.retrieveFields()
            if (sourceDataset.field.isEmpty()) throw new ExceptionGETL("Required field from dataset $sourceDataset")
        }

        TDSTable parent = new TDSTable()
        parent.field = sourceDataset.field
        if ((parent.connection as TDS).sqlHistoryFile == null)
            (parent.connection as TDS).sqlHistoryFile = langOpts.tempDBSQLHistoryFile
        if (!parent.exists) parent.create()

        if (name != null) registerDataset(parent, name, false)
        runClosure(parent, cl)

        return parent
    }

    /** Table with temporary database */
    TDSTable embeddedTableWithDataset(Dataset sourceDataset, @DelegatesTo(TDSTable) Closure cl = null) {
        embeddedTableWithDataset(null, sourceDataset, cl)
    }

    /** Bulk load CSV file to temporary database table */
    void embeddedTableBulkLoad(String name, CSVDataset source, @DelegatesTo(BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, embeddedTable(name), source, cl)
    }

    /** Bulk load CSV file to H2 database table */
    void embeddedTableBulkLoad(String name, @DelegatesTo(H2BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, embeddedTable(name), null, cl)
    }

    /** JDBC query */
    QueryDataset query(String name, @DelegatesTo(QueryDataset) Closure cl = null) {
        def isRegistered = isRegisteredDataset(QUERYDATASET, name)
        def parent = registerDataset(QUERYDATASET, name) as QueryDataset
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** JDBC query */
    QueryDataset query(@DelegatesTo(QueryDataset) Closure cl = null) {
        query(null, cl)
    }

    /** CSV connection */
    CSVConnection csvConnection(String name, @DelegatesTo(CSVConnection) Closure cl = null) {
        def parent = registerConnection(CSVCONNECTION, name) as CSVConnection
        runClosure(parent, cl)

        return parent
    }

    /** CSV connection */
    CSVConnection csvConnection(@DelegatesTo(CSVConnection) Closure cl = null) {
        csvConnection(null, cl)
    }

    /** Use default CSV connection for new datasets */
    void useCsvConnection(CSVConnection connection) {
        useFileConnection(CSVDATASET, connection)
    }

    /** CSV delimiter file */
    CSVDataset csv(String name, @DelegatesTo(CSVDataset) Closure cl = null) {
        def parent = registerDataset(CSVDATASET, name) as CSVDataset
        if (parent.connection == null) parent.connection = new CSVConnection()
        runClosure(parent, cl)

        return parent
    }

    /** CSV delimiter file */
    CSVDataset csv(@DelegatesTo(CSVDataset) Closure cl = null) {
        csv(null, cl)
    }

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(String name, Dataset sourceDataset, @DelegatesTo(CSVDataset) Closure cl = null) {
        if (sourceDataset == null) throw new ExceptionGETL("Dataset cannot be null!")
        def parent = registerDataset(CSVDATASET, name) as CSVDataset
        parent.field = sourceDataset.field

        runClosure(parent, cl)

        return parent
    }

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(Dataset sourceDataset, @DelegatesTo(CSVDataset) Closure cl = null) {
        csvWithDataset(null, sourceDataset, cl)
    }

    /** Excel connection */
    ExcelConnection excelConnection(String name, @DelegatesTo(ExcelConnection) Closure cl = null) {
        def parent = registerConnection(EXCELCONNECTION, name) as ExcelConnection
        runClosure(parent, cl)

        return parent
    }

    /** Excel connection */
    ExcelConnection excelConnection(@DelegatesTo(ExcelConnection) Closure cl) {
        excelConnection(null, cl)
    }

    /** Use default Excel connection for new datasets */
    void useExcelConnection(ExcelConnection connection) {
        useOtherConnection(EXCELDATASET, connection)
    }

    /** Excel file */
    ExcelDataset excel(String name, @DelegatesTo(ExcelDataset) Closure cl = null) {
        def parent = registerDataset(EXCELDATASET, name) as ExcelDataset
        if (parent.connection == null) parent.connection = new ExcelConnection()
        runClosure(parent, cl)

        return parent
    }

    /** Excel file */
    ExcelDataset excel(@DelegatesTo(ExcelDataset) Closure cl = null) {
        excel(null, cl)
    }

    /** JSON connection */
    JSONConnection jsonConnection(String name, @DelegatesTo(JSONConnection) Closure cl = null) {
        def parent = registerConnection(JSONCONNECTION, name) as JSONConnection
        runClosure(parent, cl)

        return parent
    }

    /** JSON connection */
    JSONConnection jsonConnection(@DelegatesTo(JSONConnection) Closure cl = null) {
        jsonConnection(null, cl)
    }

    /** Use default Json connection for new datasets */
    void useJsonConnection(JSONConnection connection) {
        useFileConnection(JSONDATASET, connection)
    }

    /** JSON file */
    JSONDataset json(String name, @DelegatesTo(JSONDataset) Closure cl = null) {
        def parent = registerDataset(JSONDATASET, name) as JSONDataset
        if (parent.connection == null) parent.connection = new JSONConnection()
        runClosure(parent, cl)

        return parent
    }

    /** JSON file */
    JSONDataset json(@DelegatesTo(JSONDataset) Closure cl = null) {
        json(null, cl)
    }

    /** XML connection */
    XMLConnection xmlConnection(String name, @DelegatesTo(XMLConnection) Closure cl = null) {
        def parent = registerConnection(XMLCONNECTION, name) as XMLConnection
        runClosure(parent, cl)

        return parent
    }

    /** XML connection */
    XMLConnection xmlConnection(@DelegatesTo(XMLConnection) Closure cl = null) {
        xmlConnection(null, cl)
    }

    /** Use default XML connection for new datasets */
    void useXmlConnection(XMLConnection connection) {
        useFileConnection(XMLDATASET, connection)
    }

    /** XML file */
    XMLDataset xml(String name, @DelegatesTo(XMLDataset) Closure cl = null) {
        def parent = registerDataset(XMLDATASET, name) as XMLDataset
        if (parent.connection == null) parent.connection = new XMLConnection()
        runClosure(parent, cl)

        return parent
    }

    /** XML file */
    XMLDataset xml(@DelegatesTo(XMLDataset) Closure cl = null) {
        xml(null, cl)
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(String name, @DelegatesTo(SalesForceConnection) Closure cl = null) {
        def parent = registerConnection(SALESFORCECONNECTION, name) as SalesForceConnection
        runClosure(parent, cl)

        return parent
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(@DelegatesTo(SalesForceConnection) Closure cl = null) {
        salesforceConnection(null, cl)
    }

    /** Use default SalesForce connection for new datasets */
    void useSalesforceConnection(SalesForceConnection connection) {
        useOtherConnection(SALESFORCEDATASET, connection)
    }

    /** SalesForce table */
    SalesForceDataset salesforce(String name, @DelegatesTo(SalesForceDataset) Closure cl = null) {
        def parent = registerDataset(SALESFORCEDATASET, name) as SalesForceDataset
        runClosure(parent, cl)

        return parent
    }

    /** SalesForce table */
    SalesForceDataset salesforce(@DelegatesTo(SalesForceDataset) Closure cl = null) {
        salesforce(null, cl)
    }

    /** Xero connection */
    XeroConnection xeroConnection(String name, @DelegatesTo(XeroConnection) Closure cl = null) {
        def parent = registerConnection(XEROCONNECTION, name) as XeroConnection
        runClosure(parent, cl)

        return parent
    }

    /** Xero connection */
    XeroConnection xeroConnection(@DelegatesTo(XeroConnection) Closure cl = null) {
        xeroConnection(null, cl)
    }

    /** Xero table */
    XeroDataset xero(String name, @DelegatesTo(XeroDataset) Closure cl = null) {
        def parent = registerDataset(XERODATASET, name) as XeroDataset
        runClosure(parent, cl)

        return parent
    }

    /** Use default Xero connection for new datasets */
    void useXeroConnection(XeroConnection connection) {
        useOtherConnection(XERODATASET, connection)
    }

    /** Xero table */
    XeroDataset xero(@DelegatesTo(XeroDataset) Closure cl = null) {
        xero(null, cl)
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(@DelegatesTo(TFS) Closure cl = null) {
        def parent = TFS.storage
        runClosure(parent, cl)

        return parent
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(String name, @DelegatesTo(TFSDataset) Closure cl = null) {
        TFSDataset parent = registerDataset(CSVTEMPDATASET, name) as TFSDataset
        runClosure(parent, cl)

        return parent
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(@DelegatesTo(TFSDataset) Closure cl = null) {
        csvTemp(null, cl)
    }

    /** Temporary CSV file */
    TFSDataset csvTempWithDataset(String name, Dataset sourceDataset, @DelegatesTo(TFSDataset) Closure cl = null) {
        if (sourceDataset == null) throw new ExceptionGETL("Dataset cannot be null!")
        TFSDataset parent = sourceDataset.csvTempFile
        if (name != null) registerDataset(parent, name, false)
        runClosure(parent, cl)

        return parent
    }

    /** Temporary CSV file */
    TFSDataset csvTempWithDataset(Dataset sourceDataset, @DelegatesTo(TFSDataset) Closure cl = null) {
        csvTempWithDataset(null, sourceDataset, cl)
    }

    /**
     * Copy rows from source to destination dataset
     * <br>Closure gets two parameters: source and destination datasets
     */
    Flow copyRows(Dataset source, Dataset dest, @DelegatesTo(FlowCopySpec) Closure cl = null) {
        if (source == null) throw new ExceptionGETL('Source dataset cannot be null!')
        if (dest == null) throw new ExceptionGETL('Dest dataset cannot be null!')
        def parent = new FlowCopySpec()
        runClosure(parent, cl, source, dest)

        def pt = startProcess("Copy rows from $source to $dest")
        Flow flow = new Flow()
        if (parent.onInit) parent.onInit.call()
        parent.prepareParams()
        def flowParams = (([source: source, dest: dest]) as Map<String, Object>) + parent.params
        flow.copy(flowParams)
        parent.countRow = flow.countRow
        parent.errorsDataset = flow.errorsDataset
        if (parent.onDone) parent.onDone.call()
        finishProcess(pt, parent.countRow)

        return flow
    }

    /** Write rows to destination dataset */
    Flow rowsTo(Dataset dest, @DelegatesTo(FlowWriteSpec) Closure cl) {
        if (dest == null) throw new ExceptionGETL('Destination dataset cannot be null!')
        def parent = new FlowWriteSpec()
        runClosure(parent, cl, dest)

        def pt = startProcess("Write rows to $dest")
        Flow flow = new Flow()
        if (parent.onInit != null) parent.onInit.call()
        parent.prepareParams()
        def flowParams = (([dest: dest]) as Map<String, Object>) + parent.params
        flow.writeTo(flowParams)
        parent.countRow = flow.countRow
        if (parent.onDone != null) parent.onDone.call()
        finishProcess(pt, parent.countRow)

        return flow
    }

    /** Write rows to many destination datasets */
    Flow rowsToMany(Map<String, Dataset> dest, @DelegatesTo(FlowWriteManySpec) Closure cl) {
        if (dest == null || dest.isEmpty()) throw new ExceptionGETL('Destination datasets cannot be null or empty!')
        def parent = new FlowWriteManySpec()
        runClosure(parent, cl, dest)

        def destNames = [] as List<String>
        dest.each { String destName, Dataset ds -> destNames.add("$destName: ${ds.toString()}".toString())}
        def pt = startProcess("Write rows to $destNames")
        Flow flow = new Flow()
        if (parent.onInit != null) parent.onInit.call()
        parent.prepareParams()
        def flowParams = (([dest: dest]) as Map<String, Object>) + parent.params
        flow.writeAllTo(flowParams)
        if (parent.onDone != null) parent.onDone.call()
        finishProcess(pt)

        return flow
    }

    /** Process rows from source dataset */
    Flow rowProcess(Dataset source, @DelegatesTo(FlowProcessSpec) Closure cl) {
        if (source == null) throw new ExceptionGETL('Source dataset cannot be null!')
        def parent = new FlowProcessSpec()
        runClosure(parent, cl, source)

        def pt = startProcess("Read rows from $source")
        Flow flow = new Flow()
        if (parent.onInit != null) parent.onInit.call()
        parent.prepareParams()
        def flowParams = (([source: source]) as Map<String, Object>) + parent.params
        flow.process(flowParams)
        parent.countRow = flow.countRow
        parent.errorsDataset = flow.errorsDataset
        if (parent.onDone != null) parent.onDone.call()
        finishProcess(pt, parent.countRow)

        return flow
    }

    /** SQL scripter */
    SQLScripter sql(JDBCConnection connection, @DelegatesTo(SQLScripter) Closure cl) {
        def parent = new SQLScripter()
        parent.connection = connection?:defaultJdbcConnection()
        parent.extVars = configContent
        def pt = startProcess('Execution SQL script')
        runClosure(parent, cl)
        finishProcess(pt, parent.rowCount)

        return parent
    }

    /** SQL scripter */
    SQLScripter sql(@DelegatesTo(SQLScripter) Closure cl) {
        sql(null, cl)
    }

    /** Process local file system */
    FileManager files(String name, @DelegatesTo(FileManager) Closure cl = null) {
        def parent = registerFileManager(FILEMANAGER, name) as FileManager
        parent.rootPath = TFS.storage.path
        parent.connect()
        if (cl != null) {
            def pt = startProcess('Process local file system')
            runClosure(parent, cl)
            finishProcess(pt)
        }

        return parent
    }

    /** Process local file system */
    FileManager files(@DelegatesTo(FileManager) Closure cl = null) {
        files(null, cl)
    }

    /** Process ftp file system */
    FTPManager ftp(String name, @DelegatesTo(FTPManager) Closure cl = null) {
        def parent = registerFileManager(FTPMANAGER, name) as FTPManager
        if (cl != null) {
            def pt = startProcess('Process ftp file system')
            try {
                runClosure(parent, cl)
            }
            finally {
                parent.disconnect()
            }
            finishProcess(pt)
        }

        return parent
    }

    /** Process ftp file system */
    FTPManager ftp(@DelegatesTo(FTPManager) Closure cl = null) {
        ftp(null, cl)
    }

    /** Process sftp file system */
    SFTPManager sftp(String name, @DelegatesTo(SFTPManager) Closure cl = null) {
        def parent = registerFileManager(SFTPMANAGER, name) as SFTPManager
        if (cl != null) {
            def pt = startProcess('Process sftp file system')
            try {
                runClosure(parent, cl)
            }
            finally {
                parent.disconnect()
            }
            finishProcess(pt)
        }

        return parent
    }

    /** Process sftp file system */
    SFTPManager sftp(@DelegatesTo(SFTPManager) Closure cl = null) {
        sftp(null, cl)
    }

    /** Process sftp file system */
    HDFSManager hdfs(String name, @DelegatesTo(HDFSManager) Closure cl = null) {
        def parent = registerFileManager(HDFSMANAGER, name) as HDFSManager
        if (cl != null) {
            def pt = startProcess('Process hdfs file system')
            try {
                runClosure(parent, cl)
            }
            finally {
                parent.disconnect()
            }
            finishProcess(pt)
        }

        return parent
    }

    /** Process sftp file system */
    HDFSManager hdfs(@DelegatesTo(HDFSManager) Closure cl = null) {
        hdfs(null, cl)
    }

    /** Run code in multithread mode */
    Executor thread(List elements, @DelegatesTo(Executor) Closure cl) {
        def disposeConnections = { Map<String, List<ExecutorThread.CloneObject>> list ->
            (list?.connections as List<ExecutorThread.CloneObject>)?.each { ExecutorThread.CloneObject cloneObject ->
                def con = cloneObject.cloneObject as Connection
                if (con != null) con.connected = false
            }

            (list?.filemanagers as List<ExecutorThread.CloneObject>)?.each { ExecutorThread.CloneObject cloneObject ->
                def man = cloneObject.cloneObject as FileManager
                if (man != null) man.connected = false
            }
        }

        def parent = new Executor()
        parent.disposeThreadResource(disposeConnections)
        if (elements != null) parent.list = elements
        def pt = startProcess('Execution threads')
        runClosure(parent, cl)
        finishProcess(pt)

        return parent
    }

    /** Run code in multithread mode */
    Executor thread(@DelegatesTo(Executor) Closure cl) {
        thread(null, cl)
    }

    /** Run code in multithread mode */
    EMailer mail(@DelegatesTo(EMailer) Closure cl) {
        def parent = new EMailer()
        def pt = startProcess('Mailer')
        runClosure(parent, cl)
        finishProcess(pt)

        return parent
    }

    FileTextSpec textFile(@DelegatesTo(FileTextSpec) Closure cl) {
        def parent = new FileTextSpec()
        def pt = startProcess('Write to text file')
        runClosure(parent, cl)
        pt?.name = "Write to text file \"${parent.fileName}\""
        finishProcess(pt)

        return parent
    }

    /** Copy file to directory */
    static void copyFileToDir(String sourceFileName, String destDirName, boolean createDir = false) {
        FileUtils.CopyToDir(sourceFileName, destDirName, createDir)
    }

    /** Copy file to another file */
    static void copyFileTo(String sourceFileName, String destFileName, boolean createDir = false) {
        FileUtils.CopyToFile(sourceFileName, destFileName, createDir)
    }

    /** Find parent directory by nearest specified name in path elements */
    static String findParentPath(String path, String find) {
        FileUtils.FindParentPath(path, find)
    }

    /** Valid exists directory */
    static Boolean existDir(String dirName) {
        FileUtils.ExistsFile(dirName, true)
    }

    /** Valid exists file */
    static Boolean existFile(String dirName) {
        FileUtils.ExistsFile(dirName)
    }

    static String extractPath(String fileName) {
        FileUtils.RelativePathFromFile(fileName)
    }
}