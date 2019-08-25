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
import getl.driver.Driver
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
        _params.executedClasses = new SynchronizeObject()

        _params.repConnections = new ConcurrentHashMap<String, Map<String, Connection>>()
        _params.repDatasets = new ConcurrentHashMap<String, Map<String, Dataset>>()
        _params.repHistoryPoints = new ConcurrentHashMap<String, SavePointManager>()
        _params.repFileManagers = new ConcurrentHashMap<String, Map<String, Manager>>()

        _params.defaultJDBCConnection = new ConcurrentHashMap<String, Map<String, JDBCConnection>>()
        _params.defaultFileConnection = new ConcurrentHashMap<String, Map<String, FileConnection>>()
        _params.defaultOtherConnection = new ConcurrentHashMap<String, Map<String, FileConnection>>()

        useEmbeddedConnection()
        useCsvTempConnection()
    }

    @Override
    Object run() { return this }

    /** Run DSL script */
    static Getl Dsl(@DelegatesTo(Getl) Closure cl) {
        def parent = new Getl()
        parent.runClosure(parent, cl)
        return parent
    }

    /** Run DSL script */
    void runDsl(@DelegatesTo(Getl) Closure cl) {
        runClosure(this, cl)
    }

    Map<String, Object> _params = new ConcurrentHashMap<String, Object>()
    /** Set language parameters */
    protected void setGetlParams(Map<String, Object> importParams) { _params = importParams }

    public final String CSVCONNECTION = 'getl.csv.CSVConnection'
    public final String CSVTEMPCONNECTION = 'getl.tfs.TFS'
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
            CSVCONNECTION, CSVTEMPCONNECTION, DB2CONNECTION, EXCELCONNECTION, H2CONNECTION, HIVECONNECTION,
            JDBCCONNECTION, JSONCONNECTION, MSSQLCONNECTION, MYSQLCONNECTION, NETSUITECONNECTION, ORACLECONNECTION,
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

    /** list of executed script classes and call parameters */
    protected SynchronizeObject getExecutedClasses() { _params.executedClasses as SynchronizeObject }

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
    protected Connection registerConnection(String connectionClassName, String name, Boolean registration = false) {
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
            if ((!BoolUtils.IsValue(registration) && langOpts.validRegisterObjects) || Thread.currentThread() instanceof ExecutorThread)
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
        if (validExist && sect.containsKey(repName))
            throw new ExceptionGETL("Connection object \"$name\" already registered in repository!")
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
    protected Dataset registerDataset(Connection connection, String datasetClassName, String name, Boolean registration = false) {
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
                if ((!BoolUtils.IsValue(registration) && langOpts.validRegisterObjects) || Thread.currentThread() instanceof ExecutorThread)
                    throw new ExceptionGETL("Dataset \"$name\" with type \"$datasetClassName\" is not exist!")

                obj = Dataset.CreateDataset(dataset: datasetClassName) as Dataset
                setDefaultConnection(datasetClassName, obj)
                sect.put(repName, obj)
            }

            if (langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
                def thread = Thread.currentThread() as ExecutorThread
                connection = connection?:obj.connection
                if (connection != null) {
                    def cloneConnection = thread.registerCloneObject('connections', connection,
                            { (it as Connection).cloneConnection() }) as Connection
                    obj = thread.registerCloneObject('datasets', obj,
                            { (it as Dataset).cloneDataset(cloneConnection) }) as Dataset
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
    protected Dataset registerDataset(String datasetClassName, String name, Boolean registration = false) {
        registerDataset(null, datasetClassName, name, registration)
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

    /** History points repository */
    protected Map<String, SavePointManager> getHistoryPoints() {
        _params.repHistoryPoints as Map<String, SavePointManager>
    }

    /** Return list of repository history points */
    @Synchronized
    List<String> listHistoryPoint(Closure<Boolean> cl = null) {
        def res = [] as List<String>
        historyPoints.each { String name, SavePointManager point ->
            if (cl == null || BoolUtils.IsValue(cl.call(name, point))) res << name
        }

        return res
    }

    /** Check history point registration in repository */
    @Synchronized
    protected Boolean isRegisteredHistoryPoint(String name) {
        return historyPoints.containsKey(name)
    }

    /** Register history point in repository */
    @Synchronized
    protected SavePointManager registerHistoryPoint(String name, Boolean registration = false) {
        SavePointManager obj
        if (name == null) {
            obj = new SavePointManager()
            if (lastJdbcDefaultConnection != null) obj.connection = lastJdbcDefaultConnection
            if (obj.connection != null && langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
                def thread = Thread.currentThread() as ExecutorThread
                obj.connection = thread.registerCloneObject('connections', obj.connection,
                        { (it as Connection).cloneConnection() }) as Connection
            }
        }
        else {
            def repName = repObjectName(name)
            obj = historyPoints.get(repName)
            if (obj == null) {
                if ((!BoolUtils.IsValue(registration) && langOpts.validRegisterObjects) || Thread.currentThread() instanceof ExecutorThread)
                    throw new ExceptionGETL("History point \"$name\" is not exist!")

                obj = new SavePointManager()
                if (lastJdbcDefaultConnection != null) obj.connection = lastJdbcDefaultConnection
                historyPoints.put(repName, obj)
            }

            if (langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
                def thread = Thread.currentThread() as ExecutorThread
                if (obj.connection != null) {
                    def cloneConnection = thread.registerCloneObject('connections', obj.connection,
                            { (it as Connection).cloneConnection() }) as JDBCConnection
                    obj = thread.registerCloneObject('historypoints', obj,
                            { (it as SavePointManager).cloneSavePointManager(cloneConnection) }) as SavePointManager
                }
                else {
                    obj = thread.registerCloneObject('historypoints', obj,
                            { (it as Dataset).cloneDataset() }) as SavePointManager
                }
            }
        }

        return obj
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
    protected Manager registerFileManager(String fileManagerClassName, String name, Boolean registration = false) {
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
            if ((!BoolUtils.IsValue(registration) && langOpts.validRegisterObjects) || Thread.currentThread() instanceof ExecutorThread)
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
    void runGroovyClass(Class groovyClass, Boolean runOnce, Map<String, Object> vars = [:]) {
        def className = groovyClass.name
        def previouslyRun = (executedClasses.indexOfListItem(className) != -1)
        if (previouslyRun && BoolUtils.IsValue(runOnce)) return

        def script = (GroovyObject)groovyClass.newInstance() as Script
        if (script instanceof Getl) {
            def scriptGetl = script as Getl
            scriptGetl.setGetlParams(_params)
        }
        script.binding = new Binding([scriptArgs: MapUtils.DeepCopy(vars)?:([:] as Map<String, Object>)])
        if (!previouslyRun) executedClasses.addToList(className)
        script.run()
    }

    /** Load and run groovy script by class */
    void runGroovyClass(Class groovyClass, Map<String, Object> vars = [:]) {
        runGroovyClass(groovyClass, false, vars)
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

    /** System temporary directory */
    static String systemTempPath() { TFS.systemPath }

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
    protected JDBCConnection jdbcConnection(String name, String connectionClassName, Boolean registration = false,
                                  @DelegatesTo(JDBCConnection) Closure cl = null) {
        def parent = registerConnection(connectionClassName?:JDBCCONNECTION, name, registration) as JDBCConnection
        runClosure(parent, cl)

        return parent
    }

    /** JDBC connection */
    protected JDBCConnection jdbcConnection(String name, Boolean registration,
                                  @DelegatesTo(JDBCConnection) Closure cl = null) {
        jdbcConnection(name, null, registration, cl)
    }

    /** JDBC connection */
    protected JDBCConnection jdbcConnection(String name, @DelegatesTo(JDBCConnection) Closure cl = null) {
        jdbcConnection(name, null, false, cl)
    }

    /** JDBC connection */
    protected JDBCConnection jdbcConnection(@DelegatesTo(JDBCConnection) Closure cl) {
        jdbcConnection(null, null, false, cl)
    }

    /** JDBC table */
    TableDataset table(String name, Boolean registration, @DelegatesTo(TableDataset) Closure cl) {
        def isRegistered = isRegisteredDataset(TABLEDATASET, name)
        def parent = registerDataset(TABLEDATASET, name, registration) as TableDataset
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** JDBC table */
    TableDataset table(String name, @DelegatesTo(TableDataset) Closure cl = null) {
        table(name, false, cl)
    }

    /** JDBC table */
    TableDataset table(@DelegatesTo(TableDataset) Closure cl) {
        table(null, false, cl)
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

    /** H2 connection */
    H2Connection h2Connection(String name, Boolean registration, @DelegatesTo(H2Connection) Closure cl) {
        jdbcConnection(name, H2CONNECTION, registration, cl) as H2Connection
    }

    /** H2 connection */
    H2Connection h2Connection(String name, @DelegatesTo(H2Connection) Closure cl = null) {
        h2Connection(name, false, cl)
    }

    /** H2 connection */
    H2Connection h2Connection(@DelegatesTo(H2Connection) Closure cl) {
        h2Connection(null, false, cl)
    }

    /** H2 current connection */
    H2Connection h2Connection() {
        defaultJdbcConnection(H2TABLE)
    }

    /** Use default H2 connection for new datasets */
    void useH2Connection(H2Connection connection) {
        useJdbcConnection(H2TABLE, connection)
    }

    /** Use default H2 connection for new datasets */
    void useEmbeddedConnection(TDS connection = new TDS()) {
        useJdbcConnection(EMBEDDEDTABLE, connection)
    }

    /** H2 table */
    H2Table h2Table(String name, Boolean registration, @DelegatesTo(H2Table) Closure cl) {
        def isRegistered = isRegisteredDataset(H2TABLE, name)
        def parent = registerDataset(H2TABLE, name, registration) as H2Table
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** H2 table */
    H2Table h2Table(String name, @DelegatesTo(H2Table) Closure cl = null) {
        h2Table(name, false, cl)
    }

    /** H2 table */
    H2Table h2Table(@DelegatesTo(H2Table) Closure cl) {
        h2Table(null, false, cl)
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
    DB2Connection db2Connection(String name, Boolean registration, @DelegatesTo(DB2Connection) Closure cl) {
        jdbcConnection(name, DB2CONNECTION, registration, cl) as DB2Connection
    }

    /** DB2 connection */
    DB2Connection db2Connection(String name, @DelegatesTo(DB2Connection) Closure cl = null) {
        jdbcConnection(name, DB2CONNECTION, false, cl) as DB2Connection
    }

    /** DB2 connection */
    DB2Connection db2Connection(@DelegatesTo(DB2Connection) Closure cl) {
        db2Connection(null, false, cl)
    }

    /** DB2 current connection */
    DB2Connection db2Connection() {
        defaultJdbcConnection(DB2TABLE)
    }

    /** Use default DB2 connection for new datasets */
    void useDb2Connection(DB2Connection connection) {
        useJdbcConnection(DB2TABLE, connection)
    }

    /** DB2 database table */
    DB2Table db2Table(String name, Boolean registration, @DelegatesTo(DB2Table) Closure cl) {
        def isRegistered = isRegisteredDataset(DB2TABLE, name)
        def parent = registerDataset(DB2TABLE, name, registration) as DB2Table
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** DB2 database table */
    DB2Table db2Table(String name, @DelegatesTo(DB2Table) Closure cl = null) {
        db2Table(name, false, cl)
    }

    /** DB2 database table */
    DB2Table db2Table(@DelegatesTo(DB2Table) Closure cl) {
        db2Table(null, false, cl)
    }

    /** Hive connection */
    HiveConnection hiveConnection(String name, Boolean registration, @DelegatesTo(HiveConnection) Closure cl) {
        jdbcConnection(name, HIVECONNECTION, registration, cl) as HiveConnection
    }

    /** Hive connection */
    HiveConnection hiveConnection(String name, @DelegatesTo(HiveConnection) Closure cl = null) {
        hiveConnection(name, false, cl)
    }

    /** Hive connection */
    HiveConnection hiveConnection(@DelegatesTo(HiveConnection) Closure cl) {
        hiveConnection(null, false, cl)
    }

    /** Hive current connection */
    HiveConnection hiveConnection() {
        defaultJdbcConnection(HIVETABLE)
    }

    /** Use default Hive connection for new datasets */
    void useHiveConnection(HiveConnection connection) {
        useJdbcConnection(HIVETABLE, connection)
    }

    /** Hive table */
    HiveTable hiveTable(String name, Boolean registration, @DelegatesTo(HiveTable) Closure cl) {
        def isRegistered = isRegisteredDataset(HIVETABLE, name)
        def parent = registerDataset(HIVETABLE, name, registration) as HiveTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** Hive table */
    HiveTable hiveTable(String name, @DelegatesTo(HiveTable) Closure cl = null) {
        hiveTable(name, false, cl)
    }

    /** Hive table */
    HiveTable hiveTable(@DelegatesTo(HiveTable) Closure cl) {
        hiveTable(null, false, cl)
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
    MSSQLConnection mssqlConnection(String name, Boolean registration, @DelegatesTo(MSSQLConnection) Closure cl) {
        jdbcConnection(name, MSSQLCONNECTION, registration, cl) as MSSQLConnection
    }

    /** MSSQL connection */
    MSSQLConnection mssqlConnection(String name, @DelegatesTo(MSSQLConnection) Closure cl = null) {
        mssqlConnection(name, false, cl)
    }

    /** MSSQL connection */
    MSSQLConnection mssqlConnection(@DelegatesTo(MSSQLConnection) Closure cl) {
        mssqlConnection(null, false, cl)
    }

    /** MSSQL current connection */
    MSSQLConnection mssqlConnection() {
        defaultJdbcConnection(MSSQLTABLE)
    }

    /** Use default MSSQL connection for new datasets */
    void useMssqlConnection(MSSQLConnection connection) {
        useJdbcConnection(MSSQLTABLE, connection)
    }

    /** MSSQL database table */
    MSSQLTable mssqlTable(String name, Boolean registration, @DelegatesTo(MSSQLTable) Closure cl) {
        def isRegistered = isRegisteredDataset(MSSQLTABLE, name)
        def parent = registerDataset(MSSQLTABLE, name, registration) as MSSQLTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** MSSQL database table */
    MSSQLTable mssqlTable(String name, @DelegatesTo(MSSQLTable) Closure cl = null) {
        mssqlTable(name, false, cl)
    }

    /** MSSQL database table */
    MSSQLTable mssqlTable(@DelegatesTo(MSSQLTable) Closure cl) {
        mssqlTable(null, cl)
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(String name, Boolean registration, @DelegatesTo(MySQLConnection) Closure cl) {
        jdbcConnection(name, MYSQLCONNECTION, registration, cl) as MySQLConnection
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(String name, @DelegatesTo(MySQLConnection) Closure cl = null) {
        mysqlConnection(name, false, cl)
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(@DelegatesTo(MySQLConnection) Closure cl) {
        mysqlConnection(null, false, cl)
    }

    /** MySQL current connection */
    MySQLConnection mysqlConnection() {
        defaultJdbcConnection(MYSQLTABLE)
    }

    /** Use default MySQL connection for new datasets */
    void useMysqlConnection(MySQLConnection connection) {
        useJdbcConnection(MYSQLTABLE, connection)
    }

    /** MySQL database table */
    MySQLTable mysqlTable(String name, Boolean registration, @DelegatesTo(MySQLTable) Closure cl) {
        def isRegistered = isRegisteredDataset(MYSQLTABLE, name)
        def parent = registerDataset(MYSQLTABLE, name, registration) as MySQLTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** MySQL database table */
    MySQLTable mysqlTable(String name, @DelegatesTo(MySQLTable) Closure cl = null) {
        mysqlTable(name, false, cl)
    }

    /** MySQL database table */
    MySQLTable mysqlTable(@DelegatesTo(MySQLTable) Closure cl) {
        mysqlTable(null, false, cl)
    }

    /** Oracle connection */
    OracleConnection oracleConnection(String name, Boolean registration, @DelegatesTo(OracleConnection) Closure cl) {
        jdbcConnection(name, ORACLECONNECTION, registration, cl) as OracleConnection
    }

    /** Oracle connection */
    OracleConnection oracleConnection(String name, @DelegatesTo(OracleConnection) Closure cl = null) {
        oracleConnection(name, false, cl)
    }

    /** Oracle connection */
    OracleConnection oracleConnection(@DelegatesTo(OracleConnection) Closure cl) {
        oracleConnection(null, false, cl)
    }

    /** Oracle current connection */
    OracleConnection oracleConnection() {
        defaultJdbcConnection(ORACLETABLE)
    }

    /** Use default Oracle connection for new datasets */
    void useOracleConnection(OracleConnection connection) {
        useJdbcConnection(ORACLETABLE, connection)
    }

    /** Oracle table */
    OracleTable oracleTable(String name, Boolean registration, @DelegatesTo(OracleTable) Closure cl) {
        def isRegistered = isRegisteredDataset(ORACLETABLE, name)
        def parent = registerDataset(ORACLETABLE, name, registration) as OracleTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** Oracle table */
    OracleTable oracleTable(String name, @DelegatesTo(OracleTable) Closure cl = null) {
        oracleTable(name, false, cl)
    }

    /** Oracle table */
    OracleTable oracleTable(@DelegatesTo(OracleTable) Closure cl) {
        oracleTable(null, false, cl)
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(String name, Boolean registration,
                                              @DelegatesTo(PostgreSQLConnection) Closure cl) {
        jdbcConnection(name, POSTGRESQLCONNECTION, registration, cl) as PostgreSQLConnection
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(String name, @DelegatesTo(PostgreSQLConnection) Closure cl = null) {
        postgresqlConnection(name, false, cl)
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(@DelegatesTo(PostgreSQLConnection) Closure cl) {
        postgresqlConnection(null, false, cl)
    }

    /** PostgreSQL default connection */
    PostgreSQLConnection postgresqlConnection() {
        defaultJdbcConnection(POSTGRESQLTABLE)
    }

    /** Use default PostgreSQL connection for new datasets */
    void usePostgresqlConnection(PostgreSQLConnection connection) {
        useJdbcConnection(POSTGRESQLTABLE, connection)
    }

    /** MySQL database table */
    PostgreSQLTable postgresqlTable(String name, Boolean registration, @DelegatesTo(PostgreSQLTable) Closure cl) {
        def isRegistered = isRegisteredDataset(POSTGRESQLTABLE, name)
        def parent = registerDataset(POSTGRESQLTABLE, name, registration) as PostgreSQLTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }


    /** MySQL database table */
    PostgreSQLTable postgresqlTable(String name, @DelegatesTo(PostgreSQLTable) Closure cl = null) {
        postgresqlTable(name, false, cl)
    }

    /** MySQL database table */
    PostgreSQLTable postgresqlTable(@DelegatesTo(PostgreSQLTable) Closure cl) {
        postgresqlTable(null, false, cl)
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(String name, Boolean registration,
                                        @DelegatesTo(VerticaConnection) Closure cl) {
        jdbcConnection(name, VERTICACONNECTION, registration, cl) as VerticaConnection
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(String name, @DelegatesTo(VerticaConnection) Closure cl = null) {
        verticaConnection(name, false, cl)
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(@DelegatesTo(VerticaConnection) Closure cl) {
        verticaConnection(null, false, cl)
    }

    /** Vertica default connection */
    VerticaConnection verticaConnection() {
        defaultJdbcConnection(VERTICATABLE)
    }

    /** Use default Vertica connection for new datasets */
    void useVerticaConnection(VerticaConnection connection) {
        useJdbcConnection(VERTICATABLE, connection)
    }

    /** Vertica table */
    VerticaTable verticaTable(String name, Boolean registration, @DelegatesTo(VerticaTable) Closure cl) {
        def isRegistered = isRegisteredDataset(VERTICATABLE, name)
        def parent = registerDataset(VERTICATABLE, name, registration) as VerticaTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** Vertica table */
    VerticaTable verticaTable(String name, @DelegatesTo(VerticaTable) Closure cl = null) {
        verticaTable(name, false, cl)
    }

    /** Vertica table */
    VerticaTable verticaTable(@DelegatesTo(VerticaTable) Closure cl) {
        verticaTable(null, false, cl)
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
    NetsuiteConnection netsuiteConnection(String name, Boolean registration,
                                          @DelegatesTo(NetsuiteConnection) Closure cl) {
        jdbcConnection(name, NETSUITECONNECTION, registration, cl) as NetsuiteConnection
    }

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(String name, @DelegatesTo(NetsuiteConnection) Closure cl = null) {
        netsuiteConnection(name, false, cl)
    }

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(@DelegatesTo(NetsuiteConnection) Closure cl) {
        netsuiteConnection(null, false, cl)
    }

    /** NetSuite default connection */
    NetsuiteConnection netsuiteConnection() {
        defaultJdbcConnection(NETSUITETABLE)
    }

    /** Use default Netsuite connection for new datasets */
    void useNetsuiteConnection(NetsuiteConnection connection) {
        useJdbcConnection(NETSUITETABLE, connection)
    }

    /** Netsuite table */
    NetsuiteTable netsuiteTable(String name, Boolean registration, @DelegatesTo(NetsuiteTable) Closure cl) {
        def isRegistered = isRegisteredDataset(NETSUITETABLE, name)
        def parent = registerDataset(NETSUITETABLE, name, registration) as NetsuiteTable
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** Netsuite table */
    NetsuiteTable netsuiteTable(String name, @DelegatesTo(NetsuiteTable) Closure cl = null) {
        netsuiteTable(name, false, cl)
    }

    /** Netsuite table */
    NetsuiteTable netsuiteTable(@DelegatesTo(NetsuiteTable) Closure cl) {
        netsuiteTable(null, false, cl)
    }

    /** Temporary database connection */
    TDS embeddedConnection(String name, Boolean registration, @DelegatesTo(TDS) Closure cl) {
        def parent = registerConnection(EMBEDDEDCONNECTION, name, registration) as TDS
        if (parent.sqlHistoryFile == null) parent.sqlHistoryFile = langOpts.tempDBSQLHistoryFile
        runClosure(parent, cl)

        return parent
    }

    /** Temporary database connection */
    TDS embeddedConnection(String name, @DelegatesTo(TDS) Closure cl = null) {
        embeddedConnection(name, true, cl)
    }

    /** Temporary database connection */
    TDS embeddedConnection(@DelegatesTo(TDS) Closure cl) {
        embeddedConnection(null, true, cl)
    }

    /** Temporary database default connection */
    TDS embeddedConnection() {
        defaultJdbcConnection(EMBEDDEDTABLE)
    }

    /** Table with temporary database */
    TDSTable embeddedTable(String name, Boolean registration, @DelegatesTo(TDSTable) Closure cl) {
        def isRegistered = isRegisteredDataset(EMBEDDEDTABLE, name)
        def parent = registerDataset(EMBEDDEDTABLE, name, registration) as TDSTable
        if ((parent.connection as TDS).sqlHistoryFile == null)
            (parent.connection as TDS).sqlHistoryFile = langOpts.tempDBSQLHistoryFile

        runClosure(parent, cl)

        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** Table with temporary database */
    TDSTable embeddedTable(String name, @DelegatesTo(TDSTable) Closure cl = null) {
        embeddedTable(name, true, cl)
    }

    /** Table with temporary database */
    TDSTable embeddedTable(@DelegatesTo(TDSTable) Closure cl) {
        embeddedTable(null, false, cl)
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

    /** JDBC query dataset */
    QueryDataset query(String name, JDBCConnection connection, Boolean registration,
                       @DelegatesTo(QueryDataset) Closure cl) {
        def isRegistered = isRegisteredDataset(QUERYDATASET, name)
        def parent = registerDataset(connection, QUERYDATASET, name, registration) as QueryDataset
        runClosure(parent, cl)
        if (langOpts.autoCSVTempForJDBDTables && !BoolUtils.IsValue(isRegistered)) createCsvTemp(name, parent)

        return parent
    }

    /** JDBC query dataset */
    QueryDataset query(String name, Boolean registration, @DelegatesTo(QueryDataset) Closure cl) {
        query(name, null, registration, cl)
    }

    /** JDBC query dataset */
    QueryDataset query(String name, @DelegatesTo(QueryDataset) Closure cl = null) {
        query(name, null, false, cl)
    }

    /** JDBC query dataset */
    QueryDataset query(@DelegatesTo(QueryDataset) Closure cl) {
        query(null, null, false, cl)
    }

    /** JDBC query dataset */
    QueryDataset query(JDBCConnection connection, @DelegatesTo(QueryDataset) Closure cl) {
        query(null, connection, false, cl)
    }

    /** JDBC query dataset */
    QueryDataset sqlQuery(JDBCConnection connection, String sql, Map vars = [:]) {
        def parent = registerDataset(connection, QUERYDATASET, null, false) as QueryDataset
        parent.query = sql
        parent.queryParams.putAll(vars)

        return parent
    }

    /** JDBC query dataset */
    QueryDataset sqlQuery(String sql, Map vars = [:]) {
        sqlQuery(null, sql, vars)
    }

    /** Return the first row from query */
    Map<String, Object> sqlQueryRow(JDBCConnection connection, String sql, Map vars = [:]) {
        def query = sqlQuery(connection, sql, vars)
        def rows = query.rows(limit: 1)
        return (!rows.isEmpty())?rows[0]:null
    }

    /** Return the first row from query */
    Map<String, Object> sqlQueryRow(String sql, Map vars = [:]) {
        sqlQueryRow(null, sql, vars)
    }

    /** CSV connection */
    CSVConnection csvConnection(String name, Boolean registration, @DelegatesTo(CSVConnection) Closure cl) {
        def parent = registerConnection(CSVCONNECTION, name, registration) as CSVConnection
        runClosure(parent, cl)

        return parent
    }

    /** CSV connection */
    CSVConnection csvConnection(String name, @DelegatesTo(CSVConnection) Closure cl = null) {
        csvConnection(name, false, cl)
    }

    /** CSV connection */
    CSVConnection csvConnection(@DelegatesTo(CSVConnection) Closure cl) {
        csvConnection(null, false, cl)
    }

    /** CSV default connection */
    CSVConnection csvConnection() {
        defaultFileConnection(CSVDATASET)
    }

    /** Use default CSV connection for new datasets */
    void useCsvConnection(CSVConnection connection) {
        useFileConnection(CSVDATASET, connection)
    }

    /** CSV delimiter file */
    CSVDataset csv(String name, Boolean registration, @DelegatesTo(CSVDataset) Closure cl) {
        def parent = registerDataset(CSVDATASET, name, registration) as CSVDataset
        if (parent.connection == null) parent.connection = new CSVConnection()
        runClosure(parent, cl)

        return parent
    }

    /** CSV delimiter file */
    CSVDataset csv(String name, @DelegatesTo(CSVDataset) Closure cl = null) {
        csv(name, false, cl)
    }

    /** CSV delimiter file */
    CSVDataset csv(@DelegatesTo(CSVDataset) Closure cl) {
        csv(null, false, cl)
    }

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(String name, Dataset sourceDataset, Boolean registration,
                              @DelegatesTo(CSVDataset) Closure cl) {
        if (sourceDataset == null) throw new ExceptionGETL("Dataset cannot be null!")
        def parent = registerDataset(CSVDATASET, name, registration) as CSVDataset
        parent.field = sourceDataset.field

        runClosure(parent, cl)

        return parent
    }

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(String name, Dataset sourceDataset, @DelegatesTo(CSVDataset) Closure cl = null) {
        csvWithDataset(name, sourceDataset, true, cl)
    }

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(Dataset sourceDataset, @DelegatesTo(CSVDataset) Closure cl = null) {
        csvWithDataset(null, sourceDataset, true, cl)
    }

    /** Excel connection */
    ExcelConnection excelConnection(String name, Boolean registration, @DelegatesTo(ExcelConnection) Closure cl) {
        def parent = registerConnection(EXCELCONNECTION, name, registration) as ExcelConnection
        runClosure(parent, cl)

        return parent
    }

    /** Excel connection */
    ExcelConnection excelConnection(String name, @DelegatesTo(ExcelConnection) Closure cl = null) {
        excelConnection(name, false, cl)
    }

    /** Excel connection */
    ExcelConnection excelConnection(@DelegatesTo(ExcelConnection) Closure cl) {
        excelConnection(null, false, cl)
    }

    /** Excel default connection */
    ExcelConnection excelConnection() {
        defaultFileConnection(EXCELDATASET)
    }

    /** Use default Excel connection for new datasets */
    void useExcelConnection(ExcelConnection connection) {
        useOtherConnection(EXCELDATASET, connection)
    }

    /** Excel file */
    ExcelDataset excel(String name, Boolean registration, @DelegatesTo(ExcelDataset) Closure cl) {
        def parent = registerDataset(EXCELDATASET, name, registration) as ExcelDataset
        if (parent.connection == null) parent.connection = new ExcelConnection()
        runClosure(parent, cl)

        return parent
    }

    /** Excel file */
    ExcelDataset excel(String name, @DelegatesTo(ExcelDataset) Closure cl = null) {
        excel(name, false, cl)
    }

    /** Excel file */
    ExcelDataset excel(@DelegatesTo(ExcelDataset) Closure cl) {
        excel(null, false, cl)
    }

    /** JSON connection */
    JSONConnection jsonConnection(String name, Boolean registration, @DelegatesTo(JSONConnection) Closure cl) {
        def parent = registerConnection(JSONCONNECTION, name, registration) as JSONConnection
        runClosure(parent, cl)

        return parent
    }

    /** JSON connection */
    JSONConnection jsonConnection(String name, @DelegatesTo(JSONConnection) Closure cl = null) {
        jsonConnection(name, false, cl)
    }

    /** JSON connection */
    JSONConnection jsonConnection(@DelegatesTo(JSONConnection) Closure cl) {
        jsonConnection(null, false, cl)
    }

    /** JSON default connection */
    JSONConnection jsonConnection() {
        defaultFileConnection(JSONDATASET)
    }

    /** Use default Json connection for new datasets */
    void useJsonConnection(JSONConnection connection) {
        useFileConnection(JSONDATASET, connection)
    }

    /** JSON file */
    JSONDataset json(String name, Boolean registration, @DelegatesTo(JSONDataset) Closure cl) {
        def parent = registerDataset(JSONDATASET, name, registration) as JSONDataset
        if (parent.connection == null) parent.connection = new JSONConnection()
        runClosure(parent, cl)

        return parent
    }

    /** JSON file */
    JSONDataset json(String name, @DelegatesTo(JSONDataset) Closure cl = null) {
        json(name, false, cl)
    }

    /** JSON file */
    JSONDataset json(@DelegatesTo(JSONDataset) Closure cl) {
        json(null, false, cl)
    }

    /** XML connection */
    XMLConnection xmlConnection(String name, Boolean registration, @DelegatesTo(XMLConnection) Closure cl) {
        def parent = registerConnection(XMLCONNECTION, name, registration) as XMLConnection
        runClosure(parent, cl)

        return parent
    }

    /** XML connection */
    XMLConnection xmlConnection(String name, @DelegatesTo(XMLConnection) Closure cl = null) {
        xmlConnection(name, false, cl)
    }

    /** XML connection */
    XMLConnection xmlConnection(@DelegatesTo(XMLConnection) Closure cl) {
        xmlConnection(null, false, cl)
    }

    /** XML default connection */
    XMLConnection xmlConnection() {
        defaultFileConnection(XMLDATASET)
    }

    /** Use default XML connection for new datasets */
    void useXmlConnection(XMLConnection connection) {
        useFileConnection(XMLDATASET, connection)
    }

    /** XML file */
    XMLDataset xml(String name, Boolean registration, @DelegatesTo(XMLDataset) Closure cl) {
        def parent = registerDataset(XMLDATASET, name, registration) as XMLDataset
        if (parent.connection == null) parent.connection = new XMLConnection()
        runClosure(parent, cl)

        return parent
    }

    /** XML file */
    XMLDataset xml(String name, @DelegatesTo(XMLDataset) Closure cl = null) {
        xml(name, false, cl)
    }

    /** XML file */
    XMLDataset xml(@DelegatesTo(XMLDataset) Closure cl) {
        xml(null, false, cl)
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(String name, Boolean registration,
                                              @DelegatesTo(SalesForceConnection) Closure cl) {
        def parent = registerConnection(SALESFORCECONNECTION, name, registration) as SalesForceConnection
        runClosure(parent, cl)

        return parent
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(String name, @DelegatesTo(SalesForceConnection) Closure cl = null) {
        salesforceConnection(name, false, cl)
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(@DelegatesTo(SalesForceConnection) Closure cl) {
        salesforceConnection(null, false, cl)
    }

    /** SalesForce default connection */
    SalesForceConnection salesforceConnection() {
        defaultOtherConnection(SALESFORCEDATASET)
    }

    /** Use default SalesForce connection for new datasets */
    void useSalesforceConnection(SalesForceConnection connection) {
        useOtherConnection(SALESFORCEDATASET, connection)
    }

    /** SalesForce table */
    SalesForceDataset salesforce(String name, Boolean registration, @DelegatesTo(SalesForceDataset) Closure cl) {
        def parent = registerDataset(SALESFORCEDATASET, name, registration) as SalesForceDataset
        runClosure(parent, cl)

        return parent
    }

    /** SalesForce table */
    SalesForceDataset salesforce(String name, @DelegatesTo(SalesForceDataset) Closure cl = null) {
        salesforce(name, false, cl)
    }

    /** SalesForce table */
    SalesForceDataset salesforce(@DelegatesTo(SalesForceDataset) Closure cl) {
        salesforce(null, false, cl)
    }

    /** Xero connection */
    XeroConnection xeroConnection(String name, Boolean registration, @DelegatesTo(XeroConnection) Closure cl) {
        def parent = registerConnection(XEROCONNECTION, name, registration) as XeroConnection
        runClosure(parent, cl)

        return parent
    }

    /** Xero connection */
    XeroConnection xeroConnection(String name, @DelegatesTo(XeroConnection) Closure cl = null) {
        xeroConnection(name, false, cl)
    }

    /** Xero connection */
    XeroConnection xeroConnection(@DelegatesTo(XeroConnection) Closure cl) {
        xeroConnection(null, false, cl)
    }

    /** Xero default connection */
    XeroConnection xeroConnection() {
        defaultOtherConnection(XERODATASET)
    }

    /** Use default Xero connection for new datasets */
    void useXeroConnection(XeroConnection connection) {
        useOtherConnection(XERODATASET, connection)
    }

    /** Xero table */
    XeroDataset xero(String name, Boolean registration, @DelegatesTo(XeroDataset) Closure cl) {
        def parent = registerDataset(XERODATASET, name, registration) as XeroDataset
        runClosure(parent, cl)

        return parent
    }

    /** Xero table */
    XeroDataset xero(String name, @DelegatesTo(XeroDataset) Closure cl = null) {
        xero(name, false, cl)
    }

    /** Xero table */
    XeroDataset xero(@DelegatesTo(XeroDataset) Closure cl) {
        xero(null, false, cl)
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(String name, Boolean registration, @DelegatesTo(TFS) Closure cl) {
        def parent = registerConnection(CSVTEMPDATASET, name, registration)
        runClosure(parent, cl)

        return parent
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(String name, @DelegatesTo(TFS) Closure cl = null) {
        csvTempConnection(name, true, cl)
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(@DelegatesTo(TFS) Closure cl) {
        csvTempConnection(null, false, cl)
    }

    /** Temporary CSV file current connection */
    TFS csvTempConnection() {
        defaultFileConnection(CSVTEMPDATASET)
    }

    /** Use default CSV temporary connection for new datasets */
    void useCsvTempConnection(TFS connection = TFS.storage) {
        useFileConnection(CSVTEMPDATASET, connection)
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(String name, Boolean registration, @DelegatesTo(TFSDataset) Closure cl) {
        TFSDataset parent = registerDataset(CSVTEMPDATASET, name, registration) as TFSDataset
        runClosure(parent, cl)

        return parent
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(String name, @DelegatesTo(TFSDataset) Closure cl = null) {
        csvTemp(name, true, cl)
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(@DelegatesTo(TFSDataset) Closure cl) {
        csvTemp(null, false, cl)
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
    Flow copyRows(Dataset source, Dataset destination, @DelegatesTo(FlowCopySpec) Closure cl = null) {
        if (source == null) throw new ExceptionGETL('Source dataset cannot be null!')
        if (destination == null) throw new ExceptionGETL('Destination dataset cannot be null!')
        def parent = new FlowCopySpec()
        parent.source = source
        parent.destination = destination
        runClosure(parent, cl)

        def pt = startProcess("Copy rows from $source to $destination")
        Flow flow = new Flow()
        if (parent.onInit) parent.onInit.call()
        parent.prepareParams()
        def flowParams = parent.params
        flow.copy(flowParams)
        parent.countRow = flow.countRow
        parent.errorsDataset = flow.errorsDataset
        if (parent.onDone) parent.onDone.call()
        finishProcess(pt, parent.countRow)

        return flow
    }

    /** Write rows to destination dataset */
    Flow rowsTo(Dataset destination, @DelegatesTo(FlowWriteSpec) Closure cl) {
        if (destination == null) throw new ExceptionGETL('Destination dataset cannot be null!')
        def parent = new FlowWriteSpec()
        parent.destination = destination
        runClosure(parent, cl)

        def pt = startProcess("Write rows to $destination")
        Flow flow = new Flow()
        if (parent.onInit != null) parent.onInit.call()
        parent.prepareParams()
        def flowParams = parent.params
        flow.writeTo(flowParams)
        parent.countRow = flow.countRow
        if (parent.onDone != null) parent.onDone.call()
        finishProcess(pt, parent.countRow)

        return flow
    }

    /** Write rows to many destination datasets */
    Flow rowsToMany(Map<String, Dataset> destinations, @DelegatesTo(FlowWriteManySpec) Closure cl) {
        if (destinations == null || destinations.isEmpty()) throw new ExceptionGETL('Destination datasets cannot be null or empty!')
        def parent = new FlowWriteManySpec()
        parent.destinations = destinations
        runClosure(parent, cl)

        def destNames = [] as List<String>
        destinations.each { String destName, Dataset ds -> destNames.add("$destName: ${ds.toString()}".toString())}
        def pt = startProcess("Write rows to $destNames")
        Flow flow = new Flow()
        if (parent.onInit != null) parent.onInit.call()
        parent.prepareParams()
        def flowParams = parent.params
        flow.writeAllTo(flowParams)
        if (parent.onDone != null) parent.onDone.call()
        finishProcess(pt)

        return flow
    }

    /** Process rows from source dataset */
    Flow rowProcess(Dataset source, @DelegatesTo(FlowProcessSpec) Closure cl) {
        if (source == null) throw new ExceptionGETL('Source dataset cannot be null!')
        def parent = new FlowProcessSpec()
        parent.source = source
        runClosure(parent, cl)

        def pt = startProcess("Read rows from $source")
        Flow flow = new Flow()
        if (parent.onInit != null) parent.onInit.call()
        parent.prepareParams()
        def flowParams = parent.params
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
    FileManager files(String name, Boolean registration, @DelegatesTo(FileManager) Closure cl) {
        def parent = registerFileManager(FILEMANAGER, name, registration) as FileManager
        if (parent.rootPath == null) parent.rootPath = new File('.').absolutePath
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
        if (cl != null) {
            def pt = startProcess('Process local file system')
            try {
                runClosure(parent, cl)
            }
            finally {
                if (parent.connected) parent.disconnect()
            }
            finishProcess(pt)
        }

        return parent
    }

    /** Process local file system */
    FileManager files(String name, @DelegatesTo(FileManager) Closure cl = null) {
        files(name, false, cl)
    }

    /** Process local file system */
    FileManager files(@DelegatesTo(FileManager) Closure cl) {
        files(null, false, cl)
    }

    /** Process ftp file system */
    FTPManager ftp(String name, Boolean registration, @DelegatesTo(FTPManager) Closure cl) {
        def parent = registerFileManager(FTPMANAGER, name, registration) as FTPManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
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
    FTPManager ftp(String name, @DelegatesTo(FTPManager) Closure cl = null) {
        ftp(name, false, cl)
    }

    /** Process ftp file system */
    FTPManager ftp(@DelegatesTo(FTPManager) Closure cl) {
        ftp(null, false, cl)
    }

    /** Process sftp file system */
    SFTPManager sftp(String name, Boolean registration, @DelegatesTo(SFTPManager) Closure cl) {
        def parent = registerFileManager(SFTPMANAGER, name, registration) as SFTPManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
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
    SFTPManager sftp(String name, @DelegatesTo(SFTPManager) Closure cl = null) {
        sftp(name, false, cl)
    }

    /** Process sftp file system */
    SFTPManager sftp(@DelegatesTo(SFTPManager) Closure cl) {
        sftp(null, false, cl)
    }

    /** Process sftp file system */
    HDFSManager hdfs(String name, Boolean registration, @DelegatesTo(HDFSManager) Closure cl) {
        def parent = registerFileManager(HDFSMANAGER, name, registration) as HDFSManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
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
    HDFSManager hdfs(String name, @DelegatesTo(HDFSManager) Closure cl = null) {
        hdfs(name, false, cl)
    }

    /** Process sftp file system */
    HDFSManager hdfs(@DelegatesTo(HDFSManager) Closure cl) {
        hdfs(null, false, cl)
    }

    /** Run code in multithread mode */
    Executor thread(@DelegatesTo(Executor) Closure cl) {
        def disposeConnections = { Map<String, List<ExecutorThread.CloneObject>> list ->
            (list?.connections as List<ExecutorThread.CloneObject>)?.each { ExecutorThread.CloneObject cloneObject ->
                def con = cloneObject.cloneObject as Connection
                if (con != null && con.driver.isSupport(Driver.Support.CONNECT)) con.connected = false
            }

            (list?.filemanagers as List<ExecutorThread.CloneObject>)?.each { ExecutorThread.CloneObject cloneObject ->
                def man = cloneObject.cloneObject as FileManager
                if (man != null) man.connected = false
            }
        }

        def parent = new Executor()
        parent.disposeThreadResource(disposeConnections)
        def pt = startProcess('Execution threads')
        runClosure(parent, cl)
        finishProcess(pt)

        return parent
    }

    /** Run code in multithread mode */
    EMailer mail(@DelegatesTo(EMailer) Closure cl) {
        def parent = new EMailer()
        def pt = startProcess('Mailer')
        runClosure(parent, cl)
        finishProcess(pt)

        return parent
    }

    /** Processing text file */
    FileTextSpec textFile(@DelegatesTo(FileTextSpec) Closure cl) {
        def parent = new FileTextSpec()
        def pt = startProcess('Processing text file')
        runClosure(parent, cl)
        pt?.name = "Processing text file \"${parent.fileName}\""
        finishProcess(pt)

        return parent
    }

    /** File path parser */
    Path filePath(@DelegatesTo(Path) Closure cl) {
        def parent = new Path()
        runClosure(parent, cl)

        return parent
    }

    /** File path parser */
    Path filePath(String mask) {
        return new Path(mask: mask)
    }

    /** Incremenal history point manager */
    SavePointManager historypoint(String name, Boolean registration, @DelegatesTo(SavePointManager) Closure cl) {
        def parent = registerHistoryPoint(name, registration) as SavePointManager
        runClosure(parent, cl)

        return parent
    }

    /** Incremenal history point manager */
    SavePointManager historypoint(String name, @DelegatesTo(SavePointManager) Closure cl = null) {
        historypoint(name, false, cl)
    }

    /** Incremenal history point manager */
    SavePointManager historypoint(@DelegatesTo(SavePointManager) Closure cl) {
        historypoint(null, false, cl)
    }
}