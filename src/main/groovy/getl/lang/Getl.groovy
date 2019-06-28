/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) 2013-2019  Alexsey Konstantonov (ASCRUS)

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

        _params.defaultJDBCConnection = new TDS()
        _params.defaultCSVConnection = TFS.storage
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

    public final String CSVDATASET = 'getl.csv.CSVDataset'
    public final String CSVTEMPDATASET = 'getl.tfs.TFSDataset'
    public final String EXCELDATASET = 'getl.excel.ExcelDataset'
    public final String H2TABLE = 'getl.h2.H2Table'
    public final String HIVETABLE = 'getl.hive.HiveTable'
    public final String JSONDATASET = 'getl.json.JSONDataset'
    public final String QUERYDATASET = 'getl.jdbc.QueryDataset'
    public final String SALESFORCEDATASET = 'getl.salesforce.SalesForceDataset'
    public final String TABLEDATASET = 'getl.jdbc.TableDataset'
    public final String EMBEDDEDTABLE = 'getl.tfs.TDSTable'
    public final String VERTICATABLE = 'getl.vertica.VerticaTable'
    public final String XERODATASET = 'getl.xero.XeroDataset'
    public final String XMLDATASET = 'getl.xml.XMLDataset'

    public final String FILEMANAGER = 'getl.files.FileManager'
    public final String FTPMANAGER = 'getl.files.FTPManager'
    public final String HDFSMANAGER = 'getl.files.HDFSManager'
    public final String SFTPMANAGER = 'getl.files.SFTPManager'

    /** Fix start process */
    ProcessTime startProcess(String name) {
        return (langOpts.processTimeTracing)?new ProcessTime(name: name):null
    }

    /** Fix finish process */
    void finishProcess(ProcessTime pt, Long countRow = null) {
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
    protected String repObjectName(String name) { name.toLowerCase() }

    /** JDBC connections repository */
    protected Map<String, Map<String, Connection>> getRepConnections() { _params.repConnections as Map<String, Map<String, Connection>> }

    /** Return list of repository connections for specified class */
    List<String> listRepConnections(String className, Closure<Boolean> cl = null) {
        def res = [] as List<String>
        def sect = repConnections.get(className) as Map<String, Connection>
        if (sect != null && !sect.isEmpty())  {
            sect.each { String name, Connection connection ->
                if (cl == null || cl.call(name, connection) == true) res << name
            }
        }

        return res
    }

    /** Process repository connections for specified class */
    void processRepConnections(String className, Closure cl) {
        def sect = repConnections.get(className) as Map<String, Connection>
        if (sect != null && !sect.isEmpty())  {
            sect.each { String name, Connection connection ->
                cl.call(name)
            }
        }
    }

    /** Register connection in repository */
    protected Connection registerConnection(String className, String name) {
        if (name == null) {
            return Connection.CreateConnection(connection: className) as Connection
        }

        def sect = repConnections.get(className) as Map<String, Connection>
        if (sect == null) {
            sect = new ConcurrentHashMap<String, Connection>()
            repConnections.put(className, sect)
        }

        def repName = repObjectName(name)
        def obj = sect.get(repName)
        if (obj == null) {
            obj = Connection.CreateConnection(connection: className) as Connection
            sect.put(repName, obj)
        }

        return obj
    }

    /** Register connection object in repository */
    void registerConnection(Connection obj, String name, Boolean validExist = true) {
        if (obj == null) throw new ExceptionGETL("Connection object cannot be null!")
        def className = obj.getClass().name

        def sect = repConnections.get(className) as Map<String, Connection>
        if (sect == null) {
            sect = new ConcurrentHashMap<String, Connection>()
            repConnections.put(className, sect)
        }

        def repName = repObjectName(name)
        if (validExist && sect.containsKey(repName)) throw new ExceptionGETL("Connection object \"$name\" already registered in repository!")
        sect.put(repName, obj)
    }

    /** JDBC tables repository */
    protected Map<String, Map<String, Dataset>> getRepDatasets() { _params.repDatasets as Map<String, Map<String, Dataset>> }

    /** Return list of repository datasets for specified class */
    List<String> listRepDatasets(String className, Closure<Boolean> cl = null) {
        def res = [] as List<String>
        def sect = repDatasets.get(className) as Map<String, Dataset>
        if (sect != null && !sect.isEmpty())  {
            sect.each { String name, Dataset dataset ->
                if (cl == null || cl.call(name, dataset) == true) res << name
            }
        }

        return res
    }

    /** Process repository datasets for specified class */
    void processRepDatasets(String className, Closure<Boolean> cl) {
        def sect = repDatasets.get(className) as Map<String, Dataset>
        if (sect != null && !sect.isEmpty())  {
            sect.each { String name, Dataset dataset ->
                cl.call(name)
            }
        }
    }

    /** Return list of repository datasets for specified class connection */
    List<String> listRepDatasetsWithConnection(String className, Closure<Boolean> cl = null) {
        def res = [] as List<String>
        repDatasets.each { String sectClassName, Map<String, Dataset> section ->
            section.each { String name, Dataset dataset ->
                if (dataset.connection.getClass().name == className) {
                    if (cl == null || cl.call(name, dataset) == true) res << name
                }
            }
        }

        return res
    }

    /** Process repository datasets for specified class connection */
    void processRepDatasetsWithConnection(String className, Closure cl) {
        def res = [] as List<String>
        repDatasets.each { String sectClassName, Map<String, Dataset> section ->
            section.each { String name, Dataset dataset ->
                if (dataset.connection.getClass().name == className) {
                    cl.call(name)
                }
            }
        }
    }

    /** Set default JDBC connection for use in JDBC datasets */
    protected void setDefaultConnection(Dataset ds) {
        if (ds instanceof TDSTable || ds instanceof TFSDataset) return

        if (ds instanceof JDBCDataset) {
            ds.connection = defaultJDBCConnection
        }
        else if (ds instanceof CSVDataset) {
            ds.connection = defaultCSVConnection
        }
    }

    /** Check dataset registration in repository */
    protected Boolean isRegisteredDataset(String className, String name) {
        if (name ==  null) return null
        if (className ==  null) throw new ExceptionGETL('Class name cannot be null!')
        def sect = repDatasets.get(className) as Map<String, Dataset>
        if (sect == null) return false
        return sect.containsKey(name)
    }

    /** Register JDBC table in repository */
    protected Dataset registerDataset(String className, String name) {
        Dataset obj
        if (name == null) {
            obj = Dataset.CreateDataset(dataset: className) as Dataset
            setDefaultConnection(obj)
        }
        else {
            def sect = repDatasets.get(className) as Map<String, Dataset>
            if (sect == null) {
                sect = new ConcurrentHashMap<String, Dataset>()
                repDatasets.put(className, sect)
            }

            def repName = repObjectName(name)
            obj = sect.get(repName)
            if (obj == null) {
                obj = Dataset.CreateDataset(dataset: className) as Dataset
                setDefaultConnection(obj)
                sect.put(repName, obj)
            }
        }

        return obj
    }

    /** Register dataset object in repository */
    void registerDataset(Dataset obj, String name, Boolean validExist = true) {
        if (obj == null) throw new ExceptionGETL("Dataset object cannot be null!")
        def className = obj.getClass().name

        def sect = repDatasets.get(className) as Map<String, Dataset>
        if (sect == null) {
            sect = new ConcurrentHashMap<String, Dataset>()
            repDatasets.put(className, sect)
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
    protected Map<String, Map<String, Manager>> getRepFileManagers() { _params.repFileManagers as Map<String, Map<String, Manager>> }

    /** Return list of repository file managers for specified class */
    List<String> listRepFileManagers(String className, Closure<Boolean> cl = null) {
        def res = [] as List<String>
        def sect = repFileManagers.get(className) as Map<String, Manager>
        if (sect != null && !sect.isEmpty())  {
            sect.each { String name, Manager manager ->
                if (cl == null || cl.call(name, manager) == true) res << name
            }
        }

        return res
    }

    /** Process repository file managers for specified class */
    void processRepFileManagers(String className, Closure cl) {
        def sect = repFileManagers.get(className) as Map<String, Manager>
        if (sect != null && !sect.isEmpty())  {
            sect.each { String name, Manager manager ->
                cl.call(name)
            }
        }
    }

    /** Register file manager in repository */
    protected Manager registerFileManager(String className, String name) {
        if (name == null) {
            return FileManager.CreateManager(manager: className) as Manager
        }

        def sect = repFileManagers.get(className) as Map<String, Manager>
        if (sect == null) {
            sect = new ConcurrentHashMap<String, Manager>()
            repFileManagers.put(className, sect)
        }

        def repName = repObjectName(name)
        def obj = sect.get(repName)
        if (obj == null) {
            obj = Manager.CreateManager(connection: className) as Manager
            sect.put(repName, obj)
        }

        return obj
    }

    /** Register file manager object in repository */
    void registerFileManager(Manager obj, String name, Boolean validExist = true) {
        if (obj == null) throw new ExceptionGETL("File manager object cannot be null!")
        def className = obj.getClass().name

        def sect = repFileManagers.get(className) as Map<String, Manager>
        if (sect == null) {
            sect = new ConcurrentHashMap<String, Manager>()
            repFileManagers.put(className, sect)
        }

        def repName = repObjectName(name)
        if (validExist && sect.containsKey(repName)) throw new ExceptionGETL("File manager object \"$name\" already registered in repository!")
        sect.put(repName, obj)
    }

    /** Load and run groovy script file */
    void runGroovyFile(String fileName, Map vars = [:]) {
        File sourceFile = new File(fileName)
        def groovyClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(sourceFile)
        def script = (GroovyObject)groovyClass.newInstance() as Script
        configVars.putAll(vars)
        if (script instanceof Getl) {
            def scriptGetl = script as Getl
            scriptGetl.setGetlParams(_params)
        }
        script.run()
    }

    /** Load and run groovy script file */
    void runGroovyScript(String className, Map vars = [:]) {
        def groovyClass = Class.forName(className)
        def script = (GroovyObject)groovyClass.newInstance() as Script
        configVars.putAll(vars)
        if (script instanceof Getl) {
            def scriptGetl = script as Getl
            scriptGetl.setGetlParams(_params)
        }
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
    Map<String, Object> getConfigContent() { Config.content }

    /** Current configuration vars */
    Map<String, Object> getConfigVars() { Config.vars }

    /** Write message as level the INFO to log */
    public void logInfo(def msg) { Logs.Info(msg.toString()) }

    /** Write message as level the WARNING to log */
    public void logWarn(def msg) { Logs.Warning(msg.toString()) }

    /** Write message as level the SEVERE to log */
    public void logError(def msg) { Logs.Severe(msg.toString()) }

    /** Write message as level the FINE to log */
    public void logFine(def msg) { Logs.Fine(msg.toString()) }

    /** Write message as level the FINER to log */
    public void logFiner(def msg) { Logs.Finer(msg.toString()) }

    /** Write message as level the FINEST to log */
    public void logFinest(def msg) { Logs.Finest(msg.toString()) }

    /** Write message as level the CONFIG to log */
    public void logConfig(def msg) { Logs.Config(msg.toString()) }

    /** Current date and time */
    public Date getNow() { DateUtils.Now() }

    /** Random integer value */
    public int getRandomInt() { GenerationUtils.GenerateInt() }

    /** Random integer value */
    public int randomInt(Integer min, Integer max) { GenerationUtils.GenerateInt(min, max) }

    /** Random long value */
    public int getRandomLong() { GenerationUtils.GenerateLong() }

    /** Random date value */
    public Date getRandomDate() { GenerationUtils.GenerateDate() }

    /** Random date value */
    public Date randomDate(Integer days) { GenerationUtils.GenerateDate(days) }

    /** Random timestamp value */
    public Date getRandomDateTime() { GenerationUtils.GenerateDateTime() }

    /** Random date value */
    public Date randomDateTime(int secs) { GenerationUtils.GenerateDateTime(secs) }

    /** Random numeric value */
    public BigDecimal getRandomNumber() { GenerationUtils.GenerateNumeric() }

    /** Random numeric value */
    public BigDecimal randomNumber(int prec) { GenerationUtils.GenerateNumeric(prec) }

    /** Random numeric value */
    public BigDecimal randomNumber(int len, int prec) { GenerationUtils.GenerateNumeric(len, prec) }

    /** Random boolean value */
    public Boolean getRandomBoolean() { GenerationUtils.GenerateBoolean() }

    /** Random double value */
    public Double getRandomDouble() { GenerationUtils.GenerateDouble() }

    /** Random string value */
    public String getRandomString() { GenerationUtils.GenerateString(255) }

    /** Random string value */
    public String randomString(int len) { GenerationUtils.GenerateString(len) }

    /** GETL DSL options */
    LangSpec options(@DelegatesTo(LangSpec) Closure cl = null) {
        runClosure(langOpts, cl)

        return langOpts
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

    /** Default JDBC connection */
    JDBCConnection getDefaultJDBCConnection() {
        (!langOpts.useThreadModelJDBCConnection)?(_params.defaultJDBCConnection as JDBCConnection):
                (_params.defaultJDBCConnection as JDBCConnection)?.cloneConnection() as JDBCConnection }
    /** Use specified JDBC connection as default */
    void useJDBCConnection(JDBCConnection value) { _params.defaultJDBCConnection = value }

    /** JDBC connection */
    JDBCConnection jdbcConnection(String name = null, String className = null, @DelegatesTo(JDBCConnection) Closure cl = null) {
        def parent = registerConnection(className?:JDBCCONNECTION, name) as JDBCConnection
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

    /** Hive connection */
    HiveConnection hiveConnection(String name, @DelegatesTo(HiveConnection) Closure cl = null) {
        jdbcConnection(name, HIVECONNECTION, cl) as HiveConnection
    }

    /** Hive connection */
    HiveConnection hiveConnection(@DelegatesTo(HiveConnection) Closure cl = null) {
        hiveConnection(null, cl)
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

    /** MySQL connection */
    MySQLConnection mysqlConnection(String name, @DelegatesTo(MySQLConnection) Closure cl = null) {
        jdbcConnection(name, MYSQLCONNECTION, cl) as MySQLConnection
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(@DelegatesTo(MySQLConnection) Closure cl = null) {
        mysqlConnection(null, cl)
    }

    /** Oracle connection */
    OracleConnection oracleConnection(String name, @DelegatesTo(OracleConnection) Closure cl = null) {
        jdbcConnection(name, ORACLECONNECTION, cl) as OracleConnection
    }

    /** Oracle connection */
    OracleConnection oracleConnection(@DelegatesTo(OracleConnection) Closure cl = null) {
        oracleConnection(null, cl)
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(String name, @DelegatesTo(PostgreSQLConnection) Closure cl = null) {
        jdbcConnection(name, POSTGRESQLCONNECTION, cl) as PostgreSQLConnection
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(@DelegatesTo(PostgreSQLConnection) Closure cl = null) {
        postgresqlConnection(null, cl)
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(String name, @DelegatesTo(VerticaConnection) Closure cl = null) {
        jdbcConnection(name, VERTICACONNECTION, cl) as VerticaConnection
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(@DelegatesTo(VerticaConnection) Closure cl = null) {
        verticaConnection(null, cl)
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

    /** Default CSV connection */
    CSVConnection getDefaultCSVConnection() { _params.defaultCSVConnection as CSVConnection}
    /** Use specified CSV connection as default */
    void useCSVConnection(CSVConnection value) { _params.defaultCSVConnection = value }

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
        parent.connection = connection?:getDefaultJDBCConnection()
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
    Executor thread(@DelegatesTo(Executor) Closure cl) {
        def parent = new Executor()
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

    FileTextSpec textFile(@DelegatesTo(FileTextSpec) Closure cl) {
        def parent = new FileTextSpec()
        def pt = startProcess('Write to text file')
        runClosure(parent, cl)
        pt.name = "Write to text file \"${parent.fileName}\""
        finishProcess(pt)

        return parent
    }

    /** Copy file to directory */
    void copyFileToDir(String sourceFileName, String destDirName, boolean createDir = false) {
        FileUtils.CopyToDir(sourceFileName, destDirName, createDir)
    }

    /** Copy file to another file */
    void copyFileTo(String sourceFileName, String destFileName, boolean createDir = false) {
        FileUtils.CopyToFile(sourceFileName, destFileName, createDir)
    }

    /** Find parent directory by nearest specified name in path elements */
    String findParentPath(String path, String find) {
        FileUtils.FindParentPath(path, find)
    }

    /** Valid exists directory */
    Boolean existDir(String dirName) {
        FileUtils.ExistsFile(dirName, true)
    }

    /** Valid exists file */
    Boolean existFile(String dirName) {
        FileUtils.ExistsFile(dirName)
    }

    String extractPath(String fileName) {
        FileUtils.RelativePathFromFile(fileName)
    }
}