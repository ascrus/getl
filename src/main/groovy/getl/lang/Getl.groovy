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
import getl.hive.*
import getl.jdbc.*
import getl.jdbc.opts.BulkLoadSpec
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
import getl.vertica.opts.VerticaBulkLoadSpec
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
        def code = cl.rehydrate(this, parent, this)
        code.resolveStrategy = Closure.OWNER_FIRST
        code.call(parent)
    }

    Map<String, Object> _params = new ConcurrentHashMap<String, Object>()
    /** Set language parameters */
    protected void setGetlParams(Map<String, Object> importParams) { _params = importParams }

    public final String CSVCONNECTION = 'getl.csv.CSVConnection'
    public final String DB2CONNECTION = 'getl.db2.DB2Connection'
    public final String EXCELCONNECTION = 'getl.Excel.Connection'
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
    public final String TEMPDBCONNECTION = 'getl.tfs.TDS'
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
    public final String TEMPDBTABLE = 'getl.tfs.TDSTable'
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
        def code = cl.rehydrate(this, stat, this)
        code.resolveStrategy = Closure.OWNER_FIRST
        stat.startProfile()
        code.call(stat)
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
    protected boolean isRegisteredDataset(String className, String name) {
        if (name ==  null) throw new ExceptionGETL('Name cannot be null!')
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

    /** Current configuration content */
    Map<String, Object> getConfigContent() { Config.content }

    /** Current configuration vars */
    Map<String, Object> getConfigVars() { Config.vars }

    /** Write message as level the INFO to log */
    public void logInfo(String msg) { Logs.Info(msg) }

    /** Write message as level the WARNING to log */
    public void logWarn(String msg) { Logs.Warning(msg) }

    /** Write message as level the SEVERE to log */
    public void logError(String msg) { Logs.Severe(msg) }

    /** Write message as level the FINE to log */
    public void logFine(String msg) { Logs.Fine(msg) }

    /** Write message as level the FINER to log */
    public void logFiner(String msg) { Logs.Finer(msg) }

    /** Write message as level the FINEST to log */
    public void logFinest(String msg) { Logs.Finest(msg) }

    /** Write message as level the CONFIG to log */
    public void logConfig(String msg) { Logs.Config(msg) }

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
    public Boolean getRandomString() { GenerationUtils.GenerateString(255) }

    /** Random string value */
    public Boolean randomString(int len) { GenerationUtils.GenerateString(len) }

    /** GETL DSL options */
    LangSpec options(@DelegatesTo(LangSpec) Closure cl = null) {
        if (cl != null) {
            def code = cl.rehydrate(this, langOpts, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code(langOpts)
        }

        return langOpts
    }

    /** Configuration options */
    ConfigSpec config(@DelegatesTo(ConfigSpec) Closure cl = null) {
        def parent = new ConfigSpec()
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** Log options */
    LogSpec log(@DelegatesTo(LogSpec) Closure cl = null) {
        def parent = new LogSpec()
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

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
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** JDBC table */
    TableDataset table(String name, @DelegatesTo(TableDataset) Closure cl = null) {
        def isRegistered = isRegisteredDataset(TABLEDATASET, name)
        def parent = registerDataset(TABLEDATASET, name) as TableDataset
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }
        if (langOpts.autoCSVTempForJDBDTables && !isRegistered) createCsvTemp(name, parent)

        return parent
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
    H2Connection h2Connection(String name = null, @DelegatesTo(H2Connection) Closure cl = null) {
        jdbcConnection(name, H2CONNECTION, cl) as H2Connection
    }

    /** H2 database table */
    H2Table h2table(String name = null, @DelegatesTo(H2Table) Closure cl = null) {
        def isRegistered = isRegisteredDataset(H2TABLE, name)
        def parent = registerDataset(H2TABLE, name) as H2Table
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }
        if (langOpts.autoCSVTempForJDBDTables && !isRegistered) createCsvTemp(name, parent)

        return parent
    }

    /** Bulk load CSV file to H2 database table */
    void h2BulkLoad(String name, CSVDataset source, @DelegatesTo(BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, h2table(name), source, cl)
    }

    /** Bulk load CSV file to H2 database table */
    void h2BulkLoad(String name, @DelegatesTo(BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, h2table(name), null, cl)
    }

    /** DB2 connection */
    DB2Connection db2Connection(String name = null, @DelegatesTo(DB2Connection) Closure cl = null) {
        jdbcConnection(name, DB2CONNECTION, cl) as DB2Connection
    }

    /** Hive connection */
    HiveConnection hiveConnection(String name = null, @DelegatesTo(HiveConnection) Closure cl = null) {
        jdbcConnection(name, HIVECONNECTION, cl) as HiveConnection
    }

    /** Hive table */
    HiveTable hivetable(String name, @DelegatesTo(HiveTable) Closure cl = null) {
        def isRegistered = isRegisteredDataset(HIVETABLE, name)
        def parent = registerDataset(HIVETABLE, name) as HiveTable
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }
        if (langOpts.autoCSVTempForJDBDTables && !isRegistered) createCsvTemp(name, parent)

        return parent
    }

    /** Bulk load CSV file to Hive table */
    void hiveBulkLoad(String name, CSVDataset source, @DelegatesTo(BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, hivetable(name), source, cl)
    }

    /** Bulk load CSV file to Hive table */
    void hiveBulkLoad(String name, @DelegatesTo(BulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, hivetable(name), null, cl)
    }

    /** MSSQL connection */
    MSSQLConnection mssqlConnection(String name = null, @DelegatesTo(MSSQLConnection) Closure cl = null) {
        jdbcConnection(name, MSSQLCONNECTION, cl) as MSSQLConnection
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(String name = null, @DelegatesTo(MySQLConnection) Closure cl = null) {
        jdbcConnection(name, MYSQLCONNECTION, cl) as MySQLConnection
    }

    /** Oracle connection */
    OracleConnection oracleConnection(String name = null, @DelegatesTo(OracleConnection) Closure cl = null) {
        jdbcConnection(name, ORACLECONNECTION, cl) as OracleConnection
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(String name = null, @DelegatesTo(PostgreSQLConnection) Closure cl = null) {
        jdbcConnection(name, POSTGRESQLCONNECTION, cl) as PostgreSQLConnection
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(String name = null, @DelegatesTo(VerticaConnection) Closure cl = null) {
        jdbcConnection(name, VERTICACONNECTION, cl) as VerticaConnection
    }

    /** Vertica table */
    VerticaTable verticatable(String name = null, @DelegatesTo(VerticaTable) Closure cl = null) {
        def isRegistered = isRegisteredDataset(VERTICATABLE, name)
        def parent = registerDataset(VERTICATABLE, name) as VerticaTable
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }
        if (langOpts.autoCSVTempForJDBDTables && !isRegistered) createCsvTemp(name, parent)

        return parent
    }

    /** Bulk load CSV file to Vertica table */
    void verticaBulkLoad(String name, CSVDataset source, @DelegatesTo(VerticaBulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, verticatable(name), source, cl)
    }

    /** Bulk load CSV file to Vertica table */
    void verticaBulkLoad(String name, @DelegatesTo(VerticaBulkLoadSpec) Closure cl = null) {
        bulkLoadTableDataset(name, verticatable(name), null, cl)
    }

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(String name = null, @DelegatesTo(NetsuiteConnection) Closure cl = null) {
        jdbcConnection(name, NETSUITECONNECTION, cl) as NetsuiteConnection
    }

    /** Temporary DB connection */
    TDS tempDBConnection(String name = null, @DelegatesTo(TDS) Closure cl = null) {
        def parent = registerConnection(TEMPDBCONNECTION, name) as TDS
        if (parent.sqlHistoryFile == null) parent.sqlHistoryFile = langOpts.tempDBSQLHistoryFile
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** Table with temporary database */
    TDSTable tempDBTable(String name = null, @DelegatesTo(TDSTable) Closure cl = null) {
        def parent = registerDataset(TEMPDBTABLE, name) as TDSTable
        if ((parent.connection as TDS).sqlHistoryFile == null)
            (parent.connection as TDS).sqlHistoryFile = langOpts.tempDBSQLHistoryFile

        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** Table with temporary database */
    TDSTable tempDBTableWithDataset(String name = null, Dataset sourceDataset, @DelegatesTo(TDSTable) Closure cl = null) {
        if (sourceDataset == null) throw new ExceptionGETL("Dataset cannot be null!")
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

        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** JDBC query */
    QueryDataset queryDataset(String name = null, @DelegatesTo(QueryDataset) Closure cl = null) {
        def isRegistered = isRegisteredDataset(QUERYDATASET, name)
        def parent = registerDataset(QUERYDATASET, name) as QueryDataset
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }
        if (langOpts.autoCSVTempForJDBDTables && !isRegistered) createCsvTemp(name, parent)

        return parent
    }

    /** Default CSV connection */
    CSVConnection getDefaultCSVConnection() { _params.defaultCSVConnection as CSVConnection}
    /** Use specified CSV connection as default */
    void useCSVConnection(CSVConnection value) { _params.defaultCSVConnection = value }

    /** CSV connection */
    CSVConnection csvConnection(String name = null, @DelegatesTo(CSVConnection) Closure cl = null) {
        def parent = registerConnection(CSVCONNECTION, name) as CSVConnection
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** CSV delimiter file */
    CSVDataset csv(String name = null, @DelegatesTo(CSVDataset) Closure cl = null) {
        def parent = registerDataset(CSVDATASET, name) as CSVDataset
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(String name = null, Dataset sourceDataset, @DelegatesTo(CSVDataset) Closure cl = null) {
        if (sourceDataset == null) throw new ExceptionGETL("Dataset cannot be null!")
        def parent = registerDataset(CSVDATASET, name) as CSVDataset
        parent.field = sourceDataset.field

        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** Excel connection */
    ExcelConnection excelConnection(String name = null, @DelegatesTo(ExcelConnection) Closure cl = null) {
        def parent = registerConnection(EXCELCONNECTION, name) as ExcelConnection
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** Excel file */
    ExcelDataset excel(String name = null, @DelegatesTo(ExcelDataset) Closure cl = null) {
        def parent = registerDataset(EXCELDATASET, name) as ExcelDataset
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** JSON connection */
    JSONConnection jsonConnection(String name = null, @DelegatesTo(JSONConnection) Closure cl = null) {
        def parent = registerConnection(JSONCONNECTION, name) as JSONConnection
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** JSON file */
    JSONDataset json(String name = null, @DelegatesTo(JSONDataset) Closure cl = null) {
        def parent = registerDataset(JSONDATASET, name) as JSONDataset
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** XML connection */
    XMLConnection xmlConnection(String name = null, @DelegatesTo(XMLConnection) Closure cl = null) {
        def parent = registerConnection(XMLCONNECTION, name) as XMLConnection
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** XML file */
    XMLDataset xml(String name = null, @DelegatesTo(XMLDataset) Closure cl = null) {
        def parent = registerDataset(XMLDATASET, name) as XMLDataset
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(String name = null, @DelegatesTo(SalesForceConnection) Closure cl = null) {
        def parent = registerConnection(SALESFORCECONNECTION, name) as SalesForceConnection
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** SalesForce table */
    SalesForceDataset salesforce(String name, @DelegatesTo(SalesForceDataset) Closure cl = null) {
        def parent = registerDataset(SALESFORCEDATASET, name) as SalesForceDataset
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** Xero connection */
    XeroConnection xeroConnection(String name = null, @DelegatesTo(XeroConnection) Closure cl = null) {
        def parent = registerConnection(XEROCONNECTION, name) as XeroConnection
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** Xero table */
    XeroDataset xero(String name = null, @DelegatesTo(XeroDataset) Closure cl = null) {
        def parent = registerDataset(XERODATASET, name) as XeroDataset
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(@DelegatesTo(TFS) Closure cl = null) {
        def parent = TFS.storage
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(String name = null, @DelegatesTo(TFSDataset) Closure cl = null) {
        TFSDataset parent = registerDataset(CSVTEMPDATASET, name) as TFSDataset

        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** Temporary CSV file */
    TFSDataset csvTempWithDataset(String name = null, Dataset sourceDataset, @DelegatesTo(TFSDataset) Closure cl = null) {
        if (sourceDataset == null) throw new ExceptionGETL("Dataset cannot be null!")
        TFSDataset parent = sourceDataset.csvTempFile
        if (name != null) registerDataset(parent, name, false)

        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST
            code.call(parent)
        }

        return parent
    }

    /** Copy rows from source to destination dataset */
    void copyRows(Dataset source, Dataset dest, @DelegatesTo(FlowCopySpec) Closure cl = null) {
        if (source == null) throw new ExceptionGETL('Source dataset cannot be null!')
        if (dest == null) throw new ExceptionGETL('Dest dataset cannot be null!')
        def parent = new FlowCopySpec()
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST

            code.call(source, dest)
        }

        def pt = startProcess("Copy rows from ${source} to ${dest}")
        Flow flow = new Flow()
        if (parent.onInit) parent.onInit.call()
        parent.prepareParams()
        def flowParams = (([source: source, dest: dest]) as Map<String, Object>) + parent.params
        flow.copy(flowParams)
        parent.countRow = flow.countRow
        parent.errorsDataset = flow.errorsDataset
        if (parent.onDone) parent.onDone.call()
        finishProcess(pt, parent.countRow)
    }

    /** Write rows to destination dataset */
    void rowsTo(Dataset dataset, @DelegatesTo(FlowWriteSpec) Closure cl) {
        if (dataset == null) throw new ExceptionGETL('Dataset cannot be null!')
        def parent = new FlowWriteSpec()
        def code = cl.rehydrate(this, parent, this)
        code.resolveStrategy = Closure.OWNER_FIRST

        code.call(dataset)

        def pt = startProcess("Write rows to $dataset")
        Flow flow = new Flow()
        if (parent.onInit != null) parent.onInit.call()
        parent.prepareParams()
        def flowParams = (([dest: dataset]) as Map<String, Object>) + parent.params
        flow.writeTo(flowParams)
        parent.countRow = flow.countRow
        if (parent.onDone != null) parent.onDone.call()
        finishProcess(pt, parent.countRow)
    }

    /** Write rows to many destination datasets */
    void rowsToMany(Map<String, Dataset> datasets, @DelegatesTo(FlowWriteManySpec) Closure cl) {
        if (datasets == null || datasets.isEmpty()) throw new ExceptionGETL('Dataset cannot be null or empty!')
        def parent = new FlowWriteManySpec()
        def code = cl.rehydrate(this, parent, this)
        code.resolveStrategy = Closure.OWNER_FIRST

        code.call(datasets)

        def destNames = [] as List<String>
        datasets.each { String destName, Dataset ds -> destNames.add("$destName: ${ds.toString()}".toString())}
        def pt = startProcess("Write rows to $destNames")
        Flow flow = new Flow()
        if (parent.onInit != null) parent.onInit.call()
        parent.prepareParams()
        def flowParams = (([dest: datasets]) as Map<String, Object>) + parent.params
        flow.writeAllTo(flowParams)
        if (parent.onDone != null) parent.onDone.call()
        finishProcess(pt)
    }

    /** Process rows from source dataset */
    void rowProcess(Dataset dataset, @DelegatesTo(FlowProcessSpec) Closure cl) {
        if (dataset == null) throw new ExceptionGETL('Dataset cannot be null!')
        def parent = new FlowProcessSpec()
        def code = cl.rehydrate(this, parent, this)
        code.resolveStrategy = Closure.OWNER_FIRST

        code.call(dataset)

        def pt = startProcess("Read rows from ${parent.source}")
        Flow flow = new Flow()
        if (parent.onInit != null) parent.onInit.call()
        parent.prepareParams()
        def flowParams = (([source: dataset]) as Map<String, Object>) + parent.params
        flow.process(flowParams)
        parent.countRow = flow.countRow
        parent.errorsDataset = flow.errorsDataset
        if (parent.onDone != null) parent.onDone.call()
        finishProcess(pt, parent.countRow)
    }

    /** SQL scripter */
    SQLScripter sql(SQLScripter parent = null, @DelegatesTo(SQLScripter) Closure cl) {
        if (parent == null) {
            parent = new SQLScripter()
            parent.connection = getDefaultJDBCConnection()
        }
        parent.vars.putAll(configVars)
        def code = cl.rehydrate(this, parent, this)
        code.resolveStrategy = Closure.OWNER_FIRST

        def pt = startProcess('Execution SQL script')
        code.call(parent)
        parent.vars.each { String name, value ->
            if (!configVars.containsKey(name)) configVars.put(name, value)
        }
        finishProcess(pt, parent.rowCount)

        return parent
    }

    /** Process local file system */
    FileManager files(String name, @DelegatesTo(FileManager) Closure cl = null) {
        def parent = registerFileManager(FILEMANAGER, name) as FileManager
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST

            def pt = startProcess('Work from local file system')
            code.call(parent)
            finishProcess(pt)
        }

        return parent
    }

    /** Process ftp file system */
    FTPManager ftp(String name = null, @DelegatesTo(FTPManager) Closure cl = null) {
        def parent = registerFileManager(FTPMANAGER, name) as FTPManager
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST

            def pt = startProcess('Work from FTP file system')
            try {
                code.call(parent)
            }
            finally {
                parent.disconnect()
            }
            finishProcess(pt)
        }

        return parent
    }

    /** Process sftp file system */
    SFTPManager sftp(String name = null, @DelegatesTo(SFTPManager) Closure cl = null) {
        def parent = registerFileManager(SFTPMANAGER, name) as SFTPManager
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST

            def pt = startProcess('Work from SFTP file system')
            try {
                code.call(parent)
            }
            finally {
                parent.disconnect()
            }
            finishProcess(pt)
        }

        return parent
    }

    /** Process sftp file system */
    HDFSManager hdfs(String name = null, @DelegatesTo(HDFSManager) Closure cl = null) {
        def parent = registerFileManager(HDFSMANAGER, name) as HDFSManager
        if (cl != null) {
            def code = cl.rehydrate(this, parent, this)
            code.resolveStrategy = Closure.OWNER_FIRST

            def pt = startProcess('Work from HDFS file system')
            try {
                code.call(parent)
            }
            finally {
                parent.disconnect()
            }
            finishProcess(pt)
        }

        return parent
    }

    /** Run code in multithread mode */
    void thread(@DelegatesTo(Executor) Closure cl) {
        def parent = new Executor()
        def code = cl.rehydrate(this, parent, this)
        code.resolveStrategy = Closure.OWNER_FIRST

        def pt = startProcess('Execution threads')
        code.call(parent)
        finishProcess(pt)
    }
}