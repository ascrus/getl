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

import getl.config.ConfigSlurper
import getl.config.opts.ConfigSpec
import getl.config.opts.LogSpec
import getl.csv.*
import getl.data.*
import getl.db2.*
import getl.excel.*
import getl.h2.*
import getl.hive.*
import getl.jdbc.*
import getl.json.*
import getl.mssql.*
import getl.mysql.*
import getl.netsuite.*
import getl.oracle.*
import getl.postgresql.*
import getl.proc.*
import getl.proc.opts.FlowCopySpec
import getl.proc.opts.FlowProcessSpec
import getl.proc.opts.FlowWriteManySpec
import getl.proc.opts.FlowWriteSpec
import getl.salesforce.*
import getl.tfs.*
import getl.utils.*
import getl.vertica.*
import getl.xero.*
import getl.xml.*
import groovy.transform.InheritConstructors

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
    }

    @Override
    Object run() {
        return null
    }

    /**
     * Run GETL lang closure
     */
    def static run(@DelegatesTo(Getl) Closure cl) {
        def lang = new Getl()
        def code = cl.rehydrate(lang, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(lang)
    }

    /**
     * Current configuration content
     */
    Map<String, Object> getConfigContent() { Config.content }

    /**
     * Write message as level the INFO to log
     */
    public void logInfo(String msg) { Logs.Info(msg) }

    /**
     * Write message as level the WARNING to log
     */
    public void logWarn(String msg) { Logs.Warning(msg) }

    /**
     * Write message as level the SEVERE to log
     */
    public void logError(String msg) { Logs.Severe(msg) }

    /**
     * Write message as level the FINE to log
     */
    public void logFine(String msg) { Logs.Fine(msg) }

    /**
     * Write message as level the FINER to log
     */
    public void logFiner(String msg) { Logs.Finer(msg) }

    /**
     * Write message as level the FINEST to log
     */
    public void logFinest(String msg) { Logs.Finest(msg) }

    /**
     * Write message as level the CONFIG to log
     */
    public void logConfig(String msg) { Logs.Config(msg) }

    /**
     * Current date and time
     */
    public Date getNow() { DateUtils.Now() }

    /**
     * Configuration options
     */
    ConfigSpec config(ConfigSpec parent = null, @DelegatesTo(ConfigSpec) Closure cl) {
        if (parent == null) parent = new ConfigSpec()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * Log options
     */
    LogSpec log(LogSpec parent = null, @DelegatesTo(LogSpec) Closure cl) {
        if (parent == null) parent = new LogSpec()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }


    /**
     * Field
     */
    Field field(Field parent = null, @DelegatesTo(Field) Closure cl) {
        if (parent == null) parent = new Field()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * JDBC connection
     */
    JDBCConnection jdbcConnection(JDBCConnection parent = null, String conClass = null, @DelegatesTo(JDBCConnection) Closure cl) {
        if (parent == null) {
            parent = JDBCConnection.CreateConnection(connection: conClass)
        }
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * H2 database connection
     */
    H2Connection h2Connection(H2Connection parent = null, @DelegatesTo(H2Connection) Closure cl) {
        jdbcConnection(parent, 'getl.h2.H2Connection', cl) as H2Connection
    }

    /**
     * H2 database table
     */
    H2Table h2table(TableDataset parent = null, @DelegatesTo(H2Table) Closure cl) {
        if (parent == null) parent = new H2Table()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * DB2 connection
     */
    DB2Connection db2Connection(DB2Connection parent = null, @DelegatesTo(DB2Connection) Closure cl) {
        jdbcConnection(parent, 'getl.db2.DB2Connection', cl) as DB2Connection
    }

    /**
     * Hive connection
     */
    HiveConnection hiveConnection(HiveConnection parent = null, @DelegatesTo(HiveConnection) Closure cl) {
        jdbcConnection(parent, 'getl.hive.HiveConnection', cl) as HiveConnection
    }

    /**
     * Hive table
     */
    HiveTable hivetable(TableDataset parent = null, @DelegatesTo(HiveTable) Closure cl) {
        if (parent == null) parent = new HiveTable()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * MSSQL connection
     */
    MSSQLConnection mssqlConnection(MSSQLConnection parent = null, @DelegatesTo(MSSQLConnection) Closure cl) {
        jdbcConnection(parent, 'getl.mssql.MSSQLConnection', cl) as MSSQLConnection
    }

    /**
     * MySQL connection
     */
    MySQLConnection mysqlConnection(MySQLConnection parent = null, @DelegatesTo(MySQLConnection) Closure cl) {
        jdbcConnection(parent, 'getl.mysql.MySQLConnection', cl) as MySQLConnection
    }

    /**
     * Oracle connection
     */
    OracleConnection oracleConnection(OracleConnection parent = null, @DelegatesTo(OracleConnection) Closure cl) {
        jdbcConnection(parent, 'getl.oracle.OracleConnection', cl) as OracleConnection
    }

    /**
     * PostgreSQL connection
     */
    PostgreSQLConnection postgresqlConnection(PostgreSQLConnection parent = null, @DelegatesTo(PostgreSQLConnection) Closure cl) {
        jdbcConnection(parent, 'getl.postgresql.PostgreSQLConnection', cl) as PostgreSQLConnection
    }

    /**
     * Vertica connection
     */
    VerticaConnection verticaConnection(VerticaConnection parent = null, @DelegatesTo(VerticaConnection) Closure cl) {
        jdbcConnection(parent, 'getl.vertica.VerticaConnection', cl) as VerticaConnection
    }

    /**
     * NetSuite connection
     */
    NetsuiteConnection netsuiteConnection(NetsuiteConnection parent = null, @DelegatesTo(NetsuiteConnection) Closure cl) {
        jdbcConnection(parent, 'getl.netsuite.NetsuiteConnection', cl) as NetsuiteConnection
    }

    /**
     * Temporary DB connection
     */
    TDS tempdb(TDS parent = null, @DelegatesTo(TDS) Closure cl) {
        if (parent == null) parent = new TDS()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * Tempopary DB connection
     */
    public TDS getTempdb() { new TDS() }

    /**
     * JDBC table
     */
    TableDataset table(TableDataset parent = null, @DelegatesTo(TableDataset) Closure cl) {
        if (parent == null) parent = new TableDataset()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * JDBC query
     */
    QueryDataset query(QueryDataset parent = null, @DelegatesTo(QueryDataset) Closure cl) {
        if (parent == null) parent = new QueryDataset()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * CSV connection
     */
    CSVConnection csvConnection(CSVConnection parent = null, @DelegatesTo(CSVConnection) Closure cl) {
        if (parent == null) {
            parent = new CSVConnection()
        }
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * CSV delimiter file
     */
    CSVDataset csv(CSVDataset parent = null, @DelegatesTo(CSVDataset) Closure cl) {
        if (parent == null) parent = new CSVDataset()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * Excel connection
     */
    ExcelConnection excelConnection(ExcelConnection parent = null, @DelegatesTo(ExcelConnection) Closure cl) {
        if (parent == null) {
            parent = new ExcelConnection()
        }
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * Excel file
     */
    ExcelDataset excel(ExcelDataset parent = null, @DelegatesTo(ExcelDataset) Closure cl) {
        if (parent == null) parent = new ExcelDataset()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * JSON connection
     */
    JSONConnection jsonConnection(JSONConnection parent = null, @DelegatesTo(JSONConnection) Closure cl) {
        if (parent == null) {
            parent = new JSONConnection()
        }
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * JSON file
     */
    JSONDataset json(JSONDataset parent = null, @DelegatesTo(JSONDataset) Closure cl) {
        if (parent == null) parent = new JSONDataset()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * XML connection
     */
    XMLConnection xmlConnection(XMLConnection parent = null, @DelegatesTo(XMLConnection) Closure cl) {
        if (parent == null) {
            parent = new XMLConnection()
        }
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * XML file
     */
    XMLDataset xml(XMLDataset parent = null, @DelegatesTo(XMLDataset) Closure cl) {
        if (parent == null) parent = new XMLDataset()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * SalesForce connection
     */
    SalesForceConnection salesforceConnection(SalesForceConnection parent = null, @DelegatesTo(SalesForceConnection) Closure cl) {
        if (parent == null) {
            parent = new SalesForceConnection()
        }
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * SalesForce table
     */
    SalesForceDataset salesforce(SalesForceDataset parent = null, @DelegatesTo(SalesForceDataset) Closure cl) {
        if (parent == null) parent = new SalesForceDataset()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * Xero connection
     */
    XeroConnection xeroConnection(XeroConnection parent = null, @DelegatesTo(XeroConnection) Closure cl) {
        if (parent == null) {
            parent = new XeroConnection()
        }
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * Xero table
     */
    XeroDataset xero(XeroDataset parent = null, @DelegatesTo(XeroDataset) Closure cl) {
        if (parent == null) parent = new XeroDataset()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * Temporary CSV connection
     */
    public TFS getCsvTempConnection() { TFS.storage }

    /**
     * Temporary CSV file
     */
    public TFSDataset getCsvTemp() { TFS.dataset() }

    /**
     * Temporary CSV file
     */
    TFSDataset csvTemp(TFSDataset parent = null, @DelegatesTo(TFSDataset) Closure cl) {
        if (parent == null) parent = TFS.dataset()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * Copy rows from source to destination dataset
     */
    FlowCopySpec copyRows(FlowCopySpec parent = null, @DelegatesTo(FlowCopySpec) Closure cl) {
        if (parent == null) parent = new FlowCopySpec()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)
        new Flow().copy(parent)

        return parent
    }

    /**
     * Write rows to destination dataset
     */
    FlowWriteSpec rowsTo(FlowWriteSpec parent = null, @DelegatesTo(FlowWriteSpec) Closure cl) {
        if (parent == null) parent = new FlowWriteSpec()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)
        new Flow().writeTo(parent)

        return parent
    }

    /**
     * Write rows to many destination datasets
     */
    FlowWriteManySpec rowsToMany(FlowWriteManySpec parent = null, @DelegatesTo(FlowWriteManySpec) Closure cl) {
        if (parent == null) parent = new FlowWriteManySpec()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)
        new Flow().writeAllTo(parent)

        return parent
    }

    /**
     * Process rows from source dataset
     */
    FlowProcessSpec rowProcess(FlowProcessSpec parent = null, @DelegatesTo(FlowProcessSpec) Closure cl) {
        if (parent == null) parent = new FlowProcessSpec()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)
        new Flow().process(parent)

        return parent
    }
}