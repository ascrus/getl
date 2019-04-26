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
        getl.deploy.Version.SayInfo()
    }

    protected Getl(Binding binding) {
        super(binding)
        getl.deploy.Version.SayInfo()
    }

    @Override
    Object run() {
        return null
    }

    def static run(@DelegatesTo(Getl) Closure cl) {
        def lang = new Getl()
        def code = cl.rehydrate(lang, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
    }

    public void logInfo(String msg) { Logs.Info(msg) }
    public void logWarn(String msg) { Logs.Warning(msg) }
    public void logError(String msg) { Logs.Severe(msg) }
    public void logFine(String msg) { Logs.Fine(msg) }
    public void logFiner(String msg) { Logs.Finer(msg) }
    public void logFinest(String msg) { Logs.Finest(msg) }
    public void logConfig(String msg) { Logs.Config(msg) }

    public Date getNow() { DateUtils.Now() }

    public Field.Type getIntegerFieldType() { Field.Type.INTEGER}
    public Field.Type getBigintFieldType() { Field.Type.BIGINT}
    public Field.Type getNumericFieldType() { Field.Type.NUMERIC}
    public Field.Type getDoubleFieldType() { Field.Type.DOUBLE}
    public Field.Type getStringFieldType() { Field.Type.STRING}
    public Field.Type getTextFieldType() { Field.Type.TEXT}
    public Field.Type getDateFieldType() { Field.Type.DATE}
    public Field.Type getTimeFieldType() { Field.Type.TIME}
    public Field.Type getDatetimeFieldType() { Field.Type.DATETIME}
    public Field.Type getBooleanFieldType() { Field.Type.BOOLEAN}
    public Field.Type getBlobFieldType() { Field.Type.BLOB}
    public Field.Type getUuidFieldType() { Field.Type.UUID}
    public Field.Type getRowidFieldType() { Field.Type.ROWID}
    public Field.Type getObjectFieldType() { Field.Type.OBJECT}

    LogSpec log(LogSpec parent = null, @DelegatesTo(LogSpec) Closure cl) {
        if (parent == null) parent = new LogSpec()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    Field field(Field parent = null, @DelegatesTo(Field) Closure cl) {
        if (parent == null) parent = new Field()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    JDBCConnection jdbcConnection(JDBCConnection parent = null, String conClass = null, @DelegatesTo(JDBCConnection) Closure cl) {
        if (parent == null) {
            parent = JDBCConnection.CreateConnection(connection: conClass)
        }
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    H2Connection h2Connection(H2Connection parent = null, @DelegatesTo(H2Connection) Closure cl) {
        jdbcConnection(parent, 'getl.h2.H2Connection', cl) as H2Connection
    }

    DB2Connection db2Connection(DB2Connection parent = null, @DelegatesTo(DB2Connection) Closure cl) {
        jdbcConnection(parent, 'getl.db2.DB2Connection', cl) as DB2Connection
    }

    HiveConnection hiveConnection(HiveConnection parent = null, @DelegatesTo(HiveConnection) Closure cl) {
        jdbcConnection(parent, 'getl.hive.HiveConnection', cl) as HiveConnection
    }

    MSSQLConnection mssqlConnection(MSSQLConnection parent = null, @DelegatesTo(MSSQLConnection) Closure cl) {
        jdbcConnection(parent, 'getl.mssql.MSSQLConnection', cl) as MSSQLConnection
    }

    MySQLConnection mysqlConnection(MySQLConnection parent = null, @DelegatesTo(MySQLConnection) Closure cl) {
        jdbcConnection(parent, 'getl.mysql.MySQLConnection', cl) as MySQLConnection
    }

    OracleConnection oracleConnection(OracleConnection parent = null, @DelegatesTo(OracleConnection) Closure cl) {
        jdbcConnection(parent, 'getl.oracle.OracleConnection', cl) as OracleConnection
    }

    PostgreSQLConnection postgresqlConnection(PostgreSQLConnection parent = null, @DelegatesTo(PostgreSQLConnection) Closure cl) {
        jdbcConnection(parent, 'getl.postgresql.PostgreSQLConnection', cl) as PostgreSQLConnection
    }

    VerticaConnection verticaConnection(VerticaConnection parent = null, @DelegatesTo(VerticaConnection) Closure cl) {
        jdbcConnection(parent, 'getl.vertica.VerticaConnection', cl) as VerticaConnection
    }

    NetsuiteConnection netsuiteConnection(NetsuiteConnection parent = null, @DelegatesTo(NetsuiteConnection) Closure cl) {
        jdbcConnection(parent, 'getl.netsuite.NetsuiteConnection', cl) as NetsuiteConnection
    }

    TDS tempdb(TDS parent = null, @DelegatesTo(TDS) Closure cl) {
        if (parent == null) parent = new TDS()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    public TDS getTempdb() { new TDS() }

    TableDataset table(TableDataset parent = null, @DelegatesTo(TableDataset) Closure cl) {
        if (parent == null) parent = new TableDataset()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    QueryDataset query(QueryDataset parent = null, @DelegatesTo(QueryDataset) Closure cl) {
        if (parent == null) parent = new QueryDataset()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    CSVConnection csvConnection(CSVConnection parent = null, @DelegatesTo(CSVConnection) Closure cl) {
        if (parent == null) {
            parent = new CSVConnection()
        }
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    CSVDataset csv(CSVDataset parent = null, @DelegatesTo(CSVDataset) Closure cl) {
        if (parent == null) parent = new CSVDataset()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    ExcelConnection excelConnection(ExcelConnection parent = null, @DelegatesTo(ExcelConnection) Closure cl) {
        if (parent == null) {
            parent = new ExcelConnection()
        }
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    ExcelDataset excel(ExcelDataset parent = null, @DelegatesTo(ExcelDataset) Closure cl) {
        if (parent == null) parent = new ExcelDataset()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    JSONConnection jsonConnection(JSONConnection parent = null, @DelegatesTo(JSONConnection) Closure cl) {
        if (parent == null) {
            parent = new JSONConnection()
        }
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    JSONDataset json(JSONDataset parent = null, @DelegatesTo(JSONDataset) Closure cl) {
        if (parent == null) parent = new JSONDataset()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    XMLConnection xmlConnection(XMLConnection parent = null, @DelegatesTo(XMLConnection) Closure cl) {
        if (parent == null) {
            parent = new XMLConnection()
        }
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    XMLDataset xml(XMLDataset parent = null, @DelegatesTo(XMLDataset) Closure cl) {
        if (parent == null) parent = new XMLDataset()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    SalesForceConnection salesforceConnection(SalesForceConnection parent = null, @DelegatesTo(SalesForceConnection) Closure cl) {
        if (parent == null) {
            parent = new SalesForceConnection()
        }
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    SalesForceDataset salesforce(SalesForceDataset parent = null, @DelegatesTo(SalesForceDataset) Closure cl) {
        if (parent == null) parent = new SalesForceDataset()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    XeroConnection xeroConnection(XeroConnection parent = null, @DelegatesTo(XeroConnection) Closure cl) {
        if (parent == null) {
            parent = new XeroConnection()
        }
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    XeroDataset xero(XeroDataset parent = null, @DelegatesTo(XeroDataset) Closure cl) {
        if (parent == null) parent = new XeroDataset()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    public TFS getTempPath() { TFS.storage }
    public TFSDataset getTempFile() { TFS.dataset() }
    TFSDataset tempFile(TFSDataset parent = null, @DelegatesTo(TFSDataset) Closure cl) {
        if (parent == null) parent = TFS.dataset()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        return parent
    }

    FlowCopySpec copyRows(FlowCopySpec parent = null, @DelegatesTo(FlowCopySpec) Closure cl) {
        if (parent == null) parent = new FlowCopySpec()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        new Flow().copy(parent)
        return parent
    }

    FlowWriteSpec rowsTo(FlowWriteSpec parent = null, @DelegatesTo(FlowWriteSpec) Closure cl) {
        if (parent == null) parent = new FlowWriteSpec()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        new Flow().writeTo(parent)
        return parent
    }

    FlowWriteManySpec rowsToMany(FlowWriteManySpec parent = null, @DelegatesTo(FlowWriteManySpec) Closure cl) {
        if (parent == null) parent = new FlowWriteManySpec()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        new Flow().writeAllTo(parent)
        return parent
    }

    FlowProcessSpec rowProcess(FlowProcessSpec parent = null, @DelegatesTo(FlowProcessSpec) Closure cl) {
        if (parent == null) parent = new FlowProcessSpec()
        def code = cl.rehydrate(parent, this, this)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code()
        new Flow().process(parent)
        return parent
    }
}