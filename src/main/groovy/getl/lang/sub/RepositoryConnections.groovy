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
package getl.lang.sub

import getl.csv.CSVConnection
import getl.data.Connection
import getl.db2.DB2Connection
import getl.excel.ExcelConnection
import getl.exception.ExceptionDSL
import getl.firebird.FirebirdConnection
import getl.h2.H2Connection
import getl.hive.HiveConnection
import getl.impala.ImpalaConnection
import getl.jdbc.JDBCConnection
import getl.json.JSONConnection
import getl.mssql.MSSQLConnection
import getl.mysql.MySQLConnection
import getl.netezza.NetezzaConnection
import getl.netsuite.NetsuiteConnection
import getl.oracle.OracleConnection
import getl.postgresql.PostgreSQLConnection
import getl.salesforce.SalesForceConnection
import getl.tfs.TDS
import getl.tfs.TFS
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.MapUtils
import getl.vertica.VerticaConnection
import getl.xero.XeroConnection
import getl.xml.XMLConnection
import getl.yaml.YAMLConnection
import groovy.transform.InheritConstructors

/**
 * Repository connections manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositoryConnections extends RepositoryObjects<Connection> {
    public static final String CSVCONNECTION = CSVConnection.name
    public static final String CSVTEMPCONNECTION = TFS.name
    public static final String DB2CONNECTION = DB2Connection.name
    public static final String EXCELCONNECTION = ExcelConnection.name
    public static final String FIREBIRDCONNECTION = FirebirdConnection.name
    public static final String H2CONNECTION = H2Connection.name
    public static final String HIVECONNECTION = HiveConnection.name
    public static final String IMPALACONNECTION = ImpalaConnection.name
    public static final String JDBCCONNECTION = JDBCConnection.name
    public static final String JSONCONNECTION = JSONConnection.name
    public static final String MSSQLCONNECTION = MSSQLConnection.name
    public static final String MYSQLCONNECTION = MySQLConnection.name
    public static final String NETEZZACONNECTION = NetezzaConnection.name
    public static final String NETSUITECONNECTION = NetsuiteConnection.name
    public static final String ORACLECONNECTION = OracleConnection.name
    public static final String POSTGRESQLCONNECTION = PostgreSQLConnection.name
    public static final String SALESFORCECONNECTION = SalesForceConnection.name
    public static final String EMBEDDEDCONNECTION = TDS.name
    public static final String VERTICACONNECTION = VerticaConnection.name
    public static final String XEROCONNECTION = XeroConnection.name
    public static final String XMLCONNECTION = XMLConnection.name
    public static final String YAMLCONNECTION = YAMLConnection.name

    /** List of allowed connection classes */
    public static final List<String> LISTCONNECTIONS = [
        CSVCONNECTION, CSVTEMPCONNECTION, DB2CONNECTION, EMBEDDEDCONNECTION, EXCELCONNECTION, FIREBIRDCONNECTION,
        H2CONNECTION, HIVECONNECTION, IMPALACONNECTION, JDBCCONNECTION, JSONCONNECTION, MSSQLCONNECTION, MYSQLCONNECTION,
        NETEZZACONNECTION, NETSUITECONNECTION, ORACLECONNECTION, POSTGRESQLCONNECTION, SALESFORCECONNECTION,
        VERTICACONNECTION, XEROCONNECTION, XMLCONNECTION, YAMLCONNECTION
    ]

    /** List of allowed jdbc connection classes */
    public static final List<String> LISTJDBCCONNECTIONS = [
        DB2CONNECTION, EMBEDDEDCONNECTION, FIREBIRDCONNECTION, H2CONNECTION, HIVECONNECTION, IMPALACONNECTION,
        JDBCCONNECTION, MSSQLCONNECTION, MYSQLCONNECTION, NETEZZACONNECTION, NETSUITECONNECTION, ORACLECONNECTION,
        POSTGRESQLCONNECTION, VERTICACONNECTION
    ]

    /** List of allowed other connection classes */
    public static final List<String> LISTFILECONNECTIONS = [
        CSVCONNECTION, CSVTEMPCONNECTION, EXCELCONNECTION, JSONCONNECTION, XMLCONNECTION, YAMLCONNECTION
    ]

    /** List of allowed jdbc connection classes */
    public static final List<String> LISTOTHERCONNECTIONS = [
        SALESFORCECONNECTION, XEROCONNECTION
    ]

    /** List of allowed connection classes */
    @Override
    List<String> getListClasses() { LISTCONNECTIONS }

    /** List of allowed jdbc connection classes */
    List<String> getListJdbcClasses() { LISTJDBCCONNECTIONS }

    /** List of allowed file connection classes */
    List<String> getListFileClasses() { LISTFILECONNECTIONS }

    /** List of allowed other connection classes */
    List<String> getListOtherClasses() { LISTOTHERCONNECTIONS }

    @Override
    protected Connection createObject(String className) {
        Connection.CreateConnection(connection: className)
    }

    @Override
    Map exportConfig(GetlRepository repobj) {
        return [connection: repobj.class.name] + ((repobj as Connection).params)
    }

    @Override
    GetlRepository importConfig(Map config) {
        return Connection.CreateConnection(config)
    }

    @Override
    boolean needEnvConfig() { true }

    @Override
    protected void initRegisteredObject(Connection obj) {
        if (obj instanceof JDBCConnection && dslCreator.options.jdbcConnectionLoggingPath != null) {
            def objname = ParseObjectName.Parse(obj.dslNameObject)
            (obj as JDBCConnection).sqlHistoryFile = FileUtils.ConvertToDefaultOSPath(dslCreator.options.jdbcConnectionLoggingPath + '/' + objname.toFileName() + '/{date}.sql')
        }
    }
}