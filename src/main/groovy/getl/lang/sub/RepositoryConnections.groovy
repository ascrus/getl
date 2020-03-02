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

import getl.data.Connection
import groovy.transform.InheritConstructors

/**
 * Repository connections manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositoryConnections extends RepositoryObjects<Connection> {
    public static final String CSVCONNECTION = 'getl.csv.CSVConnection'
    public static final String CSVTEMPCONNECTION = 'getl.tfs.TFS'
    public static final String DB2CONNECTION = 'getl.db2.DB2Connection'
    public static final String EXCELCONNECTION = 'getl.excel.ExcelConnection'
    public static final String FIREBIRDCONNECTION = 'getl.firebird.FirebirdConnection'
    public static final String H2CONNECTION = 'getl.h2.H2Connection'
    public static final String HIVECONNECTION = 'getl.hive.HiveConnection'
    public static final String IMPALACONNECTION = 'getl.impala.ImpalaConnection'
    public static final String JSONCONNECTION = 'getl.json.JSONConnection'
    public static final String MSSQLCONNECTION = 'getl.mssql.MSSQLConnection'
    public static final String MYSQLCONNECTION = 'getl.mysql.MySQLConnection'
    public static final String NETEZZACONNECTION = 'getl.netezza.NetezzaConnection'
    public static final String NETSUITECONNECTION = 'getl.netsuite.NetsuiteConnection'
    public static final String ORACLECONNECTION = 'getl.oracle.OracleConnection'
    public static final String POSTGRESQLCONNECTION = 'getl.postgresql.PostgreSQLConnection'
    public static final String SALESFORCECONNECTION = 'getl.salesforce.SalesForceConnection'
    public static final String EMBEDDEDCONNECTION = 'getl.tfs.TDS'
    public static final String VERTICACONNECTION = 'getl.vertica.VerticaConnection'
    public static final String XEROCONNECTION = 'getl.xero.XeroConnection'
    public static final String XMLCONNECTION = 'getl.xml.XMLConnection'

    @Override
    List<String> getListClasses() {
        [
            CSVCONNECTION, CSVTEMPCONNECTION, DB2CONNECTION, EMBEDDEDCONNECTION, EXCELCONNECTION, FIREBIRDCONNECTION,
            H2CONNECTION, HIVECONNECTION, IMPALACONNECTION, JSONCONNECTION, MSSQLCONNECTION, MYSQLCONNECTION,
            NETEZZACONNECTION, NETSUITECONNECTION, ORACLECONNECTION, POSTGRESQLCONNECTION, SALESFORCECONNECTION,
            VERTICACONNECTION, XEROCONNECTION, XMLCONNECTION
        ]
    }

    @Override
    protected Connection createObject(String className) {
        Connection.CreateConnection(connection: className)
    }

    @Override
    protected String getNameCloneCollection() { 'connections' }
}
