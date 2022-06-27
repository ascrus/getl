//file:noinspection unused
package getl.lang.sub

import getl.csv.CSVConnection
import getl.data.Connection
import getl.db2.DB2Connection
import getl.dbf.DBFConnection
import getl.driver.Driver
import getl.excel.ExcelConnection
import getl.firebird.FirebirdConnection
import getl.h2.H2Connection
import getl.hive.HiveConnection
import getl.impala.ImpalaConnection
import getl.jdbc.JDBCConnection
import getl.json.JSONConnection
import getl.kafka.KafkaConnection
import getl.mssql.MSSQLConnection
import getl.mysql.MySQLConnection
import getl.netezza.NetezzaConnection
import getl.netsuite.NetsuiteConnection
import getl.oracle.OracleConnection
import getl.postgresql.PostgreSQLConnection
import getl.salesforce.SalesForceConnection
import getl.tfs.TDS
import getl.tfs.TFS
import getl.vertica.VerticaConnection
import getl.xml.XMLConnection
import getl.yaml.YAMLConnection
import groovy.transform.InheritConstructors

/**
 * Repository connections manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
@SuppressWarnings(['GrMethodMayBeStatic', 'SpellCheckingInspection'])
class RepositoryConnections extends RepositoryObjects<Connection> {
    static public final String CSVCONNECTION = CSVConnection.name
    static public final String CSVTEMPCONNECTION = TFS.name
    static public final String DB2CONNECTION = DB2Connection.name
    static public final String DBFCONNECTION = DBFConnection.name
    static public final String EXCELCONNECTION = ExcelConnection.name
    static public final String FIREBIRDCONNECTION = FirebirdConnection.name
    static public final String H2CONNECTION = H2Connection.name
    static public final String HIVECONNECTION = HiveConnection.name
    static public final String IMPALACONNECTION = ImpalaConnection.name
    static public final String JDBCCONNECTION = JDBCConnection.name
    static public final String JSONCONNECTION = JSONConnection.name
    static public final String KAFKACONNECTION = KafkaConnection.name
    static public final String MSSQLCONNECTION = MSSQLConnection.name
    static public final String MYSQLCONNECTION = MySQLConnection.name
    static public final String NETEZZACONNECTION = NetezzaConnection.name
    static public final String NETSUITECONNECTION = NetsuiteConnection.name
    static public final String ORACLECONNECTION = OracleConnection.name
    static public final String POSTGRESQLCONNECTION = PostgreSQLConnection.name
    static public final String SALESFORCECONNECTION = SalesForceConnection.name
    static public final String EMBEDDEDCONNECTION = TDS.name
    static public final String VERTICACONNECTION = VerticaConnection.name
    static public final String XMLCONNECTION = XMLConnection.name
    static public final String YAMLCONNECTION = YAMLConnection.name

    /** List of allowed connection classes */
    static public final List<String> LISTCONNECTIONS = [
        CSVCONNECTION, CSVTEMPCONNECTION, DB2CONNECTION, DBFCONNECTION, EMBEDDEDCONNECTION, EXCELCONNECTION, FIREBIRDCONNECTION,
        H2CONNECTION, HIVECONNECTION, IMPALACONNECTION, JDBCCONNECTION, JSONCONNECTION, KAFKACONNECTION, MSSQLCONNECTION,
        MYSQLCONNECTION, NETEZZACONNECTION, NETSUITECONNECTION, ORACLECONNECTION, POSTGRESQLCONNECTION, SALESFORCECONNECTION,
        VERTICACONNECTION, XMLCONNECTION, YAMLCONNECTION
    ]

    /** List of allowed jdbc connection classes */
    static public final List<String> LISTJDBCCONNECTIONS = [
        DB2CONNECTION, EMBEDDEDCONNECTION, FIREBIRDCONNECTION, H2CONNECTION, HIVECONNECTION, IMPALACONNECTION,
        JDBCCONNECTION, MSSQLCONNECTION, MYSQLCONNECTION, NETEZZACONNECTION, NETSUITECONNECTION, ORACLECONNECTION,
        POSTGRESQLCONNECTION, VERTICACONNECTION
    ]

    /** List of allowed other connection classes */
    static public final List<String> LISTFILECONNECTIONS = [
        CSVCONNECTION, CSVTEMPCONNECTION, DBFCONNECTION, EXCELCONNECTION, JSONCONNECTION, XMLCONNECTION, YAMLCONNECTION
    ]

    /** List of allowed jdbc connection classes */
    static public final List<String> LISTOTHERCONNECTIONS = [
        SALESFORCECONNECTION, KAFKACONNECTION
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
    Map exportConfig(GetlRepository repObj) {
        return [connection: repObj.getClass().name] + ((repObj as Connection).params)
    }

    @Override
    GetlRepository importConfig(Map config, GetlRepository existObject) {
        (existObject != null)?(existObject as Connection).importParams(config):Connection.CreateConnection(config)
    }

    @Override
    Boolean needEnvConfig() { true }

    @Override
    protected void processUnregisteringObject(Connection obj) {
        super.processUnregisteringObject(obj)
        if (obj.driver?.isSupport(Driver.Support.CONNECT))
            obj.connected = false
    }
}