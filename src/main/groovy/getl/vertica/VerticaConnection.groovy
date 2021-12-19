//file:noinspection unused
package getl.vertica

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.stat.ProcessTime
import getl.utils.BoolUtils
import getl.utils.Path
import getl.utils.StringUtils
import groovy.transform.InheritConstructors
import getl.jdbc.JDBCConnection
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import static getl.utils.StringUtils.WithGroupSeparator

/**
 * Vertica connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VerticaConnection extends JDBCConnection {
	@Override
	protected Class<Driver> driverClass() { VerticaDriver }

	/** Current Vertica connection driver */
	@JsonIgnore
	VerticaDriver getCurrentVerticaDriver() { driver as VerticaDriver }

	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = 'com.vertica.jdbc.Driver'
	}

	/** Current session parameters */
	@JsonIgnore
	Map<String, Object> getCurrentSession() {
		def query = new QueryDataset(connection: this, query: 'SELECT * FROM current_session')
		return query.rows()[0]
	}

	private final Map<String, String> attachedVertica = [:] as Map<String, String>

	/** Detect database name from connection */
	static protected String DatabaseFromConnection(VerticaConnection con) {
		String database
		if (con.connectURL != null)
		{
			def p = new Path(mask: 'jdbc:vertica://{host}/{database}')
			def m = p.analyze(con.connectURL)

			database = m.database as String
			if (database == null)
				throw new ExceptionGETL("Invalid connect URL, database unreachable!")
			def i = database.indexOf('?')
			if (i != -1) {
				database = database.substring(0, i - 1)
			}
		}
		else {
			database = con.connectDatabase
			if (database == null)
				throw new ExceptionGETL("No database is specified for the connection!")
		}

		return database
	}

	/** Verify that the specified connection is attached to the current connection */
	Boolean isAttachConnection(VerticaConnection connection) {
		attachedVertica.containsKey(connection.toString().toLowerCase())
	}

	/**
	 * Create connections to another Vertica cluster for the current session
	 * @param anotherConnection attachable connection
	 */
	void attachExternalVertica(VerticaConnection anotherConnection) {
		tryConnect()

		if (isAttachConnection(anotherConnection))
			throw new ExceptionGETL("The Vertica cluster \"$anotherConnection\" is already attached to the current connection!")

		if (anotherConnection.login == null)
			throw new ExceptionGETL("No login is specified for the connection \"$anotherConnection\"!")

		def password = anotherConnection.loginManager.currentDecryptPassword()
		if (password == null)
			throw new ExceptionGETL("No password is specified for the connection \"$anotherConnection\"!")

		def database, host, port = 5433
		if (anotherConnection.connectURL != null) {
			def p = new Path(mask: 'jdbc:vertica://{host}/{database}')
			def m = p.analyze(anotherConnection.connectURL)
			host = m.host as String
			if (host == null)
				throw new ExceptionGETL("Invalid connect URL, host unreachable in connection \"$anotherConnection\"!")
			def i = host.indexOf(':')
			if (i != -1) {
				port = Integer.valueOf(host.substring(i + 1))
				host = host.substring(0, i)
			}

			database = m.database as String
			if (database == null)
				throw new ExceptionGETL("Invalid connect URL, database unreachable in connection \"$anotherConnection\"!")
			i = database.indexOf('?')
			if (i != -1) {
				database = database.substring(0, i - 1)
			}
		}
		else {
			host = anotherConnection.connectHost
			if (host == null)
				throw new ExceptionGETL("No host is specified for the connection \"$anotherConnection\"!")
			port = anotherConnection.connectPortNumber?:5433
			database = anotherConnection.connectDatabase
			if (database == null)
				throw new ExceptionGETL("No database is specified for the connection \"$anotherConnection\"!")
		}

		def command = "CONNECT TO VERTICA {database} USER {login} PASSWORD '{password}' ON '{host}',{port}"
		def p = [host: host, port: port, database: database,
					  login: anotherConnection.login, password: password]
		def h = StringUtils.EvalMacroString(command, p + [password: StringUtils.Replicate('*', password.length())])
		executeCommand(command, [queryParams: p, historyText: h])

		attachedVertica.put(anotherConnection.toString().toLowerCase(), database)
	}

	/**
	 * Disconnect specified connection from current
	 * @param anotherConnection detachable connection
	 */
	void detachExternalVertica(VerticaConnection anotherConnection) {
		tryConnect()

		def database = attachedVertica.get(anotherConnection.toString().toLowerCase())
		if (database == null)
			throw new ExceptionGETL("The Vertica cluster $anotherConnection is not attached to the current connection!")

		executeCommand("DISCONNECT $database")
		attachedVertica.remove(anotherConnection.toString().toLowerCase())
	}

	/**
	 * Purge tables with deleted records
	 * @param filter the need for table processing
	 * @return count of purged tables
	 */
	Integer purgeTables(@ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaTable']) Closure<Boolean> filter) {
		purgeTables(20, filter)
	}

	/**
	 * Purge tables with deleted records
	 * @param percentOfDeleteRows percentage of deleted records threshold of total
	 * @param filter the need for table processing
	 * @return count of purged tables
	 */
	Integer purgeTables(Integer percentOfDeleteRows = 20,
					 @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaTable']) Closure<Boolean> filter = null) {
		tryConnect()

		Integer res = 0

		new QueryDataset().tap {
			useConnection this

			query = """
SELECT 
	p.projection_schema as table_schema, 
	p.anchor_table_name AS table_name, 
	Sum(total_row_count) AS total_row_count, 
	Sum(deleted_row_count) AS deleted_row_count,
	Round(Sum(deleted_row_count) / Sum(total_row_count) * 100, 0)::int AS threshold
FROM storage_containers c
	INNER JOIN projections p ON p.projection_id = c.projection_id AND p.is_super_projection
GROUP BY p.projection_schema, p.anchor_table_name
HAVING Round(Sum(deleted_row_count) / Sum(total_row_count) * 100, 0)::int > $percentOfDeleteRows
ORDER BY threshold DESC, table_name;
		"""

			def rows = rows()

			rows.each { row ->
				def table = new VerticaTable(connection: this, schemaName: row.table_schema, tableName: row.table_name)
				if (filter == null || filter(table)) {
					logger.fine("Purging table $table with threshold ${row.threshold} " +
							"(total ${WithGroupSeparator(row.total_row_count as Long)} rows, " +
							"marked for deletion ${WithGroupSeparator(row.deleted_row_count as Long)} rows)")
					try {
						table.purgeTable()
						res++
					}
					catch (Exception e) {
						logger.severe(e.message)
					}
				}
			}
		}

		return res
	}

	/**
	 * Collects and aggregates data samples and storage information from all tables in schema
	 * @param percent percentage of data to read from disk
	 * @return 0 — Success
	 */
	Integer analyzeStatistics(Integer percent) {
		analyzeStatistics(null, percent)
	}

	/**
	 * Collects and aggregates data samples and storage information from all tables in schema
	 * @param schema storage scheme of analyzed tables (default all schemas)
	 * @param percent percentage of data to read from disk
	 * @return 0 — Success
	 */
	Integer analyzeStatistics(String schema = null, Integer percent = null) {
		tryConnect()
		if (percent != null && (percent <= 0 || percent > 100))
			throw new ExceptionGETL("Invalid percentage value \"$percent\"!")

		def qry = new QueryDataset()
		qry.tap {
			useConnection this
			query = "SELECT ANALYZE_STATISTICS('{%schema%}'{, %percent%}) AS res"
			queryParams.schema = schema
			queryParams.percent = percent
		}

		return qry.rows()[0].res as Integer
	}

	/**
	 * Runs Workload Analyzer, a utility that analyzes system information held in system tables
	 * @param scope specifies the catalog objects to analyze, as follows
	 * @param saveData specifies whether to save returned values from ANALYZE_WORKLOAD
	 * @return Returns aggregated tuning recommendations from TUNING_RECOMMENDATIONS
	 */
	List<Map<String, Object>> analyzeWorkload(String scope = null, Boolean saveData = null) {
		doAnalyzeWorkload(scope, null, saveData)
	}

	/**
	 * Runs Workload Analyzer, a utility that analyzes system information held in system tables
	 * @param saveData specifies whether to save returned values from ANALYZE_WORKLOAD
	 * @return Returns aggregated tuning recommendations from TUNING_RECOMMENDATIONS
	 */
	List<Map<String, Object>> analyzeWorkload(Boolean saveData) {
		doAnalyzeWorkload(null, null, saveData)
	}

	/**
	 * Runs Workload Analyzer, a utility that analyzes system information held in system tables
	 * @param scope specifies the catalog objects to analyze, as follows
	 * @param specifiedTime specifies the start time for the analysis time span, which continues up to the current system status, inclusive
	 * @return Returns aggregated tuning recommendations from TUNING_RECOMMENDATIONS
	 */
	List<Map<String, Object>> analyzeWorkload(String scope, Date specifiedTime) {
		doAnalyzeWorkload(scope, specifiedTime, false)
	}

	/**
	 * Runs Workload Analyzer, a utility that analyzes system information held in system tables
	 * @param specifiedTime specifies the start time for the analysis time span, which continues up to the current system status, inclusive
	 * @return Returns aggregated tuning recommendations from TUNING_RECOMMENDATIONS
	 */
	List<Map<String, Object>> analyzeWorkload(Date specifiedTime) {
		doAnalyzeWorkload(null, specifiedTime, false)
	}

	/**
	 * Runs Workload Analyzer, a utility that analyzes system information held in system tables
	 * @param scope specifies the catalog objects to analyze, as follows
	 * @param specifiedTime specifies the start time for the analysis time span, which continues up to the current system status, inclusive
	 * @param saveData specifies whether to save returned values from ANALYZE_WORKLOAD
	 * @return Returns aggregated tuning recommendations from TUNING_RECOMMENDATIONS
	 */
	private List<Map<String, Object>> doAnalyzeWorkload(String scope, Date specifiedTime, Boolean saveData) {
		tryConnect()

		def qry = new QueryDataset()
		qry.tap {
			useConnection this
			if (specifiedTime != null) {
				query = "SELECT ANALYZE_WORKLOAD('{%scope%}', '{time}'::timestamp)"
				queryParams.time = specifiedTime
			}
			else {
				query = "SELECT ANALYZE_WORKLOAD('{%scope%}', {save})"
				queryParams.save = BoolUtils.IsValue(saveData)
			}
			queryParams.scope = scope
		}

		return qry.rows()
	}

	/**
	 * Runs Workload Analyzer and recommendations issued
	 * @param analyzeRows rows from system table v_monitor.tuning_recommendations or the result of calling function "analyzeWorkload"
	 * @return count of recommendations processed
	 */
	Integer processWorkload(List<Map<String, Object>> analyzeRows) {
		def tempTables = new QueryDataset(connection: this).with {
			query = '''
SELECT Lower(table_schema || '.' || table_name) AS name 
FROM tables 
WHERE is_temp_table 
ORDER BY name
'''
			return rows().collect { row -> row.name }
		} as List<String>

		def res = 0
		analyzeRows.each { row ->
			if (row.tuning_command != null && (row.tuning_command as String).length() > 0) {
				if ((row.tuning_description as String).substring(0, 18).toLowerCase() == 'analyze statistics') {
					def t = (row.tuning_parameter as String).toLowerCase()
					if (t.matches(".*[.].*[.].*"))
						t = t.substring(0, t.lastIndexOf("."))

					if (!(t in tempTables)) {
						def ptw = new ProcessTime(dslCreator: dslCreator, name: row.tuning_description, debug: true)
						def queryStat = (row.tuning_command as String).replace('"', '')
						try {
							executeCommand(command: queryStat)
							ptw.finish()
							logger.info("Rebuild statistics for $t complete")
							res++
						}
						catch (Exception e) {
							logger.severe("Found error for $t: ${e.message}")
						}
					}
				} else if ((row.tuning_parameter?:'' == '' || !((row.tuning_parameter as String).toLowerCase() in tempTables))) {
					def ptw = new ProcessTime(dslCreator: dslCreator, name: row.tuning_description, debug: true)
					try {
						executeCommand(command: row.tuning_command)
						ptw.finish()
						logger.info("Complete ${row.tuning_description}")
						res++
					}
					catch (Exception e) {
						logger.severe("${row.tuning_description}: ${e.message}")
					}
				}
			}
		}

		return res
	}

	@Override
	protected Class<TableDataset> getTableClass() { VerticaTable }

	/**
	 * Return the number of cluster nodes
	 * @return number of nodes
	 */
	Integer countNodes() {
		new QueryDataset(connection: this, query: 'SELECT Count(*) AS count_nodes FROM v_catalog.nodes').rows()[0].count_nodes as Integer
	}
}