package getl.h2

import getl.csv.*
import getl.data.Field
import getl.jdbc.*
import getl.proc.*
import getl.utils.*
import java.sql.SQLException

/**
 * Function for H2 use as stored procedures
 * <h1>Functions:</h1>
 * <ul>
 * <li>GETL_FormatDate(String Mask, Date date)
 * <li>GETL_CopyToCSV(String query, String fileName, String fieldDelimiter, Boolean escaped)  
 * <li>GETL_CopyToJDBC(String query, String driverClass, String connectURL,
 * 						String login, String password, String dbName, String schemaName, String tableName, Long batchSize)
 * <li>GETL_CopyFromJDBC(String query, String driverClass, String connectURL,
 * 						String login, String password, String schemaName, String tableName, Long batchSize)
 * <li>GETL_RunGroovyScript(String script)
 * <li>GETL_DeleteFile(String fileName)
 * </ul>
 * @author Alexsey Konstantinov
 */
class H2Functions {
	/**
	 * List of function
	 */
	static public Map<String, String> FUNCTIONS = ['GETL_FormatDate': 'getl.utils.DateUtils.FormatDate',
											'GETL_CopyToCSV': 'getl.h2.H2Functions.CopyToCSV',
											'GETL_CopyToJDBC': 'getl.h2.H2Functions.CopyToJDBC',
											'GETL_CopyFromJDBC': 'getl.h2.H2Functions.CopyFromJDBC',
											'GETL_RunGroovyScript': 'getl.h2.H2Functions.RunGroovyScript',
											'GETL_ExistsFile': 'getl.utils.FileUtils.ExistsFile',
											'GETL_DeleteDir': 'getl.utils.FileUtils.DeleteDir',
											'GETL_DeleteFile': 'getl.utils.FileUtils.DeleteFile']
	
	/**
	 * Register function
	 * @param con
	 * @param funcName
	 * @param funcClass
	 */
	static void RegisterFunction(H2Connection con, String funcName, String funcClass) {
		def sql = "CREATE ALIAS IF NOT EXISTS $funcName FOR \"$funcClass\";"
		con.executeCommand(command: sql)
	} 
	
	/**
	 * Init connection environment
	 * @param con
	 */
	static void InitConnection(H2Connection con) { }
	
	/**
	 * Register function from list
	 * @param con
	 */
	static void RegisterFunctions (H2Connection con) {
		InitConnection(con)
		FUNCTIONS.each { funcName, funcClass ->
			RegisterFunction(con, funcName, funcClass)
		}
	}

	/**
	 * Unregister function	
	 * @param con
	 * @param funcName
	 */
	static void UnregisterFunction (H2Connection con, String funcName) {
		def sql = "DROP ALIAS IF EXISTS $funcName;"
		con.executeCommand(command: sql)
	}
	
	/**
	 * Unregister functions from list
	 * @param con
	 */
	static void UnregisterFunctions (H2Connection con) {
		FUNCTIONS.each { funcName, funcClass ->
			UnregisterFunction(con, funcName)
		}
	}
	
	/**
	 * Copy query result to CSV file
	 * @param sqlConn
	 * @param query
	 * @param fileName
	 * @param fieldDelimiter
	 * @param escaped
	 * @return
	 */
	@SuppressWarnings("UnnecessaryQualifiedReference")
	static Long CopyToCSV(java.sql.Connection sqlConn, String query, String fileName, String fieldDelimiter, Boolean escaped) {
		H2Connection con = new H2Connection(javaConnection: sqlConn)
		QueryDataset ds = new QueryDataset(connection: con, query: query)
		con.connected = true
		
		String pathFile = FileUtils.PathFromFile(fileName)
		fileName = FileUtils.FileName(fileName)
		
		CSVConnection path = new CSVConnection(path: pathFile, createPath: true)
		CSVDataset file = new CSVDataset(connection: path, fileName: fileName, fieldDelimiter: fieldDelimiter, escaped: escaped)

		Long count
		try { 
			count = new Flow().copy(source: ds, dest: file, inheritFields: true)
		}
		catch (Exception e) {
			throw e
		}
		
		return count
	}
	
	/**
	 * Copy query result to table with other JDBC connection
	 * @param sqlConn
	 * @param query
	 * @param driverClass
	 * @param connectURL
	 * @param login
	 * @param password
	 * @param dbName
	 * @param schemaName
	 * @param tableName
	 * @param batchSize
	 * @return
	 */
	@SuppressWarnings("UnnecessaryQualifiedReference")
	static Long CopyToJDBC(java.sql.Connection sqlConn, String query, String driverClass, String connectURL,
									String login, String password, String dbName, String schemaName, String tableName, Long batchSize) {
		H2Connection conSource = new H2Connection(javaConnection: sqlConn)
		QueryDataset ds = new QueryDataset(connection: conSource, query: query)
		conSource.connected = true
		
		JDBCConnection conDest = JDBCConnection.CreateConnection(connection: driverClass, connectURL: connectURL, login: login, password: password) as JDBCConnection
		conDest.connected = true
		def table = new TableDataset(connection: conDest, dbName: dbName, schemaName: schemaName, tableName: tableName)
		def count = 0L
		try {
			count = new Flow().copy(source: ds, dest: table, dest_batchSize: batchSize)
		}
//		catch (Exception e) {
//			throw e
//		}
		finally {
			conDest.connected = false
		}
		
		return count
	}
									
	/**
	 * Copy to table from query result with other JDBC connection
	 * @param sqlConn
	 * @param query
	 * @param driverClass
	 * @param connectURL
	 * @param login
	 * @param password
	 * @param schemaName
	 * @param tableName
	 * @param batchSize
	 * @return
	 */
	@SuppressWarnings("UnnecessaryQualifiedReference")
	static Long CopyFromJDBC(java.sql.Connection sqlConn, String query, String driverClass, String connectURL,
										String login, String password, String schemaName, String tableName, Long batchSize) {
		JDBCConnection conSource = JDBCConnection.CreateConnection(connection: driverClass, connectURL: connectURL, login: login, password: password) as JDBCConnection
		conSource.connected = true
		
		H2Connection conDest = new H2Connection(javaConnection: sqlConn)
		conDest.connected = true
		
		QueryDataset ds = new QueryDataset(connection: conSource, query: query)
		
		def table = new TableDataset(connection: conDest, schemaName: schemaName, tableName: tableName)
		def count = 0L
		try {
			count = new Flow().copy(source: ds, dest: table, dest_batchSize: batchSize)
		}
//		catch (Exception e) {
//			throw e
//		}
		finally {
			conSource.connected = false
		}
		
		return count
	}
										
	/**
	 * Run groovy script
	 * @param sqlConn
	 * @param script
	 * @return
	 */
	@SuppressWarnings("UnnecessaryQualifiedReference")
	static String RunGroovyScript(java.sql.Connection sqlConn, String script) {
		H2Connection con = new H2Connection(javaConnection: sqlConn)
		con.connected = true
		
		try {
			GenerationUtils.EvalGroovyScript(value: script, vars: [connection: con]).toString()
		}
		catch (Exception e) {
			throw e
		}
	}

	/**
	 * Run function as select and return result value of function  	
	 * @param con
	 * @param funcName
	 * @param params
	 * @return
	 */
	static def SelectFunction (H2Connection con, String funcName, Map params) {
		con.connected = true
		
		def paramName = []
		params.each { name, value -> 
			paramName << ":$name" 
		}
		def sql = "SELECT $funcName (${paramName.join(", ")}) AS res"
		QueryDataset query = new QueryDataset(connection: con, query: sql)
		def rows
		try {
			rows = query.rows(sqlParams: params)
		}
		catch (SQLException e) {
//			Logs.Dump(e, "SELECT FUNCTION", funcName, MapUtils.ToJson(params))
			throw e
		}
		if (rows.isEmpty()) return null
		
		return rows[0]."res"
	}
}
