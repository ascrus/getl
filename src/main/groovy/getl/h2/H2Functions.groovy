/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

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
	public static def FUNCTIONS = [ "GETL_FormatDate": "getl.utils.DateUtils.FormatDate",
									"GETL_CopyToCSV": "getl.h2.H2Functions.CopyToCSV",
									"GETL_CopyToJDBC": "getl.h2.H2Functions.CopyToJDBC",
									"GETL_CopyFromJDBC": "getl.h2.H2Functions.CopyFromJDBC",
									"GETL_RunGroovyScript": "getl.h2.H2Functions.RunGroovyScript",
									"GETL_ExistsFile": "getl.utils.FileUtils.ExistsFile",
									"GETL_DeleteDir": "getl.utils.FileUtils.DeleteDir",
									"GETL_DeleteFile": "getl.utils.FileUtils.DeleteFile" ]
	
	/**
	 * Register function
	 * @param con
	 * @param funcName
	 * @param funcClass
	 */
	public static void RegisterFunction(H2Connection con, String funcName, String funcClass) {
		def sql = "CREATE ALIAS IF NOT EXISTS $funcName FOR \"$funcClass\";"
		con.executeCommand(command: sql)
	} 
	
	/**
	 * Init connection environment
	 * @param con
	 */
	public static void InitConnection(H2Connection con) { }
	
	/**
	 * Register function from list
	 * @param con
	 */
	public static void RegisterFunctions (H2Connection con) {
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
	public static void UnregisterFunction (H2Connection con, String funcName) {
		def sql = "DROP ALIAS IF EXISTS $funcName;"
		con.executeCommand(command: sql)
	}
	
	/**
	 * Unregister functions from list
	 * @param con
	 */
	public static void UnregisterFunctions (H2Connection con) {
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
	public static long CopyToCSV(java.sql.Connection sqlConn, String query, String fileName, String fieldDelimiter, Boolean escaped) {
		H2Connection con = new H2Connection(javaConnection: sqlConn)
		QueryDataset ds = new QueryDataset(connection: con, query: query)
		con.connected = true
		
		String pathFile = FileUtils.PathFromFile(fileName)
		fileName = FileUtils.FileName(fileName)
		
		CSVConnection path = new CSVConnection(path: pathFile, createPath: true)
		CSVDataset file = new CSVDataset(connection: path, fileName: fileName, fieldDelimiter: fieldDelimiter, escaped: escaped)
		
		long count
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
	public static long CopyToJDBC(java.sql.Connection sqlConn, String query, String driverClass, String connectURL, 
									String login, String password, String dbName, String schemaName, String tableName, Long batchSize) {
		H2Connection conSource = new H2Connection(javaConnection: sqlConn)
		QueryDataset ds = new QueryDataset(connection: conSource, query: query)
		conSource.connected = true
		
		JDBCConnection conDest = JDBCConnection.CreateConnection(connection: driverClass, connectURL: connectURL, login: login, password: password)
		conDest.connected = true
		TableDataset table = new TableDataset(connection: conDest, dbName: dbName, schemaName: schemaName, tableName: tableName)
		long count
		try {
			count = new Flow().copy(source: ds, dest: table, dest_batchSize: batchSize)
		}
		catch (Exception e) {
			throw e
		}
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
	public static long CopyFromJDBC(java.sql.Connection sqlConn, String query, String driverClass, String connectURL,
										String login, String password, String schemaName, String tableName, Long batchSize) {
		JDBCConnection conSource = JDBCConnection.CreateConnection(connection: driverClass, connectURL: connectURL, login: login, password: password)
		conSource.connected = true
		
		H2Connection conDest = new H2Connection(javaConnection: sqlConn)
		conDest.connected = true
		
		QueryDataset ds = new QueryDataset(connection: conSource, query: query)
		
		TableDataset table = new TableDataset(connection: conDest, schemaName: schemaName, tableName: tableName)
		long count
		try {
			count = new Flow().copy(source: ds, dest: table, dest_batchSize: batchSize)
		}
		catch (Exception e) {
			throw e
		}
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
	public static String RunGroovyScript(java.sql.Connection sqlConn, String script) {
		H2Connection con = new H2Connection(javaConnection: sqlConn)
		con.connected = true
		
		try {
			GenerationUtils.EvalGroovyScript(script, [connection: con]).toString()
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
	public static def SelectFunction (H2Connection con, String funcName, Map params) {
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
			Logs.Dump(e, "SELECT FUNCTION", funcName, MapUtils.ToJson(params))
			throw e
		}
		if (rows.isEmpty()) return null
		
		rows[0]."res"
	}
}
