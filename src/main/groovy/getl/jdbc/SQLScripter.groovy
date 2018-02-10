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

package getl.jdbc

import getl.data.Field
import getl.exception.ExceptionGETL
import getl.exception.ExceptionSQLScripter
import getl.utils.*
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Statement
import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 * SQL script manager class
 * @author Alexsey Konstantinov
 *
 */
public class SQLScripter {
	/** 
	 * Type of script command 
	 */
	public enum TypeCommand {
		UNKNOWN, UPDATE, SELECT, SET, ECHO, FOR, IF, ERROR, EXIT, LOAD_POINT, SAVE_POINT, BLOCK
	}
	
	/** 
	 * Script variables
	 */
	public final Map<String, Object> vars = [:]
	public void setVars(Map<String, Object> value) { 
		vars.clear()
		vars.putAll(value)
	}
	
	/** 
	 * Current type script command 
	 */
	private TypeCommand typeSql = TypeCommand.UNKNOWN

	/*** 
	 * JDBC connection 
	 */
	private JDBCConnection connection
	public JDBCConnection getConnection() { connection }
	public void setConnection(JDBCConnection value) { connection = value }
	
	/*
	 * Connection for point manager
	 */
	private JDBCConnection pointConnection
	public JDBCConnection getPointConnection () { pointConnection }
	public void setPointConnection(JDBCConnection value) {
		pointConnection = value
	}
	
	/** 
	 * Count proccessed rows 
	 */
	public long rowCount = 0
	
	/** 
	 * Script 
	 */
	private String script
	public String getScript() { script }
	public void setScript(String value) { script = (value == null)?null:((value.trim().length() == 0)?null:value) }
	
	private java.util.logging.Level logEcho = java.util.logging.Level.FINE
	public String getLogEcho () {  logEcho.toString() }
	public void setLogEcho (String level) {  logEcho = Logs.StrToLevel(level) }
	
	/** 
	 * Load script from file
	 * @param filename
	 * @param charset
	 */
	public void loadFile (String filename, String charset) {
		setScript(new File(filename).getText(charset))
	}

	/** 
	 * SQL generated script 
	 */
	private String sql
	public String getSql() {
		sql
	}
	
	/** 
	 * Current script label variable 
	 */
	private String scriptLabel

	/** 
	 * Compile script to commands
	 * @param script 
	 */
	private void prepareSql(String script) {
		if (script == null) 
			throw new ExceptionGETL("SQLScripter: need script in prepareSql method")
			
		List varNames = []
		vars.keySet().toArray().each { varNames << it }
		
		Pattern p
		Matcher m
		
		// Compile vars
		p = Pattern.compile('(\\{\\w+\\})')
		m = p.matcher(script)
		StringBuffer b = new StringBuffer()
		String vn
		while (m.find()) {
			vn = m.group()
			vn = vn.substring(1, vn.length() - 1).trim().toLowerCase()
			
			def varName = varNames.find { vn == it.toLowerCase() }
			
			if (varName == null) continue
			def val = vars.get(varName)
			String valStr
			if (val == null) {
				valStr = "null"
			}
			else if (val instanceof List) {
				StringBuffer sb = new StringBuffer()
				sb << "\n"
				val.each {
					sb << "				"
					sb << it.toString()
					sb << "\n"
				}
				valStr = sb.toString()
			}
			else {
				valStr = val.toString()
			}
			m.appendReplacement(b, valStr)
		}
		m.appendTail(b)
		sql = b.toString().trim()
		scriptLabel = null
		
		if (sql.matches("(?is)set(\\s|\\t|\\n).*")) {
			sql = sql.substring(4).trim()
			typeSql = TypeCommand.SET
		} else if (sql.matches("(?is)echo(\\s|\\t).*")) {
			sql = sql.substring(5).trim()
			typeSql = TypeCommand.ECHO
		} else if (sql.matches("(?is)for(\\s|\\n|\\t)+select(\\s|\\n|\\t).*") || sql.matches("(?is)for(\\s|\\n|\\t)+with(\\s|\\n|\\t).*")) {
			sql = sql.substring(4).trim()
			typeSql = TypeCommand.FOR
		} else if (sql.matches("(?is)if(\\s|\\n|\\t).*")) {
			sql = "SELECT 1 AS result WHERE " + sql.substring(3).trim()
			typeSql = TypeCommand.IF
		} else if (sql.matches("(?is)error(\\s|\\t).*")) {
			sql = sql.substring(6).trim()
			typeSql = TypeCommand.ERROR
		} else if (sql.matches("(?is)exit")) {
			typeSql = TypeCommand.EXIT
		} else if (sql.matches("(?is)load_point(\\s|\\t)+.*")) {
			typeSql = TypeCommand.LOAD_POINT
		} else if (sql.matches("(?is)save_point(\\s|\\t)+.*")) {
			typeSql = TypeCommand.SAVE_POINT
		} else if (sql.matches("(?is)begin(\\s|\\t)+block(\\s|\\t)*")) {
			typeSql = TypeCommand.BLOCK
		} else {
			if (sql.matches("(?is)[/][*][:].*[*][/].*")) {
				int ic = sql.indexOf("*/")
				scriptLabel = sql.substring(2, ic).trim().substring(1).trim().toLowerCase()
				sql = sql.substring(ic + 2).trim()
			}
			if (sql.matches("(?is)SELECT(\\s|\\n|\\t)+.*") ||
                    sql.matches("(?is)WITH(\\s|\\n|\\t)+.*")) {
                typeSql = TypeCommand.SELECT
            } else {
                typeSql = TypeCommand.UPDATE
            }
		}
	}
	
	private void doLoadPoint (List<String> st, int i) {
		def m = sql =~ "(?is)load_point(\\s|\\t)+([a-z0-9_.]+)(\\s|\\t)+to(\\s|\\t)+([a-z0-9_]+)(\\s|\\t)+with(\\s|\\t)+(insert|merge)(\\s|\\t)*"
		if (m.size() == 0) throw new ExceptionGETL("Uncorrect syntax for operator LOAD_POINT: \"$sql\"")
		//noinspection GroovyAssignabilityCheck
		def point = m[0][2] as String
		//noinspection GroovyAssignabilityCheck
		def varName = m[0][5] as String
		
		def pointList = point.split('[.]').toList()
		while (pointList.size() < 4) pointList.add(0, null)
		def dbName = pointList[0]
		def schemaName = pointList[1]
		def tableName = pointList[2]
		def pointName = pointList[3]
		//noinspection GroovyAssignabilityCheck
		def methodName = m[0][8] as String
		
		if (tableName == null) throw new ExceptionGETL("SQLScripter: need table name for LOAD_POINT operator")
		if (pointName == null) throw new ExceptionGETL("SQLScripter: need pointer name for LOAD_POINT operator")
		
		def pm = new SavePointManager(connection: pointConnection?:connection, tableName: tableName, saveMethod: methodName)
		if (dbName != null) pm.dbName = dbName
		if (schemaName != null) pm.schemaName = schemaName
		
		if (pm.isExists()) {
			def res = pm.lastValue(pointName)
			def value = (res.type == null)?null:res.value
			vars.put(varName, value)
		}
		else {
			vars.put(varName, null)
		}
	}
	
	@groovy.transform.Synchronized
	private void doSavePoint (List<String> st, int i) {
		def m = sql =~ "(?is)save_point(\\s|\\t)+([a-z0-9_.]+)(\\s|\\t)+from(\\s|\\t)+([a-z0-9_]+)(\\s|\\t)+with(\\s|\\t)+(insert|merge)(\\s|\\t)*"
        if (m.size() == 0) throw new ExceptionGETL("Uncorrect syntax for operator SAVE_POINT: \"$sql\"")
		//noinspection GroovyAssignabilityCheck
		def point = m[0][2] as String
		//noinspection GroovyAssignabilityCheck
		def varName = m[0][5] as String
		def value = vars.get(varName)
		if (value == null) throw new ExceptionGETL("SQLScripter: variable \"$varName\" has empty value for SAVE_POINT operator")
		
		def pointList = point.split('[.]').toList()
		while (pointList.size() < 4) pointList.add(0, null)
		def dbName = pointList[0]
		def schemaName = pointList[1]
		def tableName = pointList[2]
		def pointName = pointList[3]
		//noinspection GroovyAssignabilityCheck
		def methodName = m[0][8] as String
		
		if (tableName == null) throw new ExceptionGETL("SQLScripter: need table name for SAVE_POINT operator")
		if (pointName == null) throw new ExceptionGETL("SQLScripter: need pointer name for SAVE_POINT operator")
		
		def pm = new SavePointManager(connection: pointConnection?:connection, tableName: tableName, saveMethod: methodName)
		if (dbName != null) pm.dbName = dbName
		if (schemaName != null) pm.schemaName = schemaName
		
		if (!pm.exists) pm.create(false)
		pm.saveValue(pointName, value)
	}
	
	/*** 
	 * Do update command 
	 * @param s
	 * @param rc
	 * @param st
	 * @param i
	 */
	private void doUpdate(List<String> st, int i) {
		long rc = connection.executeCommand(command: sql)
		rowCount += rc
		if (scriptLabel != null) {
			vars.put(scriptLabel, rc)
		}
	}
	
	/**
	 * Do select command
	 * @param st
	 * @param i
	 */
	private void doSelect(List<String> st, int i) {
		//println "Select query: ${sql}"
		QueryDataset ds = new QueryDataset(connection: connection, query: sql) 
		def rows = ds.rows()
		//rowCount += rows.size()
		if (scriptLabel != null) {
			vars.put(scriptLabel, rows)
		}
	}
	
	/*** 
	 * Do setting variable command
	 * @param s
	 * @param rc
	 * @param st
	 * @param i
	 */
	private void doSetVar(List<String> st, int i)  {
		QueryDataset query = new QueryDataset(connection: connection, query: sql)
		query.eachRow(limit: 1) { row ->
			query.field.each { Field f ->
				def fieldName = f.name.toLowerCase()
                def fieldValue = row.get(fieldName)
                if (fieldValue instanceof Date) fieldValue = new java.sql.Timestamp((fieldValue as Date).time)
				vars.put(fieldName, fieldValue)
			}
		}
	}
	
	/*** 
	 * Do each row command
	 * @param s
	 * @param rc
	 * @param st
	 * @param i
	 */
	private int doFor(List<String> st, int i) {
		int fe = -1
		int fc = 1
		StringBuffer b = new StringBuffer()
		for (int fs = i + 1; fs < st.size(); fs++) {
			String c = st[fs] 
			if (c.matches("(?is)for(\\s|\\n|\\t)+select(\\s|\\n|\\t).*")) {
				fc++
			} else if (c.matches("(?is)end(\\s|\\n|\\t)+for")) {
				fc--
				if (fc == 0) {
					fe = fs
					break
				}
			}
			b.append(c + ";\n")
		}
		if (fe == -1) throw new ExceptionGETL("SQLScripter: can not find END FOR construction")
		
		QueryDataset query = new QueryDataset(connection: connection, query: sql)
		List<Map> rows = []
		query.eachRow { Map row -> rows << row }
		
		SQLScripter ns = new SQLScripter(connection: connection, script: b.toString(), logEcho: logEcho, vars: vars)
		boolean isExit = false
		rows.each { row ->
			if (isExit) return
			
			query.field.each { Field f ->
                def fieldName = f.name.toLowerCase()
                def fieldValue = row.get(fieldName)
                if (fieldValue instanceof Date) fieldValue = new java.sql.Timestamp((fieldValue as Date).time)
                ns.vars.put(fieldName, fieldValue)
//				ns.vars."${f.name.toLowerCase()}" = row."${f.name.toLowerCase()}"
			}
			try {
				ns.runSql()
				if (ns.isRequiredExit()) {
					isExit = true
					requiredExit = true
				}
			}
			finally {
				sql = ns.getSql()
				rowCount += ns.rowCount
			}
		}
		
		fe
	}
	
	/*** 
	 * Do if command
	 * @param s
	 * @param rc
	 * @param st
	 * @param i
	 */
	private int doIf(List<String> st, int i) {
		int fe = -1
		int fc = 1
		StringBuffer b = new StringBuffer()
		for (int fs = i + 1; fs < st.size(); fs++) {
			if (st[fs].matches("(?is)if(\\s|\\n|\\t)+.*")) {
				fc++
			} else if (st[fs].matches("(?is)end(\\s|\\n|\\t)+if")) {
				fc--
				if (fc == 0) {
					fe = fs
					break
				}
			}
			b.append(st[fs] + ";\n")
		}
		if (fe == -1) throw new ExceptionGETL("SQLScripter: can not find END IF construction")
		
		QueryDataset query = new QueryDataset(connection: connection, query: sql)
		def rows = query.rows(limit: 1)  
		if (rows.isEmpty()) {
			return fe
		} 
		
		SQLScripter ns = new SQLScripter(connection: connection, script: b.toString(), logEcho: logEcho, vars: vars)
		/*ns.setScript(b.toString())
		ns.connection = this.connection
		ns.vars.putAll(vars)*/
		try {
			ns.runSql()
			if (ns.isRequiredExit()) {
				requiredExit = true
			}
		}
		finally {
			sql = ns.getSql()
			rowCount += ns.rowCount
		}
		return fe
	}
	
	private int doBlock(List<String> st, int i) {
		int fe = -1
		int fc = 1
		StringBuffer b = new StringBuffer()
		for (int fs = i + 1; fs < st.size(); fs++) {
			if (st[fs].matches('(?is)begin(\\s|\\t)+block(\\s|\\t)*')) {
				fc++
			}
			else if (st[fs].matches('(?is)end(\\s|\\t)+block(\\s|\\t)*')) {
				fc--
				if (fc == 0) {
					fe = fs
					break
				}
			}
			else if (st[fs].matches('(?is)end(\\s|\\t)+block(\\s|\\t)+(.+)')) {
				fc--
				if (fc == 0) {
					fe = fs
					def pattern = '(?is)end(\\s|\\t)+block(\\s|\\t)+(.+)'
					def m = st[fs] =~ pattern
					//noinspection GroovyAssignabilityCheck
					def symbol = m[0][3] as String
					b.append(symbol)
					break
				}
			}
			b.append(st[fs].replace('\r', '') + ";\n")
		}
		if (fe == -1) throw new ExceptionGETL("SQLScripter: can not find END BLOCK construction")

		sql = StringUtils.EvalMacroString(b.toString(), vars)
		connection.executeCommand(command: sql)
		
		return fe
	}
	
	private boolean requiredExit
	public boolean isRequiredExit() { requiredExit }
	
	/** 
	 * Run script as SQL
	 */ 
	public void runSql () {
		requiredExit = false
		def st = BatchSQL2List(script, ";")
		rowCount = 0
		for (int i = 0; i < st.size(); i++) {
			if (requiredExit) return
			prepareSql(st[i])
			
			switch (typeSql) {
				case TypeCommand.UPDATE:
					doUpdate(st, i)
					break
				case TypeCommand.SELECT:
					doSelect(st, i)
					break
				case TypeCommand.SET:
					doSetVar(st, i)
					break
				case TypeCommand.ECHO:
					Logs.Write(logEcho, sql)
					break
				case TypeCommand.FOR:
					i = doFor(st, i)
					break
				case TypeCommand.IF:
					i = doIf(st, i)
					break
				case TypeCommand.BLOCK:
					i = doBlock(st, i)
					break
				case TypeCommand.ERROR:
					throw new ExceptionSQLScripter("SQLScripter: found error $sql")
					break
				case TypeCommand.EXIT:
					requiredExit = true
					break
				case TypeCommand.LOAD_POINT:
					doLoadPoint(st, i)
					break
				case TypeCommand.SAVE_POINT:
					doSavePoint(st, i)
					break
				default:
					throw new ExceptionGETL("SQLScripter: unknown type command \"${typeSql}\"")
			}
		}
	}
	
	/** 
	 * Convert batch script to SQL command list
	 * @param sql
	 * @param delim
	 * @return
	 */
	public static List<String> BatchSQL2List (String sql, String delim) {
		if (sql == null) throw new ExceptionGETL("SQLScripter: required sql for BatchSQL2List method")
		
		// Delete multi comment
		/*StringBuffer b = new StringBuffer()
		int cur = 0
		int start = sql.indexOf("*//*")
		int finish //= -1
		while (start >= 0) {
			if (cur < start) b.append(sql.substring(cur, start))
			finish = sql.indexOf("*//*", start)
			String comment = sql.substring(start + 2, finish).trim()
			if ("+".equals(comment.substring(0, 1)) || ":".equals(comment.substring(0, 1)))
				b.append("*//*" + comment + "*//*")
			cur = finish + 2
			start = sql.indexOf("*//*", cur)
		}
		if (cur < sql.length()) b.append(sql.substring(cur))
		sql = b.toString()
		
		Pattern p
		Matcher m
		*/
		
		// Delete single comment
        def sb = new StringBuilder()
        sql.eachLine { String line ->
            def i = line.indexOf('--')
            if (i == -1) {
                sb << line
            }
            else {
                sb << line.substring(0, i)
            }
            sb << '\n'
        }
		sql = sb.toString()
		
		List<String> res = sql.split('\n')
		for (int i = 0; i < res.size(); i++) {
			String s = res[i].trim()
			if (s.matches("(?is)echo(\\s|\\t).*") || s.matches("(?is)error(\\s|\\t).*")) {
				if (s.substring(s.length() - 1) != ';') res[i] = res[i] + ';'
			}
		}
		String prepare = res.join('\n')
		res = prepare.split(delim)
		for (int i = 0; i < res.size(); i++) { res[i] = res[i].trim() }

		return res
	}

}
