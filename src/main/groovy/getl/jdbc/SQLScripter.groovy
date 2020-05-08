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

package getl.jdbc

import getl.data.Connection
import getl.data.Field
import getl.data.sub.WithConnection
import getl.exception.ExceptionGETL
import getl.exception.ExceptionSQLScripter
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.utils.*
import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 * SQL script manager class
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings("UnnecessaryQualifiedReference")
class SQLScripter implements WithConnection, Cloneable, GetlRepository {
	private String _dslNameObject
	@Override
	String getDslNameObject() { _dslNameObject }
	@Override
	void setDslNameObject(String value) { _dslNameObject = value }

	private Getl _dslCreator
	@Override
	Getl getDslCreator() { _dslCreator }
	@Override
	void setDslCreator(Getl value) { _dslCreator = value }

	@Override
	void dslCleanProps() {
		_dslNameObject = null
		_dslCreator = null
	}

	static enum TypeCommand {
		UNKNOWN, UPDATE, SELECT, SET, ECHO, FOR, IF, ERROR, EXIT, LOAD_POINT, SAVE_POINT, BLOCK
	}

	/** Local variables */
	final Map<String, Object> vars = [:] as Map<String, Object>
	/** Local variables */
	Map<String, Object> getVars() { vars }
	/** Local variables */
	void setVars(Map<String, Object> value) {
		vars.clear()
		if (value != null) vars.putAll(value)
	}

	/** External variables */
	public Map<String, Object> extVars
	/** All variables */
	Map<String, Object> getAllVars() { (extVars != null)?((vars + extVars) as Map<String, Object>):vars }
	
	/** 
	 * Current type script command 
	 */
	private TypeCommand typeSql = TypeCommand.UNKNOWN

	/***  JDBC connection */
	private JDBCConnection connection
	/***  JDBC connection */
	Connection getConnection() { connection }
	/***  JDBC connection */
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JDBCConnection))
			throw new ExceptionGETL('The SQLScripter only supports jdbc connections!')
		connection = value as JDBCConnection
	}
	/** Use specified JDBC connection */
	JDBCConnection useConnection(JDBCConnection value) {
		setConnection(value)
		return value
	}
	
	/* Connection for point manager */
	private JDBCConnection pointConnection
	/* Connection for point manager */
	JDBCConnection getPointConnection () { pointConnection }
	/* Connection for point manager */
	void setPointConnection(JDBCConnection value) { pointConnection = value }
	
	/**  Count proccessed rows */
	long rowCount = 0
	/**  Count proccessed rows */
	long getRowCount() { rowCount }

	/** Script to execute */
	private String script
	/** Script to execute */
	String getScript() { script }
	/** Script to execute */
	void setScript(String value) { script = (value == null)?null:((value.trim().length() == 0)?null:value) }
	
	private java.util.logging.Level logEcho = java.util.logging.Level.FINE
	/** Echo command output level */
	String getLogEcho () {  logEcho.toString() }
	/** Echo command output level */
	void setLogEcho(String level) {  logEcho = Logs.StrToLevel(level) }
	
	/** 
	 * Load script from file
	 * @param fileName file name sql batch file
	 * @param codePage file use specified encoding page (default utf-8)
	 */
	void loadFile (String fileName, String codePage = 'utf-8') {
		setScript(new File(FileUtils.ResourceFileName(fileName)).getText(codePage))
	}

	/**
	 * Load script from file in class path or resource directory
	 * @param fileName file name in resource catalog
	 * @param codePage file use specified encoding page (default utf-8)
	 * @param otherPath the string value or list of string values as search paths if file is not found in the resource directory
	 */
	void loadResource(String fileName, def otherPath = null, String codePage = 'utf-8') {
		setScript(FileUtils.FileFromResources(fileName, otherPath).getText(codePage?:'utf-8'))
	}

	/**
	 * SQL generated script 
	 */
	private String sql

	String getSql() {
		return sql
	}
	
	/** 
	 * Current script label variable 
	 */
	private String scriptLabel

	/** 
	 * Compile script to commands
	 * @param script 
	 */
	private boolean prepareSql(String script) {
		if (script == null) 
			throw new ExceptionGETL("SQLScripter: need script in prepareSql method")
			
		def varNames = [] as List<String>
		def locVars = allVars
		locVars.keySet().toArray().each { varNames << (it as String) }
		
		Pattern p
		Matcher m
		
		// Compile vars
		p = Pattern.compile('(?i)([{][a-z0-9._-]+[}])')
		m = p.matcher(script)
		def b = new StringBuffer()
		String vn
		while (m.find()) {
			vn = m.group()
			vn = vn.substring(1, vn.length() - 1).trim().toLowerCase()
			
			def varName = varNames.find { String s -> vn == s.toLowerCase() }
			
			if (varName == null) continue
			def val = locVars.get(varName)
			String valStr
			if (val == null) {
				valStr = "null"
			}
			else if (val instanceof List) {
				/*TODO: variable list???*/
				/*def sb = new StringBuffer()
				sb << "\n"
				val.each {
					sb << "				"
					sb << it.toString()
					sb << "\n"
				}
				valStr = sb.toString()*/
				valStr = (val as List).join(', ')
			}
			else {
				if (val instanceof Date)
					valStr = DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', val)
				else if (val instanceof String || val instanceof GString)
					valStr = val.toString().replace('$', '\\$')
				else
					valStr = val
			}
			m.appendReplacement(b, valStr)
		}
		m.appendTail(b)
		sql = b.toString().trim()
		scriptLabel = null

		def startCommand = StringUtils.DetectStartSQLCommand(sql)
		if (startCommand == -1) return false
		def cs = sql.substring(startCommand).trim()
		
		if (cs.matches("(?is)set\\s.*")) {
			sql = removeFirstOperator(sql, 'SET', startCommand)
			typeSql = TypeCommand.SET
		} else if (cs.matches("(?is)echo\\s.*")) {
			sql = StringUtils.RemoveSQLComments(sql)
			sql = removeFirstOperator(sql, 'ECHO', 0)
			typeSql = TypeCommand.ECHO
		} else if (cs.matches("(?is)for\\s+select\\s.*") || sql.matches("(?is)for\\s+with\\s.*")) {
			sql = removeFirstOperator(sql, 'FOR', startCommand)
			typeSql = TypeCommand.FOR
		} else if (cs.matches("(?is)if\\s.*")) {
			def from = ((connection.driver as JDBCDriver).sysDualTable != null)?"FROM ${(connection.driver as JDBCDriver).sysDualTable}":''
			sql = "SELECT 1 AS result $from WHERE " + removeFirstOperator(sql, 'IF', startCommand)
			typeSql = TypeCommand.IF
		} else if (cs.matches("(?is)error\\s.*")) {
			sql = StringUtils.RemoveSQLComments(sql)
			sql = removeFirstOperator(sql, 'ERROR', 0)
			typeSql = TypeCommand.ERROR
		} else if (cs.matches("(?is)exit")) {
			sql = StringUtils.RemoveSQLComments(sql)
			typeSql = TypeCommand.EXIT
		} else if (cs.matches("(?is)load_point\\s+.*")) {
			sql = StringUtils.RemoveSQLComments(sql)
			typeSql = TypeCommand.LOAD_POINT
		} else if (cs.matches("(?is)save_point\\s+.*")) {
			sql = StringUtils.RemoveSQLComments(sql)
			typeSql = TypeCommand.SAVE_POINT
		} else if (cs.matches("(?is)begin\\s+block\\s*")) {
			sql = cs
			typeSql = TypeCommand.BLOCK
		} else {
			sql = StringUtils.RemoveSQLCommentsWithoutHints(sql).trim()
			if (sql.matches("(?is)[/][*][:].*[*][/].*")) {
				int ic = sql.indexOf("*/")
				scriptLabel = sql.substring(2, ic).trim().substring(1).trim().toLowerCase()
				sql = sql.substring(ic + 2).trim()
			}
			if (cs.matches("(?is)SELECT\\s+.*") ||
                    cs.matches("(?is)WITH\\s+.*")) {
                typeSql = TypeCommand.SELECT
            } else {
                typeSql = TypeCommand.UPDATE
            }
		}

		return true
	}

	/**
	 * Remove first operator for SQL script
	 * @param sql script
	 * @param oper operator
	 * @param start start index in script
	 * @return script without first operator
	 */
	private String removeFirstOperator(String sql, String oper, Integer start) {
		def str = sql.substring(start)
		def find = "(?i)^(\\s*${StringUtils.EscapeJava(oper.toUpperCase())})"
		def pat = Pattern.compile(find)
		def mat = pat.matcher(str)
		if (!mat.find()) {
			Logs.Dump(null, getClass().name, 'sql', sql)
			throw new ExceptionGETL("Operator \"$oper\" not found in script!")
		}
		str = mat.replaceFirst('')

		if (start > 0) str = sql.substring(0, start) + str
		return str
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
		def value = allVars.get(varName)
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
		if (rc > 0) rowCount += rc
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
		def b = new StringBuffer()
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
		
		SQLScripter ns = new SQLScripter(connection: connection, script: b.toString(), logEcho: logEcho,
				vars: vars, extVars: extVars)
		boolean isExit = false
		rows.each { row ->
			if (isExit) return
			
			query.field.each { Field f ->
                def fieldName = f.name.toLowerCase()
                def fieldValue = row.get(fieldName)
                if (fieldValue instanceof Date) fieldValue = new java.sql.Timestamp((fieldValue as Date).time)
                ns.vars.put(fieldName, fieldValue)
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
		def b = new StringBuffer()
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
		
		SQLScripter ns = new SQLScripter(connection: connection, script: b.toString(), logEcho: logEcho,
				vars: vars, extVars: extVars)
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
		def b = new StringBuffer()
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
		if (fe == -1) throw new ExceptionGETL("SQLScripter: can not find END BLOCK construction!")

		sql = StringUtils.EvalMacroString(b.toString(), allVars)
		connection.executeCommand(command: sql)
		
		return fe
	}
	
	private boolean requiredExit

	boolean isRequiredExit() { requiredExit }

	/**
	 * Run SQL script
	 * @param useParsing enable script command parsing
	 */
	void runSql(boolean useParsing = true) {
		if (connection == null)
			throw new ExceptionGETL('Not defined jdbc connection for work!')

		if (!useParsing) {
			sql = StringUtils.EvalMacroString(script, allVars)
			connection.executeCommand(command: sql)
			return
		}

		requiredExit = false
		def st = BatchSQL2List(script, ";")
		rowCount = 0
		for (int i = 0; i < st.size(); i++) {
			if (requiredExit) return
			if (!prepareSql(st[i])) continue
			
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
					Logs.Write(logEcho, sql.trim())
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
	 * Run the script for the specified file
	 * @param fileName file path (use the prefix "resource:/" to load from the resource file)
	 * @param codePage text encoding (default utf-8)
	 */
	void runFile (String fileName, String codePage = 'utf-8') {
		loadFile(fileName, codePage)
		runSql()
	}

	/**
	 * Run the script for the specified file
	 * @param useParsing enable script command parsing
	 * @param fileName file path (use the prefix "resource:/" to load from the resource file)
	 * @param codePage text encoding (default utf-8)
	 */
	void runFile (boolean useParsing, String fileName, String codePage = 'utf-8') {
		loadFile(fileName, codePage)
		runSql(useParsing)
	}

	/**
	 * Run SQL script
	 * @param sql script to execute
	 */
	void exec(String sql) {
		script = sql
		runSql()
	}

	/**
	 * Run SQL script
	 * @param useParsing enable script command parsing
	 * @param sql script to execute
	 */
	void exec(boolean useParsing, String sql) {
		script = sql
		runSql(useParsing)
	}

	/** 
	 * Convert batch script to SQL command list
	 * @param sql
	 * @param delim
	 * @return
	 */
	static List<String> BatchSQL2List (String sql, String delim) {
		if (sql == null) throw new ExceptionGETL("\"sql\" parameter required!")

		List<String> res = sql.split('\n')
		for (int i = 0; i < res.size(); i++) {
			String s = res[i].trim()
			def l = s.length()
			if (l == 0) continue

			if (s.matches("(?is)echo(\\s|\\t).*") || s.matches("(?is)error(\\s|\\t).*")) {
				if (s.substring(s.length() - 1) != delim) res[i] = res[i] + delim
			}
			else if (l > 1 && s[s.length() - 1] == delim) {
				def f = s.lastIndexOf('--')
				if (f >= 0) {
					def q = 0
					for (int y = 0; y < f; y++) {
						if (s[y] == '\'') {
							if (q == 0)
								q++
							else
								q--
						}
					}
					if (q == 0)
						res[i] = s.substring(0, l - 1)
				}
			}
		}
		String prepare = res.join('\n')
		res = prepare.split(delim)
		for (int i = 0; i < res.size(); i++) { res[i] = res[i].trim() }

		return res
	}

	/**
	 * Clone scripter
	 * @param newConnection use specified connection (if null value using current connection)
	 * @param newPointConnection use specified point connection (if null value using current point connection)
	 * @return cloned object
	 */
	SQLScripter cloneSQLScripter(JDBCConnection newConnection = null, JDBCConnection newPointConnection = null) {
		if (newConnection == null) newConnection = this.connection
		if (newPointConnection == null) newPointConnection = this.pointConnection
		def className = this.getClass().name
		def res = Class.forName(className).newInstance() as SQLScripter
		if (newConnection != null) res.connection = newConnection
		if (newPointConnection != null) res.pointConnection = newPointConnection
		res.script = this.script
		res.logEcho = this.logEcho
		res.vars = CloneUtils.CloneMap(vars)

		return res
	}

	/**
	 * Clone scripter and its connections
	 * @return clone object
	 */
	SQLScripter cloneSQLScripterConnection() {
		def con = connection?.cloneConnection() as JDBCConnection
		def pointCon = pointConnection?.cloneConnection() as JDBCConnection
		return cloneSQLScripter(con, pointCon)
	}

	@Override
	Object clone() {
		return cloneSQLScripter()
	}

	Object cloneConnection() {
		return cloneSQLScripterConnection()
	}
}