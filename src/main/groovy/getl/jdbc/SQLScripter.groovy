//file:noinspection unused
package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Field
import getl.data.sub.WithConnection
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.exception.ExceptionSQLScripter
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.lang.sub.GetlValidate
import getl.utils.*
import groovy.transform.Synchronized

import java.util.regex.Pattern

/**
 * SQL script manager class
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings("UnnecessaryQualifiedReference")
class SQLScripter implements WithConnection, GetlRepository {
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

	private Date _dslRegistrationTime
	@JsonIgnore
	@Override
	Date getDslRegistrationTime() { _dslRegistrationTime }
	@Override
	void setDslRegistrationTime(Date value) { _dslRegistrationTime = value }

	@Override
	void dslCleanProps() {
		_dslNameObject = null
		_dslCreator = null
		_dslRegistrationTime = null
	}

	/** Current logger*/
	@JsonIgnore
	Logs getLogger() { (dslCreator?.logging?.manager != null)?dslCreator.logging.manager:Logs.global }

	/** Local variables */
	private final Map<String, Object> vars = new HashMap<String, Object>()
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
	Map<String, Object> getAllVars() { (extVars != null)?((extVars + vars) as Map<String, Object>):vars }
	
	/***  Source JDBC connection */
	private JDBCConnection connection
	/***  Source connection */
	@JsonIgnore
	Connection getConnection() { connection }
	/***  Source connection */
	@Synchronized
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JDBCConnection))
			throw new ExceptionGETL('The SQLScripter only supports jdbc connections!')

		useConnection(value as JDBCConnection)
	}
	/** Use specified source JDBC connection */
	@Synchronized
	JDBCConnection useConnection(JDBCConnection value) {
		this.connection = value as JDBCConnection
		return value
	}

	/** Current source JDBC connection */
	@JsonIgnore
	JDBCConnection getCurrentJDBCConnection() { connection as JDBCConnection }

	/** The name of the connection in the repository */
	String getConnectionName() { connection?.dslNameObject }
	/** The name of the connection in the repository */
	@Synchronized
	void setConnectionName(String value) {
		if (value != null) {
			GetlValidate.IsRegister(this)
			def con = dslCreator.jdbcConnection(value)
			useConnection(con)
		}
		else
			useConnection(null)
	}
	
	/* Connection for point manager */
	private JDBCConnection pointConnection
	/* Connection for point manager */
	JDBCConnection getPointConnection () { pointConnection }
	/* Connection for point manager */
	void setPointConnection(JDBCConnection value) { pointConnection = value }
	
	/**  Count processed rows */
	private Long rowCount = 0L
	/**  Count processed rows */
	Long getRowCount() { rowCount }

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

	/** Print scripts to console */
	private Boolean debugMode = false
	/** Print scripts to console */
	Boolean getDebugMode() { debugMode }
	/** Print scripts to console */
	void setDebugMode(Boolean value) { debugMode = value }
	
	/** 
	 * Load script from file
	 * @param fileName file name sql batch file
	 * @param codePage file use specified encoding page (default utf-8)
	 */
	@Synchronized
	void loadFile(String fileName, String codePage = 'utf-8') {
		def fn = FileUtils.ResourceFileName(FileUtils.TransformFilePath(fileName, dslCreator), dslCreator)
		if (fn == null)
			throw new ExceptionGETL("Script file \"$fileName\" not found!")
		def file = new File(fn)
		if (!file.exists())
			throw new ExceptionGETL("Script file \"$fileName\" not found!")
		setScript(file.getText(codePage?:'utf-8'))
	}

	/**
	 * Load script from file in class path or resource directory
	 * @param fileName file name in resource catalog
	 * @param codePage file use specified encoding page (default utf-8)
	 * @param otherPath the string value or list of string values as search paths if file is not found in the resource directory
	 */
	@Synchronized
	void loadResource(String fileName, def otherPath = null, String codePage = 'utf-8') {
		def paths = [] as List<String>
		if (dslCreator != null)
			paths.addAll(dslCreator.repositoryStorageManager.otherResourcePaths)
		if (otherPath != null) {
			if (otherPath instanceof List)
				paths.addAll(otherPath as List)
			else
				paths.add(otherPath.toString())
		}

		def file = FileUtils.FileFromResources(fileName, paths)
		if (file == null)
			throw new ExceptionGETL("Script file \"$fileName\" not found in resource!")
		setScript(file.getText(codePage?:'utf-8'))
	}

	/** SQL generated script */
	private String lastSql

	/** SQL generated script */
	String getLastSql() { lastSql }
	/** SQL generated script */
	protected void setLastSql(String value) {
		lastSql = value
		if (debugMode) {
			def str = lastSql
			if (StringUtils.RightStr(value, 1) != ';')
				str += ';'
			println str
		}
	}

	/** Commands history */
	public final StringBuffer historyCommands = new StringBuffer()

	/** DDL history */
	public final StringBuffer historyDDL = new StringBuffer()

	/** DML history */
	public final StringBuffer historyDML = new StringBuffer()

	@SuppressWarnings('GroovyAssignabilityCheck')
	private void doLoadPoint(SQLParser parser) {
		if (dslCreator == null)
			throw new ExceptionGETL('The scripter does not have an "dslCreator" assigned!')

		setLastSql(StringUtils.EvalMacroString(parser.lexer.script, allVars).trim())
		def m = lastSql =~ /(?is)load_point\s+([^\s]+)\s+to\s+([^\s]+)/
		if (m.size() == 0)
			throw new ExceptionGETL("Uncorrect syntax for statement LOAD_POINT: \"$lastSql\"!")
		def point = m[0][1] as String
		def varName = m[0][2] as String

		def pm = dslCreator.historypoint(point)
		if (pm.isExists())
			vars.put(varName, pm.lastValue(true))
		else
			vars.put(varName, null)
	}

	@SuppressWarnings('GroovyAssignabilityCheck')
	private void doSavePoint(SQLParser parser) {
		if (dslCreator == null)
			throw new ExceptionGETL('The scripter does not have an "dslCreator" assigned!')

		setLastSql(StringUtils.EvalMacroString(parser.lexer.script, allVars).trim())
		def m = lastSql =~ /(?is)save_point\s+([^\s]+)\s+from\s+([^\s]+)/
        if (m.size() == 0)
			throw new ExceptionGETL("Uncorrect syntax for SAVE_POINT statement: \"$lastSql\"!")
		def point = m[0][1] as String
		def varName = m[0][2] as String
		def value = allVars.get(varName)
		if (value == null)
			throw new ExceptionGETL("SQLScripter: variable \"$varName\" has null value for SAVE_POINT statement!")

		def pm = dslCreator.historypoint(point)
		if (!pm.exists)
			pm.create(false)
		pm.saveValue(value)
	}

	private void doRunFile(SQLParser parser) {
		setLastSql(StringUtils.EvalMacroString(parser.lexer.script, allVars).trim())
		def posCmd = parser.lexer.findKeyWord('RUN_FILE')
		def posParam = parser.lexer.scriptBuild(start: posCmd + 1, ignoreComments: true).trim()
		if (posParam.length() == 0)
			throw new ExceptionGETL('Required file name from command RUN_FILE!')

		if (!FileUtils.IsResourceFileName(posParam, true) && !FileUtils.ExistsFile(posParam))
			throw new ExceptionGETL("Script file \"$posParam\" not found and cannot be running!")

		SQLScripter ns = new SQLScripter(connection: connection, logEcho: logEcho, debugMode: debugMode,
				vars: vars, extVars: extVars, dslCreator: dslCreator)
		try {
			ns.runFile(true, posParam)
			vars.putAll(ns.vars)
		}
		finally {
			lastSql = ns.lastSql
			historyCommands.append(ns.historyCommands)
			historyDDL.append(ns.historyDDL)
			historyDML.append(ns.historyDML)
			rowCount += ns.rowCount
		}
	}

	private void doSwitchLogin(SQLParser parser) {
		setLastSql(StringUtils.EvalMacroString(parser.lexer.script, allVars).trim())
		def posCmd = parser.lexer.findKeyWord('SWITCH_LOGIN')
		def posParam = parser.lexer.scriptBuild(start: posCmd + 1, ignoreComments: true).trim()
		if (posParam.length() == 0)
			throw new ExceptionGETL('Required login from command SWITCH_LOGIN!')
		connection.useLogin(posParam)
	}

	/*** Do update command */
	private void doDML(SQLParser parser) {
		setLastSql(StringUtils.EvalMacroString(parser.lexer.script, allVars).trim())
		def rc = connection.executeCommand(command: lastSql)
		if (rc > 0)
			rowCount += rc

		historyDML.append(lastSql)
		historyDML.append('\n')

		def scriptLabel = detectScriptVariable(parser)
		if (scriptLabel != null)
			vars.put(scriptLabel, rc)
	}

	/*** Do update command */
	private void doDDL(SQLParser parser) {
		setLastSql(StringUtils.EvalMacroString(parser.lexer.script, allVars).trim())
		connection.executeCommand(command: lastSql)

		historyDDL.append(lastSql)
		historyDDL.append('\n')
	}

	/*** Do other script */
	private void doOther(SQLParser parser) {
		if (!parser.scripts(true).isEmpty()) {
			setLastSql(StringUtils.EvalMacroString(parser.lexer.script, allVars).trim())
			connection.executeCommand(command: lastSql)
		}
	}
	
	/** Do select command */
	private void doSelect(SQLParser parser) {
		setLastSql(StringUtils.EvalMacroString(parser.lexer.script, allVars).trim())
		QueryDataset ds = new QueryDataset(connection: connection, query: lastSql)
		def rows = ds.rows()

		def scriptLabel = detectScriptVariable(parser)
		if (scriptLabel != null)
			vars.put(scriptLabel, rows)
	}

	private Pattern setOperatorPattern = Pattern.compile('(?is)[\\s\\n]*[@]?SET\\s+(.*)')
	
	/*** Do setting variable command */
	private void doSetVar(SQLParser parser) {
		def matcher = setOperatorPattern.matcher(parser.lexer.scriptBuild(ignoreComments: true))
		if (!matcher)
			throw new ExceptionGETL("Invalid SET statement: ${parser.lexer.script}")

		def setScript = matcher.group(1)
		setLastSql(StringUtils.EvalMacroString(setScript, allVars).trim())

		QueryDataset query = new QueryDataset(connection: connection, query: lastSql)
		def rows = query.rows(limit: 1)
		query.field.each { Field f ->
			def fieldName = f.name.toLowerCase()
			def fieldValue = (!rows.isEmpty())?rows[0].get(fieldName):null
			if (fieldValue != null && fieldValue instanceof Date)
				fieldValue = new java.sql.Timestamp((fieldValue as Date).time)

			vars.put(fieldName, fieldValue)
		}
	}
	
	/*** Execute the code for the query records in a loop */
	private void doFor(SQLParser parser) {
		def parseScript = parser.lexer.script

		def posHeader = parser.lexer.findFunction('FOR')
		if (posHeader == -1)
			throw new ExceptionGETL("Invalid FOR statement: $parseScript")
		def tokenHeader = parser.lexer.tokens[posHeader]
		def listHeader = tokenHeader.list as List<Map>
		if (listHeader.isEmpty())
			throw new ExceptionGETL("No query specified for FOR statement: $parseScript")

		def posDo = parser.lexer.findFunction('DO')
		if (posDo == -1)
			throw new ExceptionGETL("Invalid FOR statement: $parseScript")
		def tokenDo = parser.lexer.tokens[posDo]
		def listDo = tokenDo.list as List<Map>
		if (listDo.isEmpty())
			throw new ExceptionGETL("No script specified for FOR statement: $parseScript")

		def queryText = parseScript.substring(listHeader[0].first as Integer, (listHeader[listHeader.size() - 1].last as Integer) + 1)
		setLastSql(StringUtils.EvalMacroString(queryText, allVars).trim())
		def bodyText = parseScript.substring(listDo[0].first as Integer, (listDo[listDo.size() - 1].last as Integer) + 1).trim()

		QueryDataset query = new QueryDataset(connection: connection, query: lastSql)
		def rows = query.rows()

		SQLScripter ns = new SQLScripter(connection: connection, script: bodyText, logEcho: logEcho, debugMode: debugMode,
				vars: vars, extVars: extVars, dslCreator: dslCreator)
		for (row in rows) {
			query.field.each { Field f ->
                def fieldName = f.name.toLowerCase()
                def fieldValue = row.get(fieldName)
                if (fieldValue instanceof Date)
					fieldValue = new java.sql.Timestamp((fieldValue as Date).time)
                ns.vars.put(fieldName, fieldValue)
			}
			try {
				ns.runSql(true)
				if (ns.isRequiredExit()) {
					requiredExit = true
					break
				}
			}
			finally {
				lastSql = ns.lastSql
				historyCommands.append(ns.historyCommands)
				historyDDL.append(ns.historyDDL)
				historyDML.append(ns.historyDML)
				rowCount += ns.rowCount
			}
		}
	}
	
	/*** Execute command if condition is true */
	private void doIf(SQLParser parser) {
		def parseScript = parser.lexer.script

		def posHeader = parser.lexer.findFunction('IF')
		if (posHeader == -1)
			throw new ExceptionGETL("Invalid IF statement: $parseScript")
		def tokenHeader = parser.lexer.tokens[posHeader]
		def listHeader = tokenHeader.list as List<Map>
		if (listHeader.isEmpty())
			throw new ExceptionGETL("No query specified for IF statement: $parseScript")

		def posDo = parser.lexer.findFunction('DO')
		if (posDo == -1)
			throw new ExceptionGETL("Invalid IF statement: $parseScript")
		def tokenDo = parser.lexer.tokens[posDo]
		def listDo = tokenDo.list as List<Map>
		if (listDo.isEmpty())
			throw new ExceptionGETL("No script specified for IF statement: $parseScript")

		def queryText = parseScript.substring(listHeader[0].first as Integer, (listHeader[listHeader.size() - 1].last as Integer) + 1)
		def sc = 'SELECT 1'
		if (!connection.currentJDBCDriver.isSupport(Driver.Support.SELECT_WITHOUT_FROM)) {
			if (connection.currentJDBCDriver.sysDualTable == null)
				throw new ExceptionGETL("Can not generate IF statement for $connection connection (dual table not supported)!")

			sc += ' FROM ' + connection.currentJDBCDriver.sysDualTable
		}
		sc += ' WHERE (\n' + StringUtils.EvalMacroString(queryText.trim(), allVars) + '\n)'
		lastSql = setLastSql(sc)

		def bodyText = parseScript.substring(listDo[0].first as Integer, (listDo[listDo.size() - 1].last as Integer) + 1)

		QueryDataset query = new QueryDataset(connection: connection, query: sc)
		def rows = query.rows()

		if (!rows.isEmpty()) {
			SQLScripter ns = new SQLScripter(connection: connection, script: bodyText, logEcho: logEcho, debugMode: debugMode,
					vars: vars, extVars: extVars, dslCreator: dslCreator)
			try {
				ns.runSql(true)
				vars.putAll(ns.vars)
				if (ns.isRequiredExit())
					requiredExit = true
			}
			finally {
				lastSql = ns.lastSql
				historyCommands.append(ns.historyCommands)
				historyDDL.append(ns.historyDDL)
				historyDML.append(ns.historyDML)
				rowCount += ns.rowCount
			}
		}
	}

	/** Calc block commands without parsing */
	private void doCommand(SQLParser parser) {
		def parseScript = parser.lexer.script
		def posDo = parser.lexer.findFunction('COMMAND')
		if (posDo == -1)
			throw new ExceptionGETL("Invalid COMMAND statement: $parseScript")
		def tokenDo = parser.lexer.tokens[posDo]
		def listDo = tokenDo.list as List<Map>
		if (listDo.isEmpty())
			throw new ExceptionGETL("No script specified for COMMAND statement: $parseScript")

		def bodyText = parseScript.substring(listDo[0].first as Integer, (listDo[listDo.size() - 1].last as Integer) + 1)

		setLastSql(StringUtils.EvalMacroString(bodyText, allVars).trim())
		def rc = connection.executeCommand(command: lastSql)
		if (rc > 0)
			rowCount += rc
		historyCommands.append(lastSql)
		if (StringUtils.RightStr(lastSql, 1) != ';') {
			historyCommands.append(';')
			lastSql += ';'
		}
		historyCommands.append('\n')
	}

	/** Logging echo message */
	private void doEcho(SQLParser parser) {
		def parseScript = parser.lexer.script

		def posHeader = parser.lexer.findKeyWithType([Lexer.TokenType.SINGLE_WORD, Lexer.TokenType.FUNCTION], 'ECHO')
		if (posHeader == -1)
			throw new ExceptionGETL("Invalid ECHO statement: $parseScript")
		def tokenHeader = parser.lexer.tokens[posHeader]

		String text = null
		def posText = (tokenHeader.first as Integer) + 5
		if (posText < parseScript.length() - 1)
			text = parseScript.substring(posText).trim().trim()

		if (text != null && text.length() > 0)
			logger.write(logEcho, StringUtils.UnescapeJava(StringUtils.EvalMacroString(text, allVars)))
	}

	private void doError(SQLParser parser) {
		def parseScript = parser.lexer.script

		def posHeader = parser.lexer.findKeyWithType([Lexer.TokenType.SINGLE_WORD, Lexer.TokenType.FUNCTION], 'ERROR')
		if (posHeader == -1)
			throw new ExceptionGETL("Invalid ERROR statement: $parseScript")
		def tokenHeader = parser.lexer.tokens[posHeader]

		String text = null
		def posText = tokenHeader.last as Integer + 2
		if (posText < parseScript.length() - 1)
			text = parseScript.substring(posText).trim()

		throw new ExceptionSQLScripter("SQLScripter: found error ${StringUtils.EvalMacroString(text, allVars)}!")
	}
	
	private Boolean requiredExit
	@JsonIgnore
	Boolean isRequiredExit() { requiredExit }

	/**
	 * Run SQL script
	 * @param useParsing enable script command parsing (defaults to extensionForSqlScripts from connection)
	 */
	@SuppressWarnings('GroovyFallthrough')
	@Synchronized
	void runSql(Boolean useParsing = null) {
		if (connection == null)
			throw new ExceptionGETL('Not defined jdbc connection for work!')

		if (script == null)
			throw new ExceptionGETL('No script was specified to execute!')

		if (useParsing == null)
			useParsing = connection.extensionForSqlScripts()

		lastSql = null
		historyCommands.setLength(0)
		historyDDL.setLength(0)
		historyDML.setLength(0)

		if (!useParsing) {
			setLastSql(StringUtils.EvalMacroString(script, allVars).trim())
			connection.executeCommand(command: lastSql)
			historyCommands.append(lastSql)
			if (StringUtils.RightStr(lastSql, 1) != ';') {
				historyCommands.append(';')
				lastSql += ';'
			}
			historyCommands.append('\n')

			return
		}

		//noinspection GroovyUnusedAssignment
		def st = new SQLParser(script).scripts()
		requiredExit = false
		rowCount = 0
		for (Integer i = 0; i < st.size(); i++) {
			if (requiredExit)
				break

			def parser = new SQLParser(st[i])
			def type = parser.statementType()

			try {
				switch (type) {
					case SQLParser.StatementType.INSERT: case SQLParser.StatementType.UPDATE:
					case SQLParser.StatementType.DELETE: case SQLParser.StatementType.MERGE:
					case SQLParser.StatementType.TRUNCATE: case SQLParser.StatementType.START_TRANSACTION:
					case SQLParser.StatementType.COMMIT: case SQLParser.StatementType.ROLLBACK:
						doDML(parser)
						break
					case SQLParser.StatementType.CREATE: case SQLParser.StatementType.ALTER:
					case SQLParser.StatementType.DROP:
						doDDL(parser)
						break
					case SQLParser.StatementType.SELECT:
						doSelect(parser)
						break
					case SQLParser.StatementType.GETL_SET:
						doSetVar(parser)
						break
					case SQLParser.StatementType.GETL_ECHO:
						doEcho(parser)
						break
					case SQLParser.StatementType.GETL_FOR:
						doFor(parser)
						break
					case SQLParser.StatementType.GETL_IF:
						doIf(parser)
						break
					case SQLParser.StatementType.GETL_COMMAND:
						doCommand(parser)
						break
					case SQLParser.StatementType.GETL_ERROR:
						doError(parser)
						break
					case SQLParser.StatementType.GETL_EXIT:
						requiredExit = true
						break
					case SQLParser.StatementType.GETL_LOAD_POINT:
						doLoadPoint(parser)
						break
					case SQLParser.StatementType.GETL_SAVE_POINT:
						doSavePoint(parser)
						break
					case SQLParser.StatementType.GETL_RUN_FILE:
						doRunFile(parser)
						break
					case SQLParser.StatementType.GETL_SWITCH_LOGIN:
						doSwitchLogin(parser)
						break
					case SQLParser.StatementType.SINGLE_COMMENT: case SQLParser.StatementType.MULTI_COMMENT:
						continue
					default:
						doOther(parser)
				}
			}
			catch (Exception e) {
				logger.severe("Error run script:\n${st[i]}")
				throw e
			}
		}
	}

	private Pattern scriptVariablePattern = Pattern.compile('^[:](\\w+)$')

	/** Detect count script variable name in comment */
	private String detectScriptVariable(SQLParser parser) {
		def tokens = parser.lexer.tokens
		def count = tokens.size()
		def i = 0
		def c = -1
		while (i < count) {
			def token = tokens[i]
			def type = token.type as Lexer.TokenType
			if (type == Lexer.TokenType.COMMENT)
				c = i
			else if (!(type in [Lexer.TokenType.LINE_FEED, Lexer.TokenType.SINGLE_COMMENT]))
				break

			i++
		}

		String res = null
		if (c != -1) {
			def token = tokens[c]
			if (token.value != null) {
				def comment = (token.value as String).trim().toLowerCase()
				def matcher = scriptVariablePattern.matcher(comment)
				if (matcher.matches())
					res = matcher.group(1)
			}
		}

		return res
	}

	/**
	 * Run the script for the specified file
	 * @param fileName file path (use the prefix "resource:/" to load from the resource file)
	 * @param codePage text encoding (default utf-8)
	 */
	@Synchronized
	void runFile(String fileName, String codePage = 'utf-8') {
		loadFile(fileName, codePage)
		runSql()
	}

	/**
	 * Run the script for the specified file
	 * @param useParsing enable script command parsing
	 * @param fileName file path (use the prefix "resource:/" to load from the resource file)
	 * @param codePage text encoding (default utf-8)
	 */
	@Synchronized
	void runFile(Boolean useParsing, String fileName, String codePage = 'utf-8') {
		loadFile(fileName, codePage)
		runSql(useParsing)
	}

	/**
	 * Run SQL script
	 * @param sql script to execute
	 */
	@Synchronized
	void exec(String sql) {
		script = sql
		runSql()
	}

	/**
	 * Run SQL script
	 * @param useParsing enable script command parsing
	 * @param sql script to execute
	 */
	@Synchronized
	void exec(Boolean useParsing, String sql) {
		script = sql
		runSql(useParsing)
	}

	/**
	 * Clone scripter
	 * @param newConnection use specified connection (if null value using current connection)
	 * @param newPointConnection use specified point connection (if null value using current point connection)
	 * @return cloned object
	 */
	SQLScripter cloneSQLScripter(JDBCConnection newConnection = null, JDBCConnection newPointConnection = null) {
		if (newConnection == null)
			newConnection = this.connection
		if (newPointConnection == null)
			newPointConnection = this.pointConnection

		def className = this.getClass().name
		def res = Class.forName(className).getConstructor().newInstance() as SQLScripter

		if (newConnection != null)
			res.connection = newConnection
		if (newPointConnection != null)
			res.pointConnection = newPointConnection

		res.dslCreator = dslCreator
		res.dslNameObject = dslNameObject
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
		def pointCon = (pointConnection != null)?
				((connection != pointConnection)?(pointConnection.cloneConnection() as JDBCConnection):con):null
		return cloneSQLScripter(con, pointCon)
	}

	@Override
	Object clone() {
		return cloneSQLScripter()
	}

	Object cloneWithConnection() {
		return cloneSQLScripterConnection()
	}
}