package getl.vertica

import getl.data.Field
import getl.exception.ExceptionGETL
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.proc.Flow
import getl.proc.Job
import getl.tfs.TDS
import getl.utils.BoolUtils
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.GenerationUtils
import getl.utils.ListUtils
import getl.utils.Logs
import getl.utils.MapUtils
import getl.utils.StringUtils
import groovy.transform.InheritConstructors

import java.util.regex.Matcher

/**
 * Reverse engineering database model to sql file as DDL operators
 * @author Aleksey Konstantinov
 */
@InheritConstructors
class ReverseEngineering extends Job {
	static public final BigDecimal version = 1.1

	private VerticaConnection cVertica
	/** Vertica connection */
	VerticaConnection getConnectionVertica() { cVertica }
	/** Vertica connection */
	void setConnectionVertica(VerticaConnection value) {
		cVertica = value
		tVersion.connection = cVertica
		tCurUser.connection = cVertica
		tPools.connection = cVertica
		tRoles.connection = cVertica
		tUsers.connection = cVertica
		tSchemas.connection = cVertica
		tSequences.connection = cVertica
		tTables.connection = cVertica
		tViews.connection = cVertica
		tSQLFunctions.connection = cVertica
		tGrants.connection = cVertica
	}

	/** Script files path */
	public String scriptPath
	/** Clear directory before start process */
	public Boolean isClearDir = false

	private def tVersion = new QueryDataset(query: 'SELECT Version() AS version')
	private def tCurUser = new QueryDataset(query: 'SELECT CURRENT_USER')
	private def tPools = new TableDataset(schemaName: 'v_temp_schema', tableName: 'getl_pools')
	private def tRoles = new TableDataset(schemaName: 'v_temp_schema', tableName: 'getl_roles')
	private def tUsers = new TableDataset(schemaName: 'v_temp_schema', tableName: 'getl_users')
	private def tSchemas = new TableDataset(schemaName: 'v_temp_schema', tableName: 'getl_schemas')
	private def tSequences = new TableDataset(schemaName: 'v_temp_schema', tableName: 'getl_sequences')
	private def tTables = new TableDataset(schemaName: 'v_temp_schema', tableName: 'getl_tables')
	private def tViews = new TableDataset(schemaName: 'v_temp_schema', tableName: 'getl_views')
	private def tSQLFunctions = new TableDataset(schemaName: 'v_temp_schema', tableName: 'getl_sql_functions')
	private def tGrants = new TableDataset(schemaName: 'v_temp_schema', tableName: 'getl_grants')

	private def cCache = new TDS(connectDatabase: TDS.storageDatabaseName)
	private def hFiles = new TableDataset(connection: cCache, tableName: 'files', field: [new Field(name: 'filename', length: 1024, isKey: true)])
	private def hPools = new TableDataset(connection: cCache, tableName: 'pools')
	private def hRoles = new TableDataset(connection: cCache, tableName: 'roles')
	private def hUsers = new TableDataset(connection: cCache, tableName: 'users')
	private def hSchemas = new TableDataset(connection: cCache, tableName: 'schemas')
	private def hSequences = new TableDataset(connection: cCache, tableName: 'sequences')
	private def hTables = new TableDataset(connection: cCache, tableName: 'tables')
	private def hViews = new TableDataset(connection: cCache, tableName: 'views')
	private def hSQLFunctions = new TableDataset(connection: cCache, tableName: 'sql_functions')
	private def hGrants = new TableDataset(connection: cCache, tableName: 'grants')

	private def statFilesFind = 'SELECT FILENAME FROM FILES WHERE FILENAME = ?'
	private def statFilesInsert = 'INSERT INTO FILES (FILENAME) VALUES (?)'

	private BigDecimal verticaVersion
	private String curUser
	private Map sectionCreate
	private Map sectionFileName
	private Map sectionDrop
	private String fileNamePools
	private String fileNameRoles
	private String fileNameUsers
	private String fileNameSchemas
	private String fileNameSequences
	private String fileNameTables
	private String fileNameViews
	private String fileNameSQLFunctions
	private String fileNameGrants
	private def poolEmpty
	private def userEmpty
	private def sequenceCurrent
	private def tableConstraints
	private def projectionTables
	private def projectionKsafe
	private def projectionAnalyzeSuper
	private def columnComment

	@SuppressWarnings('SpellCheckingInspection')
	private final def sqlObjects = [
		pools: [table: 'v_catalog.resource_pools', name: 'NAME',
				where: '''(NOT is_internal OR name ILIKE 'general')'''],

		roles: [table: 'v_catalog.roles', name: 'NAME',
				where: '''Lower(name) NOT IN ('pseudosuperuser', 'dbduser', 'public', 'dbadmin', 'sysmonitor')'''],

		users: [table: 'v_catalog.users', name: 'USER_NAME', where: '1 = 1'],

		schemas: [name: 'SCHEMA_NAME',
				  table: 'v_catalog.schemata s INNER JOIN v_internal.vs_schemata vs ON vs.oid = s.schema_id',
				  where: '''NOT is_system_schema AND Lower(schema_name) NOT IN ('txtindex', 'v_idol', 'v_txtindex', 'v_temp_schema', 'v_func')'''],

		sequences: [table: 'v_catalog.sequences', schema: 'SEQUENCE_SCHEMA', name: 'SEQUENCE_NAME',
					where: '''Lower(sequence_schema) NOT IN ('v_temp_schema') AND identity_table_name IS NULL'''],

		tables: [table: 'v_catalog.tables', schema: 'TABLE_SCHEMA', name: 'TABLE_NAME',
				 where: '''Lower(table_schema) NOT IN ('v_temp_schema')'''],

		views: [table: 'v_catalog.views', schema: 'TABLE_SCHEMA', name: 'TABLE_NAME',
				where: '''NOT is_system_view AND Lower(table_schema) NOT IN ('v_temp_schema')'''],

		sql_functions: [table: 'v_catalog.user_functions', schema: 'SCHEMA_NAME', name: 'FUNCTION_NAME',
						where: '''procedure_type = 'User Defined Function' 
	AND Lower(schema_name) NOT IN ('v_txtindex', 'txtindex', 'v_temp_schema') 
	AND NOT (schema_name = 'public' AND function_name IN ('isOrContains')) 
	AND function_definition ILIKE 'RETURN %\''''],

		grants: [schema: 'OBJECT_SCHEMA', name: 'OBJECT_NAME', type: 'OBJECT_TYPE',
				table: '''v_catalog.grants g LEFT JOIN v_catalog.user_functions f ON
		f.schema_name = g.object_schema AND f.function_name = g.object_name 
		AND f.procedure_type = 'User Defined Function' 
		AND f.schema_name NOT IN ('v_txtindex', 'txtindex') 
		AND NOT (f.schema_name = 'public' AND f.function_name IN ('isOrContains')) 
		AND f.function_definition ILIKE 'RETURN %\'''',
				 where: '''Lower(object_type) NOT IN ('database', 'clientauthentication', 'library')
	AND (Lower(object_type) <> 'procedure' OR (Lower(object_type) = 'procedure' AND f.function_name IS NOT NULL)) 
	AND (object_type != 'SCHEMA' OR Lower(object_name) NOT IN ('v_catalog', 'v_internal', 'v_monitor', 'public', 'v_txtindex', 'v_temp_schema'))'''],
	]

	private final def sqlPrepare = """
-- Get pools
CREATE LOCAL TEMPORARY TABLE getl_pools ON COMMIT PRESERVE ROWS AS
SELECT 
	name, memorysize, maxmemorysize, executionparallelism, priority, runtimepriority, runtimeprioritythreshold, 
	queuetimeout::varchar(100), plannedconcurrency, maxconcurrency, runtimecap::varchar(100), cpuaffinityset, cpuaffinitymode, cascadeto
FROM ${sqlObjects.pools.table}
WHERE 
	${sqlObjects.pools.where}
ORDER BY Lower(name);

-- Get roles
CREATE LOCAL TEMPORARY TABLE getl_roles ON COMMIT PRESERVE ROWS AS
SELECT *
FROM ${sqlObjects.roles.table}
WHERE 
	${sqlObjects.roles.where}
ORDER BY Lower(name);

-- Get users
CREATE LOCAL TEMPORARY TABLE getl_users ON COMMIT PRESERVE ROWS AS
SELECT *
FROM ${sqlObjects.users.table}
WHERE 
	${sqlObjects.users.where}
ORDER BY Lower(user_name);

-- Get schemas
CREATE LOCAL TEMPORARY TABLE getl_schemas ON COMMIT PRESERVE ROWS AS
SELECT *
FROM ${sqlObjects.schemas.table}
WHERE 
	${sqlObjects.schemas.where}
ORDER BY Lower(schema_name);

CREATE LOCAL TEMPORARY TABLE getl_sequences ON COMMIT PRESERVE ROWS AS
SELECT sequence_schema, sequence_name, owner_name, identity_table_name, session_cache_count, allow_cycle, output_ordered, increment_by, current_value::numeric(38)
FROM ${sqlObjects.sequences.table}
WHERE 
	${sqlObjects.sequences.where}
ORDER BY sequence_schema, sequence_name;

CREATE LOCAL TEMPORARY TABLE getl_tables ON COMMIT PRESERVE ROWS AS
SELECT *
FROM ${sqlObjects.tables.table}
WHERE 
	${sqlObjects.tables.where}
ORDER BY Lower(table_schema), Lower(table_name);

CREATE LOCAL TEMPORARY TABLE getl_views ON COMMIT PRESERVE ROWS AS
SELECT *
FROM ${sqlObjects.views.table}
WHERE 
	${sqlObjects.views.where}
ORDER BY Lower(table_schema), Lower(table_name);

CREATE LOCAL TEMPORARY TABLE getl_sql_functions ON COMMIT PRESERVE ROWS AS
SELECT *
FROM ${sqlObjects.sql_functions.table}
WHERE 
	${sqlObjects.sql_functions.where}
ORDER BY schema_name, function_name;

CREATE LOCAL TEMPORARY TABLE getl_grants ON COMMIT PRESERVE ROWS AS
SELECT *
FROM ${sqlObjects.grants.table}
WHERE 
	${sqlObjects.grants.where}
ORDER BY object_type, Lower(object_schema), Lower(object_name), Lower(grantor), Lower(grantee);
"""

	static main(args) {
		def a = MapUtils.ProcessArguments(args)
		if ((a.config as Map)?.filename == null || (a.size() == 1 && (a.containsKey('help') || a.containsKey('/help') || a.containsKey('/?')))) {
			println "### Reverse engineering Vertica tool, version $version, EasyData company (www.easydata.ru)"
			println '''
Syntax:
  getl.vertica.ReverseEngineering config.filename=<config file name> [list=<NONE|PRINT>] [clear=<true|false>] [script_path=<sql files path>]
	
list: print list of used objects (pools, roles, users, schemas, sequences, tables, views, sql_functions and grants)
clear: clearing all scripts in destination directory before processing
  
Example:
  java -cp "libs/*" getl.vertica.ReverseEngineering config.filename=demo.conf list=print clear=false script_path=../sql/demo 
'''
			return
		}

		new ReverseEngineering().run(args)
	}

	/**
	 * Build reverse statement
	 * @param pattern
	 * @param value
	 * @param quote
	 * @return
	 */
	static String stat(String pattern, def value, Boolean quote = false) {
		if (value == null) return ''
		if ((value instanceof String || value instanceof GString) && value == '')
			return ''
		if (quote) value = '\'' + value.toString() + '\''
		return StringUtils.EvalMacroString(pattern, [val: value])
	}

	/**
	 * Build reverse statement with feed line
	 */
	static String statLine(String pattern, def value, Boolean quote = false) {
		if (value == null) return ''
		if ((value instanceof String || value instanceof GString) && value == '')
			return ''
		if (quote) value = '\'' + value.toString() + '\''
		return StringUtils.EvalMacroString(pattern, [val: value]) + '\n'
	}

	/**
	 * Build parameter for statement
	 * @param param
	 * @param value
	 * @param quote
	 * @return
	 */
	static String par(String param, def value, Boolean quote = false) {
		if (value == null)
			return ''
		if ((value instanceof String || value instanceof GString) && value == '')
			return ''
		if (quote)
			value = '\'' + value.toString() + '\''
		return param + ' ' + value.toString()
	}

	/**
	 * Build parameter for statement with feed line
	 * @param param
	 * @param value
	 * @param quote
	 * @return
	 */
	@SuppressWarnings('SpellCheckingInspection')
	static String parln(String param, def value, Boolean quote = false) {
		if (value == null) return ''
		if ((value instanceof String || value instanceof GString) && value == '')
			return ''
		if (quote) value = '\'' + value.toString() + '\''
		return param + ' ' + value.toString() + '\n'
	}

	/**
	 * Convert string to list
	 * @param list
	 * @return
	 */
	static Map<String, List<String>> procList(String list, Closure valid = null) {
		def res = new HashMap<String, List<String>>()

		def withoutGrant = [] as List<String>
		def withGrant = [] as List<String>

		list = list.trim()
		if (list != '') {
			def l = list.split(',')
			l.each { s ->
				s = s.trim()
				if (valid == null || valid(s)) {
					if (!s.matches('.*[*]')) withoutGrant << s else withGrant << s.substring(0, s.length() - 1)
				}
			}
		}

		res.withoutGrant = withoutGrant.sort(false)
		res.withGrant = withGrant.sort(false)
		return res
	}

	/**
	 * Generate full name database objects
	 * @param schema
	 * @param name
	 * @return
	 */
	static String objectName(String schema, String name) {
		return '"' + schema + '"."' + name + '"'
	}

	/**
	 * Read DDL from database objects
	 * @param schema
	 * @param object
	 * @return
	 */
	String ddl(String schema, String object) {
		def name = "\"${schema.replace("'", "\\'").replace('"', '""')}\"." +
				"\"${object.replace("'", "\\'").replace('"', '""')}\""
		def qExportObjects = new QueryDataset(connection: cVertica, query: "SELECT EXPORT_OBJECTS('', E'$name', false)")
		def r = qExportObjects.rows()
		assert r.size() == 1, "Object \"$name\" not found"
		String s = (r[0].export_objects as String).replace('\r', '')
		return s.trim()
	}

	/**
	 * Generate DDL from table objects
	 * @param schema
	 * @param object
	 * @return
	 */
	@SuppressWarnings(["GroovyMissingReturnStatement", 'UnnecessaryQualifiedReference', 'UnnecessaryQualifiedReference'])
	Map<String, String> ddlTable(String schema, String object) {
		def sql = ddl(schema, object)
		StringBuilder create = new StringBuilder()
		StringBuilder alter = new StringBuilder()
		def finishAnalyze = false
		def startProj = false
		for (line in sql.lines()) {
			if (finishAnalyze || line.trim() == '\n')
				break

			Matcher projMatcher
			if (!projectionTables || projectionAnalyzeSuper || projectionKsafe != null) {
				projMatcher = line =~ /(?i)CREATE PROJECTION (.+)[.](.+)/

				if (!projectionTables && projMatcher.count == 1) {
					//noinspection GroovyUnusedAssignment
					finishAnalyze = true
					break
				}
			}

			if (line.matches('(?i)ALTER TABLE .+')) {
				if (tableConstraints) {
					if (line.matches('(?i)ALTER TABLE .+ ADD CONSTRAINT .+ PRIMARY KEY [(].+[)].*') || line.matches('(?i)ALTER TABLE .+ ADD CONSTRAINT .+ UNIQUE [(].+[)].*')) {
						create << line
						create << '\n'
					} else {
						alter << line
						alter << '\n'
					}
				}
			}
			else if (line.matches('(?i)COMMENT ON COLUMN .+;')) {
				if (columnComment) {
					create << line
					create << '\n'
				}
			}
			else if (projMatcher != null && projMatcher.count == 1) {
				if (projectionKsafe != null) startProj = true
				if (projectionAnalyzeSuper) {
					def projSchema = (projMatcher[0] as List)[1] as String
					def projName = (projMatcher[0] as List)[2] as String
					if (projName == object && !projName.matches('.+ /[*][+].+[*]/')) {
						create << "CREATE PROJECTION \"${projSchema}\".\"${projName}\" /*+createtype(A)*/"
						create << '\n'
					}
					else {
						create << line
						create << '\n'
					}
				}
				else {
					create << line
					create << '\n'
				}
			}
			else if (startProj) {
				def m = line =~ /(?i).*KSAFE (\d+);/
				if (m.count == 1) {
					startProj = false
					def ksafe = Integer.valueOf((m[0] as List)[1] as String)
					if (ksafe > projectionKsafe) {
						create << line.substring(0, m.start(1)) + projectionKsafe.toString() + line.substring(m.end(1))
						create << '\n'
					}
					else {
						create << line
						create << '\n'
					}
				}
				else if (line.matches('(?i)UNSEGMENTED .+;')) {
					startProj = false
					create << line
					create << '\n'
				}
				else {
					create << line
					create << '\n'
				}
			}
			else {
				create << line
				create << '\n'
			}
		}

		return [create: create.toString().trim(), alter: alter.toString().trim()]
	}

	@SuppressWarnings("GroovyAssignabilityCheck")
	void readVerticaVersion() {
		def r = tVersion.rows()
		def s = r[0].version
		def m = s =~ /(?i)Vertica Analytic Database v([0-9]+[.][0-9]+)[.].*/
		if (m.size() != 1) throw new ExceptionGETL("Can not parse the version of Vertica for value \"$s\"")
		verticaVersion = new BigDecimal(m[0][1])
		Logs.Info("Detected \"$verticaVersion\" version Vertica")
	}

	/**
	 * Read cur user information
	 */
	void readCurUser() {
		def r = tCurUser.rows()
		curUser = r[0].current_user
		Logs.Info("Login as \"$curUser\" user")
	}

	/**
	 * Init storage by script files name
	 */
	void initFiles() {
		hFiles.create()
		cCache.sqlConnection.cacheStatements = true
	}

	/**
	 * Current type object for write
	 */
	private def currentObject

	/**
	 * Current file mask for write
	 */
	private def currentFileMask = ''

	/**
	 * Current parameters for write
	 */
	private Map<String, String> currentVars = new HashMap<String, String>()

	/**
	 * File has write data
	 */
	private def isWriteln = false

	void setWrite(String object, String fileMask, Map vars = new HashMap()) {
		assert object != null
		currentObject = object

		assert fileMask != null, "Required file mask for \"$currentObject\" object"
		currentFileMask = fileMask

		currentVars = vars

		isWriteln = false
	}

	void write(String script) {
		def v = new HashMap<String, String>()
		currentVars.each { var ->
			v.put(var.key, var.value.replace('*', '_').replace('?', '_')
					.replace('/', '_').replace('\\', '_')
					.replace(':', '_').replace('\n', ' ')
					.replace('\r', ' ').replace('\t', ' ')
					.replace('<', '_').replace('>', '_')
					.replace('\'', '_').replace('"', '_')
					.replace('~', '_').replace('`', '_')
					.replace((Config.windows)?'%':'$', ''))
		}
		def filename = StringUtils.EvalMacroString(currentFileMask, v).toLowerCase()
		def filepath = "$scriptPath${File.separatorChar}${filename}.sql"

		def row = cCache.sqlConnection.firstRow(statFilesFind, [filename])
		def file = new File(filepath)
		if (row == null) {
			FileUtils.ValidFilePath(file)
			if (file.exists()) {
				file.text = ''
			}
			cCache.sqlConnection.executeInsert(statFilesInsert, [filename])
			script = "--- ***** $currentObject ***** ---\n\n" + script
		}

		file.append(script, 'utf-8')
		isWriteln = true
	}

	void writeln(String script) {
		write(script + '\n')
	}

	@SuppressWarnings('UnnecessaryQualifiedReference')
	static String eval(String val) {
		return GenerationUtils.EvalGroovyScript(value: '"""' + val.replace('\\', '\\\\').replace('"', '\\"') + '"""',
				vars: Config.vars + ((Job.jobArgs.vars?:new HashMap<String, Object>()) as Map<String, Object>))
	}

	/**
	 * Init reverse objects
	 */
	@SuppressWarnings('SpellCheckingInspection')
	void initReverse() {
		// Read drop parameters section
		sectionDrop = (Map)Config.content.drop?:new HashMap()

		// Read create parameters section
		sectionCreate = (Map)Config.content.create?:new HashMap()

		// Read file name parameters section
		sectionFileName = (Map)Config.content.filename?:new HashMap()
		fileNamePools = (String)sectionFileName.pools?:'pools'
		fileNameRoles = (String)sectionFileName.roles?:'roles'
		fileNameUsers = (String)sectionFileName.users?:'users'
		fileNameSchemas = (String)sectionFileName.schemas?:'schemas'
		fileNameSequences = (String)sectionFileName.sequences?:'sequences'
		fileNameTables = (String)sectionFileName.tables?:'tables'
		fileNameViews = (String)sectionFileName.views?:'views'
		fileNameSQLFunctions = (String)sectionFileName.sql_functions?:'sql_functions'
		fileNameGrants = (String)sectionFileName.grants

		// Read pool params
		poolEmpty = BoolUtils.IsValue(sectionCreate.pool_empty)

		// Read user params
		userEmpty = BoolUtils.IsValue(sectionCreate.user_empty)

		// Read sequence params
		sequenceCurrent = BoolUtils.IsValue(sectionCreate.sequences_current)

		// Read table params
		tableConstraints = BoolUtils.IsValue(sectionCreate.table_constraints, true)

		// Read projection params
		projectionTables = BoolUtils.IsValue(sectionCreate.projection_tables, true)
		projectionKsafe = (sectionCreate.projection_ksafe != null)?Integer.valueOf(sectionCreate.projection_ksafe.toString()):null
		projectionAnalyzeSuper = sectionCreate.projection_analyze_super

		// Read column params
		columnComment = BoolUtils.IsValue(sectionCreate.column_comments, true)

		// Init default wheres by objects
		def default_where = 'false'
		def pools_where = default_where
		def roles_where = default_where
		def users_where = default_where
		def schemas_where = default_where
		def sequences_where = default_where
		def tables_where = default_where
//		def aggr_projections_where = default_where
		def views_where = default_where
		def sql_functions_where = default_where
		def grants_where = default_where

		if (sectionCreate.pools) pools_where = eval(sectionCreate.pools.toString())
		Logs.Info("Reverse pools: $pools_where")
		if (sectionCreate.roles) roles_where = eval(sectionCreate.roles.toString())
		Logs.Info("Reverse roles: $roles_where")
		if (sectionCreate.users) users_where = eval(sectionCreate.users.toString())
		Logs.Info("Reverse users: $users_where")
		if (sectionCreate.schemas) schemas_where = eval(sectionCreate.schemas.toString())
		Logs.Info("Reverse schemas: $schemas_where")
		if (sectionCreate.sequences) sequences_where = eval(sectionCreate.sequences.toString())
		Logs.Info("Reverse sequences: $sequences_where")
		def tablesCreate = ''
		if (sectionCreate.tables) {
			tables_where = eval(sectionCreate.tables.toString())
			if (projectionTables) {
				tablesCreate = ", option: reverse projections by tables"
				if (projectionKsafe != null) tablesCreate += " with $projectionKsafe ksafe"
				if (projectionAnalyzeSuper) tablesCreate += " for analyze super projection and fix type if equal name from base table"
			}
		}
		Logs.Info("Reverse tables: $tables_where$tablesCreate")
//		if (sectionCreate.aggregate_projections) aggr_projections_where = sectionCreate.aggregate_projections; Logs.Info("Reverse aggregate projections: $aggr_projections_where")
		if (sectionCreate.views) views_where = eval(sectionCreate.views.toString())
		Logs.Info("Reverse views: $views_where")
		if (sectionCreate.sql_functions) sql_functions_where = eval(sectionCreate.sql_functions.toString())
		Logs.Info("Reverse sql functions: $sql_functions_where")
		if (sectionCreate.grants) grants_where = eval(sectionCreate.grants.toString())
		Logs.Info("Reverse grants: $grants_where")

		Logs.Finest("Prepared structures ...")
		initFiles()

		cVertica.executeCommand(command: sqlPrepare)

		Logs.Finest("Read object model ...")
		Long count

		count = new Flow().copy(source: tPools, source_where: (pools_where && pools_where != 'false')?'1=1':'0=1',dest: hPools, inheritFields: true, createDest: true)
		Logs.Fine("$count pools found")

		count = new Flow().copy(source: tRoles, source_where: (roles_where && roles_where != 'false')?'1=1':'0=1', dest: hRoles, inheritFields: true, createDest: true)
		Logs.Fine("$count roles found")

		count = new Flow().copy(source: tUsers, source_where: (users_where && users_where != 'false')?'1=1':'0=1', dest: hUsers, inheritFields: true, createDest: true)
		Logs.Fine("$count users found")

		count = new Flow().copy(source: tSchemas, source_where: (schemas_where && schemas_where != 'false')?'1=1':'0=1', dest: hSchemas, inheritFields: true, createDest: true)
		Logs.Fine("$count schemas found")

		count = new Flow().copy(source: tSequences, source_where: (sequences_where && sequences_where != 'false')?'1=1':'0=1', dest: hSequences, inheritFields: true, createDest: true)
		Logs.Fine("$count sequences found")

		count = new Flow().copy(source: tTables, source_where: (tables_where && tables_where != 'false')?'1=1':'0=1', dest: hTables, inheritFields: true, createDest: true)
		Logs.Fine("$count tables found")

		count = new Flow().copy(source: tViews, source_where: (views_where && views_where != 'false')?'1=1':'0=1', dest: hViews, inheritFields: true, createDest: true)
		Logs.Fine("$count views found")

		count = new Flow().copy(source: tSQLFunctions, source_where: (sql_functions_where && sql_functions_where != 'false')?'1=1':'0=1', dest: hSQLFunctions, inheritFields: true, createDest: true)
		Logs.Fine("$count sql functions found")

		count = new Flow().copy(source: tGrants, source_where: (grants_where && grants_where != 'false')?'1=1':'0=1', dest: hGrants, inheritFields: true, createDest: true)
		Logs.Fine("$count grants found")

		cCache.executeCommand(command: "DELETE FROM pools WHERE NOT ($pools_where)")

		cCache.executeCommand(command: "DELETE FROM roles WHERE NOT ($roles_where)")

		cCache.executeCommand(command: "DELETE FROM users WHERE NOT ($users_where)")

		cCache.executeCommand(command: "DELETE FROM schemas WHERE schema_owner <> 'dbadmin' AND schema_owner NOT IN (SELECT user_name FROM users)")
		cCache.executeCommand(command: "DELETE FROM schemas WHERE NOT ($schemas_where)")

		cCache.executeCommand(command: "DELETE FROM sequences WHERE owner_name <> 'dbadmin' AND owner_name NOT IN (SELECT user_name FROM users)")
		cCache.executeCommand(command: "DELETE FROM sequences WHERE sequence_schema NOT IN (SELECT schema_name FROM schemas)")
		cCache.executeCommand(command: "DELETE FROM sequences WHERE NOT ($sequences_where)")

		cCache.executeCommand(command: "DELETE FROM tables WHERE owner_name <> 'dbadmin' AND owner_name NOT IN (SELECT user_name FROM users)")
		cCache.executeCommand(command: "DELETE FROM tables WHERE table_schema NOT IN (SELECT schema_name FROM schemas)")
		cCache.executeCommand(command: "DELETE FROM tables WHERE NOT ($tables_where)")

		cCache.executeCommand(command: "DELETE FROM views WHERE owner_name <> 'dbadmin' AND owner_name NOT IN (SELECT user_name FROM users)")
		cCache.executeCommand(command: "DELETE FROM views WHERE table_schema NOT IN (SELECT schema_name FROM schemas)")
		cCache.executeCommand(command: "DELETE FROM views WHERE NOT ($views_where)")

		cCache.executeCommand(command: "DELETE FROM sql_functions WHERE owner <> 'dbadmin' AND owner NOT IN (SELECT user_name FROM users)")
		cCache.executeCommand(command: "DELETE FROM sql_functions WHERE schema_name NOT IN (SELECT schema_name FROM schemas)")
		cCache.executeCommand(command: "DELETE FROM sql_functions WHERE NOT ($sql_functions_where)")

		cCache.executeCommand(command: "DELETE FROM grants WHERE object_type = 'RESOURCEPOOL' AND object_name NOT IN (SELECT name from pools)")

		cCache.executeCommand(command: "DELETE FROM grants WHERE object_type = 'ROLE' AND object_name NOT IN (SELECT name from roles)")

		cCache.executeCommand(command: "DELETE FROM grants WHERE object_type = 'SCHEMA' AND object_name NOT IN (SELECT schema_name from schemas)")

		cCache.executeCommand(command: "DELETE FROM grants WHERE object_type = 'SEQUENCE' AND NOT EXISTS(SELECT * from sequences WHERE sequence_schema = object_schema AND sequence_name = object_name)")

		cCache.executeCommand(command: "DELETE FROM grants WHERE object_type = 'TABLE' AND NOT EXISTS(SELECT * from tables WHERE table_schema = object_schema AND table_name = object_name)")

		cCache.executeCommand(command: "DELETE FROM grants WHERE object_type = 'VIEW' AND NOT EXISTS(SELECT * from views WHERE table_schema = object_schema AND table_name = object_name)")

		cCache.executeCommand(command: "DELETE FROM grants WHERE object_type = 'PROCEDURE' AND NOT EXISTS(SELECT * from sql_functions WHERE schema_name = object_schema AND function_name = object_name)")

		cCache.executeCommand(command: "DELETE FROM grants WHERE grantee NOT IN ('public', 'dbadmin') AND (grantee NOT IN (SELECT name from roles) AND grantee NOT IN (SELECT user_name from users))")

		cCache.executeCommand(command: "DELETE FROM grants WHERE NOT ($grants_where)")
	}

	/**
	 * Finish reverse and clearing temp data
	 */
	void doneReverse() {
		cVertica.connected = false

		hFiles.drop(ifExists: true)
		hPools.drop(ifExists: true)
		hRoles.drop(ifExists: true)
		hUsers.drop(ifExists: true)
		hSchemas.drop(ifExists: true)
		hSequences.drop(ifExists: true)
		hTables.drop(ifExists: true)
		hViews.drop(ifExists: true)
		hSQLFunctions.drop(ifExists: true)
		hGrants.drop(ifExists: true)

		cCache.connected = false
	}

	/** Process job */
	void process() {
		// Set Vertica connection
		connectionVertica = new VerticaConnection(config: "vertica")

		// Set path from script files
		scriptPath = (jobArgs.script_path?:Config.content.script_path) as String
		if (scriptPath == null)
			throw new ExceptionGETL('Required value for "script_path" parameter!')

		// Set clearing flag
		isClearDir = BoolUtils.IsValue(jobArgs.clear)

		if (jobArgs.list != null) {
			def l = (jobArgs.list as String).toLowerCase()
			if (!(l in ['none', 'print'])) {
				Logs.Severe("Unknown list option \"$l\"")
				doneReverse()
				return
			}
			if (l == 'print') {
				println "*** Pools ***"
				listPools().each { r -> println r.name }

				println "*** Roles ***"
				listRoles().each { r -> println r.name }

				println "*** Users ***"
				listUsers().each { r -> println r.name }

				println "*** Schemas ***"
				listSchemas().each { r -> println r.name }

				println "*** Sequences ***"
				listSequences().each { r -> println "${r.schema}.${r.name}" }

				println "*** Tables ***"
				listTables().each { r -> println "${r.schema}.${r.name}" }

				println "*** Views ***"
				listViews().each { r -> println "${r.schema}.${r.name}" }

				println "*** SQL functions ***"
				listSql_functions().each { r -> println "${r.schema}.${r.name}" }

				println "*** Grant objects ***"
				def curType = ''
				listGrants().each { r ->
					if (r.type != curType) {
						curType = r.type
						println "* $curType *"
					}
					println "\t${(r.schema != null)?r.schema + '.':''}${r.name}"
				}
			}
		}

		reverse()
	}

	/** Reverse */
	void reverse() {
		Logs.Info("### Reverse engineering Vertica tool, version $version, EasyData company (www.easydata.ru)")

		if (cVertica == null)
			Logs.Severe('Required value for "cVertica" field!')

		// Vertica version
		readVerticaVersion()
		if (verticaVersion < 8.1) {
			Logs.Severe("Vertica version $verticaVersion not supported, required version 8.1 or greater!")
			return
		}

		// Current user
		readCurUser()

		try {
			initReverse()
		}
		catch (Exception e) {
			doneReverse()
			throw e
		}

		if (scriptPath == null) {
			Logs.Severe('Required value for "scriptPath" field!')
			doneReverse()
			return
		}

		scriptPath = FileUtils.TransformFilePath(FileUtils.ConvertToDefaultOSPath(scriptPath), dslCreator)
		Logs.Info("Write script to \"$scriptPath\" directory")
		FileUtils.ValidPath(scriptPath)

		if (isClearDir) {
			Logs.Info("Clearing the destination directory \"$scriptPath\"")
			if (!FileUtils.DeleteFolder(scriptPath, false)) {
				Logs.Severe("Can not clearing destination directory \"$scriptPath\"")
				doneReverse()
				return
			}
		}

		try {
			genPools()
			genRoles()
			genUsers()
			genSchemas()
			genSequences()
			genTables()
			genViews()
			genSQLFunctions()
			genGrants()
		}
		finally {
			doneReverse()
		}
	}

	/**
	 * Generate create resource pools script
	 */
	@SuppressWarnings('SpellCheckingInspection')
	void genPools() {
		hPools.eachRow(order: ['CASE name WHEN \'general\' THEN 0 ELSE 1 END', 'Lower(name)']) { Map r ->
			setWrite('POOLS', fileNamePools, [pool: r.name])

			if (r.name != 'general') {
				if (BoolUtils.IsValue(sectionDrop.pools)) {
					writeln"DROP RESOURCE POOL \"${r.name}\";"
				}
				writeln "CREATE RESOURCE POOL \"${r.name}\";"
				if (!poolEmpty) {
					writeln "ALTER RESOURCE POOL \"${r.name}\""
					write parln('MEMORYSIZE', r.memorysize, true)
					write parln('MAXMEMORYSIZE', r.maxmemorysize, true)
				}
			}
			else {
				if (!poolEmpty) writeln "ALTER RESOURCE POOL \"${r.name}\""
			}
			if (!poolEmpty) {
			   write parln('EXECUTIONPARALLELISM', r.executionparallelism)
			   write parln('PRIORITY', r.priority)
			   write parln('RUNTIMEPRIORITY', r.runtimepriority)
			   write parln('RUNTIMEPRIORITYTHRESHOLD', r.runtimeprioritythreshold)
			   write parln('QUEUETIMEOUT', r.queuetimeout, true)
			   write parln('PLANNEDCONCURRENCY', r.plannedconcurrency)
			   write parln('MAXCONCURRENCY', r.maxconcurrency)
			   write parln('RUNTIMECAP', r.runtimecap, true)
			   write parln('CPUAFFINITYSET', r.cpuaffinityset)
			   write parln('CPUAFFINITYMODE', r.cpuaffinitymode)
			   write parln('CASCADE TO', r.cascadeto)
			}
			if (r.name != 'general' || !poolEmpty) writeln ";"
			if (isWriteln) writeln ''
		}
		Logs.Info("${hPools.readRows} pools generated")
	}

	/**
	 * Generate create roles script
	 */
	void genRoles() {
		hRoles.eachRow(order: ['Lower(name)']) { Map r ->
			setWrite('ROLES', fileNameRoles, [role: r.name])

			if (BoolUtils.IsValue(sectionDrop.roles)) {
			   writeln "DROP ROLE \"${r.name}\" CASCADE;"
			}
		   writeln "CREATE ROLE \"${r.name}\";\n"
		}
		Logs.Info("${hRoles.readRows} roles generated")
	}

	/**
	 * Generate create users script
	 */
	@SuppressWarnings('SpellCheckingInspection')
	void genUsers() {
		hUsers.eachRow(order: ['Lower(user_name)']) { Map r ->
			setWrite('USERS', fileNameUsers, [user: r.user_name])

			if ((r.user_name as String).toLowerCase() != 'dbadmin') {
				if (BoolUtils.IsValue(sectionDrop.users)) {
				   writeln "DROP USER \"${r.user_name}\" CASCADE;"
				}
				writeln "CREATE USER \"${r.user_name}\""
			}
			else if (!userEmpty) {
				writeln "ALTER USER \"${r.user_name}\""
			}

			if (!userEmpty) {
			   write parln('RESOURCE POOL', "\"${r.resource_pool}\"")
			   write statLine('MEMORYCAP {val}', (r.memory_cap_kb == 'unlimited') ? null : (r.memory_cap_kb + 'K'), true)
			   write statLine('TEMPSPACECAP {val}', (r.temp_space_cap_kb == 'unlimited') ? null : (r.temp_space_cap_kb + 'K'), true)
			   write parln('RUNTIMECAP', (r.run_time_cap == 'unlimited') ? null : r.run_time_cap, true)
			}
		   if (isWriteln) writeln ";"

/*			def defaultRoles = procList(r.default_roles as String)
			if (!defaultRoles.withoutGrant.isEmpty()) {
			   writeln "\nALTER USER \"${r.user_name}\" DEFAULT ROLE ${ListUtils.QuoteList(defaultRoles.withoutGrant, '"').join(', ')};"
			}

			if (isWriteln) writeln ''*/
		}
		Logs.Info("${hUsers.readRows} users generated")
	}

	/**
	 * Generate create schemas script
	 */
	void genSchemas() {
		def isOneFile = (fileNameSchemas.indexOf('{') == -1)
		hSchemas.eachRow(order: ['Lower(schema_name)']) { Map r ->
			setWrite('SCHEMAS', fileNameSchemas, [schema: r.schema_name])

			if (r.schema_name == 'public') return
			if (BoolUtils.IsValue(sectionDrop.schemas)) {
			   writeln "DROP SCHEMA \"${r.schema_name}\" CASCADE;"
			}
			def privileges = (r.defaultinheritprivileges == true)?'INCLUDE':'EXCLUDE'
		   	writeln "CREATE SCHEMA \"${r.schema_name}\" AUTHORIZATION \"${r.schema_owner}\" DEFAULT $privileges SCHEMA PRIVILEGES;\n"
		}
		if (isWriteln) writeln ''
		hUsers.eachRow(order: ['Lower(user_name)']) { Map r ->
			def p = procList(r.search_path as String, { return !(it in ['v_catalog', 'v_monitor', 'v_internal', 'public', '"$user"', ''])})
			if (!p.withoutGrant.isEmpty()) {
				if (!isOneFile) setWrite('USERS', fileNameUsers, [user: r.user_name])
				writeln "ALTER USER \"${r.user_name}\" SEARCH_PATH ${ListUtils.QuoteList(p.withoutGrant, '"').join(', ')};"
			}
		}
		if (isWriteln) writeln ''

		Logs.Info("${hSchemas.readRows} schemas generated")
	}

	/**
	 * Generate create sequences script
	 */
	void genSequences() {
		hSequences.eachRow(order: ['Lower(sequence_schema)', 'Lower(sequence_name)']) { Map r ->
			setWrite('SEQUENCES', fileNameSequences, [schema: r.sequence_schema, sequence: r.sequence_name])

			def name = objectName(r.sequence_schema as String, r.sequence_name as String)
			if (BoolUtils.IsValue(sectionDrop.sequences)) {
			   writeln "DROP SEQUENCE $name;"
			}
			def sql = ddl(r.sequence_schema as String, r.sequence_name as String)
			if (verticaVersion >= 9.2) {
				def i = sql.indexOf(' SEQUENCE ')
				if (i > -1) {
					sql = sql.substring(0, i + 10) + 'IF NOT EXISTS ' + sql.substring(i + 10)
				}
			}
		   	writeln sql
			if (sequenceCurrent && r.current_value != null && r.current_value > 0)writeln "ALTER SEQUENCE $name RESTART WITH ${r.current_value};"
			if (r.owner_name != curUser) writeln "\nALTER SEQUENCE $name OWNER TO ${r.owner_name};"
			if (isWriteln) writeln ''
		}
		Logs.Info("${hSequences.readRows} sequences generated")
	}

	/**
	 * Generate create tables script
	 */
	void genTables() {
		def isOneFile = (fileNameTables.indexOf('{') == -1)

		StringBuilder alter = new StringBuilder()
		hTables.eachRow(order: ['Lower(table_schema)', 'Lower(table_name)']) { Map r ->
			setWrite('TABLES', fileNameTables, [schema: r.table_schema, table: r.table_name])

			def name = objectName(r.table_schema as String, r.table_name as String)
			if (BoolUtils.IsValue(sectionDrop.tables)) {
			   writeln "\nDROP TABLE IF EXISTS $name CASCADE;"
			}
			def stat = ddlTable(r.table_schema as String, r.table_name as String)
		   writeln stat.create
			if (r.owner_name != curUser && !r.is_temp_table) writeln "\nALTER TABLE $name OWNER TO \"${r.owner_name}\";"
			if (stat.alter != '') {
				if (isOneFile) {
					alter << stat.alter + '\n'
				}
				else {
					writeln '\n' + stat.alter
				}
			}
			if (isWriteln) writeln ''
		}
		if (alter.length() > 0) {
		   writeln ''
		   writeln alter.toString()
		}
		Logs.Info("${hTables.readRows} tables generated")
	}

	/**
	 * Generate create views script
	 */
	void genViews() {
		hViews.eachRow(order: ['table_id']) { Map r ->
			setWrite('VIEWS', fileNameViews, [schema: r.table_schema, view: r.table_name])

			def name = objectName(r.table_schema as String, r.table_name as String)
			def sql = ddl(r.table_schema as String, r.table_name as String)
			if (BoolUtils.IsValue(sectionDrop.views)) {
				def i = sql.indexOf(' VIEW')
				sql = 'CREATE OR REPLACE' + sql.substring(i)
			}
		   writeln sql
			if (r.owner_name != curUser)writeln "\nALTER VIEW $name OWNER TO \"${r.owner_name}\";"
			if (isWriteln) writeln ''
		}
		Logs.Info("${hViews.readRows} views generated")
	}

	/**
	 * Generate create SQL functions script
	 */
	void genSQLFunctions() {
		hSQLFunctions.eachRow(order: ['schema_name', 'function_name']) { Map r ->
			setWrite('SQL_FUNCTIONS', fileNameSQLFunctions, [schema: r.schema_name, sql_function: r.function_name])

			def name = objectName(r.schema_name as String, r.function_name as String)
			def sql = ddl(r.schema_name as String, r.function_name as String)
			if (BoolUtils.IsValue(sectionDrop.sql_functions)) {
				def i = sql.indexOf(' FUNCTION')
				sql = 'CREATE OR REPLACE' + sql.substring(i)
			}
			writeln sql
			if (r.owner != curUser) writeln "\nALTER FUNCTION $name OWNER TO \"${r.owner}\";"
			if (isWriteln) writeln ''
		}
		Logs.Info("${hSQLFunctions.readRows} sql functions generated")
	}

	/**
	 * Generate grant objects script
	 */
	@SuppressWarnings('SpellCheckingInspection')
	void genGrants() {
		hGrants.eachRow(order: ['Lower(object_type)', 'Lower(object_schema)', 'Lower(object_name)', 'Lower(grantee)', 'Lower(function_argument_type)', 'Lower(privileges_description)']) { Map r ->
			if (fileNameGrants != null) setWrite('GRANTS', fileNameGrants)

			def privileges = procList(r.privileges_description as String)
			switch (r.object_type) {
				case 'RESOURCEPOOL':
					if (fileNameGrants == null) setWrite('POOLS', fileNamePools, [pool: r.object_name])
					if (!privileges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withoutGrant, '"').join(', ')} ON RESOURCE POOL \"${r.object_name}\" TO \"${r.grantee}\";"
					if (!privileges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withGrant, '"').join(', ')} ON RESOURCE POOL \"${r.object_name}\" TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'SCHEMA':
					if (fileNameGrants == null) setWrite('SCHEMAS', fileNameSchemas, [schema: r.object_name])
					if (!privileges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withoutGrant, '"').join(', ')} ON SCHEMA \"${r.object_name}\" TO \"${r.grantee}\";"
					if (!privileges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withGrant, '"').join(', ')} ON SCHEMA \"${r.object_name}\" TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'SEQUENCE':
					if (fileNameGrants == null) setWrite('SEQUENCES', fileNameSequences, [schema: r.object_schema, sequence: r.object_name])
					if (!privileges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withoutGrant, '"').join(', ')} ON SEQUENCE ${objectName(r.object_schema as String, r.object_name as String)} TO \"${r.grantee}\";"
					if (!privileges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withGrant, '"').join(', ')} ON SEQUENCE ${objectName(r.object_schema as String, r.object_name as String)} TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'TABLE':
					if (fileNameGrants == null) setWrite('TABLES', fileNameTables, [schema: r.object_schema, table: r.object_name])
					if (!privileges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withoutGrant, '"').join(', ')} ON ${objectName(r.object_schema as String, r.object_name as String)} TO \"${r.grantee}\";"
					if (!privileges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withGrant, '"').join(', ')} ON ${objectName(r.object_schema as String, r.object_name as String)} TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'VIEW':
					if (fileNameGrants == null) setWrite('VIEWS', fileNameViews, [schema: r.object_schema, view: r.object_name])
					if (!privileges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withoutGrant, '"').join(', ')} ON ${objectName(r.object_schema as String, r.object_name as String)} TO \"${r.grantee}\";"
					if (!privileges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withGrant, '"').join(', ')} ON ${objectName(r.object_schema as String, r.object_name as String)} TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'PROCEDURE':
					if (fileNameGrants == null) setWrite('SQL_FUNCTIONS', fileNameSQLFunctions, [schema: r.object_schema, sql_function: r.object_name])
					if (!privileges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withoutGrant, '"').join(', ')} ON FUNCTION ${objectName(r.object_schema as String, r.object_name as String)}($r.function_argument_type) TO \"${r.grantee}\";"
					if (!privileges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(privileges.withGrant, '"').join(', ')} ON FUNCTION ${objectName(r.object_schema as String, r.object_name as String)}($r.function_argument_type) TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'ROLE':
					def grantee = (r.grantee as String).toLowerCase()
					def rows = hRoles.rows(where: "Lower(name) = '$grantee'")
					if (!rows.isEmpty()) {
						if (fileNameGrants == null) setWrite('ROLES', fileNameRoles, [role: r.grantee])
						writeln "\nGRANT \"${r.object_name}\" TO \"${r.grantee}\";"
					}
					else {
						rows = hUsers.rows(where: "Lower(user_name) = '$grantee'")
						if (!rows.isEmpty()) {
							if (fileNameGrants == null) setWrite('USERS', fileNameUsers, [user: r.grantee])
							writeln "\nGRANT \"${r.object_name}\" TO \"${r.grantee}\";"
						}
					}
					break
				default:
					new ExceptionGETL("Unknown type object \"${r.object_type}\"")
			}
		}
		Logs.Info("${hGrants.readRows} grants generated")

		hUsers.eachRow(order: ['Lower(user_name)']) { r->
			def defaultRoles = procList(r.default_roles as String)
			if (!defaultRoles.withoutGrant.isEmpty()) {
				setWrite('USERS', fileNameUsers, [user: r.user_name])
				writeln "\nALTER USER \"${r.user_name}\" DEFAULT ROLE ${ListUtils.QuoteList(defaultRoles.withoutGrant, '"').join(', ')};"
			}
		}
		Logs.Info("${hUsers.readRows} user default roles generated")
	}

	List<Map<String, Object>> listPools() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.pools.name} AS name FROM ${hPools.fullNameDataset()} ORDER BY Lower(${sqlObjects.pools.name})").rows()
	}

	List<Map<String, Object>> listRoles() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.roles.name} AS name FROM ${hRoles.fullNameDataset()} ORDER BY Lower(${sqlObjects.roles.name})").rows()
	}

	List<Map<String, Object>> listUsers() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.users.name} AS name FROM ${hUsers.fullNameDataset()} ORDER BY Lower(${sqlObjects.users.name})").rows()
	}

	List<Map<String, Object>> listSchemas() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.schemas.name} AS name FROM ${hSchemas.fullNameDataset()} ORDER BY Lower(${sqlObjects.schemas.name})").rows()
	}

	List<Map<String, Object>> listSequences() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.sequences.schema} AS schema, ${sqlObjects.sequences.name} AS name FROM ${hSequences.fullNameDataset()} ORDER BY Lower(${sqlObjects.sequences.schema}), Lower(${sqlObjects.sequences.name})").rows()
	}

	List<Map<String, Object>> listTables() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.tables.schema} AS schema, ${sqlObjects.tables.name} AS name FROM ${hTables.fullNameDataset()} ORDER BY Lower(${sqlObjects.tables.schema}), Lower(${sqlObjects.tables.name})").rows()
	}

	List<Map<String, Object>> listViews() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.views.schema} AS schema, ${sqlObjects.views.name} AS name FROM ${hViews.fullNameDataset()} ORDER BY Lower(${sqlObjects.views.schema}), Lower(${sqlObjects.views.name})").rows()
	}

	List<Map<String, Object>> listSql_functions() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.sql_functions.schema} AS schema, ${sqlObjects.sql_functions.name} AS name FROM ${hSQLFunctions.fullNameDataset()} ORDER BY Lower(${sqlObjects.sql_functions.schema}), Lower(${sqlObjects.sql_functions.name})").rows()
	}

	List<Map<String, Object>> listGrants() {
		return new QueryDataset(connection: cCache,
				query: "SELECT * FROM (SELECT DISTINCT ${sqlObjects.grants.type} AS type, ${sqlObjects.grants.schema} AS schema, ${sqlObjects.grants.name} AS name FROM ${hGrants.fullNameDataset()}) x ORDER BY type, schema, name").rows()
	}
}