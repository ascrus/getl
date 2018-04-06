/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) 2013-2017  Alexsey Konstantonov (ASCRUS)

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
import getl.utils.ConvertUtils
import getl.utils.FileUtils
import getl.utils.GenerationUtils
import getl.utils.ListUtils
import getl.utils.Logs
import getl.utils.StringUtils

import java.util.regex.Matcher

/**
 * Reverse engineering database model to sql file as DDL operators
 * @author Aleksey Konstantinov
 */
class ReverseEngineering extends Job {
	VerticaConnection cVertica = new VerticaConnection(config: "vertica")
	def tCurUser = new QueryDataset(connection: cVertica, query: 'SELECT CURRENT_USER')
	def tPools = new TableDataset(connection: cVertica, schemaName: 'v_temp_schema', tableName: 'getl_pools')
	def tRoles = new TableDataset(connection: cVertica, schemaName: 'v_temp_schema', tableName: 'getl_roles')
	def tUsers = new TableDataset(connection: cVertica, schemaName: 'v_temp_schema', tableName: 'getl_users')
	def tSchemas = new TableDataset(connection: cVertica, schemaName: 'v_temp_schema', tableName: 'getl_schemas')
	def tSequences = new TableDataset(connection: cVertica, schemaName: 'v_temp_schema', tableName: 'getl_sequences')
	def tTables = new TableDataset(connection: cVertica, schemaName: 'v_temp_schema', tableName: 'getl_tables')
	def tViews = new TableDataset(connection: cVertica, schemaName: 'v_temp_schema', tableName: 'getl_views')
	def tSQLFunctions = new TableDataset(connection: cVertica, schemaName: 'v_temp_schema', tableName: 'getl_sql_functions')
	def tGrants = new TableDataset(connection: cVertica, schemaName: 'v_temp_schema', tableName: 'getl_grants')

	def cCache = new TDS()
	def hFiles = new TableDataset(connection: cCache, tableName: 'files', field: [new Field(name: 'filename', length: 1024, isKey: true)])
	def hPools = new TableDataset(connection: cCache, tableName: 'pools')
	def hRoles = new TableDataset(connection: cCache, tableName: 'roles')
	def hUsers = new TableDataset(connection: cCache, tableName: 'users')
	def hSchemas = new TableDataset(connection: cCache, tableName: 'schemas')
	def hSequences = new TableDataset(connection: cCache, tableName: 'sequences')
	def hTables = new TableDataset(connection: cCache, tableName: 'tables')
	def hViews = new TableDataset(connection: cCache, tableName: 'views')
	def hSQLFunctions = new TableDataset(connection: cCache, tableName: 'sql_functions')
	def hGrants = new TableDataset(connection: cCache, tableName: 'grants')

	def statFilesFind = 'SELECT FILENAME FROM FILES WHERE FILENAME = ?'
	def statFilesInsert = 'INSERT INTO FILES (FILENAME) VALUES (?)'

	String scriptPath
	String curUser
	Map sectionCreate
	Map sectionFileName
	Map sectionDrop
	String fileNamePools
	String fileNameRoles
	String fileNameUsers
	String fileNameSchemas
	String fileNameSequences
	String fileNameTables
	String fileNameViews
	String fileNameSQLFunctions
	String fileNameGrants
	def poolEmpty
	def userEmpty
	def sequenceCurrent
	def tableConstraints
	def projectionTables
	def projectionKsafe
	def projectionAnalyzeSuper
	def columnComment

	final def sqlObjects = [
		pools: [table: 'v_catalog.resource_pools', name: 'NAME',
				where: '''(NOT is_internal OR name ILIKE 'general')'''],

		roles: [table: 'v_catalog.roles', name: 'NAME',
				where: '''Lower(name) NOT IN ('pseudosuperuser', 'dbduser', 'public', 'dbadmin', 'sysmonitor')'''],

		users: [table: 'v_catalog.users', name: 'USER_NAME', where: '1 = 1'],

		schemas: [table: 'v_catalog.schemata', name: 'SCHEMA_NAME',
				  where: '''NOT is_system_schema AND Lower(schema_name) NOT IN ('txtindex', 'v_idol', 'v_txtindex', 'v_temp_schema')'''],

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

	final def sqlPrepare = """
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
SELECT name
FROM ${sqlObjects.roles.table}
WHERE 
	${sqlObjects.roles.where}
ORDER BY Lower(name);

-- Get users
CREATE LOCAL TEMPORARY TABLE getl_users ON COMMIT PRESERVE ROWS AS
SELECT user_name, resource_pool, memory_cap_kb, temp_space_cap_kb, run_time_cap, all_roles, default_roles, search_path
FROM ${sqlObjects.users.table}
WHERE 
	${sqlObjects.users.where}
ORDER BY Lower(user_name);

-- Get schemas
CREATE LOCAL TEMPORARY TABLE getl_schemas ON COMMIT PRESERVE ROWS AS
SELECT schema_name, schema_owner
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
SELECT table_schema, table_name, owner_name, is_temp_table
FROM ${sqlObjects.tables.table}
WHERE 
	${sqlObjects.tables.where}
ORDER BY Lower(table_schema), Lower(table_name);

CREATE LOCAL TEMPORARY TABLE getl_views ON COMMIT PRESERVE ROWS AS
SELECT table_schema, table_name, owner_name, table_id::numeric(38, 0)
FROM ${sqlObjects.views.table}
WHERE 
	${sqlObjects.views.where}
ORDER BY Lower(table_schema), Lower(table_name);

CREATE LOCAL TEMPORARY TABLE getl_sql_functions ON COMMIT PRESERVE ROWS AS
SELECT schema_name, function_name, owner
FROM ${sqlObjects.sql_functions.table}
WHERE 
	${sqlObjects.sql_functions.where}
ORDER BY schema_name, function_name;

CREATE LOCAL TEMPORARY TABLE getl_grants ON COMMIT PRESERVE ROWS AS
SELECT grantor, privileges_description, object_type, object_schema, object_name, grantee, f.function_argument_type
FROM ${sqlObjects.grants.table}
WHERE 
	${sqlObjects.grants.where}
ORDER BY object_type, Lower(object_schema), Lower(object_name), Lower(grantor), Lower(grantee);
"""

	static main(args) {
		new ReverseEngineering().run(args)
	}

	/**
	 * Build reverse statement
	 * @param pattern
	 * @param value
	 * @param quote
	 * @return
	 */
	String stat(String pattern, def value, boolean quote) {
		if (value == null) return ''
		if ((value instanceof String || value instanceof GString) && value == '') return ''
		if (quote) value = '\'' + value + '\''
		return StringUtils.EvalMacroString(pattern, [val: value])
	}

	/**
	 * Build reverse statement
	 * @param pattern
	 * @param value
	 * @return
	 */
	String stat(String pattern, def value) {
		stat(pattern, value, false)
	}

	/**
	 * Build reverse statement with feed line
	 */
	String statln(String pattern, def value, boolean quote) {
		if (value == null) return ''
		if ((value instanceof String || value instanceof GString) && value == '') return ''
		if (quote) value = '\'' + value + '\''
		return StringUtils.EvalMacroString(pattern, [val: value]) + '\n'
	}

	/**
	 * Build reverse statement with feed line
	 * @param pattern
	 * @param value
	 * @return
	 */
	String statln(String pattern, def value) {
		statln(pattern, value, false)
	}

	/**
	 * Build parameter for statement
	 * @param param
	 * @param value
	 * @param quote
	 * @return
	 */
	String par(String param, def value, boolean quote) {
		if (value == null) return ''
		if ((value instanceof String || value instanceof GString) && value == '') return ''
		if (quote) value = '\'' + value + '\''
		return param + ' ' + value
	}

	/**
	 * Build parameter for statement
	 * @param param
	 * @param value
	 * @return
	 */
	String par(String param, def value) {
		par(param, value, false)
	}

	/**
	 * Build parameter for statement with feed line
	 * @param param
	 * @param value
	 * @param quote
	 * @return
	 */
	String parln(String param, def value, boolean quote) {
		if (value == null) return ''
		if ((value instanceof String || value instanceof GString) && value == '') return ''
		if (quote) value = '\'' + value + '\''
		return param + ' ' + value + '\n'
	}

	/**
	 * Build parameter for statement with feed line
	 * @param param
	 * @param value
	 * @return
	 */
	String parln(String param, def value) {
		parln(param, value, false)
	}

	/**
	 * Convert string to list
	 * @param list
	 * @return
	 */
	/**
	 * @param list
	 * @return
	 */
	Map<String, List<String>> procList(String list, Closure valid) {
		def res = (HashMap<String, ArrayList<String>>)[:]

		def withoutGrant = [] as ArrayList<String>
		def withGrant = [] as ArrayList<String>

		list = list.trim()
		if (list != '') {

			def l = list.split(',')
			List<String> n = []
			l.each { String s ->
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
	 * Convert string to list
	 * @param list
	 * @param valid
	 * @return
	 */
	Map<String, List<String>> procList(String list) {
		return procList(list, null)
	}

	/**
	 * Generate full name database objects
	 * @param schema
	 * @param name
	 * @return
	 */
	String objectName(String schema, String name) {
		return '"' + schema + '"."' + name + '"'
	}

	/**
	 * Read DDL from database objects
	 * @param schema
	 * @param object
	 * @return
	 */
	String ddl(String schema, String object) {
		def name = schema + '.' + object
		def qExportObjects = new QueryDataset(connection: cVertica, query: "SELECT EXPORT_OBJECTS('', '$name', false)")
		def r = qExportObjects.rows()
		assert r.size() == 1, "Object \"$name\" not found"
		String s = r[0].export_objects.replace('\r', '')
		return s.trim()
	}

	/**
	 * Generate DDL from table objects
	 * @param schema
	 * @param object
	 * @return
	 */
	Map<String, String> ddlTable(String schema, String object) {
		def sql = ddl(schema, object)
		StringBuilder create = new StringBuilder()
		StringBuilder alter = new StringBuilder()
		def finishAnalyze = false
		def startProj = false
		sql.eachLine { String line ->
			if (finishAnalyze || line.trim() == '\n') return

			Matcher projMatcher
			if (!projectionTables || projectionAnalyzeSuper || projectionKsafe != null) {
				projMatcher = line =~ /(?i)CREATE PROJECTION (.+)[.](.+)/

				if (!projectionTables && projMatcher.count == 1) {
					finishAnalyze = true
					return
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
					def projSchema = projMatcher[0][1] as String
					def projName = projMatcher[0][2] as String
					if (projName == object && !projName.matches('.+ \\/[*][+].+[*]\\/')) {
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
					def ksafe = Integer.valueOf(m[0][1] as String)
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

	/**
	 * Read cur user information
	 */
	void readCurUser() {
		def r = tCurUser.rows()
		curUser = r[0].current_user
		Logs.Info("Current user $curUser")
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
	def currentObject

	/**
	 * Current file mask for write
	 */
	def currentFileMask = ''

	/**
	 * Current parameters for write
	 */
	def currentVars = [:]

	/**
	 * File has write data
	 */
	def isWriteln = false

	void setWrite(String object, String filemask, def Map vars = [:]) {
		assert object != null
		currentObject = object

		assert filemask != null, "Required file mask for \"$currentObject\" object"
		currentFileMask = filemask

		currentVars = vars

		isWriteln = false
	}

	void write(String script) {
		def v = [:] as Map<String, String>
		currentVars.each { String var, String value ->
			v.put(var, value.replace('*', '_').replace('?', '_').replace('/', '_').replace('\\', '_'))
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

	String eval(String val) {
		return GenerationUtils.EvalGroovyScript('"""' + val.replace('\\', '\\\\').replace('"', '\\"') + '"""', Config.vars + ((Job.jobArgs.vars?:[:]) as Map<String, Object>))
	}

	/**
	 * Init reverse objects
	 */
	public void initReverse() {
		readCurUser()

		// Read drop parameters section
		sectionDrop = (Map)Config.content.drop?:[:]

		// Read create parameters section
		sectionCreate = (Map)Config.content.create?:[:]

		// Read file name parameters section
		sectionFileName = (Map)Config.content.filename?:[:]
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
		projectionKsafe = (sectionCreate.projection_ksafe != null)?Integer.valueOf(sectionCreate.projection_ksafe):null
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
		def aggr_projections_where = default_where
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

		Logs.Fine("Prepared structures ...")
		initFiles()

		cVertica.executeCommand(command: sqlPrepare)

		Logs.Fine("Read object model ...")
		def count = 0

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
	public void doneReverse() {
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

	/**
	 * Process job
	 */
	@Override
	void process() {
		Logs.Info("### Reverse engineering Vertica tool, version 1.0, EasyData company (www.easydata.ru)")
		if (!jobArgs.containsKey('list') && !jobArgs.containsKey('script_path')) {
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

		try {
			initReverse()
		}
		catch (Exception e) {
			doneReverse()
			throw e
		}

		if (jobArgs.list != null) {
			def l = jobArgs.list.toLowerCase()
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

		if (jobArgs.containsKey('script_path')) {
			if (jobArgs.script_path == null) {
				Logs.Severe('Required value from parameter "script_path"')
				doneReverse()
				return
			}
		}
		else {
			doneReverse()
			return
		}

		scriptPath = FileUtils.ConvertToDefaultOSPath(jobArgs.script_path)
		Logs.Info("Write script to \"$scriptPath\" directory")
		FileUtils.ValidPath(scriptPath)

		if (BoolUtils.IsValue(jobArgs.clear)) {
			Logs.Info("Clearing the destination directory \"$scriptPath\"")
			if (!FileUtils.DeleteFolder(jobArgs.script_path, false)) {
				Logs.Severe("Can not clearing destination directory \"$scriptPath\"")
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
	void genUsers() {
		hUsers.eachRow(order: ['Lower(user_name)']) { Map r ->
			setWrite('USERS', fileNameUsers, [user: r.user_name])

			if (r.user_name.toLowerCase() != 'dbadmin') {
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
			   write statln('MEMORYCAP {val}', (r.memory_cap_kb == 'unlimited') ? null : (r.memory_cap_kb + 'K'), true)
			   write statln('TEMPSPACECAP {val}', (r.temp_space_cap_kb == 'unlimited') ? null : (r.temp_space_cap_kb + 'K'), true)
			   write parln('RUNTIMECAP', (r.run_time_cap == 'unlimited') ? null : r.run_time_cap, true)
			}
		   if (isWriteln) writeln ";"

			def defaultRoles = procList(r.default_roles)
			if (!defaultRoles.withoutGrant.isEmpty()) {
			   writeln "\nALTER USER \"${r.user_name}\" DEFAULT ROLE ${ListUtils.QuoteList(defaultRoles.withoutGrant, '"').join(', ')};"
			}

			if (isWriteln) writeln ''
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
		   writeln "CREATE SCHEMA \"${r.schema_name}\" AUTHORIZATION ${r.schema_owner};\n"
		}
		if (isWriteln) writeln ''
		hUsers.eachRow(order: ['Lower(user_name)']) { Map r ->
			def p = procList(r.search_path, { return !(it in ['v_catalog', 'v_monitor', 'v_internal', 'public', '"$user"', ''])})
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

			def name = objectName(r.sequence_schema, r.sequence_name)
			if (BoolUtils.IsValue(sectionDrop.sequences)) {
			   writeln "DROP SEQUENCE \"$name\";"
			}
		   writeln ddl(r.sequence_schema, r.sequence_name)
			if (sequenceCurrent && r.current_value != null && r.current_value > 0)writeln "ALTER SEQUENCE $name RESTART WITH ${r.current_value};"
			if (r.owner_name != curUser)writeln "\nALTER SEQUENCE \"$name\" OWNER TO ${r.owner_name};"
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

			def name = objectName(r.table_schema, r.table_name)
			if (BoolUtils.IsValue(sectionDrop.tables)) {
			   writeln "\nDROP TABLE IF EXISTS $name CASCADE;"
			}
			def stat = ddlTable(r.table_schema, r.table_name)
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

			def name = objectName(r.table_schema, r.table_name)
			def sql = ddl(r.table_schema, r.table_name)
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

			def name = objectName(r.schema_name, r.function_name)
			def sql = ddl(r.schema_name, r.function_name)
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
	void genGrants() {
		hGrants.eachRow(order: ['Lower(object_type)', 'Lower(object_schema)', 'Lower(object_name)', 'Lower(grantee)', 'Lower(function_argument_type)', 'Lower(privileges_description)']) { Map r ->
			if (fileNameGrants != null) setWrite('GRANTS', fileNameGrants)

			def priveleges = procList(r.privileges_description)
			switch (r.object_type) {
				case 'RESOURCEPOOL':
					if (fileNameGrants == null) setWrite('POOLS', fileNamePools, [pool: r.object_name])
					if (!priveleges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withoutGrant, '"').join(', ')} ON RESOURCE POOL \"${r.object_name}\" TO \"${r.grantee}\";"
					if (!priveleges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withGrant, '"').join(', ')} ON RESOURCE POOL \"${r.object_name}\" TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'SCHEMA':
					if (fileNameGrants == null) setWrite('SCHEMAS', fileNameSchemas, [schema: r.object_name])
					if (!priveleges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withoutGrant, '"').join(', ')} ON SCHEMA \"${r.object_name}\" TO \"${r.grantee}\";"
					if (!priveleges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withGrant, '"').join(', ')} ON SCHEMA \"${r.object_name}\" TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'SEQUENCE':
					if (fileNameGrants == null) setWrite('SEQUENCES', fileNameSequences, [schema: r.object_schema, sequence: r.object_name])
					if (!priveleges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withoutGrant, '"').join(', ')} ON SEQUENCE ${objectName(r.object_schema, r.object_name)} TO \"${r.grantee}\";"
					if (!priveleges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withGrant, '"').join(', ')} ON SEQUENCE ${objectName(r.object_schema, r.object_name)} TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'TABLE':
					if (fileNameGrants == null) setWrite('TABLES', fileNameTables, [schema: r.object_schema, table: r.object_name])
					if (!priveleges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withoutGrant, '"').join(', ')} ON ${objectName(r.object_schema, r.object_name)} TO \"${r.grantee}\";"
					if (!priveleges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withGrant, '"').join(', ')} ON ${objectName(r.object_schema, r.object_name)} TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'VIEW':
					if (fileNameGrants == null) setWrite('VIEWS', fileNameViews, [schema: r.object_schema, view: r.object_name])
					if (!priveleges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withoutGrant, '"').join(', ')} ON ${objectName(r.object_schema, r.object_name)} TO \"${r.grantee}\";"
					if (!priveleges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withGrant, '"').join(', ')} ON ${objectName(r.object_schema, r.object_name)} TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'PROCEDURE':
					if (fileNameGrants == null) setWrite('SQL_FUNCTIONS', fileNameSQLFunctions, [schema: r.object_schema, sql_function: r.object_name])
					if (!priveleges.withoutGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withoutGrant, '"').join(', ')} ON FUNCTION ${objectName(r.object_schema, r.object_name)}($r.function_argument_type) TO \"${r.grantee}\";"
					if (!priveleges.withGrant.isEmpty()) writeln "\nGRANT ${ListUtils.QuoteList(priveleges.withGrant, '"').join(', ')} ON FUNCTION ${objectName(r.object_schema, r.object_name)}($r.function_argument_type) TO \"${r.grantee}\" WITH GRANT OPTION;"
					break
				case 'ROLE':
					def grantee = r.grantee.toLowerCase()
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
	}

	public List<Map<String, Object>> listPools() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.pools.name} AS name FROM ${hPools.fullNameDataset()} ORDER BY Lower(${sqlObjects.pools.name})").rows()
	}

	public List<Map<String, Object>> listRoles() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.roles.name} AS name FROM ${hRoles.fullNameDataset()} ORDER BY Lower(${sqlObjects.roles.name})").rows()
	}

	public List<Map<String, Object>> listUsers() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.users.name} AS name FROM ${hUsers.fullNameDataset()} ORDER BY Lower(${sqlObjects.users.name})").rows()
	}

	public List<Map<String, Object>> listSchemas() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.schemas.name} AS name FROM ${hSchemas.fullNameDataset()} ORDER BY Lower(${sqlObjects.schemas.name})").rows()
	}

	public List<Map<String, Object>> listSequences() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.sequences.schema} AS schema, ${sqlObjects.sequences.name} AS name FROM ${hSequences.fullNameDataset()} ORDER BY Lower(${sqlObjects.sequences.schema}), Lower(${sqlObjects.sequences.name})").rows()
	}

	public List<Map<String, Object>> listTables() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.tables.schema} AS schema, ${sqlObjects.tables.name} AS name FROM ${hTables.fullNameDataset()} ORDER BY Lower(${sqlObjects.tables.schema}), Lower(${sqlObjects.tables.name})").rows()
	}

	public List<Map<String, Object>> listViews() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.views.schema} AS schema, ${sqlObjects.views.name} AS name FROM ${hViews.fullNameDataset()} ORDER BY Lower(${sqlObjects.views.schema}), Lower(${sqlObjects.views.name})").rows()
	}

	public List<Map<String, Object>> listSql_functions() {
		return new QueryDataset(connection: cCache,
				query: "SELECT ${sqlObjects.sql_functions.schema} AS schema, ${sqlObjects.sql_functions.name} AS name FROM ${hSQLFunctions.fullNameDataset()} ORDER BY Lower(${sqlObjects.sql_functions.schema}), Lower(${sqlObjects.sql_functions.name})").rows()
	}

	public List<Map<String, Object>> listGrants() {
		return new QueryDataset(connection: cCache,
				query: "SELECT * FROM (SELECT DISTINCT ${sqlObjects.grants.type} AS type, ${sqlObjects.grants.schema} AS schema, ${sqlObjects.grants.name} AS name FROM ${hGrants.fullNameDataset()}) x ORDER BY type, schema, name").rows()
	}
}