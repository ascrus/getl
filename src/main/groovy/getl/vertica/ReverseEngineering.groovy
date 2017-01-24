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

import getl.data.Dataset
import getl.exception.ExceptionGETL
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.proc.Flow
import getl.proc.Job
import getl.tfs.TDS
import getl.utils.BoolUtils
import getl.utils.Config
import getl.utils.Logs
import getl.utils.StringUtils

/**
 * Reverse engineering database model to sql file as DDL operators
 * @author Aleksey Konstantinov
 */
class ReverseEngineering extends Job {
    VerticaConnection cVertica = new VerticaConnection(config: "vertica")
    def tCurUser = new QueryDataset(connection: cVertica, query: 'SELECT CURRENT_USER')
    def tPools = new QueryDataset(connection: cVertica, query: 'SELECT * FROM getl_pools')
    def tRoles = new QueryDataset(connection: cVertica, query: 'SELECT * FROM getl_roles')
    def tUsers = new QueryDataset(connection: cVertica, query: 'SELECT * FROM getl_users')
    def tSchemas = new QueryDataset(connection: cVertica, query: 'SELECT * FROM getl_schemas')
    def tSequences = new QueryDataset(connection: cVertica, query: 'SELECT * FROM getl_sequences')
    def tTables = new QueryDataset(connection: cVertica, query: 'SELECT * FROM getl_tables')
    def tViews = new QueryDataset(connection: cVertica, query: 'SELECT * FROM getl_views')
    def tGrants = new QueryDataset(connection: cVertica, query: 'SELECT * FROM getl_grants')

    def cCache = new TDS()
    def hPools = new TableDataset(connection: cCache, tableName: 'pools')
    def hRoles = new TableDataset(connection: cCache, tableName: 'roles')
    def hUsers = new TableDataset(connection: cCache, tableName: 'users')
    def hSchemas = new TableDataset(connection: cCache, tableName: 'schemas')
    def hSequences = new TableDataset(connection: cCache, tableName: 'sequences')
    def hTables = new TableDataset(connection: cCache, tableName: 'tables')
    def hViews = new TableDataset(connection: cCache, tableName: 'views')
    def hGrants = new TableDataset(connection: cCache, tableName: 'grants')

    def curUser
    def sqlFile
    def dropFlag

    def sqlPrepare = '''
-- Get pools
CREATE LOCAL TEMPORARY TABLE getl_pools ON COMMIT PRESERVE ROWS AS
SELECT 
    name, memorysize, maxmemorysize, executionparallelism, priority, runtimepriority, runtimeprioritythreshold, 
    queuetimeout::varchar(100), plannedconcurrency, maxconcurrency, runtimecap::varchar(100), cpuaffinityset, cpuaffinitymode, cascadeto
FROM v_catalog.resource_pools
WHERE (NOT is_internal OR name ILIKE 'general') AND {pools}
ORDER BY Lower(name);

-- Get roles
CREATE LOCAL TEMPORARY TABLE getl_roles ON COMMIT PRESERVE ROWS AS
SELECT name
FROM v_catalog.roles
WHERE Lower(name) NOT IN ('pseudosuperuser', 'dbduser', 'public', 'dbadmin') AND {roles}
ORDER BY Lower(name);

-- Get users
CREATE LOCAL TEMPORARY TABLE getl_users ON COMMIT PRESERVE ROWS AS
SELECT user_name, resource_pool, memory_cap_kb, temp_space_cap_kb, run_time_cap, all_roles, default_roles, search_path
FROM v_catalog.users
WHERE  {users}
ORDER BY Lower(user_name);

-- Get schemas
CREATE LOCAL TEMPORARY TABLE getl_schemas ON COMMIT PRESERVE ROWS AS
SELECT schema_name, schema_owner
FROM v_catalog.schemata 
WHERE NOT is_system_schema AND Lower(schema_name) NOT IN ('txtindex') AND {schemas}
ORDER BY Lower(schema_name);

CREATE LOCAL TEMPORARY TABLE getl_sequences ON COMMIT PRESERVE ROWS AS
SELECT sequence_schema, sequence_name, owner_name, identity_table_name, session_cache_count, allow_cycle, output_ordered, increment_by
FROM v_catalog.sequences
WHERE identity_table_name IS NULL AND {sequences}
ORDER BY sequence_schema, sequence_name;

CREATE LOCAL TEMPORARY TABLE getl_tables ON COMMIT PRESERVE ROWS AS
SELECT table_schema, table_name, owner_name, is_temp_table
FROM v_catalog.tables
WHERE {tables}
ORDER BY Lower(table_schema), Lower(table_name);

CREATE LOCAL TEMPORARY TABLE getl_views ON COMMIT PRESERVE ROWS AS
SELECT table_schema, table_name, owner_name, table_id::numeric(38, 0)
FROM v_catalog.views
WHERE NOT is_system_view AND {views}
ORDER BY Lower(table_schema), Lower(table_name);

CREATE LOCAL TEMPORARY TABLE getl_grants ON COMMIT PRESERVE ROWS AS
SELECT grantor, privileges_description, object_type, object_schema, object_name, grantee
FROM v_catalog.grants
WHERE 
    Lower(object_type) NOT IN ('procedure') AND 
    (object_type != 'SCHEMA' OR object_name NOT IN ('v_catalog', 'v_internal', 'v_monitor', 'public', 'v_txtindex')) AND 
    {grants}
ORDER BY object_type, object_schema, object_name, grantor, grantee;
'''

    static main(args) {
        new ReverseEngineering().run(args)
    }

    String stat(String pattern, def value, boolean quote) {
        if (value == null) return ''
        if ((value instanceof String || value instanceof GString) && value == '') return ''
        if (quote) value = '\'' + value + '\''
        return StringUtils.EvalMacroString(pattern, [val: value])
    }

    String stat(String pattern, def value) {
        stat(pattern, value, false)
    }

    String statln(String pattern, def value, boolean quote) {
        if (value == null) return ''
        if ((value instanceof String || value instanceof GString) && value == '') return ''
        if (quote) value = '\'' + value + '\''
        return StringUtils.EvalMacroString(pattern, [val: value]) + '\n'
    }

    String statln(String pattern, def value) {
        statln(pattern, value, false)
    }

    String par(String param, def value, boolean quote) {
        if (value == null) return ''
        if ((value instanceof String || value instanceof GString) && value == '') return ''
        if (quote) value = '\'' + value + '\''
        return param + ' ' + value
    }

    String par(String param, def value) {
        par(param, value, false)
    }

    String parln(String param, def value, boolean quote) {
        if (value == null) return ''
        if ((value instanceof String || value instanceof GString) && value == '') return ''
        if (quote) value = '\'' + value + '\''
        return param + ' ' + value + '\n'
    }

    String parln(String param, def value) {
        parln(param, value, false)
    }

    List<String> procList(String list) {
        def l = list.split(',')
        List<String> n = []
        l.each { String s ->
            s = s.trim()
            if (!s.matches('.*[*]')) n << s
        }
        return n
    }

    List<String> procList(String list, Closure valid) {
        def l = list.split(',')
        List<String> n = []
        l.each { String s ->
            s = s.trim()
            if (valid(s)) n << s
        }
        return n
    }

    String objectName(String schema, String name) {
        return '"' + schema + '"."' + name + '"'
    }

    String ddl(String schema, String object) {
        def name = schema + '.' + object
        def qExportObjects = new QueryDataset(connection: cVertica, query: "SELECT EXPORT_OBJECTS('', '$name', false)")
        def r = qExportObjects.rows()
        assert r.size() == 1, "Object \"$name\" not found"
        String s = r[0].export_objects.replace('\r', '')
        return s.trim()
    }

    Map<String, String> ddlTable(String schema, String object) {
        def sql = ddl(schema, object)
        StringBuilder create = new StringBuilder()
        StringBuilder alter = new StringBuilder()
        sql.eachLine { String l ->
            if (l.trim() == '\n') return
            if (l.matches('(?i)ALTER TABLE .+')) {
                if (l.matches('(?i)ALTER TABLE .+ ADD CONSTRAINT .+ PRIMARY KEY [(].+[)];.*')) {
                    create << l + '\n'
                }
                else {
                    alter << l + '\n'
                }
            }
            else {
                create << l + '\n'
            }
        }
        return [create: create.toString(), alter: alter.toString()]
    }

    void readCurUser() {
        def r = tCurUser.rows()
        curUser = r[0].current_user
        Logs.Info("Current user $curUser")
    }

    @Override
    void process() {
        Logs.Info("### Reverse engineering Vertica tool")

        if (jobArgs.sqlfile == null) {
            Logs.Severe('Required argument "sqlfile"')
            return
        }
        sqlFile = new File(jobArgs.sqlfile).newPrintWriter()

        readCurUser()

        // Read drop flags
        dropFlag = Config.content.drop?:[:]

        // Init default wheres by objects
        def default_where = 'false'
        def pools_where = default_where
        def roles_where = default_where
        def users_where = default_where
        def schemas_where = default_where
        def sequences_where = default_where
        def tables_where = default_where
        def views_where = default_where
        def grants_where = default_where

        if (Config.content.pools) pools_where = Config.content.pools; Logs.Info("Reverse pools: $pools_where")
        if (Config.content.roles) roles_where = Config.content.roles; Logs.Info("Reverse roles: $roles_where")
        if (Config.content.users) users_where = Config.content.users; Logs.Info("Reverse users: $users_where")
        if (Config.content.schemas) schemas_where = Config.content.schemas; Logs.Info("Reverse schemas: $schemas_where")
        if (Config.content.sequences) sequences_where = Config.content.sequences; Logs.Info("Reverse sequences: $sequences_where")
        if (Config.content.tables) tables_where = Config.content.tables; Logs.Info("Reverse tables: $tables_where")
        if (Config.content.views) views_where = Config.content.views; Logs.Info("Reverse views: $views_where")
        if (Config.content.grants) grants_where = Config.content.grants; Logs.Info("Reverse grants: $grants_where")

        Logs.Fine("Prepared structures ...")
        cVertica.executeCommand(command: sqlPrepare,
                queryParams: [pools: pools_where, roles: roles_where, users: users_where,
                        schemas: schemas_where, sequences: sequences_where,
                        tables: tables_where, views: views_where, grants: grants_where])

        Logs.Fine("Read object model ...")
        def count = 0

        count = new Flow().copy(source: tPools, dest: hPools, inheritFields: true, createDest: true)
        Logs.Fine("$count pools found")

        count = new Flow().copy(source: tRoles, dest: hRoles, inheritFields: true, createDest: true)
        Logs.Fine("$count roles found")

        count = new Flow().copy(source: tUsers, dest: hUsers, inheritFields: true, createDest: true)
        Logs.Fine("$count users found")

        count = new Flow().copy(source: tSchemas, dest: hSchemas, inheritFields: true, createDest: true)
        Logs.Fine("$count schemas found")

        count = new Flow().copy(source: tSequences, dest: hSequences, inheritFields: true, createDest: true)
        Logs.Fine("$count sequences found")

        count = new Flow().copy(source: tTables, dest: hTables, inheritFields: true, createDest: true)
        Logs.Fine("$count tables found")

        count = new Flow().copy(source: tViews, dest: hViews, inheritFields: true, createDest: true)
        Logs.Fine("$count views found")

        count = new Flow().copy(source: tGrants, dest: hGrants, inheritFields: true, createDest: true)
        Logs.Fine("$count grants found")\

        genPools()
        genRoles()
        genUsers()
        genSchemas()
        genSequences()
        genTables()
        genViews()
        genGrants()

        sqlFile.close()
    }

    void genPools() {
        sqlFile.println "/***** BEGIN POOLS *****/"
        hPools.eachRow(order: ['CASE name WHEN \'general\' THEN 0 ELSE 1 END', 'Lower(name)']) { Map r ->
            sqlFile.println ''
            if (r.name != 'general') {
                if (BoolUtils.IsValue(dropFlag.pools)) {
                    sqlFile.println "DROP RESOURCE POOL ${r.name};"
                }
                sqlFile.println "CREATE RESOURCE POOL ${r.name}"
                sqlFile.print parln('MEMORYSIZE', r.memorysize, true)
                sqlFile.print parln('MAXMEMORYSIZE', r.maxmemorysize, true)
            }
            else {
                sqlFile.println "ALTER RESOURCE POOL ${r.name}"
            }
            sqlFile.print parln('EXECUTIONPARALLELISM', r.executionparallelism)
            sqlFile.print parln('PRIORITY', r.priority)
            sqlFile.print parln('RUNTIMEPRIORITY', r.runtimepriority)
            sqlFile.print parln('RUNTIMEPRIORITYTHRESHOLD', r.runtimeprioritythreshold)
            sqlFile.print parln('QUEUETIMEOUT', r.queuetimeout, true)
            sqlFile.print parln('PLANNEDCONCURRENCY', r.plannedconcurrency)
            sqlFile.print parln('MAXCONCURRENCY', r.maxconcurrency)
            sqlFile.print parln('RUNTIMECAP', r.runtimecap, true)
            sqlFile.print parln('CPUAFFINITYSET', r.cpuaffinityset)
            sqlFile.print parln('CPUAFFINITYMODE', r.cpuaffinitymode)
            sqlFile.print parln('CASCADE TO', r.cascadeto, true)
            sqlFile.println ";"
        }
        sqlFile.println "\n/***** FINISH POOLS *****/\n\n"
        Logs.Info("${hPools.readRows} pools generated")
    }

    void genRoles() {
        sqlFile.println "/***** BEGIN ROLES *****/\n"
        hRoles.eachRow(order: ['Lower(name)']) { Map r ->
            if (BoolUtils.IsValue(dropFlag.roles)) {
                sqlFile.println "DROP ROLE ${r.name} CASCADE;"
            }
            sqlFile.println "CREATE ROLE ${r.name};"
        }
        sqlFile.println "\n/***** FINISH ROLES *****/\n\n"
        Logs.Info("${hRoles.readRows} roles generated")
    }

    void genUsers() {
        sqlFile.println "/***** BEGIN USERS *****/"
        hUsers.eachRow(order: ['Lower(user_name)']) { Map r ->
            sqlFile.println ''
            if (r.user_name != 'dbadmin') {
                if (BoolUtils.IsValue(dropFlag.users)) {
                    sqlFile.println "DROP USER ${r.user_name} CASCADE;"
                }
                sqlFile.println "CREATE USER ${r.user_name}"
            }
            else {
                sqlFile.println "ALTER USER ${r.user_name}"
            }
            sqlFile.print parln('RESOURCE POOL', r.resource_pool)
            sqlFile.print statln('MEMORYCAP {val}', (r.memory_cap_kb == 'unlimited')?null:(r.memory_cap_kb + 'K'), true)
            sqlFile.print statln('TEMPSPACECAP {val}', (r.temp_space_cap_kb == 'unlimited')?null:(r.temp_space_cap_kb + 'K'), true)
            sqlFile.print parln('RUNTIMECAP', (r.run_time_cap == 'unlimited')?null:r.run_time_cap, true)
            sqlFile.println ";"

            def userRoles = procList(r.all_roles)
            if (!userRoles.isEmpty()) {
                sqlFile.println "GRANT ${userRoles.join(', ')} TO ${r.user_name};"
            }

            def defaultRoles = procList(r.default_roles)
            if (!defaultRoles.isEmpty()) {
                sqlFile.println "ALTER USER ${r.user_name} DEFAULT ROLE ${defaultRoles.join(', ')};"
            }
        }
        sqlFile.println "\n/***** FINISH USERS *****/\n\n"
        Logs.Info("${hUsers.readRows} users generated")
    }

    void genSchemas() {
        sqlFile.println "/***** BEGIN SCHEMAS *****/\n"
        hSchemas.eachRow(order: ['Lower(schema_name)']) { Map r ->
            if (r.schema_name == 'public') return
            if (BoolUtils.IsValue(dropFlag.schemas)) {
                sqlFile.println "DROP SCHEMA ${r.schema_name} CASCADE;"
            }
            sqlFile.println "CREATE SCHEMA ${r.schema_name} AUTHORIZATION ${r.schema_owner};\n"
        }
        hUsers.eachRow(order: ['Lower(user_name)']) { Map r ->
            def p = procList(r.search_path, { return !(it in ['v_catalog', 'v_monitor', 'v_internal', 'public', '"$user"', ''])})
            if (p.isEmpty()) return
            sqlFile.println "ALTER USER ${r.user_name} SEARCH_PATH ${p.join(', ')};"
        }

        sqlFile.println "\n/***** FINISH SCHEMAS *****/\n\n"
        Logs.Info("${hSchemas.readRows} schemas generated")
    }

    void genSequences() {
        sqlFile.println "/***** BEGIN SEQUENCES *****/"
        hSequences.eachRow(order: ['Lower(sequence_schema)', 'Lower(sequence_name)']) { Map r ->
            def name = objectName(r.sequence_schema, r.sequence_name)
            if (BoolUtils.IsValue(dropFlag.sequences)) {
                sqlFile.println "DROP SEQUENCE $name;"
            }
            sqlFile.println '\n' + ddl(r.sequence_schema, r.sequence_name)
            if (r.owner_name != curUser) sqlFile.println "ALTER SEQUENCE $name OWNER TO ${r.owner_name};"
        }
        sqlFile.println "\n/***** FINISH SEQUENCES *****/\n\n"
        Logs.Info("${hSequences.readRows} sequences generated")
    }

    void genTables() {
        sqlFile.println "/***** BEGIN TABLES *****/"
        StringBuilder alter = new StringBuilder()
        hTables.eachRow(order: ['Lower(table_schema)', 'Lower(table_name)']) { Map r ->
            def name = objectName(r.table_schema, r.table_name)
            if (BoolUtils.IsValue(dropFlag.tables)) {
                sqlFile.println "DROP TABLE IF EXISTS $name CASCADE;"
            }
            def stat = ddlTable(r.table_schema, r.table_name)
            sqlFile.println '\n' + stat.create
            if (stat.alter != '') alter << stat.alter
            if (r.owner_name != curUser) sqlFile.println "ALTER TABLE $name OWNER TO ${r.owner_name};"
        }
        if (alter.length() > 0) {
            sqlFile.println ''
            sqlFile.println alter.toString()
        }
        sqlFile.println "\n/***** FINISH TABLES *****/\n\n"
        Logs.Info("${hTables.readRows} tables generated")
    }

    void genViews() {
        sqlFile.println "/***** BEGIN VIEWS *****/"
        hViews.eachRow(order: ['table_id']) { Map r ->
            def name = objectName(r.table_schema, r.table_name)
            def sql = ddl(r.table_schema, r.table_name)
            if (BoolUtils.IsValue(dropFlag.views)) {
                def i = sql.indexOf(' VIEW')
                sql = 'CREATE OR REPLACE' + sql.substring(i)
            }
            sqlFile.println '\n' + sql
            if (r.owner_name != curUser) sqlFile.println "ALTER VIEW $name OWNER TO ${r.owner_name};"
        }
        sqlFile.println "\n/***** FINISH VIEWS *****/\n\n"
        Logs.Info("${hViews.readRows} views generated")
    }

    void genGrants() {
        sqlFile.println "/***** BEGIN GRANTS *****/"
        hGrants.eachRow(order: ['Lower(object_type)', 'Lower(object_schema)', 'Lower(object_name)', 'Lower(grantee)']) { Map r ->
            def priveleges = procList(r.privileges_description)
            if (priveleges.isEmpty()) return
            switch (r.object_type) {
                case 'RESOURCEPOOL':
                    sqlFile.println "\nGRANT ${priveleges.join(', ')} ON RESOURCE POOL ${r.object_name} TO ${r.grantee};"
                    break
                case 'SCHEMA':
                    sqlFile.println "\nGRANT ${priveleges.join(', ')} ON SCHEMA ${r.object_name} TO ${r.grantee};"
                    break
                case 'SEQUENCE':
                    sqlFile.println "\nGRANT ${priveleges.join(', ')} ON SEQUENCE ${objectName(r.object_schema, r.object_name)} TO ${r.grantee};"
                    break
                case 'TABLE': case 'VIEW':
                    sqlFile.println "\nGRANT ${priveleges.join(', ')} ON ${objectName(r.object_schema, r.object_name)} TO ${r.grantee};"
                    break
                default:
                    new ExceptionGETL("Unknown type object \"${r.object_type}\"")
            }
        }
        sqlFile.println "\n/***** FINISH GRANTS *****/\n\n"
        Logs.Info("${hGrants.readRows} grants generated")

        def grantPools = new QueryDataset(connection: cCache,
                query: "SELECT privileges_description, object_name, grantee FROM grants WHERE object_type = 'RESOURCEPOOL' ORDER BY Lower(object_name), Lower(grantee)")
    }

}