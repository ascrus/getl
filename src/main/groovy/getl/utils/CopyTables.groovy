package getl.utils

import com.sun.org.apache.xpath.internal.operations.Bool
import getl.data.Field
import getl.jdbc.JDBCConnection
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.proc.Executor
import getl.proc.Flow
import getl.proc.Job
import getl.stat.ProcessTime
import getl.tfs.TDS

/**
 * Copyes tables data from source to destination
 * @author Aleksey Konstantinov
 */
class CopyTables extends Job {
    static main(args) {
        new CopyTables().run(args)
    }

    @Override
    void process() {
        Logs.Info("### Copy data tables tool")

        def source = JDBCConnection.CreateConnection(config: 'source')
        assert source instanceof JDBCConnection, "Allow only JDBC source connection"

        def dest = JDBCConnection.CreateConnection(config: 'destination')
        assert dest instanceof JDBCConnection, "Allow only JDBC destination connection"

        source.connected = true
        Logs.Fine('Testing the source connection completed')

        dest.connected = true
        Logs.Fine('Testing the destination connection completed')

        def tables = TDS.dataset()
        tables.field << new Field(name: 'schema_name', length: 1024, isKey: true)
        tables.field << new Field(name: 'table_name', length: 1024, isKey: true)
        tables.create()

        if (Config.content.tables == null) {
            Logs.Warning('Tables section required')
            return
        }

        def copyes = (Config.content.tables as Map<String, String>)
        def rule = new QueryDataset(connection: source)
        def totalCount = 0
        copyes.each { name, sql ->
            if (name.substring(0, 1) == '_') {
                Logs.Warning("Skip rule \"$name\"")
                return
            }
            Logs.Fine("Read rule \"$name\" ...")
            rule.query = sql
            def count = new Flow().copy(source: rule, dest: tables)
            Logs.Info("Found $count tables for copy from \"$name\" rule")
            totalCount += count
        }
        Logs.Info("Found $totalCount total tables for copy")

        Logs.Fine("Checking source tables ...")
        tables.eachRow { Map r ->
            def t = new TableDataset(connection: source, schemaName: r.schema_name, tableName: r.table_name)
            assert t.exists, "Source table ${t.fullNameDataset()} not found"
        }

        Logs.Fine("Checking destination tables ...")
        tables.eachRow { Map r ->
            def t = new TableDataset(connection: dest, schemaName: r.schema_name, tableName: r.table_name)
            assert t.exists, "Destination table ${t.fullNameDataset()} not found"
        }

        source.connected = false
        dest.connected = false

        def abortOnError = BoolUtils.IsValue(Config.content.abortOnError, false)
        if (abortOnError) {
            Logs.Info("Stopping work on any errors")
        }

        def clearDestination = BoolUtils.IsValue(Config.content.clearDestination)
        if (clearDestination) {
            Logs.Info("Used clearing the destination tables before copy")
        }

        def threads = Config.content.threads as Integer?:1
        Logs.Info("Used $threads the copy threads")

        def copyParams = Config.content.copyParams as Map<String, Object>?:[:]
        def destParams = [:] as Map<String, Object>
        copyParams.each { name, value ->
            destParams.put("dest_$name", value)
        }

        Logs.Info("Used parameters for copy: $destParams")

        def rows = tables.rows(order: ['Lower(schema_name)', 'Lower(table_name)'])
        new Executor(abortOnError: abortOnError).run(rows, threads) { row ->
            def cSource = source.cloneConnection()
            def tSource = new TableDataset(connection: cSource, schemaName: row.schema_name, tableName: row.table_name)

            def cDest = dest.cloneConnection()
            def tDest = new TableDataset(connection: cDest, schemaName: row.schema_name, tableName: row.table_name)

            def pt = new ProcessTime(name: "Copy table ${tSource.fullNameDataset()}", logLevel: 'INFO')
            Logs.Fine("Copy ${tSource.fullNameDataset()} ...")
            def count = new Flow().copy([source: tSource, dest: tDest, clear: clearDestination] + destParams)
            pt.finish(count)
        }
    }
}
