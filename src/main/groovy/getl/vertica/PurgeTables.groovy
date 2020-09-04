package getl.vertica

import getl.proc.*
import getl.utils.*
import getl.jdbc.*
import getl.stat.*
import groovy.transform.InheritConstructors

/**
 * Run purge on Vertica defragmentation tables 
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class PurgeTables extends Job {
	VerticaConnection cVertica = new VerticaConnection(config: "vertica")
	QueryDataset qTables = new QueryDataset(connection: cVertica)

	static main(args) {
		new PurgeTables().run(args)
	}

	@Override
	void process () {
		def perc = Config.content."purge_percent"?:30
		List excludeTables = (Config.content."exclude" as List)?:[]
		def excludeWhere = ""
		
		Logs.Info("### Purge vertica tables tool")
		Logs.Info("Using the $perc coefficient to determine the table to purge")
		if (!excludeTables.isEmpty()) {
			Logs.Info("From processing excluded $excludeTables tables")
			excludeWhere = "WHERE x.table_name NOT IN (${ListUtils.QuoteList(excludeTables, "'")*.toLowerCase().join(', ')})"
		}
		
		cVertica.connected = true
		
		def query = """
SELECT x.table_name, total_row_count, deleted_row_count, 'SELECT PURGE_TABLE(''' || x.table_name || '''); -- ' || deleted_row_count || ' records' AS script
		FROM (
			SELECT Lower(p.projection_schema) || '.' || Lower(p.anchor_table_name) AS table_name, Sum(total_row_count) AS total_row_count, Sum(deleted_row_count) AS deleted_row_count
			FROM storage_containers c
				INNER JOIN projections p ON p.projection_id = c.projection_id AND p.is_super_projection
			WHERE deleted_row_count > (total_row_count*${perc / 100})
			GROUP BY Lower(p.projection_schema) || '.' ||  Lower(p.anchor_table_name)
		) AS x
		$excludeWhere
		ORDER BY deleted_row_count DESC, x.table_name;
		"""
		
		qTables.query = query
		def tables = qTables.rows()
		tables.each { row ->
			def pt = new ProcessTime(name: "Purge ${row.table_name} (${row.deleted_row_count} deleted/${row.total_row_count} total)", debug: true)
			try {
				cVertica.executeCommand(command: row.script)
				pt.finish()
				Logs.Info("${pt.name} complete")
			}
			catch (Exception e) {
				Logs.Severe("Detected error by purge ${row.table_name}: ${e.message}")
			}
		}
	}
}
