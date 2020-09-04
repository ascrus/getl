package getl.vertica

import getl.proc.*
import getl.utils.*
import getl.jdbc.*
import getl.stat.*
import groovy.transform.InheritConstructors

/**
 * Run workload Vertica recommendation 
 * @author Konstantinov Alexsey
 *
 */
@InheritConstructors
class ProcessWorkload extends Job {
	VerticaConnection cVertica = new VerticaConnection(config: "vertica")
	QueryDataset qTables = new QueryDataset(connection: cVertica)
	QueryDataset qWorkload = new QueryDataset(connection: cVertica, query: "SELECT ANALYZE_WORKLOAD('')")
	
	static main(args) {
		new ProcessWorkload().run(args)
	}

	@Override
	void process() {
		Logs.Info("### Analyze Vertica workload tool")

		if (Config.content."interval" != null) {
			Logs.Info("Use workload from time \"${Config.content."interval"}\"")
			qWorkload.query = "SELECT ANALYZE_WORKLOAD('', ${Config.content."interval"})"
		}
		else if (BoolUtils.IsValue(Config.content."save")) {
			Logs.Info("Use workload with save result")
			qWorkload.query = "SELECT ANALYZE_WORKLOAD('', true)"
		}
		else {
            Logs.Info("Use workload without save result")
            qWorkload.query = "SELECT ANALYZE_WORKLOAD('', false)"
		}

		List excludeTables = (Config.content."exclude" as List)?:[]
		def excludeWhere = ""

		if (!excludeTables.isEmpty()) {
			Logs.Info("Excluded $excludeTables schemas")
			excludeWhere = "AND Lower(table_schema) NOT IN (${ListUtils.QuoteList(excludeTables, "'")*.toLowerCase().join(', ')})"
		}
		
		cVertica.connected = true
		
		def query = """
SELECT Lower(table_schema || '.' || table_name) AS name 
FROM tables 
WHERE NOT is_temp_table $excludeWhere 
ORDER BY name
"""
		qTables.query = query
		
		def rows_table = qTables.rows()
		def tables = []
		rows_table.each { r ->
			tables << r.name
		}
		Logs.Info("Found ${tables.size()} tables for analize")
	
		def pt = new ProcessTime(name: "Get workload recommendation", debug: true)
		def rows = qWorkload.rows()
		pt.finish(rows.size())
		Logs.Info("Found ${rows.size()} recommendation for database")
		
		rows.each { row ->
			if ((row.tuning_description as String).substring(0, 18).toLowerCase() == 'analyze statistics') {
				def t = (row.tuning_parameter as String).toLowerCase()
				if (t.matches(".*[.].*[.].*")) {
					t = t.substring(0, t.lastIndexOf("."))
				}
				if (tables.indexOf(t) != -1) {
					def ptw = new ProcessTime(name: row.tuning_description, debug: true)
					def queryStat = (row.tuning_command as String).replace('"', '')
					try {
						cVertica.executeCommand(command: queryStat)
						ptw.finish()
						Logs.Info("Rebuild statistics for $t complete")
					}
					catch (Exception e) {
						Logs.Severe("Found error for $t: ${e.message}")
					}
				}
			}
			else {
				Logs.Fine("Skip recommendation ${row.tuning_description}")
			}
		}
	}
}
