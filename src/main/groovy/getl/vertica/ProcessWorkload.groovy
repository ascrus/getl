package getl.vertica

import getl.proc.*
import getl.utils.*
import getl.jdbc.*
import getl.stat.*

class ProcessWorkload extends Job {
	VerticaConnection cVertica = new VerticaConnection(config: "vertica")
	QueryDataset qTables = new QueryDataset(connection: cVertica)
	QueryDataset qWorkload = new QueryDataset(connection: cVertica, query: "SELECT ANALYZE_WORKLOAD('', true)")
	
	static main(args) {
		new ProcessWorkload().run(args)
	}

	@Override
	public void process() {
		List excludeTables = Config.content."exclude"?:[]
		def excludeWhere = ""
		
		Logs.Info("### Analyze vertica workload utilite")
		if (!excludeTables.isEmpty()) {
			Logs.Info("From processing excluded $excludeTables schemas")
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
			if (row.tuning_description.substring(0, 18).toLowerCase() == 'analyze statistics') {
				def t = row.tuning_parameter.toLowerCase()
				if (t.matches(".*[.].*[.].*")) {
					t = t.substring(0, t.lastIndexOf("."))
				}
				if (tables.indexOf(t) != -1) {
					def ptw = new ProcessTime(name: row.tuning_description, debug: true)
					def queryStat = row.tuning_command.replace('"', '')
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
