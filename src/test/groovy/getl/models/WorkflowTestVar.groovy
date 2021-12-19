package getl.models

import getl.jdbc.QueryDataset
import getl.lang.Getl
import groovy.transform.BaseScript
import groovy.transform.Field

@BaseScript Getl main

@Field SetOfTables model = null
@Field Integer param_id = null

void check() {
    assert model != null
}

Map row = null

if (scriptExtendedVars.id != null)
    param_id = scriptExtendedVars.id as Integer

model.usedTables.each { node ->
    def query = (node.partitionsDataset as QueryDataset)
    def id = param_id?:(node.variable('id') as Integer)?:(query.queryParams.id as Integer)
    def rows = query.rows(queryParams: [table: node.sourceTable.fullTableName, id: id])
    assert rows.size() == 1
    row = rows[0]
}

return row