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
package getl.models.opts


import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.models.ReferenceVerticaTables
import getl.utils.BoolUtils
import getl.utils.Logs
import getl.utils.StringUtils
import getl.vertica.VerticaConnection
import getl.vertica.VerticaTable
import groovy.transform.InheritConstructors

/**
 * Reference model table specification
 * @author ALexsey Konstantinov
 */
@InheritConstructors
class ReferenceVerticaTableSpec extends DatasetSpec {
    /** Owner reference model */
    protected ReferenceVerticaTables getOwnerReferenceVerticaTableModel() { ownerModel as ReferenceVerticaTables }

    /** Work table name */
    String getWorkTableName() { datasetName }
    /** Work table  */
    VerticaTable getWorkTable() { modelDataset as VerticaTable }

    /** Reference table name */
    String getReferenceTableName() { "m_${workTable.schemaName}_${workTable.tableName}" }

    /** Reference table */
    VerticaTable getReferenceTable() {
        def destTable = new VerticaTable()
        destTable.with {
            useConnection ownerReferenceVerticaTableModel.referenceConnection
            schemaName = ownerReferenceVerticaTableModel.referenceSchemaName
            tableName = referenceTableName
        }

        return destTable
    }

    /** The condition for copying rows to a reference tables from an external source  */
    String getWhereCopy() { params.whereCopy as String }
    /** The condition for copying rows to a reference tables from an external source */
    void setWhereCopy(String value) { params.whereCopy = value }

    /** Percentage of sampling rows when copying from an external source */
    Integer getSampleCopy() { params.sampleCopy as Integer }
    /** Percentage of sampling rows when copying from an external source */
    void setSampleCopy(Integer value) { params.sampleCopy = value }

    /** Limit the number of rows when copying from an external source */
    Long getLimitCopy() { params.limitCopy as Long }
    /** Limit the number of rows when copying from an external source */
    void setLimitCopy(Long value) { params.limitCopy = value }

    /** The table is used when copying from an external source (default false) */
    Boolean getAllowCopy() { BoolUtils.IsValue(params.allowCopy, whereCopy != null) }
    /** The table is used when copying from an external source (default false) */
    void setAllowCopy(Boolean value) { params.allowCopy = value}

    /**
     * Create a reference table in the database schema for the model
     * @param recreate re-create the table if already exists (default false)
     */
    void createReferenceTable(boolean recreate = false) {
        def refTable = referenceTable as VerticaTable
        def sourceTable = workTable

        def exists = refTable.exists
        if (!recreate && exists) {
            Logs.Warning("Table $refTable exists and is skipped!")
            return
        }

        if (exists)
            ownerReferenceVerticaTableModel.referenceConnection.executeCommand("DROP TABLE IF EXISTS ${refTable.fullTableName} CASCADE")

        ownerReferenceVerticaTableModel.referenceConnection.executeCommand("CREATE TABLE ${refTable.fullTableName} LIKE ${sourceTable.fullTableName} INCLUDING PROJECTIONS")
        if (exists)
            Logs.Info("Reference table \"$datasetName\" successfully recreated")
        else
            Logs.Info("Reference table \"$datasetName\" successfully created")
    }

    /**
     * Filling reference data from source table
     * @onlyForEmpty copy data for empty table only (default true)
     * @return reference data was copied
     */
    @SuppressWarnings("DuplicatedCode")
    boolean copyFromSourceTable(boolean onlyForEmpty = true) {
        if (!allowCopy) {
            Logs.Info("Reference table \"$datasetName\" is not used in copying and is skipped")
            return false
        }

        def sourceTable = workTable
        if (!sourceTable.exists)
            throw new ExceptionModel("Source table $sourceTable not found!")

        def destTable = referenceTable as VerticaTable

        def sourceRows = sourceTable.countRow(whereCopy, ownerReferenceVerticaTableModel.modelVars + objectVars)
        if (sourceRows == 0) {
            destTable.truncate(truncate: true)
            def msgWhere = (whereCopy != null)?" for given conditions \"$whereCopy\"":''
            Logs.Warning("No rows were found in table $sourceTable$msgWhere!")
            return false
        }

        if (destTable.countRow() > 0) {
            if (onlyForEmpty) {
                Logs.Warning("Table $destTable is not empty and is skipped!")
                return false
            }
            else {
                destTable.truncate(truncate: true)
            }
        }

        ownerReferenceVerticaTableModel.referenceConnection.with {
            transaction {
                def p = [_model_sourcetable_: sourceTable.fullTableName, _model_desttable_:destTable.fullTableName, where: '', sample: '']
                if (whereCopy != null) p.where = 'WHERE ' + whereCopy
                if (sampleCopy != null) p.sample = 'SAMPLE ' + sampleCopy
                executeCommand('INSERT /*+direct*/ INTO {_model_desttable_} SELECT * FROM {_model_sourcetable_} {where} {sample}', [queryParams: p])
            }
        }

        def destRows = destTable.countRow()
        if ((sampleCopy?:0) > 0) {
            if (destRows == 0)
                throw new ExceptionModel("Failed to copy rows from $sourceTable to $destTable!")
        }
        else if (sourceRows != destRows)
            throw new ExceptionModel("The number of copied source and destination rows does not match, " +
                    "${StringUtils.WithGroupSeparator(sourceRows)} rows in the source $sourceTable, " +
                    "${StringUtils.WithGroupSeparator(destRows)} rows in the destination $destTable!")

        Logs.Info("${StringUtils.WithGroupSeparator(destRows)} rows copied to reference table \"$datasetName\"")

        return true
    }

    /**
     * Fill the reference table with data from the table of the specified Vertica connection
     * @externalConnection Vertica cluster from which to copy data
     * @onlyForEmpty copy data for empty tables only (default true)
     * @return reference data was copied
     */
    @SuppressWarnings("DuplicatedCode")
    boolean copyFromVertica(VerticaConnection externalConnection, boolean onlyForEmpty = true) {
        if (!allowCopy) {
            Logs.Info("Reference table \"$datasetName\" is not used in copying and is skipped")
            return false
        }

        def sourceTable = workTable.cloneDataset(externalConnection) as VerticaTable
        if (!sourceTable.exists)
            throw new ExceptionModel("Source table $sourceTable not found in Vertica cluster!")

        def destTable = referenceTable
        def sourceRows = sourceTable.countRow(whereCopy, ownerReferenceVerticaTableModel.modelVars + objectVars)
        if (sourceRows == 0) {
            destTable.truncate(truncate: true)
            def msgWhere = (whereCopy != null)?" for given conditions \"$whereCopy\"":''
            Logs.Warning("No rows were found in table $sourceTable$msgWhere!")

            return false
        }

        def cols = [] as List<String>
        new QueryDataset().with {
            useConnection ownerReferenceVerticaTableModel.referenceConnection
            query = '''
SELECT column_name
FROM columns 
WHERE 
  table_schema ILIKE '{schema}' AND 
  table_name ILIKE '{table}' AND
  column_set_using = ''
ORDER BY ordinal_position'''
            queryParams.schema = sourceTable.schemaName
            queryParams.table = sourceTable.tableName
            eachRow { row -> cols << '"' + (row.column_name as String) + '"' }
        }
        if (cols.isEmpty())
            throw new ExceptionModel("No columns found for table $sourceTable!")

        if (destTable.countRow() > 0) {
            if (onlyForEmpty) {
                Logs.Warning("Table $destTable is not empty and is skipped!")
                return false
            }
            else {
                destTable.truncate(truncate: true)
            }
        }

        externalConnection.with {
            def p = [:]
            p.putAll(ownerReferenceVerticaTableModel.modelVars)
            p.putAll(objectVars)
            p._model_sourcetable_ = sourceTable.fullTableName
            p._model_destdatabase_ = externalConnection.connectDatabase
            p._model_desttable_ = destTable.fullTableName
            p._model_cols_ = cols.join(', ')
            p._model_sample_ = (sampleCopy != null)?"TABLESAMPLE($sampleCopy)":''
            p._model_limit_ = (limitCopy)?"LIMIT $limitCopy":''

            def script = """
EXPORT TO VERTICA {_model_destdatabase_}.{_model_desttable_} 
  ({_model_cols_}) 
  AS 
  SELECT 
    {_model_cols_} 
  FROM {_model_sourcetable_} {_model_sample_} 
  ${(whereCopy != null)?"WHERE $whereCopy":''}
  {_model_limit_}
"""

            executeCommand(script, [queryParams: p])
        }

        def destRows = destTable.countRow()
        if ((sampleCopy?:0) > 0) {
            if (destRows == 0)
                throw new ExceptionModel("Failed to copy rows from $sourceTable to $destTable!")
        }
        else if (sourceRows != destRows)
            throw new ExceptionModel("The number of copied source and destination rows does not match, " +
                    "${StringUtils.WithGroupSeparator(sourceRows)} rows in the source $sourceTable, " +
                    "${StringUtils.WithGroupSeparator(destRows)} rows in the destination $destTable!")

        Logs.Info("${StringUtils.WithGroupSeparator(destRows)} rows copied to reference table \"$datasetName\"")

        return true
    }

    /**
     * Fill table with data from reference table
     */
    @SuppressWarnings("GroovyVariableNotAssigned")
    boolean fillFromReferenceTable() {
        def sourceTable = referenceTable as VerticaTable
        def destTable = workTable as VerticaTable

        def sourceRows = sourceTable.countRow()
        if (sourceRows == destTable.countRow()) {
            if (sourceRows > 0)
                Logs.Warning("Table ${destTable.fullTableName} contains the required number of ${StringUtils.WithGroupSeparator(sourceRows)} rows and is skipped")
            else
                Logs.Warning("Table ${destTable.fullTableName} has no reference rows and has been cleared")

            return false
        }
        destTable.truncate(truncate: true)

        if (sourceRows > 0) {
            def tableAttrs = new QueryDataset()
            tableAttrs.with {
                useConnection ownerReferenceVerticaTableModel.referenceConnection
                query = '''
SELECT NullIf(partition_expression, '') AS partition_expression
FROM tables
WHERE table_schema ILIKE '{schema}' AND table_name ILIKE '{table}'
'''
                queryParams.schema = sourceTable.schemaName
                queryParams.table = sourceTable.tableName
            }
            def partExpression = (tableAttrs.rows()[0].partition_expression as String)
            def startPart, finishPart
            if (partExpression == null) {
                ownerReferenceVerticaTableModel.referenceConnection.with {
                    transaction {
                        executeCommand '''INSERT /*+direct*/ INTO {dest} SELECT * FROM {source}''',
                                [queryParams: [source: sourceTable.fullTableName, dest: destTable.fullTableName]]
                    }
                }
            } else {
                def partDays = new QueryDataset()
                partDays.with {
                    useConnection ownerReferenceVerticaTableModel.referenceConnection
                    query = 'SELECT Min({part_expr}) AS part_min, Max({part_expr}) AS part_max FROM {table}'
                    queryParams.table = sourceTable.fullTableName
                    queryParams.part_expr = partExpression
                }
                def partRow = partDays.rows()[0]
                startPart = partRow.part_min
                finishPart = partRow.part_max
                ownerReferenceVerticaTableModel.referenceConnection.executeCommand '''SELECT COPY_PARTITIONS_TO_TABLE('{source}', '{start}', '{finish}', '{dest}', true)''',
                        [queryParams: [source: sourceTable.fullTableName, dest: destTable.fullTableName, start: startPart, finish: finishPart]]
            }

            def destRows = destTable.countRow()
            if (sourceRows != destRows)
                throw new ExceptionModel("The number of copied rows is ${StringUtils.WithGroupSeparator(destRows)} table $destTable and does not match the number of source rows ${StringUtils.WithGroupSeparator(sourceRows)} reference table \"$datasetName\"!")

            if (partExpression == null)
                Logs.Info("Copied ${StringUtils.WithGroupSeparator(destRows)} rows to table $destTable from reference table \"$datasetName\"")
            else
                Logs.Info("Copied ${StringUtils.WithGroupSeparator(destRows)} rows to table $destTable from reference table \"$datasetName\" (partitions \"$startPart\" to \"$finishPart\")")
        }
        else {
            Logs.Info("Reference table \"$datasetName\" has no rows, table $destTable is cleared")
        }

        return true
    }
}