//file:noinspection unused
package getl.models.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.exception.DatasetError
import getl.exception.ModelError
import getl.exception.RequiredParameterError
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.models.ReferenceVerticaTables
import getl.models.sub.DatasetSpec
import getl.proc.Flow
import getl.utils.BoolUtils
import getl.utils.ConvertUtils
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
    /** Work table name */
    void setWorkTableName(String value) { datasetName = value }
    /** Work table  */
    @JsonIgnore
    VerticaTable getWorkTable() { modelDataset as VerticaTable }

    /** Reference table name */
    @JsonIgnore
    String getReferenceTableName() { "m_${workTable.schemaName()}_${workTable.tableName}" }

    /** Reference table */
    @JsonIgnore
    VerticaTable getReferenceTable() {
        def destTable = new VerticaTable()
        destTable.tap {
            useConnection ownerReferenceVerticaTableModel.referenceConnection
            schemaName = ownerReferenceVerticaTableModel.referenceSchemaName
            tableName = referenceTableName
        }

        return destTable
    }

    /** The condition for copying rows to reference tables from source  */
    String getWhereCopy() { params.whereCopy as String }
    /** The condition for copying rows to reference tables from source  */
    void setWhereCopy(String value) { saveParamValue('whereCopy', value) }

    /** Percentage of sampling rows when copying from source */
    Integer getSampleCopy() { params.sampleCopy as Integer }
    /** Percentage of sampling rows when copying from source */
    void setSampleCopy(Integer value) { saveParamValue('sampleCopy', value) }

    /** Limit the number of rows when copying from source */
    Long getLimitCopy() { params.limitCopy as Long }
    /** Limit the number of rows when copying from source */
    void setLimitCopy(Long value) { saveParamValue('limitCopy', value) }

    /** The table is used when copying from source (default false) */
    Boolean getAllowCopy() { ConvertUtils.Object2Boolean(params.allowCopy) }
    /** The table is used when copying from source (default false) */
    void setAllowCopy(Boolean value) { saveParamValue('allowCopy', value) }

    /** The table is used when copying from source */
    Boolean isAllowCopy() { BoolUtils.IsValue(allowCopy, whereCopy != null) }

    /** Always fill the table (default false) */
    Boolean getAlwaysFill() { ConvertUtils.Object2Boolean(params.alwaysFill) }
    /** Always fill the table (default false) */
    void setAlwaysFill(Boolean value) { saveParamValue('alwaysFill', value) }

    /** Reference data source name */
    @JsonIgnore
    String getSourceDatasetName() { params.sourceDatasetName as String }
    /** Reference data source name */
    void setSourceDatasetName(String value) {
        if (value != null) ownerModel.dslCreator.dataset(value)
        params.sourceDatasetName = value
    }

    /** Reference data source */
    @JsonIgnore
    Dataset getSourceDataset() { (sourceDatasetName != null)?ownerModel.dslCreator.dataset(sourceDatasetName):null }
    /** Reference data source */
    void setSourceDataset(Dataset value) {
        if (value == null) {
            params.sourceDatasetName = null
            return
        }
        if (value.dslNameObject == null)
            throw new DatasetError(value, '#dsl.object.not_register')

        params.sourceDatasetName = value.dslNameObject
    }

    /**
     * Create a reference table in the database schema for the model
     * @param recreate re-create the table if already exists (default false)
     */
    void createReferenceTable(Boolean recreate = false) {
        def refTable = referenceTable as VerticaTable
        def sourceTable = workTable

        def exists = refTable.exists
        if (!recreate && exists) {
            ownerModel.dslCreator.logWarn("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: reference table exists and is skipped!")
            return
        }

        if (exists)
            ownerReferenceVerticaTableModel.referenceConnection.executeCommand("DROP TABLE IF EXISTS ${refTable.fullTableName} CASCADE")

        ownerReferenceVerticaTableModel.referenceConnection.executeCommand("CREATE TABLE ${refTable.fullTableName} LIKE ${sourceTable.fullTableName} INCLUDING PROJECTIONS")
        if (exists)
            ownerModel.dslCreator.logInfo("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: reference table \"$refTable\" successfully recreated")
        else
            ownerModel.dslCreator.logInfo("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: reference table \"$refTable\" successfully created")
    }

    /**
     * Filling reference data from source table
     * @param onlyForEmpty copy data for empty tables only (default true)
     * @param source source dataset (default current table)
     * @param bulkLoad bulk load from source dataset (default false)
     * @param nullAsValue encoding null as specified value for bulkload copy type
     * @return reference data was copied
     */
    Boolean copyFromDataset(Boolean onlyForEmpty = true, Dataset source = null, Boolean bulkLoad = false, String nullAsValue = null) {
        bulkLoad = BoolUtils.IsValue(bulkLoad)
        if (source == null && bulkLoad)
            throw new RequiredParameterError(ownerDatasetsModel, 'source', 'copyFromDataset')
        if (!bulkLoad && nullAsValue != null)
            throw new ModelError(ownerDatasetsModel, '#dsl.model.reference_vertica_tables.invalid_null_value')

        def destTable = referenceTable as VerticaTable

        if (!isAllowCopy()) {
            ownerModel.dslCreator.logInfo("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: reference table is " +
                    "not used in copying and is skipped")
            destTable.truncate()
            return false
        }

        if (destTable.countRow() > 0) {
            if (onlyForEmpty) {
                ownerModel.dslCreator.logWarn("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: reference table " +
                        "is not empty and is skipped!")
                return false
            }
            else {
                destTable.truncate()
            }
        }

        Long destRows

        def sourceTable = source?:sourceDataset?:workTable
        if (sourceTable.connection?.dslNameObject == destTable.connection?.dslNameObject) {
            def vSource = sourceTable.cloneDataset() as VerticaTable
            vSource.readOpts {opt ->
                if (whereCopy != null)
                    opt.where = StringUtils.EvalMacroString(whereCopy, ownerReferenceVerticaTableModel.modelVars + objectVars, false)
                if (sampleCopy != null && sampleCopy > 0)
                    opt.tablesample = sampleCopy
                if (limitCopy != null && limitCopy > 0)
                    opt.limit = limitCopy
            }

            def vDest = destTable
            vDest.writeOpts {direct = 'DIRECT' }

            destRows = vSource.copyTo(vDest)
        }
        else {
            def vSource = sourceTable.cloneDataset()
            if (whereCopy != null) {
                if (vSource instanceof TableDataset)
                    (vSource as TableDataset).readOpts.where = StringUtils.EvalMacroString(whereCopy, ownerReferenceVerticaTableModel.modelVars + objectVars, false)
                else
                    ownerModel.dslCreator.logWarn("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: the source " +
                            "is not a JDBC table and cannot use conditions in \"whereCopy\"!")
            }

            if (sampleCopy != null) { /* TODO: Проверить что работает */
                if (vSource instanceof VerticaTable)
                    (vSource as VerticaTable).readOpts.tablesample = sampleCopy
                else
                    ownerModel.dslCreator.logWarn("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: the source " +
                            "is not a Vertica table and cannot use sampling in \"sampleCopy\"!")
            }

            if (limitCopy != null) { /* TODO: Проверить что работает */
                if (vSource instanceof TableDataset)
                    (vSource as TableDataset).readOpts.limit = limitCopy
                else
                    ownerModel.dslCreator.logWarn("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: the source " +
                            "is not a JDBC table and cannot use limit in \"limitCopy\"!")
            }

            destRows = new Flow(ownerModel.dslCreator).copy(source: vSource, dest: destTable,
                    bulkLoad: bulkLoad, bulkAsGZIP: true, bulkNullAsValue: nullAsValue)
        }

        ownerModel.dslCreator.logInfo("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: " +
                "${StringUtils.WithGroupSeparator(destRows)} rows copied from \"${sourceTable}\" to " +
                "reference table \"$destTable\"")

        return true
    }

    /** Copy source data from another Vertica cluster with EXPORT TO VERTICA operator */
    static public final String exportToVerticaCopyType = 'EXPORT'
    /** Copy source data from another Vertica cluster with standard copy rows */
    static public final String etlCopyType = 'ETL'
    /** Copy source data from another Vertica cluster with bulk loading temporary csv files */
    static public final String bulkloadCopyType = 'BULKLOAD'

    /**
     * Fill the reference table with data from the table of the specified Vertica connection
     * @param externalConnection Vertica cluster from which to copy data
     * @param onlyForEmpty copy data for empty tables only (default true)
     * @param copyType copy type (ETL, BULKLOAD OR EXPORT), default ETL
     * @param nullAsValue encoding null as specified value for bulkload copy type
     * @return reference data was copied
     */
    Boolean copyFromVertica(VerticaConnection externalConnection, Boolean onlyForEmpty = true, String copyType = etlCopyType, String nullAsValue = null) {
        if (externalConnection == null)
            throw new NullPointerException('Required external connection!')
        onlyForEmpty = BoolUtils.IsValue(onlyForEmpty, true)
        if (copyType == null)
            copyType = etlCopyType
        copyType = copyType.toUpperCase()
        if (!(copyType in [etlCopyType, bulkloadCopyType, exportToVerticaCopyType]))
            throw new ModelError(ownerDatasetsModel, '#dsl.model.reference_vertica_tables.invalid_copy_type', [type: copyType])

        def sourceTable = workTable.cloneDataset(externalConnection) as VerticaTable
        if (!sourceTable.exists)
            throw new DatasetError(sourceTable, '#jdbc.table.not_found', [table: sourceTable.fullTableName])

        if (copyType != exportToVerticaCopyType) {
            return copyFromDataset(onlyForEmpty, sourceTable, copyType == bulkloadCopyType, nullAsValue)
        }

        def destTable = referenceTable
        if (!isAllowCopy()) {
            ownerModel.dslCreator.logInfo("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: reference table is " +
                    "not used in copying and is skipped")
            destTable.truncate()
            return false
        }

        if (destTable.countRow() > 0) {
            if (onlyForEmpty) {
                ownerModel.dslCreator.logWarn("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: reference table " +
                        "is not empty and is skipped!")
                return false
            }
            else {
                destTable.truncate()
            }
        }

        Long destRows

        def cols = [] as List<String>
        new QueryDataset().tap {
            useConnection ownerReferenceVerticaTableModel.referenceConnection
            query = '''
SELECT column_name
FROM columns 
WHERE 
  table_schema ILIKE '{schema}' AND 
  table_name ILIKE '{table}' AND
  column_set_using = ''
ORDER BY ordinal_position'''
            queryParams.schema = sourceTable.schemaName()
            queryParams.table = sourceTable.tableName
            eachRow { row -> cols << '"' + (row.column_name as String) + '"' }
        }
        if (cols.isEmpty())
            throw new DatasetError(sourceTable, '#dataset.fail_read_columns', [table: sourceTable.fullTableName])

        def p = new HashMap()
        p.putAll(ownerReferenceVerticaTableModel.modelVars)
        p.putAll(objectVars)
        p._model_sourcetable_ = sourceTable.fullTableName
        p._model_destdatabase_ = externalConnection.connectDatabase
        p._model_desttable_ = destTable.fullTableName
        p._model_cols_ = cols.join(', ')
        p._model_sample_ = (sampleCopy != null) ? "TABLESAMPLE($sampleCopy)" : ''
        p._model_limit_ = (limitCopy) ? "LIMIT $limitCopy" : ''

        externalConnection.tap {
            def script = """
EXPORT TO VERTICA {_model_destdatabase_}.{_model_desttable_} 
  ({_model_cols_}) 
  AS 
  SELECT 
    {_model_cols_} 
  FROM {_model_sourcetable_} {_model_sample_} 
  ${(whereCopy != null) ? "WHERE $whereCopy" : ''}
  {_model_limit_}
"""
            executeCommand(script, [queryParams: p])
        }

        destRows = destTable.countRow()
        ownerModel.dslCreator.logInfo("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: " +
                "${StringUtils.WithGroupSeparator(destRows)} rows copied to reference table from other Vertica server")

        return true
    }

    /**
     * Fill table with data from reference table
     */
    @SuppressWarnings('SpellCheckingInspection')
    Boolean fillFromReferenceTable(Boolean usePartitions = true) {
        def sourceTable = referenceTable as VerticaTable
        def destTable = workTable as VerticaTable

        def sourceRows = sourceTable.countRow()
        if (!BoolUtils.IsValue(alwaysFill) && sourceRows == destTable.countRow()) {
            if (sourceRows > 0)
                ownerModel.dslCreator.logWarn("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: " +
                        "table ${destTable.fullTableName} contains the required number " +
                        "of ${StringUtils.WithGroupSeparator(sourceRows)} rows and is skipped")
            else
                ownerModel.dslCreator.logWarn("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: " +
                        "table ${destTable.fullTableName} has no reference rows and has been cleared")

            return false
        }

        ownerModel.dslCreator.logFinest("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: filling ...")
        destTable.truncate()
        if (sourceRows > 0) {
            def tableAttrs = new QueryDataset()
            tableAttrs.tap {
                useConnection ownerReferenceVerticaTableModel.referenceConnection
                query = '''
SELECT NullIf(partition_expression, '') AS partition_expression, IsNull(c.count_inc_cols, 0) AS count_inc_cols
FROM v_catalog.tables t
   LEFT JOIN (
      SELECT table_id, Count(*) AS count_inc_cols
      FROM v_catalog.columns c
      WHERE c.is_identity OR c.column_default ILIKE 'nextval(%)\'
      GROUP BY table_id
   ) c ON c.table_id = t.table_id
WHERE table_schema ILIKE '{schema}' AND table_name ILIKE '{table}'
'''
                queryParams.schema = destTable.schemaName()
                queryParams.table = destTable.tableName
            }
            def row = tableAttrs.rows()[0]
            def partExpression = (row.partition_expression as String)
            def countIncrementCols = row.count_inc_cols as Integer
            def startPart = null, finishPart = null
            def notPartitions = (!usePartitions || partExpression == null || countIncrementCols > 0)
            if (notPartitions) {
                sourceTable.copyTo(destTable)
            } else {
                def partDays = new QueryDataset()
                partDays.tap {
                    useConnection ownerReferenceVerticaTableModel.referenceConnection
                    query = 'SELECT Min({part_expr}) AS part_min, Max({part_expr}) AS part_max FROM {table} AS "{alias}"'
                    queryParams.table = sourceTable.fullTableName
                    queryParams.alias = destTable.tableName
                    queryParams.part_expr = partExpression
                }
                def partRow = partDays.rows()[0]
                startPart = partRow.part_min
                finishPart = partRow.part_max
                ownerReferenceVerticaTableModel.referenceConnection.executeCommand(
                        '''SELECT COPY_PARTITIONS_TO_TABLE('{source}', '{start}', '{finish}', '{dest}', true)''',
                        [queryParams: [
                                source: sourceTable.fullTableName, dest: destTable.fullTableName, start: startPart,
                                finish: finishPart]
                        ]
                )
            }

            def destRows = destTable.countRow()
            if (sourceRows != destRows)
                throw new ModelError(ownerModel, '#dsl.model.reference_vertica_tables.fail_fill',
                        [source_rows: StringUtils.WithGroupSeparator(sourceRows), source_table: sourceTable.fullTableName,
                         dest_rows: StringUtils.WithGroupSeparator(destRows), dest_table: destTable.fullTableName])

            if (notPartitions)
                ownerModel.dslCreator.logInfo("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: " +
                        "copied ${StringUtils.WithGroupSeparator(destRows)} rows to table ${destTable.fullTableName} from reference table")
            else
                ownerModel.dslCreator.logInfo("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: " +
                        "copied ${StringUtils.WithGroupSeparator(destRows)} rows to table ${destTable.fullTableName} from " +
                        "reference table (partitions \"$startPart\" to \"$finishPart\")")
        }
        else {
            ownerModel.dslCreator.logInfo("${ownerReferenceVerticaTableModel.repositoryModelName}.[${datasetName}]: reference table has no rows, " +
                    "table ${destTable.fullTableName} is cleared")
        }

        return true
    }
}