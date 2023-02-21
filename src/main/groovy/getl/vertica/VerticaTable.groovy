//file:noinspection unused
//file:noinspection DuplicatedCode
package getl.vertica

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.CSVDataset
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.jdbc.opts.*
import getl.utils.BoolUtils
import getl.utils.DateUtils
import getl.vertica.opts.*
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Vertica table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VerticaTable extends TableDataset {
    /** Fill field value with value from table on insert */
    static public final lookupDefaultType = 'DEFAULT'
    /** Fill field value with value from table when calling function REFRESH_COLUMNS */
    static public final lookupUsingType = 'USING'
    /** Fill field value with value from table on insert and when calling function REFRESH_COLUMNS */
    static public final lookupDefaultUsingType = 'DEFAULT USING'

    /*@Override
    protected void registerParameters() {
        super.registerParameters()
    }*/

    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof VerticaConnection))
            throw new ExceptionGETL('Connection to VerticaConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified Vertica connection */
    VerticaConnection useConnection(VerticaConnection value) {
        setConnection(value)
        return value
    }

    /** Current Vertica connection */
    @JsonIgnore
    VerticaConnection getCurrentVerticaConnection() { connection as VerticaConnection }

    @Override
    protected CreateSpec newCreateTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new VerticaCreateSpec(this, useExternalParams, opts)
    }

    /** Options for creating Vertica table */
    VerticaCreateSpec getCreateOpts() {
        new VerticaCreateSpec(this, true, createDirective)
    }

    /** Options for creating Vertica table */
    VerticaCreateSpec createOpts(@DelegatesTo(VerticaCreateSpec)
                                 @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaCreateSpec'])
                                         Closure cl = null) {
        genCreateTable(cl) as VerticaCreateSpec
    }

    @Override
    protected ReadSpec newReadTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new VerticaReadSpec(this, useExternalParams, opts)
    }

    /** Options for reading from Vertica table */
    VerticaReadSpec getReadOpts() { new VerticaReadSpec(this, true, readDirective) }

    /** Options for reading from Vertica table */
    VerticaReadSpec readOpts(@DelegatesTo(VerticaReadSpec)
                             @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaReadSpec'])
                                     Closure cl = null) {
        genReadDirective(cl) as VerticaReadSpec
    }

    @Override
    protected WriteSpec newWriteTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new VerticaWriteSpec(this, useExternalParams, opts)
    }

    /** Options for writing to Vertica table */
    VerticaWriteSpec getWriteOpts() { new VerticaWriteSpec(this, true, writeDirective) }

    /** Options for writing to Vertica table */
    VerticaWriteSpec writeOpts(@DelegatesTo(VerticaWriteSpec)
                               @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaWriteSpec'])
                                       Closure cl = null) {
        genWriteDirective(cl) as VerticaWriteSpec
    }

    @Override
    protected BulkLoadSpec newBulkLoadTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new VerticaBulkLoadSpec(this, useExternalParams, opts)
    }

    /** Options for loading csv files to Vertica table */
    VerticaBulkLoadSpec getBulkLoadOpts() {
        new VerticaBulkLoadSpec(this, true, bulkLoadDirective)
    }

    /** Options for loading csv files to Vertica table */
    VerticaBulkLoadSpec bulkLoadOpts(@DelegatesTo(VerticaBulkLoadSpec)
                                     @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaBulkLoadSpec'])
                                             Closure cl = null) {
        genBulkLoadDirective(cl) as VerticaBulkLoadSpec
    }

    /**
     * Load specified csv files to Vertica table
     * @param source file to load
     * @param cl bulk option settings
     * @return bulk options
     */
    VerticaBulkLoadSpec bulkLoadCsv(CSVDataset source,
                                    @DelegatesTo(VerticaBulkLoadSpec)
                                    @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaBulkLoadSpec'])
                                            Closure cl = null) {
        doBulkLoadCsv(source, cl) as VerticaBulkLoadSpec
    }

    /**
     * Load specified csv files to Vertica table
     * @param cl bulk option settings
     * @return bulk options
     */
    VerticaBulkLoadSpec bulkLoadCsv(@DelegatesTo(VerticaBulkLoadSpec)
                                    @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaBulkLoadSpec'])
                                            Closure cl) {
        doBulkLoadCsv(null, cl) as VerticaBulkLoadSpec
    }

    /**
     * Process partition parameters
     * @param startPartition partition start parameter
     * @param finishPartition partition finish parameter
     * @return parameter processing result
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    protected Map<String, Object> processPartitionParams(def startPartition, def finishPartition, Boolean truncateToDate) {
        def res = new HashMap<String, Object>()

        if (startPartition instanceof String || startPartition instanceof GString)
            res.start = '\'' + (startPartition as Object).toString() + '\''
        else if (startPartition instanceof Date) {
            if (truncateToDate || startPartition instanceof java.sql.Date)
                res.start = '\'' + DateUtils.FormatDate('yyyy-MM-dd', startPartition as Date) + '\'::date'
            else
                res.start = '\'' + DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', startPartition as Date) + '\'::timestamp'
        }
        else
            res.start = startPartition

        if (finishPartition instanceof String || finishPartition instanceof GString)
            res.finish = '\'' + (finishPartition as Object).toString() + '\''
        else if (finishPartition instanceof Date) {
            if (truncateToDate || finishPartition instanceof java.sql.Date)
                res.finish = '\'' + DateUtils.FormatDate('yyyy-MM-dd', finishPartition as Date) + '\'::date'
            else
                res.finish = '\'' + DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', finishPartition as Date) + '\'::timestamp'
        }
        else
            res.finish = finishPartition

        return res
    }

    /**
     * Checking parameters for calling the partition function
     * @param startPartition partition start parameter
     * @param finishPartition partition finish parameter
     * @param needFinishParam The finish parameter must be filled
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    private void validPartitionParams(def startPartition, def finishPartition, Boolean needFinishParam) {
        if (startPartition == null)
            throw new ExceptionGETL('Requires value for parameter "startPartition"!')
        if (needFinishParam && finishPartition == null)
            throw new ExceptionGETL('Requires value for parameter "finishPartition"!')
    }

    /**
     * Drop specified interval partitions in table
     * @param startPartition start of the partition interval
     * @param finishPartition  enf of the partitions interval
     * @param isSplit force split ros containers
     * @param truncateToDate truncate partition timestamp value to date
     * @return function result
     */
    String dropPartitions(def startPartition, def finishPartition,
                          Boolean isSplit = false, Boolean truncateToDate = true) {
        validTableName()

        if (isSplit == null)
            throw new ExceptionGETL('Requires a value for parameter "isSplit"!')

        validPartitionParams(startPartition, finishPartition, true)
        def part = processPartitionParams(startPartition, finishPartition, truncateToDate)

        def qry = new QueryDataset()
        qry.tap {
            useConnection currentVerticaConnection
            query = "SELECT DROP_PARTITIONS('{table}', {start}, {finish}, {split}) AS res"
            queryParams.table = fullTableName
            queryParams.start = part.start
            queryParams.finish = part.finish
            queryParams.split = isSplit
        }

        return qry.rows()[0].res as String
    }

    /**
     * Copies partitions from one table to another
     * @param startPartition start of the partition interval
     * @param finishPartition  enf of the partitions interval
     * @param isSplit force split ros containers
     * @param truncateToDate truncate partition timestamp value to date
     * @return function result
     */
    String copyPartitionsToTable(def startPartition, def finishPartition, VerticaTable destinationTable,
                                 Boolean isSplit = false, Boolean truncateToDate = true) {
        validTableName()
        validPartitionParams(startPartition, finishPartition, true)
        def part = processPartitionParams(startPartition, finishPartition, truncateToDate)

        if (destinationTable == null)
            throw new ExceptionGETL('Requires to set the destination table!')

        if (destinationTable.tableName == null)
            throw new ExceptionGETL('Requires to specify the destination table name!')

        if (isSplit == null)
            throw new ExceptionGETL('Requires a value for parameter "isSplit"!')

        def qry = new QueryDataset()
        qry.tap {
            useConnection currentVerticaConnection
            query = "SELECT COPY_PARTITIONS_TO_TABLE('{source}', {start}, {finish}, '{dest}', {split}) AS res"
            queryParams.source = fullTableName
            queryParams.start = part.start
            queryParams.finish = part.finish
            queryParams.dest = destinationTable.fullTableName
            queryParams.split = isSplit
        }

        return qry.rows()[0].res as String
    }

    /**
     * Moves partitions from one table to another
     * @param startPartition start of the partition interval
     * @param finishPartition  enf of the partitions interval
     * @param isSplit force split ros containers
     * @param truncateToDate truncate partition timestamp value to date
     * @return function result
     */
    String movePartitionsToTable(def startPartition, def finishPartition, VerticaTable destinationTable,
                                 Boolean isSplit = false, Boolean truncateToDate = true) {
        validTableName()
        validPartitionParams(startPartition, finishPartition, true)
        def part = processPartitionParams(startPartition, finishPartition, truncateToDate)

        if (destinationTable == null)
            throw new ExceptionGETL('Requires to set the destination table!')

        if (destinationTable.tableName == null)
            throw new ExceptionGETL('Requires to specify the destination table name!')

        if (isSplit == null)
            throw new ExceptionGETL('Requires a value for parameter "isSplit"!')

        def qry = new QueryDataset()
        qry.tap {
            useConnection currentVerticaConnection
            query = "SELECT MOVE_PARTITIONS_TO_TABLE('{source}', {start}, {finish}, '{dest}', {split}) AS res"
            queryParams.source = fullTableName
            queryParams.start = part.start
            queryParams.finish = part.finish
            queryParams.dest = destinationTable.fullTableName
            queryParams.split = isSplit
        }

        return qry.rows()[0].res as String
    }

    /**
     * Moves partitions from one table to another
     * @param startPartition start of the partition interval
     * @param finishPartition  enf of the partitions interval
     * @param isSplit force split ros containers
     * @param truncateToDate truncate partition timestamp value to date
     * @return function result
     */
    String swapPartitionsBetweenTables(def startPartition, def finishPartition, VerticaTable destinationTable,
                                       Boolean isSplit = false, Boolean truncateToDate = true) {
        validTableName()
        validPartitionParams(startPartition, finishPartition, true)
        def part = processPartitionParams(startPartition, finishPartition, truncateToDate)

        if (destinationTable == null)
            throw new ExceptionGETL('Requires to set the destination table!')

        if (destinationTable.tableName == null)
            throw new ExceptionGETL('Requires to specify the destination table name!')

        if (isSplit == null)
            throw new ExceptionGETL('Requires a value for parameter "isSplit"!')

        def qry = new QueryDataset()
        qry.tap {
            useConnection currentVerticaConnection
            query = "SELECT SWAP_PARTITIONS_BETWEEN_TABLES('{source}', {start}, {finish}, '{dest}', {split}) AS res"
            queryParams.source = fullTableName
            queryParams.start = part.start
            queryParams.finish = part.finish
            queryParams.dest = destinationTable.fullTableName
            queryParams.split = isSplit
        }

        return qry.rows()[0].res as String
    }

    /**
     * Collects and aggregates data samples and storage information from all nodes that store projections associated with the table
     * @param percent percentage of data to read from disk
     * @param columns list of columns in table, typically a predicate column
     * @return 0 — Success
     */
    Integer analyzeStatistics(Integer percent = null, List<String> columns = null) {
        validTableName()
        if (percent != null && (percent <= 0 || percent > 100))
            throw new ExceptionGETL("Invalid percentage value \"$percent\"!")

        def qry = new QueryDataset()
        qry.tap {
            useConnection currentVerticaConnection
            query = "SELECT ANALYZE_STATISTICS('{table}'{, '%columns%'}{, %percent%}) AS res"
            queryParams.table = fullTableName
            queryParams.columns = (columns != null && !columns.isEmpty())?columns.join(','):null
            queryParams.percent = percent
        }

        return qry.rows()[0].res as Integer
    }

    /**
     * Permanently removes deleted data from physical storage so disk space can be reused
     * @return function result
     */
    String purgeTable() {
        validTableName()

        def qry = new QueryDataset()
        qry.tap {
            useConnection currentVerticaConnection
            query = "SELECT PURGE_TABLE('{table}') AS res"
            queryParams.table = fullTableName
        }

        return qry.rows()[0].res as String
    }

    /**
     * Creates the table by replicating an existing table
     * @param sourceTable source table to copy structure
     * @param ifNotExists create a table if it does not exist
     * @param inheritProjections copy projections from the source table
     * @param inheritedPrivilegesSchema the table inherits privileges that are set on its schema
     */
    void createLike(VerticaTable sourceTable, Boolean ifNotExists = false, Boolean inheritProjections = false, Boolean inheritedPrivilegesSchema = false) {
        validTableName()

        if (sourceTable == null)
            throw new ExceptionGETL('Requires to set the source table!')

        if (sourceTable.tableName == null)
            throw new ExceptionGETL('Requires to specify the source table name!')

        def p = new HashMap<String, Object>()
        p.table = fullTableName
        p.source = sourceTable.fullTableName
        p.exists = (ifNotExists)?'IF NOT EXISTS':''
        p.proj = (inheritProjections)?'INCLUDING PROJECTIONS':'EXCLUDING PROJECTIONS'
        p.grant = (inheritedPrivilegesSchema)?'INCLUDE SCHEMA PRIVILEGES':'EXCLUDE SCHEMA PRIVILEGES'
        currentVerticaConnection.executeCommand('CREATE TABLE {exists} {table} LIKE {source} {proj} {grant}', [queryParams: p])
    }

    @SuppressWarnings('GroovyMissingReturnStatement')
    @Override
    void retrieveOpts() {
        super.retrieveOpts()

        new QueryDataset().tap {
            useConnection this.currentJDBCConnection
            def whereExpr = ['Upper(table_name) = \'' + this.tableName.toUpperCase() + '\'']
            if (this.schemaName() != null)
                whereExpr << 'Upper(table_schema) = \'' + this.schemaName().toUpperCase() + '\''
            queryParams.where = whereExpr.join(' AND ')
            query = 'SELECT partition_expression FROM v_catalog.tables WHERE {where}'
            def rows = rows()
            if (!rows.isEmpty()) {
                def part = rows[0].partition_expression as String
                this.createOpts.partitionBy = (part != '')?part:null
            }
        }
    }
}