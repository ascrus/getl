package getl.hive

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.CSVDataset
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.hive.opts.HiveBulkLoadSpec
import getl.hive.opts.HiveCreateSpec
import getl.hive.opts.HiveWriteSpec
import getl.jdbc.*
import getl.jdbc.opts.BulkLoadSpec
import getl.jdbc.opts.CreateSpec
import getl.jdbc.opts.WriteSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Hive database table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class HiveTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof HiveConnection))
            throw new ExceptionGETL('Connection to HiveConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    HiveConnection useConnection(HiveConnection value) {
        setConnection(value)
        return value
    }

    /** Current Hive connection */
    @JsonIgnore
    HiveConnection getCurrentHiveConnection() { connection as HiveConnection }

    @Override
    protected CreateSpec newCreateTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new HiveCreateSpec(this, useExternalParams, opts)
    }

    /** Options for creating Hive table */
    HiveCreateSpec getCreateOpts() { new HiveCreateSpec(this, true, createDirective) }

    /** Options for creating Hive table */
    HiveCreateSpec createOpts(@DelegatesTo(HiveCreateSpec)
                              @ClosureParams(value = SimpleType, options = ['getl.hive.opts.HiveCreateSpec'])
                                      Closure cl = null) {
        genCreateTable(cl) as HiveCreateSpec
    }

    @Override
    protected WriteSpec newWriteTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new HiveWriteSpec(this, useExternalParams, opts)
    }

    /** Options for writing to Hive table */
    HiveWriteSpec getWriteOpts() { new HiveWriteSpec(this, true, writeDirective) }

    /** Options for writing to Hive table */
    HiveWriteSpec writeOpts(@DelegatesTo(HiveWriteSpec)
                               @ClosureParams(value = SimpleType, options = ['getl.hive.opts.HiveWriteSpec'])
                                       Closure cl = null) {
        genWriteDirective(cl) as HiveWriteSpec
    }

    @Override
    protected BulkLoadSpec newBulkLoadTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new HiveBulkLoadSpec(this, useExternalParams, opts)
    }

    /** Options for loading csv files to Hive table */
    HiveBulkLoadSpec getBulkLoadOpts() { new HiveBulkLoadSpec(this, true, bulkLoadDirective) }

    /** Options for loading csv files to Hive table */
    HiveBulkLoadSpec bulkLoadOpts(@DelegatesTo(HiveBulkLoadSpec)
                                     @ClosureParams(value = SimpleType, options = ['getl.hive.opts.HiveBulkLoadSpec'])
                                             Closure cl = null) {
        genBulkLoadDirective(cl) as HiveBulkLoadSpec
    }

    /**
     * Load specified csv files to Vertica table
     * @param source File to load
     * @param cl Load setup code
     */
    HiveBulkLoadSpec bulkLoadCsv(CSVDataset source,
                                 @DelegatesTo(HiveBulkLoadSpec)
                                    @ClosureParams(value = SimpleType, options = ['getl.hive.opts.HiveBulkLoadSpec'])
                                            Closure cl = null) {
        doBulkLoadCsv(source, cl) as HiveBulkLoadSpec
    }

    /**
     * Load specified csv files to Hive table
     * @param cl Load setup code
     */
    HiveBulkLoadSpec bulkLoadCsv(@DelegatesTo(HiveBulkLoadSpec)
                                    @ClosureParams(value = SimpleType, options = ['getl.hive.opts.HiveBulkLoadSpec'])
                                            Closure cl) {
        doBulkLoadCsv(null, cl) as HiveBulkLoadSpec
    }
}