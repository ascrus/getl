//file:noinspection unused
package getl.postgresql

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.CSVDataset
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.jdbc.opts.BulkLoadSpec
import getl.jdbc.opts.CreateSpec
import getl.postgresql.opts.PostgreSQLBulkLoadSpec
import getl.postgresql.opts.PostgreSQLCreateSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * PostgreSQL table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class PostgreSQLTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof PostgreSQLConnection))
            throw new ExceptionGETL('Connection to PostgreSQLConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    PostgreSQLConnection useConnection(PostgreSQLConnection value) {
        setConnection(value)
        return value
    }

    /** Current PostgreSQL connection */
    @JsonIgnore
    PostgreSQLConnection getCurrentPostgreSQLConnection() { connection as PostgreSQLConnection }

    @Override
    protected CreateSpec newCreateTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new PostgreSQLCreateSpec(this, useExternalParams, opts)
    }

    /** Options for creating PostgreSQL table */
    PostgreSQLCreateSpec getCreateOpts() {
        new PostgreSQLCreateSpec(this, true, createDirective)
    }

    /** Options for creating PostgreSQL table */
    PostgreSQLCreateSpec createOpts(@DelegatesTo(PostgreSQLCreateSpec)
                                 @ClosureParams(value = SimpleType, options = ['getl.postgresql.opts.PostgreSQLCreateSpec'])
                                         Closure cl = null) {
        genCreateTable(cl) as PostgreSQLCreateSpec
    }

    @Override
    protected BulkLoadSpec newBulkLoadTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new PostgreSQLBulkLoadSpec(this, useExternalParams, opts)
    }

    /** Options for loading csv files to PostgreSQL table */
    PostgreSQLBulkLoadSpec getBulkLoadOpts() {
        new PostgreSQLBulkLoadSpec(this, true, bulkLoadDirective)
    }

    /** Options for loading csv files to PostgreSQL table */
    PostgreSQLBulkLoadSpec bulkLoadOpts(@DelegatesTo(PostgreSQLBulkLoadSpec)
                                     @ClosureParams(value = SimpleType, options = ['getl.postgresql.opts.PostgreSQLBulkLoadSpec'])
                                             Closure cl = null) {
        genBulkLoadDirective(cl) as PostgreSQLBulkLoadSpec
    }

    /**
     * Load specified csv files to PostgreSQL table
     * @param source file to load
     * @param cl bulk option settings
     * @return bulk options
     */
    PostgreSQLBulkLoadSpec bulkLoadCsv(CSVDataset source,
                                    @DelegatesTo(PostgreSQLBulkLoadSpec)
                                    @ClosureParams(value = SimpleType, options = ['getl.postgresql.opts.PostgreSQLBulkLoadSpec'])
                                            Closure cl = null) {
        doBulkLoadCsv(source, cl) as PostgreSQLBulkLoadSpec
    }

    /**
     * Load specified csv files to PostgreSQL table
     * @param cl bulk option settings
     * @return bulk options
     */
    PostgreSQLBulkLoadSpec bulkLoadCsv(@DelegatesTo(PostgreSQLBulkLoadSpec)
                                    @ClosureParams(value = SimpleType, options = ['getl.postgresql.opts.PostgreSQLBulkLoadSpec'])
                                            Closure cl) {
        doBulkLoadCsv(null, cl) as PostgreSQLBulkLoadSpec
    }

    @Override
    void optimizeAsBuffer() {
        super.optimizeAsBuffer()
        createOpts.unlogged = true
    }
}