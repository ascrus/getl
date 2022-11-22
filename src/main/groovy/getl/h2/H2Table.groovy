package getl.h2

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.CSVDataset
import getl.data.Connection
import getl.exception.DatasetError
import getl.h2.opts.*
import getl.jdbc.*
import getl.jdbc.opts.*
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * H2 database table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class H2Table extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof H2Connection))
            throw new DatasetError(this, 'H2Connection connection only allowed')

        super.setConnection(value)
    }

    /** Use specified H2 connection */
    H2Connection useConnection(H2Connection value) {
        setConnection(value)
        return value
    }

    /** Current H2 connection */
    @JsonIgnore
    H2Connection getCurrentH2Connection() { connection as H2Connection }

    @Override
    protected CreateSpec newCreateTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        return new H2CreateSpec(this, useExternalParams, opts)
    }

    /** Options for creating H2 table */
    H2CreateSpec getCreateOpts() { new H2CreateSpec(this, true, createDirective) }

    /** Options for creating H2 table */
    H2CreateSpec createOpts(@DelegatesTo(H2CreateSpec)
                            @ClosureParams(value = SimpleType, options = ['getl.h2.opts.H2CreateSpec'])
                                    Closure cl = null) {
        genCreateTable(cl) as H2CreateSpec
    }

    /**
     * Load specified csv files to H2 table
     * @param source File to load
     * @param cl Load setup code
     */
    BulkLoadSpec bulkLoadCsv(CSVDataset source,
                               @DelegatesTo(BulkLoadSpec)
                               @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.BulkLoadSpec'])
                                       Closure cl = null) {
        doBulkLoadCsv(source, cl) as BulkLoadSpec
    }

    /**
     * Load specified csv files to H2 table
     * @param cl Load setup code
     */
    BulkLoadSpec bulkLoadCsv(@DelegatesTo(BulkLoadSpec)
                               @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.BulkLoadSpec'])
                                       Closure cl) {
        doBulkLoadCsv(null, cl) as BulkLoadSpec
    }
}