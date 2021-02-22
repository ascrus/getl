package getl.oracle

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.jdbc.opts.ReadSpec
import getl.oracle.opts.OracleReadSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Oracle table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class OracleTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof OracleConnection))
            throw new ExceptionGETL('Connection to OracleConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    OracleConnection useConnection(OracleConnection value) {
        setConnection(value)
        return value
    }

    /** Current Oracle connection */
    @JsonIgnore
    OracleConnection getCurrentOracleConnection() { connection as OracleConnection }

    @Override
    protected ReadSpec newReadTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new OracleReadSpec(this, useExternalParams, opts)
    }

    /** Read table options */
    OracleReadSpec getReadOpts() { new OracleReadSpec(this, true, readDirective) }

    /** Read table options */
    OracleReadSpec readOpts(@DelegatesTo(OracleReadSpec)
                            @ClosureParams(value = SimpleType, options = ['getl.oracle.opts.OracleReadSpec'])
                                    Closure cl = null) {
        genReadDirective(cl) as OracleReadSpec
    }
}