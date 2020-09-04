package getl.mssql

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.TableDataset
import getl.jdbc.opts.ReadSpec
import getl.mssql.opts.MSSQLReadSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * MS SQLServer table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class MSSQLTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof MSSQLConnection))
            throw new ExceptionGETL('Сonnection to MSSQLConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    MSSQLConnection useConnection(MSSQLConnection value) {
        setConnection(value)
        return value
    }

    /** Current MSSQL connection */
    @JsonIgnore
    MSSQLConnection getCurrentMSSQLConnection() { connection as MSSQLConnection }

    @Override
    protected ReadSpec newReadTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new MSSQLReadSpec(this, useExternalParams, opts)
    }

    /**
     * Read table options
     */
    MSSQLReadSpec readOpts(@DelegatesTo(MSSQLReadSpec)
                           @ClosureParams(value = SimpleType, options = ['getl.mssql.opts.MSSQLReadSpec>'])
                                   Closure cl = null) {
        genReadDirective(cl) as MSSQLReadSpec
    }
}