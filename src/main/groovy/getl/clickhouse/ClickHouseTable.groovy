//file:noinspection unused
package getl.clickhouse

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.clickhouse.opts.ClickHouseCreateSpec
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.jdbc.opts.CreateSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * ClickHouse database table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ClickHouseTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof ClickHouseConnection))
            throw new ExceptionGETL('Connection to ClickHouseConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified ClickHouse connection */
    ClickHouseConnection useConnection(ClickHouseConnection value) {
        setConnection(value)
        return value
    }

    /** Current ClickHouse connection */
    @JsonIgnore
    ClickHouseConnection getCurrentClickHouseConnection() { connection as ClickHouseConnection }

    @Override
    protected CreateSpec newCreateTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        return new ClickHouseCreateSpec(this, useExternalParams, opts)
    }

    /** Options for creating ClickHouse table */
    ClickHouseCreateSpec getCreateOpts() {
        return new ClickHouseCreateSpec(this, true, createDirective)
    }

    /** Options for creating ClickHouse table */
    ClickHouseCreateSpec createOpts(@DelegatesTo(ClickHouseCreateSpec)
                            @ClosureParams(value = SimpleType, options = ['getl.clickhouse.opts.ClickHouseCreateSpec'])
                                    Closure cl = null) {
        genCreateTable(cl) as ClickHouseCreateSpec
    }

    @SuppressWarnings('GroovyMissingReturnStatement')
    @Override
    void retrieveOpts() {
        super.retrieveOpts()

        new QueryDataset().tap {
            useConnection this.currentJDBCConnection
            def whereExpr = ['Upper(name) = \'' + this.tableName.toUpperCase() + '\'']
            if (this.schemaName() != null)
                whereExpr << 'Upper(database) = \'' + this.schemaName().toUpperCase() + '\''
            queryParams.where = whereExpr.join(' AND ')
            query = 'SELECT engine, partition_key, sorting_key FROM system.tables WHERE {where}'
            def rows = rows()
            if (!rows.isEmpty()) {
                def engine = rows[0].engine as String
                this.createOpts.engine = (engine != null && engine.trim().length() > 0)?engine:null

                def part = rows[0].partition_key as String
                this.createOpts.partitionBy = (part != null && part.trim().length() > 0)?part:null

                def ord = rows[0].sorting_key as String
                if (ord != null && ord.trim().length() > 0)
                    this.createOpts.orderBy = ord.split(',').collect { col -> col.trim() }
                else
                    this.createOpts.orderBy = null
            }
        }
    }
}