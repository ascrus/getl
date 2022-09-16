package getl.clickhouse

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

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
}