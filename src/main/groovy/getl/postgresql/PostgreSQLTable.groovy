package getl.postgresql

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.*
import groovy.transform.InheritConstructors

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
}