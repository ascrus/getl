package getl.mysql

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.*
import groovy.transform.InheritConstructors

/**
 * MySQL table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class MySQLTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof MySQLConnection))
            throw new ExceptionGETL('Сonnection to MySQLConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    MySQLConnection useConnection(MySQLConnection value) {
        setConnection(value)
        return value
    }

    /** Current MySQL connection */
    @JsonIgnore
    MySQLConnection getCurrentMySQLConnection() { connection as MySQLConnection }
}