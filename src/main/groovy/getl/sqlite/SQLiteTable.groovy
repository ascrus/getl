package getl.sqlite

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.TableDataset

/**
 * SQLite database table
 * @author Alexsey Konstantinov
 */
class SQLiteTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof SQLiteConnection))
            throw new ExceptionGETL('Connection to SQLiteConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified SAP Hana connection */
    SQLiteConnection useConnection(SQLiteConnection value) {
        setConnection(value)
        return value
    }

    /** Current SAP Hana connection */
    @JsonIgnore
    SQLiteConnection getCurrentHanaConnection() { connection as SQLiteConnection }
}