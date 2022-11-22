package getl.db2

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.exception.DatasetError
import getl.jdbc.*
import groovy.transform.InheritConstructors

/**
 * IBM DB2 table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class DB2Table extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof DB2Connection))
            throw new DatasetError(this, '#dataset.invalid_connection', [className: DB2Connection.name])

        super.setConnection(value)
    }

    /** Use specified connection */
    DB2Connection useConnection(DB2Connection value) {
        setConnection(value)
        return value
    }

    /** Current DB2 connection */
    @JsonIgnore
    DB2Connection getCurrentDb2Connection() { connection as DB2Connection }
}