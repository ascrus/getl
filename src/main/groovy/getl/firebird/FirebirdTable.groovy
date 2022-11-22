package getl.firebird

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.exception.DatasetError
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * Firebird database table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FirebirdTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof FirebirdConnection))
            throw new DatasetError(this, '#dataset.invalid_connection', [className: FirebirdConnection.name])

        super.setConnection(value)
    }

    /** Use specified Firebird connection */
    FirebirdConnection useConnection(FirebirdConnection value) {
        setConnection(value)
        return value
    }

    /** Current Firebird connection */
    @JsonIgnore
    FirebirdConnection getCurrentFirebirdConnection() { connection as FirebirdConnection }
}