package getl.netezza

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * Vertica table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class NetezzaTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof NetezzaConnection))
            throw new ExceptionGETL('Connection to NetezzaConnection class is allowed!')

        super.setConnection(value)
    }

    NetezzaConnection useConnection(NetezzaConnection value) {
        setConnection(value)
        return value
    }

    /** Current Netezza connection */
    @JsonIgnore
    NetezzaConnection getCurrentNetezzaConnection() { connection as NetezzaConnection }
}