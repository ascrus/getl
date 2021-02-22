package getl.netsuite

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.*
import groovy.transform.InheritConstructors

/**
 * Netsuite table dataset
 * @author Dmitry Shaldin
 *
 */
@InheritConstructors
class NetsuiteTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof NetsuiteConnection))
            throw new ExceptionGETL('Connection to NetsuiteConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    NetsuiteConnection useConnection(NetsuiteConnection value) {
        setConnection(value)
        return value
    }

    /** Current Netsuite connection */
    @JsonIgnore
    NetsuiteConnection getCurrentNetsuiteConnection() { connection as NetsuiteConnection }
}