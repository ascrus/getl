//file:noinspection unused
package getl.sap

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * SAP Hana database table
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class HanaTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof HanaConnection))
            throw new ExceptionGETL('Connection to HanaConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified SAP Hana connection */
    HanaConnection useConnection(HanaConnection value) {
        setConnection(value)
        return value
    }

    /** Current SAP Hana connection */
    @JsonIgnore
    HanaConnection getCurrentHanaConnection() { connection as HanaConnection }
}