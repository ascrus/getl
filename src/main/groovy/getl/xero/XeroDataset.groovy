package getl.xero

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.exception.ExceptionGETL
import groovy.transform.InheritConstructors

/**
 * Xero dataset
 * @author Alexsey Konstantinov
 *
 */
class XeroDataset extends Dataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof XeroConnection))
            throw new ExceptionGETL('Connection to XeroConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    XeroConnection useConnection(XeroConnection value) {
        setConnection(value)
        return value
    }

    /** Current Xero connection */
    @JsonIgnore
    XeroConnection getCurrentXeroConnection() { connection as XeroConnection }

    /**
     * Object name by Xero
     */
    String getXeroObjectName () { params.xeroObjectName }

    void setXeroObjectName (String value) {
        params.xeroObjectName = value
    }

    @Override
    @JsonIgnore
    String getObjectName() {
        return xeroObjectName
    }

    @Override
    @JsonIgnore
    String getObjectFullName() {
        return currentXeroConnection?.configInResource?:'' +'.' + xeroObjectName
    }
}