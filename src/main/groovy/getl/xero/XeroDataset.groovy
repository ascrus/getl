package getl.xero

import getl.data.Connection
import getl.data.Dataset
import groovy.transform.InheritConstructors

/**
 * Xero dataset
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class XeroDataset extends Dataset {
    /*
    XeroDataset() {
        super()
    }
    */

    @Override
    void setConnection(Connection value) {
        assert value == null || value instanceof XeroConnection
        super.setConnection(value)
    }

    /**
     * Object name by Xero
     */
    public String getXeroObjectName () { params.xeroObjectName }
    public void setXeroObjectName (String value) {
        params.xeroObjectName = value
    }

    @Override
    String getObjectName() {
        return xeroObjectName
    }

    @Override
    String getObjectFullName() {
        return (connection as XeroConnection).configInResource?:'' +'.' + xeroObjectName
    }
}