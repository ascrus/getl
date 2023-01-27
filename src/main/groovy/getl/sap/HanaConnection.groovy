//file:noinspection unused
package getl.sap

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * SAP Hana connection class
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class HanaConnection extends JDBCConnection {
    @Override
    protected Class<Driver> driverClass() { HanaDriver }

    /** Current SAP Hana driver */
    @JsonIgnore
    HanaDriver getCurrentHanaDriver() { driver as HanaDriver }

    @Override
    protected void doInitConnection () {
        super.doInitConnection()
        driverName = 'com.sap.db.jdbc.Driver'
    }

    @Override
    protected Class<TableDataset> getTableClass() { HanaTable }

    @Override
    protected void initParams() {
        super.initParams()

        if (connectProperty.deferredPrepare == null)
            connectProperty.deferredPrepare = true
    }
}