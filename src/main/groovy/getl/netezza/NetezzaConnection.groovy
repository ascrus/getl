package getl.netezza

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * Netezza connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class NetezzaConnection extends JDBCConnection {
    @Override
    protected Class<Driver> driverClass() { NetezzaDriver }

    /** Current Netezza connection driver */
    @JsonIgnore
    NetezzaDriver getCurrentNetezzaDriver() { driver as NetezzaDriver }

    @Override
    protected void doInitConnection () {
        super.doInitConnection()
        driverName = 'org.netezza.Driver'
    }

    @Override
    protected Class<TableDataset> getTableClass() { NetezzaTable }
}