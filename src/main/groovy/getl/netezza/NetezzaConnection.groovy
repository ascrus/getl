package getl.netezza

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset

/**
 * Netezza connection class
 * @author Alexsey Konstantinov
 *
 */
class NetezzaConnection extends JDBCConnection {
    NetezzaConnection() {
        super(driver: NetezzaDriver)
    }

    NetezzaConnection(Map params) {
        super(new HashMap([driver: NetezzaDriver]) + params?:[:])
        if (this.getClass().name == 'getl.netezza.NetezzaConnection') methodParams.validation("Super", params?:[:])
    }

    /** Current Netezza connection driver */
    @JsonIgnore
    NetezzaDriver getCurrentNetezzaDriver() { driver as NetezzaDriver }

    @Override
    protected void onLoadConfig (Map configSection) {
        super.onLoadConfig(configSection)
        if (this.getClass().name == 'getl.netezza.NetezzaConnection') methodParams.validation("Super", params)
    }

    @Override
    protected void doInitConnection () {
        super.doInitConnection()
        driverName = 'org.netezza.Driver'
    }

    @Override
    protected Class<TableDataset> getTableClass() { NetezzaTable }
}