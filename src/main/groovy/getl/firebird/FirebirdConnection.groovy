package getl.firebird

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset

/**
 * Firebird connection class
 * @author Alexsey Konstantinov
 *
 */
class FirebirdConnection extends JDBCConnection {
    FirebirdConnection() {
        super(driver: FirebirdDriver)
    }

    FirebirdConnection(Map params) {
        super(new HashMap([driver: FirebirdDriver]) + params?:[:])

        if (this.getClass().name == 'getl.firebird.FirebirdConnection') methodParams.validation('Super', params?:[:])
    }

    /** Current Firebird connection driver */
    @JsonIgnore
    FirebirdDriver getCurrentFirebirdDriver() { driver as FirebirdDriver }

    @Override
    protected void onLoadConfig (Map configSection) {
        super.onLoadConfig(configSection)
        if (this.getClass().name == 'getl.firebird.FirebirdConnection') methodParams.validation('Super', params)
    }

    @Override
    protected void doInitConnection () {
        super.doInitConnection()
        driverName = 'org.firebirdsql.jdbc.FBDriver'
    }

    @Override
    protected Class<TableDataset> getTableClass() { FirebirdTable }
}