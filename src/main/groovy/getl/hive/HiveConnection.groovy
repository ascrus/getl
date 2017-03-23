package getl.hive

import getl.jdbc.JDBCConnection

/**
 * Created by ascru on 15.03.2017.
 */
class HiveConnection extends JDBCConnection {
    HiveConnection() {
        super(driver: HiveDriver)
    }

    HiveConnection(Map params) {
        super(new HashMap([driver: HiveDriver]) + params)
    }

    @Override
    protected void registerParameters () {
        super.registerParameters()
        //methodParams.register('Super', ['inMemory', 'exclusive'])
    }

    @Override
    protected void onLoadConfig (Map configSection) {
        super.onLoadConfig(configSection)
        if (this.getClass().name == 'getl.hive.HiveConnection') methodParams.validation('Super', params)
    }

    @Override
    protected void doInitConnection () {
        super.doInitConnection()
        driverName = 'com.cloudera.hive.jdbc41.HS2Driver'
    }
}
