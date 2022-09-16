package getl.clickhouse

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * ClickHouse connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ClickHouseConnection extends JDBCConnection {
    @Override
    protected Class<Driver> driverClass() { ClickHouseDriver }

    /** Current ClickHouse driver */
    @JsonIgnore
    ClickHouseDriver getCurrentClickHouseDriver() { driver as ClickHouseDriver }

    @Override
    protected void doInitConnection () {
        super.doInitConnection()
        driverName = 'com.clickhouse.jdbc.ClickHouseDriver'
    }

    @Override
    protected Class<TableDataset> getTableClass() { ClickHouseTable }
}
