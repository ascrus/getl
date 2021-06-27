package getl.mysql

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.TableDataset
import getl.utils.BoolUtils
import getl.jdbc.JDBCConnection
import groovy.transform.InheritConstructors

/**
 * MySQL connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class MySQLConnection extends JDBCConnection {
	@Override
	protected Class<Driver> driverClass() { MySQLDriver }

	/** Current MySQL connection driver */
	@JsonIgnore
	MySQLDriver getCurrentMySQLDriver() { driver as MySQLDriver }

	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', ['usedOldDriver'])
	}
	
	/** Enable if a driver under version 6 is used. */
	Boolean getUsedOldDriver() { params.usedOldDriver as Boolean }
	/** Enable if a driver under version 6 is used. */
	void setUsedOldDriver(Boolean value) { params.usedOldDriver = value }
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = (!BoolUtils.IsValue(usedOldDriver))?'com.mysql.cj.jdbc.Driver':'com.mysql.jdbc.Driver'
	}

	@Override
	protected Class<TableDataset> getTableClass() { MySQLTable }
}