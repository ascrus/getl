package getl.mysql

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.TableDataset
import getl.utils.BoolUtils
import getl.jdbc.JDBCConnection

/**
 * MySQL connection class
 * @author Alexsey Konstantinov
 *
 */
class MySQLConnection extends JDBCConnection {
	MySQLConnection() {
		super(driver: MySQLDriver)
	}
	
	MySQLConnection(Map params) {
		super(new HashMap([driver: MySQLDriver]) + params?:[:])
		if (this.getClass().name == 'getl.mysql.MySQLConnection') methodParams.validation("Super", params?:[:])
	}

	/** Current MySQL connection driver */
	@JsonIgnore
	MySQLDriver getCurrentMySQLDriver() { driver as MySQLDriver }

	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', ['usedOldDriver'])
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.mysql.MySQLConnection') methodParams.validation("Super", params)
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