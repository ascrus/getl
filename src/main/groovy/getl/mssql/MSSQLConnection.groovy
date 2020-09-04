package getl.mssql

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset

/**
 * MSSQL connection class
 * @author Alexsey Konstantinov
 *
 */
class MSSQLConnection extends JDBCConnection {
	MSSQLConnection() {
		super(driver: MSSQLDriver)
	}
	
	MSSQLConnection(Map params) {
		super(new HashMap([driver: MSSQLDriver]) + params?:[:])
		if (this.getClass().name == 'getl.mssql.MSSQLConnection') methodParams.validation("Super", params?:[:])
	}

	/** Current MSSQL connection driver */
	@JsonIgnore
	MSSQLDriver getCurrentMSSQLDriver() { driver as MSSQLDriver }
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.mssql.MSSQLConnection') methodParams.validation("Super", params)
	}
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
	}

	@Override
	protected Class<TableDataset> getTableClass() { MSSQLTable }
}