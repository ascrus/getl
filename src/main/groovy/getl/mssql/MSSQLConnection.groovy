package getl.mssql

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * MSSQL connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class MSSQLConnection extends JDBCConnection {
	@Override
	protected Class<Driver> driverClass() { MSSQLDriver }

	/** Current MSSQL connection driver */
	@JsonIgnore
	MSSQLDriver getCurrentMSSQLDriver() { driver as MSSQLDriver }

	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
	}

	@Override
	protected Class<TableDataset> getTableClass() { MSSQLTable }
}