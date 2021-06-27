package getl.db2

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.TableDataset
import getl.jdbc.JDBCConnection
import groovy.transform.InheritConstructors

/**
 * IBM DB2 connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class DB2Connection extends JDBCConnection {
	@Override
	protected Class<Driver> driverClass() { DB2Driver }

	/** Current DB2 connection driver */
	@JsonIgnore
	DB2Driver getCurrentDB2Driver() { driver as DB2Driver }
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = "com.ibm.db2.jcc.DB2Driver"
	}

	@Override
	protected Class<TableDataset> getTableClass() { DB2Table }
}