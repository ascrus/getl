package getl.db2

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.TableDataset
import getl.jdbc.JDBCConnection

/**
 * IBM DB2 connection class
 * @author Alexsey Konstantinov
 *
 */
class DB2Connection extends JDBCConnection {
	DB2Connection() {
		super(driver: DB2Driver)
	}
	
	DB2Connection(Map params) {
		super(new HashMap([driver: DB2Driver]) + params?:[:])
		if (this.getClass().name == 'getl.db2.DB2Connection') methodParams.validation("Super", params?:[:])
	}

	/** Current DB2 connection driver */
	@JsonIgnore
	DB2Driver getCurrentDB2Driver() { driver as DB2Driver }
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.db2.DB2Connection') methodParams.validation("Super", params)
	}
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = "com.ibm.db2.jcc.DB2Driver"
	}

	@Override
	protected Class<TableDataset> getTableClass() { DB2Table }
}