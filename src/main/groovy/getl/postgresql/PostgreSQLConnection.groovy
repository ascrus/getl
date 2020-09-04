package getl.postgresql

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset

/**
 * PostgreSQL connection class
 * @author Alexsey Konstantinov
 *
 */
class PostgreSQLConnection extends JDBCConnection {
	PostgreSQLConnection() {
		super(driver: PostgreSQLDriver)
	}
	
	PostgreSQLConnection(Map params) {
		super(new HashMap([driver: PostgreSQLDriver]) + params?:[:])
		if (this.getClass().name == 'getl.postgresql.PostgreSQLConnection') methodParams.validation("Super", params?:[:])
	}

	/** Current PostgreSQL connection driver */
	@JsonIgnore
	PostgreSQLDriver getCurrentPostgreSQLDriver() { driver as PostgreSQLDriver }
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.postgresql.PostgreSQLConnection') methodParams.validation("Super", params)
	}
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = "org.postgresql.Driver"
	}

	@Override
	protected Class<TableDataset> getTableClass() { PostgreSQLTable }
}