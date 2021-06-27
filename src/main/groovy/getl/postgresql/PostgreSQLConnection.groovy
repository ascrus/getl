package getl.postgresql

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * PostgreSQL connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class PostgreSQLConnection extends JDBCConnection {
	@Override
	protected Class<Driver> driverClass() { PostgreSQLDriver }

	/** Current PostgreSQL connection driver */
	@JsonIgnore
	PostgreSQLDriver getCurrentPostgreSQLDriver() { driver as PostgreSQLDriver }
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = "org.postgresql.Driver"
	}

	@Override
	protected Class<TableDataset> getTableClass() { PostgreSQLTable }
}