package getl.oracle

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * Oracle connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class OracleConnection extends JDBCConnection {
	@Override
	protected Class<Driver> driverClass() { OracleDriver }

	/** Current Oracle connection driver */
	@JsonIgnore
	OracleDriver getCurrentOracleDriver() { driver as OracleDriver }

	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', ['locale'])
	}

	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = 'oracle.jdbc.OracleDriver'
	}

	@Override
	protected Class<TableDataset> getTableClass() { OracleTable }
}