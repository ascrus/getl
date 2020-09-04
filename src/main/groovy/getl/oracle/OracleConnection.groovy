package getl.oracle

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset

/**
 * Oracle connection class
 * @author Alexsey Konstantinov
 *
 */
class OracleConnection extends JDBCConnection {
	OracleConnection() {
		super(driver: OracleDriver)
	}
	
	OracleConnection(Map params) {
		super(new HashMap([driver: OracleDriver]) + params?:[:])
		if (this.getClass().name == 'getl.oracle.OracleConnection') methodParams.validation('Super', params?:[:])
	}

	/** Current Oracle connection driver */
	@JsonIgnore
	OracleDriver getCurrentOracleDriver() { driver as OracleDriver }

	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', ['locale'])
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.oracle.OracleConnection') methodParams.validation('Super', params)
	}
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = 'oracle.jdbc.OracleDriver'
	}

	@Override
	protected Class<TableDataset> getTableClass() { OracleTable }
}