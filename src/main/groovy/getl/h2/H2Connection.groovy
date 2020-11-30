package getl.h2

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.JDBCDriver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.utils.*

/**
 * H2 connection class
 * @author Alexsey Konstantinov
 *
 */
class H2Connection extends JDBCConnection {
	H2Connection() {
		super(driver: H2Driver)
		if (connectProperty.LOCK_TIMEOUT == null) connectProperty.LOCK_TIMEOUT = 10000
		connectProperty.CASE_INSENSITIVE_IDENTIFIERS = true
		connectProperty.ALIAS_COLUMN_NAME = true
	}
	
	H2Connection(Map params) {
		super(new HashMap([driver: H2Driver]) + params?:[:])
		if (connectProperty.LOCK_TIMEOUT == null) connectProperty.LOCK_TIMEOUT = 10000
		connectProperty.CASE_INSENSITIVE_IDENTIFIERS = true
		connectProperty.ALIAS_COLUMN_NAME = true
		if (this.getClass().name == 'getl.h2.H2Connection') methodParams.validation('Super', params?:[:])
	}

	/** Current H2 connection driver */
	@JsonIgnore
	H2Driver getCurrentH2Driver() { driver as H2Driver }
	
	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', ['inMemory', 'exclusive'])
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.h2.H2Connection') methodParams.validation('Super', params)
	}
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = 'org.h2.Driver'
	}
	
	/** Enabled "in-memory" mode */
	Boolean getInMemory () { BoolUtils.IsValue(params.inMemory, false) }
	/** Enabled "in-memory" mode */
	void setInMemory (Boolean value) { params.inMemory = value }

    /** Exclusive connection */
	Integer getExclusive() { sessionProperty.exclusive as Integer }
	/** Exclusive connection */
	void setExclusive(Integer value) {
        if (connected && exclusive != value) (driver as JDBCDriver).changeSessionProperty('exclusive', value)
        sessionProperty.exclusive = value
    }

	@Override
	protected Class<TableDataset> getTableClass() { H2Table }

	@Override
	String currentConnectDatabase() { FileUtils.TransformFilePath(connectDatabase, false) }
}