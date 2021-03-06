package getl.h2

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.JDBCDriver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.utils.*
import groovy.transform.InheritConstructors

/**
 * H2 connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class H2Connection extends JDBCConnection {
	@Override
	protected Class<Driver> driverClass() { H2Driver }

	@Override
	protected void initParams() {
		super.initParams()

		if (connectProperty.LOCK_TIMEOUT == null)
			connectProperty.LOCK_TIMEOUT = 10000
		connectProperty.CASE_INSENSITIVE_IDENTIFIERS = true
		connectProperty.ALIAS_COLUMN_NAME = true
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