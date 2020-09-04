package getl.netsuite

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset

/**
 * Netsuite connection class
 * @author Dmitry Shaldin
 *
 */
class NetsuiteConnection extends JDBCConnection {
	NetsuiteConnection() {
		super(driver: NetsuiteDriver)
	}

	NetsuiteConnection(Map params) {
		super(new HashMap([driver: NetsuiteDriver]) + params?:[:])
		if (this.getClass().name == 'getl.netsuite.NetsuiteConnection') methodParams.validation("Super", params?:[:])
	}

	/** Current Netsuite connection driver */
	@JsonIgnore
	NetsuiteDriver getCurrentNetsuiteDriver() { driver as NetsuiteDriver }

	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', ['serverDataSource', 'ciphersuites', 'accountId'])
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)

		if (this.getClass().name == 'getl.netsuite.NetsuiteConnection') methodParams.validation('Super', params)
	}
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = 'com.netsuite.jdbc.openaccess.OpenAccessDriver'
	}

	/** Server Data Source */
	String getServerDataSource () { params.serverDataSource }
	/** Server Data Source */
    void setServerDataSource (String value) { params.serverDataSource = value }

	/** Ciphersuites */
	String getCiphersuites () { params.ciphersuites }
	/** Ciphersuites */
    void setCiphersuites (String value) { params.ciphersuites = value }

	/** Account ID */
	Integer getAccountId () { params.accountId as Integer }
	/** Account ID */
    void setAccountId (Integer value) { params.accountId = value }

	@Override
	protected Class<TableDataset> getTableClass() { NetsuiteTable }
}