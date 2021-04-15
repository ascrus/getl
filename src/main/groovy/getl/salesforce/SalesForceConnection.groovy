package getl.salesforce

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.lang.Getl
import getl.lang.sub.UserLogins
import getl.lang.sub.LoginManager
import getl.lang.sub.StorageLogins

/**
 * SalesForce Connection class
 * @author Dmitry Shaldin
 */
class SalesForceConnection extends Connection implements UserLogins {
    SalesForceConnection() {
		super(driver: SalesForceDriver)
	}

	SalesForceConnection(Map params) {
		super(new HashMap([driver: SalesForceDriver]) + params?:[:])

		if (this.getClass().name == 'getl.salesforce.SalesForceConnection') {
			methodParams.validation("Super", params?:[:])
		}
	}

	/** Current SalesForce connection driver */
	@JsonIgnore
	SalesForceDriver getCurrentSalesForceDriver() { driver as SalesForceDriver }

	@Override
	void initParams() {
		super.initParams()
		loginManager = new LoginManager(this)
		params.storedLogins = new StorageLogins(loginManager)
	}

	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('Super', ['login', 'password', 'connectURL', 'batchSize', 'storedLogins'])
	}

	@Override
	protected void onLoadConfig(Map configSection) {
		super.onLoadConfig(configSection)

		if (this.getClass().name == 'getl.salesforce.SalesForceConnection') {
			methodParams.validation('Super', params)
		}
	}

	/** SalesForce login */
	@Override
	String getLogin() { params.login }
	/** SalesForce login */
	@Override
    void setLogin(String value) { params.login = value }

	/** SalesForce password and token */
	@Override
	String getPassword() { params.password }
	/** SalesForce password and token */
	@Override
    void setPassword(String value) { params.password = loginManager.encryptPassword(value) }

	@Override
	Map<String, String> getStoredLogins() { params.storedLogins as Map<String, String> }
	@Override
	void setStoredLogins(Map<String, String> value) {
		storedLogins.clear()
		if (value != null) storedLogins.putAll(value)
	}

	/**
	 * SalesForce SOAP Auth Endpoint
	 * Example: https://login.salesforce.com/services/Soap/u/40.0
	 */
	String getConnectURL() { params.connectURL }
	/**
	 * SalesForce SOAP Auth Endpoint
	 * <br>Example: https://login.salesforce.com/services/Soap/u/40.0
	 */
    void setConnectURL(String value) { params.connectURL = value }

	/**
	 * Batch Size for SalesForce connection
     * <br>This param do nothing for readAsBulk.
	 */
	Integer getBatchSize() { (params.batchSize as Integer)?:200 }
	/**
	 * Batch Size for SalesForce connection
	 * <br>This param do nothing for readAsBulk.
	 */
    void setBatchSize(Integer value) { params.batchSize = value }

	@Override
	protected Class<Dataset> getDatasetClass() { SalesForceDataset }

	/** Logins manager */
	protected LoginManager loginManager

	@Override
	void useLogin(String user) {
		loginManager.useLogin(user)
	}

	@Override
	void switchToNewLogin(String user) {
		loginManager.switchToNewLogin(user)
	}

	@Override
	void switchToPreviousLogin() {
		loginManager.switchToPreviousLogin()
	}

	@Override
	void setDslCreator(Getl value) {
		if (dslCreator != value)
			useDslCreator(value)
	}

	/**
	 * Use new Getl instance
	 * @param value Getl instance
	 */
	protected void useDslCreator(Getl value) {
		def passwords = loginManager.decryptObject()
		sysParams.dslCreator = value
		loginManager.encryptObject(passwords)
	}

	@Override
	protected List<String> ignoreCloneClasses() { [StorageLogins.name] }

	@Override
	protected void afterClone(Connection original) {
		super.afterClone(original)
		def o = original as SalesForceConnection
		def passwords = o.loginManager.decryptObject()
		loginManager.encryptObject(passwords)
	}
}