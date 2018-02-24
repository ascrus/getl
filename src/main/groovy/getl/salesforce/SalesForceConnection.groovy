package getl.salesforce

import getl.data.Connection
import groovy.transform.InheritConstructors

/**
 * SalesForce Connection class
 * @author Dmitry Shaldin
 */
@InheritConstructors
class SalesForceConnection extends Connection {
	SalesForceConnection () {
		super(driver: SalesForceDriver)
	}

	SalesForceConnection (Map params) {
		super(new HashMap([driver: SalesForceDriver]) + params)

		if (this.getClass().name == 'getl.salesforce.SalesForceConnection') {
			methodParams.validation("Super", params)
		}
	}

	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', ['login', 'password', 'connectURL'])
	}

	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)

		if (this.getClass().name == 'getl.salesforce.SalesForceConnection') {
			methodParams.validation('Super', params)
		}
	}

	/**
	 * SalesForce login
	 * @return
	 */
	public String getLogin () { params.login }
	public void setLogin (String value) { params.login = value }

	/**
	 * SalesForce password and token
	 * @return
	 */
	public String getPassword () { params.password }
	public void setPassword (String value) { params.password = value }

	/**
	 * SalesForce SOAP Auth Endpoint
	 */
	public String getConnectURL () { params.connectURL }
	public void setConnectURL (String value) { params.connectURL = value }
}
