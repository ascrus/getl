package getl.xml

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.WebServiceConnection
import getl.driver.Driver
import getl.exception.ExceptionGETL
import groovy.transform.InheritConstructors

/**
 * XML connection class 
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class XMLConnection extends WebServiceConnection {
	@Override
	protected Class<Driver> driverClass() { XMLDriver }

	@Override
	protected void registerParameters() {
		super.registerParameters()

		methodParams.register('Super', ['defaultAccessMethod'])
	}

	/** Current XML connection driver */
	@JsonIgnore
	XMLDriver getCurrentXMLDriver() { driver as XMLDriver }

	/** Use default the attribute access method (default) */
	static public final Integer DEFAULT_ATTRIBUTE_ACCESS = 0
	/** Use default the node access method */
	static public final Integer DEFAULT_NODE_ACCESS = 1

	/** How read field if not specified the alias property
	 * <br>default: DEFAULT_ATTRIBUTE_ACCESS
	 */
	Integer getDefaultAccessMethod() { (params.defaultAccessMethod as Integer)?:DEFAULT_ATTRIBUTE_ACCESS }
	/** How read field if not specified the alias property
	 * <br>default: DEFAULT_ATTRIBUTE_ACCESS
	 */
	void setDefaultAccessMethod(Integer value) {
		if (value != null && !(value in [DEFAULT_NODE_ACCESS, DEFAULT_ATTRIBUTE_ACCESS]))
			throw new ExceptionGETL('Invalid default access method property!')
		params.defaultAccessMethod = value
	}

	@Override
	protected Class<Dataset> getDatasetClass() { XMLDataset }
}
