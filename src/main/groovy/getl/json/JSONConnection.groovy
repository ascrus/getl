package getl.json

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.FileConnection
import getl.data.WebServiceConnection
import getl.driver.Driver
import groovy.transform.InheritConstructors

/**
 * JSON connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class JSONConnection extends WebServiceConnection {
	@Override
	protected Class<Driver> driverClass() { JSONDriver }

	/** Current JSON connection driver */
	@JsonIgnore
	JSONDriver getCurrentJSONDriver() { driver as JSONDriver }

	@Override
	protected Class<Dataset> getDatasetClass() { JSONDataset }
}