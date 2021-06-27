package getl.yaml

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.WebServiceConnection
import getl.driver.Driver
import groovy.transform.InheritConstructors

/**
 * JSON connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class YAMLConnection extends WebServiceConnection {
    @Override
    protected Class<Driver> driverClass() { YAMLDriver }

    /** Current JSON connection driver */
    @JsonIgnore
    YAMLDriver getCurrentYAMLDriver() { driver as YAMLDriver }

    @Override
    protected Class<Dataset> getDatasetClass() { YAMLDataset }
}
