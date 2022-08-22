package getl.transform

import getl.data.Connection
import getl.driver.Driver
import groovy.transform.InheritConstructors

/**
 * Connection for array datasets
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ArrayDatasetConnection extends Connection {
    @Override
    protected Class<Driver> driverClass() { ArrayDatasetDriver }
}