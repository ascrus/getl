//file:noinspection unused
package getl.dbf

import getl.data.Dataset
import getl.data.FileConnection
import getl.driver.Driver
import groovy.transform.InheritConstructors

@InheritConstructors
class DBFConnection extends FileConnection {
    @Override
    protected Class<Driver> driverClass() { DBFDriver }

    @Override
    protected void registerParameters() {
        super.registerParameters()

        methodParams.register('Super', ['fileMemoExtension'])
    }

    /** File extension for storing memo fields */
    String getFileMemoExtension() { params.fileMemoExtension as String }
    /** File extension for storing memo fields */
    void setFileMemoExtension(String value) { params.fileMemoExtension = value }
    /** File extension for storing memo fields */
    String fileMemoExtension() { fileMemoExtension?:'dbt' }

    @Override
    String codePage() { codePage?:'cp866' }

    @Override
    protected Class<Dataset> getDatasetClass() { DBFDataset }
}