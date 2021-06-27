package getl.xero

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.driver.Driver
import getl.utils.FileUtils
import getl.utils.StringUtils
import groovy.transform.InheritConstructors

/**
 * Xero connection manager
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class XeroConnection extends Connection {
    @Override
    protected Class<Driver> driverClass() { XeroDriver }

    /** Current Xero connection driver */
    @JsonIgnore
    XeroDriver getCurrentXeroDriver() { driver as XeroDriver }

    @Override
    protected void registerParameters () {
        super.registerParameters()

        methodParams.register('Super', ['useResourceFile', 'configInResource', 'historyFile'])
    }

    /** Use resource file with configuration file and certificate file */
    String getUseResourceFile () { params.useResourceFile }
    /** Use resource file with configuration file and certificate file */
    void setUseResourceFile(String value) {
        params.useResourceFile = value
    }

    /** Config file name by resource */
    String getConfigInResource () { params.configInResource }
    /** Config file name by resource */
    void setConfigInResource (String value) {
        params.configInResource = value
    }

    /** Command history file */
    String getHistoryFile () { params.historyFile }
    /** Command history file */
    void setHistoryFile(String value) { params.historyFile = value }

    @Override
    protected Class<Dataset> getDatasetClass() { XeroDataset }

    /** Real script history file name */
    protected String fileNameHistory

    /**
     * Validation script history file
     */
    protected validHistoryFile () {
        if (fileNameHistory == null) {
            fileNameHistory = StringUtils.EvalMacroString(historyFile, StringUtils.MACROS_FILE)
            FileUtils.ValidFilePath(fileNameHistory)
        }
    }
}