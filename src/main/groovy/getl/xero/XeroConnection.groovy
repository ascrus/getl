package getl.xero

import getl.data.Connection
import getl.utils.FileUtils
import getl.utils.StringUtils
import groovy.transform.InheritConstructors

/**
 * Xero connection manager
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
public class XeroConnection extends Connection {
    XeroConnection() {
        super(driver: XeroDriver)
    }

    XeroConnection (Map params) {
        super(new HashMap([driver: XeroDriver]) + params)

        if (this.getClass().name == 'getl.xero.XeroConnection') {
            methodParams.validation("Super", params)
        }
    }

    @Override
    protected void registerParameters () {
        super.registerParameters()
        methodParams.register('Super', ['useResourceFile', 'configInResource', 'historyFile'])
    }

    @Override
    protected void onLoadConfig (Map configSection) {
        super.onLoadConfig(configSection)

        if (this.getClass().name == 'getl.xero.XeroConnection') {
            methodParams.validation('Super', params)
        }
    }

    /**
     * Use resource file with configuration file and certificate file
     */
    public String getUseResourceFile () { params.useResourceFile }
    public void setUseResourceFile(String value) {
        params.useResourceFile = value
    }

    /**
     * Config file name by resource
     */
    public String getConfigInResource () { params.configInResource }
    public void setConfigInResource (String value) {
        params.configInResource = value
    }

    /**
     * Command history file
     */
    public String getHistoryFile () { params.historyFile }
    public void setHistoryFile(String value) { params.historyFile = value }

    /**
     * Real script history file name
     */
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