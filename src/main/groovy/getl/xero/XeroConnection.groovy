/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

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