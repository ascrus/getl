package getl.excel

import getl.data.Connection

/**
 * Excel Connection class
 * @author Dmitry Shaldin
 *
 */
class ExcelConnection extends Connection {
    ExcelConnection () {
        super(driver: ExcelDriver)
    }

    ExcelConnection (Map params) {
        super(new HashMap([driver: ExcelDriver]) + params)
    }

    /**
     * Connection path
     */
    public String getPath () { params.path }
    public void setPath (String value) { params.path = value }

    /**
     * File name
     */
    public String getFileName () { params.fileName }
    public void setFileName (String value) { params.fileName = value }

    /**
     * Extension for file
     */
    public String getExtension () { params.extension }
    public void setExtension (String value) { params.extension = value }
}
