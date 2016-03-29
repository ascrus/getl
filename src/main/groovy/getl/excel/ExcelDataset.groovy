package getl.excel

import getl.data.Connection
import getl.data.Dataset
import getl.utils.FileUtils

/**
 * Excel Dataset class
 * @author Dmitry Shaldin
 */
class ExcelDataset extends Dataset {
    ExcelDataset () {
        super()
        params.header = true
    }

    @Override
    void setConnection(Connection value) {
        assert value == null || value instanceof ExcelConnection
        super.setConnection(value)
    }

    /**
     * List name
     * @return
     */
    String getListName () { params.listName }
    void setListName (final String value) { params.listName = value }

    /**
     * Offset param
     * @return
     */
    int getOffset() { params.offset }
    void setOffset(final Map<String, Integer> value) { params.offset = value }

    /**
     * Limit rows to return
     * @return
     */
    int getLimit() { params.limit }
    void setLimit(final int value) { params.limit = value }

    /**
     * Header row
     * @return
     */
    boolean getHeader() { params.header }
    void setHeader(final boolean value) { params.header = value }

    @Override public String getObjectName() { connection.params.fileName }
    @Override public String getObjectFullName() { FileUtils.ConvertToDefaultOSPath(connection.params.path + File.separator + objectName) }
}
