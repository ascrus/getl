package getl.excel

import getl.data.Connection
import getl.data.Dataset

/**
 * Excel Dataset class
 * @author Dmitry Shaldin
 */
class ExcelDataset extends Dataset {
    ExcelDataset () {
        super()
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
    void setListName (String value) { params.listName = value }

    /**
     * Params for start and end position
     * @return
     */
    int getStartRow() { params.startRow }
    void setStartRow(int value) { params.startRow = value }

    int getStartCell() { params.startCell }
    void setStartCell(int value) { params.startCell = value }

    int getEndRow() { params.endRow }
    void setEndRow(int value) { params.endRow = value }

    int getEndCell() { params.endCell }
    void setEndCell(int value) { params.endCell = value }

    /**
     * Limit rows to return
     * @return
     */
    int getLimit() { params.limit }
    void setLimit(int value) { params.limit = value }

    /**
     * Header row
     * @return
     */
    boolean getHeader() { params.header }
    void setHeader(boolean value) { params.header = value }
}
