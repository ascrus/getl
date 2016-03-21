package getl.excel

import getl.data.Connection
import getl.data.Dataset

/**
 * Excel Dataset class
 * @author Dmitry Shaldin
 *
 */
class ExcelDataset extends Dataset {
    ExcelDataset () {
        super()
    }

    @Override
    public void setConnection(Connection value) {
        assert value == null || value instanceof ExcelConnection
        super.setConnection(value)
    }

    /**
     * List name
     */
    public String getListName () { params.listName }
    public void setListName (String value) { params.listName = value }
}
