package getl.dbf

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.FileDataset
import getl.exception.DatasetError
import groovy.transform.InheritConstructors

@InheritConstructors
class DBFDataset extends FileDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof DBFConnection))
            throw new DatasetError(this, '#dataset.invalid_connection', [className: DBFConnection.name])

        super.setConnection(value)
    }

    /** Use specified connection */
    DBFConnection useConnection(DBFConnection value) {
        setConnection(value)
        return value
    }

    /** Current DBF connection */
    @JsonIgnore
    DBFConnection getCurrentDbfConnection() { connection as DBFConnection }

    /** File extension for storing memo fields */
    String getFileMemoExtension() { params.fileMemoExtension as String }
    /** File extension for storing memo fields */
    void setFileMemoExtension(String value) { params.fileMemoExtension = value }
    /** File extension for storing memo fields */
    String fileMemoExtension() { fileMemoExtension?:currentDbfConnection?.fileMemoExtension() }
}