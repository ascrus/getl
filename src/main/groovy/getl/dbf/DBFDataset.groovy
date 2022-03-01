package getl.dbf

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.FileDataset
import getl.exception.ExceptionGETL
import groovy.transform.InheritConstructors

@InheritConstructors
class DBFDataset extends FileDataset {
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