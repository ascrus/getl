package getl.dbf.proc

import com.linuxense.javadbf.DBFRow
import getl.dbf.sub.ReadProcessor
import groovy.transform.InheritConstructors

/**
 * Convert integer field by row with DBF file to destination row
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ReadInteger extends ReadProcessor {
    @Override
    protected Object readValue(DBFRow source) {
        source.getInt(sourceFieldName)
    }
}