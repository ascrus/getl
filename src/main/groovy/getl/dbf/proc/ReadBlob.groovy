package getl.dbf.proc

import com.linuxense.javadbf.DBFRow
import getl.dbf.sub.ReadProcessor
import groovy.transform.InheritConstructors

/**
 * Convert string field by row with DBF file to destination row
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ReadBlob extends ReadProcessor {
    @Override
    protected Object readValue(DBFRow source) {
        source.getBytes(sourceFieldName)
    }
}