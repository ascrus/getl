package getl.dbf.proc

import com.linuxense.javadbf.DBFRow
import getl.dbf.sub.ReadProcessor
import groovy.transform.InheritConstructors

/**
 * Convert bigint field by row with DBF file to destination row
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ReadBigint extends ReadProcessor {
    @Override
    protected Object readValue(DBFRow source) {
        source.getLong(sourceFieldName)
    }
}