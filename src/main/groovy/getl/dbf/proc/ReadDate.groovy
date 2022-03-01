package getl.dbf.proc

import com.linuxense.javadbf.DBFRow
import getl.dbf.sub.ReadProcessor
import groovy.transform.InheritConstructors

/**
 * Convert date field by row with DBF file to destination row
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ReadDate extends ReadProcessor {
    @Override
    protected Object readValue(DBFRow source) {
        def d = source.getDate(sourceFieldName)
        return (d != null)?new java.sql.Date(d.time):null
    }
}