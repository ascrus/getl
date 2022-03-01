package getl.dbf.proc

import com.linuxense.javadbf.DBFRow
import getl.dbf.sub.ReadProcessor
import groovy.transform.InheritConstructors

import java.sql.Timestamp

/**
 * Convert datetime field by row with DBF file to destination row
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ReadDatetime extends ReadProcessor {
    @Override
    protected Object readValue(DBFRow source) {
        def d = source.getDate(sourceFieldName)
        return (d != null)?new Timestamp(d.time):d
    }
}