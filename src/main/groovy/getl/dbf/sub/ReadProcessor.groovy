package getl.dbf.sub

import com.linuxense.javadbf.DBFRow
import getl.data.Field
import groovy.transform.CompileStatic

/**
 * Convert field by row with DBF file to destination row
 * @author Alexsey Konstantinov
 */
@CompileStatic
abstract class ReadProcessor {
    ReadProcessor(Field field) {
        this.field = field
        this.sourceFieldName = field.name.toUpperCase()
        this.destFieldName = field.name.toLowerCase()
    }

    /** Source field */
    protected Field field
    /** Field name in DBF source */
    protected String sourceFieldName
    /** Field name in dataset destination */
    protected String destFieldName

    /** Read field value by DBF row */
    abstract protected Object readValue(DBFRow source)

    /**
     * Read field from row and save converted value to destination row
     * @param source
     * @param dest
     */
    void read(DBFRow source, Map<String, Object> dest) {
        def val = readValue(source)
        if (val != null)
            dest.put(destFieldName, val)
    }
}