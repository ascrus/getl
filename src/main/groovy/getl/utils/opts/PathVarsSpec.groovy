package getl.utils.opts

import getl.data.Field
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Definition variable for path processing class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class PathVarsSpec extends BaseSpec {
    /** Integer field type */
    static public final Field.Type integerFieldType = Field.Type.INTEGER
    /** Bigint field type */
    static public final Field.Type bigintFieldType = Field.Type.BIGINT
    /** Numeric (decimal) field type */
    static public final Field.Type numericFieldType = Field.Type.NUMERIC
    /** Double field type */
    static public final Field.Type doubleFieldType = Field.Type.DOUBLE
    /** String field type */
    static public final Field.Type stringFieldType = Field.Type.STRING
    /** Text (clob) field type */
    static public final Field.Type textFieldType = Field.Type.TEXT
    /** Date field type */
    static public final Field.Type dateFieldType = Field.Type.DATE
    /** Time field type */
    static public final Field.Type timeFieldType = Field.Type.TIME
    /** Date and time field type */
    static public final Field.Type datetimeFieldType = Field.Type.DATETIME
    /** Timestamp with time zone field type */
    static public final Field.Type timestamp_with_timezoneFieldType = Field.Type.TIMESTAMP_WITH_TIMEZONE
    /** Boolean field type */
    static public final Field.Type booleanFieldType = Field.Type.BOOLEAN
    /** Blob field type */
    static public final Field.Type blobFieldType = Field.Type.BLOB
    /** UUID field type */
    static public final Field.Type uuidFieldType = Field.Type.UUID
    /** RowID field type */
    static public final Field.Type rowidFieldType = Field.Type.ROWID
    /** Object field type */
    static public final Field.Type objectFieldType = Field.Type.OBJECT

    /** Variable value type */
    Field.Type getType() { params.type as Field.Type }
    /** Variable value type */
    void setType(Field.Type value) { saveParamValue('type', value) }

    /**
     * Format of parsing variable value
     * <ul>
     * <li>use regular expression for string type
     * <li>use date and time mask for date-time type
     * </ul>
     */
    String getFormat() { params.format as String }
    /**
     * Format of parsing variable value
     * <ul>
     * <li>use regular expression for string type
     * <li>use date and time mask for date-time type
     * </ul>
     */
    void setFormat(String value) { saveParamValue('format', value) }

    /** The length of variable value */
    Integer getLength() { params.len as Integer }
    /** The length of variable value */
    void setLength(Integer value) { saveParamValue('len', value) }

    /** The minimum length of variable value */
    Integer getMinimumLength() { params.lenMin as Integer }
    /** The minimum length of variable value */
    void setMinimumLength(Integer value) { saveParamValue('lenMin', value) }

    /** The maximum length of variable value */
    Integer getMaximumLength() { params.lenMax as Integer }
    /** The maximum length of variable value */
    void setMaximumLength(Integer value) { saveParamValue('lenMax', value) }

    /** Value calculation code */
    Closure getOnCalc() { params.calc as Closure }
    /** Value calculation code */
    void setOnCalc(Closure value) { saveParamValue('calc', value) }
    /** Value calculation code */
    void calc(@ClosureParams(value = SimpleType, options = ['java.util.Map<String, Object>'])
                      Closure value) {
        setOnCalc(value)
    }
}