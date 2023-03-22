//file:noinspection unused
package getl.vertica.opts

import getl.exception.ExceptionGETL
import getl.jdbc.opts.BulkLoadSpec
import getl.utils.BoolUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Options for bulk loading Vertica table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VerticaBulkLoadSpec extends BulkLoadSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.parser == null)
            params.parser = new HashMap<String, String>()
    }

    /**
     * Auto detect how load rows
     */
    static public final String AUTO = 'AUTO'

    /**
     * Load rows to ROS
     */
    static public final String DIRECT = 'DIRECT'

    /**
     * Load rows to WOS
     */
    static public final String TRICKLE = 'TRICKLE'

    /**
     * Uncompressed file
     */
    static public final String UNCOMPRESSED = 'UNCOMPRESSED'

    /**
     * BZIP compress file
     */
    static public final String BZIP = 'BZIP'

    /**
     * GZIP compress file
     */
    static public final String GZIP = 'GZIP'

    /**
     * LZO compress file
     */
    static public final String LZO = 'LZO'

    /**
     * ZSTD compress file
     */
    static public final String ZSTD = 'ZSTD'

    /** Specified parser for loading data from file */
    VerticaBulkLoadParserSpec getParser() {
        new VerticaBulkLoadParserSpec(ownerObject, true, params.parser as Map<String, Object>)
    }

    /** Specified parser for loading data from file */
    VerticaBulkLoadParserSpec parser(@DelegatesTo(VerticaBulkLoadParserSpec)
                @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaBulkLoadParserSpec'])
                        Closure cl) {
        def parent = parser
        parent.runClosure(cl)

        return parent
    }

    /**
     * Load method (AUTO, DIRECT, TRICKLE)
     */
    String getLoadMethod() { params.loadMethod as String }
    /**
     * Load method (AUTO, DIRECT, TRICKLE)
     */
    void setLoadMethod(String value) {
        if (loadMethod != null) {
            value = value.trim().toUpperCase()
            if (!(value in ['AUTO', 'DIRECT', 'TRICKLE']))
                throw new ExceptionGETL("Invalid load option \"$value\", allowed: AUTO, DIRECT AND TRICKLE!")
        }
        saveParamValue('loadMethod', value)
    }

    /**
     * Determines whether load truncates or rejects data rows of type char, varchar, binary, and varbinary if they do not fit the target table.
     * <br>Specifying the optional ENFORCELENGTH parameter rejects rows.
     * <br>Default: true value
     */
    Boolean getEnforceLength() { BoolUtils.IsValue(params.enforceLength, true) }
    /**
     * Determines whether load truncates or rejects data rows of type char, varchar, binary, and varbinary if they do not fit the target table.
     * <br>Specifying the optional ENFORCELENGTH parameter rejects rows.
     * <br>Default: true value
     */
    void setEnforceLength(Boolean value) { saveParamValue('enforceLength', value) }

    /**
     * Specifies the input format file
     * <br>One of the following: UNCOMPRESSED (default), BZIP, GZIP, LZO, ZSTD
     */
    String getCompressed() { params.compressed as String }
    /**
     * Specifies the input format file
     * <br>One of the following: UNCOMPRESSED (default), BZIP, GZIP, LZO, ZSTD
     */
    void setCompressed(String value) {
        if (params.compressed != null) {
            value = value.trim().toUpperCase()
            if (!(value in ['UNCOMPRESSED', 'BZIP', 'GZIP', 'LZO', 'ZSTD']))
                throw new ExceptionGETL("Invalid compression type \"$value\", allowed: UNCOMPRESSED, BZIP, GZIP, LZO, ZSTD!")
        }
        saveParamValue('compressed', value)
    }

    /**
     * Specifies the file name or absolute path of the file in which to write exceptions.
     */
    String getExceptionPath() { params.exceptionPath as String }
    /**
     * Specifies the file name or absolute path of the file in which to write exceptions.
     */
    void setExceptionPath(String value) { saveParamValue('exceptionPath', value) }

    /**
     * Specifies the file name or absolute path to write each row that failed to load. If this parameter is specified, records that failed due to parsing errors are always written.
     */
    String getRejectedPath() { params.rejectedPath as String }
    /**
     * Specifies the file name or absolute path to write each row that failed to load. If this parameter is specified, records that failed due to parsing errors are always written.
     */
    void setRejectedPath(String value) { saveParamValue('rejectedPath', value) }

    /**
     * Specifies a maximum number of logical records that can be rejected before a load fails.
     */
    Long getRejectMax() { params.rejectMax as Long }
    /**
     * Specifies a maximum number of logical records that can be rejected before a load fails.
     */
    void setRejectMax(Long value) { saveParamValue('rejectMax', value) }

    /**
     * Specifies where files are located. if not specified, local files are loaded.
     * <br>Examples:
     * <br>location = 'V_VMART_0001'
     * <br>location = '(VMART_0001,VMART_0002,VMART_0003)'
     * <br>location = 'ANY NODE
     */
    @SuppressWarnings('SpellCheckingInspection')
    String getLocation() { params.location as String }
    /**
     * Specifies where files are located. if not specified, local files are loaded.
     * <br><br>Examples:<ul>
     * <li>location = 'V_VMART_0001'
     * <li>location = '(VMART_0001,VMART_0002,VMART_0003)'
     * <li>location = 'ANY NODE
     * </ul>
     */
    void setLocation(String value) { saveParamValue('location', value) }

    /**
     * Supplies a COPY load stream identifier. Using a stream name helps to quickly identify a particular load. The STREAM NAME value that you supply
     * in the load statement appears in the STREAM_NAME column of the LOAD_STREAMS and LOAD_SOURCES system tables
     */
    String getStreamName() { params.streamName as String }
    /**
     * Supplies a COPY load stream identifier. Using a stream name helps to quickly identify a particular load. The STREAM NAME value that you supply
     * in the load statement appears in the STREAM_NAME column of the LOAD_STREAMS and LOAD_SOURCES system tables
     */
    void setStreamName(String value) { saveParamValue('streamName', value) }

    /** Specifies the format for parsing date type columns in file */
    String getFormatDate() { params.formatDate as String }
    /** Specifies the format for parsing date type columns in file */
    void setFormatDate(String value) { saveParamValue('formatDate', value) }

    /** Specifies the format for parsing time type columns in file */
    String getFormatTime() { params.formatTime as String }
    /** Specifies the format for parsing time type columns in file */
    void setFormatTime(String value) { saveParamValue('formatTime', value) }

    /** Specifies the format for parsing datetime type columns in file */
    String getFormatDateTime() { params.formatDateTime as String }
    /** Specifies the format for parsing datetime type columns in file */
    void setFormatDateTime(String value) { saveParamValue('formatDateTime', value) }

    /** Escape character */
    String getEscapeChar() { params.escapeChar as String }
    /** Escape character */
    void setEscapeChar(String value) { saveParamValue('escapeChar', value) }
}