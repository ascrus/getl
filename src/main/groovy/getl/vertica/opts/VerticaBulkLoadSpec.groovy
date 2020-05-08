/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

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
        if (params.parser == null) params.parser = [:] as Map<String, String>
        if (params.expression == null) params.expression = [:] as Map<String, String>
    }

    /**
     * Auto detect how load rows
     */
    final static AUTO = 'AUTO'

    /**
     * Load rows to ROS
     */
    final static DIRECT = 'DIRECT'

    /**
     * Load rows to WOS
     */
    final static TRICKLE = 'TRICKLE'

    /**
     * Uncompressed file
     */
    final static UNCOMPRESSED = 'UNCOMPRESSED'

    /**
     * BZIP compress file
     */
    final static BZIP = 'BZIP'

    /**
     * GZIP compress file
     */
    final static GZIP = 'GZIP'

    /**
     * LZO compress file
     */
    final static LZO = 'LZO'

    /**
     * ZSTD compress file
     */
    final static ZSTD = 'ZSTD'

    /**
     * Specified parser for loading data from file
     */
    Map<String, Object> getParser() { params.parser as Map<String, Object> }
    /**
     * Specified parser for loading data from file
     */
    void setParser(Map<String, Object> value) {
        parser.clear()
        if (value != null) parser.putAll(value)
    }
    /**
     * Specified parser for loading data from file
     */
    void parser(@DelegatesTo(VerticaBulkLoadParserSpec)
                @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaBulkLoadParserSpec'])
                        Closure cl) {
        def parent = new VerticaBulkLoadParserSpec(ownerObject, true, parser)
        parent.runClosure(cl)
        parser = parent.params
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
        params.loadMethod = value
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
    void setEnforceLength(Boolean value) { params.enforceLength = value }

    /** Stop loading rows on any error (default true) */
    //Boolean getAbortOnError() { BoolUtils.IsValue(params.abortOnError, true) }
    /** Stop loading rows on any error (default true) */
    //void setAbortOnError(Boolean value) { params.abortOnError = value }

    /**
     * Describes the SQL expression of loading file columns into table fields.
     * <br>Example: [table_field1: 'Upper(file_col1)']
     */
    Map<String, String> getExpression() { params.expression as Map<String, String> }
    /**
     * Describes the expression of loading file columns into table fields.
     * <br>Example: [file_column_1: 'FILLER varchar(50)', field_in_table: 'Upper(col1)']
     */
    void setExpression(Map<String, String> value) {
        expression.clear()
        if (value != null) expression.putAll(value)
    }

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
        params.compressed = value
    }

    /**
     * Specifies the file name or absolute path of the file in which to write exceptions.
     */
    String getExceptionPath() { params.exceptionPath as String }
    /**
     * Specifies the file name or absolute path of the file in which to write exceptions.
     */
    void setExceptionPath(String value) { params.exceptionPath = value }

    /**
     * Specifies the file name or absolute path to write each row that failed to load. If this parameter is specified, records that failed due to parsing errors are always written.
     */
    String getRejectedPath() { params.rejectedPath as String }
    /**
     * Specifies the file name or absolute path to write each row that failed to load. If this parameter is specified, records that failed due to parsing errors are always written.
     */
    void setRejectedPath(String value) { params.rejectedPath = value }

    /**
     * Specifies a maximum number of logical records that can be rejected before a load fails.
     */
    Long getRejectMax() { params.rejectMax as Long }
    /**
     * Specifies a maximum number of logical records that can be rejected before a load fails.
     */
    void setRejectMax(Long value) { params.rejectMax = value }

    /**
     * Specifies where files are located. if not specified, local files are loaded.
     * <br>Examples:
     * <br>location = 'V_VMART_0001'
     * <br>location = '(VMART_0001,VMART_0002,VMART_0003)'
     * <br>location = 'ANY NODE
     */
    String getLocation() { params.location as String }
    /**
     * Specifies where files are located. if not specified, local files are loaded.
     * <br><br>Examples:<ul>
     * <li>location = 'V_VMART_0001'
     * <li>location = '(VMART_0001,VMART_0002,VMART_0003)'
     * <li>location = 'ANY NODE
     * </ul>
     */
    void setLocation(String value) { params.location = value }

    /**
     * Supplies a COPY load stream identifier. Using a stream name helps to quickly identify a particular load. The STREAM NAME value that you supply in the load statement appears in the STREAM_NAME column of the LOAD_STREAMS and LOAD_SOURCES system tables.
     */
    String getStreamName() { params.streamName as String }
    /**
     * Supplies a COPY load stream identifier. Using a stream name helps to quickly identify a particular load. The STREAM NAME value that you supply in the load statement appears in the STREAM_NAME column of the LOAD_STREAMS and LOAD_SOURCES system tables.
     */
    void setStreamName(String value) { params.streamName = value }

    /**
     * Specifies the format for parsing date type columns in file.
     */
    String getMaskDate() { params.maskDate as String }
    /**
     * Specifies the format for parsing date type columns in file.
     */
    void setMaskDate(String value) { params.maskDate = value }

    /**
     * Specifies the format for parsing time type columns in file.
     */
    String getMaskTime() { params.maskTime as String }
    /**
     * Specifies the format for parsing time type columns in file.
     */
    void setMaskTime(String value) { params.maskTime = value }

    /**
     * Specifies the format for parsing datetime type columns in file.
     */
    String getMaskDateTime() { params.maskDateTime as String }
    /**
     * Specifies the format for parsing datetime type columns in file.
     */
    void setMaskDateTime(String value) { params.maskDateTime = value }
}