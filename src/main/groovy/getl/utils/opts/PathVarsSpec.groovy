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

package getl.utils.opts

import getl.data.Field
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

/**
 * Definition variable for path processing class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class PathVarsSpec extends BaseSpec {
    Field.Type getType() { params.type as Field.Type }
    /** Variable value type */
    void setType(Field.Type value) { params.type = value }

    /**
     * Format of parsing variable value
     * <ul>
     * <li>use reqular expression for string type
     * <li>use date and time mask for date-time type
     * </ul>
     */
    String getFormat() { params.format as String }
    /**
     * Format of parsing variable value
     * <ul>
     * <li>use reqular expression for string type
     * <li>use date and time mask for date-time type
     * </ul>
     */
    void setFormat(String value) { params.format = value }

    /** The length of variable value */
    Integer getLength() { params.len as Integer }
    /** The length of variable value */
    void setLength(Integer value) { params.len = value }

    /** The minimum length of variable value */
    Integer getMinimumLength() { params.lenMin as Integer }
    /** The minimum length of variable value */
    void setMinimumLength(Integer value) { params.lenMin = value }

    /** The maximum length of variable value */
    Integer getMaximumLength() { params.lenMax as Integer }
    /** The maximum length of variable value */
    void setMaximumLength(Integer value) { params.lenMax = value }

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
}