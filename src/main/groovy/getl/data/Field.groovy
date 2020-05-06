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

package getl.data

import getl.exception.ExceptionGETL
import getl.utils.*
import groovy.transform.Synchronized

/**
 * Base field class
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings("UnnecessaryQualifiedReference")
class Field implements Serializable, Cloneable {
	/**
	 * Data type
	 */
	static enum Type {
		STRING, INTEGER, BIGINT, NUMERIC, DOUBLE, BOOLEAN, DATE, TIME, DATETIME, TIMESTAMP_WITH_TIMEZONE,
		BLOB, TEXT, OBJECT, ROWID, UUID
	}

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

	private String name = null
	/**
	 * Field name
	 */
	String getName() { return this.name }
	/**
	 * Field name
	 */
	void setName(String value) { this.name = value }
	
	private Type type = Type.STRING
	/**
	 * Data type
	 */
	Type getType() { return this.type }
	/**
	 * Data type
	 */
	void setType(Type value) { this.type = value }
	
	/**
	 * Database field type
	 */
	public Object dbType
	
	/**
	 * Database field name
	 */
	public String typeName
	
	private Boolean isNull = true
	/**
	 * Value can not be null
	 */
	Boolean getIsNull() { return this.isNull }
	/**
	 * Value can not be null
	 */
	void setIsNull(Boolean value) { this.isNull = value }
	
	private Integer length
	/**
	 * Length of value
	 */
	Integer getLength() { return this.length }
	/**
	 * Length of value
	 */
	void setLength(Integer value) { this.length = value }
	
	private Integer precision
	/**
	 * Precision number of numeric value
	 */
	Integer getPrecision() { return this.precision }
	/**
	 * Precision number of numeric value
	 */
	void setPrecision(Integer value) { this.precision = value }
	
	private Boolean isKey = false
	/**
	 * Field is primary key
	 */
	Boolean getIsKey() { return this.isKey }
	/**
	 * Field is primary key
	 */
	void setIsKey(Boolean value) {
		this.isKey = value
		if (this.isKey) this.isNull = false else this.ordKey = null
	}
	
	private Integer ordKey
	/**
	 * Number order from primary key
	 */
	Integer getOrdKey() { return this.ordKey }
	/**
	 * Number order from primary key
	 */
	void setOrdKey(Integer value) { ordKey = value }

	private Boolean isPartition = false
	/**
	 * Use field in partition key
	 */
	Boolean getInPartition() { return this.isPartition}
	/**
	 * Use field in partition key
	 */
	void setIsPartition(Boolean value) { this.isPartition = value }

	private Integer ordPartition
	/**
	 * Number order if field use in partition key
	 */
	Integer getOrdPartition() { return this.ordPartition }
	/**
	 * Number order if field use in partition key
	 */
	void setOrdPartition(Integer value) { this.ordPartition = value }
	
	private Boolean isAutoincrement = false
	/**
	 * Field is auto increment
	 */
	Boolean getIsAutoincrement() { return this.isAutoincrement }
	/**
	 * Field is auto increment
	 */
	void setIsAutoincrement(Boolean value) { this.isAutoincrement = value }
	
	private Boolean isReadOnly = false
	/**
	 * Field can not write
	 */
	Boolean getIsReadOnly() { return this.isReadOnly }
	/**
	 * Field can not write
	 */
	void setIsReadOnly(Boolean value) { this.isReadOnly = value }
	
	private String defaultValue = null
	/**
	 * Default value from field (used only creating dataset)
	 */
	String getDefaultValue() { return this.defaultValue }
	/**
	 * Default value from field (used only creating dataset)
	 */
	void setDefaultValue(String value) { this.defaultValue = value }
	
	private String compute
	/**
	 * Compute columns
	 */
	String getCompute() { return this.compute }
	/**
	 * Compute columns
	 */
	void setCompute(String value) { this.compute = value }
	
	private def minValue = null
	/**
	 * Minimum value (for validation and generation)
	 */
	def getMinValue() { return this.minValue }
	/**
	 * Minimum value (for validation and generation)
	 */
	void setMinValue(def value) { this.minValue = value }
	
	private def maxValue = null
	/**
	 * Minimum value (for validation and generation)
	 */
	def getMaxValue() { return this.maxValue }
	/**
	 * Minimum value (for validation and generation)
	 */
	void setMaxValue(def value) { this.maxValue = value }
	
	private String format
	/**
	 * Format pattern on numeric and datetime fields
	 */
	String getFormat () { return this.format }
	/**
	 * Format pattern on numeric and datetime fields
	 */
	void setFormat(String value) { this.format = value }
	
	private String alias
	/**
	 * Name of the field in the data source (if different from Field name)
	 */
	String getAlias() { return this.alias }
	/**
	 * Name of the field in the data source (if different from Field name)
	 */
	void setAlias(String value) { this.alias = value }
	
	private Boolean trim = false
	/**
	 * Trim space (used for reading datasource)
	 */
	Boolean getTrim() { return this.trim }
	/**
	 * Trim space (used for reading datasource)
	 */
	void setTrim(Boolean value) { this.trim = value }
	
	private String decimalSeparator
	/**
	 * Decimal separator
	 */
	String getDecimalSeparator() { return this.decimalSeparator }
	/**
	 * Decimal separator
	 */
	void setDecimalSeparator (String value) { this.decimalSeparator = value }
	
	private String description
	/**
	 * Field description (comments)
	 */
	String getDescription() { return this.description }
	/**
	 * Field description (comments)
	 */
	void setDescription(String value) { this.description = value }
	
	private final Map extended = [:] as Map<String, Object>
	/**
	 * Extended attributes
	 */
	Map getExtended() { return this.extended }
	/**
	 * Extended attributes
	 */
	void setExtended (Map value) {
		this.extended.clear()
		if (value != null) this.extended.putAll(value)
	}
	
	/**
	 * Get value method
	 */
	public String getMethod

	/**
	 * Allow length for field
	 */
	static Boolean AllowLength(Field f) {
		return (f.type in [Field.Type.STRING, Field.Type.NUMERIC, Field.Type.BLOB, Field.Type.TEXT, Field.Type.ROWID])
	}
	
	/**
	 * Allow precision for field
	 */
	static Boolean AllowPrecision(Field f) {
		return (f.type in [Field.Type.NUMERIC])
	}
	
	/**
	 * Allow create field in table
	 */
	static Boolean AllowCreatable(Field f) {
		return !(f.type in [Field.Type.ROWID])
	}
	
	/**
	 * Build map from field
	 */
	Map toMap() {
		def n = [:]
		n.name = name
		n.type = type.toString()
		if (typeName != null) n."typeName" = typeName
		
		if (AllowLength(this) && length != null) n.length = length
		if (AllowPrecision(this) && precision != null) n.precision = precision
		if (!isNull) n.isNull = isNull
		if (isKey) n.isKey = isKey
		if (ordKey != null) n.ordKey = ordKey
		if (isPartition) n.isPartition = isPartition
		if (ordPartition != null) n.ordPartition = ordPartition
		if (isAutoincrement) n.isAutoincrement = isAutoincrement
		if (isReadOnly) n.isReadOnly = isReadOnly
		if (defaultValue != null) n.defaultValue = defaultValue
		if (compute != null) n.compute = compute
		if (format != null) n.format = format
		if (alias != null) n.alias = alias
		if (trim) n.trim = trim
		if (decimalSeparator != null) n.decimalSeparator = decimalSeparator
		if (description != null) n.description = description
		
		return n
	}
	
	/**
	 * Parse map to field
	 * @param strField
	 * @return
	 */
	static Field ParseMap(Map strField) {
		if (strField == null) throw new ExceptionGETL("Can not parse null Map to fields")
		String name = strField.name
		if (strField.name == null) throw new ExceptionGETL("Required field name: ${strField}")
		Field.Type type = (strField.type as Field.Type)?:Field.Type.STRING
		String typeName = strField.typeName
		Boolean isNull = BoolUtils.IsValue(strField.isNull,true)
		def length = strField.length as Integer
		def precision = strField.precision as Integer
		def isKey = BoolUtils.IsValue(strField.isKey, false)
		def ordKey = strField.ordKey as Integer
		def isPartition = BoolUtils.IsValue(strField.isPartition, false)
		def ordPartition = strField.ordPartition as Integer
		def isAutoincrement = BoolUtils.IsValue(strField.isAutoincrement, false)
		def isReadOnly = BoolUtils.IsValue(strField.isReadOnly, false)
		def defaultValue = strField.defaultValue as String
		def compute = strField.compute as String
		def minValue = strField.minValue
		def maxValue = strField.maxValue
		def format = strField.format as String
		def alias = strField.alias as String
		def trim = BoolUtils.IsValue(strField.trim,false)
		def decimalSeparator = strField.decimalSeparator as String
		def description = strField.description as String
		def extended = strField.extended as Map<String, Object>
		
		return new Field(
					name: name, type: type, typeName: typeName, isNull: isNull, length: length, precision: precision,
					isKey: isKey, ordKey: ordKey, isPartition: isPartition, ordPartition: ordPartition,
					isAutoincrement: isAutoincrement, isReadOnly: isReadOnly,
					defaultValue: defaultValue, compute: compute, minValue: minValue, maxValue: maxValue,
					format: format, alias: alias, trim: trim,
					decimalSeparator: decimalSeparator, description: description, extended: extended
		)
	}
	
	/**
	 * Attribute to string
	 */
	String toString() {
		def s = [:]
		s.name = name
		s.type = type
		s.typeName = typeName
		s.isNull = isNull
		s.length = length
		s.precision = precision
		s.isKey = isKey
		s.ordKey = ordKey
		s.isPartition = isPartition
		s.ordPartition = ordPartition
		s.isAutoincrement = isAutoincrement
		s.isReadOnly = isReadOnly
		s.defaultValue = defaultValue
		s.compute = compute
		s.minValue = minValue
		s.maxValue = maxValue
		s.format = format
		s.alias = alias
		s.trim = trim
		s.decimalSeparator = decimalSeparator
		s.description = description
		s.extended = extended
		s.getMethod = getMethod
		
		return MapUtils.ToJson(s)
	}
	
	/**
	 * Assign from field
	 */
	void assign(Field f) {
		type = (f.type != Type.OBJECT)?f.type:type
		typeName = (f.typeName != null)?f.typeName:typeName
		dbType = (f.dbType != null)?f.dbType:dbType
		isNull = (!f.isNull)?false:isNull
		isKey = (f.isKey)?true:isKey
		ordKey = (f.ordKey != null)?f.ordKey:ordKey
		isPartition = (f.isPartition)?true:isPartition
		ordPartition = (f.ordPartition != null)?f.ordPartition:ordPartition
		length = (f.length > 0)?f.length:length
		precision = (f.precision > 0)?f.precision:precision
		isAutoincrement = (f.isAutoincrement)?true:isAutoincrement
		isReadOnly = (f.isReadOnly)?true:isReadOnly
		defaultValue = (f.defaultValue != null)?f.defaultValue:defaultValue
		compute = (f.compute != null)?f.compute:compute
		minValue = (f.minValue != null)?f.minValue:minValue
		maxValue = (f.maxValue != null)?f.maxValue:maxValue
		format = (f.format != null)?f.format:format
		alias = (f.alias != null)?f.alias:alias
		trim = (f.trim != null)?f.trim:trim
		decimalSeparator = (f.decimalSeparator != null)?f.decimalSeparator:decimalSeparator
		description = (f.description != null)?f.description:description
		if (f.extended != null) MapUtils.MergeMap(extended, f.extended)
	}
	
	/**
	 * Clone field
	 */
	@Synchronized
	Field copy() {
		return new Field(
				name: this.name, type: this.type, typeName: this.typeName, dbType: this.dbType, isNull: this.isNull,
				length: this.length, precision: this.precision, isKey: this.isKey, ordKey: this.ordKey,
				isPartition: this.isPartition, ordPartition: this.ordPartition, isAutoincrement: this.isAutoincrement,
				isReadOnly: this.isReadOnly, defaultValue: this.defaultValue, compute: this.compute,
				minValue: this.minValue, maxValue: this.maxValue, format: this.format, alias: this.alias,
				trim: this.trim, decimalSeparator: this.decimalSeparator, description: this.description,
				extended: CloneUtils.CloneMap(extended, false)
		)
	}

	/**
	 * Valid convert one type to another
	 */
	static Boolean IsConvertibleType(Field.Type source, Field.Type dest) {
		if (source == dest) return true
		
		Boolean res = false
		switch (dest) {
			case Field.Type.STRING: case Field.Type.TEXT:
				res = true
				break
				
			case Field.Type.BIGINT:
				res = (source in [Field.Type.INTEGER, Field.Type.NUMERIC])
				break
				
			case Field.Type.DATETIME:
				res = (source in [Field.Type.DATE, Field.Type.TIME])
				break
				
			case Field.Type.DOUBLE:
				res = (source in [Field.Type.NUMERIC, Field.Type.INTEGER, Field.Type.BIGINT])
				break
				
			case Field.Type.NUMERIC:
				res = (source in [Field.Type.DOUBLE, Field.Type.INTEGER, Field.Type.BIGINT])
				break
				
			case Field.Type.BLOB:
				res = (source in [Field.Type.STRING, Field.Type.TEXT])
				break
				
		}
		
		return res
	}

    /**
     * Valid equal current object by object
     */
    static Boolean canEqual(java.lang.Object other) {
        return other instanceof Field
    }

	@SuppressWarnings("DuplicatedCode")
	Boolean equalsAll(java.lang.Object other) {
		if (other == null) return false
		if (this.is(other)) return true
		if (!(other instanceof Field)) return false
		if (!other.canEqual(this)) return false

        def o = other as Field

        if (this.name?.toUpperCase() != o.name?.toUpperCase()) return false
        if (this.type != o.type) return false
        if (this.isNull != o.isNull) return false
        if (BoolUtils.IsValue(this.isKey) != BoolUtils.IsValue(o.isKey)) return false
        if (this.ordKey != o.ordKey) return false
		if (BoolUtils.IsValue(this.isPartition) != BoolUtils.IsValue(o.isPartition)) return false
		if (this.ordPartition != o.ordPartition) return false
        if (this.length != o.length) return false
        if (this.precision != o.precision) return false
        if (this.dbType != o.dbType) return false
        if (this.typeName?.toUpperCase() != o.typeName?.toUpperCase()) return false
        if (this.decimalSeparator != o.decimalSeparator) return false
        if (this.alias?.toUpperCase() != o.alias?.toUpperCase()) return false
        if (this.compute != o.compute) return false
        if (this.defaultValue != o.defaultValue) return false
        if (this.minValue != o.minValue) return false
        if (this.maxValue != o.maxValue) return false
        if (this.format != o.format) return false
        if (BoolUtils.IsValue(this.isAutoincrement) != BoolUtils.IsValue(o.isAutoincrement)) return false
        if (BoolUtils.IsValue(this.isReadOnly) != BoolUtils.IsValue(o.isReadOnly)) return false
        if (BoolUtils.IsValue(this.trim) != BoolUtils.IsValue(o.trim)) return false
        if (this.extended != o.extended) return false
        if (this.description != o.description) return false
        if (this.getMethod != o.getMethod) return false

		return true
	}

	@SuppressWarnings("DuplicatedCode")
	@Override
	boolean equals(java.lang.Object other) {
		if (other == null) return false
		if (this.is(other)) return true
		if (!(other instanceof Field)) return false
		if (!other.canEqual(this)) return false

		def o = other as Field

		if (this.name?.toUpperCase() != o.name?.toUpperCase()) return false
		if (this.type != o.type && !(this.type in [Field.Type.STRING, Field.Type.TEXT] && o.type in [Field.Type.STRING, Field.Type.TEXT])) return false
		if (this.isNull != o.isNull) return false
		if (this.isKey != o.isKey) return false
		if (this.isPartition != o.isPartition) return false
		if (AllowLength(this) && this.length != o.length) return false
		if (AllowPrecision(this) && this.precision != o.precision) return false
		if (this.isAutoincrement != o.isAutoincrement) return false

		return true
	}

	/** Convert field type to dsl type name */
	static String TypeToDsl(Type type) {
		return type.toString().toLowerCase() + 'FieldType'
	}

	String generateDsl() {
		def l = [] as List<String>
		l << "type = ${Field.TypeToDsl(type)}".toString()
		if (AllowLength(this) && length != null) l << "length = $length".toString()
		if (AllowPrecision(this) && precision != null) l << "precision = $precision".toString()
		if (!isNull) l << 'isNull = false'
		if (BoolUtils.IsValue(isKey)) {
			l << 'isKey = true'
			if (ordKey > 0) l << "ordKey = $ordKey".toString()
		}
		if (BoolUtils.IsValue(isPartition)) {
			l << 'isPartition = true'
			if (ordPartition != null) l << "ordPartition = $ordPartition".toString()
		}
		if (BoolUtils.IsValue(isAutoincrement)) l << 'isAutoincrement = true'
		if (BoolUtils.IsValue(isReadOnly)) l << 'isReadOnly = true'
		if (defaultValue != null) l << "defaultValue = '$defaultValue'".toString()
		if (compute != null) l << "compute = '$compute'".toString()
		if (minValue != null) l << "minValue = $minValue".toString()
		if (maxValue != null) l << "minValue = $maxValue".toString()
		if (format != null) l << "format = '$format'".toString()
		if (alias != null) l << "alias = '$alias'".toString()
		if (BoolUtils.IsValue(trim)) l << 'trim = true'
		if (decimalSeparator != null) l << "decimalSeparator = '$decimalSeparator'".toString()
		if (description != null) l << "description = '$description'".toString()

		return "field('$name') { ${l.join('; ')} }"
	}

	@Override
	Object clone() {
		return copy()
	}
}