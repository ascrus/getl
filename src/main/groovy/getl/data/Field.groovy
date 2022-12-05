//file:noinspection unused
package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.exception.RequiredParameterError
import getl.utils.*
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Base field class
 * @author Alexsey Konstantinov
 *
 */
class Field implements Serializable, Cloneable {
	/**
	 * Data type
	 */
	static enum Type {
		STRING, INTEGER, BIGINT, NUMERIC, DOUBLE, BOOLEAN, DATE, TIME, DATETIME, TIMESTAMP_WITH_TIMEZONE,
		BLOB, TEXT, OBJECT, ROWID, UUID, ARRAY
	}

	/** Integer field type */
	static public final Type integerFieldType = Type.INTEGER
	/** Bigint field type */
	static public final Type bigintFieldType = Type.BIGINT
	/** Numeric (decimal) field type */
	static public final Type numericFieldType = Type.NUMERIC
	/** Double field type */
	static public final Type doubleFieldType = Type.DOUBLE
	/** String field type */
	static public final Type stringFieldType = Type.STRING
	/** Text (clob) field type */
	static public final Type textFieldType = Type.TEXT
	/** Date field type */
	static public final Type dateFieldType = Type.DATE
	/** Time field type */
	static public final Type timeFieldType = Type.TIME
	/** Timestamp field type */
	static public final Type timestampFieldType = Type.DATETIME
	/** Timestamp field type */
	static public final Type datetimeFieldType = Type.DATETIME
	/** Timestamp with time zone field type */
	static public final Type timestamp_with_timezoneFieldType = Type.TIMESTAMP_WITH_TIMEZONE
	/** Boolean field type */
	static public final Type booleanFieldType = Type.BOOLEAN
	/** Blob field type */
	static public final Type blobFieldType = Type.BLOB
	/** UUID field type */
	static public final Type uuidFieldType = Type.UUID
	/** RowID field type */
	static public final Type rowidFieldType = Type.ROWID
	/** Object field type */
	static public final Type objectFieldType = Type.OBJECT
	/** Array field type */
	static public final Type arrayFieldType = Type.ARRAY

	private String name = null
	/** Field name */
	String getName() { return this.name }
	/** Field name */
	void setName(String value) { this.name = value }
	
	private Type type = Type.STRING
	/** Data type */
	Type getType() { return this.type }
	/** Data type */
	void setType(Type value) {
		this.type = value
		if (!AllowLength(this))
			this.length = null
		if (!AllowPrecision(this))
			this.precision = null
	}
	
	/** Database type number */
	@JsonIgnore
	public Object dbType
	
	/** Database type name */
	@JsonIgnore
	public String typeName

	/** Metadata column class name */
	@JsonIgnore
	public String columnClassName
	
	private Boolean isNull = true
	/** Value can not be null */
	Boolean getIsNull() { return this.isNull }
	/** Value can not be null */
	void setIsNull(Boolean value) { this.isNull = value }
	
	private Integer length
	/** Length of value */
	Integer getLength() { return this.length }
	/** Length of value */
	void setLength(Integer value) { this.length = value }
	
	private Integer precision
	/** Precision number of numeric value */
	Integer getPrecision() { return this.precision }
	/** Precision number of numeric value */
	void setPrecision(Integer value) { this.precision = value }
	
	private Boolean isKey = false
	/** Field is primary key */
	Boolean getIsKey() { return this.isKey }
	/** Field is primary key */
	void setIsKey(Boolean value) {
		this.isKey = value
		if (this.isKey)
			this.isNull = false
		else
			this.ordKey = null
	}
	
	private Integer ordKey
	/** Number order from primary key */
	Integer getOrdKey() { return this.ordKey }
	/** Number order from primary key */
	void setOrdKey(Integer value) { ordKey = value }

	private Boolean isPartition = false
	/** Use field in partition key */
	Boolean getIsPartition() { return this.isPartition}
	/** Use field in partition key */
	void setIsPartition(Boolean value) { this.isPartition = value }

	private Integer ordPartition
	/** Number order if field use in partition key */
	Integer getOrdPartition() { return this.ordPartition }
	/** Number order if field use in partition key */
	void setOrdPartition(Integer value) { this.ordPartition = value }
	
	private Boolean isAutoincrement = false
	/** Field is auto increment */
	Boolean getIsAutoincrement() { return this.isAutoincrement }
	/** Field is auto increment */
	void setIsAutoincrement(Boolean value) { this.isAutoincrement = value }
	
	private Boolean isReadOnly = false
	/** Field can not write */
	Boolean getIsReadOnly() { return this.isReadOnly }
	/** Field can not write */
	void setIsReadOnly(Boolean value) { this.isReadOnly = value }
	
	private String defaultValue = null
	/** Default value from field (used only creating dataset) */
	String getDefaultValue() { return this.defaultValue }
	/** Default value from field (used only creating dataset) */
	void setDefaultValue(String value) { this.defaultValue = value }
	
	private String compute
	/** Compute columns */
	String getCompute() { return this.compute }
	/** Compute columns */
	void setCompute(String value) { this.compute = value }

	private String checkValue
	/** Check value expression */
	String getCheckValue() { checkValue }
	/** Check value expression */
	void setCheckValue(String value) { this.checkValue = value }

	private String arrayType
	/** Primitive type for array */
	String getArrayType() { arrayType }
	/** Primitive type for array */
	void setArrayType(String value) { arrayType = value }
	
	private def minValue = null
	/** Minimum value (for validation and generation) */
	@JsonIgnore
	Object getMinValue() { return this.minValue }
	/** Minimum value (for validation and generation) */
	void setMinValue(Object value) { this.minValue = value }
	
	private def maxValue = null
	/** Maximum value (for validation and generation) */
	@JsonIgnore
	def getMaxValue() { return this.maxValue }
	/** Maximum value (for validation and generation) */
	void setMaxValue(def value) { this.maxValue = value }
	
	private String format
	/** Format pattern on numeric and datetime fields */
	String getFormat () { return this.format }
	/** Format pattern on numeric and datetime fields */
	void setFormat(String value) { this.format = value }
	
	private String alias
	/** Name of the field in the data source (if different from Field name) */
	String getAlias() { return this.alias }
	/** Name of the field in the data source (if different from Field name) */
	void setAlias(String value) { this.alias = value }
	
	private Boolean trim = false
	/** Trim space (used for reading datasource) */
	Boolean getTrim() { return this.trim }
	/** Trim space (used for reading datasource) */
	void setTrim(Boolean value) { this.trim = value }
	
	private String decimalSeparator
	/** Decimal separator */
	String getDecimalSeparator() { return this.decimalSeparator }
	/** Decimal separator */
	void setDecimalSeparator (String value) { this.decimalSeparator = value }
	
	private String description
	/** Field description (comments) */
	String getDescription() { this.description }
	/** Field description (comments) */
	void setDescription(String value) { this.description = value }
	
	private final Map<String, Object> extended = new HashMap<String, Object>()
	/** Extended attributes */
	Map<String, Object> getExtended() { return this.extended }
	/** Extended attributes */
	void setExtended (Map<String, Object> value) {
		this.extended.clear()
		if (value != null) this.extended.putAll(value)
	}
	
	/** Allow length for field */
	static Boolean AllowLength(Field f) {
		return (f.type in [Type.STRING, Type.NUMERIC, Type.BLOB, Type.TEXT, Type.ROWID])
	}
	
	/** Allow precision for field */
	static Boolean AllowPrecision(Field f) {
		return (f.type in [Type.NUMERIC])
	}
	
	/** Allow create field in table */
	static Boolean AllowCreatable(Field f) {
		return !(f.type in [Type.ROWID])
	}
	
	/** Build map from field */
	Map toMap() {
		def n = new LinkedHashMap()
		n.name = name
		n.type = type.toString()
		if (typeName != null) n.typeName = typeName
		if (columnClassName != null) n.columnClassName = columnClassName
		
		if (AllowLength(this) && length != null) n.length = length
		if (AllowPrecision(this) && precision != null) n.precision = precision
		if (!isNull) n.isNull = isNull
		if (isKey) n.isKey = isKey
		if (ordKey != null) n.ordKey = ordKey
		if (isPartition) n.isPartition = isPartition
		if (ordPartition != null) n.ordPartition = ordPartition
		if (isAutoincrement) n.isAutoincrement = isAutoincrement
		if (isReadOnly) n.isReadOnly = isReadOnly
		if (arrayType != null) n.arrayType = arrayType
		if (defaultValue != null) n.defaultValue = defaultValue
		if (compute != null) n.compute = compute
		if (checkValue != null) n.checkValue = checkValue
		if (format != null) n.format = format
		if (alias != null) n.alias = alias
		if (trim) n.trim = trim
		if (decimalSeparator != null) n.decimalSeparator = decimalSeparator
		if (description != null) n.description = description
		if (!extended.isEmpty()) n.extended = MapUtils.Copy(extended)

		return n
	}
	
	/**
	 * Parse map to field
	 * @param map
	 * @return
	 */
	static Field ParseMap(Map map) {
		if (map == null)
			throw new RequiredParameterError('map', 'ParseMap')

		def name = map.name as String
		if (name == null || (name as String).length() == 0)
			throw new RequiredParameterError('map.name', 'ParseMap')

		def typeStr = StringUtils.NullIsEmpty(map.type as String)
		def type = (typeStr != null)? Type.valueOf(typeStr): Type.STRING
		def typeName = StringUtils.NullIsEmpty(map.typeName as String)
		def columnClassName = StringUtils.NullIsEmpty(map.columnClassName as String)
		def isNull = BoolUtils.IsValue(map.isNull,true)
		def length = NumericUtils.Obj2Integer(map.length)
		def precision = NumericUtils.Obj2Integer(map.precision)
		def isKey = BoolUtils.IsValue(map.isKey, false)
		def ordKey = NumericUtils.Obj2Integer(map.ordKey)
		def isPartition = BoolUtils.IsValue(map.isPartition, false)
		def ordPartition = NumericUtils.Obj2Integer(map.ordPartition)
		def isAutoincrement = BoolUtils.IsValue(map.isAutoincrement, false)
		def isReadOnly = BoolUtils.IsValue(map.isReadOnly, false)
		def arrayType = StringUtils.NullIsEmpty(map.arrayType as String)
		def defaultValue = StringUtils.NullIsEmpty(map.defaultValue as String)
		def compute = StringUtils.NullIsEmpty(map.compute as String)
		def checkValue = StringUtils.NullIsEmpty(map.checkValue as String)
		def minValue = (map.minValue instanceof String && (map.minValue as String).length() == 0)?null:map.minValue
		def maxValue = (map.maxValue instanceof String && (map.maxValue as String).length() == 0)?null:map.maxValue
		def format = StringUtils.NullIsEmpty(map.format as String)
		def alias = StringUtils.NullIsEmpty(map.alias as String)
		def trim = BoolUtils.IsValue(map.trim,false)
		def decimalSeparator = StringUtils.NullIsEmpty(map.decimalSeparator as String)
		def description = StringUtils.NullIsEmpty(map.description as String)
		def extended = map.extended as Map<String, Object>

		def res = new Field(
					name: name, type: type, typeName: typeName, columnClassName: columnClassName, isNull: isNull, length: length, precision: precision,
					isKey: isKey, ordKey: ordKey, isPartition: isPartition, ordPartition: ordPartition,
					isAutoincrement: isAutoincrement, isReadOnly: isReadOnly, arrayType: arrayType,
					defaultValue: defaultValue, compute: compute, checkValue: checkValue,
				    minValue: minValue, maxValue: maxValue, format: format, alias: alias, trim: trim,
					decimalSeparator: decimalSeparator, description: description, extended: extended
		)

		if (!res.isKey) {
			if (res.ordKey != null)
				res.ordKey = null
		}
		else if (res.isNull)
			res.isNull = false

		if (!AllowLength(res) && res.length != null)
			res.length = null

		if (!AllowPrecision(res) && res.precision != null)
			res.precision = null

		return res
	}
	
	/**
	 * Attribute to string
	 */
	@Override
	String toString() {
		return MapUtils.RemoveKeys(toMap()) { key, value -> return value == null }
	}
	
	/**
	 * Assign from field
	 */
	void assign(Field f) {
		type = (f.type != Type.OBJECT)?f.type:type
		typeName = (f.typeName != null)?f.typeName:typeName
		columnClassName = (f.columnClassName != null)?f.columnClassName:columnClassName
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
		arrayType = (f.arrayType != null)?f.arrayType:arrayType
		defaultValue = (f.defaultValue != null)?f.defaultValue:defaultValue
		compute = (f.compute != null)?f.compute:compute
		checkValue = (f.checkValue != null)?f.checkValue:checkValue
		minValue = (f.minValue != null)?f.minValue:minValue
		maxValue = (f.maxValue != null)?f.maxValue:maxValue
		format = (f.format != null)?f.format:format
		alias = (f.alias != null)?f.alias:alias
		trim = (f.trim != null)?f.trim:trim
		decimalSeparator = (f.decimalSeparator != null)?f.decimalSeparator:decimalSeparator
		description = (f.description != null)?f.description:description
		if (f.extended != null) MapUtils.MergeMap(extended, f.extended)

		if (!isKey) {
			if (ordKey != null)
				ordKey = null
		}
		else if (isNull)
			isNull = false

		if (!AllowLength(this) && length != null)
			length = null

		if (!AllowPrecision(this) && precision != null)
			precision = null
	}
	
	/**
	 * Clone field
	 */
	@Synchronized
	Field copy() {
		return new Field(
				name: this.name, type: this.type, typeName: this.typeName, columnClassName: this.columnClassName, dbType: this.dbType, isNull: this.isNull,
				length: this.length, precision: this.precision, isKey: this.isKey, ordKey: this.ordKey,
				isPartition: this.isPartition, ordPartition: this.ordPartition, isAutoincrement: this.isAutoincrement,
				isReadOnly: this.isReadOnly, arrayType: this.arrayType, defaultValue: this.defaultValue, compute: this.compute, checkValue: this.checkValue,
				minValue: this.minValue, maxValue: this.maxValue, format: this.format, alias: this.alias,
				trim: this.trim, decimalSeparator: this.decimalSeparator, description: this.description,
				extended: CloneUtils.CloneMap(extended, false)
		)
	}

	/**
	 * Valid convert one type to another
	 */
	@SuppressWarnings('GroovyFallthrough')
	static Boolean IsConvertibleType(Type source, Type dest) {
		if (source == dest) return true
		
		Boolean res = false
		switch (dest) {
			case Type.STRING: case Type.TEXT:
				res = true
				break
				
			case Type.BIGINT:
				res = (source in [Type.INTEGER, Type.NUMERIC])
				break
				
			case Type.DATETIME:
				res = (source in [Type.DATE, Type.TIME])
				break
				
			case Type.DOUBLE:
				res = (source in [Type.NUMERIC, Type.INTEGER, Type.BIGINT])
				break
				
			case Type.NUMERIC:
				res = (source in [Type.DOUBLE, Type.INTEGER, Type.BIGINT])
				break
				
			case Type.BLOB:
				res = (source in [Type.STRING, Type.TEXT])
				break
				
		}
		
		return res
	}

    /**
     * Valid equal current object by object
     */
    static Boolean canEqual(Object other) {
        return other instanceof Field
    }

	Boolean equalsAll(Object other) {
		if (other == null) return false
		if (this.is(other)) return true
		if (!(other instanceof Field)) return false
		//noinspection UnnecessaryQualifiedReference
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
		if (this.columnClassName?.toUpperCase() != o.columnClassName?.toUpperCase()) return false
        if (this.decimalSeparator != o.decimalSeparator) return false
        if (this.alias?.toUpperCase() != o.alias?.toUpperCase()) return false
		if (this.arrayType?.toUpperCase() != o.arrayType?.toUpperCase()) return false
        if (this.compute != o.compute) return false
		if (this.checkValue != o.checkValue) return false
        if (this.defaultValue != o.defaultValue) return false
        if (this.minValue != o.minValue) return false
        if (this.maxValue != o.maxValue) return false
        if (this.format != o.format) return false
        if (BoolUtils.IsValue(this.isAutoincrement) != BoolUtils.IsValue(o.isAutoincrement)) return false
        if (BoolUtils.IsValue(this.isReadOnly) != BoolUtils.IsValue(o.isReadOnly)) return false
        if (BoolUtils.IsValue(this.trim) != BoolUtils.IsValue(o.trim)) return false
        if (this.extended != o.extended) return false
        if (this.description != o.description) return false

		return true
	}

	@SuppressWarnings("DuplicatedCode")
	@Override
	boolean equals(Object other) {
		if (other == null) return false
		if (this.is(other)) return true
		if (!(other instanceof Field)) return false
		//noinspection UnnecessaryQualifiedReference
		if (!other.canEqual(this)) return false

		return compare(other as Field)
	}

	/**
	 * Compare from another field
	 * @param softComparison compare for compatibility of storing values in fields (default false)
	 * @param compareExpressions compare default, check and compute expressions (default true)
	 * @param compareLength compare field length (default true)
	 */
	@SuppressWarnings('GroovyFallthrough')
	boolean compare(Field o, Boolean softComparison = false, Boolean compareExpressions = true, Boolean compareLength = true) {
		if (this.name?.toUpperCase() != o.name?.toUpperCase()) return false

		if (this.type != o.type) {
			if (!softComparison)
				return false

			def ct = false
			switch (this.type) {
				case integerFieldType:
					ct = (o.type == bigintFieldType) || (o.type == numericFieldType && (o.length?:0) >= 9 && (o.precision?:0) == 0)
					break
				case stringFieldType: case textFieldType:
					ct = (o.type in [stringFieldType, textFieldType])
					break
				case dateFieldType:
					ct = (o.type == datetimeFieldType)
					break
				case doubleFieldType: case numericFieldType:
					ct == (o.type in [numericFieldType, doubleFieldType])
					break
			}
			if (!ct)
				return false
		}

		if (this.isAutoincrement != o.isAutoincrement)
			return false

		if (!softComparison) {
			if (this.isNull != o.isNull)
				return false
			if (this.isKey != o.isKey)
				return false
			if (this.isAutoincrement != o.isAutoincrement)
				return false
			if (compareLength && AllowLength(this) && (this.length ?: -1) != (o.length ?: -1))
				return false
			if (compareLength && AllowPrecision(this) && (this.precision ?: -1) != (o.precision ?: -1))
				return false
		}
		else {
			if (BoolUtils.IsValue(this.isNull) && !BoolUtils.IsValue(o.isNull))
				return false
			/*if (!BoolUtils.IsValue(this.isKey) && BoolUtils.IsValue(o.isKey))
				return false*/
			if (compareLength && AllowLength(this) && (this.length ?: -1) > (o.length ?: -1))
				return false
			if (compareLength && AllowPrecision(this) && (this.precision ?: -1) > (o.precision ?: -1))
				return false
		}
		if (this.isPartition != o.isPartition)
			return false
		if (this.type == arrayFieldType && this.arrayType != o.arrayType)
			return false
		if (this.isReadOnly != o.isReadOnly)
			return false

		if (compareExpressions) {
			if (this.defaultValue != o.defaultValue)
				return false
			if (this.checkValue != o.checkValue)
				return false
			if (this.compute != o.compute)
				return false
		}

		return true
	}

	@Override
	Object clone() {
		return copy()
	}

	/**
	 * Create new field
	 * @param name field name
	 * @param cl initialization code
	 * @return created field
	 */
	static Field New(String name,
					 @DelegatesTo(Field) @ClosureParams(value = SimpleType, options = ['getl.data.Field']) Closure cl = null) {
		def parent = new Field(name: name)
		if (cl != null)
			parent.tap(cl)
		return parent
	}
}