package getl.utils

import getl.data.*
import getl.exception.ExceptionGETL
import getl.jdbc.*
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import org.codehaus.groovy.control.CompilerConfiguration

/**
 * Generation code library functions class 
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings("UnnecessaryQualifiedReference")
class GenerationUtils {
	static public final Long EMPTY_BIGINT = null
	static public final def EMPTY_BLOB = null
	static public final def EMPTY_CLOB = null
	static public final Boolean EMPTY_BOOLEAN = null
	static public final Date EMPTY_DATE = null
	static public final java.sql.Timestamp EMPTY_DATETIME = null
	static public final Double EMPTY_DOUBLE = null
	static public final Integer EMPTY_INTEGER = null
	static public final BigDecimal EMPTY_NUMERIC = null
	static public final def EMPTY_OBJECT = null
	static public final String EMPTY_STRING = null
	static public final def EMPTY_TEXT = null
	static public final java.sql.Time EMPTY_TIME = null
	
	/**
	 * Convert string alias as a modifier to access the value of field
	 * @param field source field
	 * @param quote quoted name in field
	 * @param sysChars special characters before the name of the object
	 * @return parsed result
	 */
	static String ProcessAlias(String value, Boolean quote = true, String sysChars = null) {
		return StringUtils.ProcessObjectName(value, quote, true, sysChars)
	}
	
	/** 
	 * Convert alias of field as a modifier to access the value of field
	 * @param field source field
	 * @param quote quoted name in field
	 * @param sysChars special characters before the name of the object
	 * @return parsed result
	 */
	static String Field2Alias(Field field, Boolean quote = true, String sysChars = null) {
		String a = (field.alias != null)?field.alias:field.name
        return ProcessAlias(a, quote, sysChars)
	}
	
	/**
	 * Generation code create empty value as field type into variable
	 * @param t
	 * @param v
	 * @return
	 */
	static String GenerateEmptyValue(getl.data.Field.Type type, String variableName) {
		String r
		switch (type) {
			case Field.Type.STRING: case getl.data.Field.Type.UUID:
				r = "String ${variableName}"
				break
			case Field.Type.BOOLEAN:
				r =  "Boolean ${variableName}"
				break
			case Field.Type.INTEGER:
				r =  "Integer ${variableName}"
				break
			case Field.Type.BIGINT:
				r =  "Long ${variableName}"
				break
			case Field.Type.NUMERIC:
				r =  "BigDecimal ${variableName}"
				break
			case Field.Type.DOUBLE:
				r =  "Double ${variableName}"
				break
			case Field.Type.DATE:
				r =  "java.sql.Date ${variableName}"
				break
			case Field.Type.DATETIME: case Field.Type.TIMESTAMP_WITH_TIMEZONE:
				r =  "java.sql.Timestamp ${variableName}"
				break
			case getl.data.Field.Type.TIME:
				r = "java.sql.Time ${variableName}"
				break
			case getl.data.Field.Type.OBJECT: case getl.data.Field.Type.BLOB: case getl.data.Field.Type.TEXT:
				r =  "def ${variableName}"
				break
			default:
				throw new ExceptionGETL("Type ${type} not supported")
		}
		return r
	}

	static String DateFormat(getl.data.Field.Type type) {
		String df
		
		if (type == Field.Type.DATE)
			df = 'yyyy-MM-dd'
		else if (type == Field.Type.TIME)
			df = 'HH:mm:ss'
		else if (type in [Field.Type.DATETIME, Field.Type.TIMESTAMP_WITH_TIMEZONE])
			df = 'yyyy-MM-dd HH:mm:ss'
		else
			throw new ExceptionGETL("Can not return date format from \"${type}\" type")

		return df
	}
	
	/**
	 * Generate convert code from source field to destination field
	 * @param dest
	 * @param source
	 * @param dataformat
	 * @param sourceValue
	 * @return
	 */
	static String GenerateConvertValue(Field dest, Field source, String dataformat, String sourceValue, Boolean cloneObject = true) {
		String r
		
		switch (dest.type) {
			case Field.Type.STRING: case getl.data.Field.Type.TEXT:
				switch (source.type) {
					case Field.Type.STRING: case Field.Type.INTEGER: case Field.Type.BIGINT: case Field.Type.NUMERIC:
					case Field.Type.DOUBLE: case Field.Type.UUID: case Field.Type.OBJECT: case Field.Type.ROWID:
					case Field.Type.TEXT:
						r = "$sourceValue?.toString()"

						break

					case Field.Type.DATE: case Field.Type.TIME: case Field.Type.DATETIME: case Field.Type.TIMESTAMP_WITH_TIMEZONE:
						dataformat = (dataformat != null)?dataformat:GenerationUtils.DateFormat(source.type)
						r =  "getl.utils.DateUtils.FormatDate('${StringUtils.EscapeJava(dataformat)}', (Date)$sourceValue)"

						break

					case Field.Type.BOOLEAN:
						List<String> values = ['TRUE', 'FALSE']
						if (dest.format != null) {
							values = dest.format.split("[|]")
						}
						r = "($sourceValue != null)?(((Boolean)$sourceValue)?'${values[0]}':'${values[1]}'):null as String"

						break

					case Field.Type.BLOB:
						r = "(((byte[])$sourceValue)?.length > 0)?new String((byte[])$sourceValue):null as String"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.Type.BOOLEAN:
				switch (source.type) {
					case Field.Type.BOOLEAN:
						r = "($sourceValue != null)?new Boolean((Boolean)$sourceValue):null as Boolean"

						break

					case Field.Type.INTEGER:
						r = "($sourceValue != null)?((Integer)$sourceValue)?:0 != 0:null as Boolean"

						break

					case Field.Type.BIGINT:
						r = "($sourceValue != null)?((Long)$sourceValue)?:0 != 0:null as Boolean"

						break

					case Field.Type.NUMERIC:
						r = "($sourceValue != null)?((java.math.BigDecimal)$sourceValue)?.toDouble()?:0 != 0:null as Boolean"

						break

					case Field.Type.STRING:
						def trueValue = 'true'
						if (dataformat != null) {
							def bf = dataformat.toLowerCase().split("[|]")
							trueValue = bf[0]
						}
//						else {
							r = "($sourceValue != null)?((String)$sourceValue).toLowerCase() == '$trueValue'):null as Boolean"
//						}

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.Type.INTEGER:
				switch (source.type) {
					case Field.Type.INTEGER:
						r = "($sourceValue != null)?new Integer((Integer)$sourceValue):null as Integer"

						break

					case Field.Type.STRING:
						r = "($sourceValue != null)?Integer.valueOf((String)$sourceValue):null as Integer"

						break

					case Field.Type.BIGINT:
						r = "((Long)$sourceValue)?.intValue()"

						break

					case Field.Type.DOUBLE:
						r = "((Double)$sourceValue)?.intValue()"

						break

					case Field.Type.NUMERIC:
						r = "((java.math.BigDecimal)$sourceValue)?.intValue()"

						break

					case Field.Type.BOOLEAN:
						r = "($sourceValue != null)?(((Boolean)$sourceValue == true)?1:0):null as Integer"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break
				
			case Field.Type.BIGINT:
				switch (source.type) {
					case Field.Type.BIGINT:
						r = "($sourceValue != null)?new Long((Long)$sourceValue):null as Long"

						break

					case Field.Type.STRING:
						r = "($sourceValue != null)?Long.valueOf((String)$sourceValue):null as Long"

						break

					case Field.Type.INTEGER:
						r = "($sourceValue != null)?Long.valueOf((Integer)$sourceValue):null as Long"

						break

					case Field.Type.DOUBLE:
						r = "((Double)$sourceValue)?.longValue()"

						break

					case Field.Type.NUMERIC:
						r = "((java.math.BigDecimal)$sourceValue)?.longValue()"

						break

					case Field.Type.BOOLEAN:
						r = "($sourceValue != null)?new Long(((Boolean)$sourceValue == true)?1:0):null as Long"

						break

					case Field.Type.DATE: case Field.Type.TIME: case Field.Type.DATETIME: case Field.Type.TIMESTAMP_WITH_TIMEZONE:
						r = "((Date)$sourceValue)?.time"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break
				
			case Field.Type.NUMERIC:
				switch (source.type) {
					case Field.Type.NUMERIC:
						r = "($sourceValue != null)?new java.math.BigDecimal(((BigDecimal)$sourceValue).toString()):null as BigDecimal"

						break

					case Field.Type.STRING:
						r = "($sourceValue != null)?new java.math.BigDecimal((String)$sourceValue):null as BigDecimal"

						break

					case Field.Type.INTEGER:
						r = "($sourceValue != null)?new java.math.BigDecimal((Integer)$sourceValue):null as BigDecimal"

						break

					case Field.Type.BIGINT:
						r = "($sourceValue != null)?new java.math.BigDecimal((Long)$sourceValue):null as BigDecimal"

						break

					case Field.Type.DOUBLE:
						r = "($sourceValue != null)?new java.math.BigDecimal((Double)$sourceValue):null as BigDecimal"

						break

					case Field.Type.BOOLEAN:
						r = "($sourceValue != null)?new BigDecimal(((Boolean)$sourceValue)?1:0):null as BigDecimal"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case getl.data.Field.Type.DOUBLE:
				switch (source.type) {
					case Field.Type.DOUBLE:
						r = "($sourceValue != null)?new Double((Double)$sourceValue):null as Double"

						break

					case Field.Type.STRING:
						r = "($sourceValue != null)?Double.valueOf((String)$sourceValue):null as Double"

						break

					case Field.Type.INTEGER:
						r = "($sourceValue != null)?Double.valueOf((Integer)$sourceValue):null as Double"

						break

					case Field.Type.BIGINT:
						r = "($sourceValue != null)?Double.valueOf((Long)$sourceValue):null as Double"

						break

					case Field.Type.NUMERIC:
						r = "((java.math.BigDecimal)$sourceValue)?.doubleValue()"

						break

					case Field.Type.BOOLEAN:
						r = "($sourceValue != null)?new Double(((Boolean)$sourceValue == true)?1:0):null as Double"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case getl.data.Field.Type.DATE:
				dataformat = dataformat?:GenerationUtils.DateFormat(dest.type)

				switch (source.type) {
					case Field.Type.DATE: case Field.Type.TIME: case Field.Type.DATETIME: case Field.Type.TIMESTAMP_WITH_TIMEZONE:
						r = "($sourceValue != null)?new java.sql.Date(((Date)$sourceValue).time):null as java.sql.Date"

						break

					case Field.Type.STRING:
						r =  "getl.utils.DateUtils.ParseSQLDate('$dataformat', (String)$sourceValue, false)"

						break

					case Field.Type.BIGINT:
						r =  "($sourceValue != null)?new java.sql.Date((Long)$sourceValue):null as java.sql.Date"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break
				
			case Field.Type.DATETIME: case Field.Type.TIMESTAMP_WITH_TIMEZONE:
				dataformat = dataformat?:GenerationUtils.DateFormat(dest.type)

				switch (source.type) {
					case Field.Type.DATETIME: case Field.Type.DATE: case Field.Type.TIME: case Field.Type.TIMESTAMP_WITH_TIMEZONE:
						r = "($sourceValue != null)?new java.sql.Timestamp(((Date)$sourceValue).time):null as java.sql.Timestamp"

						break

					case Field.Type.STRING:
						r =  "getl.utils.DateUtils.ParseSQLTimestamp('$dataformat', (String)$sourceValue, false)"

						break

					case Field.Type.BIGINT:
						r =  "($sourceValue != null)?new java.sql.Timestamp((Long)$sourceValue):null as java.sql.Timestamp"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break
				
			case getl.data.Field.Type.TIME:
				dataformat = dataformat?:GenerationUtils.DateFormat(dest.type)

				switch (source.type) {
					case Field.Type.DATE: case Field.Type.TIME: case Field.Type.DATETIME: case Field.Type.TIMESTAMP_WITH_TIMEZONE:
						r = "($sourceValue != null)?new java.sql.Time(((Date)$sourceValue).time):null as java.sql.Time"

						break

					case Field.Type.STRING:
						r =  "getl.utils.DateUtils.ParseSQLTime('$dataformat', (String)$sourceValue, false)"

						break

					case Field.Type.BIGINT:
						r =  "($sourceValue != null)?new java.sql.Time((Long)$sourceValue):null as java.sql.Time"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case getl.data.Field.Type.BLOB:
				switch (source.type) {
					case Field.Type.BLOB:
						r = "($sourceValue != null)?((byte[])$sourceValue).clone():null as byte[]"

						break

					case Field.Type.STRING:
						r = "($sourceValue != null)?((String)$sourceValue).bytes):null as byte[]"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.Type.UUID:
				switch (source.type) {
					case Field.Type.UUID:
						r = "($sourceValue != null)?java.util.UUID.fromString(((java.util.UUID)$sourceValue).toString()):null as java.util.UUID"

						break

					case Field.Type.STRING:
						r = "($sourceValue != null)?java.util.UUID.fromString((String)$sourceValue):null as java.util.UUID"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.Type.OBJECT:
				if (cloneObject)
					r = "($sourceValue instanceof Cloneable)?${sourceValue}?.clone():$sourceValue"
				else
					r = sourceValue
				
				break

			default:
				throw new ExceptionGETL("Type ${dest.type} not supported (${dest.name})")
		}
		
		return r
	}
	
	static public final Random random = new Random()

	@groovy.transform.CompileStatic
	static Integer GenerateInt () {
        return random.nextInt()
	}
	
	@groovy.transform.CompileStatic
	static Integer GenerateInt (Integer minValue, Integer maxValue) {
		def res = minValue - 1
		while (res < minValue) res = random.nextInt(maxValue + 1)
        return res
	}
	
	@groovy.transform.CompileStatic
	static String GenerateString (Integer length) {
		String result = ""
		while (result.length() < length) result += ((result.length() > 0)?" ":"") + StringUtils.RandomStr().replace('-', ' ')
		
		def l2 = (int)(length / 2)
		def l = GenerateInt(l2, length)

        return StringUtils.LeftStr(result + "a", l)
	}
	
	@groovy.transform.CompileStatic
	static Long GenerateLong () {
        return random.nextLong()
	}
	
	@groovy.transform.CompileStatic
	static BigDecimal GenerateNumeric () {
        return BigDecimal.valueOf(random.nextDouble()) + random.nextInt()
	}
	
	@groovy.transform.CompileStatic
	static BigDecimal GenerateNumeric (Integer precision) {
        return NumericUtils.Round(BigDecimal.valueOf(random.nextDouble()) + random.nextInt(), precision)
	}
	
	@groovy.transform.CompileStatic
	static BigDecimal GenerateNumeric (Integer length, Integer precision) {
		BigDecimal res
		def intSize = length - precision
		if (intSize == 0) {
			res = NumericUtils.Round(BigDecimal.valueOf(random.nextDouble()), precision)
		}
		else { //if (intSize < 15) {
			def lSize = ((Double)Math.pow(10, intSize)).intValue() - 1
			res = NumericUtils.Round(BigDecimal.valueOf(random.nextDouble()) + random.nextInt(lSize), precision)
		}
		/*else {
			res = NumericUtils.Round(BigDecimal.valueOf(random.nextDouble()) + random.nextLong(), precision)
		}*/

        return res
	}
	
	@groovy.transform.CompileStatic
	static double GenerateDouble () {
        return random.nextDouble() + random.nextLong()
	}
	
	@groovy.transform.CompileStatic
	static Boolean GenerateBoolean () {
        return random.nextBoolean()
	}
	
	@groovy.transform.CompileStatic
	static Date GenerateDate() {
        return DateUtils.AddDate("dd", -GenerateInt(0, 365), DateUtils.CurrentDate())
	}
	
	@groovy.transform.CompileStatic
	static Date GenerateDate(Integer days) {
        return DateUtils.AddDate("dd", -GenerateInt(0, days), DateUtils.CurrentDate())
	}

	@groovy.transform.CompileStatic
	static Date GenerateDate(Date date, Integer days) {
		return DateUtils.AddDate("dd", GenerateInt(0, days), date)
	}
	
	@groovy.transform.CompileStatic
	static Date GenerateDateTime() {
        return DateUtils.AddDate("ss", -GenerateInt(0, 300000000), DateUtils.Now())
	}
	
	@groovy.transform.CompileStatic
	static Date GenerateDateTime(Integer seconds) {
        return DateUtils.AddDate("ss", -GenerateInt(0, seconds), DateUtils.Now())
	}

	@groovy.transform.CompileStatic
	static Date GenerateDateTime(Date date, Integer seconds) {
		return DateUtils.AddDate("ss", GenerateInt(0, seconds), date)
	}
	
	@groovy.transform.CompileStatic
	static def GenerateValue (Field f) {
        return GenerateValue(f, null)
	}
	
	/**
	 * Generate random value from fields
	 * @param f
	 * @param rowID
	 * @return
	 */
	@groovy.transform.CompileStatic
	static def GenerateValue (Field f, def rowID) {
		def result
		def l = f.length?:1
		
		if (f.isNull && GenerateBoolean()) return null
		
		switch (f.type) {
			case getl.data.Field.Type.STRING:
				result = GenerateString(l)
				break
			case getl.data.Field.Type.BOOLEAN:
				result = GenerateBoolean()
				break
			case getl.data.Field.Type.INTEGER:
                if (f.isKey && rowID != null) {
                    result = rowID
                }
                else {
                    if (f.minValue == null && f.maxValue == null) result = GenerateInt() else result = GenerateInt((Integer)f.minValue?:0, (Integer)f.maxValue?:1000000)
                }

                break
			case getl.data.Field.Type.BIGINT:
				if (f.isKey && rowID != null) {
					result = rowID
				}
				else {
					if (f.minValue == null && f.maxValue == null) result = GenerateLong() else result = Long.valueOf(GenerateInt((Integer)f.minValue?:0, (Integer)f.maxValue?:1000000))
				}

				break
			case getl.data.Field.Type.NUMERIC:
				if (f.isKey && rowID != null) {
					result = rowID
				}
				else {
					if (f.length != null) {
						if (f.precision != null) {
							result = GenerateNumeric(f.length, f.precision)
						}
						else {
							result = GenerateNumeric(f.length, 0)
						}
					}
					else if (f.precision != null) {
						result = GenerateNumeric(f.precision)
					}
					else {
						result = GenerateNumeric()
					}
				}
				break
			case getl.data.Field.Type.DOUBLE:
				result = GenerateDouble()
				break
			case getl.data.Field.Type.DATE:
				result = new java.sql.Timestamp(GenerateDate().time)
				break
			case getl.data.Field.Type.TIME:
				result = new java.sql.Timestamp(GenerateDate().time)
				break
			case getl.data.Field.Type.DATETIME: case Field.Type.TIMESTAMP_WITH_TIMEZONE:
				result = new java.sql.Timestamp(GenerateDateTime().time)
				break
            case getl.data.Field.Type.TEXT:
				result = GenerateString((l < 65536)?l:65536)
                break
            case getl.data.Field.Type.BLOB:
                result = GenerateString((l < 65536)?l:65536).bytes
                break
			case getl.data.Field.Type.UUID:
				result = UUID.randomUUID().toString()
				break
			default:
				result = GenerateString((l < 65536)?l:65536)
		}

        return result
	}
	
	@groovy.transform.CompileStatic
	static Map GenerateRowValues (List<Field> field) {
        return GenerateRowValues(field, null)
	}
	
	@groovy.transform.CompileStatic
	static Map GenerateRowValues (List<Field> field, def rowID) {
		Map row = [:]
		field.each { Field f ->
			def fieldName = f.name.toLowerCase()
			def value = GenerateValue(f, rowID)
			row.put(fieldName, value)
		}

        return row
	}

	/**
	 * Generate closure code generating random dataset field values
	 * <br><br>The rules syntax is:
	 * <br>_abs_: only generate positive numbers for all number fields (true or false)
	 * <br>_minValue_ and _maxValue_: generate all integer numeric fields within specified boundaries</li>
	 * <br>_divLength_: when generating strings, divide their length by the specified value
	 * <br>_date_: generate fields with date and datetime starting from the specified date
	 * <br>_days_: generate fields with a date within the specified number of days from the current or specified date
	 * <br>_seconds_: generate fields with datetime within the specified number of seconds from the current or specified date
	 * <br><br>Use the rule for a specific field: field_name = [rule: value, rule: value, ...]
	 * <br><br>Field rules (the default value is from the field):
	 * <ul>
	 *     <li>isNull: allow generate null value</li>
	 *     <li>length: generate value of specified length for string and decimal fields</li>
	 *     <li>precision: generate precision decimal fields of specified length</li>
	 *     <li>minValue and maxValue: generate integer numeric fields within specified boundaries</li>
	 *     <li>date: generate fields with date and datetime starting from the specified date</li>
	 *     <li>days: generate fields with a date within the specified number of days from the current or specified date</li>
	 *     <li>seconds: generate fields with datetime within the specified number of seconds from the current or specified date</li>
	 *     <li>list: generate for the text field a value from the specified list</li>
	 *     <li>divLength: when generating strings, divide their length by the specified value
	 * </ul>
	 * Example:
	 * <pre>
	 * def code = GenerationUtils.GenerateRandomRow(dataset, ['field1'],
	 *     ['_abs_': true, field2: [minValue:1,maxValue:100]])
	 * def row = [:]
	 * code(row)
	 * </pre>
	 * @param dataset required for filling the dataset
	 * @param excludeFields list of field names excluded from generation
	 * @param rules additional rules for generating values
	 * @return code for generating a record with field values
	 */
	@CompileStatic
	static Closure GenerateRandomRow(Dataset dataset, List excludeFields = [], Map rules = [:]) {
		if (excludeFields == null) excludeFields = [] as List<String>
		excludeFields = (excludeFields as List<String>)*.toLowerCase()
		if (rules == null) rules = [:]
		def absAll = BoolUtils.IsValue(rules.get('_abs_'))
		def minValueAll = rules.get('_minValue_') as Integer
		def maxValueAll = rules.get('_maxValue_') as Integer
		def divLengthAll = rules.get('_divLength_') as Integer
		def dateAll =  rules.get('_date_') as Date
		def daysAll =  rules.get('_days_') as Integer
		def secondsAll = rules.get('_seconds_') as Integer

		def sb = new StringBuilder()
		sb << '{ Map row ->\n'
		sb << '  generateRow(row)\n'
		sb << '}\n'
		sb << '@groovy.transform.CompileStatic\n'
		sb << 'static void generateRow(final Map row) {\n'
		def count = 0

		dataset.field.each { Field f ->
			def fieldName = f.name.toLowerCase()
			if (fieldName in excludeFields) return
			count++

			def isNull = f.isNull
			def length = f.length
			def precision = f.precision
			def minValue = ListUtils.NotNullValue(f.minValue, minValueAll) as Integer
			def maxValue = ListUtils.NotNullValue(f.maxValue, maxValueAll) as Integer

			def rule = (rules.get(fieldName)?:[:]) as Map<String, Object>
			if (rule.isNull != null) isNull = BoolUtils.IsValue(rule.isNull)
			if (rule.containsKey('length')) length = rule.length as Integer
			if (rule.containsKey('precision')) precision = rule.precision as Integer
			if (rule.containsKey('minValue')) minValue = rule.minValue as Integer
			if (rule.containsKey('maxValue')) maxValue = rule.maxValue as Integer
			def date = (rule.date as Date)?:dateAll
			def days = (rule.days as Integer)?:daysAll
			def seconds = (rule.seconds as Integer)?:secondsAll
			def list = rule.list as List<String>
			def divLength = ListUtils.NotNullValue(rule.divLength, divLengthAll) as Integer
			def abs = BoolUtils.IsValue(rule.abs, absAll)

			String func
			switch (f.type) {
				case Field.integerFieldType: case Field.bigintFieldType:
					def absFunc = (abs)?'.abs()':''
					def generate = (minValue != null && maxValue != null)?
							"getl.utils.GenerationUtils.GenerateInt($minValue, $maxValue)":
							"getl.utils.GenerationUtils.GenerateInt()$absFunc"
					if (isNull)
						func = "(getl.utils.GenerationUtils.GenerateBoolean())?$generate:(null as Integer)"
					else
						func = generate

					break

				case Field.booleanFieldType:
					def generate = "getl.utils.GenerationUtils.GenerateBoolean()"
					if (isNull)
						func = "(getl.utils.GenerationUtils.GenerateBoolean())?$generate:(null as Boolean)"
					else
						func = generate

					break

				case Field.dateFieldType:
					String generate
					if (date != null && days != null) {
						generate = "getl.utils.GenerationUtils.GenerateDate(new Date(${date.time}), $days)"
					}
					else {
					 	if (days != null)
							generate = "getl.utils.GenerationUtils.GenerateDate($days)"
						else
							generate = "getl.utils.GenerationUtils.GenerateDate()"
					}

					if (isNull)
						func = "(getl.utils.GenerationUtils.GenerateBoolean())?$generate:(null as Date)"
					else
						func = generate

					break

				case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
					String generate
					if (date != null && seconds != null) {
						generate = "getl.utils.GenerationUtils.GenerateDateTime(new Date(${date.time}), $seconds)"
					}
					else {
						if (seconds != null)
							generate = "getl.utils.GenerationUtils.GenerateDateTime($seconds)"
						else
							generate = "getl.utils.GenerationUtils.GenerateDateTime()"
					}

					if (isNull)
						func = "(getl.utils.GenerationUtils.GenerateBoolean())?$generate:(null as Date)"
					else
						func = generate

					break

				case Field.doubleFieldType:
					def absFunc = (abs)?'.abs()':''
					def generate = "getl.utils.GenerationUtils.GenerateDouble()$absFunc"
					if (isNull)
						func = "(getl.utils.GenerationUtils.GenerateBoolean())?$generate:(null as Double)"
					else
						func = generate

					break

				case Field.numericFieldType:
					def absFunc = (abs)?'.abs()':''
					String generate
					if (length != null) {
						if (precision != null)
							generate = "getl.utils.GenerationUtils.GenerateNumeric($length, $precision)$absFunc"
						else
							generate = "getl.utils.GenerationUtils.GenerateNumeric($length)$absFunc"
					}
					else {
						generate = "getl.utils.GenerationUtils.GenerateNumeric()$absFunc"
					}

					if (isNull)
						func = "(getl.utils.GenerationUtils.GenerateBoolean())?$generate:(null as BigDecimal)"
					else
						func = generate

					break

				case Field.stringFieldType:
					if (divLength != null)
						length = (length / divLength).intValue().toInteger()

					String generate
					if (list != null) {
						sb << "  final def ${fieldName}_list = [${ListUtils.QuoteList(list, '\'').join(',')}]\n"
						generate = "${fieldName}_list[getl.utils.GenerationUtils.GenerateInt(0, ${list.size() - 1})]"
					}
					else
						generate = "getl.utils.GenerationUtils.GenerateString(${length?:255})"

					if (isNull)
						func = "(getl.utils.GenerationUtils.GenerateBoolean())?$generate:(null as String)"
					else
						func = generate

					break

				default:
					throw new ExceptionGETL("Generation of type \"${f.type}\" for field \"${f.name}\" is not supported!")
			}
			sb << "  row.put('$fieldName', $func)\n"
		}
		if (count == 0)
			throw new ExceptionGETL('No fields were found for generation!')
		sb << '}'

//		println sb.toString()

		return EvalGroovyClosure(sb.toString())
	}
	
	/**
	 * Return string value for generators 
	 * @param value
	 * @return
	 */
	@groovy.transform.CompileStatic
	static String GenerateStringValue (String value) {
		if (value == null) return "null"
		return '"' + value.replace('"', '\\"') + '"'
	}
	
	@groovy.transform.CompileStatic
	static String GenerateCommand(String command, Integer numTab, Boolean condition) {
		if (!condition) return ""
        return StringUtils.Replicate("\t", numTab) + command
	}
	
	/**
	 * Generation groovy closure create fields from dataset fields
	 * @param fields
	 * @return
	 */
	static String GenerateScriptAddFields (List<Field> fields) {
		StringBuilder sb = new StringBuilder()
		sb << "{\nList<Field> res = []\n\n"
		fields.each { Field f ->
			sb << """// ${f.name}
res << new Field(
		name: ${GenerateStringValue(f.name)}, 
		type: ${GenerateStringValue(f.type.toString())},
"""
		def cmd = []
		def c

		c = GenerateCommand("length: ${f.length}", 2, Field.AllowLength(f) && f.length != null)
		if (c != "") cmd << c
		  
		c = GenerateCommand("precision: ${f.precision}", 2, Field.AllowPrecision(f) && f.precision != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("isNull: ${f.isNull}", 2, !f.isNull)
		if (c != "") cmd << c
		
		c = GenerateCommand("isKey: ${f.isKey}", 2, f.isKey)
		if (c != "") cmd << c
		
		c = GenerateCommand("isAutoincrement: ${f.isAutoincrement}", 2, f.isAutoincrement)
		if (c != "") cmd << c
		
		c = GenerateCommand("isReadOnly: ${f.isReadOnly}", 2, f.isReadOnly)
		if (c != "") cmd << c
		
		c = GenerateCommand("defaultValue: ${GenerateStringValue(f.defaultValue)}", 2, f.defaultValue != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("compute: ${GenerateStringValue(f.compute)}", 2, f.compute != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("minValue: ${f.minValue}", 2, f.minValue != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("maxValue: ${f.maxValue}", 2, f.maxValue != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("format: ${GenerateStringValue(f.format)}", 2, f.format != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("alias: ${GenerateStringValue(f.alias)}", 2, f.alias != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("trim: ${f.trim}", 2, f.trim)
		if (c != "") cmd << c
		
		c = GenerateCommand("decimalSeparator: ${GenerateStringValue(f.decimalSeparator)}", 2, f.decimalSeparator != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("description: ${GenerateStringValue(f.description)}", 2, f.description != null)
		if (c != "") cmd << c
		
		sb << cmd.join(",\n")
		
sb << """
	)

"""
		}
		sb << "res\n\n}"
		
		return sb.toString()
	}
	
	/**
	 * Convert list of field to Map structure
	 * @param fields
	 * @return
	 */
	static Map Fields2Map (List<Field> fields) {
		if (fields == null) return null
		
		def res = [:]
		res.fields = []
		def l = res.fields
		
		fields.each { Field f ->
			l << f.toMap()
		}

        return res
	}
	
	/**
	 * Convert list of field to name of list structure
	 * @param fields
	 * @return
	 */
	static List<String> Fields2List (JDBCDataset dataset, List<String> excludeFields = null) {
		if (dataset == null) return null
		
		def res = []
		
		dataset.field.each { Field f ->
			if (excludeFields != null && excludeFields.find { it.toLowerCase() == f.name.toLowerCase() } != null) return
			res << dataset.sqlObjectName(f.name)
		}

        return res
	}
	
	/**
	 * Convert list of field to JSON string
	 * @param fields
	 * @return
	 */
	static String GenerateJsonFields (List<Field> fields) {
        return MapUtils.ToJson(Fields2Map(fields))
	}
	
	/**
	 * Convert map to list of field
	 * @param value
	 * @return
	 */
	static List<Field> Map2Fields (Map value) {
		List<Field> res = []
		
		value.fields?.each { Map f ->
			res << Field.ParseMap(f)
		}

        return res
	}
	
	/**
	 * Parse JSON to list of field
	 * @param value
	 * @return
	 */
	static List<Field> ParseJsonFields (String value) {
		if (value == null) return null
		
		def b = new JsonSlurper()
		Map l = b.parseText(value) as Map

        return Map2Fields(l)
	}
	
	/**
	 * Remove fields in list of field by field name and return removed fields
	 * @param fields
	 * @param names
	 */
	static List<Field> RemoveFields (List<Field> fields, List<String> names) {
		List<Field> res = []
		names.each { name ->
			name = name.toLowerCase()
			def o = fields.find { Field f -> f.name.toLowerCase() == name }
			if (o != null) {
				res << o
				fields.remove(o)
			}
		}
        return res
	}

	/**
	 * Disable field attribute and return new list field	
	 * @param fields
	 * @param disableNotNull
	 * @param disableKey
	 * @param disableAutoincrement
	 * @param disableExtended
	 * @return
	 */
	static List<Field> DisableAttributeField (List<Field> fields, Boolean disableNotNull, Boolean disableKey, Boolean disableAutoincrement,
											  Boolean disableExtended, Boolean excludeReadOnly) {
		List<Field> res = []
		fields.each { Field f -> 
			def nf = f.copy()
			
			if (!excludeReadOnly || !nf.isReadOnly ) {
				if (disableNotNull && !nf.isNull) nf.isNull = true
				if (disableKey && nf.isKey) nf.isKey = false
				if (disableAutoincrement && nf.isAutoincrement) nf.isAutoincrement = false
				if (disableExtended && nf.extended != null) nf.extended = null
			
				res << nf
			} 
		}

        return res
	}

	/**
	 * Compile groovy script to closure
	 */
	static Closure EvalGroovyClosure(String value, Map<String, Object> vars = null, Boolean convertReturn = false, ClassLoader classLoader = null) {
        return EvalGroovyScript(value, vars, convertReturn, classLoader) as Closure
	}

	/**
	 * Run groovy script
	 * @param value
	 * @param vars
	 * @return
	 */
	@groovy.transform.CompileStatic
	static def EvalGroovyScript(String value, Map<String, Object> vars = null, Boolean convertReturn = false, ClassLoader classLoader = null) {
		if (value == null) return null
		if (convertReturn) value = value.replace('\r', '\u0001')
		
		Binding bind = new Binding()
		vars?.each { String key, Object val ->
			bind.setVariable(key, val)
		}

		def sh = (classLoader == null)?new GroovyShell(bind):new GroovyShell(classLoader, bind, CompilerConfiguration.DEFAULT)
		
		def res
		try {
			res = sh.evaluate(value)
			if (convertReturn && res != null) res = (res as String).replace('\u0001', '\r')
		}
		catch (Exception e) {
			Logs.Severe("Error parse [${StringUtils.CutStr(value, 1000)}]")
			StringBuilder sb = new StringBuilder("script:\n$value\nvars:")
			vars?.each { varName, varValue -> sb.append("\n	$varName: ${StringUtils.LeftStr(varValue.toString(), 256)}") }
			Logs.Dump(e, 'GenerationUtils', 'EvalGroovyScript', sb.toString())
			throw e
		}

        return res
	}
	
	/**
	 * Evaluate ${variable} in text
	 * @param value
	 * @param vars
	 * @return
	 */
	@groovy.transform.CompileStatic
	static String EvalText(String value, Map<String, Object> vars) {
		if (value == null) return null
		vars.each { String key, Object val ->
			key = '${' + key + '}'
			value = value.replace(key, val.toString())
		}

        return value
	}
	
	/**
	 * Convert field type to string type
	 * @param field
	 * @return
	 */
	static void FieldConvertToString (getl.data.Field field) {
		def len
		def type = getl.data.Field.Type.STRING
		switch (field.type) {
			case getl.data.Field.Type.STRING:
				break
			case getl.data.Field.Type.ROWID:
				field.length = 50
				break
			case getl.data.Field.Type.TEXT: case getl.data.Field.Type.BLOB:
				type = getl.data.Field.Type.STRING
				if (field.length == null) len = 65535
				break
			case getl.data.Field.Type.BIGINT:
				len = 38
				break
			case getl.data.Field.Type.INTEGER:
				len = 13
				break
			case getl.data.Field.Type.DATE: case getl.data.Field.Type.DATETIME: case getl.data.Field.Type.TIME:
			case Field.Type.TIMESTAMP_WITH_TIMEZONE:
				len = 30
				break
			case getl.data.Field.Type.BOOLEAN:
				len = 5
				break
			case getl.data.Field.Type.DOUBLE: 
				len = 50
				break
			case getl.data.Field.Type.NUMERIC:
				len = (field.length?:50) + 1
				break
			case getl.data.Field.Type.UUID:
				type = getl.data.Field.Type.STRING
				len = 36
				break
			default:
				throw new ExceptionGETL("Not support convert field type \"${field.type}\" to \"STRING\" from field \"${field.name}\"")
		}
		field.type = type
		if (len != null) field.length = len
		field.precision = null
		field.typeName = null
	}
	
	/**
	 * Convert all dataset fields to string
	 * @param dataset source dataset
	 */
	@groovy.transform.CompileStatic
	static void ConvertToStringFields (Dataset dataset) {
		dataset.field.each { FieldConvertToString(it) }
	}
	
	/**
	 * Return field name with SQL syntax
	 * @param dataset source dataset
	 * @param name field name
	 * @return processed field name
	 */
	static String SqlObjectName(JDBCDataset dataset, String name) {
		JDBCDriver drv = dataset.connection.driver as JDBCDriver

        return drv.prepareObjectNameForSQL(name, dataset)
	}

	/**
	 * Return object name with SQL syntax
	 * @param connection source connection
	 * @param name field name
	 * @return processed field name
	 */
	static String SqlObjectName(JDBCConnection connection, String name) {
		JDBCDriver drv = connection.driver as JDBCDriver

		return drv.prepareObjectNameForSQL(name)
	}
	
	/**
	 * Return list object name with SQL syntax
	 * @param dataset source dataaset
	 * @param listNames list of field name
	 * @return list of processed field name
	 */
	static List<String> SqlListObjectName (JDBCDataset dataset, List<String> listNames) {
		List<String> res = []
		JDBCDriver drv = dataset.connection.driver as JDBCDriver

		listNames.each { name ->
			res << drv.prepareObjectNameForSQL(name, dataset)
		}

        return res
	}

	/**
	 * Return list object name with SQL syntax
	 * @param connection source connection
	 * @param listNames list of field name
	 * @return list of processed field name
	 */
	static List<String> SqlListObjectName (JDBCConnection connection, List<String> listNames) {
		List<String> res = []
		JDBCDriver drv = connection.driver as JDBCDriver

		listNames.each { name ->
			res << drv.prepareObjectNameForSQL(name)
		}

		return res
	}

    /**
     * Remove all pseudo character in field name
     * @param fieldName field name
     * @return processed field name
     */
	static String Field2ParamName(String fieldName) {
		if (fieldName == null) return null

        return fieldName.replaceAll("(?i)[^a-z0-9_]", "_").toLowerCase()
	}
	
	/**
	 * Return key fields name by sql syntax with expression and exclude fields list
	 * @param expr - string expression with {field} and {orig} macros
	 * @return
	 */
	static List<String> SqlKeyFields (JDBCDataset dataset, List<Field> fields, String expr, List<String> excludeFields) {
		excludeFields = (excludeFields != null)?excludeFields*.toLowerCase():[]
		List<Field> kf = []
		fields.each { Field f ->
			if ((!(f.name.toLowerCase() in excludeFields)) && f.isKey) kf << f
		}
		kf.sort(true) { Field a, Field b -> (a.ordKey?:999999999) <=> (b.ordKey?:999999999) }
		
		List<String> res = []
		kf.each { Field f ->
			if (expr == null) {
				res << SqlObjectName(dataset, f.name)
			} 
			else {
				res << expr.replace("{orig}", f.name.toLowerCase()).replace("{field}", SqlObjectName(dataset, f.name)).replace("{param}", "${Field2ParamName(f.name)}")
			}
		}

        return res
	}
	
	/**
	 * Return fields name by sql syntax with expression and exclude fields list
	 * @param expr - string expression with {field} macros
	 * @return
	 */
	static List<String> SqlFields (JDBCDataset dataset, List<Field> fields, String expr, List<String> excludeFields) {
		excludeFields = (excludeFields != null)?excludeFields*.toLowerCase():[]
		
		List<String> res = []
		fields.each { Field f ->
			if (!(f.name.toLowerCase() in excludeFields)) {
				if (expr == null) {
					res << SqlObjectName(dataset, f.name)
				} 
				else {
					res << expr.replace("{orig}", f.name.toLowerCase()).replace("{field}", SqlObjectName(dataset, f.name)).replace("{param}", "${Field2ParamName(f.name)}")
				}
			}
		}

        return res
	}
	
	/**
	 * Return values only key fields from row
	 * @param fields
	 * @param row
	 * @return
	 */
	@groovy.transform.CompileStatic
	static Map RowKeyMapValues(List<Field> fields, Map row, List<String> excludeFields) {
		Map res = [:]
		if (excludeFields != null) excludeFields = excludeFields*.toLowerCase() else excludeFields = []
		fields.each { Field f ->
			if (f.isKey) {
				if (!(f.name.toLowerCase() in excludeFields)) res.put(f.name.toLowerCase(), row.get(f.name.toLowerCase()))
			}
		}

        return res
	}

	/**
	 * Return list of fields row values	
	 * @param fields
	 * @param row
	 * @return
	 */
	@groovy.transform.CompileStatic
	static List RowListValues (List<String> fields, Map row) {
		def res = new ArrayList()
		fields.each { String n ->
			res << row.get(n.toLowerCase())
		}

        return res
	}
	
	/**
	 * Return map of fields row values
	 * @param fields
	 * @param row
	 * @param toLower
	 * @return
	 */
	@groovy.transform.CompileStatic
	static Map RowMapValues (List<String> fields, Map row, Boolean toLower) {
		Map res = [:]
		if (toLower) {
			fields.each { String n ->
				n = n.toLowerCase()
				res.put(n, row.get(n))
			}
		}
		else {
			fields.each { String n ->
				n = n.toLowerCase()
				res.put(n.toUpperCase(), row.get(n))
			}
		}

        return res
	}

	/**
	 * Return map of fields row values	
	 * @param fields
	 * @param row
	 * @return
	 */
	@groovy.transform.CompileStatic
	static Map RowMapValues (List<String> fields, Map row) {
		RowMapValues(fields, row, true)
	}
	
	/**
	 * Generation row copy closure
	 * @param fields
	 * @return
	 */
	static Map GenerateRowCopy(JDBCDriver driver, List<Field> fields, Boolean sourceIsMap = false) {
		if (!driver.isConnected()) driver.connect()

		StringBuilder sb = new StringBuilder()
		sb << "{ java.sql.Connection connection, ${(sourceIsMap)?'Map<String, Object>':'groovy.sql.GroovyResultSet'} inRow, Map<String, Object> outRow -> methodRowCopy(connection, inRow, outRow) }\n"
		sb << '\n@groovy.transform.CompileStatic\n'
		sb << "void methodRowCopy(java.sql.Connection connection, ${(sourceIsMap)?'Map<String, Object>':'groovy.sql.GroovyResultSet'} inRow, Map<String, Object> outRow) {\n"
		def i = 0
		fields.each { Field f ->
			i++

			def fName = f.name.toLowerCase().replace("'", "\\'")

			sb << "	def _getl_temp_var_$i = inRow.getAt('$fName')\n"
			sb << "	if (_getl_temp_var_$i == null) outRow.put('$fName', null) else {\n"
			if (f.getMethod != null) sb << "		_getl_temp_var_$i = ${f.getMethod.replace("{field}", "_getl_temp_var_$i")}\n"

			switch (f.type) {
				case getl.data.Field.Type.BLOB:
					if (driver.blobReadAsObject()) {
						sb << "	outRow.put('$fName', (_getl_temp_var_${i} as java.sql.Blob).getBytes((long)1, (int)((_getl_temp_var_${i} as java.sql.Blob).length())))"
					}
					else {
						sb << "	outRow.put('$fName', _getl_temp_var_${i})"
					}
					break
				case getl.data.Field.Type.TEXT:
					if (driver.textReadAsObject()) {
						sb << "		String clob_value = (_getl_temp_var_${i} as java.sql.Clob).getSubString((long)1, ((int)(_getl_temp_var_${i} as java.sql.Clob).length()))\n"
						sb << "		outRow.put('$fName', clob_value)"
					}
					else {
						sb << "		outRow.put('$fName', _getl_temp_var_${i})"
					}
					break
				case getl.data.Field.Type.UUID:
					sb << "		outRow.put('$fName', _getl_temp_var_${i}.toString())"
					break
				default:
					sb << "		outRow.put('$fName', _getl_temp_var_${i})"
			}

			sb << '\n	}\n'

		}
		sb << "}"
		def statement = sb.toString()

//		println statement

		Closure code = EvalGroovyClosure(statement, null, false, (driver.useLoadedDriver)?driver.jdbcClass?.classLoader:null)

        return [statement: statement, code: code]
	}

	/**
	 * Generation field copy by fields
	 * @param fields
	 * @return
	 */
	static Closure GenerateFieldCopy(List<Field> fields) {
		StringBuilder sb = new StringBuilder()
		sb << "{ Map<String, Object> inRow, Map<String, Object> outRow -> methodCopy(inRow, outRow) }"
		sb << '\n@groovy.transform.CompileStatic\n'
		sb << 'void methodCopy(Map<String, Object> inRow, Map<String, Object> outRow) {\n'
		fields.each { Field f ->
			def fName = f.name.toLowerCase().replace("'", "\\'")
			sb << "outRow.put('$fName', inRow.get('$fName'))\n"
		}
		sb << "}"
		Closure result = GenerationUtils.EvalGroovyClosure(sb.toString())
        return result
	}

	static String GenerateSetParam(JDBCDriver driver, Integer paramNum, Field field, Integer fieldType, String value) {
		String res
		Map types = driver.javaTypes()
		switch (fieldType) {
			case types.BIGINT:
				res = "if ($value != null) _getl_stat.setLong($paramNum, ($value) as Long) else _getl_stat.setNull($paramNum, java.sql.Types.BIGINT)"
				break
				 
			case types.INTEGER:
				res = "if ($value != null) _getl_stat.setInt($paramNum, ($value) as Integer) else _getl_stat.setNull($paramNum, java.sql.Types.INTEGER)"
				break
			
			case types.STRING:
				res = "if ($value != null) _getl_stat.setString($paramNum, ($value) as String) else _getl_stat.setNull($paramNum, java.sql.Types.VARCHAR)"
				break
			
			case types.BOOLEAN: case types.BIT:
				res = "if ($value != null) _getl_stat.setBoolean($paramNum, ($value) as Boolean) else _getl_stat.setNull($paramNum, java.sql.Types.BOOLEAN)"
				break
				
			case types.DOUBLE:
				res = "if ($value != null) _getl_stat.setDouble($paramNum, ($value) as Double) else _getl_stat.setNull($paramNum, java.sql.Types.DOUBLE)"
				break
				
			case types.NUMERIC:
				res = "if ($value != null) _getl_stat.setBigDecimal($paramNum, ($value) as BigDecimal) else _getl_stat.setNull($paramNum, java.sql.Types.DECIMAL)"
				break
				
			case types.BLOB:
				res = "blobWrite(_getl_con, _getl_stat, $paramNum, ($value) as byte[])"
				break
				
			case types.TEXT:
				if (driver.textReadAsObject()) {
					res = "clobWrite(_getl_con, _getl_stat, $paramNum, ($value) as String)"
				}
				else {
					res = "if ($value != null) _getl_stat.setString($paramNum, ($value) as String) else _getl_stat.setNull($paramNum, java.sql.Types.VARCHAR)"
				}
				break
				
			case types.DATE:
				res = "if ($value != null) _getl_stat.setDate($paramNum, new java.sql.Date(((${value}) as Date).getTime())) else _getl_stat.setNull($paramNum, java.sql.Types.DATE)"
				break
				
			case types.TIME:
				res = "if ($value != null) _getl_stat.setTime($paramNum, new java.sql.Time(((${value}) as Date).getTime())) else _getl_stat.setNull($paramNum, java.sql.Types.TIME)"
				break
				
			case types.TIMESTAMP:
				res = "if ($value != null) _getl_stat.setTimestamp($paramNum, new java.sql.Timestamp(((${value}) as Date).getTime())) else _getl_stat.setNull($paramNum, java.sql.Types.TIMESTAMP)"
				break

			case types.TIMESTAMP_WITH_TIMEZONE:
				if (!driver.timestampWithTimezoneConvertOnWrite())
					res = "if ($value != null) _getl_stat.setTimestamp($paramNum, new java.sql.Timestamp(((${value}) as Date).getTime())) else _getl_stat.setNull($paramNum, java.sql.Types.TIMESTAMP_WITH_TIMEZONE)"
				else
					res = "if ($value != null) _getl_stat.setObject($paramNum, ((${value}) as Date).toInstant().atZone(java.time.ZoneId.of('UTC')).toLocalDateTime()) else _getl_stat.setNull($paramNum, java.sql.Types.TIMESTAMP_WITH_TIMEZONE)"
				break

			default:
				if (field.type == Field.Type.UUID) {
					if (driver.uuidReadAsObject()) {
						res = "if ($value != null) _getl_stat.setObject($paramNum, UUID.fromString(($value) as String), java.sql.Types.OTHER) else _getl_stat.setNull($paramNum, java.sql.Types.OTHER)"
					} else {
						res = "if ($value != null) _getl_stat.setString($paramNum, ($value) as String) else _getl_stat.setNull($paramNum, java.sql.Types.VARCHAR)"
					}
				}
				else {
					res = "if ($value != null) _getl_stat.setObject($paramNum, $value) else _getl_stat.setNull($paramNum, java.sql.Types.JAVA_OBJECT)"
				}
		}

        return res
	}
}