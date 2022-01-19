//file:noinspection UnnecessaryQualifiedReference
package getl.utils

import getl.data.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.lang.Getl
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.transform.NamedVariant
import org.codehaus.groovy.control.CompilerConfiguration

/**
 * Generation code library functions class 
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings('unused')
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
	 * Generate declaration of map sections
	 * @param rootPath Data source
	 * @param rootNode dot-separated section path
	 * @return list of section declarations
	 */
	static List<String> GenerateRootSections(String rootPath, String rootNode, String className) {
		if (rootNode == null || rootNode.length() == 0)
			throw new ExceptionGETL("Invalid section path!")

		def subRootNodes = rootNode.split('[|]')
		if (subRootNodes.length > 2)
			throw new ExceptionGETL("Not a correct root node \"$rootNode\", no more than one level attachment is supported!")

		def res = [] as List<String>

		def secPath = subRootNodes[0].split('[.]')
		def len = secPath.length

		if (len == 1) {
			res.add("	def _getl_root_0 = ${rootPath}.get(${StringUtils.ProcessObjectName(secPath[0], true, false)}) as List<$className>")
		}
		else {
			res.add("	def _getl_root_0 = ${rootPath}.get(${StringUtils.ProcessObjectName(secPath[0], true, false)}) as $className")

			for (int i = 1; i < len - 1; i++) {
				def sectionName = StringUtils.ProcessObjectName(secPath[i], true, false)
				res.add("	def _getl_root_${i} = _getl_root_${i - 1}?.get(${sectionName}) as $className")
			}

			res.add("	def _getl_root_${len - 1} = _getl_root_${len - 2}?.get(${StringUtils.ProcessObjectName(secPath[len - 1], true, false)}) as List<$className>")
		}

		return res
	}

	/**
	 * Generate each row code on structure object
	 */
	static void GenerateEachRow(StructureFileDataset dataset, String methodParam, String rootElement, String rowMap,
								String body, StringBuilder sb) {
		def rootNode = dataset.rootNode
		if (rootNode == null) {
			sb << """def $rootElement = $methodParam as Map
	Map<String, Object> $rowMap = new HashMap<String, Object>()
$body
	code.call($rowMap)
"""
			return
		}

		def subRootNodes = dataset.rootNodePath()
		if (subRootNodes.size() > 2)
			throw new ExceptionGETL("Not a correct root node \"$rootNode\", no more than one level attachment is supported!")
		def isDetails = (subRootNodes.size() != 1)

		sb << "	def cur = 0L\n"
		if (rootNode != '.') {
			def sect = GenerateRootSections('(data as Map)', rootNode, 'Map')
			sb << sect.join('\n')
			sb << '\n'
			sb << "	def rootList = _getl_root_${sect.size() - 1}"
		}
		else {
			sb << '	def rootList = data as List<Map>'
		}
		sb << '\n'

		if (!isDetails) {
			sb << """	rootList?.each { Map struct ->
		if (limit > 0) {
			cur++
			if (cur > limit) {
				directive = Closure.DONE
				return
			}
		}
		Map<String, Object> row = new HashMap<String, Object>()
$body
		code.call(row)
		if (code.directive == Closure.DONE)
			directive = Closure.DONE
	}"""
		}
		else {
			sb << """	rootList?.each { Map parent ->
		(parent.get('${subRootNodes[1]}') as List<Map>)?.each { Map struct ->
			if (limit > 0) {
				cur++
				if (cur > limit) {
					directive = Closure.DONE
					return
				}
			}
			Map<String, Object> row = new HashMap<String, Object>()
$body
			code.call(row)
		}
		if (limit > 0 && cur > limit) {
			directive = Closure.DONE
			return
		}
	}"""
		}
	}

	/**
	 * Formatters for generation DateFormatter
	 */
	@InheritConstructors
	static class FormatElement {
		public Field.Type type
		public String format
	}

	/**
	 * Formatter name
	 * @param type
	 * @param format
	 * @return
	 */
	static String FormatterName(Field.Type type, String format) {
		return "_getl_df_${type.toString().toLowerCase()}_${format.hashCode().toString().replace('-', '0')}"
	}

	/**
	 * Generate header for parsing builder map values into row fields
	 * @param dataset source structured dataset
	 * @param onlyFields parse only specified fields (if empty or null, all fields are parsed)
	 * @param className class name of used nodes
	 * @param structName the name of the variable from which to take the field values
	 * @param rowName name of the map variable, where to store the field values
	 * @param tabHead code indentation in head script
	 * @param tabBody code indentation in body script
	 * @param prepareField
	 * @return scripts sections for code generation (init - parsing initialization, body - parsing fields)
	 */
	static Map<String, String> GenerateConvertFromBuilderMap(StructureFileDataset dataset, List<String> onlyFields,
															 String className, String structName, String rowName,
															 Integer tabHead, Integer tabBody, Boolean saveOnlyWithValue,
															 Closure<String> prepareField = null) {
		def rootNode = dataset.rootNode
		def subRootNodes = dataset.rootNodePath()
		if (subRootNodes.size() > 2)
			throw new ExceptionGETL("Not a correct root node \"$rootNode\", no more than one level attachment is supported!")
		def isDetail = (subRootNodes.size() == 2)

		def rootPath = dataset.dataNode

		def fields = [] as List<Field>
		if (onlyFields?.isEmpty()) onlyFields = null
		dataset.field.each { f ->
			def fn = f.name
			if (onlyFields != null && !(onlyFields.find { it.toLowerCase() == fn }))
				return

			fields << f
		}
		if (fields.isEmpty())
			return null

		// Map sections hierarchy
		def sections = new HashMap<String, Map>()
		// Sections id
		def idxSections = [] as List<String>
		// List using format date time fields
		def listFormats = [] as List<FormatElement>
		// List of field parsing code
		def listGenVal = [] as List<String>

		// Fields processing
		fields.each {destField ->
			// Source field
			def sourceField = destField.clone() as Field
			// Format field
			String format = dataset.fieldFormat(destField)

			// Add using format
			if (destField.type in [Field.dateFieldType, Field.timeFieldType, Field.datetimeFieldType, Field.timestamp_with_timezoneFieldType]) {
				if (format.toLowerCase() in ['@java', '@unix'])
					sourceField.type = Field.bigintFieldType
				else {
					sourceField.type = Field.stringFieldType
					//sourceField.format = null
					def el = new FormatElement()
					el.type = destField.type
					el.format = format
					if (listFormats.find {it.type == el.type && it.format == el.format } == null)
						listFormats.add(el)
				}
			}

			// Add using sections and setting
			def fn = destField.alias?:destField.name
			def isAlias = (destField.alias != null)
			def sp = fn
			if (/*!isAlias && */rootPath != null)
				sp = rootPath + '.' + sp
			def fnPath = sp.split('[.]')
			def sect = sections
			def lenPath = fnPath.length
			String curSectName = null
			for (int i = 0; i < lenPath - 1; i++) {
				def name = fnPath[i]
				if (curSectName == null)
					curSectName = name
				else
					curSectName = curSectName + '.' + name
				if (!sect.containsKey(name)) {
					def newSect = new HashMap<String, Map>()
					sect.put(name, newSect)
					idxSections.add(curSectName)
					sect = newSect
				}
				else {
					sect = sect.get(name)
				}
			}
			def path = (lenPath > 1)?"_getl_sect_${idxSections.indexOf(curSectName)}?":structName
			def name = fnPath[lenPath - 1]
			if (prepareField != null)
				name = prepareField.call(dataset, destField, name, isAlias)
			else
				name = StringUtils.ProcessObjectName(name, true, false)
			def value = GenerateConvertValue(dest: destField, source: sourceField, format: format,
					sourceMap: path, sourceValue: name, destMap: rowName, cloneObject:  false,
					datetimeFormatterName: '_getl_df', saveOnlyWithValue: saveOnlyWithValue)
			listGenVal.add(value)
		}

		def tabHeadSpace = ((tabHead?:0) > 0)?StringUtils.Replicate('\t', tabHead):''
		def sbHead = new StringBuilder()
		listFormats.each {el ->
			def dfName = FormatterName(el.type, el.format)
			sbHead.append(tabHeadSpace)
			switch (el.type) {
				case Field.dateFieldType:
					sbHead.append("def $dfName = getl.utils.DateUtils.BuildDateFormatter(\"${StringUtils.EscapeJava(el.format)}\")\n")
					break
				case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
					sbHead.append("def $dfName = getl.utils.DateUtils.BuildDateTimeFormatter(\"${StringUtils.EscapeJava(el.format)}\")\n")
					break
				case Field.timeFieldType:
					sbHead.append("def $dfName = getl.utils.DateUtils.BuildTimeFormatter(\"${StringUtils.EscapeJava(el.format)}\")\n")
					break
			}

			sbHead.append(tabHeadSpace)
		}

		def tabBodySpace = ((tabBody?:0) > 0)?StringUtils.Replicate('\t', tabBody):''
		def sbBody = new StringBuilder()
		sections.each {name, sect ->
			GenerateConvertFromMapSections(structName, name, name, sect, isDetail, className, idxSections, tabBodySpace, sbBody)
		}
		listGenVal.each {val ->
			sbBody.append(tabBodySpace)
			sbBody.append(val)
			sbBody.append('\n')
		}

		def res = new HashMap<String, String>()
		res.put('head', sbHead.toString())
		res.put('body', sbBody.toString())

		/*println "*** Head:"
		println res.head
		println "*** Body:"
		println res.body*/

		return res
	}

	/**
	 * Generate definition sections
	 * @param root root node object name
	 * @param name processed node name
	 * @param path parent path
	 * @param sect processed section
	 * @param className class name of used nodes
	 * @param idxSections list of known sections
	 * @param tabStr
	 * @param sb
	 */
	static private void GenerateConvertFromMapSections(String root, String name, String path, Map<String, Map> sect,
													   Boolean isDetail, String className, List<String> idxSections,
													   String tabStr, StringBuilder sb) {
		def idx = idxSections.indexOf(path)
		if (idx == -1)
			throw new ExceptionGETL("Section $path not found!")

		def sectName = "_getl_sect_$idx"

		sb.append(tabStr)
		if (isDetail && path == '#parent')
			sb.append("def ${sectName} = parent\n")
		else {
			def sectValue = StringUtils.ProcessObjectName(name, true, false)
			sb.append("def ${sectName} = ${root}?.get(${sectValue}) as $className\n")
		}
		sect.each {cName, cChild ->
			GenerateConvertFromMapSections(sectName, cName, name + '.' + cName, cChild, isDetail, className, idxSections, tabStr, sb)
		}
	}
	
	/**
	 * Generation code create empty value as field type into variable
	 * @param t
	 * @param v
	 * @return
	 */
	static String GenerateEmptyValue(Field.Type type, String variableName) {
		String r
		switch (type) {
			case Field.stringFieldType: case Field.uuidFieldType:
				r = "String ${variableName}"
				break
			case Field.booleanFieldType:
				r =  "Boolean ${variableName}"
				break
			case Field.integerFieldType:
				r =  "Integer ${variableName}"
				break
			case Field.bigintFieldType:
				r =  "Long ${variableName}"
				break
			case Field.numericFieldType:
				r =  "BigDecimal ${variableName}"
				break
			case Field.doubleFieldType:
				r =  "Double ${variableName}"
				break
			case Field.dateFieldType:
				r =  "java.sql.Date ${variableName}"
				break
			case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
				r =  "java.sql.Timestamp ${variableName}"
				break
			case Field.timeFieldType:
				r = "java.sql.Time ${variableName}"
				break
			case Field.objectFieldType: case Field.blobFieldType: case Field.textFieldType: case Field.arrayFieldType:
				r =  "def ${variableName}"
				break
			default:
				throw new ExceptionGETL("Type ${type} not supported")
		}
		return r
	}

	static String DateFormat(Field.Type type) {
		String df
		
		if (type == Field.dateFieldType)
			df = 'yyyy-MM-dd'
		else if (type == Field.timeFieldType)
			df = 'HH:mm:ss'
		else if (type == Field.datetimeFieldType)
			df = 'yyyy-MM-dd HH:mm:ss'
		else if (type == Field.timestamp_with_timezoneFieldType)
			df = 'yyyy-MM-dd\'T\'HH:mm:ss ZZ'
		else
			throw new ExceptionGETL("Can not return date format from \"${type}\" type")

		return df
	}

	/**
	 * Generate convert code from source field to destination field
	 * @param dest destination field
	 * @param source source field
	 * @param formatField parsing format
	 * @param sourceValue path to source value
	 */
	static String GenerateConvertValue(Field dest, Field source, String formatField, String sourceMap, String sourceValue,
										String destMap = 'row', Boolean saveOnlyWithValue = false) {
		def res = GenerateConvertValue(dest: dest, source: source, format: formatField, sourceMap: sourceMap,
				sourceValue: sourceValue, destMap: destMap, saveOnlyWithValue: saveOnlyWithValue)
		return res
	}
	
	/**
	 * Generate convert code from source field to destination field
	 * @param params convert parameters
	 * @return parsing code
	 */
	static String GenerateConvertValue(Map params) {
		String r

		def dest = params.dest as Field
		def source = params.source as Field
		def formatField = params.format as String
		def sourceMap = params.sourceMap
		def sourceValue = params.sourceValue as String
		def destMap = params.destMap as String
		def cloneObject = BoolUtils.IsValue(params.cloneObject, true)
		def datetimeFormatterName = params.datetimeFormatterName as String
		def convertEmptyToNull = BoolUtils.IsValue(params.convertEmptyToNull)
		def saveOnlyWithValue = BoolUtils.IsValue(params.saveOnlyWithValue)

		String validSource
		String containSource = null
		if (saveOnlyWithValue) {
			def i = sourceValue.lastIndexOf('"', sourceValue.length() - 2)
			while (i > 0 && sourceValue[i - 1] == '\\')
				i = sourceValue.lastIndexOf('"', i - 1)
			if (i > 0) {
				if (sourceValue.substring(i - 1, i) != '.')
					throw new ExceptionGETL("Failed to parse field name ${source.name} with map [${sourceMap}] alias [${sourceValue}], last dot not found!")
				containSource = "${sourceMap}.${sourceValue.substring(1, i)}containsKey(${sourceValue.substring(i)})"
			}
			else if (i == 0) {
				containSource = "${sourceMap}.containsKey(${sourceValue.substring(i)})"
			}
			else
				containSource = "${sourceMap}.containsKey($sourceValue)"
		}
		if (saveOnlyWithValue && (convertEmptyToNull && source.type == Field.stringFieldType))
			validSource = "$containSource && ${sourceMap}.${sourceValue} != null && (${sourceMap}.${sourceValue} as String).length() > 0"
		else if (saveOnlyWithValue)
			validSource = "$containSource && ${sourceMap}.${sourceValue} != null"
		else if (convertEmptyToNull && source.type == Field.stringFieldType)
			validSource = "${sourceMap}.${sourceValue} != null && (${sourceMap}.${sourceValue} as String).length() > 0"
		else
			validSource = "${sourceMap}.${sourceValue} != null"

		sourceValue = "${sourceMap}.${sourceValue}"
		def destValue = dest.name.toLowerCase().replace('\'', '\\\'')

		switch (dest.type) {
			case Field.stringFieldType: case Field.textFieldType:
				switch (source.type) {
					case Field.stringFieldType: case Field.textFieldType:
						r = sourceValue
						break

					case Field.integerFieldType: case Field.bigintFieldType:
					case Field.numericFieldType: case Field.doubleFieldType: case Field.uuidFieldType:
					case Field.objectFieldType: case Field.rowidFieldType: case Field.arrayFieldType:
						r = "${sourceValue}.toString()"
						break

					case Field.dateFieldType: case Field.timeFieldType: case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
						formatField = (formatField != null)?formatField:DateFormat(source.type)
						if (datetimeFormatterName != null) {
							def dfn = FormatterName(source.type, formatField)
							r = "getl.utils.DateUtils.FormatDate($dfn, $sourceValue as Date)"
						}
						else
							r = "getl.utils.DateUtils.FormatDate(\"${StringUtils.EscapeJava(formatField)}\", $sourceValue as Date)"

						break

					case Field.booleanFieldType:
						List<String> values = ['TRUE', 'FALSE']
						if (dest.format != null) {
							values = dest.format.split("[|]")
						}
						r = "($sourceValue as Boolean)?'${values[0]}':'${values[1]}'"

						break

					case Field.blobFieldType:
						r = "(($sourceValue as byte[]).length > 0)?new String($sourceValue as byte[]):(null as String)"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.booleanFieldType:
				switch (source.type) {
					case Field.booleanFieldType:
						r = sourceValue

						break

					case Field.integerFieldType:
						r = "($sourceValue as Integer) != 0"

						break

					case Field.bigintFieldType:
						r = "($sourceValue as Long) != 0"

						break

					case Field.numericFieldType:
						r = "($sourceValue as java.math.BigDecimal).toDouble() != 0"

						break

					case Field.stringFieldType:
						def trueValue = 'true'
						if (formatField != null) {
							def bf = formatField.toLowerCase().split("[|]")
							trueValue = bf[0]
						}
						trueValue = StringUtils.EscapeJava(trueValue)
						r = "($sourceValue as String).toLowerCase() == '$trueValue'"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.integerFieldType:
				switch (source.type) {
					case Field.integerFieldType:
						r = sourceValue

						break

					case Field.stringFieldType:
						if (formatField == null)
							r = "Integer.valueOf($sourceValue as String)"
						else {
							switch (formatField.trim().toLowerCase()) {
								case 'standard': case 'comma':
									r = "Integer.valueOf($sourceValue as String)"
									break
								case 'report': case 'report_with_comma':
									r = "Integer.valueOf(($sourceValue as String).replace(' ', '').replace('\u00A0', ''))"
									break
								default:
									throw new ExceptionGETL("Unknown format type \"$formatField\" for numeric field \"${dest.name}\"!")
							}
						}

						break

					case Field.bigintFieldType:
						r = "($sourceValue as Long).intValue()"

						break

					case Field.doubleFieldType:
						r = "($sourceValue as Double).intValue()"

						break

					case Field.numericFieldType:
						r = "($sourceValue as java.math.BigDecimal).intValue()"

						break

					case Field.booleanFieldType:
						r = "($sourceValue as Boolean)?1:0"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.bigintFieldType:
				switch (source.type) {
					case Field.bigintFieldType:
						r = sourceValue

						break

					case Field.stringFieldType:
						if (formatField == null)
							r = "Long.valueOf($sourceValue as String)"
						else {
							switch (formatField.trim().toLowerCase()) {
								case 'standard': case 'comma':
									r = "Long.valueOf($sourceValue as String)"
									break
								case 'report': case 'report_with_comma':
									r = "Long.valueOf(($sourceValue as String).replace(' ', '').replace('\u00A0', ''))"
									break
								default:
									throw new ExceptionGETL("Unknown format type \"$formatField\" for numeric field \"${dest.name}\"!")
							}
						}

						break

					case Field.integerFieldType:
						r = "Long.valueOf($sourceValue as Integer)"

						break

					case Field.doubleFieldType:
						r = "($sourceValue as Double).longValue()"

						break

					case Field.numericFieldType:
						r = "($sourceValue as java.math.BigDecimal).longValue()"

						break

					case Field.booleanFieldType:
						r = "new Long(($sourceValue)?1:0)"

						break

					case Field.dateFieldType: case Field.timeFieldType: case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
						r = "($sourceValue as Date).time"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.numericFieldType:
				switch (source.type) {
					case Field.numericFieldType:
						//r = "new BigDecimal(($sourceValue as BigDecimal).toString())" // ?
						r = sourceValue

						break

					case Field.stringFieldType:
						if (formatField == null)
							r = "new BigDecimal($sourceValue as String)"
						else {
							switch (formatField.trim().toLowerCase()) {
								case 'standard':
									r = "new BigDecimal($sourceValue as String)"
									break
								case 'comma':
									r = "new BigDecimal(($sourceValue as String).replace(',', '.'))"
									break
								case 'report':
									r = "new BigDecimal(($sourceValue as String).replace(' ', '').replace('\u00A0', ''))"
									break
								case 'report_with_comma':
									r = "new BigDecimal(($sourceValue as String).replace(',', '.').replace(' ', '').replace('\u00A0', ''))"
									break
								default:
									throw new ExceptionGETL("Unknown format type \"$formatField\" for numeric field \"${dest.name}\"!")
							}
						}

						break

					case Field.integerFieldType:
						r = "new java.math.BigDecimal($sourceValue as Integer)"

						break

					case Field.bigintFieldType:
						r = "new java.math.BigDecimal($sourceValue as Long)"

						break

					case Field.doubleFieldType:
						r = "new java.math.BigDecimal($sourceValue as Double)"

						break

					case Field.booleanFieldType:
						r = "new BigDecimal(($sourceValue as Boolean)?1:0)"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.doubleFieldType:
				switch (source.type) {
					case Field.doubleFieldType:
						r = sourceValue

						break

					case Field.stringFieldType:
						if (formatField == null)
							r = "Double.valueOf($sourceValue as String)"
						else {
							switch (formatField.trim().toLowerCase()) {
								case 'standard':
									r = "Double.valueOf($sourceValue as String)"
									break
								case 'comma':
									r = "Double.valueOf(($sourceValue as String).replace(',', '.'))"
									break
								case 'report':
									r = "Double.valueOf(($sourceValue as String).replace(' ', '').replace('\u00A0', ''))"
									break
								case 'report_with_comma':
									r = "Double.valueOf(($sourceValue as String).replace(',', '.').replace(' ', '').replace('\u00A0', ''))"
									break
								default:
									throw new ExceptionGETL("Unknown format type \"$formatField\" for numeric field \"${dest.name}\"!")
							}
						}

						break

					case Field.integerFieldType:
						r = "Double.valueOf($sourceValue as Integer)"

						break

					case Field.bigintFieldType:
						r = "Double.valueOf($sourceValue as Long)"

						break

					case Field.numericFieldType:
						r = "($sourceValue as java.math.BigDecimal).doubleValue()"

						break

					case Field.booleanFieldType:
						r = "new Double(($sourceValue as Boolean)?1:0)"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.dateFieldType:
				formatField = formatField?:DateFormat(dest.type)

				switch (source.type) {
					case Field.dateFieldType:
						r = sourceValue

						break
					case Field.timeFieldType: case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
						r = "new java.sql.Date(($sourceValue as Date).time)"

						break

					case Field.stringFieldType:
						if (datetimeFormatterName != null) {
							def dfn = FormatterName(dest.type, formatField)
							r = "getl.utils.DateUtils.ParseSQLDate($dfn, $sourceValue as String, false)"
						}
						else
							r =  "getl.utils.DateUtils.ParseSQLDate(\"${StringUtils.EscapeJava(formatField)}\", $sourceValue as String, false)"

						break

					case Field.bigintFieldType:
						formatField = (formatField?:'@java').toLowerCase()
						if (formatField == '@java')
							r =  "new java.sql.Date(($sourceValue as Long))"
						else if (formatField == '@unix')
							r =  "new java.sql.Date(($sourceValue as Long) * 1000)"
						else
							throw new ExceptionGETL("Unknown format value \"$formatField\" for field \"${source.name}\"!")

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
				formatField = formatField?:DateFormat(dest.type)

				switch (source.type) {
					case Field.datetimeFieldType:
						r = sourceValue

						break
					case Field.dateFieldType: case Field.timeFieldType: case Field.timestamp_with_timezoneFieldType:
						r = "new java.sql.Timestamp(($sourceValue as Date).time)"

						break

					case Field.stringFieldType:
						if (datetimeFormatterName != null) {
							def dfn = FormatterName(dest.type, formatField)
							r = "getl.utils.DateUtils.ParseSQLTimestamp($dfn, $sourceValue as String, false)"
						}
						else
							r =  "getl.utils.DateUtils.ParseSQLTimestamp(\"${StringUtils.EscapeJava(formatField)}\", $sourceValue as String, false)"

						break

					case Field.bigintFieldType:
						formatField = (formatField?:'@java').toLowerCase()
						if (formatField == '@java')
							r =  "new java.sql.Timestamp(($sourceValue as Long))"
						else if (formatField == '@unix')
							r =  "new java.sql.Timestamp(($sourceValue as Long) * 1000)"
						else
							throw new ExceptionGETL("Unknown format value \"$formatField\" for field \"${source.name}\"!")


						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.timeFieldType:
				formatField = formatField?: DateFormat(dest.type)

				switch (source.type) {
					case Field.timeFieldType:
						r = sourceValue

						break

					case Field.dateFieldType: case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
						r = "new java.sql.Time(($sourceValue as Date).time)"

						break

					case Field.stringFieldType:
						if (datetimeFormatterName != null) {
							def dfn = FormatterName(dest.type, formatField)
							r = "getl.utils.DateUtils.ParseSQLTime($dfn, $sourceValue as String, false)"
						}
						else
							r =  "getl.utils.DateUtils.ParseSQLTime(\"${StringUtils.EscapeJava(formatField)}\", $sourceValue as String, false)"

						break

					case Field.bigintFieldType:
						formatField = (formatField?:'@java').toLowerCase()
						if (formatField == '@java')
							r =  "new java.sql.Time(($sourceValue as Long))"
						else if (formatField == '@unix')
							r =  "new java.sql.Time(($sourceValue as Long) * 1000)"
						else
							throw new ExceptionGETL("Unknown format value \"$formatField\" for field \"${source.name}\"!")

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.blobFieldType:
				switch (source.type) {
					case Field.blobFieldType:
						r = "($sourceValue as byte[]).clone()"

						break

					case Field.stringFieldType:
						r = "($sourceValue as String).bytes)"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.uuidFieldType:
				switch (source.type) {
					case Field.uuidFieldType:
						r = sourceValue

						break

					case Field.stringFieldType:
						r = "java.util.UUID.fromString($sourceValue as String)"

						break

					default:
						throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				}

				break

			case Field.objectFieldType: case Field.arrayFieldType:
				if (cloneObject)
					r = "($sourceValue instanceof Cloneable)?${sourceValue}.clone():$sourceValue"
				else
					r = sourceValue

				break

			default:
				throw new ExceptionGETL("Type ${dest.type} not supported (${dest.name})")
		}

		def res = "if ($validSource) ${destMap}.put('$destValue', $r)"

		return res
	}
	
	static public final Random random = new Random()

	@CompileStatic
	static Integer GenerateInt () {
        return random.nextInt()
	}
	
	@CompileStatic
	static Integer GenerateInt (Integer minValue, Integer maxValue) {
		def res = minValue - 1
		while (res < minValue) res = random.nextInt(maxValue + 1)
        return res
	}
	
	@CompileStatic
	static String GenerateString(Integer length) {
		String result = ""
		while (result.length() < length) result += ((result.length() > 0)?" ":"") + StringUtils.RandomStr().replace('-', ' ')
		
		def l2 = (int)(length / 2)
		def l = GenerateInt(l2, length)

        return StringUtils.LeftStr(result + "a", l)
	}
	
	@CompileStatic
	static Long GenerateLong () {
        return random.nextLong()
	}
	
	@CompileStatic
	static BigDecimal GenerateNumeric () {
        return BigDecimal.valueOf(random.nextDouble()) + random.nextInt()
	}
	
	@CompileStatic
	static BigDecimal GenerateNumeric (Integer precision) {
        return NumericUtils.Round(BigDecimal.valueOf(random.nextDouble()) + random.nextInt(), precision)
	}
	
	@CompileStatic
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
	
	@CompileStatic
	static double GenerateDouble () {
        return random.nextDouble() + random.nextLong()
	}
	
	@CompileStatic
	static Boolean GenerateBoolean () {
        return random.nextBoolean()
	}
	
	@CompileStatic
	static Date GenerateDate() {
        return DateUtils.AddDate("dd", -GenerateInt(0, 365), DateUtils.CurrentDate())
	}
	
	@CompileStatic
	static Date GenerateDate(Integer days) {
        return DateUtils.AddDate("dd", -GenerateInt(0, days), DateUtils.CurrentDate())
	}

	@CompileStatic
	static Date GenerateDate(Date date, Integer days) {
		return DateUtils.AddDate("dd", GenerateInt(0, days), date)
	}
	
	@CompileStatic
	static Date GenerateDateTime() {
        return DateUtils.AddDate("ss", -GenerateInt(0, 300000000), DateUtils.Now())
	}
	
	@CompileStatic
	static Date GenerateDateTime(Integer seconds) {
        return DateUtils.AddDate("ss", -GenerateInt(0, seconds), DateUtils.Now())
	}

	@CompileStatic
	static Date GenerateDateTime(Date date, Integer seconds) {
		return DateUtils.AddDate("ss", GenerateInt(0, seconds), date)
	}
	
	@CompileStatic
	static def GenerateValue(Field f, Boolean lengthTextInBytes = false) {
        return GenerateValue(f, lengthTextInBytes, null)
	}
	
	/**
	 * Generate random value from fields
	 * @param f
	 * @param rowID
	 * @return
	 */
	@CompileStatic
	static def GenerateValue(Field f, Boolean lengthTextInBytes, def rowID) {
		def result
		def l = f.length?:1
		
		if (f.isNull && GenerateBoolean())
			return null
		
		switch (f.type) {
			case Field.stringFieldType:
				if (lengthTextInBytes)
					l = l.intdiv(2) as Integer
				result = GenerateString(l)
				break
			case Field.booleanFieldType:
				result = GenerateBoolean()
				break
			case Field.integerFieldType:
                if (f.isKey && rowID != null) {
                    result = rowID
                }
                else {
                    if (f.minValue == null && f.maxValue == null) result = GenerateInt() else result = GenerateInt((Integer)f.minValue?:0, (Integer)f.maxValue?:1000000)
                }

                break
			case Field.bigintFieldType:
				if (f.isKey && rowID != null) {
					result = rowID
				}
				else {
					if (f.minValue == null && f.maxValue == null) result = GenerateLong() else result = Long.valueOf(GenerateInt((Integer)f.minValue?:0, (Integer)f.maxValue?:1000000))
				}

				break
			case Field.numericFieldType:
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
			case Field.doubleFieldType:
				result = GenerateDouble()
				break
			case Field.dateFieldType:
				result = new java.sql.Date(GenerateDate().time)
				break
			case Field.timeFieldType:
				result = new java.sql.Time(GenerateDate().time)
				break
			case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
				result = new java.sql.Timestamp(GenerateDateTime().time)
				break
            case Field.textFieldType:
				result = GenerateString((l < 65536)?l:65536)
                break
            case Field.blobFieldType:
				l = (l?:65536).intdiv(2) as Integer
                result = GenerateString(l).bytes
                break
			case Field.uuidFieldType:
				result = UUID.randomUUID().toString()
				break
			case Field.arrayFieldType:
				def list = [] as List<Integer>
				(1..GenerateInt(1, 10)).each {
					list.add(GenerateInt(1, 100))
				}
				result = list

				break
			default:
				l = (l?:65536).intdiv(2) as Integer
				result = GenerateString(l)
		}

        return result
	}
	
	@CompileStatic
	static Map GenerateRowValues(List<Field> field, Boolean lengthTextInBytes = false, def rowID = null) {
		Map row = new HashMap()
		field.each { Field f ->
			def fieldName = f.name.toLowerCase()
			def value = GenerateValue(f, lengthTextInBytes, rowID)
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
	static Closure GenerateRandomRow(Dataset dataset, List excludeFields = [], Map rules = new HashMap()) {
		if (excludeFields == null) excludeFields = [] as List<String>
		excludeFields = (excludeFields as List<String>)*.toLowerCase()
		if (rules == null)
			rules = new HashMap()
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

			def rule = (rules.get(fieldName)?:new HashMap<String, Object>()) as Map<String, Object>
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

		return EvalGroovyClosure(value: sb.toString(), owner: dataset.dslCreator)
	}
	
	/**
	 * Return string value for generators 
	 * @param value
	 * @return
	 */
	@CompileStatic
	static String GenerateStringValue (String value) {
		if (value == null) return "null"
		return '"' + value.replace('"', '\\"') + '"'
	}
	
	@CompileStatic
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
	 * @param fields list of field
	 * @param propName field array name
	 * @return map structure
	 */
	static Map Fields2Map (List<Field> fields, String propName = 'fields') {
		if (fields == null)
			return null
		
		def res = new HashMap()
		def l = [] as List<Map>
		res.put(propName, l)

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
	 * @param value object properties
	 * @param propName field array name
	 * @return list of field
	 */
	static List<Field> Map2Fields(Map value, String propName = 'fields') {
		if (value == null)
			return null

		List<Field> res = []
		
		(value.get(propName) as List<Map>)?.each { f ->
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
	@NamedVariant
	static Closure EvalGroovyClosure(String value, Map vars = null,
									 Boolean convertReturn = false, ClassLoader classLoader = null, Getl owner = null) {
        return EvalGroovyScript(value: value, vars: vars, convertReturn: BoolUtils.IsValue(convertReturn),
				classLoader: classLoader, owner: owner) as Closure
	}

	/**
	 * Evaluate ${variable} in text
	 * @param value
	 * @param vars
	 * @return
	 */
	@CompileStatic
	static String EvalText(String value, Map<String, Object> vars) {
		if (value == null) return null
		vars.each { String key, Object val ->
			key = '${' + key + '}'
			value = value.replace(key, val.toString())
		}

        return value
	}

	/**
	 * Run groovy script
	 * @param value code text
	 * @param vars used variables
	 * @param convertReturn convert the line return character so that it is not involved in processing
	 * @param classLoader the load class through which to compile the code
	 * @param owner Getl creator
	 * @return generated closure code
	 */
	@CompileStatic
	@NamedVariant
	static def EvalGroovyScript(String value, Map vars = null,
								Boolean convertReturn = false, ClassLoader classLoader = null, Getl owner = null) {
		if (value == null)
			return null

		convertReturn = BoolUtils.IsValue(convertReturn)
		if (convertReturn)
			value = value.replace('\r', '\u0001')

		def logger = (owner?.logging?.manager != null)?owner.logging.manager:Logs.global

		Binding bind = new Binding()
		(vars as Map<String, Object>)?.each { String key, Object val ->
			bind.setVariable(key, val)
		}

		def sh = (classLoader == null)?new GroovyShell(bind):new GroovyShell(classLoader, bind,
				CompilerConfiguration.DEFAULT)

		def res
		try {
			res = sh.evaluate(value)
			if (convertReturn && res != null)
				res = (res as String).replace('\u0001', '\r')
		}
		catch (Exception e) {
			logger.severe("Error parse [${StringUtils.CutStr(value, 1000)}]")
			StringBuilder sb = new StringBuilder("script:\n$value\nvars:")
			vars?.each { varName, varValue -> sb.append("\n	$varName: ${StringUtils.LeftStr(varValue.toString(), 256)}") }
			logger.dump(e, 'GenerationUtils', 'EvalGroovyScript', sb.toString())
			throw e
		}

		return res
	}

	/**
	 * Convert field type to string type
	 * @param field
	 * @return
	 */
	static void FieldConvertToString (Field field) {
		def len
		def type = Field.stringFieldType
		switch (field.type) {
			case Field.stringFieldType:
				break
			case Field.rowidFieldType:
				field.length = 50
				break
			case Field.textFieldType: case Field.blobFieldType:
				type = Field.stringFieldType
				if (field.length == null) len = 65535
				break
			case Field.bigintFieldType:
				len = 38
				break
			case Field.integerFieldType:
				len = 13
				break
			case Field.dateFieldType: case Field.datetimeFieldType: case Field.timeFieldType:
			case Field.timestamp_with_timezoneFieldType:
				len = 30
				break
			case Field.booleanFieldType:
				len = 5
				break
			case Field.doubleFieldType:
				len = 50
				break
			case Field.numericFieldType:
				len = (field.length?:50) + 1
				break
			case Field.uuidFieldType:
				type = Field.stringFieldType
				len = 36
				break
			default:
				throw new ExceptionGETL("Not support convert field type \"${field.type}\" to \"STRING\" from field \"${field.name}\"")
		}
		field.type = type
		if (len != null) field.length = len
		field.precision = null
		field.typeName = null
		field.columnClassName = null
	}
	
	/**
	 * Convert all dataset fields to string
	 * @param dataset source dataset
	 */
	@CompileStatic
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
	 * @param dataset source dataset
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
	@CompileStatic
	static Map RowKeyMapValues(List<Field> fields, Map row, List<String> excludeFields) {
		Map res = new HashMap()
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
	@CompileStatic
	static List RowListValues (List<String> fields, Map row) {
		def res = []
		fields.each { String n ->
			res.add(row.get(n.toLowerCase()))
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
	@CompileStatic
	static Map RowMapValues(List<String> fields, Map row, Boolean toLower) {
		Map res = new HashMap()
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
	@CompileStatic
	static Map RowMapValues (List<String> fields, Map row) {
		RowMapValues(fields, row, true)
	}
	
	/**
	 * Generation row copy closure
	 * @param fields
	 * @return
	 */
	static Map<String, Object> GenerateRowCopy(JDBCDriver driver, List<Field> fields, Boolean sourceIsMap = false) {
		if (!driver.isConnected())
			driver.connect()

		def getSourceMethod = (sourceIsMap)?'get':'getAt'

		StringBuilder sb = new StringBuilder()
		sb << "Closure code = { java.sql.Connection connection, ${(sourceIsMap)?'Map':'groovy.sql.GroovyResultSet'} inRow, Map outRow -> methodRowCopy(connection, inRow, outRow) }\n"
		sb << '\n@groovy.transform.CompileStatic\n'
		sb << "void methodRowCopy(java.sql.Connection connection, ${(sourceIsMap)?'Map':'groovy.sql.GroovyResultSet'} inRow, Map outRow) {\n"
		def i = 0
		fields.each { Field f ->
			i++

			def fName = f.name.toLowerCase().replace("'", "\\'")

			sb << "	def _getl_temp_var_$i = inRow.${getSourceMethod}('$fName')\n"
			sb << "	if (_getl_temp_var_$i == null) outRow.put('$fName', null) else {\n"
			if (f.getMethod != null) sb << "\t\t_getl_temp_var_$i = ${f.getMethod.replace("{field}", "_getl_temp_var_$i")}\n"

			switch (f.type) {
				case Field.timestamp_with_timezoneFieldType:
					if (!driver.timestamptzReadAsTimestamp())
						//sb << " outRow.put('$fName', java.sql.Timestamp.valueOf((_getl_temp_var_${i} as java.time.OffsetDateTime).toLocalDateTime()))"
						sb << " outRow.put('$fName', (_getl_temp_var_${i} as java.time.OffsetDateTime).toDate().toTimestamp())"
					else
						sb << "	outRow.put('$fName', _getl_temp_var_${i})"
					break
				case Field.blobFieldType:
					if (driver.blobReadAsObject()) {
						sb << "	outRow.put('$fName', (_getl_temp_var_${i} as java.sql.Blob).getBytes((long)1, (int)((_getl_temp_var_${i} as java.sql.Blob).length())))"
					}
					else {
						sb << "	outRow.put('$fName', _getl_temp_var_${i})"
					}
					break
				case Field.textFieldType:
					if (driver.textReadAsObject()) {
						sb << "		String clob_value = (_getl_temp_var_${i} as java.sql.Clob).getSubString((long)1, ((int)(_getl_temp_var_${i} as java.sql.Clob).length()))\n"
						sb << "		outRow.put('$fName', clob_value)"
					}
					else {
						sb << "		outRow.put('$fName', _getl_temp_var_${i})"
					}
					break
				case Field.uuidFieldType:
					sb << "		outRow.put('$fName', _getl_temp_var_${i}.toString())"
					break
				case Field.arrayFieldType:
					sb << "		outRow.put('$fName', ((_getl_temp_var_${i} as java.sql.Array).array as Object[]).toList())"
					break
				default:
					sb << "		outRow.put('$fName', _getl_temp_var_${i})"
			}

			sb << '\n	}\n'

		}
		sb << "}\nreturn code"
		def statement = sb.toString()
//		println statement

		Closure code = EvalGroovyClosure(value: statement, convertReturn: false,
				classLoader: (driver.useLoadedDriver)?driver.jdbcClass?.classLoader:null, owner: driver.connection.dslCreator)

        return [statement: statement, code: code]
	}

	/**
	 * Generation field copy by fields
	 * @param fields list of fields
	 * @param dataset owner dataset
	 * @return
	 */
	static Closure GenerateFieldCopy(List<Field> fields, Dataset dataset = null) {
		StringBuilder sb = new StringBuilder()
		sb << "{ Map<String, Object> inRow, Map<String, Object> outRow -> methodCopy(inRow, outRow) }"
		sb << '\n@groovy.transform.CompileStatic\n'
		sb << 'void methodCopy(Map<String, Object> inRow, Map<String, Object> outRow) {\n'
		fields.each { Field f ->
			def fName = f.name.toLowerCase().replace("'", "\\'")
			sb << "outRow.put('$fName', inRow.get('$fName'))\n"
		}
		sb << "}"
		Closure result = EvalGroovyClosure(value: sb.toString(), owner: dataset?.dslCreator)
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
				if (!driver.isSupport(Driver.Support.BOOLEAN))
					throw new ExceptionGETL("${driver.class.simpleName} driver not support \"BOOLEAN\" type in field \"${field.name}\"!")

				res = "if ($value != null) _getl_stat.setBoolean($paramNum, ($value) as Boolean) else _getl_stat.setNull($paramNum, java.sql.Types.BOOLEAN)"
				break
				
			case types.DOUBLE:
				res = "if ($value != null) _getl_stat.setDouble($paramNum, ($value) as Double) else _getl_stat.setNull($paramNum, java.sql.Types.DOUBLE)"
				break
				
			case types.NUMERIC:
				res = "if ($value != null) _getl_stat.setBigDecimal($paramNum, ($value) as BigDecimal) else _getl_stat.setNull($paramNum, java.sql.Types.DECIMAL)"
				break
				
			case types.BLOB:
				if (!driver.isSupport(Driver.Support.BLOB))
					throw new ExceptionGETL("${driver.class.simpleName} driver not support \"BLOB\" type in field \"${field.name}\"!")

				res = "blobWrite(_getl_con, _getl_stat, $paramNum, ($value) as byte[])"
				break
				
			case types.TEXT:
				if (!driver.isSupport(Driver.Support.CLOB))
					throw new ExceptionGETL("${driver.class.simpleName} driver not support \"CLOB\" type in field \"${field.name}\"!")

				if (driver.textReadAsObject()) {
					res = "clobWrite(_getl_con, _getl_stat, $paramNum, ($value) as String)"
				}
				else {
					res = "if ($value != null) _getl_stat.setString($paramNum, ($value) as String) else _getl_stat.setNull($paramNum, java.sql.Types.VARCHAR)"
				}
				break
				
			case types.DATE:
				if (!driver.isSupport(Driver.Support.DATE))
					throw new ExceptionGETL("${driver.class.simpleName} driver not support \"DATE\" type in field \"${field.name}\"!")
				res = "if ($value != null) _getl_stat.setDate($paramNum, new java.sql.Date(((${value}) as Date).getTime())) else _getl_stat.setNull($paramNum, java.sql.Types.DATE)"
				break
				
			case types.TIME:
				if (!driver.isSupport(Driver.Support.TIME))
					throw new ExceptionGETL("${driver.class.simpleName} driver not support \"TIME\" type in field \"${field.name}\"!")
				res = "if ($value != null) _getl_stat.setTime($paramNum, new java.sql.Time(((${value}) as Date).getTime())) else _getl_stat.setNull($paramNum, java.sql.Types.TIME)"
				break
				
			case types.TIMESTAMP:
				if (!driver.isSupport(Driver.Support.TIMESTAMP))
					throw new ExceptionGETL("${driver.class.simpleName} driver not support \"TIMESTAMP\" type in field \"${field.name}\"!")
				res = "if ($value != null) _getl_stat.setTimestamp($paramNum, new java.sql.Timestamp(((${value}) as Date).getTime())) else _getl_stat.setNull($paramNum, java.sql.Types.TIMESTAMP)"
				break

			case types.TIMESTAMP_WITH_TIMEZONE:
				if (!driver.isSupport(Driver.Support.TIMESTAMP_WITH_TIMEZONE))
					throw new ExceptionGETL("${driver.class.simpleName} driver not support \"TIMESTAMP WITH TIMEZONE\" type in field \"${field.name}\"!")
				if (!driver.timestampWithTimezoneConvertOnWrite())
					res = "if ($value != null) _getl_stat.setTimestamp($paramNum, new java.sql.Timestamp(((${value}) as Date).getTime())) else _getl_stat.setNull($paramNum, java.sql.Types.TIMESTAMP_WITH_TIMEZONE)"
				else
					res = "if ($value != null) _getl_stat.setObject($paramNum, ((${value}) as Date).toInstant().atZone(java.time.ZoneId.of('UTC')).toLocalDateTime()) else _getl_stat.setNull($paramNum, java.sql.Types.TIMESTAMP_WITH_TIMEZONE)"
				break

			case types.ARRAY:
				if (!driver.isSupport(Driver.Support.ARRAY))
					throw new ExceptionGETL("${driver.class.simpleName} driver not support \"ARRAY\" type in field \"${field.name}\"!")

				res = """if ($value != null) {
    def val_$paramNum = $value
    def arr_$paramNum = (val_${paramNum}.class.isArray())?val_${paramNum}:(val_${paramNum} as List).toArray()
	//_getl_stat.setArray($paramNum, _getl_con.createArrayOf('${field.arrayType?:'OBJECT'}', arr_$paramNum))
	_getl_stat.setObject($paramNum, arr_$paramNum)
}
else 
	_getl_stat.setNull($paramNum, java.sql.Types.ARRAY)"""
				break
			default:
				if (field.type == Field.uuidFieldType) {
					if (!driver.isSupport(Driver.Support.UUID))
						throw new ExceptionGETL("${driver.class.simpleName} driver not support \"DATETIME\" type in field \"${field.name}\"!")

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

	/**
	 * Return the format of the specified field
	 * @param field dataset field
	 * @return format
	 */
	@CompileStatic
	static String FieldFormat(Map formats, Field field) {
		if (field.format != null)
			return field.format
		String dtFormat = null
		if (formats.uniFormatDateTime != null)
			dtFormat = formats.uniFormatDateTime as String

		String res = null
		switch (field.type) {
			case Field.dateFieldType:
				res = dtFormat?:(formats.formatDate as String)
				break
			case Field.datetimeFieldType:
				res = dtFormat?:(formats.formatDateTime as String)
				break
			case Field.timestamp_with_timezoneFieldType:
				res = dtFormat?:(formats.formatTimestampWithTz as String)
				break
			case Field.timeFieldType:
				res = dtFormat?:(formats.formatTime as String)
				break
			case Field.booleanFieldType:
				res = (formats.formatBoolean as String)
				break
			case Field.integerFieldType: case Field.bigintFieldType: case Field.numericFieldType:
				res = (formats.formatNumeric as String)
		}

		return res
	}
}