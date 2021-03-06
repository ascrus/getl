package getl.utils

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.files.FileManager
import getl.jdbc.TableDataset
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import groovy.transform.CompileStatic
import getl.utils.opts.PathVarsSpec
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import java.time.format.DateTimeFormatter
import java.util.regex.Matcher
import java.util.regex.Pattern
import getl.data.Field
import getl.exception.ExceptionGETL

/**
 * Analyze and processing path value class
 * @author Alexsey Konstantinov
 */
class Path implements Cloneable, GetlRepository {
	Path() {
		registerMethod()
	}

	Path(Map params) {
		registerMethod()
		if (params?.vars != null) {
			setMaskVariables(params.vars as Map)
		}
		compile(MapUtils.CleanMap(params, ['vars']))
	}

	protected ParamMethodValidator methodParams = new ParamMethodValidator()

	private void registerMethod() {
		methodParams.register("compile", ["mask", "sysVar", "patterns", "vars"])
	}

	/**
	 * Change from process path Windows file separator to Unix separator
	 */
	public Boolean changeSeparator = (File.separatorChar != '/'.charAt(0))

	/**
	 * Ignoring converting error and set null value
	 */
	public Boolean ignoreConvertError = false

	/** Mask path
	 * <br>(use {var} for definition variables) */
	private String mask
	/** Mask path
	 * <br>(use {var} for definition variables) */
	String getMask() { mask }
	/** Mask path
	 * <br>(use {var} for definition variables) */
	void setMask(String value) {
		isCompile = false
		mask = value
	}

	/** Original string mask */
	private String maskStr
	/** Original string mask */
	String getMaskStr() { maskStr }

	/** Path elements */
	private List<Map> elements = []
	/** Path elements */
	List<Map> getElements() { elements }

	/** SQL like elements */
	private List<Map> likeElements = []
	/** SQL like elements */
	@SuppressWarnings('unused')
	List<Map> getLikeElements() { likeElements }

	/** Count level for mask */
	Integer getCountLevel() { elements.size() }

	/** Root path with mask */
	private String rootPath
	/** Root path with mask */
	String getRootPath() { this.rootPath }

	/** Count elements in root path */
	private Integer numLocalPath
	/** Count elements in root path */
	Integer getNumLocalPath() { this.numLocalPath }

	/** Expression file path with mask */
	private String maskPath
	/** Expression file path with mask */
	String getMaskPath() { this.maskPath }

	/** Expression folder path with mask */
	private String maskFolder
	/** Expression folder path with mask */
	String getMaskFolder() { this.maskFolder }

	/** Expression mask file */
	private String maskFile
	/** Expression mask file */
	String getMaskFile() { this.maskFile }

	/** Expression folder path with mask for SQL like */
	private String likeFolder
	/** Expression folder path with mask for SQL like */
	String getLikeFolder() { this.likeFolder }

	/** Expression mask file for SQL like */
	private String likeFile
	/** Expression mask file for SQL like */
	String getLikeFile() { this.likeFile }

	private Map<String, Map> vars = (MapUtils.UnmodifiableMap(new HashMap<String, Map>()) as Map<String, Map>)

	/**
	 * Compiled mask variables<br><br>
	 * <b>Field for var:</b>
	 * <ul>
	 * <li>Field.Type type	- type var
	 * <li>String format	- parse format
	 * <li>int len			- length value of variable
	 * <li>int lenMin		- minimum length of variable
	 * <li>int lenMax		- maximum length of variable
	 * <li>Closure calc		- value calculation code
	 * </ul>
	 */
	Map<String, Map> getVars() { this.vars }

	/**
	 * Variable parameters for compiling a mask
	 * <b>Field for var:</b>
	 * <ul>
	 * <li>Field.Type type	- type var
	 * <li>String format	- parse format
	 * <li>int len			- length value of variable
	 * <li>int lenMin		- minimum length of variable
	 * <li>int lenMax		- maximum length of variable
	 * <li>Closure calc		- value calculation code
	 * </ul>
	 */
	private final def maskVariables = ([:] as Map<String, Map<String, Object>>)

	/**
	 * Variable parameters for compiling a mask
	 * <b>Field for var:</b>
	 * <ul>
	 * <li>Field.Type type	- type var
	 * <li>String format	- parse format
	 * <li>int len			- length value of variable
	 * <li>int lenMin		- minimum length of variable
	 * <li>int lenMax		- maximum length of variable
	 * <li>Closure calc		- value calculation code
	 * </ul>
	 */
	Map<String, Map<String, Object>> getMaskVariables() { maskVariables }

	/**
	 * Variable parameters for compiling a mask
	 * <b>Field for var:</b>
	 * <ul>
	 * <li>Field.Type type	- type var
	 * <li>String format	- parse format
	 * <li>int len			- length value of variable
	 * <li>int lenMin		- minimum length of variable
	 * <li>int lenMax		- maximum length of variable
	 * <li>Closure calc		- value calculation code
	 * </ul>
	 */
	void setMaskVariables(Map<String, Map<String, Object>> value) {
		maskVariables.clear()
		if (value != null) maskVariables.putAll(value)
	}

	/** Date formatter for variables */
	private final Map<String, DateTimeFormatter> varDateFormatter = [:] as Map<String, DateTimeFormatter>

	/** System parameters */
	private final Map<String, Object> sysParams = [:] as Map<String, Object>
	/** System parameters */
	Map<String, Object> getSysParams() { sysParams }

	@JsonIgnore
	String getDslNameObject() { sysParams.dslNameObject as String }
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	@JsonIgnore
	Getl getDslCreator() { sysParams.dslCreator as Getl }
	void setDslCreator(Getl value) { sysParams.dslCreator = value }

	/** Define variable options */
	Map variable(String name,
				 @DelegatesTo(PathVarsSpec) @ClosureParams(value = SimpleType, options = ['getl.utils.opts.PathsVarSpec'])
					Closure cl = null) {
		if (name == null || name == '') throw new ExceptionGETL('Name required for variable!')

		def var = maskVariables.get(name) as Map
		if (var == null) {
			var = [:]
			maskVariables.put(name, (var as Map<String, Object>))
			isCompile = false
		}

		def parent = new PathVarsSpec(dslCreator, true, var)
		parent.runClosure(cl)
		if (cl != null) isCompile = false

		return parent.params
	}

	/** Mask path pattern */
	private Pattern maskPathPattern
	/** Mask path pattern */
	Pattern getMaskPathPattern() { maskPathPattern }
	/** Path already compiled */
	private Boolean isCompile = false
	/** Path already compiled */
	Boolean getIsCompile() { isCompile }

	/**
	 * Compile path mask<br><br>
	 * <b>Parameters</b>
	 * <ul>
	 * <li>String mask - mask of filename<br>
	 * <li>boolean sysVar - include system variables #file_date and #file_size
	 * <li>Map patterns - pattern of variable for use in generation regular expressions
	 * <li>Map<String, Map> vars - attributes of variable
	 * </ul>
	 */
	void compile(Map params = [:]) {
		if (params == null) params = [:]
		methodParams.validation("compile", params)
		maskStr = (params.mask as String)?:mask
		if (maskStr == null) throw new ExceptionGETL("Required parameter \"mask\"")
		def sysVar = BoolUtils.IsValue(params.sysVar)
		Map patterns = (params.patterns as Map)?:[:]
		def varAttr = ([:] as Map<String, Map<String, Object>>)
		varAttr.putAll(maskVariables)
		if (params.vars != null) MapUtils.MergeMap(varAttr, params.vars as Map<String, Map<String, Object>>)

		elements.clear()
		likeElements.clear()
		rootPath = null
		maskPath = null
		maskPathPattern = null
		maskFolder = null
		maskFile = null
		likeFolder = null
		likeFile = null
		numLocalPath = -1
		varDateFormatter.clear()

		def compVars = ([:] as Map<String, Map<String, Object>>)
		def rMask = FileUtils.FileMaskToMathExpression(maskStr)

		String[] d = rMask.split("/")
		StringBuilder rb = new StringBuilder()

		def listFoundVars = [] as List<String>
		def varsFormat = [:] as Map<String, String>
		for (Integer i = 0; i < d.length; i++) {
			Map p = [:]
			p.vars = []
			StringBuilder b = new StringBuilder()

			def f = 0
			def s = d[i].indexOf('{')
			while (s >= 0) {
				def e = d[i].indexOf('}', s)
				if (e >= 0) {
					String df = null

					b.append(d[i].substring(f, s))

					String vn = d[i].substring(s + 1, e).toLowerCase()
					def pm = patterns.get(vn)
					if (pm == null) {
						def vt = varAttr.get(vn)
						def type = vt?.type as Field.Type
						if (type != null && type instanceof String) type = Field.Type."$type"
						if (type in [Field.dateFieldType, Field.timeFieldType, Field.datetimeFieldType, Field.timestamp_with_timezoneFieldType]) {
							if (vt.format != null) {
								df = vt.format
							}
							else {
								if (type == Field.dateFieldType) {
									df = 'yyyy-MM-dd'
								}
								else if (type == Field.timeFieldType) {
									df = 'HH:mm:ss'
								}
								else if (type == Field.datetimeFieldType) {
									df = 'yyyy-MM-dd HH:mm:ss'
								}
								else {
									df = 'yyyy-MM-dd\'T\'HH:mm:ssZ'
								}
							}

							def vm = df.toLowerCase()
							vm = vm.replace('\'', '')
									.replace('d', '\\d')
									.replace('y', '\\d')
									.replace('m', '\\d')
									.replace('h', '\\d')
									.replace('s', '\\d')
									.replace('t', '\\d')
									.replace('z', '\\d')

							b.append("($vm)")
						}
						else if (type in [Field.bigintFieldType, Field.integerFieldType]) {
							if (vt?.len != null) {
								b.append("(\\d{${vt.len}})")
							}
							else if (vt?.lenMin != null && vt?.lenMax != null) {
								b.append("(\\d{${vt.lenMin},${vt.lenMax}})")
							}
							else {
								b.append("(\\d+)")
							}
						}
						else {
							if (vt?.format != null) {
								df = vt.format as String
								b.append("(${vt.format})")
							}
							else {
								b.append("(.+)")
							}
						}
					}
					else {
						b.append("(${pm})")
					}

					if (vn in listFoundVars)
						throw new ExceptionGETL("Duplicate variable \"${vn}\" in path mask \"$maskStr\"")

					(p.vars as List).add(vn)
					listFoundVars << vn
					if (df != null)
						varsFormat.put(vn, df)

					f = e + 1
				}
				s = d[i].indexOf('{', f)
			}
			if (f > 0 && f < d[i].length() - 1) {
				b.append(d[i].substring(f))
			}

			p.mask = ((p.vars as List).size() == 0)?d[i]:b.toString()
			p.like = ((p.vars as List).size() == 0)?d[i]:'%'
			elements.add(p)

			if ((p.vars as List).size() == 0) {
				if (numLocalPath == -1 && i < d.length - 1)
					rb.append("/" + d[i])
			}
			else
				if (numLocalPath == -1) numLocalPath = i
		}
		rootPath = (rb.length() == 0)?".":rb.toString().substring(1)

		for (Integer i = 0; i < elements.size(); i++) {
			List<String> v = elements[i].vars as List<String>
			for (Integer x = 0; x < v.size(); x++) {
				def varName = v[x] as String
				def varValue = [:] as Map<String, Object>

				def df = varsFormat.get(varName)
				if (df != null)
					varValue.put('format', df)

				compVars.put(varName, varValue)
			}
		}

		patterns.each { k, v ->
			def p = compVars.get(k) as Map
			if (p != null) {
				p.pattern = v
			}
		}
		generateMaskPattern()

		if (sysVar) {
			compVars.put("#file_date", [:])
			compVars.put("#file_size", [:])
		}

		varAttr.each { name, values ->
			if (values == null)
				return

			def fn = name.toLowerCase()
			Map<String, Object> attrList = compVars.get(fn)
			if (attrList != null) {
				if (values.calc != null)
					throw new ExceptionGETL("The calculated variable \"$name\" cannot be used in the mask!")
				values.each { attr, value ->
					def a = attr.toLowerCase()
					if (!(a in ['type', 'format', 'len', 'lenmin', 'lenmax']))
						throw new ExceptionGETL("Unknown variable attribute \"${attr}\"!")

					attrList.put(a, value)
				}
			}
		}

		maskFile = maskStr.replace('$', '\\$')
		likeFile = maskStr
		compVars.each { key, value ->
			def vo = "(?i)(\\{${key}\\})"
			maskFile = maskFile.replaceAll(vo, "\\\$\$1")
			likeFile = likeFile.replace("{$key}", "%")
		}
		maskFile = maskFile.replace('\\', '\\\\')
		likeFile = likeFile.replace('*', '%').replace('\\', '\\\\')
				.replace('.', '\\.').replace('$', '\\$')

		maskVariables.each { name, value ->
			if (value == null)
				return

			if (value.calc != null) {
				def cm = compVars.get(name)?:([:] as Map<String, Object>)
				MapUtils.MergeMap(cm, value, false, false)
				compVars.put(name, value)
			}
			else if (!compVars.containsKey(name)) {
				compVars.put(name, value)
			}
		}

		compVars.each { name, value ->
			def type = value.type as Field.Type
			if (type in [Field.dateFieldType, Field.timeFieldType, Field.datetimeFieldType, Field.timestamp_with_timezoneFieldType]) {
				def format = value.format as String
				if (format == null)
					throw new ExceptionGETL("Format is required for variable \"$name\"!")

				DateTimeFormatter df = null
				switch (type) {
					case Field.dateFieldType:
						df = DateUtils.BuildDateFormatter(format)
						break
					case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
						df = DateUtils.BuildDateTimeFormatter(format)
						break
					case Field.timeFieldType:
						df = DateUtils.BuildTimeFormatter(format)
						break
				}

				varDateFormatter.put(name, df)
			}
		}

		vars = (MapUtils.UnmodifiableMap(compVars) as Map<String, Map>)
		mask = maskStr
		isCompile = true
	}

	/** Generation mask path pattern on elements */
	void generateMaskPattern () {
		StringBuilder b = new StringBuilder()
		b.append("(?i)")
		for (Integer i = 0; i < elements.size(); i++) {
			b.append(elements[i].mask)
			if (i < elements.size() - 1) b.append("/")
		}
		maskPath = b.toString()
		maskPathPattern = Pattern.compile(maskPath)
		maskFolder = null
		likeFolder = null

		if (!elements.isEmpty() /*&& elements[elements.size() - 2].vars.size() > 0*/) {
			StringBuilder mp = new StringBuilder()
			StringBuilder lp = new StringBuilder()
			mp.append("(?i)")
			for (Integer i = 0; i < elements.size() - 1; i++) {
				mp.append((elements[i].mask as String) + "/")
				lp.append((elements[i].like as String).replace('.', '\\.') + "/")
			}
			maskFolder = mp.toString() + "(.+)"
			likeFolder = lp.toString()
			if (elements.size() > 1) likeFolder = likeFolder.substring(0, lp.length() - 1)
		}
	}

	/** Generation mask path pattern on elements */
	Pattern generateMaskPattern (Integer countElements) {
		if (countElements > elements.size()) throw new ExceptionGETL("Can not generate pattern on ${countElements} level for ${elements.size()} elements")
		StringBuilder b = new StringBuilder()
		b.append("(?i)")
		for (Integer i = 0; i < countElements; i++) {
			b.append(elements[i].mask)
			if (i < countElements - 1) b.append("/")
		}
		def mp = b.toString()
		Pattern.compile(mp)
	}

	/** Get all files in specific path */
	List<Map> getFiles(String path) {
		File dir = new File(path)
		File[] f = dir.listFiles()

		List<Map> l = []

		if (f == null) return l

		for (Integer i = 0; i < f.length; i++) {
			if (f[i].isDirectory()) {
				List<Map> n = getFiles(path + "/" + f[i].getName())
				for (Integer x = 0; x < n.size(); x++) {
					def m = [:]
					m.fileName = n[x].fileName
					l << m
				}
			}
			else
				if (f[i].isFile()) {
					def m = [:]
					m.fileName = path +"/" + f[i].getName()
					l << m
				}
		}
		l
	}

	/** Get all files with root path */
	List<Map> getFiles() {
		getFiles(rootPath)
	}

	/** Analyze file with mask and return value of variables */
	Map analyzeFile(String fileName, Map<String, Object> extendVars = null) {
		analyze(fileName, false, extendVars)
	}

	/** Analyze dir with mask and return value of variables */
	Map analyzeDir(String dirName, Map<String, Object> extendVars = null) {
		analyze(dirName, true, extendVars)
	}

	/** Checking the value by compiled mask */
	Boolean match(String value) {
		return value.matches(maskPathPattern)
	}

	/** Analyze object name */
	@SuppressWarnings('UnnecessaryQualifiedReference')
	Map analyze(String objName, Boolean isHierarchy = false, Map<String, Object> extendVars = null) {
		if (!isCompile) compile()

		def fn = objName
		if (fn == null) return null

		if (extendVars == null)
			extendVars = [:] as Map<String, Object>
		else
			extendVars = MapUtils.CopyOnly(extendVars, ['filename', 'filedate', 'filesize']) as Map<String, Object>

		if (changeSeparator) fn = fn.replace('\\', '/')

		Integer countDirs
		String pattern

		if (isHierarchy) {
			countDirs = fn.split("/").length
			pattern = generateMaskPattern(countDirs)
		}
		else {
			pattern = maskPathPattern
		}

		if (!fn.matches(pattern)) return null

		def res = ([:] as Map<String, Object>)

		Matcher mat
		mat = fn =~ pattern
		if (mat.groupCount() >= 1 && ((List)mat[0]).size() > 1) {
			def i = 0
            def isError = false
			vars.each { key, value ->
                if (isError) {
					directive = Closure.DONE
					return
				}
				if (value.calc != null) return

                //noinspection GroovyAssignabilityCheck
                def v = (mat[0][i + 1]) as String

				if (v?.length() == 0) v = null

				if (v != null && value.len != null && v.length() != value.len) {
					if (!ignoreConvertError)
						throw new ExceptionGETL("Invalid [${key}]: length value \"${v}\" <> ${value.len}!")
					v = null
				}

                //noinspection GroovyAssignabilityCheck
                if (v != null && value.lenMin != null && v.length() < Integer.valueOf(value.lenMin)) {
					if (!ignoreConvertError)
						throw new ExceptionGETL("Invalid [${key}]: length value \"${v}\" < ${value.lenMin}!")
					v = null
				}
                //noinspection GroovyAssignabilityCheck
                if (v != null && value.lenMax != null && v.length() > Integer.valueOf(value.lenMax)) {
					if (!ignoreConvertError)
						throw new ExceptionGETL("Invalid [${key}]: length value \"${v}\" > ${value.lenMax}!")
					v = null
				}

				if (value.type != null && v != null) {
					def type = value.type
					if (type instanceof String)
						type = Field.Type."$type"
					switch (type) {
						case Field.dateFieldType:
							try {
								v = DateUtils.ParseSQLDate(varDateFormatter.get(key) as DateTimeFormatter, v, ignoreConvertError)
							}
							catch (Exception e) {
								throw new ExceptionGETL("Invalid [${key}]: can not parse value \"${v}\" to date: ${e.message}!")
							}
                            isError = (v == null)
							break
						case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
							try {
								v = DateUtils.ParseSQLTimestamp(varDateFormatter.get(key) as DateTimeFormatter, v, ignoreConvertError)
							}
							catch (Exception e) {
								throw new ExceptionGETL("Invalid [${key}]: can not parse value \"${v}\" to date: ${e.message}!")
							}
							isError = (v == null)
							break
						case Field.timeFieldType:
							try {
								v = DateUtils.ParseSQLTime(varDateFormatter.get(key) as DateTimeFormatter, v, ignoreConvertError)
							}
							catch (Exception e) {
								throw new ExceptionGETL("Invalid [${key}]: can not parse value \"${v}\" to date: ${e.message}!")
							}
							isError = (v == null)
							break
						case Field.integerFieldType:
							try {
								v = Integer.valueOf(v)
							}
							catch (Exception e) {
								if (!ignoreConvertError)
									throw new ExceptionGETL("Invalid [${key}]: can not parse value \"${v}\" to integer: ${e.message}!")
								v = null
							}
                            isError = (v == null)
							break
						case Field.bigintFieldType:
							try {
								v = Long.valueOf(v)
							}
							catch (Exception e) {
								if (!ignoreConvertError)
									throw new ExceptionGETL("Invalid [${key}]: can not parse value \"${v}\" to bigint: ${e.message}!")
								v = null
							}
                            isError = (v == null)
							break
						case Field.stringFieldType:
							break
						default:
							throw new ExceptionGETL("Unknown type ${value.type}!")
					}
				}
                if (isError) return

				res.put(key, v)
				i++
			}
            if (isError) return null

			vars.each { key,value ->
				if (value.calc != null) {
					def r = res + extendVars
					def varRes = (value.calc as Closure).call(r)
					res.put(key, varRes)
				}
			}
		}

		return res
	}

	/** Filter files with mask */
	List<Map> filterFiles(List<Map> files) {
		List<Map> res = []
		files.each { file ->
			def m = analyzeFile(file.filename as String)
			if (m != null) res << file
		}

		return res
	}

	/** Filter files from root path with mask */
	@SuppressWarnings('unused')
	List<Map> filterFiles() {
		filterFiles(getFiles())
	}

	/** Generation file name with variables */
	@CompileStatic
	String generateFileName(Map varValues, Boolean formatValue) {
		if (!isCompile) compile()

		def v = [:] as Map<String, Object>
		if (formatValue) {
			vars.each { key, value ->
				def val = formatVariable(key, varValues.get(key))
				v.put(key, val)
			}
		}
		else {
			vars.each { key, value ->
				v.put(key, varValues.get(key))
			}
		}
		def res = GenerationUtils.EvalText(maskFile, v)

		return res
	}

	/** Generation file name with variables */
	@CompileStatic
	String generateFileName(Map varValues) {
		generateFileName(varValues, true)
	}

	/**
	 * Format variable with type of value
	 */
	String formatVariable(String varName, def value) {
		if (!vars.containsKey(varName))
			throw new ExceptionGETL("Variable ${varName} not found!")

		def v = vars.get(varName) as Map<String, Object>
		def varType = v.type
		Field.Type type
		if (varType == null) {
            type = Field.stringFieldType
        }
        else if (varType instanceof String) {
            type = Field.Type."$varType" as Field.Type
        }
		else
			type = varType as Field.Type

		switch (type) {
			case Field.dateFieldType:
				value = varDateFormatter.get(varName).format((value as Date).toLocalDate())
				break
			case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
				value = varDateFormatter.get(varName).format((value as Date).toLocalDateTime())
				break
			case Field.timeFieldType:
				value = varDateFormatter.get(varName).format((value as Date).toLocalTime())
				break
		}

		return value
	}

	String toString() {
		StringBuilder b = new StringBuilder()
		b.append("""original mask: $maskStr
root path: $rootPath
count level in path: $countLevel
count root levels in path: $numLocalPath
mask path: $maskPath
mask folder: $maskFolder
mask file: $maskFile
like folder: $likeFolder
like file: $likeFile
variables: $vars
elements:
""")
		for (Integer i = 0; i < elements.size(); i++) {
			def pe = elements[i]
			b.append("[${i+1}]:\t")
			b.append(pe.mask)
            List vr = pe.vars as List
			if (vr.size() > 0) b.append(" [")
			for (Integer v = 0; v < vr.size(); v++) {
				b.append(vr.get(v))
				if (v < vr.size() - 1)
					b.append(", ")
			}
			if (vr.size() > 0) b.append("]")
			b.append("\n")
		}

		return b.toString()
	}

	void dslCleanProps() {
		sysParams.dslNameObject = null
		sysParams.dslCreator = null
	}

	/**
	 * Convert a list of masks to a list of paths
	 * @param masks list of masks
	 * @param vars variable masks
	 * @return list of paths
	 */
	@CompileStatic
	static List<Path> Masks2Paths(List<String> masks, Map<String, Map> vars = null) {
		if (masks == null) return null
		def res = [] as List<Path>
		masks.each {mask ->
			if (mask == null)
				throw new ExceptionGETL('It is not allowed to specify null as a list value!')
			res.add(new Path(mask: mask, vars: vars))
		}
		return res
	}

	/**
	 * Matching specified value with list of paths
	 * @param paths list of paths
	 * @param value compare value
	 * @return true if matches are found
	 */
	@CompileStatic
	static Boolean MatchList(String value, List<Path> paths) {
		if (paths == null)
			throw new ExceptionGETL('Required paths parameter!')
		if (value == null) return null
		for (int i = 0; i < paths.size(); i++) {
			def p = paths[i]
			if (p == null)
				throw new ExceptionGETL('It is not allowed to specify null as a list value!')
			if (p.match(value))
				return true
		}
		return false
	}

	/** Clone path */
	Path clonePath() {
		return new Path(mask: this.mask, vars: MapUtils.Clone(this.maskVariables))
	}

	@Override
	Object clone() {
		return clonePath()
	}

	/**
	 * Create a file history table in the database
	 * @param table file history table
	 * @return the table was created in the database
	 */
	@SuppressWarnings('unused')
	Boolean createStoryTable(TableDataset table) {
		return new FileManager().createStoryTable(table, this)
	}
}