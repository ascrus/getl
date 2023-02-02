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
class Path implements GetlRepository {
	Path() {
		registerMethod()
	}

	/**
	 * Create new path for parameters<br>
	 * <ul>
	 * <li>String mask: mask of filename
	 * <li>boolean sysVar: include system variables #file_date and #file_size
	 * <li>Map<String, Map> vars: attributes of variable
	 * </ul>
	 * <i>Variable elements:</i>
	 * <ul>
	 * <li>type:type of the variable in the mask (STRING, INTEGER, BIGINT, DATE, DATETIME)</li>
	 * <li>format: variable parsing format</li>
	 * <li>len: constant variable length</li>
	 * <li>lenMin: minimum variable length</li>
	 * <li>lenMax: maximum variable length</li>
	 * <li>regular: regular expression</li>
	 * <li>calc: computed variable code
	 * </ul>
	 * @param params
	 */
	Path(Map params) {
		if (params == null)
			throw new ExceptionGETL('Parameter "params" required for filepath mask!')
		registerMethod()
		if (params?.vars != null) {
			setMaskVariables(params.vars as Map)
		}
		compile(MapUtils.CleanMap(params, ['vars']))
	}

	static public final VarNamePattern = Pattern.compile('(?i)^[a-z0-9_-]+$')

	/**
	 * Create new path for description<br><br>
	 * The parameter description specifies the mask and its list of variables.<br><br>
	 * <i>Syntax:</i><br>
	 * mask?var_name|type|format|length|min_length|max_length;var_name2|...</b><br><br>
	 * Description items:<br>
	 * <ul>
	 * <li>mask: mask of filename</li>
	 * <li>var_name: variable name in mask</li>
	 * <li>type: type of the variable in the mask (STRING, INTEGER, BIGINT, DATE, DATETIME)</li>
	 * <li>format: variable parsing format</li>
	 * <li>length: constant variable length</li>
	 * <li>min_length: minimum variable length</li>
	 * <li>max_length: maximum variable length</li>
	 * </ul>
	 * @param description - properties of the path being created
	 */
	Path(String description) {
		if (description == null || description.length() == 0)
			throw new ExceptionGETL('Parameter "params" required for filepath mask!')
		registerMethod()
		def params = new HashMap<String, Object>()
		def i = description.indexOf('?')
		if (i == -1)
			params.mask = description
		else {
			params.mask = description.substring(0, i)

			def vars = new LinkedHashMap<String, Map>()
			def list = description.substring(i + 1).replace('\\|', '\u0001').replace('\\;', '\u0002')
					.split(';')

			def varNum = 0
			list.each {str ->
				varNum++

				def opts = str.split('[|]').collect {
					return it.replace('\u0001', '|').replace('\u0002', ';')
				}
				if (opts.size() > 6)
					throw new ExceptionGETL("Incorrect number of parameters from [$varNum] variable in filepath mask [${params.mask}]!")

				def varName = opts[0].trim()
				if (varName.length() == 0)
					throw new ExceptionGETL("Required name for [$varNum] variable in filepath mask [${params.mask}]!")
				if (!varName.matches(VarNamePattern))
					throw new ExceptionGETL("Incorrent variable name [$varNum] in filepath mask [${params.mask}]!")

				def varType = (opts.size() > 1 && opts[1].trim().length() > 0)?opts[1].trim().toUpperCase():null

				def varFormat = (opts.size() > 2 && opts[2].trim().length() > 0)?opts[2].trim():null

				def varLen = (opts.size() > 3 && opts[3].trim().length() > 0)?opts[3].trim():null
				if (varLen != null && !varLen.integer)
					throw new ExceptionGETL("Incorrect value \"$varLen\" of parameter \"length\" for variable [$varName] in filepath mask [${params.mask}]!")

				def varMinLen = (opts.size() > 4 && opts[4].trim().length() > 0)?opts[4].trim():null
				if (varMinLen != null && !varMinLen.integer)
					throw new ExceptionGETL("Incorrect value \"$varMinLen\" of parameter \"min_length\" for variable [$varName] in filepath mask [${params.mask}]!")

				def varMaxLen = (opts.size() > 5 && opts[5].trim().length() > 0)?opts[5].trim():null
				if (varMaxLen != null && !varMaxLen.integer)
					throw new ExceptionGETL("Incorrect value \"$varMaxLen\" of parameter \"max_length\" for variable [$varName] in filepath mask [${params.mask}]!")

				def varParams = new LinkedHashMap<String, Object>()
				if (varType != null)
					varParams.put('type', varType)
				if (varFormat != null) {
					if (varFormat.matches('[/].+[/]'))
						varParams.put('regular', varFormat.substring(1, varFormat.length() - 1).replace('\\\\', '\\'))
					else
						varParams.put('format', varFormat.replace('\\\\', '\\'))
				}
				if (varLen != null)
					varParams.put('len', varLen.toInteger())
				if (varMinLen != null)
					varParams.put('lenMin', varMinLen.toInteger())
				if (varMaxLen != null)
					varParams.put('lenMax', varMaxLen.toInteger())

				vars.put(varName, varParams)
			}
			if (!vars.isEmpty())
				setMaskVariables(vars)
		}

		compile(params)
	}

	/** Logger */
	@JsonIgnore
	Logs getLogger() { (dslCreator != null)?dslCreator.logging.manager:Logs.global }

	/** Parameters validator */
	private ParamMethodValidator methodParams = new ParamMethodValidator()

	private void registerMethod() {
		methodParams.register('compile', ['mask', 'sysVar', 'vars'])
	}

	/**
	 * Change from process path Windows file separator to Unix separator
	 */
	@JsonIgnore
	public Boolean changeSeparator = (File.separatorChar != '/'.charAt(0))

	/**
	 * Ignoring converting error and set null value
	 */
	@JsonIgnore
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
	@JsonIgnore
	String getMaskStr() { maskStr }

	/** Path elements */
	private List<Map> elements = []
	/** Path elements */
	@JsonIgnore
	List<Map> getElements() { elements }

	/** SQL like elements */
	private List<Map> likeElements = []
	/** SQL like elements */
	@SuppressWarnings('unused')
	@JsonIgnore
	List<Map> getLikeElements() { likeElements }

	/** Count level for mask */
	@JsonIgnore
	Integer getCountLevel() { elements.size() }

	/** Root path with mask */
	private String rootPath
	/** Root path with mask */
	@JsonIgnore
	String getRootPath() { this.rootPath }

	/** Count elements in root path */
	private Integer numLocalPath
	/** Count elements in root path */
	@JsonIgnore
	Integer getNumLocalPath() { this.numLocalPath }

	/** Expression file path with mask */
	private String maskPath
	/** Expression file path with mask */
	@JsonIgnore
	String getMaskPath() { this.maskPath }

	/** Expression folder path with mask */
	private String maskFolder
	/** Expression folder path with mask */
	@JsonIgnore
	String getMaskFolder() { this.maskFolder }

	/** Expression mask file */
	private String maskFile
	/** Expression mask file */
	@JsonIgnore
	String getMaskFile() { this.maskFile }

	/** Expression folder path with mask for SQL like */
	private String likeFolder
	/** Expression folder path with mask for SQL like */
	@JsonIgnore
	String getLikeFolder() { this.likeFolder }

	/** Expression mask file for SQL like */
	private String likeFile
	/** Expression mask file for SQL like */
	@JsonIgnore
	String getLikeFile() { this.likeFile }

	/** Compiled variables */
	private Map<String, Map> vars = (MapUtils.UnmodifiableMap(new LinkedHashMap<String, Map>()) as Map<String, Map>)

	/**
	 * Compiled mask variables<br><br>
	 * Variable fields:
	 * <ul>
	 * <li>Field.Type type: variable type</li>
	 * <li>String format: parsing format</li>
	 * <li>int len: length value of variable</li>
	 * <li>int lenMin: minimum length of variable</li>
	 * <li>int lenMax: maximum length of variable</li>
	 * <li>String regular: regular expression (specified instead of format)</li>
	 * <li>Closure calc: calculated variable code</li>
	 * </ul>
	 */
	@JsonIgnore
	Map<String, Map> getVars() { this.vars }

	/**
	 * Variable parameters for compiling mask
	 * Variable fields:
	 * <ul>
	 * <li>Field.Type type: variable type</li>
	 * <li>String format: parsing format</li>
	 * <li>int len: length value of variable</li>
	 * <li>int lenMin: minimum length of variable</li>
	 * <li>int lenMax: maximum length of variable</li>
	 * <li>String regular: regular expression (specified instead of format)</li>
	 * <li>Closure calc: calculated variable code</li>
	 * </ul>
	 */
	private final Map<String, Map<String, Object>> maskVariables = new LinkedHashMap<String, Map<String, Object>>()

	/**
	 * Variable parameters for compiling mask
	 * Variable fields:
	 * <ul>
	 * <li>Field.Type type: variable type</li>
	 * <li>String format: parsing format</li>
	 * <li>int len: length value of variable</li>
	 * <li>int lenMin: minimum length of variable</li>
	 * <li>int lenMax: maximum length of variable</li>
	 * <li>String regular: regular expression (specified instead of format)</li>
	 * <li>Closure calc: calculated variable code</li>
	 * </ul>
	 */
	Map<String, Map<String, Object>> getMaskVariables() { maskVariables }

	/**
	 * Variable parameters for compiling mask
	 * Variable fields:
	 * <ul>
	 * <li>Field.Type type: variable type</li>
	 * <li>String format: parsing format</li>
	 * <li>int len: length value of variable</li>
	 * <li>int lenMin: minimum length of variable</li>
	 * <li>int lenMax: maximum length of variable</li>
	 * <li>String regular: regular expression (specified instead of format)</li>
	 * <li>Closure calc: calculated variable code</li>
	 * </ul>
	 */
	void setMaskVariables(Map<String, Map<String, Object>> value) {
		maskVariables.clear()
		if (value != null)
			maskVariables.putAll(value)
	}

	/** Date formatter for variables */
	private final Map<String, DateTimeFormatter> varDateFormatter = new HashMap<String, DateTimeFormatter>()

	/** System parameters */
	private final Map<String, Object> sysParams = new HashMap<String, Object>()
	/** System parameters */
	@JsonIgnore
	Map<String, Object> getSysParams() { sysParams }

	@JsonIgnore
	@Override
	String getDslNameObject() { sysParams.dslNameObject as String }
	@Override
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	@JsonIgnore
	@Override
	Getl getDslCreator() { sysParams.dslCreator as Getl }
	@Override
	void setDslCreator(Getl value) { sysParams.dslCreator = value }

	@JsonIgnore
	@Override
	Date getDslRegistrationTime() { sysParams.dslRegistrationTime as Date }
	@Override
	void setDslRegistrationTime(Date value) { sysParams.dslRegistrationTime = value }

	@Override
	void dslCleanProps() {
		sysParams.dslNameObject = null
		sysParams.dslCreator = null
		sysParams.dslRegistrationTime = null
	}

	/** Define variable options */
	PathVarsSpec variable(String name,
				 @DelegatesTo(PathVarsSpec) @ClosureParams(value = SimpleType, options = ['getl.utils.opts.PathsVarSpec'])
					Closure cl = null) {
		if (name == null || name == '')
			throw new ExceptionGETL("Name required for variable for filepath mask [$maskStr}]!")

		def var = maskVariables.get(name) as Map<String, Object>
		if (var == null) {
			var = new LinkedHashMap<String, Object>()
			maskVariables.put(name, (var as Map<String, Object>))
			isCompile = false
		}

		def parent = new PathVarsSpec(dslCreator, true, var)
		parent.runClosure(cl)
		if (cl != null)
			isCompile = false

		return parent
	}

	/** Mask path pattern */
	private Pattern maskPathPattern
	/** Mask path pattern */
	@JsonIgnore
	Pattern getMaskPathPattern() { maskPathPattern }
	/** Path already compiled */
	private Boolean isCompile = false
	/** Path already compiled */
	@JsonIgnore
	Boolean getIsCompile() { isCompile }

	/**
	 * Compile path mask.<br><br>
	 * <i>Path parameters:</i><br>
	 * <ul>
	 * <li>String mask: mask of filename</li>
	 * <li>boolean sysVar: include system variables #file_date and #file_size</li>
	 * <li>Map<String, Map> vars: attributes of variable</li>
	 * </ul>
	 * <i>Variable elements:</i>
	 * <ul>
	 * <li>type:type of the variable in the mask (STRING, INTEGER, BIGINT, DATE, DATETIME)</li>
	 * <li>format: variable parsing format</li>
	 * <li>len: constant variable length</li>
	 * <li>lenMin: minimum variable length</li>
	 * <li>lenMax: maximum variable length</li>
	 * <li>regular: regular expression</li>
	 * <li>calc: computed variable code
	 * </ul>
	 * @param params path parameters
	 */
	@SuppressWarnings('GroovyFallthrough')
	void compile(Map params = new HashMap()) {
		if (params == null)
			params = new HashMap()
		methodParams.validation("compile", params)
		maskStr = (params.mask as String)?:mask
		if (maskStr == null)
			throw new ExceptionGETL("Required parameter \"mask\" in filepath mask!")
		def sysVar = BoolUtils.IsValue(params.sysVar)
		def varAttr = new LinkedHashMap<String, Map<String, Object>>()
		varAttr.putAll(maskVariables)
		if (params.vars != null)
			MapUtils.MergeMap(varAttr, params.vars as Map<String, Map<String, Object>>)
		varAttr.each { name, varParams ->
			if (varParams == null) {
				varParams = new LinkedHashMap<String, Object>()
				varAttr.put(name, varParams)
			}

			varParams.keySet().toList().each { opt ->
				if (!(opt in ['type', 'format', 'len', 'lenMin', 'lenMax', 'calc', 'regular']))
					throw new ExceptionGETL("Unknown attribute \"$opt\" for [$name] variable in filepath mask [$maskStr}]!")
			}

			if (varParams.containsKey('type')) {
				def type = varParams.get('type')
				if (type instanceof String || type instanceof GString)
					varParams.put('type', Field.Type.valueOf((type as Object).toString()))
			}

			if (varParams.len != null && (!(varParams.len.toString()).integer))
				throw new ExceptionGETL("Invalid value \"${varParams.len}\" for \"len\" attribute for [$name] variable in filepath mask [$maskStr}]!")

			if (varParams.lenMin != null && (!(varParams.lenMin.toString()).integer))
				throw new ExceptionGETL("Invalid value \"${varParams.lenMin}\" for \"lenMin\" attribute for [$name] variable in filepath mask [$maskStr}]!")

			if (varParams.lenMax != null && (!(varParams.lenMax.toString()).integer))
				throw new ExceptionGETL("Invalid value \"${varParams.lenMax}\" for \"lenMax\" attribute for [$name] variable in filepath mask [$maskStr}]!")
		}

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

		def compVars = (new LinkedHashMap<String, Map<String, Object>>())
		def rMask = FileUtils.FileMaskToMathExpression(maskStr)

		String[] d = rMask.split("/")
		StringBuilder rb = new StringBuilder()

		def listFoundVars = [] as List<String>
		for (Integer i = 0; i < d.length; i++) {
			Map p = new LinkedHashMap()
			p.vars = []
			StringBuilder b = new StringBuilder()

			def f = 0
			def s = d[i].indexOf('{')
			while (s >= 0) {
				def e = d[i].indexOf('}', s)
				if (e >= 0) {
					String df

					b.append(d[i].substring(f, s))

					String vn = d[i].substring(s + 1, e).toLowerCase()
					def vt = varAttr.get(vn)?:(new LinkedHashMap<String, Object>())

					if (vt.type == null)
						vt.type = Field.stringFieldType
					compVars.put(vn, vt)

					def type = vt.type as Field.Type

					if (vt.regular != null)
						b.append("(${vt.regular})")
					else if (type in [Field.dateFieldType, Field.timeFieldType, Field.datetimeFieldType, Field.timestamp_with_timezoneFieldType]) {
						if (vt.format != null) {
							df = vt.format.toString()
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

							vt.format = df
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
						if (vt.len != null)
							b.append("(\\d{${vt.len}})")
						else if (vt.lenMin != null && vt.lenMax != null)
							b.append("(\\d{${vt.lenMin},${vt.lenMax}})")
						else
							b.append("(\\d+)")
					}
					else {
						if (vt.format != null) {
							df = vt.format.toString()
							b.append("($df)")
						}
						else {
							if (vt.len != null)
								b.append("(.+{${vt.len}})")
							else if (vt.lenMin != null && vt.lenMax != null)
								b.append("(.+{${vt.lenMin},${vt.lenMax}})")
							else
								b.append("(.+)")
						}
					}

					if (vn in listFoundVars)
						throw new ExceptionGETL("Duplicate variable [$vn] in filepath mask [$maskStr]")

					(p.vars as List).add(vn)
					listFoundVars << vn

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
		generateMaskPattern()

		if (sysVar) {
			compVars.put("#file_date", new LinkedHashMap())
			compVars.put("#file_size", new LinkedHashMap())
		}

		varAttr.findAll { name, value -> value.calc != null }.each { name, values ->
			if (name.toLowerCase() in listFoundVars)
				throw new ExceptionGETL("The calculated variable [$name] cannot be used in filepath mask [$maskStr]!")
			compVars.put(name, values)
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

		compVars.each { name, value ->
			def type = value.type as Field.Type
			if (type in [Field.dateFieldType, Field.timeFieldType, Field.datetimeFieldType, Field.timestamp_with_timezoneFieldType]) {
				def format = value.format as String
				if (format == null)
					throw new ExceptionGETL("Format is required for variable [$name] in filepath mask [$maskStr}]!")

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

		if (!elements.isEmpty()) {
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
		if (countElements > elements.size())
			throw new ExceptionGETL("Can not generate pattern on ${countElements} level for ${elements.size()} elements in filepath mask [$maskStr}]")
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
					def m = new HashMap()
					m.fileName = n[x].fileName
					l << m
				}
			}
			else
				if (f[i].isFile()) {
					def m = new HashMap()
					m.fileName = path +"/" + f[i].getName()
					l << m
				}
		}
		l
	}

	/** Get all files with root path */
	@JsonIgnore
	List<Map> getFiles() { getFiles(rootPath) }

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
	@SuppressWarnings(['UnnecessaryQualifiedReference', 'GroovyFallthrough'])
	Map analyze(String objName, Boolean isHierarchy = false, Map<String, Object> extendVars = null) {
		if (!isCompile) compile()

		def fn = objName
		if (fn == null) return null

		if (extendVars == null)
			extendVars = new LinkedHashMap<String, Object>()
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

		def res = (new LinkedHashMap<String, Object>())

		Matcher mat
		mat = fn =~ pattern
		if (mat.groupCount() >= 1 && ((List)mat[0]).size() > 1) {
			def i = 0
            def isError = false
			for (varE in vars) {
				def key = varE.key
				def value = varE.value
                if (isError)
					break
				if (value.calc != null)
					continue

                //noinspection GroovyAssignabilityCheck
                def v = (mat[0][i + 1]) as String

				if (v?.length() == 0) v = null

				if (v != null && value.len != null && v.length() != value.len) {
					if (!ignoreConvertError)
						throw new ExceptionGETL("Invalid [${key}] in filepath mask [$maskStr]: length value \"${v}\" <> ${value.len}!")
					v = null
				}

                //noinspection GroovyAssignabilityCheck
                if (v != null && value.lenMin != null && v.length() < Integer.valueOf(value.lenMin)) {
					if (!ignoreConvertError)
						throw new ExceptionGETL("Invalid [${key}] in filepath mask [$maskStr]: length value \"${v}\" < ${value.lenMin}!")
					v = null
				}
                //noinspection GroovyAssignabilityCheck
                if (v != null && value.lenMax != null && v.length() > Integer.valueOf(value.lenMax)) {
					if (!ignoreConvertError)
						throw new ExceptionGETL("Invalid [${key}] in filepath mask [$maskStr]: length value \"${v}\" > ${value.lenMax}!")
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
								logger.severe("Invalid [${key}] in filepath mask [$maskStr]: can not parse value \"${v}\" to date", e)
								throw e
							}
                            isError = (v == null)
							break
						case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
							try {
								v = DateUtils.ParseSQLTimestamp(varDateFormatter.get(key) as DateTimeFormatter, v, ignoreConvertError)
							}
							catch (Exception e) {
								logger.severe("Invalid [${key}] in filepath mask [$maskStr]: can not parse value \"${v}\" to date", e)
								throw e
							}
							isError = (v == null)
							break
						case Field.timeFieldType:
							try {
								v = DateUtils.ParseSQLTime(varDateFormatter.get(key) as DateTimeFormatter, v, ignoreConvertError)
							}
							catch (Exception e) {
								logger.severe("Invalid [${key}] in filepath mask [$maskStr]: can not parse value \"${v}\" to date", e)
								throw e
							}
							isError = (v == null)
							break
						case Field.integerFieldType:
							try {
								v = Integer.valueOf(v)
							}
							catch (Exception e) {
								if (!ignoreConvertError) {
									logger.severe("Invalid [${key}] in filepath mask [$maskStr]: can not parse value \"${v}\" to integer", e)
									throw e
								}
								v = null
							}
                            isError = (v == null)
							break
						case Field.bigintFieldType:
							try {
								v = Long.valueOf(v)
							}
							catch (Exception e) {
								if (!ignoreConvertError) {
									logger.severe("Invalid [${key}] in filepath mask [$maskStr]: can not parse value \"${v}\" to bigint", e)
									throw e
								}
								v = null
							}
                            isError = (v == null)
							break
						case Field.stringFieldType:
							break
						default:
							throw new ExceptionGETL("Unknown type ${value.type} in filepath mask [$maskStr]!")
					}
				}
                if (isError)
					continue

				res.put(key, v)
				i++
			}
            if (isError)
				return null

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

		def v = new HashMap<String, Object>()
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
	@SuppressWarnings('GroovyFallthrough')
	String formatVariable(String varName, def value) {
		if (!vars.containsKey(varName))
			throw new ExceptionGETL("Variable ${varName} not found in filepath mask [$maskStr}]!")

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
		if (mask == null)
			return ''

		if (!isCompile)
			compile()

		def sb = new StringBuilder()
		sb.append(mask)
		if (!vars.isEmpty()) {
			def varDesc = [] as List<String>
			vars.each { name, val ->
				if (val == null && val.isEmpty())
					return

				def elements = [] as List<String>
				def addElements = { v ->
					if (v != null)
						elements.add(v.toString())
					else
						elements.add('')
				}

				addElements((val.type != null && val.type != Field.stringFieldType)?(val.type as Field.Type).toString().toLowerCase():null)
				String varFormat
				if (val.regular != null)
					varFormat = '/' + (val.regular as String).replace('\\', '\\\\') + '/'
				else
					varFormat = (val.format as String)?.replace('\\', '\\\\')
				addElements(varFormat?.replace('|', '\\|')?.replace(';', '\\;'))
				addElements(val.len)
				addElements(val.lenMin)
				addElements(val.lenMax)

				for (int i = elements.size() - 1; i >= 0; i--) {
					if (elements[i].length() == 0)
						elements.remove(i)
					else
						break
				}

				if (!elements.isEmpty())
					varDesc.add(name + '|' + elements.join('|'))
			}
			if (!varDesc.isEmpty()) {
				sb.append('?')
				sb.append(varDesc.join(';'))
			}
		}

		return sb.toString()
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
				throw new ExceptionGETL('It is not allowed to specify null as a list value for function Masks2Path!')
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
			throw new ExceptionGETL('Required paths parameter for function MatchList!')
		if (value == null) return null
		for (int i = 0; i < paths.size(); i++) {
			def p = paths[i]
			if (p == null)
				throw new ExceptionGETL('It is not allowed to specify null as a list value for function MatchList!')
			if (p.match(value))
				return true
		}
		return false
	}

	/** Clone path */
	Path clonePath() {
		def res = new Path()
		res.dslCreator = this.dslCreator
		res.mask = this.mask
		res.maskVariables = MapUtils.Clone(this.maskVariables) as Map<String, Map<String, Object>>
		res.compile()
		return res
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
	Boolean createStoryTable(TableDataset table, Boolean useDateSizeInBuildList = false) {
		FileManager.createStoryTable(table, this, useDateSizeInBuildList)
	}

	/**
	 * Prepare structure of story table
	 * @param storyTable table for preparing
	 */
	@SuppressWarnings('unused')
	void prepareStoryTable(TableDataset table, Boolean useDateSizeInBuildList = false) {
		FileManager.PrepareStoryTable(table, this, useDateSizeInBuildList)
	}
}