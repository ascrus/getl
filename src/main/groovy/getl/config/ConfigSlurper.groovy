package getl.config

import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.lang.opts.BaseSpec
import getl.proc.Job
import getl.utils.*
import groovy.time.Duration
import groovy.transform.InheritConstructors
import groovy.transform.NamedVariant

import java.sql.Time
import java.sql.Timestamp

/**
 * Slurper configuration manager class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ConfigSlurper extends ConfigManager {
	static {
		System.setProperty('line.separator', '\n')
	}

	@Override
	void setEvalVars(Boolean value) { super.setEvalVars(false) }

	@SuppressWarnings("DuplicatedCode")
	@Override
	void init(Map<String, Object> initParams) {
		super.init(initParams)
		evalVars = false

		if (Job.jobArgs.environment != null)
			environment = Job.jobArgs.environment

		if (initParams?.config == null)
			return

		def config = initParams.config as Map<String, Object>
		if (config.path != null) {
			this.path = config.path as String
			if (!(new File(this.path()).exists()))
				throw new ExceptionGETL("Can not find config path \"${this.path()}\"")
			logger.config("config: set path ${this.path()}")
		}
		def configPath = (this.path != null)?"${this.path()}${File.separator}":""
		if (config.filename != null) {
			def fn = config.filename as String
			if (fn.indexOf(";") == -1) {
				this.fileName = fn
				logger.config("config: use file ${this.fileName}")
			}
			else {
				def fs = fn.split(";")
				fs.each {
					if (!(new File(configPath + it).exists())) throw new ExceptionGETL("Can not find config file \"${it}\"")
				}
				this.files = []
				this.files.addAll(fs)
				logger.config("config: use files ${this.files}")
			}
		}
	}

	/** Configuration files code page */
	String getCodePage () { (params.codePage as String)?:'UTF-8' }
	/** Configuration files code page */
	void setCodePage (String value) {
		if (value.trim() == '') throw new ExceptionGETL('Code page value can not have empty value')
		params.codePage = value
	}

	/** Configuration file name */
	String getFileName () { params.fileName as String}
	/** Configuration file name */
	void setFileName (String value) {
		if (value.trim() == '') throw new ExceptionGETL('The file name can not have empty value')
		params.fileName = value?.trim()
	}

	/** List of configuration files */
	List<String> getFiles () { params.files as List<String> }
	/** List of configuration files */
	void setFiles (List<String> value) {
		value.each {
			if (it == null || it.trim() == '') {
				throw new ExceptionGETL('The file name can not have empty value')
			}
		}

		List<String> f = files
		if (f == null) {
			f = new ArrayList<String>()
			params.files = f
		}
		this.files.clear()
		this.files.addAll(value*.trim() as List<String>)
	}

	/** Directory for configuration files */
	String getPath() { params.path as String }
	/** Directory for configuration files */
	void setPath(String value) {
		if (value != null && !(new File(value).exists()))
			throw new ExceptionGETL("Directory \"$value\" not exists!")

		params.path = value
	}
	/** Full path to the directory for the configuration files */
	String path() { FileUtils.TransformFilePath(path) }

	/** Use environment section */
	String getEnvironment() { params.environment as String }
	/** Use environment section */
	void setEnvironment(String value) {
		if (value != null && value.trim().length() == 0)
			throw new ExceptionGETL('The environment can not have empty value')

		params.environment = value
	}

	/**
	 * Evaluate file path for specified configuration file
	 * @param value
	 * @return
	 */
	static String fullConfigName (String pathFile, String value) {
		return FileUtils.TransformFilePath(((pathFile != null)?FileUtils.ConvertToUnixPath(pathFile) + '/':'') + value)
	}

	/** Return file path for current configuration file */
	String getFullName() { fullConfigName(path(), fileName) }

	@Override
	protected void loadContent(Map<String, Object> readParams = [:]) {
		def fp = FileUtils.TransformFilePath(readParams?.path as String)?:this.path()
		def fn = (readParams?.fileName as String)?:this.fileName
		def fl = (readParams?.files as List<String>)?:this.files
		def env = (readParams?.environment as String)?:this.environment?:'prod'
		def cp = (readParams?.codePage as String)?:this.codePage
		def vars = readParams.vars as Map<String, Object>

		if (fn != null) {
			def rp = FileUtils.RelativePathFromFile(fn)
			if (rp == '.') {
				rp = fp
			} else {
				fn = FileUtils.FileName(fn)
			}
			def ff = new File(fullConfigName(rp, fn))
			def data = LoadConfigFile(file: ff, codePage: cp, environment: env, configVars: vars,
					owner: dslCreator)
			mergeConfig(data)
		}
		else if (fl != null) {
			fl.each { String name ->
				def rp = FileUtils.RelativePathFromFile(name)
				if (rp == '.') {
					rp = fp
				}
				else {
					name = FileUtils.FileName(name)
				}
				def ff = new File(fullConfigName(rp, name))
				def data = LoadConfigFile(file: ff, codePage: cp, environment: env,
						configVars: vars, owner: dslCreator)
				mergeConfig(data)
			}
		}
	}

	/**
	 * Load configuration from file
	 * @param file loaded file descriptor
	 * @param codePage encode page
	 * @param environment load specified environment
	 * @param configVars configuration variables
	 * @param owner Getl instance
	 * @return loaded configuration content
	 */
	@NamedVariant
	static Map<String, Object> LoadConfigFile(File file, String codePage = 'utf-8', String environment = null,
											  Map<String, Object> configVars = null, Getl owner = null) {
		if (file == null)
			throw new ExceptionGETL("No file specified!")
		if (!file.exists())
			throw new ExceptionGETL("Configuration file \"$file\" not found!")

		def logger = (owner != null)?owner.logging.manager:Logs.global
		if (codePage == null)
			codePage = 'utf-8'

		String text
		try {
			text = file.getText(codePage)
		}
		catch (Exception e) {
			logger.severe("Error read configuration file \"${file.canonicalPath}\", error: ${e.message}!")
			throw e
		}

		def cfg = (environment == null)?new groovy.util.ConfigSlurper():new groovy.util.ConfigSlurper(environment)
		def args = [vars: ListUtils.NotNullValue([configVars, Config.vars, [:]])]

		Map<String, Object> res
		try {
			cfg.setBinding(args)
			def data = cfg.parse(text) as Map
			res = CheckDataMap(cfg, data, [configvars: (data.configvars as Map)?:[:]] + args) as Map<String, Object>
			if ((res.configvars as Map)?.isEmpty()) {
				res.remove('configvars')
			}
			else {
				def vars = res.configvars as Map
				res.remove('configvars')
				if (res.vars == null) res.vars = [:]
				(res.vars as Map).putAll(vars)
			}
		}
		catch (Exception e) {
			logger.severe("Error parse configuration file \"$file\", error: ${e.message}!")
			throw e
		}

		return res
	}

	/**
	 * Check closure in configuration
	 * @param data
	 * @param vars
	 */
	static private Map CheckDataMap(groovy.util.ConfigSlurper cfg, Map data, Map vars) {
		def res = [:]
		data.each { key, value ->
			if (value instanceof Map) {
				res.put(key, CheckDataMap(cfg, value, vars))
			}
			else if (value instanceof List) {
				res.put(key, CheckDataList(cfg, value, vars))
			}
			else  if (value instanceof Closure) {
				Closure cl = value as Closure
				def val = [:]
				val.putAll(vars)
				def code = cl.rehydrate(val, val, val)
				//noinspection UnnecessaryQualifiedReference
				code.delegate = Closure.DELEGATE_ONLY
				cfg.parse(code)
				val.remove('vars')
				val.remove('configvars')
				res.put(key, val)
			}
			else {
				res.put(key, value)
			}
		}

		return res
	}

	/**
	 * Check closure in configuration
	 * @param data
	 * @param vars
	 */
	static private List CheckDataList(groovy.util.ConfigSlurper cfg, List data, Map vars) {
		def res = []
		def i = 0
		data.each { Object value ->
			if (value instanceof Map) {
				res << CheckDataMap(cfg, value, vars)
			}
			else if (value instanceof List) {
				res << CheckDataList(cfg, value, vars)
			}
			else  if (value instanceof Closure) {
				Closure cl = value as Closure
				def val = [:]
				val.putAll(vars)
				def code = cl.rehydrate(val, val, val)
				//noinspection UnnecessaryQualifiedReference
				code.delegate = Closure.DELEGATE_ONLY
				cfg.parse(code as Script)
				val.remove('vars')
				val.remove('configvars')
				res << val
			}
			else {
				res << value
			}
			i++
		}

		return res
	}

	@Override
	void saveConfig(Map<String, Object> content, Map<String, Object> saveParams = [:]) {
		def fp = FileUtils.TransformFilePath(saveParams?.path as String)?:this.path()
		def fn = (saveParams?.fileName as String)?:this.fileName
		def cp = (saveParams?.codePage as String)?:this.codePage
		def convVars = BoolUtils.IsValue(saveParams?.convertVars)
		def tm = BoolUtils.IsValue(saveParams?.trimMap)
		def sw = BoolUtils.IsValue(saveParams?.smartWrite)

		if (fn == null) throw new ExceptionGETL('Required parameter "fileName"')

		def rp = FileUtils.RelativePathFromFile(fn)
		if (rp == '.') {
			rp = fp
		}
		else {
			fn = FileUtils.FileName(fn)
		}
		def ff = new File(fullConfigName(rp, fn))

		SaveConfigFile(data: content, file: ff, codePage: cp, convertVars: convVars, trimMap: tm, smartWrite:  sw,
				owner: dslCreator)
	}

	/**
	 * Save map data to file
	 * @param data saved configuration
	 * @param file saved file descriptor
	 * @param codePage text encoding
	 * @param convertVars convert $ {variable} to $ {vars.variable}
	 * @param trimMap
	 * @param smartWrite don't overwrite the file if it hasn't changed
	 * @param owner Getl instance
	 */
	@NamedVariant
	static void SaveConfigFile(Map data, File file, String codePage = 'utf-8', Boolean convertVars = false,
							   Boolean trimMap = false, Boolean smartWrite = false, Getl owner = null) {
		def vars = data.vars as Map
		if (vars != null && !vars.isEmpty()) {
			data = [configvars: vars] + MapUtils.Copy(data, ['vars'])
		}

		if (codePage == null)
			codePage = 'utf-8'

		def logger = (owner != null)?owner.logging.manager:Logs.global
		StringBuilder sb = new StringBuilder()
		if (SaveMap(data, sb, convertVars, trimMap) > 0) {
			int oldHash = (smartWrite && file.exists())?file.text.hashCode():0
			try {
				def str = sb.toString()
				if (!smartWrite || oldHash != str.hashCode())
					file.setText(str.toString(), codePage?:'utf-8')
			}
			catch (Exception e) {
				logger.severe("Error save configuration to file \"$file\", error: ${e.message}")
				throw e
			}
		}
	}

	/**
	 * Preparing variable name
	 * @param name variable name
	 * @return
	 */
	static String PrepareVariableName(String name, Boolean isListMap) {
		String res
		if (!isListMap)
			res = (name.matches('(?i)^([a-z]|[0-9]|[_])+$') && (name.matches('(?i)[_]*[a-z]+.*')) && !name.matches('^[0-9]+.*'))?
					name:('this."${\'' + StringUtils.EscapeJava(name) + '\'}"')
		else
			res = '"' + StringUtils.EscapeJava(name) + '"'

		return res
	}

	/**
	 * Write map data
	 * @param data stored map data
	 * @param writer writer object
	 * @param convertVars convert $ {variable} to $ {vars.variable}
	 * @param trimMap
	 * @param tab indent when writing to text
	 * @param isListMap data is in the list
	 * @return count saved items
	 */
	static Integer SaveMap(Map data, StringBuilder writer, Boolean convertVars = false, Boolean trimMap = false, Integer tab = 0, Boolean isListMap = false) {
		def tabStr = (tab > 0)?StringUtils.Replicate('  ', tab):''
		def res = 0
		def lines = [] as List<String>
		data.each { key, value ->
			if (value == null) return
			def varName = PrepareVariableName(key.toString(), isListMap)

			if (value instanceof Map) {
				def map = value as Map
				if (!map.isEmpty()) {
					def sb = new StringBuilder()
					if (SaveMap(map, sb, convertVars, trimMap, tab + 1, isListMap) > 0) {
						def eqStr = (isListMap)?':':''
						def dvStr1 = (isListMap)?'[':'{'
						def dvStr2 = (isListMap)?']':'}'
						lines.add("${tabStr}${varName}${eqStr} ${dvStr1}\n" + sb.toString() + "${tabStr}${dvStr2}")
						res++
					}
				}
			}
			else if (value instanceof List) {
				def list = value as List
				if (!list.isEmpty()) {
					def sb = new StringBuilder()
					if (SaveList(list, sb, convertVars, trimMap, tab + 1) > 0) {
						def eqStr = (isListMap)?':':' ='
						lines.add("${tabStr}${varName}${eqStr} [\n" + sb.toString() + "${tabStr}]")
						res++
					}
				}
			}
			else if (value instanceof BaseSpec) {
				def map = (value as BaseSpec).params
				if (!map.isEmpty()) {
					def sb = new StringBuilder()
					if (SaveMap(map, sb, convertVars, trimMap, tab + 1, isListMap) > 0) {
						def eqStr = (isListMap)?':':''
						def dvStr1 = (isListMap)?'[':'{'
						def dvStr2 = (isListMap)?']':'}'
						lines.add("${tabStr}${varName}${eqStr} ${dvStr1}\n" + sb.toString() + "${tabStr}${dvStr2}")
						res++
					}
				}
			}
			else {
				def sb = new StringBuilder()
				if (SaveObject(key, value, sb, convertVars, tab, isListMap)) {
					lines.add(sb.toString())
					res++
				}
			}
		}

		if (isListMap)
			writer.append(lines.join(',\n'))
		else
			writer.append(lines.join('\n'))

		writer.append('\n')

		return res
	}

	/**
	 * Write list data
	 * @param data stored list data
	 * @param writer writer object
	 * @param convertVars convert $ {variable} to $ {vars.variable}
	 * @param trimMap
	 * @param tab indent when writing to text
	 * @return count saved items
	 */
	static Integer SaveList(List data, StringBuilder writer, Boolean convertVars = false, Boolean trimMap = false, Integer tab = 0) {
		def tabStr = (tab > 0)?StringUtils.Replicate('  ', tab):''
		def i = 0
		def res = 0
		def lines = [] as List<String>
		data.each { value ->
			i++
			if (value instanceof Map) {
				def map = value as Map
				if (!map.isEmpty()) {
					def sb = new StringBuilder()
					if (SaveMap(map, sb, convertVars, trimMap, tab + 1, true) > 0) {
						lines.add("${tabStr}[\n" + sb.toString() + "${tabStr}]")
						res++
					}
				}
			}
			else if (value instanceof List) {
				def list = value as List
				if (!list.isEmpty()) {
					def sb = new StringBuilder()
					if (SaveList(list, sb, convertVars, trimMap, tab + 1) > 0) {
						lines.add("${tabStr}[\n" + sb.toString() + "${tabStr}]")
						res++
					}
				}
			}
			else if (value instanceof BaseSpec) {
				def map = (value as BaseSpec).params
				if (!map.isEmpty()) {
					def sb = new StringBuilder()
					if (SaveMap(map, sb, convertVars, trimMap, tab + 1, true) > 0) {
						lines.add("${tabStr}[\n" + sb.toString() + "${tabStr}]")
						res++
					}
				}
			}
			else {
				def sb = new StringBuilder()
				if (SaveObject(null, value, sb, convertVars, tab)) {
					lines.add(sb.toString())
					res++
				}
			}
		}

		writer.append(lines.join(',\n'))
		writer.append('\n')

		return res
	}

	/**
	 * Write object
	 * @param key object name
	 * @param value object value
	 * @param writer writer object
	 * @param convertVars convert $ {variable} to $ {vars.variable}
	 * @param tab indent when writing to text
	 * @param isListMap object is in the list
	 * @return object saved
	 */
	static Boolean SaveObject(def key, def value, StringBuilder writer, Boolean convertVars = false, Integer tab = 0, Boolean isListMap = false) {
		if (value instanceof Closure) return false

		def tabStr = (tab > 0)?StringUtils.Replicate('  ', tab):''
		def eqStr = (isListMap)?':':' ='
		def keyStr = (key != null)?"${PrepareVariableName(key.toString(), isListMap)}$eqStr ":''
		if (value instanceof Timestamp) {
			def str = "getl.utils.DateUtils.ParseSQLTimestamp(getl.utils.DateUtils.defaultDateTimeMask, '${DateUtils.FormatDate(DateUtils.defaultDateTimeMask, value as Timestamp)}', false)"
			writer.append("${tabStr}${keyStr}$str")
		}
		else if (value instanceof java.sql.Date) {
			def str = "getl.utils.DateUtils.ParseSQLDate(getl.utils.DateUtils.defaultDateMask, '${DateUtils.FormatDate(DateUtils.defaultDateMask, value as java.sql.Date)}', false)"
			writer.append("${tabStr}${keyStr}$str")
		}
		else if (value instanceof Time) {
			def str = "getl.utils.DateUtils.ParseSQLTime(getl.utils.DateUtils.defaultTimeMask, '${DateUtils.FormatDate(DateUtils.defaultTimeMask, value as Time)}', false)"
			writer.append("${tabStr}${keyStr}$str")
		}
		else if (value instanceof Date) {
			def str = "getl.utils.DateUtils.ParseDate(getl.utils.DateUtils.defaultDateTimeMask, '${DateUtils.FormatDate(DateUtils.defaultDateTimeMask, value as Date)}', false)"
			writer.append("${tabStr}${keyStr}$str")
		}
		else if (value instanceof Duration) {
			def dur = value as Duration
			def str = "new groovy.time.Duration(${dur.days}, ${dur.hours}, ${dur.minutes}, ${dur.seconds}, ${dur.millis})"
			writer.append("${tabStr}${keyStr}$str")
		}
		else if (value instanceof Enum) {
			def e = value as Enum
			def str = e.getClass().name.replace('$', '.') + '.' + e.name()
			writer.append("${tabStr}${keyStr}$str")
		}
		else if (value instanceof String || value instanceof GString || value instanceof Enum) {
			def str = value.toString()
			def quote = (str.indexOf('${') == -1)?'\'':'"'
			if (convertVars) {
				value = value.toString().replace('${', '${vars.')
			}
			writer.append("${tabStr}${keyStr}${quote}${StringUtils.EscapeJavaWithoutUTF(value.toString())}${quote}")
		}
		else if (value instanceof Number || value instanceof Boolean) {
			writer.append("${tabStr}${keyStr}${value.toString()}")
		}
		else
			return false

		return true
	}

	static void main(def args) {
		def a = MapUtils.ProcessArguments(args) as Map<String, Object>
		def l = a.keySet().toList()
		if (!('source' in l) || !('dest' in l) || ('help' in l) || ('/help' in l) || ('/?' in l) || ('?' in l) ||
				(l - ['source', 'dest', 'codepage', 'convert_vars', 'rules']).size() > 0) {
			println '''### Convert configuration files tool, EasyData company (www.easydata.ru)
Syntax:
  getl.config.ConfigSlurper source=<json file> dest=<groovy config file> [options]
  
Options:
codepage=<code page text>
convert_vars=<true|false>
rules=<rules config file>

Rule file structure:
rules=[
	'<mask>': '<new item name>',
	...
]

Example:
  java -cp "libs/*" getl.config.ConfigSlurper source=demo.json dest=demo.conf codepage=UTF-8 rule=convert_rules.conf
'''
			return
		}

		String sourceName = a.source
		String destName = a.dest
		String codePage = a.codepage?:'UTF-8'
		Boolean convertVars = BoolUtils.IsValue(a.convert_vars, false)
		String ruleFileName = a.rules

		assert new File(a.source as String).exists(), "Source file \"$sourceName\" not found!"
		assert a.source != a.dest, 'Source and destination file must have different names!'

		Map<String, String> rules
		if (ruleFileName != null) {
			def ruleFile = new File(ruleFileName)
			assert ruleFile.exists(), "Rule config file \"$ruleFileName\" not found!"
			rules = LoadConfigFile(file: ruleFile, codePage: codePage) as Map<String, String>
			assert (rules.rules as Map)?.size() > 0, "Rules not found in config file \"$ruleFileName\"!"
		}

		def content
		def sourceFile = new File(sourceName)
		try {
			content = ConfigFiles.LoadConfigFile(file: sourceFile, codePage: codePage)
		}
		catch (Exception e) {
			Logs.Severe("Invalid parsing file \"${sourceFile.canonicalPath}\", error: ${e.message}")
			throw e
		}
		if (content.isEmpty()) {
			println "Configuration JSON file is empty!"
			return
		}

		if (rules != null) {
			rules.rules.each { String source, dest ->
				MapUtils.FindKeys(content, source) { Map map, String key, item ->
					map.put(dest, map.remove(key))
				}
			}
		}

		SaveConfigFile(data: content, file: new File(destName), codePage: codePage, convertVars: convertVars)
		println "Convert \"$sourceName\" to \"$destName\" complete."
	}
}