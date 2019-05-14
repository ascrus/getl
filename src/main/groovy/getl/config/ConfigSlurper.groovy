/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) 2013-2019  Alexsey Konstantonov (ASCRUS)

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

package getl.config

import getl.exception.ExceptionGETL
import getl.utils.*
import groovy.transform.CompileStatic

/**
 * Slurper configuration manager class
 * @author Alexsey Konstantinov
 *
 */
class ConfigSlurper extends ConfigManager {
	@Override
	void init(Map<String, Object> initParams) {
		if (initParams?.config == null) return
		def config = initParams.config as Map<String, Object>
		if (config.path != null) {
			this.path = config.path
			if (!(new File(this.path).exists())) throw new ExceptionGETL("Can not find config path \"${this.path}\"")
			Logs.Config("config: set path ${this.path}")
		}
		def configPath = (this.path != null)?"${this.path}${File.separator}":""
		if (config.filename != null) {
			def fn = config.filename as String
			if (fn.indexOf(";") == -1) {
				this.fileName = fn
				Logs.Config("config: use file ${this.fileName}")
			}
			else {
				def fs = fn.split(";")
				fs.each {
					if (!(new File(configPath + it).exists())) throw new ExceptionGETL("Can not find config file \"${it}\"")
				}
				this.files = []
				this.files.addAll(fs)
				Logs.Config("config: use files ${this.files}")
			}
		}
	}

	/**
	 * Configuration files code page
	 */
	String getCodePage () { (params.codePage as String)?:'UTF-8' }
	void setCodePage (String value) {
		if (value.trim() == '') throw new ExceptionGETL('Code page value can not have empty value')
		params.codePage = value
	}

	/**
	 * Configuration file name
	 */
	String getFileName () { params.fileName as String}
	void setFileName (String value) {
		if (value.trim() == '') throw new ExceptionGETL('The file name can not have empty value')
		params.fileName = value?.trim()
	}

	/**
	 * List of configuration files
	 */
	public List<String> getFiles () { params.files as List<String> }
	public void setFiles (List<String> value) {
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
		this.files.addAll(value*.trim())
	}

	/**
	 * Configuration directory
	 */
	String getPath() { params.path as String }
	void setPath(String value) {
		if (value != null && !new File(value).exists())
			throw new ExceptionGETL("Directory \"$value\" not exists!")

		params.path = value
	}

	/**
	 * Use environment section
	 */
	String getEnvironment() { params.environment as String }
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
		((pathFile != null)?FileUtils.ConvertToUnixPath(pathFile) + '/':'') + value
	}

	/**
	 * Return file path for current configuration file
	 * @return
	 */
	String getFullName () { fullConfigName(path, fileName) }

	@Override
	void loadConfig(Map<String, Object> readParams = [:]) {
		def fp = (readParams?.path as String)?:this.path
		def fn = (readParams?.fileName as String)?:this.fileName
		def fl = (readParams?.files as List<String>)?:this.files
		def env = (readParams?.environment as String)?:this.environment
		def cp = (readParams?.codePage as String)?:this.codePage

		if (fn != null) {
			def rp = FileUtils.RelativePathFromFile(fn)
			if (rp == '.') {
				rp = fp
			} else {
				fn = FileUtils.FileName(fn)
			}
			def ff = new File(fullConfigName(rp, fn))
			def data = LoadConfigFile(ff, cp, env)
			Config.MergeConfig(data)
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
				def data = LoadConfigFile(ff, cp, env)
				Config.MergeConfig(data)
			}
		}
	}

	/**
	 * Load configuration from file
	 * @param file
	 * @param codePage
	 */
	static Map<String, Object> LoadConfigFile (File file, String codePage = 'UTF-8', String environment = null) {
		if (!file.exists()) throw new ExceptionGETL("Configuration file \"$file\" not found!")

		String text
		try {
			text = file.getText(codePage)
		}
		catch (Exception e) {
			Logs.Severe("Error read configuration file \"$file\", error: ${e.message}!")
			throw e
		}

		def cfg = (environment == null)?new groovy.util.ConfigSlurper():new groovy.util.ConfigSlurper(environment)
		Map<String, Object> vars = [vars: Config.vars?:[:]]

		def writer = new StringWriter()
		SaveMap(vars, writer)
		text = writer.toString() + '\n\n' + text

		Map<String, Object> res
		try {
			def data = cfg.parse(text)
			CheckDataMap(data, [vars: data.vars?:[:]])
			res = MapUtils.DeepCopy(data)
			if ((res.vars as Map)?.isEmpty()) {
				res.remove('vars')
			}
		}
		catch (Exception e) {
			Logs.Severe("Error parse configuration file \"$file\", error: ${e.message}!")
			throw e
		}

		return res
	}

	/**
	 * Check closure in configuration
	 * @param data
	 * @param vars
	 */
	private static void CheckDataMap(Map<String, Object> data, Map<String, Object> vars) {
		def cfg = new groovy.util.ConfigSlurper()

		data.each { String key, Object value ->
			if (value instanceof Map) {
				CheckDataMap(value, vars)
			}
			else if (value instanceof List) {
				CheckDataList(value, vars)
			}
			else  if (value instanceof Closure) {
				Map<String, Object> res = MapUtils.Copy(vars)
				Closure cl = value
				def code = cl.rehydrate(res, cfg, cl.thisObject)
				code.resolveStrategy = Closure.DELEGATE_FIRST

				cfg.parse(code)
				res.remove('vars')
				data.put(key, res)
			}
		}
	}

	/**
	 * Check closure in configuration
	 * @param data
	 * @param vars
	 */
	private static void CheckDataList(List data, Map<String, Object> vars) {
		groovy.util.ConfigSlurper cfg = new groovy.util.ConfigSlurper()

		int i = 0
		data.each { Object value ->
			if (value instanceof Map) {
				CheckDataMap(value, vars)
			}
			else if (value instanceof List) {
				CheckDataList(value, vars)
			}
			else  if (value instanceof Closure) {
				Map<String, Object> res = MapUtils.Copy(vars)
				Closure cl = value
				Closure code = cl.rehydrate(res, cfg, cl.thisObject)
				code.resolveStrategy = Closure.DELEGATE_FIRST

				cfg.parse(code)
				res.remove('vars')
				data[i] = res
			}
			i++
		}
	}

	@Override
	public void saveConfig (Map<String, Object> content, Map<String, Object> saveParams = [:]) {
		def fp = (saveParams?.path as String)?:this.path
		def fn = (saveParams?.fileName as String)?:this.fileName
		def cp = (saveParams?.codePage as String)?:this.codePage
		def convVars = BoolUtils.IsValue(saveParams?.convertVars, false)

		if (fn == null) throw new ExceptionGETL('Required parameter "fileName"')

		def rp = FileUtils.RelativePathFromFile(fn)
		if (rp == '.') {
			rp = fp
		}
		else {
			fn = FileUtils.FileName(fn)
		}
		def ff = new File(fullConfigName(rp, fn))

		SaveConfigFile(content, ff, cp, convVars)
	}

	/**
	 * Save config to file
	 * @param data
	 * @param file
	 * @param codePage
	 */
	@CompileStatic
	static void SaveConfigFile (Map<String, Object> data, File file, String codePage = 'UTF-8', Boolean convertVars = false) {
		Writer writer
		try {
			if (file.exists()) file.delete()
			writer = file.newWriter(codePage)
		}
		catch (Exception e) {
			Logs.Severe("Error create configuration file \"$file\", error: ${e.message}")
			throw e
		}

		try {
			SaveMap(data, writer, convertVars)
		}
		catch (Exception e) {
			Logs.Severe("Error save configuration to file \"$file\", error: ${e.message}")
			writer.close()
			if (file.exists()) file.delete()
			throw e
		}
		finally {
			writer.close()
		}
	}

	@CompileStatic
	static void SaveMap(Map<String, Object> data, Writer writer, Boolean convertVars = false, Integer tab = 0, Boolean isListMap = false) {
		def tabStr = (tab > 0)?StringUtils.Replicate('  ', tab):''
		int i = 0
		int count = data.size()
		data.each { key, value ->
			i++
			if (value instanceof Map) {
				def eqStr = (isListMap)?':':''
				writer.println("${tabStr}${key}${eqStr} {")
				SaveMap(value as Map, writer, convertVars, tab + 1)
				writer.print("${tabStr}}")
			}
			else if (value instanceof List) {
				def eqStr = (isListMap)?':':' ='
				writer.println("${tabStr}${key}${eqStr} [")
				SaveList(value as List, writer, convertVars, tab + 1)
				writer.print("${tabStr}]")
			}
			else {
				SaveObject(key, value, writer, convertVars, tab, isListMap)
			}

			if (isListMap && i < count) writer.println(',') else writer.println()
		}
	}

	@CompileStatic
	static void SaveList(List data, Writer writer, Boolean convertVars = false, Integer tab = 0) {
		def tabStr = (tab > 0)?StringUtils.Replicate('  ', tab):''
		int i = 0
		int count = data.size()
		data.each { value ->
			i++
			if (value instanceof Map) {
				writer.println("${tabStr}[")
				SaveMap(value as Map, writer, convertVars, tab + 1, true)
				writer.print("${tabStr}]")
			}
			else if (value instanceof List) {
				writer.println("${tabStr}[")
				SaveList(value as List, writer, convertVars, tab + 1)
				writer.print("${tabStr}]")
			}
			else {
				SaveObject(null, value, writer, convertVars, tab)
			}
			if (i < count) writer.println(',') else writer.println()
		}
	}

	@CompileStatic
	static void SaveObject(def key, def value, Writer writer, Boolean convertVars = false, Integer tab = 0, Boolean isListMap = false) {
		def tabStr = (tab > 0)?StringUtils.Replicate('  ', tab):''
		def eqStr = (isListMap)?':':' ='
		def keyStr = (key != null)?"$key$eqStr ":''
		if (value instanceof Date) {
			writer.print("${tabStr}${keyStr}new java.sql.Timestamp(Date.parse('yyyy-MM-dd HH:mm:ss.SSS', '${DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss.SSS', value as Date)}').time)")
		}
		else if (value instanceof String || value instanceof GString) {
			def str = value.toString()
			def quote = (str.indexOf('${') == -1)?'\'':'"'
			if (convertVars) {
				value = value.toString().replace('${', '${vars.')
			}
			writer.print("${tabStr}${keyStr}${quote}${StringUtils.EscapeJavaWithoutUTF(value.toString())}${quote}")
		}
		else {
			writer.print("${tabStr}${keyStr}${value.toString()}")
		}
	}

	static void main(def args) {
		def a = MapUtils.ProcessArguments(args)
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
		String copePage = a.codepage?:'UTF-8'
		Boolean convertVars = BoolUtils.IsValue(a.convert_vars, false)
		String ruleFileName = a.rules

		assert new File(a.source).exists(), "Source file \"$sourceName\" not found!"
		assert a.source != a.dest, 'Source and destination file must have different names!'

		Map<String, String> rules
		if (ruleFileName != null) {
			def ruleFile = new File(ruleFileName)
			assert ruleFile.exists(), "Rule config file \"$ruleFileName\" not found!"
			rules = LoadConfigFile(ruleFile, copePage)
			assert (rules.rules as Map)?.size() > 0, "Rules not found in config file \"$ruleFileName\"!"
		}

		Config.evalVars = false
		Config.LoadConfig(fileName: sourceName, codePage: copePage)
		if (Config.IsEmpty()) {
			println "Configuration JSON file is empty!"
			return
		}

		if (rules != null) {
			rules.rules.each { source, dest ->
				MapUtils.FindKeys(Config.content, source) { Map map, String key, item ->
					map.put(dest, map.remove(key))
				}
			}
		}

		Config.configClassManager = new ConfigSlurper()
		if ((Config.content.vars as Map)?.size() == 0) {
			Config.content.remove('vars')
		}
		Config.SaveConfig(fileName: destName, codePage: copePage, convertVars: convertVars)
		println "Convert \"$sourceName\" to \"$destName\" complete."
	}
}