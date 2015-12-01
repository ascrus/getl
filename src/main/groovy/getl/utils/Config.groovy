/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

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

package getl.utils

import groovy.transform.Synchronized
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

import getl.exception.ExceptionGETL
import getl.utils.MapUtils

/**
 * Configuration manager class
 * @author Alexsey Konstantinov
 *
 */
class Config {
	Config () {
		throw new ExceptionGETL("Deny create instance Config class")
	}
	
	public static final def JavaVersion = new BigDecimal(System.getProperty("java.vm.specification.version"))
	
	/**
	 *  Parameters
	 */
	public static final Map params = [:]
	
	/**
	 * Path for configuration files
	 */
	private static String path
	public static String getPath () { path }
	public static void setPath (String value) { path = value }

	/**
	 * Configuration file name
	 */
	private static String fileName
	public static String getFileName () { fileName }
	public static void setFileName (String value) { fileName = value }
	
	/**
	 * List of configuration files
	 */
	private static List files = []
	public static List getFiles () { files }
	public static void setFiles (def value) {
		files.clear()
		files.addAll(value)
	}
	
	/**
	 * Configuration files code page
	 * @return
	 */
	public static String getCodePage () { (params.codePage != null)?params.codePage:"UTF-8" }
	public static void setCodePage (String value) { params.codePage = value } 
	
	public static String getFullName () { ((path != null)?FileUtils.ConvertToUnixPath(path) + "/":"") + fileName }
	public static String FullConfigName (String value) { ((path != null)?FileUtils.ConvertToUnixPath(path) + "/":"") + value }
	
	/**
	 * Content config file	
	 */
	public static final Map content = [vars: [:]]
	
	/**
	 * Variables
	 */
	public static Map getVars() { Config.content."vars" }
	
	public void setVars(Map value) { 
		if (vars == null) Config.content."vars" = [:]
		vars.putAll(value)
	}
	
	/**
	 * Initialization code on load config
	 */
	public static final List<Closure> init = []
	
	@Synchronized
	public static void RegisterOnInit(Closure code) {
		init << code
	}
	
	/**
	 * Load configuration from file
	 * Use parameters: params.path and params.fileName) 
	 */
	@Synchronized
	public static void LoadConfig () {
		if (fileName != null) {
			try {
				LoadConfigFile(new File(fullName), this.codePage)
			}
			catch (Exception e) {
				Logs.Severe("Error read config file \"${fullName}\"")
				throw e
			}
			DoInitEvent()
		}
		
		if (files != null) {
			files.each {
				try { 
					LoadConfigFile(new File(FullConfigName(it)), this.codePage)
				}
				catch (Exception e) {
					Logs.Severe("Error read config file \"${FullConfigName(it)}\"")
					throw e
				}
			}
			DoInitEvent()
		}
	}
	
	/**
	 * Load configuration from file with class name
	 * Use parameters: params.path
	 * @param jobClass
	 */
	@Synchronized
	public static void LoadConfigClass(Class jobClass) {
		LoadConfig(jobClass, this.codePage)
	}
	
	/**
	 * Load configuration from file with class name
	 * Use parameters: params.path
	 * @param jobClass
	 * @param codePage
	 */
	@Synchronized
	public static void LoadConfigClass(Class jobClass, String codePage) {
		String className = jobClass.name.replace(".", File.separator)
		def fn = ((path != null)?path + File.separator:"") + className + ".conf"
		LoadConfigFile(new File(fn), codePage)
		DoInitEvent()
	}
	
	/**
	 * Load configuration from file
	 * @param fileName
	 */
	@Synchronized
	public static void LoadConfig (String fileName) {
		if (path != null) fileName = path + File.separator + fileName
		LoadConfigFile(new File(fileName), this.codePage)
		DoInitEvent()
	}
	
	/**
	 * Load configuration from file
	 * @param fileName
	 * @param codePage
	 */
	@Synchronized
	public static void LoadConfig (String fileName, String codePage) {
		if (path != null) fileName = path + File.separator + fileName
		LoadConfigFile(new File(fileName), codePage)
		DoInitEvent()
	}
	
	/**
	 * Load configuration from file
	 * @param file
	 */
	@Synchronized
	public static void LoadConfigFile (File file) {
		LoadConfigFile(file, this.codePage)
	}
	

	/**
	 * Load configuration from file	
	 * @param file
	 * @param codePage
	 */
	@Synchronized
	public static void LoadConfigFile (File file, String codePage) {
		if (!file.exists()) throw new ExceptionGETL("Config file \"${file.path}\" not found")
		Logs.Config("Load config file \"${file.absolutePath}\"")
		try {
			LoadSection(file.newReader(codePage))
		}
		catch (Exception e) {
			Logs.Severe("Invalid JSON file \"${file.name}\"")
			throw e
		}
	}
	
	/**
	 * Load configuration from reader
	 * @param reader
	 */
	@Synchronized
	public static void LoadConfigReader (Reader reader) {
		LoadSection(reader)
	}
	
	/**
	 * Run every eventer after load config files
	 */
	@Synchronized
	public static void DoInitEvent () {
		init.each { doInit ->
			doInit()
		}
	}
	
	/**
	 * Load configuration file to config section 
	 * @param section
	 * @param reader
	 */
	@Synchronized
	public static void LoadSection(Reader reader) {
		def json = new JsonSlurper()
		def data
		try {
			data = json.parse(reader)
		}
		catch (groovy.json.JsonException e) {
			Logs.Severe("Invalid json text, error: ${e.message}")
			throw e
		}
		finally {
			reader.close()
		}
		
		Map vars = content.vars?:[:]
		if (data.vars != null) MapUtils.MergeMap(vars, data.vars)
		
		if (data != null) {
			if (!vars.isEmpty() && data instanceof Map) {
				if (!data.isEmpty()) {
					try {
						data = MapUtils.EvalMacroValues(data, vars)
					}
					catch (groovy.lang.MissingPropertyException e) {
						Logs.Severe("${e.message}, avaible vars: ${vars.keySet().toList()}")
						throw e
					}
				}
			}
		
			MapUtils.MergeMap(content, data)
		}
	}
	
	/**
	 * Load configuration file to config section
	 * @param fileName
	 */
	@Synchronized
	public static void LoadSection (String fileName) {
		LoadSection(new File(fileName), this.codePage)
	}
	
	/**
	 * Load configuration file to config section
	 * @param fileName
	 * @param codePage
	 */
	@Synchronized
	public static void LoadSection (String fileName, String codePage) {
		LoadSection(new File(fileName), codePage)
	}
	
	/**
	 * Load configuration file to config section
	 * @param file
	 * @param codePage
	 */
	@Synchronized
	public static void LoadSection (File file, String codePage) {
		if (!file.exists()) throw new ExceptionGETL("File \"${file.path}\" not found")
		LoadSection(file.newReader(codePage))
	}
	
	/**
	 * Load configuration file to config section
	 * @param file
	 */
	@Synchronized
	public static void LoadSection (File file) {
		if (!file.exists()) throw new ExceptionGETL("File \"${file.path}\" not found")
		LoadSection(file.newReader(this.codePage))
	}
	
	/**
	 * Find config section by section name
	 * Syntax section name: section[.section[.section.[...]]] 
	 * @param section
	 * @return
	 */
	@Synchronized
	public static Map FindSection (String section) {
		if (section == null) return null
		def res = MapUtils.FindSection(content, section)
		(res != null)?res:[:]
	}
	
	/**
	 * Validation contains section
	 * @param section
	 * @return
	 */
	@Synchronized
	public static boolean ContainsSection (String section) {
		MapUtils.ContainsSection(content, section)
	}

	/**
	 * Set value in content	
	 * @param name
	 * @param value
	 */
	@Synchronized
	public static void SetValue(String name, value) {
		MapUtils.SetValue(content, name, value)
	}
	
	/**
	 * Save content to JSON configuration file
	 * @param writer
	 */
	@Synchronized
	public static void SaveConfig (Writer writer) {
		JsonBuilder b = new JsonBuilder()
		b.call(content)
		writer.println(b.toPrettyString())
	}
	
	/**
	 * Save content to JSON configuration file
	 * @param file
	 * @param codePage
	 */
	@Synchronized
	public static void SaveConfig (File file, String codePage) {
		def writer = file.newWriter(codePage)
		try {
			SaveConfig(writer)
		}
		finally {
			writer.close()
		}
	}
	
	/**
	 * Save content to JSON configuration file
	 * @param file
	 */
	@Synchronized
	public static void SaveConfig (File file) {
		SaveConfig(file, "UTF-8")
	}
	
	/**
	 * Save content to JSON configuration file
	 * @param file
	 * @param codePage
	 */
	@Synchronized
	public static void SaveConfig (String file, String codePage) {
		SaveConfig(new File(file), codePage)
	}
	
	/**
	 * Save content to JSON configuration file
	 * @param file
	 */
	@Synchronized
	public static void SaveConfig (String file) {
		SaveConfig(file, "UTF-8")
	}
	
	/**
	 * Evaluation macros in the configuration values 
	 */
	@Synchronized
	public static void EvalConfig () {
		def vars = Config.content."vars"?:[:]
		def evalContent = MapUtils.EvalMacroValues(Config.content, vars)
		Config.content.clear()
		Config.content.putAll(evalContent)
	}

	/**
	 * Current OS	
	 */
	private static String OS = System.getProperty("os.name").toLowerCase()
	
	/**
	 * Current OS is Windows
	 */
	public static boolean isWindows() { (OS.indexOf("win") >= 0) }
	
	/**
	 * Current OS is Mac
	 */
	public static boolean isMac() { (OS.indexOf("mac") >= 0) }
		
	/**
	 * Current OS is Unix
	 */
	public static boolean isUnix() { (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0 ) }

	/**
	 * Current OS is Solaris
	 */
	public static boolean isSolaris() { (OS.indexOf("sunos") >= 0) }
}
