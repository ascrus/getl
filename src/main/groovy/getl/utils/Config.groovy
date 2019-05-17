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

import getl.config.ConfigFiles
import getl.config.ConfigManager
import getl.proc.Job
import getl.exception.ExceptionGETL

/**
 * Configuration manager class
 * @author Alexsey Konstantinov
 *
 */
class Config {
	Config () {
		throw new ExceptionGETL("Deny create instance Config class")
	}

    /**
     * Current OS
     */
    public  static String OS = System.getProperty("os.name").toLowerCase()

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

    /**
	 * Used Java version
	 */
	public static final def JavaVersion = new BigDecimal(System.getProperty("java.vm.specification.version"))
	
	/**
	 *  Parameters
	 */
	public static Map<String, Object> getParams() { configClassManager.params }

    /**
     * Evaluate variables where load configuration
     */
    public static boolean evalVars = true

    /**
     * Class used for configuration management
     */
    private static ConfigManager configClassManager = new ConfigFiles()
	public static getConfigClassManager() { configClassManager }
    public static void setConfigClassManager(ConfigManager value) {
        configClassManager = value
    }

    public static void Init(Map initParams) {
		if ((initParams.config as Map)?.manager != null) {
            configClassManager = Class.forName((initParams.config as Map).manager as String).newInstance() as ConfigManager
            Logs.Config("config: use ${configClassManager.getClass().name} class for config manager")
        }

        configClassManager.init(initParams)
    }

	/**
	 * Content config file	
	 */
	public static final Map<String, Object> content = [vars: new HashMap<String, Object>()]
	
	/**
	 * Variables
	 */
	public static Map<String, Object> getVars() { Config.content."vars" as Map<String, Object>}

    /**
     * Set variables
     * @param value
     */
	public static void setVars(Map value) {
        if (value == null) throw new ExceptionGETL('Null "value" detected!')
		def v = content.vars as Map<String, Object>
		if (v == null) {
			v = new HashMap<String, Object>()
			content.put('vars', v)
		}
		v.putAll(value)
	}
	
	/**
	 * List of initialization object code on load config
	 */
	public static final List<Closure> init = []

	/**
	 * Re-initialization class
	 */
	@groovy.transform.Synchronized
	public static void ReInit() {
		init.clear()
		ClearConfig()
		configClassManager = new ConfigFiles()
	}

	/**
	 * Registration object code closure on load of the configuration files
	 * @param code
	 */
	@groovy.transform.Synchronized
	public static void RegisterOnInit(Closure code) {
		if (init.find { it == code } == null) {
			init << code
		}
	}

	/**
	 * Unregistration object code closure on load of the configuration files
	 * @param code
	 */
	@groovy.transform.Synchronized
	public static void UnregisterOnInit(Closure code) {
		if (init.find { it == code } != null) {
			init.remove(code)
		}
	}

	/**
	 * Clear all configurations
	 */
	@groovy.transform.Synchronized
	public static void ClearConfig () {
		content.clear()
        this.vars = new HashMap<String, Object>()
	}
	
	/**
	 * Load configuration
	 */
	@groovy.transform.Synchronized
	public static void LoadConfig (Map readParams = [:]) {
        configClassManager.loadConfig(readParams)
		DoInitEvent()
	}

    public static void MergeConfig (Map data) {
        if (data == null) throw new ExceptionGETL('Null "data" detected!')

        Map<String, Object> currentVars = this.vars
        if (data.vars != null) MapUtils.MergeMap(currentVars, (Map<String, Object>)(data.vars))
        if (!(Job.jobArgs.vars as Map)?.isEmpty()) MapUtils.MergeMap(currentVars, (Map<String, Object>)(Job.jobArgs.vars))

        if (evalVars && configClassManager.evalVars && !currentVars.isEmpty() && !data.isEmpty()) {
            try {
                data = MapUtils.EvalMacroValues(data, currentVars)
            }
            catch (MissingPropertyException e) {
                Logs.Severe("${e.message}, available variables: ${currentVars.keySet().toList()}")
                throw e
            }
        }

        MapUtils.MergeMap(content, (Map) data)
    }
	
	/**
	 * Run every eventer after load config files
	 */
	@groovy.transform.Synchronized
	public static void DoInitEvent () {
		init.each { doInit ->
			doInit()
		}
	}

	/**
	 * Find config section by section name
	 * Syntax section name: section[.section[.section.[...]]] 
	 * @param section
	 * @return
	 */
	@groovy.transform.Synchronized
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
	@groovy.transform.Synchronized
	public static boolean ContainsSection (String section) {
		MapUtils.ContainsSection(content, section)
	}

	/**
	 * Set value in content	
	 * @param name
	 * @param value
	 */
	@groovy.transform.Synchronized
	public static void SetValue(String name, value) {
		MapUtils.SetValue(content, name, value)
	}
	
	/**
	 * Save content to JSON configuration file
	 * @param writer
	 */
	@groovy.transform.Synchronized
	public static void SaveConfig (Map saveParams = [:]) {
        configClassManager.saveConfig(content, saveParams)
	}
	
	public static boolean IsEmpty() {
        return (content.size() == 1 && vars.isEmpty())
    }
}