package getl.utils

import getl.config.ConfigFiles
import getl.config.ConfigManager
import getl.proc.Job
import getl.exception.ExceptionGETL
import groovy.transform.Synchronized

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
    static public final String OS = System.getProperty("os.name").toLowerCase()

    /**
     * Current OS is Windows
     */
    static Boolean isWindows() { (OS.indexOf("win") >= 0) }

    /**
     * Current OS is Mac
     */
    static Boolean isMac() { (OS.indexOf("mac") >= 0) }

    /**
     * Current OS is Unix
     */
    static Boolean isUnix() { (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0 ) }

    /**
     * Current OS is Solaris
     */
    static Boolean isSolaris() { (OS.indexOf("sunos") >= 0) }

    /**
	 * Used Java version
	 */
	static public final BigDecimal JavaVersion = new BigDecimal(System.getProperty("java.vm.specification.version"))
	
	/**
	 *  Parameters
	 */
	static Map<String, Object> getParams() { configClassManager.params }

    /**
     * Evaluate variables where load configuration
     */
    static public Boolean evalVars = true

    /**
     * Class used for configuration management
     */
    static private ConfigManager configClassManager = new ConfigFiles()

	static ConfigManager getConfigClassManager() { configClassManager }

	static void setConfigClassManager(ConfigManager value) {
        configClassManager = value
    }

	static void Init(Map initParams) {
		if ((initParams.config as Map)?.manager != null) {
            configClassManager = Class.forName((initParams.config as Map).manager as String).newInstance() as ConfigManager
            Logs.Config("config: use ${configClassManager.getClass().name} class for config manager")
        }

        configClassManager.init(initParams)
    }

	/**
	 * Content config file	
	 */
	static public final Map<String, Object> content = [vars: new HashMap<String, Object>()]
	
	/**
	 * Variables
	 */
	static Map<String, Object> getVars() { content.vars as Map<String, Object>}

    /**
     * Set variables
     * @param value
     */
	static void setVars(Map value) {
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
	static public final List<Closure> init = [] as List<Closure>

	/**
	 * Re-initialization class
	 */
	@Synchronized
	static void ReInit() {
		init.clear()
		ClearConfig()
		configClassManager = new ConfigFiles()
	}

	/**
	 * Registration object code closure on load of the configuration files
	 * @param code
	 */
	@Synchronized
	static void RegisterOnInit(Closure code) {
		if (init.find { it == code } == null) {
			init << code
		}
	}

	/**
	 * Unregistration object code closure on load of the configuration files
	 * @param code
	 */
	@Synchronized
	static void UnregisterOnInit(Closure code) {
		if (init.find { it == code } != null) {
			init.remove(code)
		}
	}

	/**
	 * Clear all configurations
	 */
	@Synchronized
	static void ClearConfig () {
		content.clear()
        this.vars = new HashMap<String, Object>()
	}
	
	/**
	 * Load configuration
	 */
	@Synchronized
	static void LoadConfig (Map readParams = [:]) {
		if (readParams.files != null) {
			def l = [] as List<String>
			(readParams.files as List).each {
				if (it instanceof String)
					l << FileUtils.ResourceFileName(it as String)
				else
					l << it.toString()
			}
			readParams.files = l
		}
		if (readParams.fileName != null && readParams.fileName instanceof String) {
			readParams.fileName = FileUtils.ResourceFileName(readParams.fileName as String)
		}
        configClassManager.loadConfig(readParams)
		DoInitEvent()
	}

	static void MergeConfig (Map data) {
        if (data == null) throw new ExceptionGETL('Null "data" detected!')

        Map<String, Object> currentVars = vars
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
	@Synchronized
	static void DoInitEvent () {
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
	@Synchronized
	static Map FindSection (String section) {
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
	static Boolean ContainsSection (String section) {
		MapUtils.ContainsSection(content, section)
	}

	/**
	 * Set value in content	
	 * @param name
	 * @param value
	 */
	@Synchronized
	static void SetValue(String name, value) {
		MapUtils.SetValue(content, name, value)
	}
	
	/**
	 * Save content to JSON configuration file
	 * @param writer
	 */
	@Synchronized
	static void SaveConfig (Map saveParams = [:]) {
        configClassManager.saveConfig(content, saveParams)
	}

	static Boolean IsEmpty() {
        return (content.size() == 1 && vars.isEmpty())
    }
}