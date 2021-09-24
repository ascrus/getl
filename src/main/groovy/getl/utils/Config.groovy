package getl.utils

import getl.config.ConfigFiles
import getl.config.ConfigManager
import getl.exception.ExceptionGETL
import getl.lang.Getl
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Configuration manager class
 * @author Alexsey Konstantinov
 *
 */
class Config {
	Config () {
		throw new ExceptionGETL("Deny create instance Config class")
	}

    /** Current OS */
    static public final String OS = System.getProperty("os.name").toLowerCase()

    /** Current OS is Windows */
    static Boolean isWindows() { (OS.indexOf("win") >= 0) }

    /** Current OS is Mac */
    static Boolean isMac() { (OS.indexOf("mac") >= 0) }

    /** Current OS is Unix */
    static Boolean isUnix() { (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0 ) }

    /** Current OS is Solaris */
    static Boolean isSolaris() { (OS.indexOf("sunos") >= 0) }

    /** Used Java version */
	static public final BigDecimal JavaVersion = new BigDecimal(System.getProperty("java.vm.specification.version"))
	
	/**  Parameters */
	static Map<String, Object> getParams() { configClassManager.params }

    /** Evaluate variables where load configuration */
    static Boolean getEvalVars() { configClassManager.evalVars }
	/** Evaluate variables where load configuration */
	static void setEvalVars(Boolean value) { configClassManager.evalVars = value }

    /** Class used for configuration management */
    static private ConfigManager configClassManager = new ConfigFiles()
	/** Class used for configuration management */
	@Synchronized
	static ConfigManager getConfigClassManager() { configClassManager }
	/** Class used for configuration management */
	@Synchronized
	static void setConfigClassManager(ConfigManager value) {
		if (configClassManager == value)
			return

		if (configClassManager != null)
			configClassManager.init.each {value.registerOnInit(it) }

		def oldManager = configClassManager
        configClassManager = value
		CallChangeManagerEvents(oldManager, value)
    }

	/** List events on change config manager */
	static private List<Closure> listChangeManagerEvents = []
	/** Register event on change config manager */
	@Synchronized
	static void RegisterChangeManagerEvent(@ClosureParams(value = SimpleType,
			options = ['getl.config.ConfigManager', 'getl.config.ConfigManager']) Closure cl) {
		if (cl == null)
			throw new ExceptionGETL('The event code cannot be null!')
		if (listChangeManagerEvents.find { it == cl } == null)
			listChangeManagerEvents.add(cl)
	}
	/** Unregister event on change config manager */
	@Synchronized
	static void UnregisterChangeManagerEvent(@ClosureParams(value = SimpleType,
			options = ['getl.config.ConfigManager', 'getl.config.ConfigManager']) Closure cl) {
		listChangeManagerEvents.remove(cl)
	}
	/** Clear event on change config manager */
	@Synchronized
	static void ClearChangeManagerEvents() {
		listChangeManagerEvents.clear()
	}
	/** Call events on change config manager */
	static private void CallChangeManagerEvents(ConfigManager oldManager, ConfigManager newManager) {
		listChangeManagerEvents.each {code ->
			code.call(oldManager, newManager)
		}
	}

	/** Init configuration */
	static void Init(Map initParams) {
		if ((initParams.config as Map)?.manager != null) {
            configClassManager = Class.forName((initParams.config as Map).manager as String).newInstance() as ConfigManager
            Logs.Config("config: use ${configClassManager.getClass().name} class for config manager")
        }

        configClassManager.init(initParams)
    }

	/** Content config file */
	static Map<String, Object> getContent() { configClassManager.content }
	
	/**	Variables */
	static Map<String, Object> getVars() { configClassManager.vars }
	/**	Variables */
	static void setVars(Map value) { configClassManager.vars = value }

	/** List of initialization object code on load config */
	static List<Closure> getInit() { configClassManager.init }
	static void setInit(List<Closure> value) { configClassManager.init = value }

	/** Re-initialization class */
	@Synchronized
	static void ReInit() {
		configClassManager = null
		ClearChangeManagerEvents()
		configClassManager = new ConfigFiles()
	}

	/**
	 * Registration initialization code closure on load of the configuration files
	 * @param code initialization code
	 */
	static void RegisterOnInit(Closure code) {
		configClassManager.registerOnInit(code)
	}

	/**
	 * Unregister initialization code closure on load of the configuration files
	 * @param code initialization code
	 */
	static void UnregisterOnInit(Closure code) {
		configClassManager.unregisterOnInit(code)
	}

	/** Clear all configurations */
	static void ClearConfig () {
		configClassManager.clearConfig()
	}
	
	/**	 Load configuration content from file */
	static void LoadConfig(Map readParams = [:]) { configClassManager.loadConfig(readParams) }

	/**
	 * Merge specified configuration to current content
	 * @param data merged configuration
	 */
	static void MergeConfig (Map data) { configClassManager.mergeConfig(data) }

	/**
	 * Run every event subscriber after load config files
	 */
	static void DoInitEvent() {
		configClassManager.initEvents()
	}

	/**
	 * Find config section by section name
	 * @param section section name (syntax: section[.section[.section.[...]]])
	 * @return found section
	 */
	static Map FindSection(String section) { configClassManager.findSection(section) }
	
	/**
	 * Validation contains section
	 * @param section section name (syntax: section[.section[.section.[...]]])
	 * @return section search result
	 */
	static Boolean ContainsSection(String section) { configClassManager.containsSection(section) }

	/**
	 * Set the value for the specified content element
	 * @param name element name (syntax: section[.section[.section.[...]]].name)
	 * @param value element value
	 */
	static void SetValue(String name, value) { configClassManager.setContentValue(name, value) }
	
	/**
	 * Save content to configuration file
	 * @param writer
	 */
	static void SaveConfig(Map saveParams = [:]) { configClassManager.saveContent(saveParams) }

	/**
	 * Check configuration content is empty
	 * @return result checking
	 */
	static Boolean IsEmpty() { configClassManager.isEmpty() }

	/**
	 * Return system properties and environment OS variables
	 * @return list of environment variables
	 */
	static Map<String, String> SystemProps() {
		def res = [:] as Map<String, String>
		res.putAll(System.getenv())
		System.properties.each { prop ->
			res.put(prop.key.toString(), prop.value as String)
		}

		return res
	}

	/** Return config variable by Getl config manager or global content */
	static Map<String, Object> ConfigVars(Getl dslCreator) {
		return (dslCreator != null)?dslCreator.configVars:vars
	}
}