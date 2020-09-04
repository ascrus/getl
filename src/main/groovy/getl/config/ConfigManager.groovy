package getl.config

/**
 * Configuration manager class
 * @author Alexsey Konstantinov
 *
 */
abstract class ConfigManager {
    ConfigManager() { }

    ConfigManager(Map<String, Object> params) {
        this.params.putAll(params)
    }

    /**
     * Evaluate variables where load configuration
     */
    Boolean getEvalVars() { false }

    /**
     * Parameters of configuration
     */
    protected final Map<String, Object> params = [:] as Map<String, Object>

    /**
     * Load configuration
     * @param readParams
     */
    abstract void loadConfig(Map<String, Object> readParams = [:])

    /**
     * Save configuration
     * @param content
     * @param saveParams
     */
    abstract void saveConfig(Map<String, Object> content, Map<String, Object> saveParams = [:])

    /**
     * Init manager
     * @param initParams
     */
    abstract void init(Map<String, Object> initParams)
}