package getl.config

abstract class ConfigManager {
    /**
     * Load configuration
     * @readParams
     */
    abstract public void loadConfig(Map<String, Object> readParams = [:])

    /**
     * Save configuration
     * @param content
     * @param saveParams
     */
    abstract public void saveConfig(Map<String, Object> content, Map<String, Object> saveParams = [:])

    /**
     * Init manager
     * @param initParams
     */
    abstract public void init(Map<String, Object> initParams)
}
