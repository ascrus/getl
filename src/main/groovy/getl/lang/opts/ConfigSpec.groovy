package getl.lang.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.config.*
import getl.exception.ExceptionDSL
import getl.lang.Getl
import getl.utils.*
import groovy.transform.InheritConstructors

/**
 * Config specification class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ConfigSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.configManager == null) {
            def manager = new ConfigSlurper(ownerObject as Getl)
            saveParamValue('configManager', manager)
            Config.configClassManager = manager
        }
        else
            Config.configClassManager = manager
    }

    /** Configuration manager */
    @JsonIgnore
    ConfigSlurper getManager() { params.configManager as ConfigSlurper}

    /** Configuration files path */
    String getPath() { manager.path }
    /** Configuration files path */
    void setPath(String value) { manager.path = value }
    /** Configuration files full path */
    String fullPath() { manager.path() }

    /**
     * Code page in configuration files
     */
    String getCodePage() { manager.codePage }
    /**
     * Code page in configuration files
     */
    void setCodePage(String value) { manager.codePage = value }

    /**
     * Load configuration file
     * @param fileName configuration file name
     * @param environment environment
     * @param codePage code page file
     */
    void load(String fileName, String environment = null, String codePage = null) {
        manager.loadConfig([fileName: FileUtils.ResourceFileName(fileName, ownerObject as Getl), codePage: codePage,
                           environment: environment])
    }

    /**
     * Load configuration file
     * @param file configuration file
     * @param environment environment
     * @param codePage code page file
     */
    void load(File file, String environment = null, String codePage = null) {
        manager.loadConfig([fileName: file.path, codePage: codePage, environment: environment])
    }

    /**
     * Load configuration file
     * @param fileName configuration file name
     * @param environment environment
     * @param secretKey encode key string
     */
    void loadEncrypt(String fileName, String environment = null, String secretKey = null) {
        def data = ConfigStores.LoadSection(FileUtils.ResourceFileName(fileName, ownerObject as Getl), secretKey, environment?:manager.environment?:'all')
        manager.mergeConfig(data)
    }

    /**
     * Load configuration file
     * @param fileName configuration file
     * @param environment environment
     * @param secretKey encode key string
     */
    void loadEncrypt(File file, String environment = null, String secretKey = null) {
        def data = ConfigStores.LoadSection(file.path, secretKey, environment?:manager.environment?:'all')
        manager.mergeConfig(data)
    }

    /**
     * Save configuration file
     * @param fileName configuration file name
     * @param codePage code page file
     */
    void save(String fileName, String codePage = null) {
        manager.saveContent([fileName: fileName, codePage: codePage])
    }

    /**
     * Save configuration file
     * @param data saved data
     * @param fileName configuration file name
     * @param environment environment
     * @param secretKey encode key string
     */
    void saveEncrypt(Map data, String fileName, String environment = null, String secretKey = null) {
        ConfigStores.SaveSection(data, fileName, secretKey, environment?:manager.environment?:'all')
    }

    /**
     * Clear configuration content
     */
    void clear() { manager.clearConfig() }

    /**
     * Read fields from the specified configuration section
     * @param section path to store variables in configuration
     * @param validExist check for the existence of fields in the script
     */
    void readFields(String section, Boolean validExist = true) {
        def vars = manager.findSection(section)
        if (vars == null)
            throw new ExceptionDSL("Configuration section \"$section\" not found!")
        if (vars.isEmpty())
            return

        (ownerObject as Getl)._fillFieldFromVars(ownerObject as Script, vars, validExist)
    }

    /** Current environment */
    String getEnvironment() {
        return manager.environment
    }
    /** Current environment */
    void setEnvironment(String value) {
        manager.environment = value
    }
}