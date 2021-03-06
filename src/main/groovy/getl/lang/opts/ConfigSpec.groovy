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
    /**
     * Configuration manager
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    @JsonIgnore
    ConfigSlurper getManager() { Config.configClassManager as ConfigSlurper }

    /** Configuration files path */
    @SuppressWarnings("GrMethodMayBeStatic")
    String getPath() { manager.path }
    /** Configuration files path */
    @SuppressWarnings("GrMethodMayBeStatic")
    void setPath(String value) { manager.path = value }
    /** Configuration files full path */
    String fullPath() { manager.path() }

    /**
     * Code page in configuration files
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    String getCodePage() { manager.codePage }
    /**
     * Code page in configuration files
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    void setCodePage(String value) { manager.codePage = value }

    /**
     * Load configuration file
     * @param fileName configuration file name
     * @param environment environment
     * @param codePage code page file
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    void load(String fileName, String environment = null, String codePage = null) {
        Config.LoadConfig([fileName: FileUtils.ResourceFileName(fileName, ownerObject as Getl), codePage: codePage,
                           environment: environment])
    }

    /**
     * Load configuration file
     * @param fileName configuration file name
     * @param environment environment
     * @param secretKey encode key string
     */
    void loadEncrypt(String fileName, String environment = null, String secretKey = null) {
        def data = ConfigStores.LoadSection(FileUtils.ResourceFileName(fileName, ownerObject as Getl), secretKey, environment?:manager.environment?:'all')
        Config.MergeConfig(data)
    }

    /**
     * Save configuration file
     * @param fileName configuration file name
     * @param codePage code page file
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    void save(String fileName, String codePage = null) {
        Config.SaveConfig(fileName: fileName, codePage: codePage)
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
    @SuppressWarnings("GrMethodMayBeStatic")
    void clear() { Config.ClearConfig() }

    /**
     * Read fields from the specified configuration section
     * @param section path to store variables in configuration
     * @param validExist check for the existence of fields in the script
     */
    void readFields(String section, Boolean validExist = true) {
        def vars = Config.FindSection(section)
        if (vars == null)
            throw new ExceptionDSL("Configuration section \"$section\" not found!")
        if (vars.isEmpty())
            return

        (ownerObject as Getl)._fillFieldFromVars(ownerObject as Script, vars, validExist)
    }

    /** Current environment */
    @SuppressWarnings("GrMethodMayBeStatic")
    String getEnvironment() {
        return manager.environment
    }
    /** Current environment */
    @SuppressWarnings("GrMethodMayBeStatic")
    void setEnvironment(String value) {
        manager.environment = value
    }
}