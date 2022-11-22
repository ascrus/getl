package getl.config

import getl.exception.IOFilesError
import getl.exception.IncorrectParameterError
import getl.exception.RequiredParameterError
import getl.utils.FileUtils
import getl.utils.MapUtils
import groovy.json.JsonSlurper
import groovy.transform.InheritConstructors
import org.h2.mvstore.MVMap
import org.h2.mvstore.MVStore

/**
 * Configuration security store manager class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ConfigStores extends ConfigManager {
    @Override
    void init(Map<String, Object> initParams) {
        super.init(initParams)
        evalVars = true

        if (initParams?.config == null)
            return

        Map config = initParams.config as Map<String, Object>
        if (config.filename != null) {
            def fn = config.filename as String
            this.fileName = fn
            logger.config("config: use file ${this.fileName}")
        }

        if (config.section != null) {
            this.section = config.section
            logger.config("config: use section \"${this.fileName}\"")
        }
    }

    /**
     * Configuration file name
     */
    String getFileName () { params.fileName as String }

    /**
     * Set configuration file name
     * @param value
     */
    void setFileName (String value) {
        if (value == null || value.trim() == '')
            throw new IncorrectParameterError('#params.empty', 'fileName')
        params.fileName = value.trim()
    }

    /**
     * Configuration section name
     */
    String getSection () { (params.section as String)?:'config' }

    /**
     * Set configuration section name
     * @param value
     */
    void setSection (String value) {
        if (value == null || value.trim() == '')
            throw new IncorrectParameterError('#params.empty', 'section')
        params.section = value.trim()
    }

    /** Configuration secret key */
    String getSecretKey() { (params.secretKey as String) }
    /** Configuration secret key */
    void setSecretKey(String value) {
        if (value == '')
            throw new IncorrectParameterError('#params.empty', 'secretKey')
        params.secretKey = value
    }

    @Override
    protected void loadContent(Map<String, Object> readParams = new HashMap<String, Object>()) {
        def fileName = (readParams?.fileName as String)?:this.fileName
        def section = (readParams?.section as String)?:this.section
        def secretKey = (readParams?.secretKey as String)?:this.secretKey

        if (fileName == null) return

        Map<String, Object> data = LoadSection(fileName, secretKey, section)
        mergeConfig(data)
    }

    /**
     * Open MVStore file
     * @param fileName store file name
     * @param secretKey password
     * @param readOnly read only operation
     * @return MVStore session
     */
    static MVStore OpenStore(String fileName, String secretKey, Boolean readOnly = true) {
        def builder = new MVStore.Builder().fileName(fileName).encryptionKey((secretKey?:(this.getClass().name)).toCharArray())
                .pageSplitSize(1024).compressHigh()
        if (readOnly) builder.readOnly()
        return builder.open()
    }

    /**
     * Load section from MVStore file
     * @param fileName store file name
     * @param secretKey password
     * @param sectionName section name
     * @return section data
     */
    static Map<String, Object> LoadSection(String fileName, String secretKey, String sectionName) {
        if (fileName == null)
            throw new RequiredParameterError('fileName', 'LoadSection')

        if (sectionName == null)
            throw new RequiredParameterError('sectionName', 'LoadSection')

        Map<String, Object> data = new HashMap<String, Object>()

        fileName = FileUtils.AddExtension(fileName, 'store')
        if (!FileUtils.ExistsFile(fileName))
            throw new IOFilesError('#io.file.not_found', [path: fileName, type: 'Config'])

        def store = OpenStore(fileName, secretKey)
        try {
            MVMap<String, HashMap<String, Object>> map = store.openMap(sectionName)
            data.putAll(map)
        }
        finally {
            store.closeImmediately()
        }

        return data
    }

    @Override
    void saveConfig(Map<String, Object> content, Map<String, Object> saveParams) {
        def fileName = (saveParams?.fileName as String)?:this.fileName
        def section = (saveParams?.section as String)?:this.section
        def secretKey = (saveParams?.secretKey as String)?:this.secretKey

        SaveSection(content, fileName, secretKey, section)
    }

    /**
     * Save data to MVStore file by specified section
     * @param data data for saving
     * @param fileName store file name
     * @param secretKey password
     * @param sectionName section name
     */
    static void SaveSection(Map<String, Object> data, String fileName, String secretKey, String sectionName) {
        if (fileName == null)
            throw new RequiredParameterError('fileName', 'SaveSection')

        if (sectionName == null)
            throw new RequiredParameterError('sectionName', 'SaveSection')

        def text = MapUtils.ToJson(data)
        def json = new JsonSlurper()
        data = MapUtils.Lazy2HashMap(json.parseText(text) as Map)

        fileName = FileUtils.AddExtension(fileName, 'store')
        FileUtils.ValidFilePath(fileName)

        def store = OpenStore(fileName, secretKey, false)
        try {
            MVMap<String, HashMap<String, Object>> map = store.openMap(sectionName)
            map.putAll(data)
            store.commit()
            store.compactFile(1000)
        }
        catch (Exception ignored) {
            store.rollback()
        }
        finally {
            store.closeImmediately()
        }
    }
}