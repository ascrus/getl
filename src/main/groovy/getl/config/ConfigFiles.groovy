package getl.config

import getl.exception.ExceptionGETL
import getl.utils.*
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper


/**
 * Configuration manager class
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings("DuplicatedCode")
class ConfigFiles extends ConfigManager {
    @Override
    Boolean getEvalVars() { true }

    @SuppressWarnings("DuplicatedCode")
    @Override
    void init(Map<String, Object> initParams) {
        if (initParams?.config == null) return
        Map config = initParams.config as Map<String, Object>
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

	/** Path for configuration files */
	String getPath () { params.path as String }
    /** Path for configuration files */
    void setPath (String value) {
        if (value.trim() == '') throw new ExceptionGETL('The path can not have empty value')
        params.path = value?.trim()
    }

	/** Configuration file name */
	String getFileName () { params.fileName as String}
    /** Configuration file name */
    void setFileName (String value) {
        if (value.trim() == '') throw new ExceptionGETL('The file name can not have empty value')
        params.fileName = value?.trim()
    }

	/** List of configuration files */
	List<String> getFiles () { params.files as List<String> }
    /** List of configuration files */
    void setFiles (List<String> value) {
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
        this.files.addAll(value*.trim() as List<String>)
	}
	
	/** Configuration files code page */
	String getCodePage () { (params.codePage as String)?:'UTF-8' }
    /** Configuration files code page */
    void setCodePage (String value) {
        if (value.trim() == '') throw new ExceptionGETL('Code page value can not have empty value')
        params.codePage = value
    }

    /**
     * Evaluate file path to specified config file
     * @param filePath path to file
     * @param fileName  file name
     * @return full file path
     */
    static String fullConfigName (String filePath, String fileName) {
        ((filePath != null)?FileUtils.ConvertToUnixPath(filePath) + '/':'') + fileName
    }

    /** Full file path to the current config file */
    String getFullName () { fullConfigName(path, this.fileName) }

	@Override
    void loadConfig(Map<String, Object> readParams = [:]) {
        def fp = (readParams?.path as String)?:this.path
        def fn = (readParams?.fileName as String)?:this.fileName
        def fl = (readParams?.files as List<String>)?:this.files
        def cp = (readParams?.codePage as String)?:this.codePage

        Map<String, Object> data = null
		if (fn != null) {
            def rp = FileUtils.RelativePathFromFile(fn)
            if (rp == '.') {
                rp = fp
            }
            else {
                fn = FileUtils.FileName(fn)
            }
            def ff = new File(fullConfigName(rp, fn))
			data = LoadConfigFile(ff, cp)
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
    			data = LoadConfigFile(ff, cp)
                Config.MergeConfig(data)
			}
		}
	}
	
	/**
	 * Load configuration from file	
	 * @param file file for loading
	 * @param codePage encoding page
     * @return config content
	 */
	static Map<String, Object> LoadConfigFile (File file, String codePage) {
		if (!file.exists()) throw new ExceptionGETL("Config file \"$file\" not found")
		Logs.Config("Load config file \"${file.canonicalPath}\"")
        def data = null

        def json = new JsonSlurper()
        def reader = file.newReader(codePage)
		try {
            data = json.parse(reader)
		}
		catch (Exception e) {
            Logs.Severe("Invalid json text in file \"$file\", error: ${e.message}")
			throw e
		}
        finally {
            reader.close()
        }

        return data as Map<String, Object>
	}

    @SuppressWarnings("DuplicatedCode")
    @Override
    void saveConfig (Map<String, Object> content, Map<String, Object> saveParams = [:]) {
        def fp = (saveParams?.path as String)?:this.path
        def fn = (saveParams?.fileName as String)?:this.fileName
        def cp = (saveParams?.codePage as String)?:this.codePage

        if (fn == null) throw new ExceptionGETL('Required parameter "fileName"')

        def rp = FileUtils.RelativePathFromFile(fn)
        if (rp == '.') {
            rp = fp
        }
        else {
            fn = FileUtils.FileName(fn)
        }
        def ff = new File(fullConfigName(rp, fn))

        SaveConfigFile(content, ff, cp)
    }

    /**
     * Save config to file
     * @param data content
     * @param file config file
     * @param codePage encoding page
     */
	static void SaveConfigFile (Map<String, Object> data, File file, String codePage) {
        JsonBuilder b = new JsonBuilder()
        try {
            b.call(data)
        }
        catch (Exception e) {
            Logs.Severe("Error save configuration to file \"$file\", error: ${e.message}")
            throw e
        }

        def writer = file.newWriter(codePage)
        try {
            writer.println(b.toPrettyString())
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
}