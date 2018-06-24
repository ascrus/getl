/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2018  Alexsey Konstantonov (ASCRUS)

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
class ConfigFiles extends ConfigManager {
    @Override
    public void init(Map<String, Object> initParams) {
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

	/**
	 * Path for configuration files
	 */
	public String getPath () { params.path as String }

    /**
     * Set path for configuration files
     * @param value
     */
	public void setPath (String value) {
        if (value.trim() == '') throw new ExceptionGETL('The path can not have empty value')
        params.path = value?.trim()
    }

	/**
	 * Configuration file name
	 */
	public String getFileName () { params.fileName as String}

    /**
     * Set configuration file name
     * @param value
     */
	public void setFileName (String value) {
        if (value.trim() == '') throw new ExceptionGETL('The file name can not have empty value')
        params.fileName = value?.trim()
    }

	/**
	 * List of configuration files
	 */
	public List<String> getFiles () { params.files as List<String>}

    /**
     * Set list of configuration files
     * @param value
     */
	public void setFiles (List<String> value) {
        value.each {
            if (it == null || it.trim() == '') {
                throw new ExceptionGETL('The file name can not have empty value')
            }
        }

        List<String>  f = files
        if (f == null) {
            f = new ArrayList<String>()
            params.files = f
        }
        this.files.clear()
        this.files.addAll(value*.trim())
	}
	
	/**
	 * Configuration files code page
	 * @return
	 */
	public String getCodePage () { (params.codePage as String)?:'UTF-8' }

    /**
     * Set configuration files code page
     * @param value
     */
	public void setCodePage (String value) {
        if (value.trim() == '') throw new ExceptionGETL('Code page value can not have empty value')
        params.codePage = value
    }

    /**
     * Evaluate file path for specified configuration file
     * @param value
     * @return
     */
    public static String fullConfigName (String pathFile, String value) { ((pathFile != null)?FileUtils.ConvertToUnixPath(pathFile) + "/":"") + value }

    /**
     * Return file path for current configuration file
     * @return
     */
    public String getFullName () { fullConfigName(path, fileName) }

	@Override
	public void loadConfig(Map<String, Object> readParams = [:]) {
        def fp = (readParams?.path as String)?:this.path
        def fn = (readParams?.fileName as String)?:this.fileName
        def fl = (readParams?.files as List<String>)?:this.files
        def cp = (readParams?.codePage as String)?:this.codePage

        Map<String, Object> data = null
		if (fn != null) {
            def ff = new File(fullConfigName(fp, fn))
			data = LoadConfigFile(ff, cp)
            Config.MergeConfig(data)
		}
		
		if (fl != null) {
			fl.each { String name ->
                def ff = new File(fullConfigName(fp, name))
    			data = LoadConfigFile(ff, cp)
                Config.MergeConfig(data)
			}
		}
	}
	
	/**
	 * Load configuration from file	
	 * @param file
	 * @param codePage
	 */
	public static Map<String, Object> LoadConfigFile (File file, String codePage) {
		if (!file.exists()) throw new ExceptionGETL("Config file \"$file\" not found")
		Logs.Config("Load config file \"${file.absolutePath}\"")
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

    @Override
    public void saveConfig (Map<String, Object> content, Map<String, Object> saveParams = [:]) {
        def fp = (saveParams?.path as String)?:this.path
        def fn = (saveParams?.fileName as String)?:this.fileName
        def cp = (saveParams?.codePage as String)?:this.codePage

        if (fn == null) throw new ExceptionGETL('Required parameter "fileName"')
        SaveConfigFile(content, new File(fullConfigName(fp, fn)), cp)
    }

    /**
     * Save config to file
     * @param data
     * @param file
     * @param codePage
     */
	public static void SaveConfigFile (Map<String, Object> data, File file, String codePage) {
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