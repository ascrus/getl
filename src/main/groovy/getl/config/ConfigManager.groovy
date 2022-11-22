package getl.config

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.RequiredParameterError
import getl.lang.Getl
import getl.proc.Job
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.MapUtils
import groovy.transform.Synchronized

/**
 * Configuration manager class
 * @author Alexsey Konstantinov
 *
 */
abstract class ConfigManager {
    ConfigManager(Getl owner = null) {
        dslCreator = owner
    }

    ConfigManager(Map<String, Object> importParams, Getl owner = null) {
        dslCreator = owner
        params.putAll(importParams)
    }

    /** Getl instance */
    protected Getl dslCreator

    /** Current logger */
    @JsonIgnore
    Logs getLogger() { (dslCreator?.logging?.manager != null)?dslCreator.logging.manager:Logs.global }

    /** Evaluate variables where load configuration */
    private Boolean evalVars
    /** Evaluate variables where load configuration */
    Boolean getEvalVars() { evalVars }
    /** Evaluate variables where load configuration */
    void setEvalVars(Boolean value) {
        if (value == null)
            throw new RequiredParameterError('evalVars')
        evalVars = value
    }

    /** Configuration content */
    final Map<String, Object> content = [vars: new HashMap<String, Object>()]
    /** Configuration content */
    Map<String, Object> getContent() { content }
    /** Configuration content */
    @Synchronized
    void setContent(Map<String, Object> value) {
        content.clear()
        if (value != null)
            content.putAll(value)
    }

    /** Parameters of configuration */
    final Map<String, Object> params = new HashMap<String, Object>()
    /** Parameters of configuration */
    Map<String, Object> getParams() { params }
    /** Parameters of configuration */
    @Synchronized
    void setParams(Map<String, Object> value) {
        params.clear()
        if (value != null)
            params.putAll(value)
    }

    /**	Variables */
    Map<String, Object> getVars() { content.vars as Map<String, Object> }
    /**	Variables */
    @Synchronized
    void setVars(Map value) {
        if (value == null)
            throw new RequiredParameterError('vars')

        def v = content.vars as Map<String, Object>
        if (v == null) {
            v = new HashMap<String, Object>()
            content.put('vars', v)
        }
        v.putAll(value)
    }

    /** List of initialization object code on load config */
    final List<Closure> init = [] as List<Closure>
    /** List of initialization object code on load config */
    List<Closure> getInit() { init }
    /** List of initialization object code on load config */
    @Synchronized
    void setInit(List<Closure> value) {
        init.clear()
        if (value != null)
            init.addAll(value)
    }

    /**
     * Registration object code closure on load of the configuration files
     * @param code
     */
    @Synchronized
    void registerOnInit(Closure code) {
        if (init.find { it == code } == null) {
            init << code
        }
    }

    /**
     * Unregister object code closure on load of the configuration files
     * @param code initialization code
     */
    @Synchronized
    void unregisterOnInit(Closure code) {
        if (init.find { it == code } != null) {
            init.remove(code)
        }
    }

    /** Clear configurations */
    @Synchronized
    void clearConfig () {
        init()
    }

    /**
     * Load content configuration from file
     * @param readParams
     */
    abstract protected void loadContent(Map<String, Object> readParams = new HashMap<String, Object>())

    /**
     * Load content configuration from file
     * @param readParams
     */
    @Synchronized
    void loadConfig(Map<String, Object> readParams = new HashMap<String, Object>()) {
        if (readParams.files != null) {
            def l = [] as List<String>
            (readParams.files as List).each {
                if (it instanceof String)
                    l << FileUtils.TransformFilePath(it as String, dslCreator)
                else
                    l << it.toString()
            }
            readParams.files = l
        }

        if (readParams.fileName != null && readParams.fileName instanceof String) {
            readParams.fileName = FileUtils.TransformFilePath(readParams.fileName as String, dslCreator)
        }

        loadContent(readParams)
        initEvents()
    }

    /**
     * Save specified configuration to file
     * @param content saved configuration
     * @param saveParams save options
     */
    abstract void saveConfig(Map<String, Object> content, Map<String, Object> saveParams = new HashMap<String, Object>())

    /**
     * Save current configuration to file
     * @param saveParams save options
     */
    void saveContent(Map<String, Object> saveParams = new HashMap<String, Object>()) {
        saveConfig(content, saveParams)
    }

    /**
     * Init manager
     * @param initParams
     */
    @Synchronized
    void init(Map<String, Object> initParams = null) {
        content.clear()
        content.put('vars', new HashMap<String, Object>())
        evalVars = false
    }

    /** Run every event subscriber after load config files */
    @Synchronized
    void initEvents() {
        init.each { doInit ->
            doInit()
        }
    }

    /**
     * Find config section by section name
     * @param section section name (syntax: section[.section[.section.[...]]])
     * @return found section
     */
    Map findSection(String section) {
        if (section == null)
            return null

        return MapUtils.FindSection(content, section)?:new HashMap()
    }

    /**
     * Validation contains section
     * @param section section name (syntax: section[.section[.section.[...]]])
     * @return section search result
     */
    Boolean containsSection(String section) { MapUtils.ContainsSection(content, section) }

    /**
     * Set the value for the specified content element
     * @param name element name (syntax: section[.section[.section.[...]]].name)
     * @param value element value
     */
    @Synchronized
    void setContentValue(String name, value) {
        MapUtils.SetValue(content, name, value)
    }

    /**
     * Check configuration content is empty
     * @return result checking
     */
    Boolean isEmpty() { (content.size() == 1 && vars.isEmpty()) }

    /**
     * Merge specified configuration to current content
     * @param data merged configuration
     */
    @Synchronized
    void mergeConfig(Map data) {
        if (data == null)
            throw new RequiredParameterError('data', 'mergeConfig')

        Map<String, Object> currentVars = vars
        if (data.vars != null)
            MapUtils.MergeMap(currentVars, data.vars as Map<String, Object>)
        if (!(Job.jobArgs.vars as Map)?.isEmpty())
            MapUtils.MergeMap(currentVars, Job.jobArgs.vars as Map<String, Object>)

        if (evalVars && !currentVars.isEmpty() && !data.isEmpty()) {
            try {
                data = MapUtils.EvalMacroValues(data, currentVars, false)
            }
            catch (MissingPropertyException e) {
                logger.severe("${e.message}, available variables: ${currentVars.keySet().toList()}")
                throw e
            }
        }

        MapUtils.MergeMap(content, data)
    }
}