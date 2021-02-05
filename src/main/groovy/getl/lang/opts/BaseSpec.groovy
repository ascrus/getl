package getl.lang.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.utils.CloneUtils
import getl.utils.MapUtils
import groovy.transform.CompileStatic
import groovy.transform.Synchronized

import java.util.concurrent.ConcurrentHashMap

/**
 * Base options class
 * @author Alexsey Konstantinov
 *
 */
class BaseSpec {
    /** Create options instance */
    BaseSpec(Object owner) {
        _owner = owner
        _params = new ConcurrentHashMap<String, Object>()
        _savedOptions = new Stack<Map<String, Object>>()
        initSpec()
    }

    /** Create options instance */
    BaseSpec(Object owner, Boolean useExternalParams, Map<String, Object> importParams) {
        _owner = owner
        _savedOptions = new Stack<Map<String, Object>>()
        if (importParams != null) {
            if (useExternalParams) {
                _params = importParams
                initSpec()
            } else {
                _params = new ConcurrentHashMap<String, Object>()
                initSpec()
                importFromMap(importParams)
            }
        }
        else {
            _params = new ConcurrentHashMap<String, Object>()
            initSpec()
        }
    }

    /** Options owner */
    protected Object _owner
    /** Options owner */
    @JsonIgnore
    Object getOwnerObject() { _owner }

    /** Init options after create object */
    protected void initSpec() { }

    /** Detect delegate object for closure code */
    static Object DetectClosureDelegate(Object obj) { Getl.DetectClosureDelegate(obj) }

    /** Run closure for this object */
    void runClosure(Closure cl) {
        if (cl == null) return
        this.with(cl)
    }

    /** Run closure for specified object */
    @SuppressWarnings('GrMethodMayBeStatic')
    void runClosure(Object parent, Closure cl) {
        if (cl == null) return
        parent.with(cl)
    }

    private Map<String, Object> _params
    /** Object parameters */
    @JsonIgnore
    Map<String, Object> getParams() { _params }

    /**
     * Save value parameter
     * @param key parameter name
     * @param value parameter value
     */
    protected void saveParamValue(String key, Object value) {
        if (value != null)
            _params.put(key, value)
        else
            _params.remove(key)
    }

    /**
     * Detected ignore key map from import
     */
    @SuppressWarnings("GrMethodMayBeStatic")
    protected List<String> ignoreImportKeys(Map<String, Object> importParams) { [] as List<String> }

    /**
     * Import options from map
     */
    void importFromMap(Map<String, Object> importParams) {
        if (importParams == null)
            throw new ExceptionGETL('Required "importParams" value!')

        params.putAll(MapUtils.Copy(importParams, ignoreImportKeys(importParams)))
    }

    /** Clear all options */
    void clearOptions() {
        params.clear()
    }

    /** Options stack */
    private Stack<Map<String, Object>> _savedOptions

    /**
     * Save current options to stack
     * @param resetToDefault reset options to default after saving (default false)
     * @param cloneChildrenObject clone childs objects when saving (default false)
     */
    @Synchronized
    void pushOptions(Boolean resetToDefault = false, Boolean cloneChildrenObject = false) {
        _savedOptions.push(CloneUtils.CloneMap(_params, cloneChildrenObject) as Map<String, Object>)
        if (resetToDefault) clearOptions()
    }

    /**
     * Restore last saved options from stack
     * @param throwIfNotExist generate an error if options were not saved (default true)
     */
    @Synchronized
    void pullOptions(Boolean throwIfNotExist = true) {
        if (_savedOptions.isEmpty()) {
            if (!throwIfNotExist) return
            throw new ExceptionGETL("No saved options for ${getClass().name} object available!")
        }

        clearOptions()
        _params.putAll(_savedOptions.pop() as Map<String, Object>)
    }

    @Override
    Object clone() {
        /*TODO:Valid clone in threads*/
        def clParams = MapUtils.Clone(params)
        def res = this.getClass().newInstance(this.ownerObject) as BaseSpec
        res.params.putAll(clParams)
        return res
    }
}