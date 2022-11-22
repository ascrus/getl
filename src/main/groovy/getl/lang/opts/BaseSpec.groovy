//file:noinspection GrMethodMayBeStatic
package getl.lang.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.utils.CloneUtils
import getl.utils.MapUtils
import groovy.transform.Synchronized

/**
 * Base options class
 * @author Alexsey Konstantinov
 *
 */
class BaseSpec implements Cloneable {
    /** Create options instance */
    BaseSpec(Object owner) {
        _owner = owner
        _params = new HashMap<String, Object>()
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
                this.importParams(importParams)
            }
        }
        else {
            _params = new HashMap<String, Object>()
            initSpec()
        }
    }

    /**
     * Import parameters to current object
     * @param importParams imported parameters
     * @return current object
     */
    BaseSpec importParams(Map<String, Object> importParams, Boolean useExternalParams = false) {
        if (!useExternalParams) {
            _params = new HashMap<String, Object>()
            initSpec()
            importFromMap(importParams)
        }
        else {
            _params = importParams
        }
        return this
    }

    /** Owner */
    protected Object _owner
    /** Owner */
    @JsonIgnore
    Object getOwnerObject() { _owner }
    /** Owner */
    void setOwnerObject(Object value) { _owner = value }

    /** Init options after create object */
    protected void initSpec() { }

    /**
     * Detect delegate object for closure code
     * @param obj analyzed object
     * @param ignore options and return their owner
     * @return found object
     */
    static Object DetectClosureDelegate(Object obj, Boolean ignoreOpts = false) {
        Getl.DetectClosureDelegate(obj, ignoreOpts)
    }

    /** Run closure for this object */
    void runClosure(Closure cl) {
        if (cl == null)
            return

        try {
            this.tap(cl)
        }
        finally {
            this.pullAllOptions(false)
        }
    }

    /** Run closure for specified object */
    @SuppressWarnings('GrMethodMayBeStatic')
    void runClosure(Object parent, Closure cl) {
        if (cl == null)
            return

        parent.tap(cl)
    }

    private Map<String, Object> _params
    private final Object synchParams = new Object()

    /** Object parameters */
    @JsonIgnore
    @Synchronized('synchParams')
    Map<String, Object> getParams() { _params }

    /**
     * Save value parameter
     * @param key parameter name
     * @param value parameter value
     */
    @Synchronized('synchParams')
    protected void saveParamValue(String key, Object value) {
        if (value != null)
            _params.put(key, value)
        else
            _params.remove(key)
    }

    /** Detected ignore key map from import */
    protected List<String> ignoreImportKeys(Map<String, Object> importParams) { [] as List<String> }

    /** Import options from map */
    void importFromMap(Map<String, Object> importParams) {
        if (importParams == null)
            throw new ExceptionGETL('#params.required', [param: 'importParams', detail: 'importFromMap'])

        def c = MapUtils.Copy(importParams, ignoreImportKeys(importParams)) as Map<String, Object>
        MapUtils.MergeMap(params, c, true, true)
    }

    /** Clear all options */
    void clearOptions() {
        params.clear()
        initSpec()
    }

    /** Options stack */
    private Stack<Map<String, Object>> _savedOptions

    /**
     * Save current options to stack
     * @param resetToDefault reset options to default after saving (default false)
     * @param cloneChildrenObject clone childs objects when saving (default false)
     */
    @Synchronized('synchParams')
    void pushOptions(Boolean resetToDefault = false, Boolean cloneChildrenObject = false) {
        _savedOptions.push(CloneUtils.CloneMap(_params, cloneChildrenObject) as Map<String, Object>)
        if (resetToDefault)
            clearOptions()
    }

    /**
     * Restore last saved options from stack
     * @param throwIfNotExist generate an error if options were not saved (default true)
     */
    @Synchronized('synchParams')
    void pullOptions(Boolean throwIfNotExist = true) {
        if (_savedOptions.isEmpty()) {
            if (!throwIfNotExist)
                return

            throw new ExceptionGETL('#options.non_saved', [className: getClass().name])
        }

        clearOptions()
        _params.putAll(_savedOptions.pop() as Map<String, Object>)
    }

    /**
     * Restore first saved options from stack
     * @param throwIfNotExist generate an error if options were not saved (default true)
     */
    @Synchronized('synchParams')
    void pullAllOptions(Boolean throwIfNotExist = true) {
        if (_savedOptions.isEmpty()) {
            if (!throwIfNotExist)
                return

            throw new ExceptionGETL('#options.non_saved', [className: getClass().name])
        }

        clearOptions()
        while (_savedOptions.size() > 1) { _savedOptions.pop() }
        _params.putAll(_savedOptions.pop() as Map<String, Object>)
    }

    @Override
    Object clone() {
        def clParams = CloneUtils.CloneMap(params)
        //def res = this.getClass().newInstance(this.ownerObject, false, clParams) as BaseSpec

        def constr = this.getClass().getConstructor([Object, Boolean, Map].toArray([] as Class[]))
        def res
        try {
            res = constr.newInstance(this.ownerObject, false, clParams) as BaseSpec
        }
        catch (Exception e) {
            println this.getClass()
            throw e
        }

        return res
    }
}