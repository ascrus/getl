/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

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
package getl.lang.opts

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
        initSpec()
    }

    /** Create options instance */
    BaseSpec(Object owner, Boolean useExternalParams, Map<String, Object> importParams) {
        _owner = owner
        if (importParams != null) {
            if (useExternalParams) {
                _params = importParams
                initSpec()
            } else {
                initSpec()
                importFromMap(importParams)
            }
        }
    }

    /** Options owner */
    protected Object _owner
    /** Options owner */
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
    void runClosure(Object parent, Closure cl) {
        if (cl == null) return
        parent.with(cl)
    }

    private Map<String, Object> _params = new ConcurrentHashMap<String, Object>()
    /** Object parameters */
    Map<String, Object> getParams() { _params }

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

    static protected final savedOptionsName = '__getl_saved_options__'

    /** Push current options to saved list */
    @Synchronized
    void pushOptions(boolean cloneChildrenObject = false) {
        Stack<Map<String, Object>> savedOptions = (params.get(savedOptionsName) as Stack<Map<String, Object>>)
        if (savedOptions == null) {
            savedOptions = new Stack<Map<String, Object>>()
            params.put(savedOptionsName, savedOptions)
        }
        def m = MapUtils.CleanMap(params, [savedOptionsName]) as Map<String, Object>
        def n = CloneUtils.CloneMap(m, cloneChildrenObject) as Map<String, Object>
        //noinspection GroovyAssignabilityCheck
        savedOptions.push(n)
    }

    /** Pull last saved options to current options */
    @Synchronized
    void pullOptions(boolean checkExists = true) {
        def savedOptions = (params.get(savedOptionsName) as Stack<Map<String, Object>>)
        if (savedOptions == null || savedOptions.isEmpty()) {
            if (!checkExists) return
            throw new ExceptionGETL("No saved options ${getClass().name} available!")
        }

        def n = savedOptions.pop() as Map<String, Object>
        params.clear()
        params.putAll(n)
        params.put(savedOptionsName, savedOptions)
    }
}