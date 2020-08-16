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
package getl.models.sub

import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.models.opts.BaseSpec
import getl.utils.MapUtils
import groovy.transform.InheritConstructors

import java.lang.reflect.ParameterizedType

/**
 * Base class model
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class BaseModel<T extends BaseSpec> extends getl.lang.opts.BaseSpec implements GetlRepository {
    private String dslNameObject
    @Override
    String getDslNameObject() { dslNameObject }
    @Override
    void setDslNameObject(String value) { dslNameObject = value }

    /** Repository model name */
    String getRepositoryModelName() { dslNameObject?:'noname' }

    private Getl dslCreator

    Getl getDslCreator() { dslCreator }
    void setDslCreator(Getl value) { dslCreator = value }

    @Override
    void dslCleanProps() {
        dslNameObject = null
        dslCreator = null
    }

    /** Description of model */
    String getDescription() { params.description as String }
    /** Description of model */
    void setDescription(String value) { params.description = value }

    @Override
    protected void initSpec() {
        super.initSpec()

        if (params.modelVars == null)
            params.modelVars = [:] as Map<String, Object>

        if (params.usedObjects == null)
            params.usedObjects = [] as List<T>
    }

    @Override
    void importFromMap(Map<String, Object> importParams) {
        if (importParams == null)
            throw new ExceptionGETL('Required "importParams" value!')

        def objparams = importParams.usedObjects as List<Map>
        def objects = [] as List<T>
        objparams?.each { obj ->
            objects << newSpec(obj)
        }
        params.putAll(importParams)
        params.usedObjects = objects
    }

    /** Model objects */
    protected List<T> getUsedObjects() { params.usedObjects as List<T> }

    /** Model variables */
    Map<String, Object> getModelVars() { params.modelVars as Map<String, Object> }
    /** Model variables */
    void setModelVars(Map<String, Object> value) {
        modelVars.clear()
        if (value != null)
            modelVars.putAll(value)
    }

    /** Create new instance model object */
    protected T newSpec(Object... args) {
        def modelClass = (this.getClass().genericSuperclass as ParameterizedType).actualTypeArguments[0] as Class<T>
        def param = [this] as List<Object>
        if (args != null) param.addAll(args.toList())
        def res = modelClass.newInstance(param.toArray(String[])) as T
        usedObjects << res
        return res
    }

    /**
     * Check model parameters
     * @param validObjects check parameters of model objects
     */
    void checkModel(boolean checkObjects = true) {
        if (checkObjects)
            usedObjects.each { obj -> checkObject(obj) }
    }

    /** Check object parameter */
    void checkObject(BaseSpec object) { }
}