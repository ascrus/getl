package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
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
    @JsonIgnore
    String getDslNameObject() { dslNameObject }
    @Override
    void setDslNameObject(String value) { dslNameObject = value }

    private Getl dslCreator
    @Override
    @JsonIgnore
    Getl getDslCreator() { dslCreator }
    @Override
    void setDslCreator(Getl value) { dslCreator = value }

    @Override
    void dslCleanProps() {
        dslNameObject = null
        dslCreator = null
    }

    /** Repository model name */
    @JsonIgnore
    String getRepositoryModelName() { dslNameObject?:'noname' }

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

        def objParams = importParams.usedObjects as List<Map>
        def objects = [] as List<T>
        objParams?.each { obj ->
            objects << newSpec(obj)
        }
        params.putAll(importParams)
        params.usedObjects = objects
    }

    /** Model objects */
    protected List<T> getUsedObjects() { params.usedObjects as List<T> }
    /** Model objects */
    protected void setUsedObjects(List<T> value) {
        usedObjects.clear()
        if (value != null)
            usedObjects.addAll(value)
    }

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
    void checkModel(Boolean checkObjects = true) {
        if (checkObjects)
            usedObjects.each { obj -> checkObject(obj) }
    }

    /** Check object parameter */
    void checkObject(BaseSpec object) { }
}