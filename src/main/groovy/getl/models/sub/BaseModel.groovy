package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionDSL
import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.utils.MapUtils
import groovy.transform.InheritConstructors

import java.lang.reflect.ParameterizedType
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

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
    void setDescription(String value) { saveParamValue('description', value) }

    @Override
    protected void initSpec() {
        super.initSpec()

        if (params.modelVars == null)
            params.modelVars = new ConcurrentHashMap<String, Object>()

        if (params.modelAttrs == null)
            params.modelAttrs = new ConcurrentHashMap<String, Object>()

        if (params.usedObjects == null)
            params.usedObjects = new CopyOnWriteArrayList<>(new ArrayList<T>())
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
    @SuppressWarnings('unused')
    void setModelVars(Map<String, Object> value) {
        modelVars.clear()
        if (value != null)
            modelVars.putAll(value)
    }

    /** Model attributes */
    Map<String, Object> getModelAttrs() { params.modelAttrs as Map<String, Object> }
    /** Model attributes */
    void setModelAttrs(Map<String, Object> value) {
        modelAttrs.clear()
        if (value != null)
            modelAttrs.putAll(value)
    }
    /**
     * Get the value of the specified attribute
     * @param name attribute name
     * @return attribute value
     */
    Object modelAttribute(String name) { modelAttrs.get(name) }

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

    /**
     * Check attribute naming and generate an unknown error for used objects
     * @param allowAttrs list of allowed attribute names
     */
    void checkAttrs(List<String> allowAttrs) {
        if (allowAttrs == null)
            throw new ExceptionDSL('The list of attribute names in parameter "allowAttrs" is not specified!')

        def unknownKeys = MapUtils.Unknown(modelAttrs, allowAttrs)
        if (!unknownKeys.isEmpty())
            throw new ExceptionDSL("Unknown attributes were detected in model \"$dslNameObject\": $unknownKeys, allow attributes: $allowAttrs")

        usedObjects.each { node ->
            node.checkAttrs(allowAttrs)
        }
    }
}