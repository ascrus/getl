package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionGETL
import getl.exception.ExceptionModel
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.utils.DateUtils
import getl.utils.MapUtils
import getl.utils.Path
import getl.utils.StringUtils
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import java.lang.reflect.ParameterizedType
import java.sql.Timestamp
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Base class model
 * @author Alexsey Konstantinov
 */
@SuppressWarnings(['UnnecessaryQualifiedReference', 'unused'])
@InheritConstructors
class BaseModel<T extends getl.models.sub.BaseSpec> extends getl.lang.opts.BaseSpec implements GetlRepository {
    private String _dslNameObject
    @JsonIgnore
    @Override
    String getDslNameObject() { _dslNameObject }
    @Override
    void setDslNameObject(String value) { _dslNameObject = value }

    private Getl _dslCreator
    @JsonIgnore
    @Override
    Getl getDslCreator() { _dslCreator }
    @Override
    void setDslCreator(Getl value) { _dslCreator = value }

    private Date _dslRegistrationTime
    @JsonIgnore
    @Override
    Date getDslRegistrationTime() { _dslRegistrationTime }
    @Override
    void setDslRegistrationTime(Date value) { _dslRegistrationTime = value }

    @Override
    void dslCleanProps() {
        _dslNameObject = null
        _dslCreator = null
        _dslRegistrationTime = null
    }

    /** Repository model name */
    @JsonIgnore
    String getRepositoryModelName() { _dslNameObject?:'noname' }

    /** Description of model */
    String getDescription() { params.description as String }
    /** Description of model */
    void setDescription(String value) { saveParamValue('description', value) }

    @Override
    protected void initSpec() {
        super.initSpec()

        if (params.modelVars == null)
            params.modelVars = new LinkedHashMap<String, Object>()

        if (params.modelAttrs == null)
            params.modelAttrs = new LinkedHashMap<String, Object>()

        if (params.usedObjects == null)
            params.usedObjects = new CopyOnWriteArrayList<>(new ArrayList<T>())
    }

    @Override
    void importFromMap(Map<String, Object> importParams) {
        if (importParams == null)
            throw new ExceptionGETL('Required "importParams" value!')

        def objParams = importParams.usedObjects as List<Object>
        def objects = [] as List<T>
        objParams?.each { obj ->
            objects << newSpec(obj)
        }
        //params.putAll(importParams)
        MapUtils.MergeMap(params, importParams)
        params.usedObjects = objects
    }

    private final Object synchObjects = new Object()

    /** Model objects */
    @Synchronized('synchObjects')
    protected List<T> getUsedObjects() { params.usedObjects as List<T> }
    /** Model objects */
    @Synchronized('synchObjects')
    protected void setUsedObjects(List<T> value) {
        usedObjects.clear()
        if (value != null)
            usedObjects.addAll(value)
    }
    /** Model objects */
    @Synchronized('synchObjects')
    List<BaseSpec> listModelObjects() { params.usedObjects as List<BaseSpec> }

    private final Object synchVars = new Object()

    /** Model variables */
    @Synchronized('synchVars')
    Map<String, Object> getModelVars() { params.modelVars as Map<String, Object> }
    /** Model variables */
    @Synchronized('synchVars')
    void setModelVars(Map<String, Object> value) {
        modelVars.clear()
        if (value != null)
            modelVars.putAll(value)
    }
    /**
     * Return model variables
     * @param mask mask name
     * @return variable set
     */
    @Synchronized('synchVars')
    Map<String, Object> modelVariables(String mask = null) { MapUtils.FindNodes(modelVars, mask) }

    private final Object synchAttrs = new Object()

    /** Model attributes */
    @Synchronized('synchAttrs')
    Map<String, Object> getModelAttrs() { params.modelAttrs as Map<String, Object> }
    /** Model attributes */
    @Synchronized('synchAttrs')
    void setModelAttrs(Map<String, Object> value) {
        modelAttrs.clear()
        if (value != null)
            modelAttrs.putAll(value)
    }
    /** Save attribute value */
    @Synchronized('synchAttrs')
    void saveModelAttribute(String name, Object value) {
        modelAttrs.put(name, value)
    }

    /**
     * Get the value of the specified attribute
     * @param name attribute name
     * @return attribute value
     */
    @Synchronized('synchAttrs')
    Object modelAttribute(String name) { modelAttrs.get(name) }

    /**
     * Get the value of the specified attribute with parsing variables
     * @param name attribute name
     * @param extVars extend variables
     * @return parsed attribute value
     */
    @Synchronized('synchAttrs')
    String modelAttributeValue(String name, Map extVars = null) {
        def val = modelAttributes().get(name)
        if (val == null)
            return null

        String res
        try {
            res = StringUtils.EvalMacroString(val.toString(), modelVariables() + (extVars?:new HashMap()))
        }
        catch (Exception e) {
            dslCreator.logError("Error parsing the value of the \"$name\" model attribute", e)
            throw e
        }

        return res
    }

    /**
     * Get the integer value of the specified attribute with parsing variables
     * @param name attribute name
     * @param extVars extend variables
     * @return parsed integer value
     */
    @Synchronized('synchAttrs')
    Integer modelAttributeIntegerValue(String name, Map extVars = null) {
        def value = modelAttributeValue(name, extVars)
        if (value == null || value.length() == 0)
            return null

        if (!value.integer)
            throw new ExceptionDSL("Error converting the value \"$value\" of the model attribute \"$name\" into a number!")

        return value.toInteger()
    }

    /**
     * Get the long value of the specified attribute with parsing variables
     * @param name attribute name
     * @param extVars extend variables
     * @return parsed long value
     */
    @Synchronized('synchAttrs')
    Long modelAttributeLongValue(String name, Map extVars = null) {
        def value = modelAttributeValue(name, extVars)
        if (value == null || value.length() == 0)
            return null

        if (!value.bigInteger)
            throw new ExceptionDSL("Error converting the value \"$value\" of the model attribute \"$name\" into a number!")

        return value.toBigInteger().longValue()
    }

    /**
     * Get the timestamp value of the specified attribute with parsing variables
     * @param name attribute name
     * @param extVars extend variables
     * @param format parse format (default yyyy-MM-dd HH:mm:ss)
     * @return parsed timestamp value
     */
    @Synchronized('synchAttrs')
    Timestamp modelAttributeTimestampValue(String name, Map extVars = null, String format = null) {
        def value = modelAttributeValue(name, extVars)
        if (value == null || value.length() == 0)
            return null

        Timestamp res
        try {
            res = DateUtils.ParseSQLTimestamp((format?:'yyyy-MM-dd HH:mm:ss'), value, false)
        }
        catch (Exception e) {
            dslCreator.logError("Error converting the value \"$value\" of the model attribute \"$name\" into a timestamp", e)
            throw e
        }

        return res
    }

    /**
     * Return model attributes
     * @param mask mask name
     * @return attribute set
     */
    @Synchronized('synchAttrs')
    Map<String, Object> modelAttributes(String mask = null) { MapUtils.FindNodes(modelAttrs, mask) }
    /**
     * Return model sub attributes
     * @param topName attribute group name for which all subordinate attributes should be returned
     * @return attribute set
     */
    @Synchronized('synchAttrs')
    Map<String, Object> modelSubAttributes(String topName) { MapUtils.FindSubNodes(modelAttrs, topName) }

    private final Object synchStoryDataset = new Object()

    /** Name dataset of mapping processing history */
    @Synchronized('synchStoryDataset')
    protected String getStoryDatasetName() { params.storyDatasetName as String }
    /** Name dataset of mapping processing history */
    @Synchronized('synchStoryDataset')
    protected void setStoryDatasetName(String value) { useStoryDatasetName(value) }
    /** Dataset of mapping processing history */
    @Synchronized('synchStoryDataset')
    protected Dataset getStoryDataset() {
        if (storyDatasetName == null)
            return null

        checkGetlInstance()
        def dsName = modelStoryDatasetName()
        Dataset res
        try {
            res = _dslCreator.dataset(dsName)
        }
        catch (ExceptionDSL e) {
            _dslCreator.logError("Model \"$this\" has invalid story dataset \"$dsName\"", e)
            throw e
        }
    }
    /** Dataset of mapping processing history */
    @Synchronized('synchStoryDataset')
    protected void setStoryDataset(Dataset value) { useStoryDataset(value) }
    /** Dataset of mapping processing history */
    String modelStoryDatasetName() { StringUtils.EvalMacroString(storyDatasetName, modelVars, false) }

    /** Use specified dataset name of mapping processing history */
    @Synchronized('synchStoryDataset')
    protected void useStoryDatasetName(String datasetName) {
        checkGetlInstance()
        saveParamValue('storyDatasetName', datasetName)
    }
    /** Use specified dataset of mapping processing history */
    @Synchronized('synchStoryDataset')
    protected void useStoryDataset(Dataset dataset) {
        if (dataset != null && dataset.dslNameObject == null)
            throw new ExceptionModel('Dataset not registered in Getl repository!')

        saveParamValue('storyDatasetName', dataset?.dslNameObject)
    }

    /** Create new instance model object */
    protected T newSpec(Object... args) {
        def genClass = this.getClass()
        while (!(genClass.genericSuperclass instanceof ParameterizedType)) {
            genClass = genClass.superclass
            if (!BaseModel.isAssignableFrom(genClass))
                throw new ExceptionDSL("Can't find super class model for class \"${this.getClass()}\"!")
        }
        def modelSpecClass = (genClass.genericSuperclass as ParameterizedType).actualTypeArguments[0] as Class<T>
        def param = [this] as List<Object>
        if (args != null) param.addAll(args.toList())
        def res = modelSpecClass.newInstance(param.toArray(String[])) as T
        (params.usedObjects as List<T>).add(res)
        return res
    }

    /**
     * Check model parameters
     * @param validObjects check parameters of model objects
     */
    @Synchronized('synchObjects')
    void checkModel(Boolean checkObjects = true) {
        if (checkObjects)
            usedObjects.each { obj -> checkObject(obj) }
    }

    /** Check object parameter */
    @Synchronized('synchObjects')
    void checkObject(getl.models.sub.BaseSpec object) { }

    /**
     * Check attribute naming and generate an unknown error for used objects
     * @param allowAttrs list of allowed attribute names
     * @param checkObjects check attributes in objects
     */
    @Synchronized('synchAttrs')
    void checkAttrs(List<String> allowAttrs, Boolean checkObjects = true) {
        if (allowAttrs == null)
            throw new ExceptionDSL('The list of attribute names in parameter "allowAttrs" is not specified!')

        def validation = Path.Masks2Paths(allowAttrs)
        def unknownKeys = [] as List<String>
        modelAttrs.each { k, v ->
            if (!Path.MatchList(k, validation))
                unknownKeys.add(k)
        }

        if (!unknownKeys.isEmpty())
            throw new ExceptionDSL("Unknown attributes were detected in model \"$_dslNameObject\": $unknownKeys, allow attributes: $allowAttrs")

        if (checkObjects) {
            usedObjects.each { node ->
                node.checkAttrsInternal(validation, allowAttrs)
            }
        }
    }

    /** Check model owner */
    protected void checkGetlInstance() {
        if (_dslCreator == null)
            throw new ExceptionDSL('Requires a Getl instance for the model!')
    }

    @Override
    Object clone() {
        def res = super.clone() as BaseModel
        res.dslCreator = _dslCreator
        res.dslNameObject = _dslNameObject
        return res
    }

    /**
     * Return a list of model objects
     * @param includeMask list of masks to include objects
     * @param excludeMask list of masks to exclude objects
     * @return list of names of found model objects
     */
    List<String> findModelObjects(List<String> includeMask = null, List<String> excludeMask = null) {
        def res = [] as List<String>

        if (includeMask?.isEmpty())
            includeMask = null
        if (excludeMask?.isEmpty())
            excludeMask = null

        def includePath = Path.Masks2Paths(includeMask)
        def excludePath = Path.Masks2Paths(excludeMask)

        usedObjects.each { obj ->
            def objName = obj.objectNameInModel()
            if (objName != null) {
                if (includePath == null && excludePath == null)
                    res << objName
                else if (includePath != null) {
                    if (Path.MatchList(objName, includePath))
                        if (excludePath == null || !Path.MatchList(objName, excludePath))
                            res << objName
                } else if (excludePath != null && !Path.MatchList(objName, excludePath))
                    res << objName
            }
        }

        return res
    }

    /** Return model object by name */
    T objectByName(String name) {
        return usedObjects.find { obj -> obj.objectNameInModel() == name }
    }

    @Override
    String toString() { "Model \"${dslNameObject?:'unnamed'}\"" }
}