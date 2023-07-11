package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.FileDataset
import getl.exception.DatasetError
import getl.exception.DslError
import getl.exception.ExceptionGETL
import getl.exception.ModelError
import getl.exception.RequiredParameterError
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.jdbc.ViewDataset
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.lang.sub.ObjectTags
import getl.lang.sub.ParseObjectName
import getl.utils.DateUtils
import getl.utils.MapUtils
import getl.utils.Path
import getl.utils.StringUtils
import groovy.transform.CompileStatic
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
@CompileStatic
@InheritConstructors
class BaseModel<T extends getl.models.sub.BaseSpec> extends getl.lang.opts.BaseSpec implements GetlRepository, ObjectTags {
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

    private Date _dslSaveTime
    @JsonIgnore
    @Override
    Date getDslSaveTime() { _dslSaveTime as Date }
    @Override
    void setDslSaveTime(Date value) { _dslSaveTime = value }

    @Override
    void dslCleanProps() {
        _dslNameObject = null
        _dslCreator = null
        _dslRegistrationTime = null
        _dslSaveTime = null
    }

    /** Repository model name */
    @JsonIgnore
    String getRepositoryModelName() { _dslNameObject?:'noname' }

    /** Description of model */
    @Override
    String getDescription() { params.description as String }
    /** Description of model */
    @Override
    void setDescription(String value) { saveParamValue('description', value) }

    @Override
    protected void initSpec() {
        super.initSpec()

        params.objectTags = new ArrayList<String>()

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
            throw new RequiredParameterError(this, 'importParams')

        def objParams = importParams.usedObjects as List
        def objects = [] as List<T>
        objParams?.each { obj ->
            if (obj instanceof BaseSpec)
                objects.add(createSpec((obj as BaseSpec).params))
            else if (obj instanceof Map)
                objects.add(createSpec(obj as Map))
            else
                throw new ModelError(this, '#dsl.model.invalid_used_object_class', [class: obj.getClass().toString()])
        }
        MapUtils.MergeMap(params, importParams)
        params.usedObjects = objects
    }

    /** Synchronize field */
    protected final Object synchObjects = new Object()

    /** Close all resource by model */
    @Synchronized('synchObjects')
    void doneModel() { }

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

    /** Object tags */
    @Override
    List<String> getObjectTags() { params.objectTags as List<String> }
    /** Object tags */
    @Override
    void setObjectTags(List<String> value) {
        objectTags.clear()
        if (value != null)
            objectTags.addAll(value)
    }

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
            throw new ModelError(this, '#dsl.model.invalid_attribute_type', [value: value, attr: name, type: 'number'])

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
            throw new ModelError(this, '#dsl.model.invalid_attribute_type', [value: value, attr: name, type: 'number'])

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
    @SuppressWarnings('DuplicatedCode')
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
        catch (ExceptionGETL e) {
            _dslCreator.logError("Model \"$this\" has invalid story dataset \"$dsName\"", e)
            throw e
        }

        return res
    }
    /** Dataset of mapping processing history */
    @Synchronized('synchStoryDataset')
    protected void setStoryDataset(Dataset value) { useStoryDataset(value) }
    /** Dataset of mapping processing history */
    String modelStoryDatasetName() {
        def extVars = (dslCreator?.scriptExtendedVars)?:([:] as Map<String, Object>)
        return StringUtils.EvalMacroString(storyDatasetName, modelVars + extVars, false)
    }
    /** Story dataset as TableDataset */
    @JsonIgnore
    protected TableDataset getStoryTable() { storyDataset as TableDataset }

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
            throw new ModelError(this, '#dsl.model.dataset_not_registered', [dataset: dataset])

        saveParamValue('storyDatasetName', dataset?.dslNameObject)
    }

    protected Class<T> usedModelClass() {
        def genClass = this.getClass()
        while (!(genClass.genericSuperclass instanceof ParameterizedType)) {
            genClass = genClass.superclass
            if (!BaseModel.isAssignableFrom(genClass))
                throw new ModelError(this, "#dsl.model.super_class_not_found")
        }
        return (genClass.genericSuperclass as ParameterizedType).actualTypeArguments[0] as Class<T>
    }

    /** Create new instance model object */
    protected T createSpec(Map objectParams) {
        def modelSpecClass = usedModelClass()
        def constr = modelSpecClass.getConstructor([BaseModel, Boolean, Map].toArray([] as Class[]))
        def res = constr.newInstance(this, false, objectParams) as T

        return res
    }

    /** Add model object to list of model objects */
    protected T addSpec(T spec) {
        (params.usedObjects as List<T>).add(spec)
        return spec
    }

    /**
     * Check model base and objects parameters
     * @param checkObjects check model object parameters
     * @param checkNodeCode additional validation code for model objects
     */
    @Synchronized('synchObjects')
    protected void checkModel(Boolean checkObjects = true, Closure checkNodeCode = null) {
        checkGetlInstance()

        checkDataset(modelStoryDatasetName())

        if (checkObjects) {
            usedObjects.each { obj ->
                checkObject(obj)
                if (checkNodeCode != null)
                    checkNodeCode.call(obj)
            }
        }
    }

    /** Check model object parameter */
    @Synchronized('synchObjects')
    protected void checkObject(getl.models.sub.BaseSpec object) { }

    /** Check dataset parameters */
    protected void checkDataset(String dsName) {
        if (dsName == null)
            return

        def ds = dslCreator.findDataset(dsName)
        if (ds == null)
            throw new ModelError(this, '#dsl.model.dataset_not_found', [dataset: dsName])

        checkDataset(ds)
    }

    /** Check dataset parameters */
    protected void checkDataset(Dataset ds) {
        if (ds == null)
            return

        if (dslCreator.findDataset(ds) == null)
            throw new ModelError(this, '#dsl.model.dataset_not_registered', [dataset: ds])

        if (ds.connection == null)
            throw new DatasetError(ds, '#dataset.non_connection')

        if (ds instanceof TableDataset) {
            def jdbcTable = ds as TableDataset
            if (jdbcTable.tableName == null)
                throw new DatasetError(ds, '#jdbc.table.non_table_name')
        }
        else if (ds instanceof ViewDataset) {
            def viewTable = ds as ViewDataset
            if (viewTable.tableName == null)
                throw new DatasetError(ds, '#jdbc.table.non_table_name')
        }
        else if (ds instanceof QueryDataset) {
            def queryTable = ds as QueryDataset
            if (queryTable.query == null && queryTable.scriptFilePath == null)
                throw new DatasetError(ds, '#jdbc.query.non_script')
        }
        else if (ds instanceof FileDataset) {
            def fileTable = ds as FileDataset
            if (fileTable.fileName == null)
                throw new DatasetError(ds, '#dataset.non_filename')
        }
    }

    /**
     * Check dataset model
     * @param ds checking dataset
     * @param connectionName the name of the connection used for the dataset
     */
    @Synchronized('synchObjects')
    protected void checkModelDataset(Dataset ds, String connectionName = null) {
        if (ds == null)
            throw new RequiredParameterError(this, 'ds')

        if (ds.connection.dslNameObject != connectionName)
            throw new ModelError(this, "#dsl.model.invalid_connection_for_dataset", [dataset: ds])
    }

    /**
     * Check attribute naming and generate an unknown error for used objects
     * @param allowAttrs list of allowed attribute names
     * @param checkObjects check attributes in objects
     */
    @Synchronized('synchAttrs')
    void checkAttrs(List<String> allowAttrs, Boolean checkObjects = true) {
        if (allowAttrs == null)
            throw new RequiredParameterError(this, 'allowAttrs')

        def validation = Path.Masks2Paths(allowAttrs)
        def unknownKeys = [] as List<String>
        modelAttrs.each { k, v ->
            if (!Path.MatchList(k, validation))
                unknownKeys.add(k)
        }

        if (!unknownKeys.isEmpty())
            throw new ModelError(this, '#dsl.model.invalid_attributes', [attrs: allowAttrs, unknown_attrs: unknownKeys])

        if (checkObjects) {
            usedObjects.each { node ->
                node.checkAttrsInternal(validation, allowAttrs)
            }
        }
    }

    /** Check model owner */
    protected void checkGetlInstance() {
        if (_dslCreator == null)
            throw new DslError(this, '#dsl.owner_required')
    }

    @Override
    Object clone() {
        def res = super.clone() as BaseModel
        res.dslCreator = _dslCreator
        res.dslNameObject = _dslNameObject
        return res
    }

    /** Get model object by index */
    @JsonIgnore
    T getModelObject(Integer index) { usedObjects[index] }

    /** Count of tables in model */
    @JsonIgnore
    Integer getCountUsedModelObjects() { usedObjects.size() }

    /**
     * Find object from model by name
     * @param name object name
     * @return model object
     */
    T findModelObject(String name) {
        def objName = ParseObjectName.Parse(name).name
        return usedObjects.find { obj -> obj.objectNameInModel() == objName }
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
                    res.add(objName)
                else if (includePath != null) {
                    if (Path.MatchList(objName, includePath))
                        if (excludePath == null || !Path.MatchList(objName, excludePath))
                            res.add(objName)
                } else if (excludePath != null && !Path.MatchList(objName, excludePath))
                    res.add(objName)
            }
        }

        return res
    }

    @Override
    String toString() {
        return ((dslNameObject != null)?dslNameObject:(getClass().simpleName)) + " [${StringUtils.WithGroupSeparator(countUsedModelObjects)} objects]"
    }

    /**
     * Check the presence of a object in the model by its name
     * @param name model object name
     * @return find result
     */
    Boolean isObjectInModel(String name) {
        return findModelObject(name) != null
    }

    /** Clear incremental points for model tables */
    void clearStoryDataset(List<String> includedTables = null, List<String> excludedTables = null) {
        if (storyDatasetName == null)
            return

        if (storyDataset instanceof TableDataset) {
            (storyDataset as TableDataset).tap {
                if (exists)
                    truncate()
            }
        }
        else if (storyDataset instanceof FileDataset)
            (storyDataset as FileDataset).tap {
                if (existsFile())
                    drop()
            }
        else
            throw new ModelError(this, '#dsl.model.story_dataset_not_support_clear', [dataset: modelStoryDatasetName()])
    }
}