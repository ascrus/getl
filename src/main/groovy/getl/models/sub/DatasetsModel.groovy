//file:noinspection unused
package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.data.Field
import getl.data.sub.AttachData
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.exception.ExceptionModel
import getl.exception.ModelError
import getl.exception.RequiredParameterError
import getl.jdbc.HistoryPointManager
import getl.jdbc.TableDataset
import getl.utils.StringUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.transform.NamedVariant
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Datasets model
 * @author Alexsey Konstantinov
 */
@CompileStatic
@InheritConstructors
class DatasetsModel<T extends DatasetSpec> extends BaseModel {
    /** Repository connection name */
    protected String getModelConnectionName() { params.modelConnectionName as String }
    /** Repository connection name */
    protected void setModelConnectionName(String value) { saveParamValue('modelConnectionName', value) }
    /** Set the name of the connection */
    protected void useModelConnection(String connectionName) {
        if (connectionName == null)
            throw new RequiredParameterError(this, 'connectionName')
        dslCreator.connection(connectionName)

        saveParamValue('modelConnectionName', connectionName)
    }
    /** Used connection */
    protected Connection getModelConnection() { dslCreator.connection(modelConnectionName) }
    /** Used connection */
    protected setModelConnection(Connection value) { useModelConnection(value) }
    /** Set connection */
    protected void useModelConnection(Connection connection) {
        if (connection == null)
            throw new RequiredParameterError(this, 'connection')
        if (connection.dslNameObject == null)
            throw new ModelError(this, '#dsl.model.connection_not_registered', [connection: connection])

        saveParamValue('modelConnectionName', connection.dslNameObject)
    }

    /** Used datasets */
    protected List<T> getUsedDatasets() { usedObjects }

    /** Name dataset of processing history */
    String getStoryDatasetName() { super.storyDatasetName }
    /** Name dataset of processing history */
    void setStoryDatasetName(String value) { super.useStoryDatasetName(value) }
    /** Dataset of processing history */
    @JsonIgnore
    Dataset getStoryDataset() { super.storyDataset }
    /** Dataset of processing history */
    @JsonIgnore
    void setStoryDataset(Dataset value) { super.setStoryDataset(value) }
    /** Use specified dataset name of processing history */
    void useStoryDatasetName(String datasetName) { super.useStoryDatasetName(datasetName) }
    /** Use specified dataset of processing history */
    void useStoryDataset(Dataset dataset) { super.useStoryDataset(dataset) }

    @Override
    TableDataset getStoryTable() { super.storyTable }

    private final Object synchIncrementDataset = new Object()

    /** Dataset name for storing incremental values of model tables */
    @Synchronized('synchIncrementDataset')
    String getIncrementDatasetName() { params.incrementDatasetName as String }
    /** Dataset name for storing incremental values of model tables */
    @Synchronized('synchIncrementDataset')
    void setIncrementDatasetName(String value) { useIncrementDatasetName(value) }
    /** Dataset for storing incremental values of model tables */
    @SuppressWarnings('DuplicatedCode')
    @Synchronized('synchIncrementDataset')
    @JsonIgnore
    TableDataset getIncrementDataset() {
        if (incrementDatasetName == null)
            return null

        checkGetlInstance()
        def dsName = modelIncrementDatasetName()
        Dataset res
        try {
            res = dslCreator.jdbcTable(dsName)
        }
        catch (ExceptionGETL e) {
            dslCreator.logError("Model \"$this\" has invalid increment dataset \"$dsName\"", e)
            throw e
        }

        return res
    }
    /** Dataset for storing incremental values of model tables */
    @Synchronized('synchIncrementDataset')
    void setIncrementDataset(TableDataset value) { useIncrementDataset(value) }
    /** Dataset name for storing incremental values of model tables */
    String modelIncrementDatasetName() {
        def extVars = (dslCreator?.scriptExtendedVars)?:([:] as Map<String, Object>)
        return StringUtils.EvalMacroString(incrementDatasetName, modelVars + extVars, false)
    }

    /** Use the specified dataset to store incremental values of model tables */
    @Synchronized('synchIncrementDataset')
    void useIncrementDatasetName(String datasetName) {
        checkGetlInstance()
        saveParamValue('incrementDatasetName', datasetName)
        usedDatasets.each { node ->
            node._historyPointObject = null
        }
    }
    /** Use the specified dataset to store incremental values of model tables */
    @Synchronized('synchIncrementDataset')
    void useIncrementDataset(TableDataset dataset) {
        if (dataset != null && dataset.dslNameObject == null)
            throw new ModelError(this, '#dsl.model.dataset_not_registered', [dataset: dataset])

        saveParamValue('incrementDatasetName', dataset?.dslNameObject)
    }

    /** Return history point manager by table model */
    @Synchronized('synchIncrementDataset')
    HistoryPointManager historyPointTable(String modelTableName) {
        if (incrementDatasetName == null)
            return null

        def spec = dataset(modelTableName)
        return spec.historyPointObject
    }

    /** Create new model table */
    DatasetSpec newDataset(String tableName) {
        def modelSpecClass = usedModelClass()
        def constr = modelSpecClass.getConstructor([DatasetsModel, String].toArray([] as Class[]))
        def res = constr.newInstance(this, tableName) as T

        return res
    }

    /**
     * Use dataset in model
     * @param datasetName name dataset in repository
     * @param cl parameter description code
     */
    protected T dataset(String datasetName, Closure cl = null) {
        if (datasetName == null) {
            def owner = DetectClosureDelegate(cl)
            if (owner instanceof Dataset)
                datasetName = (owner as Dataset).dslNameObject
        }

        if (datasetName == null)
            throw new RequiredParameterError(this, 'datasetName')

        checkModel(false)

        def table = dslCreator.dataset(datasetName)
        def dslDatasetName = table.dslNameObject

        def parent = (usedDatasets.find { t -> t.datasetName == dslDatasetName })
        if (parent == null) {
            if (cl != null)
                parent = addSpec(newDataset(dslDatasetName)) as T
            else
                throw new ModelError(this, '#dsl.model.table_not_found', [table: datasetName])
        }

        parent.runClosure(cl)

        return parent
    }

    /**
     * Use dataset in model
     * @param dataset dataset in repository
     * @param cl parameter description code
     */
    protected T useDataset(Dataset dataset, Closure cl = null) {
        if (dataset.dslNameObject == null)
            throw new ModelError(this, '#dsl.model.dataset_not_registered', [dataset: dataset])
        this.dataset(dataset.dslNameObject, cl)
    }

    /**
     * Add datasets to the model using the specified mask
     * @param mask dataset search mask
     * @param code parameter description code
     */
    @NamedVariant
    protected void addDatasets(String mask,
                               @ClosureParams(value = SimpleType, options = ['java.lang.String'])
                                       Closure<Boolean> filter = null,
                               Closure code = null) {
        dslCreator.processDatasets(mask) {datasetName ->
            if (filter == null || filter.call(datasetName))
                dataset(datasetName, code)
        }
    }


    /**
     * Check model
     * @param checkObjects check model object parameters
     * @param checkNodeCode additional validation code for model objects
     */
    @Override
    protected void checkModel(Boolean checkObjects = true, Closure cl = null) {
        if (modelConnectionName == null)
            throw new ModelError(this, '#dsl.model.non_source_connection')

        super.checkModel(checkObjects, cl)

        checkDataset(modelIncrementDatasetName())
    }

    @Override
    protected void checkObject(BaseSpec obj) {
        super.checkObject(obj)
        def node = obj as DatasetSpec

        checkDataset(node.modelDataset)
        if (node.parentDatasetName == null)
            checkModelDataset(node.modelDataset)

        if (node.incrementFieldName != null) {
            if (incrementDatasetName == null)
                throw new ExceptionModel(this, '#dsl.model.datasets.non_increment_dataset', [table: node.datasetName])

            def field = node.modelDataset.fieldByName(node.incrementFieldName)
            if (field == null)
                throw new ExceptionModel(this, '#dsl.model.datasets.increment_field_not_found', [field: node.incrementFieldName, table: node.datasetName])

            if (!(field.type in [Field.integerFieldType, Field.bigintFieldType, Field.numericFieldType, Field.dateFieldType, Field.datetimeFieldType,
                                 Field.timestamp_with_timezoneFieldType]))
                throw new ExceptionModel(this, '#dsl.model.datasets.invalid_increment_field_type',
                        [field: node.incrementFieldName, type: field.type.toString(), table: node.datasetName])
        }

        if (node.partitionsDatasetName != null)
            checkDataset(node.partitionsDatasetName)

        if (node.parentDatasetName != null) {
            if (findModelObject(node.parentDatasetName) == null)
                throw new ModelError(this, '#dsl.model.datasets.non_parent_dataset', [table: node.datasetName])
            def parentDataset = node.parentDataset
            checkDataset(parentDataset)
            if (!(node.modelDataset instanceof AttachData))
                throw new ModelError(this, '#dsl.model.datasets.invalid_child_dataset', [table: node.datasetName])
            if (node.parentLinkFieldName != null) {
                if (!parentDataset.field.isEmpty() && parentDataset.fieldByName(node.parentLinkFieldName) == null)
                    throw new ModelError(this, '#dsl.model.datasets.link_field_not_found',
                            [field: node.parentLinkFieldName, table: node.datasetName, parent_table: node.parentDatasetName])
            }
        }
        else if (node.parentLinkFieldName != null)
            throw new ModelError(this, '#dsl.model.datasets.required_parent_table', [table: node.datasetName, field: node.parentLinkFieldName])
    }

    @Override
    protected void checkModelDataset(Dataset ds, String connectionName = null) {
        super.checkModelDataset(ds, connectionName?:modelConnectionName)
    }

    /** Clear incremental points for model tables */
    void clearIncrementDataset(List<String> includedTables = null, List<String> excludedTables = null) {
        if (incrementDatasetName == null)
            return

        if (!incrementDataset.exists)
            return

        def listObjects = findModelObjects(includedTables, excludedTables)
        listObjects.each { tabName ->
            def node = this.findModelObject(tabName) as DatasetSpec
            if (node.incrementFieldName != null)
                node.historyPointObject.clearValue()
        }
    }

    @Override
    void doneModel() {
        super.doneModel()
        usedDatasets.each { node ->
            if (node.datasetName != null && node.modelDataset.connection.driver.isSupport(Driver.Support.CONNECT))
                node.modelDataset.connection.connected = false

            if (node._historyPointObject != null)
                node._historyPointObject.currentJDBCConnection?.connected = false

            if (node.partitionsDatasetName != null && node.partitionsDataset.connection.driver.isSupport(Driver.Support.CONNECT))
                node.partitionsDataset.connection.connected = false
        }

        if (storyDatasetName != null && storyDataset.connection.driver.isSupport(Driver.Support.CONNECT))
            storyDataset.connection.connected = false

        if (modelConnectionName != null && modelConnection.driver.isSupport(Driver.Support.CONNECT))
            modelConnection.connected = false
    }
}