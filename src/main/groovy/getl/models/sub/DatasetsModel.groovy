//file:noinspection unused
package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.data.Field
import getl.data.FileDataset
import getl.data.sub.AttachData
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.jdbc.ViewDataset
import groovy.transform.InheritConstructors
import groovy.transform.NamedVariant
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Datasets model
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class DatasetsModel<T extends DatasetSpec> extends BaseModel {
    /** Repository connection name */
    protected String getModelConnectionName() { params.modelConnectionName as String }
    /** Repository connection name */
    protected void setModelConnectionName(String value) { saveParamValue('modelConnectionName', value) }
    /** Set the name of the connection */
    protected void useModelConnection(String connectionName) {
        if (connectionName == null)
            throw new ExceptionModel('Connection name required!')
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
            throw new ExceptionModel('Connection required!')
        if (connection.dslNameObject == null)
            throw new ExceptionModel('Connection not registered in repository!')

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

    /** Create new model table */
    protected DatasetSpec newDataset(String tableName) {
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
    protected T dataset(String datasetName, Closure cl) {
        if (datasetName == null) {
            def owner = DetectClosureDelegate(cl)
            if (owner instanceof Dataset)
                datasetName = (owner as Dataset).dslNameObject
        }

        if (datasetName == null)
            throw new ExceptionModel("No repository dataset name specified!")

        checkModel(false)

        def table = dslCreator.dataset(datasetName)
        def dslDatasetName = table.dslNameObject

        def parent = (usedDatasets.find { t -> t.datasetName == dslDatasetName })
        if (parent == null)
            parent = addSpec(newDataset(dslDatasetName)) as T

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
            throw new ExceptionModel("The dataset \"$dataset\" is not registered in the repository!")
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

    @Override
    void checkModel(Boolean checkObjects = true) {
        if (modelConnectionName == null)
            throw new ExceptionModel("The model connection is not specified!")

        super.checkModel(checkObjects)
    }

    @Override
    void checkObject(BaseSpec obj) {
        super.checkObject(obj)
        def node = obj as DatasetSpec

        checkDataset(node.modelDataset)
        if (node.parentDatasetName == null)
            checkModelDataset(node.modelDataset)

        def hp = (node.historyPointName != null)?dslCreator.findHistorypoint(node.historyPointName):null
        if (node.historyPointName != null) {
            if (hp == null)
                throw new ExceptionModel("Increment point manager \"${node.historyPointName}\" not found!")
            hp.checkManager()
        }
        if (node.incrementFieldName != null) {
            def field = node.modelDataset.fieldByName(node.incrementFieldName)
            if (field == null)
                throw new ExceptionModel("Increment field \"${node.incrementFieldName}\" was not found in dataset \"${node.modelDataset}\"!")
            if (hp != null) {
                switch (hp.sourceType) {
                    case hp.identitySourceType:
                        if (!(field.type in [Field.integerFieldType, Field.bigintFieldType, Field.numericFieldType]))
                            throw new ExceptionModel("Field \"${node.incrementFieldName}\" has type \"${field.type}\", which is not compatible with " +
                                    "type \"${hp.sourceType}\" of incremental manager \"$hp\" (allowed INTEGER, BIGINT and NUMERIC types)!")
                        break
                    case hp.timestampSourceType:
                        if (!(field.type in [Field.dateFieldType, Field.datetimeFieldType, Field.timestamp_with_timezoneFieldType]))
                            throw new ExceptionModel("Field \"${node.incrementFieldName}\" has type \"${field.type}\", which is not compatible with " +
                                    "type \"${hp.sourceType}\" of incremental manager \"$hp\" (allowed DATE, DATETIME and TIMESTAMP_WITH_TIMEZONE types)!")
                }
            }
        }

        if (node.partitionsDatasetName != null) {
            def ds = dslCreator.findDataset(node.partitionsDatasetName)
            if (ds == null)
                throw new ExceptionModel("Dataset of the list of partitions \"${node.partitionsDatasetName}\" not found for model table \"${node.datasetName}\"!")

            checkDataset(node.partitionsDataset)
        }

        if (node.parentDatasetName != null) {
            if (findModelObject(node.parentDatasetName) == null)
                throw new ExceptionModel("Parent dataset \"${node.parentDatasetName}\" for model table \"${node.datasetName}\" " +
                        "not defined in model!")
            def parentDataset = node.parentDataset
            checkDataset(parentDataset)
            if (!(node.modelDataset instanceof AttachData))
                throw new ExceptionModel("Dataset \"${node.datasetName}\" does not support working with local data and cannot be used as a child dataset in a model!")
            if (node.parentLinkFieldName != null) {
                if (!parentDataset.field.isEmpty() && parentDataset.fieldByName(node.parentLinkFieldName) == null)
                    throw new ExceptionModel("Link field \"${node.parentLinkFieldName}\" not found in parent dataset \"${node.parentDatasetName}\" " +
                            "for model table \"${node.datasetName}\"!")
            }
        }
        else if (node.parentLinkFieldName != null)
            throw new ExceptionModel("When specifying the link field \"${node.parentLinkFieldName}\" in \"parentLinkFieldName\", " +
                    "needed set parent dataset for model table \"${node.datasetName}\"!")
    }

    protected checkDataset(Dataset ds) {
        if (ds == null)
            return

        if (dslCreator.findDataset(ds) == null)
            throw new ExceptionModel("Dataset \"$ds\" is not registered in the repository!")

        def dsn = ds.dslNameObject
        if (ds.connection == null)
            throw new ExceptionModel("The connection for the dataset \"$dsn\" [$ds] is not specified!")

        if (ds instanceof TableDataset) {
            def jdbcTable = ds as TableDataset
            if (jdbcTable.tableName == null)
                throw new ExceptionModel("Table \"$dsn\" [$ds] does not have a table name!")
        }
        else if (ds instanceof ViewDataset) {
            def viewTable = ds as ViewDataset
            if (viewTable.tableName == null)
                throw new ExceptionModel("View \"$dsn\" [$ds] does not have a table name!")
        }
        else if (ds instanceof QueryDataset) {
            def queryTable = ds as QueryDataset
            if (queryTable.query == null && queryTable.scriptFilePath == null)
                throw new ExceptionModel("Query \"$dsn\" [$ds] does not have a sql script!")
        }
        else if (ds instanceof FileDataset) {
            def fileTable = ds as FileDataset
            if (fileTable.fileName == null)
                throw new ExceptionModel("File dataset \"$dsn\" [$ds] does not have a file name!")
        }
    }

    /**
     * Check dataset model
     * @param ds checking dataset
     * @param connectionName the name of the connection used for the dataset
     */
    protected void checkModelDataset(Dataset ds, String connectionName = null) {
        if (ds == null)
            throw new ExceptionModel('No dataset specified!')

        def dsn = ds.dslNameObject
        if (ds.connection.dslNameObject != (connectionName?:modelConnectionName))
            throw new ExceptionModel("The connection of dataset \"$dsn\" does not match the specified connection to the model connection!")
    }
}