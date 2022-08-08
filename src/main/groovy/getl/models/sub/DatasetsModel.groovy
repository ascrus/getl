//file:noinspection unused
package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.data.Field
import getl.data.FileDataset
import getl.exception.ExceptionDSL
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
        checkModelDataset(table)
        def dslDatasetName = table.dslNameObject

        def parent = (usedDatasets.find { t -> t.datasetName == dslDatasetName })
        if (parent == null)
            //parent = createSpec(dslDatasetName) as T
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
            throw new ExceptionDSL("The dataset \"$dataset\" is not registered in the repository!")
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
        def spec = obj as DatasetSpec
        checkModelDataset(spec.modelDataset)

        def hp = (spec.historyPointName != null)?dslCreator.findHistorypoint(spec.historyPointName):null
        if (spec.historyPointName != null) {
            if (hp == null)
                throw new ExceptionDSL("Increment point manager \"${spec.historyPointName}\" not found!")
            hp.checkManager()
        }
        if (spec.incrementFieldName != null) {
            def field = spec.modelDataset.fieldByName(spec.incrementFieldName)
            if (field == null)
                throw new ExceptionDSL("Increment field \"${spec.incrementFieldName}\" was not found in dataset \"${spec.modelDataset}\"!")
            if (hp != null) {
                switch (hp.sourceType) {
                    case hp.identitySourceType:
                        if (!(field.type in [Field.integerFieldType, Field.bigintFieldType, Field.numericFieldType]))
                            throw new ExceptionDSL("Field \"${spec.incrementFieldName}\" has type \"${field.type}\", which is not compatible with " +
                                    "type \"${hp.sourceType}\" of incremental manager \"$hp\" (allowed INTEGER, BIGINT and NUMERIC types)!")
                        break
                    case hp.timestampSourceType:
                        if (!(field.type in [Field.dateFieldType, Field.datetimeFieldType, Field.timestamp_with_timezoneFieldType]))
                            throw new ExceptionDSL("Field \"${spec.incrementFieldName}\" has type \"${field.type}\", which is not compatible with " +
                                    "type \"${hp.sourceType}\" of incremental manager \"$hp\" (allowed DATE, DATETIME and TIMESTAMP_WITH_TIMEZONE types)!")
                }
            }
        }
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
            throw new ExceptionDSL('No dataset specified!')

        checkDataset(ds)

        def dsn = ds.dslNameObject
        if (ds.connection.dslNameObject != (connectionName?:modelConnectionName))
            throw new ExceptionModel("The connection of dataset \"$dsn\" does not match the specified connection to the model connection!")
    }

    /**
     * Return a list of model datasets
     * @param includeMask list of masks to include datasets
     * @param excludeMask list of masks to exclude datasets
     * @return list of names of found model datasets
     */
    List<String> findModelDatasets(List<String> includeMask = null, List<String> excludeMask = null) {
        return findModelObjects(includeMask, excludeMask)
    }

    /**
     * Check the presence of a dataset in the model by its name
     * @param name source dataset name
     */
    Boolean datasetInModel(String name) {
        return usedDatasets.find {it.datasetName == name.toLowerCase() } != null
    }
}