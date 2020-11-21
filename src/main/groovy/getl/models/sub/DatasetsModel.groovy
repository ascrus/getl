package getl.models.sub

import getl.data.Connection
import getl.data.Dataset
import getl.data.FileDataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.jdbc.ViewDataset
import getl.models.opts.BaseSpec
import getl.models.opts.DatasetSpec
import groovy.transform.InheritConstructors

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
        validDataset(table)
        def dslDatasetName = table.dslNameObject

        def parent = (usedDatasets.find { t -> t.datasetName == dslDatasetName })
        if (parent == null)
            parent = newSpec(dslDatasetName) as T

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

    @Override
    void checkModel(Boolean checkObjects = true) {
        if (modelConnectionName == null)
            throw new ExceptionModel("The model connection is not specified!")

        super.checkModel(checkObjects)
    }

    @Override
    void checkObject(BaseSpec obj) {
        super.checkObject(obj)
        validDataset((obj as DatasetSpec).modelDataset)
    }

    /**
     * Check attribute naming and generate an unknown error for used objects
     * @param allowAttrs list of allowed attribute names
     */
    void checkAttrs(List<String> allowAttrs) {
        if (allowAttrs == null)
            throw new ExceptionDSL('The list of attribute names in parameter "allowAttrs" is not specified!')

        usedDatasets.each { node ->
            node.checkAttrs(allowAttrs)
        }
    }

    /**
     * Valid dataset parameters
     * @param ds validation dataset
     * @param connectionName the name of the connection used for the dataset
     */
    protected void validDataset(Dataset ds, String connectionName = null) {
        if (ds == null)
            throw new ExceptionDSL('No dataset specified!')

        if (ds.dslNameObject == null)
            throw new ExceptionModel("Dataset \"$ds\" is not registered in the repository!")

        def dsn = ds.dslNameObject
        if (ds.connection == null)
            throw new ExceptionModel("The connection for the dataset \"$dsn\" is not specified!")
        if (ds.connection.dslNameObject != (connectionName?:modelConnectionName))
            throw new ExceptionModel("The connection of dataset \"$dsn\" does not match the specified connection to the model connection!")

        if (ds instanceof TableDataset) {
            def jdbcTable = ds as TableDataset
            if (jdbcTable.schemaName == null)
                throw new ExceptionModel("Table \"$dsn\" [$ds] does not have a schema!")
            if (jdbcTable.tableName == null)
                throw new ExceptionModel("Table \"$dsn\" [$ds] does not have a table name!")
        }
        else if (ds instanceof ViewDataset) {
            def viewTable = ds as ViewDataset
            if (viewTable.schemaName == null)
                throw new ExceptionModel("View \"$dsn\" does not have a schema!")
            if (viewTable.tableName == null)
                throw new ExceptionModel("View \"$dsn\" does not have a table name!")
        }
        else if (ds instanceof QueryDataset) {
            def queryTable = ds as QueryDataset
            if (queryTable.query == null)
                throw new ExceptionModel("Query \"$dsn\" does not have a sql script!")
        }
        else if (ds instanceof FileDataset) {
            def fileTable = ds as FileDataset
            if (fileTable.fileName == null)
                throw new ExceptionModel("File dataset \"$dsn\" does not have a file name!")
        }
    }
}