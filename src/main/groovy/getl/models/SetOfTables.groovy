package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.exception.ExceptionDSL
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.models.opts.TableSpec
import getl.models.sub.DatasetsModel
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Table list model
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class SetOfTables extends DatasetsModel<TableSpec> {
    /** Connection name in the repository */
    @Synchronized
    String getSourceConnectionName() { modelConnectionName }
    /** Connection name in the repository */
    @Synchronized
    void setSourceConnectionName(String value) { useSourceConnection(value) }

    /** Used connection */
    @JsonIgnore
    @Synchronized
    JDBCConnection getSourceConnection() { modelConnection as JDBCConnection }
    /** Used connection */
    @JsonIgnore
    @Synchronized
    void setSourceConnection(JDBCConnection value) { useSourceConnection(value) }

    /** Use specified repository connection name */
    @Synchronized
    void useSourceConnection(String connectionName) {
        def con = dslCreator.connection(connectionName)
        if (!(con instanceof JDBCConnection))
            throw new ExceptionDSL("Connection \"$connectionName\" is not JDBC compatible!")
        useModelConnection(connectionName)
    }
    /** Use specified connection */
    @Synchronized
    void useSourceConnection(JDBCConnection connection) { useModelConnection(connection) }

    /** Used tables */
    @Synchronized
    List<TableSpec> getUsedTables() { usedObjects as List<TableSpec> }
    /** Used tables */
    @Synchronized
    void setUsedTables(List<TableSpec> value) {
        usedTables.clear()
        if (value != null)
            usedTables.addAll(value)
    }

    /** Name of the dataset storing the history of table processing */
    String getStoryDatasetName() { super.storyDatasetName }
    /** Name of the dataset storing the history of table processing */
    void setStoryDatasetName(String value) { super.useStoryDatasetName(value) }
    /** Dataset for storing the history of table processing */
    @JsonIgnore
    Dataset getStoryDataset() { super.storyDataset }
    /** Dataset for storing the history of table processing */
    @JsonIgnore
    void setStoryDataset(Dataset value) { super.setStoryDataset(value) }

    /** Use a dataset to store the history of table processing */
    void useStoryDatasetName(String datasetName) { super.useStoryDatasetName(datasetName) }
    /** Use a dataset to store the history of table processing */
    void useStoryDataset(Dataset dataset) { super.useStoryDataset(dataset) }

    /**
     * Use table in list
     * @param tableName repository table name
     * @param cl defining code
     * @return table spec
     */
    @Synchronized
    TableSpec table(String tableName,
                    @DelegatesTo(TableSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.TableSpec']) Closure cl = null) {
        super.dataset(tableName, cl)
    }

    /**
     * Use table in list
     * @param cl defining code
     * @return table spec
     */
    @Synchronized
    TableSpec table(@DelegatesTo(TableSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.TableSpec']) Closure cl) {
        def owner = DetectClosureDelegate(cl)
        if (owner instanceof TableDataset)
            return table((owner as TableDataset).dslNameObject)

        throw new ExceptionDSL('Required to specify jdbc compatible table!')
    }

    /**
     * Use table in list
     * @param table repository table
     * @param cl defining code
     * @return table spec
     */
    @Synchronized
    TableSpec table(TableDataset table,
                    @DelegatesTo(TableSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.TableSpec']) Closure cl = null) {
        super.useDataset(table, cl)
    }

    @Override
    String toString() { "Grouping ${usedObjects.size()} tables from \"$sourceConnectionName\" connection" }
}