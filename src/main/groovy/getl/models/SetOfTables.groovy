//file:noinspection unused
package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.data.FileConnection
import getl.jdbc.JDBCConnection
import getl.models.opts.TableSpec
import getl.models.sub.DatasetsModel
import getl.utils.CloneUtils
import getl.utils.MapUtils
import groovy.transform.CompileStatic
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
    String getSourceConnectionName() { modelConnectionName }
    /** Connection name in the repository */
    void setSourceConnectionName(String value) { useSourceConnection(value) }

    /** Used connection */
    @JsonIgnore
    Connection getSourceConnection() { modelConnection as Connection }
    /** Used connection */
    @JsonIgnore
    void setSourceConnection(Connection value) { useSourceConnection(value) }

    /** Use specified repository connection name */
    void useSourceConnection(String connectionName) {
        useModelConnection(connectionName)
    }
    /** Use specified connection */
    void useSourceConnection(Connection connection) { useModelConnection(connection) }

    /** Source connection as JDBC connection */
    @JsonIgnore
    JDBCConnection getSourceJdbcConnection() { sourceConnection as JDBCConnection }

    /** Source connection as file connection */
    @JsonIgnore
    FileConnection getSourceFileConnection() { sourceConnection as FileConnection }

    /** Used tables */
    List<TableSpec> getUsedTables() { usedObjects as List<TableSpec> }
    /** Used tables */
    void setUsedTables(List<TableSpec> value) {
        usedTables.clear()
        if (value != null)
            usedTables.addAll(value)
    }
    /** Assign used tables from list of map structure */
    void assignUsedTables(List<Map> value) {
        def own = this
        def list = [] as List<TableSpec>
        value?.each { node ->
            def p = CloneUtils.CloneMap(node, true)
            p.datasetName = p.sourceTableName

            p.remove('id')
            p.remove('index')
            p.remove('sourceTableName')

            MapUtils.RemoveKeys(p) { k, v ->
                return (v == null) || (v instanceof String && (v as String).length() == 0) || (v instanceof GString && (v as GString).length() == 0)
            }

            list.add(new TableSpec(own, p))
        }

        usedTables = list
    }

    /**
     * Use table in list
     * @param tableName repository table name
     * @param cl defining code
     * @return table spec
     */
    TableSpec table(String tableName,
                    @DelegatesTo(TableSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.TableSpec']) Closure cl = null) {
        dataset(tableName, cl)
    }

    /**
     * Use table in list
     * @param cl defining code
     * @return table spec
     */
    TableSpec table(@DelegatesTo(TableSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.TableSpec']) Closure cl) {
        def owner = DetectClosureDelegate(cl, true)
        return table((owner as Dataset).dslNameObject, cl)
    }

    /**
     * Use table in list
     * @param table repository table
     * @param cl defining code
     * @return table spec
     */
    TableSpec table(Dataset table,
                    @DelegatesTo(TableSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.TableSpec']) Closure cl = null) {
        useDataset(table, cl)
    }

    /**
     * Add tables to the model using the specified mask
     * @param maskName dataset search mask
     * @param cl parameter description code
     */
    void addTables(String maskName,
                   @DelegatesTo(TableSpec)
                   @ClosureParams(value = SimpleType, options = ['getl.models.opts.TableSpec']) Closure cl = null) {
        addDatasets(mask: maskName, code: cl)
    }

    private final Object synchModel = synchObjects

    /**
     * Check model
     * @param checkObjects check model object parameters
     * @param checkNodeCode additional validation code for model objects
     */
    @Synchronized('synchModel')
    void checkModel(Boolean checkObjects = true,
                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.TableSpec']) Closure checkNodeCode = null) {
        super.checkModel(checkObjects, checkNodeCode)
    }

    /** Find table in model */
    TableSpec findTable(String name) { findModelObject(name) as TableSpec }
}