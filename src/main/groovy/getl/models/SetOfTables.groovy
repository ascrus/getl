//file:noinspection unused
package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionDSL
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.models.opts.TableSpec
import getl.models.sub.DatasetsModel
import getl.utils.CloneUtils
import getl.utils.MapUtils
import groovy.transform.InheritConstructors
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
    JDBCConnection getSourceConnection() { modelConnection as JDBCConnection }
    /** Used connection */
    @JsonIgnore
    void setSourceConnection(JDBCConnection value) { useSourceConnection(value) }

    /** Use specified repository connection name */
    void useSourceConnection(String connectionName) {
        def con = dslCreator.connection(connectionName)
        if (!(con instanceof JDBCConnection))
            throw new ExceptionDSL("Connection \"$connectionName\" is not JDBC compatible!")
        useModelConnection(connectionName)
    }
    /** Use specified connection */
    void useSourceConnection(JDBCConnection connection) { useModelConnection(connection) }

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
            p.remove('sourceTableName')

            MapUtils.RemoveKeys(p) { k, v ->
                return (v == null) || (v instanceof String && v.length() == 0) || (v instanceof GString && v.length() == 0)
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
    TableSpec table(TableDataset table,
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
        addDatasets(maskName, cl)
    }

    @Override
    String toString() { "Grouping ${usedObjects.size()} tables from \"$sourceConnectionName\" connection" }
}