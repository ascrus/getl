package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionDSL
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.models.opts.TableSpec
import getl.models.sub.DatasetsModel
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
    /** Assign tables from list of map */
    void assignUsedTables(List<Map> list) {
        usedTables.clear()
        list?.each {val ->
            table(val.sourceTableName) {
                if (val.attrs != null) attrs.putAll(val.attrs as Map)
                if (val.objectVars != null) objectVars.putAll(val.objectVars as Map)
            }
        }
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
        super.dataset(tableName, cl)
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
        super.useDataset(table, cl)
    }

    @Override
    String toString() { "Grouping ${usedObjects.size()} tables from \"$sourceConnectionName\" connection" }
}