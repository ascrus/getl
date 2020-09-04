package getl.jdbc.opts

import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Options for retrieving a dataset list from a connection
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RetrieveDatasetsSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.tableMask == null) params.tableMask = [] as List<String>
        if (params.type == null) params.type = [tableType] as List<String>
    }

    /** Use specified database */
    String getDbName() { params.dbName as String }
    /** Use specified database */
    void setDbName(String value) { params.dbName = value }

    /** Use specified schema */
    String getSchemaName() { params.schemaName as String }
    /** Use specified schema */
    void setSchemaName(String value) { params.schemaName = value }

    /** Use specified table sql pattern */
    String getTablePattern() { params.tableName as String }
    /** Use specified table sql pattern mask  */
    void setTablePattern(String value) { params.tableName = value }

    /** Use specified table mask */
    List<String> getTableMask() { params.tableMask as List<String> }
    /** Use specified table mask */
    void setTableMask(List<String> value) {
        tableMask.clear()
        if (value != null)
            tableMask.addAll(value)
    }

    /** Database tables */
    static public final tableType = 'TABLE'
    /** Global temporary database tables */
    static public final globalTableType = 'GLOBAL TEMPORARY'
    /** Database views */
    static public final viewType = 'VIEW'
    /** Database system tables */
    static public final systemTableType = 'SYSTEM TABLE'
    /** Local temporary database tables */
    static public final localTableType = 'LOCAL TEMPORARY'

    /** Return objects of specified types (tables are returned by default) */
    List<String> getFilterByObjectType() { params.type as List<String> }
    /** Return objects of specified types (tables are returned by default) */
    void setFilterByObjectType(List<String> value) {
        filterByObjectType.clear()
        if (value != null)
            filterByObjectType.addAll(value*.toUpperCase() as List<String>)
    }

    /** Filter objects with custom code */
    Closure<Boolean> getOnFilter() { params.filter as Closure<Boolean> }
    /** Filter objects with custom code */
    void setOnFilter(Closure<Boolean> value) { params.filter = value }
    /** Filter objects with custom code */
    void filter(@DelegatesTo(RetrieveDatasetsSpec)
                @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.RetrieveDatasetsSpec'])
                        Closure<Boolean> cl = null) {
        setOnFilter(cl)
    }
}