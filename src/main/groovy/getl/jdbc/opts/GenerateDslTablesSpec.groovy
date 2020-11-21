package getl.jdbc.opts

import getl.exception.ExceptionGETL
import getl.lang.opts.BaseSpec
import getl.utils.BoolUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Options for generate dsl scripts table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class GenerateDslTablesSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.types == null) params.types = [] as List<String>
        if (params.listTableSavedData == null) params.listTableSavedData = [] as List<String>
        if (params.listTableExcluded == null) params.listTableExcluded = [] as List<String>
    }

    /** The package name of the generated script space */
    String getPackageName() { (params.packageName as String)?.toLowerCase( )}
    /** The package name of the generated script space */
    void setPackageName(String value) { saveParamValue('packageName', value) }

    /** The name of the group to the registration tables in the repository */
    String getGroupName() { (params.groupName as String)?.toLowerCase() }
    /** The name of the group to the registration tables in the repository */
    void setGroupName(String value) { saveParamValue('groupName', value) }

    /** The name of the connection used for the tables */
    String getConnectionName() { (params.connectionName as String)?.toLowerCase() }
    /** The name of the connection used for the tables */
    void setConnectionName(String value) { saveParamValue('connectionName', value) }

    /** Overwrite existing script */
    Boolean getOverwriteScript() { BoolUtils.IsValue(params.overwriteScript) }
    /** Overwrite existing script */
    void setOverwriteScript(Boolean value) { saveParamValue('overwriteScript', value) }

    /** Path and file name to store table scripts (use capitalized file name and extension ".groovy") */
    String getScriptPath() { params.scriptPath as String}
    /** Path and file name to store table scripts (use capitalized file name and extension ".groovy") */
    void setScriptPath(String value) { saveParamValue('scriptPath', value) }

    /** Select tables from the specified database name */
    String getDbName() { params.dbName as String }
    /** Select tables from the specified database */
    void setDbName(String value) { saveParamValue('dbName', value) }

    /** Select tables from the specified schema name */
    String getSchemaName() { params.schemaName as String }
    /** Select tables from the specified database */
    void setSchemaName(String value) { saveParamValue('schemaName', value) }

    /** Select tables from the specified table pattern by JDBC syntax */
    String getTableName() { params.tableName as String }
    /** Select tables from the specified database */
    void setTableName(String value) { saveParamValue('tableName', value) }

    /** Select tables from the specified table mask (String or List) */
    Object getTableMask() { params.tableMask }
    /** Select tables from the specified table mask (String or List) */
    void setTableMask(Object value) {
        if (!(value instanceof String) && !(value instanceof List))
            throw new ExceptionGETL('For "tableMask" it is allowed to have a string type or a list of strings')

        saveParamValue('tableMask', value)
    }

    /** Filter by object type */
    List getTypes() { params.types as List<String> }
    /** Filter by object type */
    void setTypes(List value) {
        types.clear()
        if (value != null) types.addAll(value)
    }

    /** List of tables excluded from the filter */
    List getListTableExcluded() { params.listTableExcluded as List<String> }
    /** List of tables excluded from the filter */
    void setListTableExcluded(List value) {
        listTableExcluded.clear()
        if (value != null) listTableExcluded.addAll(value)
    }

    /** Create tables in the database after registration */
    Boolean getCreateTables() { BoolUtils.IsValue(params.createTables) }
    /** Create tables in the database after registration */
    void setCreateTables(Boolean value) { saveParamValue('createTables', value) }

    /** Delete existing tables in the database before creating */
    Boolean getDropTables() { BoolUtils.IsValue(params.dropTables) }
    /** Delete existing tables in the database before creating */
    void setDropTables(Boolean value) { saveParamValue('dropTables', value) }

    /** Save field definition in resource files */
    Boolean getDefineFields() { BoolUtils.IsValue(params.saveFields, true) }
    /** Save field definition in resource files */
    void setDefineFields(Boolean value) { saveParamValue('saveFields', value) }

    /** Save database type names for fields in schema files */
    Boolean getSaveTypeNameForFields() { BoolUtils.IsValue(params.saveTypeNameForFields) }
    /** Save database type names for fields in schema files */
    void setSaveTypeNameForFields(Boolean value) { saveParamValue('saveTypeNameForFields', value) }

    /** List of tables for which to save data in resource files */
    List getListTableSavedData() { params.listTableSavedData as List<String> }
    /** List of tables for which to save data in resource files */
    void setListTableSavedData(List value) {
        listTableSavedData.clear()
        if (value != null) listTableSavedData.addAll(value)
    }

    /** Path to resource files for storing table data */
    String getResourcePath() { params.resourcePath as String }
    /** Path to resource files for storing table data */
    void setResourcePath(String value) { saveParamValue('resourcePath', value) }

    String getResourceRoot() { params.resourceRoot as String }
    void setResourceRoot(String value) { saveParamValue('resourceRoot', value) }

    Closure getOnFilter() { params.filter as Closure<Boolean> }
    void setOnFilter(Closure<Boolean> value) { saveParamValue('filter', value) }
    void filter(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure<Boolean> cl) {
        setOnFilter(cl)
    }
}