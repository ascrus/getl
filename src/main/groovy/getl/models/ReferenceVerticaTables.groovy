package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.models.opts.ReferenceVerticaTableSpec
import getl.models.sub.DatasetsModel
import getl.utils.BoolUtils
import getl.utils.Logs
import getl.vertica.VerticaConnection
import getl.vertica.VerticaTable
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Reference tables model
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ReferenceVerticaTables extends DatasetsModel<ReferenceVerticaTableSpec> {
    /** Vertica connection name in the repository */
    String getReferenceConnectionName() { modelConnectionName }
    /** Vertica connection name in the repository */
    void setReferenceConnectionName(String value) { useReferenceConnection(value) }

    /** Used Vertica connection */
    @JsonIgnore
    VerticaConnection getReferenceConnection() { modelConnection as VerticaConnection }

    /** Use specified Vertica connection */
    void useReferenceConnection(String verticaConnectionName) { useModelConnection(verticaConnectionName) }
    /** Use specified Vertica connection */
    void useReferenceConnection(VerticaConnection verticaConnection) { useModelConnection(verticaConnection) }

    /** List of used tables */
    List<ReferenceVerticaTableSpec> getUsedTables() { usedDatasets as List<ReferenceVerticaTableSpec> }
    /** List of used tables */
    void setUsedTables(List<ReferenceVerticaTableSpec> value) {
        usedTables.clear()
        if (value != null)
            usedTables.addAll(value)
    }
    /** Assign tables from list of map */
    void assignUsedTables(List<Map> list) {
        usedTables.clear()
        list?.each { val ->
            referenceFromTable(val.workTableName as String) {
                if (val.attrs != null) attrs.putAll(val.attrs as Map)
                if (val.objectVars != null) objectVars.putAll(val.objectVars as Map)
                if (val.allowCopy != null) allowCopy = BoolUtils.IsValue(val.allowCopy)
                if (val.limitCopy != null) limitCopy = val.limitCopy as Long
                if (val.sampleCopy != null) sampleCopy = val.sampleCopy as Integer
                if (val.whereCopy != null) whereCopy = val.whereCopy as String
            }
        }
    }

    /** Reference storage schema */
    String getReferenceSchemaName() { params.referenceSchemaName as String }
    /** Reference storage schema */
    void setReferenceSchemaName(String value) { saveParamValue('referenceSchemaName', value) }

    /**
     * The source table for which reference data is needed
     * @param tableName repository Vertica table name
     * @param cl defining code
     * @return table spec
     */
    ReferenceVerticaTableSpec referenceFromTable(String tableName,
                                                 @DelegatesTo(ReferenceVerticaTableSpec)
                             @ClosureParams(value = SimpleType, options = ['getl.models.opts.ReferenceVerticaTableSpec'])
                                     Closure cl = null) {
        super.dataset(tableName, cl) as ReferenceVerticaTableSpec
    }

    /**
     * The source table for which reference data is needed
     * @param cl defining code
     * @return table spec
     */
    ReferenceVerticaTableSpec referenceFromTable(@DelegatesTo(ReferenceVerticaTableSpec)
                             @ClosureParams(value = SimpleType, options = ['getl.models.opts.ReferenceVerticaTableSpec'])
                                    Closure cl) {
        referenceFromTable(null, cl)
    }

    @Override
    protected void validDataset(Dataset ds, String connectionName = null) {
        super.validDataset(ds, connectionName)

        def dsn = ds.dslNameObject
        if (!(ds instanceof VerticaTable))
            throw new ExceptionModel("Table \"$dsn\" [$ds] is not a Vertica table!")

        def vtb = ds as VerticaTable
        if (vtb.schemaName.toLowerCase() == referenceSchemaName.toLowerCase())
            throw new ExceptionModel("The schema of table \"$dsn\" [$ds] cannot be a model schema!")
    }

    /**
     * Create reference tables
     * @param recreate recreate table if exists
     * @param grantRolesToSchema give access grants to the reference schema for user roles
     */
    void createReferenceTables(Boolean recreate = false, Boolean grantRolesToSchema = false) {
        checkModel()
        new QueryDataset().with {
            useConnection referenceConnection
            query = """SELECT Count(*) AS count FROM schemata WHERE schema_name ILIKE '{schema}'"""
            queryParams.schema = referenceSchemaName
            if (rows()[0].count == 0) {
                currentJDBCConnection.executeCommand('CREATE SCHEMA {schema} DEFAULT INCLUDE SCHEMA PRIVILEGES',
                        [queryParams: [schema: referenceSchemaName]])
                Logs.Info("To store the reference data of model \"$repositoryModelName\" created scheme \"$referenceSchemaName\"")
                if (grantRolesToSchema) {
                    query = '''SELECT Replace(default_roles, '*', '') AS roles FROM users WHERE user_name = CURRENT_USER'''
                    def rows = rows()
                    if (rows.size() != 1)
                        throw new ExceptionDSL("Granting error for the reference scheme for model \"$repositoryModelName\": user \"${currentJDBCConnection.login}\" not found in Vertica!")
                    def roles = rows[0].roles as String
                    if (roles != null && roles != '') {
                        currentJDBCConnection.executeCommand('GRANT ALL PRIVILEGES EXTEND ON SCHEMA {schema} TO {roles}',
                                [queryParams: [schema: referenceSchemaName, roles: roles]])
                        Logs.Info("For reference schema \"$referenceSchemaName\" of model \"$repositoryModelName\", full access is given for roles: $roles")
                    }
                }
            }
        }

        usedTables.each { modelTable -> modelTable.createReferenceTable(recreate) }
        Logs.Info("Reference model \"$repositoryModelName\" successfully ${(recreate)?'recreated':'created'}")
    }

    /**
     * Filling reference data from other Vertica cluster
     * @param externalConnection Vertica cluster from which to copy data
     * @param onlyForEmpty copy data for empty tables only (default true)
     * @return count of tables copied
     */
    Integer copyFromVertica(VerticaConnection externalConnection, Boolean onlyForEmpty = true) {
        checkModel()

        def res = 0
        externalConnection.attachExternalVertica(referenceConnection)
        Logs.Info("Connection to \"$referenceConnection\" of \"$externalConnection\" is established")
        try {
            usedTables.each { modelTable ->
                if (modelTable.copyFromVertica(externalConnection, onlyForEmpty)) res++
            }

            Logs.Info("$res tables copied successfully to the reference model \"$repositoryModelName\"")
        }
        finally {
            externalConnection.detachExternalVertica(referenceConnection)
            Logs.Info("Connection with \"$referenceConnection\" of \"$externalConnection\" is broken")
        }

        return res
    }

    /**
     * Filling reference data from source tables
     * @param onlyForEmpty copy data for empty tables only (default true)
     * @param return count of tables copied
     */
    Integer copyFromSourceTables(Boolean onlyForEmpty = true) {
        checkModel()

        def res = 0
        usedTables.each { modelTable ->
            if (modelTable.copyFromSourceTable(onlyForEmpty)) res++
        }

        Logs.Info("$res tables copied successfully to the reference model \"$repositoryModelName\"")

        return res
    }

    /**
     * Fill tables with data from reference tables
     * @return count of tables filled
     */
    Integer fill() {
        checkModel()

        Logs.Fine("Start deploying tables for \"$repositoryModelName\" model")

        def res = 0
        usedTables.each { modelTable ->
            if (modelTable.fillFromReferenceTable()) res++
        }

        Logs.Info("$res source tables successfully filled with data from model \"$repositoryModelName\"")

        return res
    }

    /**
     * Clear source tables
     * @return count of tables cleared
     */
    Integer clear() {
        checkModel()

        Logs.Fine("Start clearing tables for \"$repositoryModelName\" model")

        def res = 0
        usedTables.each { modelTable ->
            modelTable.workTable.truncate(truncate: true)
            res++
        }

        Logs.Info("$res tables for model \"$repositoryModelName\" successfully cleared")

        return res
    }

    @Override
    String toString() { "Referencing ${usedObjects.size()} tables from \"$referenceConnectionName\" connection in \"$referenceSchemaName\" schemata" }
}