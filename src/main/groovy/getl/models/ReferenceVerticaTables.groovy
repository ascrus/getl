package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.models.opts.ReferenceVerticaTableSpec
import getl.models.sub.DatasetsModel
import getl.utils.Logs
import getl.utils.Path
import getl.vertica.VerticaConnection
import getl.vertica.VerticaTable
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Reference tables model
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ReferenceVerticaTables extends DatasetsModel<ReferenceVerticaTableSpec> {
    /** Vertica connection name in the repository */
    @Synchronized
    String getReferenceConnectionName() { modelConnectionName }
    /** Vertica connection name in the repository */
    @Synchronized
    void setReferenceConnectionName(String value) { useReferenceConnection(value) }

    /** Used Vertica connection */
    @JsonIgnore
    @Synchronized
    VerticaConnection getReferenceConnection() { modelConnection as VerticaConnection }
    /** Used Vertica connection */
    @JsonIgnore
    @Synchronized
    void setReferenceConnection(VerticaConnection value) { useReferenceConnection(value) }

    /** Use specified Vertica connection */
    @Synchronized
    void useReferenceConnection(String verticaConnectionName) { useModelConnection(verticaConnectionName) }
    /** Use specified Vertica connection */
    @Synchronized
    void useReferenceConnection(VerticaConnection verticaConnection) { useModelConnection(verticaConnection) }

    /** List of used tables */
    @Synchronized
    List<ReferenceVerticaTableSpec> getUsedTables() { usedDatasets as List<ReferenceVerticaTableSpec> }
    /** List of used tables */
    @Synchronized
    void setUsedTables(List<ReferenceVerticaTableSpec> value) {
        usedTables.clear()
        if (value != null)
            usedTables.addAll(value)
    }

    /** Reference storage schema */
    @Synchronized
    String getReferenceSchemaName() { params.referenceSchemaName as String }
    /** Reference storage schema */
    @Synchronized
    void setReferenceSchemaName(String value) { saveParamValue('referenceSchemaName', value) }

    /**
     * The source table for which reference data is needed
     * @param tableName repository Vertica table name
     * @param cl defining code
     * @return table spec
     */
    @Synchronized
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
    @Synchronized
    ReferenceVerticaTableSpec referenceFromTable(@DelegatesTo(ReferenceVerticaTableSpec)
                             @ClosureParams(value = SimpleType, options = ['getl.models.opts.ReferenceVerticaTableSpec'])
                                    Closure cl) {
        referenceFromTable(null, cl)
    }

    @Override
    @Synchronized
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
        Logs.Fine("*** Create reference tables for model \"$repositoryModelName\"")
        new QueryDataset().with {
            useConnection referenceConnection
            query = """SELECT Count(*) AS count FROM schemata WHERE schema_name ILIKE '{schema}'"""
            queryParams.schema = referenceSchemaName
            if (rows()[0].count == 0) {
                try {
                    currentJDBCConnection.executeCommand('CREATE SCHEMA {schema} DEFAULT INCLUDE SCHEMA PRIVILEGES',
                            [queryParams: [schema: referenceSchemaName]])
                }
                catch (Exception e) {
                    Logs.Severe("Error creating reference schema \"$referenceSchemaName\" in model \"$repositoryModelName\": ${e.message}")
                    throw e
                }
                Logs.Info("$repositoryModelName: to store the reference data created scheme \"$referenceSchemaName\"")
                if (grantRolesToSchema) {
                    query = '''SELECT Replace(default_roles, '*', '') AS roles FROM users WHERE user_name = CURRENT_USER'''
                    def rows = rows()
                    if (rows.size() != 1)
                        throw new ExceptionDSL("Granting error for the reference scheme for model \"$repositoryModelName\": user \"${currentJDBCConnection.login}\" not found in Vertica!")
                    def roles = rows[0].roles as String
                    if (roles != null && roles != '') {
                        try {
                            currentJDBCConnection.executeCommand('GRANT ALL PRIVILEGES EXTEND ON SCHEMA {schema} TO {roles}',
                                    [queryParams: [schema: referenceSchemaName, roles: roles]])
                        }
                        catch (Exception e) {
                            Logs.Severe("Error granting reference schema \"$referenceSchemaName\" in model \"$repositoryModelName\": ${e.message}")
                            throw e
                        }
                        Logs.Info("$repositoryModelName: reference schema \"$referenceSchemaName\" granted to roles: $roles")
                    }
                }
            }
        }

        usedTables.each { spec ->
            def name = spec.workTable.dslNameObject
            try {
                spec.createReferenceTable(recreate)
            }
            catch (Exception e) {
                Logs.Severe("Error creating reference table \"$name\" in model \"$repositoryModelName\": ${e.message}")
                throw e
            }
        }
        Logs.Info("$repositoryModelName: reference model successfully ${(recreate)?'recreated':'created'}")
    }

    /** Drop reference schema and tables in Vertica database */
    void dropReferenceTables() {
        checkModel(false)
        Logs.Info("*** Drop reference schema and tables for model \"${repositoryModelName}\"")
        try {
            referenceConnection.executeCommand("DROP SCHEMA IF EXISTS \"${referenceSchemaName}\" CASCADE")
        }
        catch (Exception e) {
            Logs.Severe("Error droping reference schema \"$referenceSchemaName\" in model \"$repositoryModelName\": ${e.message}")
            throw e
        }
        Logs.Info("$repositoryModelName: reference schema \"${referenceSchemaName}\" dropped")
    }

    /**
     * Filling reference data from other Vertica cluster
     * @param externalConnection Vertica cluster from which to copy data
     * @param onlyForEmpty copy data for empty tables only (default true)
     * @param useExportCopy copy data between clusters Vertica using operator "EXPORT TO" (default false)
     * @param include list of tables included for processing
     * @param exclude list of tables excluded from processing
     * @return count of tables copied
     */
    Integer copyFromVertica(VerticaConnection externalConnection, Boolean onlyForEmpty = true, Boolean useExportCopy = false,
                            List<String> include = null, List<String> exclude = null) {
        checkModel()

        Logs.Fine("*** Copy Vertica external tables to reference model \"$repositoryModelName\"")

        def includePath = Path.Masks2Paths(include)
        def excludePath = Path.Masks2Paths(exclude)

        def res = 0
        if (useExportCopy) {
            externalConnection.attachExternalVertica(referenceConnection)
            Logs.Info("$repositoryModelName: connection to \"$referenceConnection\" of \"$externalConnection\" is established")
        }
        try {
            usedTables.each { spec ->
                def name = spec.workTable.dslNameObject
                if (includePath != null && !Path.MatchList(name, includePath)) {
                    Logs.Warning("${repositoryModelName}.[${name}]: the table is not listed in the include list and is skipped")
                    return
                }
                if (excludePath != null && Path.MatchList(name, excludePath)) {
                    Logs.Warning("${repositoryModelName}.[${name}]: the table is listed in the exclude list and is skipped")
                    return
                }

                try {
                    if (spec.copyFromVertica(externalConnection, onlyForEmpty, useExportCopy)) res++
                }
                catch (Exception e) {
                    Logs.Severe("Error copying to reference table \"$name\" in model \"$repositoryModelName\": ${e.message}")
                    throw e
                }
            }

            Logs.Info("${repositoryModelName}: $res tables copied successfully to reference tables from other Vertica cluster")
        }
        finally {
            if (useExportCopy) {
                externalConnection.detachExternalVertica(referenceConnection)
                Logs.Info("${repositoryModelName}: connection with \"$referenceConnection\" of \"$externalConnection\" is broken")
            }
        }

        return res
    }

    /**
     * Filling reference data from source tables
     * @param onlyForEmpty copy data for empty tables only (default true)
     * @param include list of tables included for processing
     * @param exclude list of tables excluded from processing
     * @param return count of tables copied
     */
    Integer copyFromSourceTables(Boolean onlyForEmpty = true, List<String> include = null, List<String> exclude = null) {
        checkModel()

        Logs.Fine("*** Copy source tables to reference model \"$repositoryModelName\"")

        def includePath = Path.Masks2Paths(include)
        def excludePath = Path.Masks2Paths(exclude)

        def res = 0
        usedTables.each { spec ->
            def name = spec.workTable.dslNameObject
            if (includePath != null && !Path.MatchList(name, includePath)) {
                Logs.Warning("${repositoryModelName}.[${name}]: the table is not listed in the include list and is skipped")
                return
            }
            if (excludePath != null && Path.MatchList(name, excludePath)) {
                Logs.Warning("${repositoryModelName}.[${name}]: the table is listed in the exclude list and is skipped")
                return
            }

            try {
                if (spec.copyFromDataset(onlyForEmpty)) res++
            }
            catch (Exception e) {
                Logs.Severe("Error copying to reference table \"$name\" in model \"$repositoryModelName\": ${e.message}")
                throw e
            }
        }

        Logs.Info("${repositoryModelName}: $res tables copied successfully to reference tables")

        return res
    }

    /**
     * Fill tables with data from reference tables
     * @return count of tables filled
     */
    Integer fill(List<String> include = null, List<String> exclude = null) {
        checkModel()

        Logs.Fine("*** Start deploying tables for \"$repositoryModelName\" model")

        def includePath = Path.Masks2Paths(include)
        def excludePath = Path.Masks2Paths(exclude)

        def res = 0
        usedTables.each { spec ->
            def name = spec.workTable.dslNameObject
            if (includePath != null && !Path.MatchList(name, includePath)) {
                Logs.Warning("${repositoryModelName}.[${name}]: the table is not listed in the include list and is skipped")
                return
            }
            if (excludePath != null && Path.MatchList(name, excludePath)) {
                Logs.Warning("${repositoryModelName}.[${name}]: the table is listed in the exclude list and is skipped")
                return
            }

            try {
                if (spec.fillFromReferenceTable()) res++
            }
            catch (Exception e) {
                Logs.Severe("Error filling reference data to table \"$name\" in model \"$repositoryModelName\": ${e.message}")
                throw e
            }
        }

        Logs.Info("${repositoryModelName}: $res tables successfully filled from reference tables")

        return res
    }

    /**
     * Clear source tables
     * @return count of tables cleared
     */
    Integer clear(List<String> include = null, List<String> exclude = null) {
        checkModel()

        Logs.Fine("*** Start clearing tables for \"$repositoryModelName\" model")

        def includePath = Path.Masks2Paths(include)
        def excludePath = Path.Masks2Paths(exclude)

        def res = 0
        usedTables.each { spec ->
            def name = spec.workTable.dslNameObject
            if (includePath != null && !Path.MatchList(name, includePath)) {
                Logs.Warning("${repositoryModelName}.[${name}]: the table is not listed in the include list and is skipped")
                return
            }
            if (excludePath != null && Path.MatchList(name, excludePath)) {
                Logs.Warning("${repositoryModelName}.[${name}]: the table is listed in the exclude list and is skipped")
                return
            }

            try {
                spec.workTable.truncate(truncate: true)
            }
            catch (Exception e) {
                Logs.Severe("Error clearing table \"$name\" in model \"$repositoryModelName\": ${e.message}")
                throw e
            }
            Logs.Info("${repositoryModelName}.[${name}]: table cleared")
            res++
        }

        Logs.Info("${repositoryModelName}: $res tables successfully cleared")

        return res
    }

    @Override
    String toString() { "Referencing ${usedObjects.size()} tables from \"$referenceConnectionName\" connection in \"$referenceSchemaName\" schemata" }
}