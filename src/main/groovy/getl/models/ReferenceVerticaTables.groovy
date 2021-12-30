//file:noinspection unused
package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.models.opts.ReferenceVerticaTableSpec
import getl.models.sub.DatasetsModel
import getl.utils.CloneUtils
import getl.utils.MapUtils
import getl.utils.Path
import getl.utils.StringUtils
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
    /** Used Vertica connection */
    @JsonIgnore
    void setReferenceConnection(VerticaConnection value) { useReferenceConnection(value) }

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
    /** Convert a list of parameters to usable reference tables */
    void assignUsedTables(List<Map> value) {
        def own = this
        def list = [] as List<ReferenceVerticaTableSpec>
        value?.each { node ->
            def p = CloneUtils.CloneMap(node, true)
            p.datasetName = p.workTableName

            p.remove('id')
            p.remove('workTableName')

            MapUtils.RemoveKeys(p) { k, v ->
                return (v == null) || (v instanceof String && v.length() == 0) || (v instanceof GString && v.length() == 0)
            }

            list.add(new ReferenceVerticaTableSpec(own, p))
        }
        usedTables = list
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
        if (!(dslCreator.dataset(tableName) instanceof VerticaTable))
            throw new ExceptionDSL('Vertica table is required!')

        dataset(tableName, cl) as ReferenceVerticaTableSpec
    }

    /**
     * The source table for which reference data is needed
     * @param cl defining code
     * @return table spec
     */
    ReferenceVerticaTableSpec referenceFromTable(@DelegatesTo(ReferenceVerticaTableSpec)
                             @ClosureParams(value = SimpleType, options = ['getl.models.opts.ReferenceVerticaTableSpec'])
                                    Closure cl) {
        def owner = DetectClosureDelegate(cl, true)
        if (!(owner instanceof VerticaTable))
            throw new ExceptionDSL('Vertica table is required!')

        return referenceFromTable((owner as VerticaTable).dslNameObject, cl)
    }

    /**
     * Add reference tables to the model using the specified mask
     * @param maskName dataset search mask
     * @param cl parameter description code
     */
    void addReferenceTables(String maskName,
                            @DelegatesTo(ReferenceVerticaTableSpec)
                            @ClosureParams(value = SimpleType, options = ['getl.models.opts.ReferenceVerticaTableSpec'])
                                    Closure cl = null) {
        addDatasets(mask: maskName, code: cl, filter: { String name -> dslCreator.dataset(name) instanceof VerticaTable })
    }

    @Override
    protected void checkModelDataset(Dataset ds, String connectionName = null) {
        super.checkModelDataset(ds, connectionName)

        def dsn = ds.dslNameObject
        if (!(ds instanceof VerticaTable))
            throw new ExceptionModel("Table \"$dsn\" [$ds] is not a Vertica table!")

        def vtb = ds as VerticaTable
        if (vtb.schemaName().toLowerCase() == referenceSchemaName.toLowerCase())
            throw new ExceptionModel("The schema of table \"$dsn\" [$ds] cannot be a model schema!")
    }

    /**
     * Create reference tables
     * @param recreate recreate table if exists
     * @param grantRolesToSchema give access grants to the reference schema for user roles
     */
    void createReferenceTables(Boolean recreate = false, Boolean grantRolesToSchema = false) {
        checkModel()
        dslCreator.logFine("*** Create reference tables for model \"$repositoryModelName\"")
        new QueryDataset(dslCreator: dslCreator).tap {
            useConnection referenceConnection
            query = """SELECT Count(*) AS count FROM schemata WHERE schema_name ILIKE '{schema}'"""
            queryParams.schema = referenceSchemaName
            if (rows()[0].count == 0) {
                try {
                    currentJDBCConnection.executeCommand('CREATE SCHEMA {schema} DEFAULT INCLUDE SCHEMA PRIVILEGES',
                            [queryParams: [schema: referenceSchemaName]])
                }
                catch (Exception e) {
                    dslCreator.logError("Error creating reference schema \"$referenceSchemaName\" in model \"$repositoryModelName\": ${e.message}")
                    throw e
                }
                dslCreator.logInfo("$repositoryModelName: to store the reference data created scheme \"$referenceSchemaName\"")
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
                            dslCreator.logError("Error granting reference schema \"$referenceSchemaName\" in model \"$repositoryModelName\": ${e.message}")
                            throw e
                        }
                        dslCreator.logInfo("$repositoryModelName: reference schema \"$referenceSchemaName\" granted to roles: $roles")
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
                dslCreator.logError("Error creating reference table \"$name\" in model \"$repositoryModelName\": ${e.message}")
                throw e
            }
        }
        dslCreator.logInfo("$repositoryModelName: reference model successfully ${(recreate)?'recreated':'created'}")
    }

    /** Drop reference schema and tables in Vertica database */
    void dropReferenceTables() {
        checkModel(false)
        dslCreator.logInfo("*** Drop reference schema and tables for model \"${repositoryModelName}\"")
        try {
            referenceConnection.executeCommand("DROP SCHEMA IF EXISTS \"${referenceSchemaName}\" CASCADE")
        }
        catch (Exception e) {
            dslCreator.logError("Error droping reference schema \"$referenceSchemaName\" in model \"$repositoryModelName\": ${e.message}")
            throw e
        }
        dslCreator.logInfo("$repositoryModelName: reference schema \"${referenceSchemaName}\" dropped")
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

        dslCreator.logFine("*** Copy Vertica external tables to reference model \"$repositoryModelName\"")

        def includePath = Path.Masks2Paths(include)
        def excludePath = Path.Masks2Paths(exclude)

        def res = 0
        if (useExportCopy) {
            externalConnection.attachExternalVertica(referenceConnection)
            dslCreator.logInfo("$repositoryModelName: connection to \"$referenceConnection\" of \"$externalConnection\" is established")
        }
        try {
            usedTables.each { spec ->
                def name = spec.workTable.dslNameObject
                if (includePath != null && !Path.MatchList(name, includePath)) {
                    dslCreator.logWarn("${repositoryModelName}.[${name}]: the table is not listed in the include list and is skipped")
                    return
                }
                if (excludePath != null && Path.MatchList(name, excludePath)) {
                    dslCreator.logWarn("${repositoryModelName}.[${name}]: the table is listed in the exclude list and is skipped")
                    return
                }

                try {
                    if (spec.copyFromVertica(externalConnection, onlyForEmpty, useExportCopy)) res++
                }
                catch (Exception e) {
                    dslCreator.logError("Error copying to reference table \"$name\" in model \"$repositoryModelName\": ${e.message}")
                    throw e
                }
            }

            dslCreator.logInfo("${repositoryModelName}: $res tables copied successfully to reference tables from other Vertica cluster")
        }
        finally {
            if (useExportCopy) {
                externalConnection.detachExternalVertica(referenceConnection)
                dslCreator.logInfo("${repositoryModelName}: connection with \"$referenceConnection\" of \"$externalConnection\" is broken")
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

        dslCreator.logFine("*** Copy source tables to reference model \"$repositoryModelName\"")

        def includePath = Path.Masks2Paths(include)
        def excludePath = Path.Masks2Paths(exclude)

        def res = 0
        usedTables.each { spec ->
            def name = spec.workTable.dslNameObject
            if (includePath != null && !Path.MatchList(name, includePath)) {
                dslCreator.logWarn("${repositoryModelName}.[${name}]: the table is not listed in the include list and is skipped")
                return
            }
            if (excludePath != null && Path.MatchList(name, excludePath)) {
                dslCreator.logWarn("${repositoryModelName}.[${name}]: the table is listed in the exclude list and is skipped")
                return
            }

            try {
                if (spec.copyFromDataset(onlyForEmpty)) res++
            }
            catch (Exception e) {
                dslCreator.logError("Error copying to reference table \"$name\" in model \"$repositoryModelName\": ${e.message}")
                throw e
            }
        }

        dslCreator.logInfo("${repositoryModelName}: $res tables copied successfully to reference tables")

        return res
    }

    /**
     * Fill tables with data from reference tables
     * @param include process only specified table masks
     * @param exclude exclude specified table masks
     * @return count of tables filled
     */
    Integer fill(List<String> include = null, List<String> exclude = null, Boolean usePartitions = true) {
        checkModel()

        dslCreator.logFinest("+++ Start deploying tables for \"$repositoryModelName\" model ...")

        def includePath = Path.Masks2Paths(include)
        def excludePath = Path.Masks2Paths(exclude)

        def res = 0
        usedTables.each { spec ->
            def name = spec.workTable.dslNameObject
            if (includePath != null && !Path.MatchList(name, includePath)) {
                dslCreator.logWarn("${repositoryModelName}.[${name}]: the table is not listed in the include list and is skipped")
                return
            }
            if (excludePath != null && Path.MatchList(name, excludePath)) {
                dslCreator.logWarn("${repositoryModelName}.[${name}]: the table is listed in the exclude list and is skipped")
                return
            }

            try {
                if (spec.fillFromReferenceTable(usePartitions))
                    res++
            }
            catch (Exception e) {
                dslCreator.logError("Error filling reference data to table \"$name\" in model \"$repositoryModelName\": ${e.message}")
                throw e
            }
        }

        dslCreator.logInfo("+++ Deployment ${StringUtils.WithGroupSeparator(res)} tables for model " +
                "\"$repositoryModelName\" completed successfully")

        return res
    }

    /**
     * Change reference tables to the current structure of the original tables
     * @param include process only specified table masks
     * @param exclude
     */
    void changeReferenceTables(List<String> include = null, List<String> exclude = null) {
        checkModel()

        dslCreator.logFine("*** Change reference tables for \"$repositoryModelName\" model")

        def includePath = Path.Masks2Paths(include)
        def excludePath = Path.Masks2Paths(exclude)

        def res = 0
        usedTables.each { spec ->
            def name = spec.workTable.dslNameObject
            if (includePath != null && !Path.MatchList(name, includePath)) {
                dslCreator.logWarn("${repositoryModelName}.[${name}]: the table is not listed in the include list and is skipped")
                return
            }
            if (excludePath != null && Path.MatchList(name, excludePath)) {
                dslCreator.logWarn("${repositoryModelName}.[${name}]: the table is listed in the exclude list and is skipped")
                return
            }

            res ++
            try {
                spec.workTable.truncate()
                spec.fillFromReferenceTable(false)
                spec.createReferenceTable(true)
                spec.copyFromDataset(false)
            }
            catch (Exception e) {
                dslCreator.logError("Error change reference table \"$name\" in model \"$repositoryModelName\": ${e.message}")
                throw e
            }
        }

        dslCreator.logInfo("${repositoryModelName}: $res reference tables successfully changed")
    }

    /**
     * Clear source tables
     * @return count of tables cleared
     */
    Integer clear(List<String> include = null, List<String> exclude = null) {
        checkModel()

        dslCreator.logFine("*** Start clearing tables for \"$repositoryModelName\" model")

        def includePath = Path.Masks2Paths(include)
        def excludePath = Path.Masks2Paths(exclude)

        def res = 0
        usedTables.each { spec ->
            def name = spec.workTable.dslNameObject
            if (includePath != null && !Path.MatchList(name, includePath)) {
                dslCreator.logWarn("${repositoryModelName}.[${name}]: the table is not listed in the include list and is skipped")
                return
            }
            if (excludePath != null && Path.MatchList(name, excludePath)) {
                dslCreator.logWarn("${repositoryModelName}.[${name}]: the table is listed in the exclude list and is skipped")
                return
            }

            try {
                spec.workTable.truncate(truncate: true)
            }
            catch (Exception e) {
                dslCreator.logError("Error clearing table \"$name\" in model \"$repositoryModelName\": ${e.message}")
                throw e
            }
            dslCreator.logInfo("${repositoryModelName}.[${name}]: table cleared")
            res++
        }

        dslCreator.logInfo("${repositoryModelName}: $res tables successfully cleared")

        return res
    }

    @Override
    String toString() { "Referencing ${usedObjects.size()} tables from \"$referenceConnectionName\" connection in \"$referenceSchemaName\" schemata" }
}