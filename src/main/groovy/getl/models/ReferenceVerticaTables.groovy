/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/
package getl.models

import getl.data.Dataset
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.models.opts.ReferenceVerticaTableSpec
import getl.models.sub.DatasetsModel
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
    /** Use specified Vertica connection */
    void useReferenceConnection(String verticaConnectionName) { useModelConnection(verticaConnectionName) }
    /** Use specified Vertica connection */
    void useReferenceConnection(VerticaConnection verticaConnection) { useModelConnection(verticaConnection) }
    /** Used Vertica connection */
    VerticaConnection getReferenceConnection() { modelConnection as VerticaConnection }

    /** List of used tables */
    List<ReferenceVerticaTableSpec> getUsedTables() { usedDatasets as List<ReferenceVerticaTableSpec> }

    /** Reference storage schema */
    String getReferenceSchemaName() { params.referenceSchemaName as String }
    /** Reference storage schema */
    void setReferenceSchemaName(String value) { params.referenceSchemaName = value }

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
     */
    void createReferenceTables(boolean recreate = false) {
        checkModel()
        new QueryDataset().with {
            useConnection referenceConnection
            query = """SELECT Count(*) AS count FROM schemata WHERE schema_name ILIKE '{schema}'"""
            queryParams.schema = referenceSchemaName
            if (rows()[0].count == 0) {
                referenceConnection.executeCommand('CREATE SCHEMA {schema} DEFAULT INCLUDE SCHEMA PRIVILEGES',
                        [queryParams: [schema: referenceSchemaName]])
                Logs.Info("To store the reference data of model \"${dslNameObject}\" created scheme \"$referenceSchemaName\"")
            }
        }

        usedTables.each { modelTable -> modelTable.createReferenceTable(recreate) }
        Logs.Info("Reference model \"${dslNameObject}\" successfully ${(recreate)?'recreated':'created'}")
    }

    /**
     * Filling reference data from other Vertica cluster
     * @param externalConnection Vertica cluster from which to copy data
     * @param onlyForEmpty copy data for empty tables only (default true)
     * @return count of tables copied
     */
    int copyFromVertica(VerticaConnection externalConnection, boolean onlyForEmpty = true) {
        checkModel()

        def res = 0
        externalConnection.attachExternalVertica(referenceConnection)
        Logs.Info("Connection to \"$referenceConnection\" of \"$externalConnection\" is established")
        try {
            usedTables.each { modelTable ->
                if (modelTable.copyFromVertica(externalConnection, onlyForEmpty)) res++
            }

            Logs.Info("$res tables copied successfully to the reference model \"$dslNameObject\"")
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
    int copyFromSourceTables(boolean onlyForEmpty = true) {
        checkModel()

        def res = 0
        usedTables.each { modelTable ->
            if (modelTable.copyFromSourceTable(onlyForEmpty)) res++
        }

        Logs.Info("$res tables copied successfully to the reference model \"$dslNameObject\"")

        return res
    }

    /**
     * Fill tables with data from reference tables
     * @return count of tables filled
     */
    int fill() {
        checkModel()

        Logs.Fine("Start deploying tables for \"$dslNameObject\" model")

        def res = 0
        usedTables.each { modelTable ->
            if (modelTable.fillFromReferenceTable()) res++
        }

        Logs.Info("$res source tables successfully filled with data from model \"$dslNameObject\"")

        return res
    }

    /**
     * Clear source tables
     * @return count of tables cleared
     */
    int clear() {
        checkModel()

        Logs.Fine("Start clearing tables for \"$dslNameObject\" model")

        int res = 0
        usedTables.each { modelTable ->
            modelTable.workTable.truncate(truncate: true)
            res++
        }

        Logs.Info("$res tables for model \"$dslNameObject\" successfully cleared")

        return res
    }
}