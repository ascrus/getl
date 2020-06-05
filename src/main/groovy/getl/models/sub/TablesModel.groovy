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
package getl.models.sub

import getl.data.Connection
import getl.data.Dataset
import getl.data.FileDataset
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.jdbc.ViewDataset
import getl.models.opts.BaseSpec
import getl.models.opts.TableSpec
import groovy.transform.InheritConstructors

/**
 * Datasets model
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class TablesModel<T extends TableSpec> extends BaseModel {
    /** Connection name for model */
    protected String getModelConnectionName() { params.modelConnectionName as String }
    /** Set the name of the connection for the model */
    protected void useModelConnection(String connectionName) {
        if (connectionName == null)
            throw new ExceptionModel('Connection name required!')
        dslCreator.connection(connectionName)

        params.modelConnectionName = connectionName
    }
    /** Set connection for the model */
    protected void useModelConnection(Connection connection) {
        if (connection == null)
            throw new ExceptionModel('Connection required!')
        if (connection.dslNameObject == null)
            throw new ExceptionModel('Connection not registered in Getl repository!')

        params.modelConnectionName = connection.dslNameObject
    }
    /** Connection for model */
    protected Connection getModelConnection() { dslCreator.connection(modelConnectionName) }

    /** Source tables */
    protected List<T> getUsedDatasets() { usedObjects as List<T> }

    /** Define model table */
    protected T modelTable(String datasetName, Closure cl) {
        if (datasetName == null) {
            def owner = DetectClosureDelegate(cl)
            if (owner instanceof Dataset)
                datasetName = (owner as Dataset).dslNameObject
        }

        if (datasetName == null)
            throw new ExceptionModel("The repository table name is not specified!")

        checkModel(false)

        def table = dslCreator.dataset(datasetName)
        validDataset(table)
        def dslDatasetName = table.dslNameObject

        def parent = (usedDatasets.find { t -> t.datasetName == dslDatasetName })
        if (parent == null)
            parent = newSpec(dslDatasetName) as T

        parent.runClosure(cl)

        return parent
    }

    /** Use table in model */
    protected T useModelTable(Dataset table, Closure cl = null) {
        modelTable(table.dslNameObject, cl)
    }

    /**
     * Valid source table attributes
     * @param ds checking dataset
     * @param connectionName the name of the connection used for the dataset
     */
    protected void validDataset(Dataset ds, String connectionName = null) {
        if (ds.connection == null)
            throw new ExceptionModel("The connection for the dataset $ds is not specified!")
        if (ds.connection?.dslNameObject != (connectionName?:modelConnectionName))
            throw new ExceptionModel("The connection of dataset $ds does not match the specified connection to the model connection!")
        if (ds.dslNameObject == null)
            throw new ExceptionModel("Dataset $ds is not registered in the repository!")

        if (ds instanceof TableDataset) {
            def jdbcTable = ds as TableDataset
            if (jdbcTable.schemaName == null)
                throw new ExceptionModel("Table $ds does not have a schema!")
            if (jdbcTable.tableName == null)
                throw new ExceptionModel("Table $ds does not have a table name!")
        }
        else if (ds instanceof ViewDataset) {
            def viewTable = ds as ViewDataset
            if (viewTable.schemaName == null)
                throw new ExceptionModel("View $ds does not have a schema!")
            if (viewTable.tableName == null)
                throw new ExceptionModel("View $ds does not have a table name!")
        }
        else if (ds instanceof QueryDataset) {
            def queryTable = ds as QueryDataset
            if (queryTable.query == null)
                throw new ExceptionModel("Query $ds does not have a sql script!")
        }
        else if (ds instanceof FileDataset) {
            def fileTable = ds as FileDataset
            if (fileTable.fileName == null)
                throw new ExceptionModel("File $ds does not have a file name!")
        }
    }

    @Override
    void checkModel(boolean checkObjects = true) {
        if (modelConnectionName == null)
            throw new ExceptionModel("The model connection is not specified!")

        super.checkModel(checkObjects)
    }

    @Override
    void checkObject(BaseSpec obj) {
        super.checkObject(obj)
        validDataset((obj as TableSpec).dataset)
    }
}