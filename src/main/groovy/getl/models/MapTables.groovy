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

import getl.data.Connection
import getl.data.Dataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionModel
import getl.models.opts.BaseSpec
import getl.models.opts.MapTableSpec
import getl.models.sub.DatasetsModel
import getl.utils.MapUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Mapping tables model
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class MapTables extends DatasetsModel<MapTableSpec> {
    /** Repository connection name for source datasets */
    String getSourceConnectionName() { modelConnectionName }
    /** Use specified connection for source datasets */
    void useSourceConnection(String connectionName) { useModelConnection(connectionName) }
    /** Use specified connection for source datasets */
    void useSourceConnection(Connection connection) { useModelConnection(connection) }
    /** Used source connection */
    Connection getSourceConnection() { modelConnection }

    /** Used mapping datasets */
    List<MapTableSpec> getUsedMapping() { usedObjects as List<MapTableSpec> }

    /** Repository connection name for destination datasets */
    String getDestinationConnectionName() { params.destinationConnectionName as String }
    /** Use specified connection for destination datasets */
    void useDestinationConnection(String connectionName) {
        if (connectionName == null)
            throw new ExceptionModel('Connection name required!')
        dslCreator.connection(connectionName)

        params.destinationConnectionName = connectionName
    }
    /** Use specified connection for destination datasets */
    void useDestinationConnection(Connection connection) {
        if (connection == null)
            throw new ExceptionModel('Connection required!')
        if (connection.dslNameObject == null)
            throw new ExceptionModel('Connection not registered in Getl repository!')

        params.destinationConnectionName = connection.dslNameObject
    }
    /** Destination datasets connection */
    Connection getDestinationConnection() { dslCreator.connection(destinationConnectionName) }

    /**
     * Use dataset for the mapping
     * @param datasetName repository dataset name
     * @param cl defining code
     * @return mapping spec
     */
    MapTableSpec mapTable(String datasetName,
                          @DelegatesTo(MapTableSpec)
                          @ClosureParams(value = SimpleType, options = ['getl.models.opts.MapTableSpec'])
                                  Closure cl = null) {
        super.dataset(datasetName, cl)
    }

    /**
     * Use dataset for mapping
     * @param cl defining code
     * @return mapping spec
     */
    MapTableSpec mapTable(@DelegatesTo(MapTableSpec)
                          @ClosureParams(value = SimpleType, options = ['getl.models.opts.MapTableSpec']) Closure cl) {
        mapTable(null as String, cl)
    }

    /**
     * Use dataset for mapping
     * @param dataset repository dataset
     * @param cl defining code
     * @return mapping spec
     */
    MapTableSpec mapTable(Dataset dataset,
                          @DelegatesTo(MapTableSpec)
                          @ClosureParams(value = SimpleType, options = ['getl.models.opts.MapTableSpec'])
                                  Closure cl = null) {
        super.useDataset(dataset, cl)
    }

    @Override
    void checkModel(boolean checkObjects = true) {
        if (sourceConnectionName == null)
            throw new ExceptionModel("The source connection is not specified!")
        if (destinationConnectionName == null)
            throw new ExceptionModel("The destination connection is not specified!")

        super.checkModel(checkObjects)

        if (checkObjects)
            checkMapping()
    }

    @Override
    void checkObject(BaseSpec obj) {
        super.checkObject(obj)
        validDataset((obj as MapTableSpec).destination, destinationConnectionName)
    }

    /**
     * Check mapping objects
     * @param cl validation code
     */
    void checkMapping(@DelegatesTo(MapTableSpec)
                      @ClosureParams(value = SimpleType, options = ['getl.models.opts.MapTableSpec']) Closure cl = null) {
        usedMapping.each { node ->
            if (node.destinationName == null)
                throw new ExceptionDSL("The destination is not specified for table \"${node.sourceName}\"!")

            if (cl != null)
                node.with(cl)
        }
    }

    @Override
    String toString() { "Mapping ${usedObjects.size()} tables from \"$sourceConnectionName\" to \"$destinationConnectionName\" connections" }
}