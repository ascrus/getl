package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionModel
import getl.models.opts.BaseSpec
import getl.models.opts.MapTableSpec
import getl.models.sub.DatasetsModel
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
    /** Repository connection name for source datasets */
    void setSourceConnectionName(String value) { useSourceConnection(value) }

    /** Connection for source datasets */
    @JsonIgnore
    Connection getSourceConnection() { modelConnection }

    /** Use specified connection for source datasets */
    void useSourceConnection(String connectionName) { useModelConnection(connectionName) }
    /** Use specified connection for source datasets */
    void useSourceConnection(Connection connection) { useModelConnection(connection) }


    /** Used mapping datasets */
    List<MapTableSpec> getUsedMapping() { usedObjects as List<MapTableSpec> }

    /** Repository connection name for destination datasets */
    String getDestinationConnectionName() { params.destinationConnectionName as String }
    /** Repository connection name for destination datasets */
    void setDestinationConnectionName(String value) { useDestinationConnection(value) }

    /** Connection for destination datasets */
    @JsonIgnore
    Connection getDestinationConnection() { dslCreator.connection(destinationConnectionName) }

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
    void checkModel(Boolean checkObjects = true) {
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