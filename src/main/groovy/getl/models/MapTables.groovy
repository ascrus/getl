package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionModel
import getl.models.sub.BaseSpec
import getl.models.opts.MapTableSpec
import getl.models.sub.DatasetsModel
import getl.proc.sub.ExecutorListElement
import getl.utils.CloneUtils
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
    /** Repository connection name for source datasets */
    void setSourceConnectionName(String value) { useSourceConnection(value) }

    /** Connection for source datasets */
    @JsonIgnore
    Connection getSourceConnection() { modelConnection }
    /** Connection for source datasets */
    @JsonIgnore
    void setSourceConnection(Connection value) { useSourceConnection(value) }

    /** Use specified connection for source datasets */
    void useSourceConnection(String connectionName) { useModelConnection(connectionName) }
    /** Use specified connection for source datasets */
    void useSourceConnection(Connection connection) { useModelConnection(connection) }

    /** Used mapping datasets */
    List<MapTableSpec> getUsedMapping() { usedObjects as List<MapTableSpec> }
    /** Used mapping datasets */
    void setUsedMapping(List<MapTableSpec> value) { usedObjects = value }
    /** Used mapping datasets */
    void assignUsedMapping(List<Map> value) {
        def own = this
        def list = [] as List<MapTableSpec>
        value?.each { node ->
            def p = CloneUtils.CloneMap(node, true)
            p.datasetName = p.sourceName
            p.remove('sourceName')

            MapUtils.RemoveKeys(p) { k, v ->
                return (v == null) || (v instanceof String && v.length() == 0) || (v instanceof GString && v.length() == 0)
            }

            list.add(new MapTableSpec(own, p))
        }
        usedMapping = list
    }

    /** Repository connection name for destination datasets */
    String getDestinationConnectionName() { params.destinationConnectionName as String }
    /** Repository connection name for destination datasets */
    void setDestinationConnectionName(String value) { useDestinationConnection(value) }

    /** Connection for destination datasets */
    @JsonIgnore
    Connection getDestinationConnection() { dslCreator.connection(destinationConnectionName) }
    /** Connection for destination datasets */
    @JsonIgnore
    void setDestinationConnection(Connection value) { useDestinationConnection(value) }
    /** Use specified connection for destination datasets */
    void useDestinationConnection(String connectionName) {
        if (connectionName == null)
            throw new ExceptionModel('Connection name required!')
        dslCreator.connection(connectionName)

        saveParamValue('destinationConnectionName', connectionName)
    }
    /** Use specified connection for destination datasets */
    void useDestinationConnection(Connection connection) {
        if (connection == null)
            throw new ExceptionModel('Connection required!')
        if (connection.dslNameObject == null)
            throw new ExceptionModel('Connection not registered in Getl repository!')

        saveParamValue('destinationConnectionName', connection.dslNameObject)
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
        dataset(datasetName, cl)
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
        useDataset(dataset, cl)
    }

    /**
     * Add source tables to the model using the specified mask
     * @param maskName dataset search mask
     * @param cl parameter description code
     */
    void addMapTables(String maskName,
                      @DelegatesTo(MapTableSpec)
                      @ClosureParams(value = SimpleType, options = ['getl.models.opts.MapTableSpec'])
                              Closure cl = null) {
        addDatasets(maskName, cl)
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

            if (node.partitionsDatasetName != null && dslCreator.findDataset(node.partitionsDatasetName) == null)
                throw new ExceptionDSL("The dataset of the list of partitions \"${node.partitionsDatasetName}\" " +
                        "specified for table \"${node.sourceName}\" was not found!")

            if (cl != null)
                node.with(cl)
        }
    }

    @Override
    String toString() { "Mapping ${usedObjects.size()} tables from \"$sourceConnectionName\" to \"$destinationConnectionName\" connections" }

    /**
     * Add linked tables to mapping
     * @param listOfLinkTables list of linked tables
     */
    void addLinkTables(List<ExecutorListElement> listOfLinkTables,
                       @DelegatesTo(MapTableSpec)
                       @ClosureParams(value = SimpleType, options = ['getl.models.opts.MapTableSpec'])
                               Closure cl = null) {
        listOfLinkTables.each { elem ->
            mapTable(elem.source as String) {
                linkTo(elem.destination as String)
            }
            mapTable(elem.source as String, cl)
        }
    }
}