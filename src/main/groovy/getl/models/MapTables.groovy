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
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
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
    @Synchronized
    Connection getSourceConnection() { modelConnection }
    /** Connection for source datasets */
    @JsonIgnore
    @Synchronized
    void setSourceConnection(Connection value) { useSourceConnection(value) }

    /** Use specified connection for source datasets */
    @Synchronized
    void useSourceConnection(String connectionName) { useModelConnection(connectionName) }
    /** Use specified connection for source datasets */
    @Synchronized
    void useSourceConnection(Connection connection) { useModelConnection(connection) }

    /** Used mapping datasets */
    @Synchronized
    List<MapTableSpec> getUsedMapping() { usedObjects as List<MapTableSpec> }
    /** Used mapping datasets */
    @Synchronized
    void setUsedMapping(List<MapTableSpec> value) { usedObjects = value }
    @Synchronized
    void assignUsedMapping(List<Map> value) {
        usedMapping.clear()
        def o = this
        value?.each { node ->
            def p = CloneUtils.CloneMap(node, true)
            p.datasetName = p.sourceName
            p.remove('sourceName')
            usedMapping.add(new MapTableSpec(o, p))
        }
    }

    /** Repository connection name for destination datasets */
    @Synchronized
    String getDestinationConnectionName() { params.destinationConnectionName as String }
    /** Repository connection name for destination datasets */
    @Synchronized
    void setDestinationConnectionName(String value) { useDestinationConnection(value) }

    /** Connection for destination datasets */
    @JsonIgnore
    @Synchronized
    Connection getDestinationConnection() { dslCreator.connection(destinationConnectionName) }
    /** Connection for destination datasets */
    @JsonIgnore
    @Synchronized
    void setDestinationConnection(Connection value) { useDestinationConnection(value) }
    /** Use specified connection for destination datasets */
    @Synchronized
    void useDestinationConnection(String connectionName) {
        if (connectionName == null)
            throw new ExceptionModel('Connection name required!')
        dslCreator.connection(connectionName)

        saveParamValue('destinationConnectionName', connectionName)
    }
    /** Use specified connection for destination datasets */
    @Synchronized
    void useDestinationConnection(Connection connection) {
        if (connection == null)
            throw new ExceptionModel('Connection required!')
        if (connection.dslNameObject == null)
            throw new ExceptionModel('Connection not registered in Getl repository!')

        saveParamValue('destinationConnectionName', connection.dslNameObject)
    }

    /** Name dataset of mapping processing history */
    String getStoryDatasetName() { super.storyDatasetName }
    /** Name dataset of mapping processing history */
    void setStoryDatasetName(String value) { super.useStoryDatasetName(value) }
    /** Dataset of mapping processing history */
    @JsonIgnore
    Dataset getStoryDataset() { super.storyDataset }
    /** Dataset of mapping processing history */
    @JsonIgnore
    void setStoryDataset(Dataset value) { super.setStoryDataset(value) }
    /** Use specified dataset name of mapping processing history */
    void useStoryDatasetName(String datasetName) { super.useStoryDatasetName(datasetName) }
    /** Use specified dataset of mapping processing history */
    void useStoryDataset(Dataset dataset) { super.useStoryDataset(dataset) }

    /**
     * Use dataset for the mapping
     * @param datasetName repository dataset name
     * @param cl defining code
     * @return mapping spec
     */
    @Synchronized
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
    @Synchronized
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
    @Synchronized
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
    @Synchronized
    void checkObject(BaseSpec obj) {
        super.checkObject(obj)
        validDataset((obj as MapTableSpec).destination, destinationConnectionName)
    }

    /**
     * Check mapping objects
     * @param cl validation code
     */
    @Synchronized
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