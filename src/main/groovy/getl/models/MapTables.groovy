//file:noinspection unused
package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.data.FileConnection
import getl.driver.Driver
import getl.exception.ConnectionError
import getl.exception.ModelError
import getl.exception.RequiredParameterError
import getl.jdbc.JDBCConnection
import getl.models.opts.MapTableSpec
import getl.models.sub.DatasetsModel
import getl.proc.sub.ExecutorListElement
import getl.utils.CloneUtils
import getl.utils.MapUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Mapping tables model
 * @author Alexsey Konstantinov
 */
@CompileStatic
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

    /** Source connection as JDBC connection */
    @JsonIgnore
    JDBCConnection getSourceJdbcConnection() { sourceConnection as JDBCConnection }

    /** Source connection as file connection */
    @JsonIgnore
    FileConnection getSourceFileConnection() { sourceConnection as FileConnection }

    /** Used mapping datasets */
    List<MapTableSpec> getUsedMapping() { usedObjects as List<MapTableSpec> }
    /** Used mapping datasets */
    void setUsedMapping(List<MapTableSpec> value) {
        usedMapping.clear()
        if (value != null)
            usedMapping.addAll(value)
    }
    /** Convert a list of parameters to usable map rules */
    void assignUsedMapping(List<Map> value) {
        def own = this
        def list = [] as List<MapTableSpec>
        value?.each { node ->
            def p = CloneUtils.CloneMap(node, true)
            p.datasetName = p.sourceName

            p.remove('id')
            p.remove('index')
            p.remove('sourceName')

            MapUtils.RemoveKeys(p) { k, v ->
                return (v == null) || (v instanceof String && (v as String).length() == 0) || (v instanceof GString && (v as GString).length() == 0)
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
            throw new RequiredParameterError(this, 'connectionName', 'useDestinationConnection')
        dslCreator.connection(connectionName)

        saveParamValue('destinationConnectionName', connectionName)
    }
    /** Use specified connection for destination datasets */
    void useDestinationConnection(Connection connection) {
        if (connection == null)
            throw new RequiredParameterError(this, 'connection', 'useDestinationConnection')
        if (connection.dslNameObject == null)
            throw new ConnectionError(connection, '#dsl.object.not_register')

        saveParamValue('destinationConnectionName', connection.dslNameObject)
    }

    /** Destination connection as JDBC connection */
    @JsonIgnore
    JDBCConnection getDestinationJdbcConnection() { destinationConnection as JDBCConnection }

    /** Destination connection as file connection */
    @JsonIgnore
    FileConnection getDestinationFileConnection() { destinationConnection as FileConnection }

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
        addDatasets(mask: maskName, code: cl)
    }

    private final Object synchModel = synchObjects

    /**
     * Check model
     * @param checkObjects check model object parameters
     * @param checkDestination check destination tables
     * @param checkMapping check mapping fields
     * @param checkNodeCode additional validation code for model objects
     */
    @Synchronized('synchModel')
    void checkModel(Boolean checkObjects = true, Boolean checkDestination = true, Boolean checkMapping = true,
                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.MapTableSpec']) Closure checkNodeCode = null) {
        super.checkModel(false)

        if (destinationConnectionName == null && checkDestination)
            throw new ModelError(this, '#dsl.model.non_dest_connection')

        if (checkObjects)
            usedMapping.each { obj ->
                checkMapTable(obj, checkDestination, checkMapping)
                if (checkNodeCode != null)
                    checkNodeCode.call(obj)
            }
    }

    /**
     * Check map table of model
     * @param obj object of model
     * @param checkDestination check destination tables
     * @param checkMapping check mapping fields
     */
    @Synchronized('synchModel')
    protected void checkMapTable(MapTableSpec obj, Boolean checkDestination = true, Boolean checkMapping = true) {
        super.checkObject(obj)

        def node = obj as MapTableSpec

        if (node.destinationName == null && checkDestination)
            throw new ModelError(this, '#dsl.model.non_dest_table', [table: node.sourceName])

        if (node.destinationName != null) {
            if (dslCreator.findDataset(node.destinationName) == null)
                throw new ModelError(this, '#dsl.model.dest_table_not_found', [table: node.sourceName, destTable: node.destinationName])

            checkDataset(node.destination)
            checkModelDataset(node.destination, destinationConnectionName)

            if (!node.destination.field.isEmpty() && checkMapping) {
                node.map.each { destName, expr ->
                    if (!destName.matches('^[*].+') &&  node.destination.fieldByName(destName) == null)
                        throw new ModelError(this, '#dsl.model.invalid_destination_field',
                                [table: node.sourceName, field: destName, destTable: node.destinationName])
                }
            }
        }
    }

    @Override
    String toString() { "mapTables('${dslNameObject?:'unregister'}')" }

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

    /**
     * Find table in model
     */
    MapTableSpec findMapTable(String name) { findModelObject(name) as MapTableSpec }

    @Override
    void doneModel() {
        super.doneModel()
        usedDatasets.each { node ->
            if (node.destinationName != null && node.destination.connection.driver.isSupport(Driver.Support.CONNECT))
                node.destination.connection.connected = false
        }
    }
}