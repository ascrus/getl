//file:noinspection unused
package getl.models.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.FileDataset
import getl.exception.DatasetError
import getl.exception.ModelError
import getl.exception.RequiredParameterError
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.models.MapTables
import getl.models.sub.DatasetSpec
import groovy.transform.InheritConstructors
import java.util.concurrent.CopyOnWriteArrayList

@InheritConstructors
class MapTableSpec extends DatasetSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.listPartitions == null)
            params.listPartitions = new CopyOnWriteArrayList(new ArrayList())
        if (params.map == null)
            params.map = new LinkedHashMap<String, String>()
    }

    /** Owner processing model */
    protected MapTables getOwnerMapModel() { ownerModel as MapTables }

    /** Source dataset name */
    String getSourceName() { datasetName }
    /** Source dataset name */
    void setSourceName(String value) { datasetName = value }
    /** Source dataset */
    @JsonIgnore
    Dataset getSource() { modelDataset }

    /** Source dataset as JDBC table */
    @JsonIgnore
    TableDataset getSourceTable() { source as TableDataset }

    /** Source dataset as JDBC query */
    @JsonIgnore
    QueryDataset getSourceQuery() { source as QueryDataset }

    /** Source dataset as file dataset */
    @JsonIgnore
    FileDataset getSourceFile() { source as FileDataset }

    /** Destination dataset name */
    String getDestinationName() { params.destinationName as String }
    /** Destination dataset name */
    void setDestinationName(String value) { linkTo(value) }
    /** Set destination table name */
    void linkTo(String destinationName) {
        checkGetlInstance()

        if (destinationName == null)
            throw new RequiredParameterError(ownerModel, 'destinationName', sourceName)

        if (destinationName == sourceName)
            throw new ModelError(ownerModel, '#dsl.model.map_tables.invalid_dest_some', [table: sourceName])

        saveParamValue('destinationName', destinationName)
    }
    /** Set destination table name */
    void linkTo(Dataset destinationDataset) {
        checkGetlInstance()

        if (destinationDataset == null)
            throw new RequiredParameterError(ownerModel, 'destinationDataset', sourceName)

        def destinationDatasetName = destinationDataset.dslNameObject
        if (destinationDatasetName == null)
            throw new DatasetError(destinationDataset, '#dsl.object.not_register')

        linkTo(destinationDatasetName)
    }

    /** Destination dataset */
    @JsonIgnore
    Dataset getDestination() {
        checkGetlInstance()
        return ownerModel.dslCreator.dataset(destinationName)
    }

    /** Destination dataset as JDBC table */
    @JsonIgnore
    TableDataset getDestinationTable() { destination as TableDataset }

    /** Destination dataset as file dataset */
    @JsonIgnore
    FileDataset getDestinationFile() { destination as FileDataset }

    /** Mapping the relationship of the fields of the destination table to the source table: map.put('destinationField', 'sourceField') */
    LinkedHashMap<String, String> getMap() { params.map as LinkedHashMap<String, String> }
    /** Mapping the relationship of the fields of the destination table to the source table: map.put('destinationField', 'sourceField') */
    void setMap(LinkedHashMap<String, String> value) {
        map.clear()
        if (value != null)
            map.putAll(value)
    }
}