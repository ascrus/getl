//file:noinspection unused
package getl.models.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.FileDataset
import getl.exception.ExceptionModel
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
            params.map = [:] as Map<String, String>
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
            throw new ExceptionModel("$sourceName: destination name can not be null!")

        def ds = ownerModel.dslCreator.dataset(destinationName)
        def name = ds.dslNameObject
        if (name == datasetName)
            throw new ExceptionModel("$sourceName: you cannot use the same dataset for source and destination!")

        saveParamValue('destinationName', name)
    }
    /** Set destination table name */
    void linkTo(Dataset destinationDataset) {
        if (destinationDataset == null)
            throw new ExceptionModel("$sourceName: destination can not be null!")

        def name = destinationDataset.dslNameObject
        if (name == null)
            throw new ExceptionModel("$sourceName: the dataset $destinationDataset must be registered in the repository!")

        if (name == datasetName)
            throw new ExceptionModel("$sourceName: you cannot use the same dataset for source and destination!")

        saveParamValue('destinationName', name)
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

    /** List of key values for partitions being processed */
    List getListPartitions() { params.listPartitions as List }
    /** List of key values for partitions being processed */
    void setListPartitions(List value) {
        listPartitions.clear()
        if (value != null)
            listPartitions.addAll(value)
    }

    /** Get a list of partitions from the specified dataset */
    String getPartitionsDatasetName() { params.partitionsDatasetName as String }
    /** Get a list of partitions from the specified dataset */
    void setPartitionsDatasetName(String value) { saveParamValue('partitionsDatasetName', value) }
    /** Get a list of partitions from the specified dataset */
    @JsonIgnore
    Dataset getPartitionsDataset() {
        checkGetlInstance()
        return (partitionsDatasetName != null)?ownerModel.dslCreator?.dataset(partitionsDatasetName):null
    }
    /** Get a list of partitions from the specified dataset */
    @JsonIgnore
    void setPartitionsDataset(Dataset value) { usePartitionsFrom(value) }

    /** Use a list of partitions from the specified dataset */
    void usePartitionsFrom(String listDatasetName) { partitionsDatasetName = listDatasetName }
    /** Use a list of partitions from the specified dataset */
    void usePartitionsFrom(Dataset value) {
        checkGetlInstance()
        if (value != null) {
            def name = value.dslNameObject
            if (name == null || value.dslCreator == null)
                throw new ExceptionModel("$sourceName: the dataset \"$value\" must be registered in the repository!")

            partitionsDatasetName = name
        }
        else
            partitionsDatasetName = null
    }

    /** Mapping the relationship of the fields of the destination table to the source table: map.put('destinationField', 'sourceField') */
    Map<String, String> getMap() { params.map as Map<String, String> }
    /** Mapping the relationship of the fields of the destination table to the source table: map.put('destinationField', 'sourceField') */
    void setMap(Map<String, String> value) {
        map.clear()
        if (value != null)
            map.putAll(value)
    }

    /**
     * Return partitions from list or dataset
     * @param queryParams parameters for getting a list of partitions from a dataset
     * @return list of partitions
     */
    List<Map<String, Object>> readListPartitions(Map queryParams = null) {
        def res = listPartitions.collect { [partition: it] }
        if (res.isEmpty() && partitionsDatasetName != null) {
            def qp = [:]
            if (queryParams != null)
                qp.put('queryParams', queryParams)
            res = partitionsDataset.rows(qp)
        }

        return res
    }
}