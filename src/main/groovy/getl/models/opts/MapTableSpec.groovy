package getl.models.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.exception.ExceptionModel
import getl.models.MapTables
import getl.models.sub.DatasetSpec
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
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
    Dataset getSource() { ownerModel.dslCreator.dataset(datasetName) }

    /** Destination dataset name */
    String getDestinationName() { params.destinationName as String }
    /** Destination dataset name */
    void setDestinationName(String value) { linkTo(value) }
    /** Set destination table name */
    void linkTo(String destinationName) {
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
    Dataset getDestination() { ownerModel.dslCreator.dataset(destinationName) }

    /** List of key values for partitions being processed */
    @Synchronized
    List getListPartitions() { params.listPartitions as List }
    /** List of key values for partitions being processed */
    @Synchronized
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
        return (partitionsDatasetName != null)?ownerModel.dslCreator.dataset(partitionsDatasetName):null
    }
    /** Get a list of partitions from the specified dataset */
    @JsonIgnore
    void setPartitionsDataset(Dataset value) { usePartitionsFrom(value) }

    /** Use a list of partitions from the specified dataset */
    void usePartitionsFrom(String listDatasetName) { partitionsDatasetName = listDatasetName }
    /** Use a list of partitions from the specified dataset */
    void usePartitionsFrom(Dataset listDataset) {
        if (listDataset != null) {
            def name = listDataset.dslNameObject
            if (name == null)
                throw new ExceptionModel("$sourceName: the dataset $listDataset must be registered in the repository!")

            partitionsDatasetName = name
        }
        else
            partitionsDatasetName = null
    }

    /** Mapping the relationship of the fields of the destination table to the source table: map.put('destinationField', 'sourceField') */
    @Synchronized
    Map<String, String> getMap() { params.map as Map<String, String> }
    /** Mapping the relationship of the fields of the destination table to the source table: map.put('destinationField', 'sourceField') */
    @Synchronized
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
    List readListPartitions(Map queryParams = null) {
        def res = listPartitions
        if (res.isEmpty() && partitionsDatasetName != null) {
            def qp = [:]
            if (queryParams != null)
                qp.put('queryParams', queryParams)
            res = partitionsDataset.rows(qp).collect { row -> row.values()[0] }
        }
        return res
    }
}