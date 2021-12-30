//file:noinspection unused
package getl.models.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.FileDataset
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.models.SetOfTables
import getl.models.sub.DatasetSpec
import groovy.transform.InheritConstructors

import java.util.concurrent.CopyOnWriteArrayList

/**
 * Table specification
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class TableSpec extends DatasetSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.listPartitions == null)
            params.listPartitions = new CopyOnWriteArrayList(new ArrayList())
        if (params.map == null)
            params.map = [:] as Map<String, String>
    }

    /** Owner list tables model */
    protected SetOfTables getOwnerSetOfTables() { ownerModel as SetOfTables }

    /** Source dataset name */
    String getSourceTableName() { datasetName }
    /** Source dataset name */
    void setSourceTableName(String value) { datasetName = value }
    /** Source dataset */
    @JsonIgnore
    Dataset getSourceDataset() { ownerModel.dslCreator.dataset(datasetName) as Dataset }

    /** Source dataset as JDBC table */
    @JsonIgnore
    TableDataset getSourceTable() { sourceDataset as TableDataset }

    /** Source dataset as JDBC query */
    @JsonIgnore
    QueryDataset getSourceQuery() { sourceDataset as QueryDataset }

    /** Source dataset as file dataset */
    @JsonIgnore
    FileDataset getSourceFile() { sourceDataset as FileDataset }

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
                throw new ExceptionModel("$sourceTableName: the dataset $listDataset must be registered in the repository!")

            partitionsDatasetName = name
        }
        else
            partitionsDatasetName = null
    }

    /** source table mapping rules: map.put('sourceField', 'expression') */
    Map<String, String> getMap() { params.map as Map<String, String> }
    /** source table mapping rules: map.put('sourceField', 'expression') */
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