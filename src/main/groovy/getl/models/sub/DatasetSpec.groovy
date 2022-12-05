//file:noinspection unused
package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.exception.ExceptionModel
import getl.jdbc.HistoryPointManager
import getl.utils.MapUtils
import groovy.transform.InheritConstructors

/**
 * Base model dataset specification
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class DatasetSpec extends BaseSpec {
    DatasetSpec(DatasetsModel model, String tableName) {
        super(model)
        setDatasetName(tableName)
    }

    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.scripts == null)
            params.scripts = [:] as Map<String, String>
    }

    /** Owner processing model */
    protected DatasetsModel getOwnerDatasetsModel() { ownerModel as DatasetsModel }

    /** Model dataset name */
    protected String getDatasetName() { params.datasetName as String }
    /** Model dataset name */
    protected void setDatasetName(String value) { saveParamValue('datasetName', value) }

    /** Incremental manager for storing the last data capture point from the dataset */
    String getHistoryPointName() { params.historyPointName as String }
    /** Incremental manager for storing the last data capture point from the dataset */
    void setHistoryPointName(String value) { saveParamValue('historyPointName', value) }
    /** Incremental manager for storing the last data capture point from the dataset */
    @JsonIgnore
    HistoryPointManager getHistoryPointObject() {
        checkGetlInstance()
        return (historyPointName != null)?ownerModel.dslCreator.historypoint(historyPointName):null
    }
    /** Incremental manager for storing the last data capture point from the dataset */
    @JsonIgnore
    void setHistoryPointObject(HistoryPointManager value) { useHistoryPointObject(value) }
    /** Incremental manager for storing the last data capture point from the dataset */
    void useHistoryPointObject(HistoryPointManager value) {
        checkGetlInstance()

        if (value != null) {
            def name = value.dslNameObject
            if (name == null || value.dslCreator == null)
                throw new ExceptionModel("$datasetName: the history point manager \"$value\" must be registered in the repository!")

            historyPointName = name
        }
        else
            historyPointName = null
    }

    /** Field name for getting incremental values */
    String getIncrementFieldName() { params.incrementFieldName as String }
    /** Field name for getting incremental values */
    void setIncrementFieldName(String value) { saveParamValue('incrementFieldName', value) }

    /** Parent dataset name to which the current one will be attached */
    String getParentDatasetName() { params.parentDatasetName as String }
    /** Parent dataset name to which the current one will be attached */
    void setParentDatasetName(String value) { attachToParentDataset(value) }
    /** Attach the dataset as a child to the model's parent dataset */
    void attachToParentDataset(String parentDatasetName, String linkFieldName = null) {
        checkGetlInstance()

        if (parentDatasetName != null) {
            if (ownerDatasetsModel.findModelObject(parentDatasetName) == null)
                throw new ExceptionModel("$datasetName: dataset is trying to refer to a non-existent parent dataset \"$parentDatasetName\"!")

            if (datasetName == parentDatasetName)
                throw new ExceptionModel("$datasetName: cannot use the same dataset for source and parent dataset!")

            if ((ownerDatasetsModel.findModelObject(parentDatasetName) as DatasetSpec).parentDatasetName != null)
                throw new ExceptionModel("$datasetName: not allowed to refer to a dataset \"$parentDatasetName\" that is itself a child!")
        }

        saveParamValue('parentDatasetName', parentDatasetName)

        if (linkFieldName != null)
            setParentLinkFieldName(linkFieldName)
    }

    /** Parent dataset name to which the current one will be attached */
    @JsonIgnore
    Dataset getParentDataset() {
        if (parentDatasetName == null)
            return null

        checkGetlInstance()
        return ownerModel.dslCreator.dataset(parentDatasetName)
    }
    /** Parent dataset name to which the current one will be attached */
    void setParentDataset(Dataset value) { attachToParentDataset(value) }
    /** Attach the dataset as a child to the model's parent dataset */
    void attachToParentDataset(Dataset parentDataset, String linkFieldName = null) {
        checkGetlInstance()

        String parentDatasetName = null
        if (parentDataset != null) {
            parentDatasetName = parentDataset.dslNameObject
            if (parentDatasetName == null)
                throw new ExceptionModel("$datasetName: the dataset \"$parentDataset\" must be registered in the repository!")
        }

        attachToParentDataset(parentDatasetName, linkFieldName)
    }

    /** The name of the field from the parent dataset to link to the current one */
    String getParentLinkFieldName() { params.parentLinkFieldName as String }
    /** The name of the field from the parent dataset to link to the current one */
    void setParentLinkFieldName(String value) { saveParamValue('parentLinkFieldName', value) }

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
                throw new ExceptionModel("$datasetName: the dataset \"$value\" must be registered in the repository!")

            partitionsDatasetName = name
        }
        else
            partitionsDatasetName = null
    }

    /**
     * Return partitions from list or dataset
     * @param queryParams parameters for getting a list of partitions from a dataset
     * @return list of partitions
     */
    List<Map<String, Object>> readListPartitions(Map queryParams = null) {
        def res = listPartitions.collect { [partition: it] }
        if (res.isEmpty() && partitionsDatasetName != null) {
            def qp = new HashMap()
            if (queryParams != null)
                qp.put('queryParams', queryParams)
            res = partitionsDataset.rows(qp)
        }

        return res
    }

    /** Model dataset */
    @JsonIgnore
    protected Dataset getModelDataset() {
        checkGetlInstance()
        return ownerModel.dslCreator.dataset(datasetName)
    }

    @Override
    protected String objectNameInModel() { datasetName }

    @Override
    String toString() { datasetName }

    /** Extended scripts */
    Map<String, String> getScripts() { params.scripts as Map<String, String>}
    /** Extended scripts */
    void setScripts(Map<String, String> value) {
        scripts.clear()
        if (value != null)
            scripts.putAll(value)
    }

    /**
     * List of scripts by specified mask name
     * @param maskName mask name
     * @return list of scripts
     */
    Map<String, String> findScripts(String maskName) {
        return MapUtils.FindNodes(scripts, maskName) as Map<String, String>
    }
}