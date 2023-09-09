//file:noinspection unused
package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.Field
import getl.exception.ExceptionModel
import getl.exception.ModelError
import getl.jdbc.HistoryPointManager
import getl.utils.MapUtils
import groovy.transform.CompileStatic
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

    /** Field name for store incremental values */
    String getIncrementFieldName() { params.incrementFieldName as String }
    /** Field name for store incremental values */
    void setIncrementFieldName(String value) {
        saveParamValue('incrementFieldName', value)
        _historyPointObject = null
    }
    /** Field for store incremental values */
    @JsonIgnore
    Field getIncrementField() {
        if (incrementFieldName == null)
            return null

        def res = modelDataset.fieldByName(incrementFieldName)
        if (res == null)
            throw new ModelError(ownerDatasetsModel, '#dsl.model.datasets.increment_field_not_found', [table: datasetName, field: incrementFieldName])

        return res
    }

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
        def res = listPartitions.collect { [partition: it] } as List<Map<String, Object>>
        if (res.isEmpty() && partitionsDatasetName != null) {
            def qp = new HashMap()
            if (queryParams != null)
                qp.put('queryParams', queryParams)
            res = partitionsDataset.rows(qp) as List<Map<String, Object>>
        }

        return res
    }

    /** Model dataset */
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
        return MapUtils.FindNodes(scripts as Map<String, Object>, maskName) as Map<String, String>
    }

    protected HistoryPointManager _historyPointObject

    /** Return history point object */
    @SuppressWarnings(['UnnecessaryQualifiedReference', 'GroovyFallthrough'])
    @JsonIgnore
    HistoryPointManager getHistoryPointObject() {
        if (_historyPointObject != null)
            return _historyPointObject

        if (ownerDatasetsModel.incrementDatasetName == null || incrementFieldName == null || datasetName == null)
            return null

        def hp = ownerDatasetsModel.dslCreator.historypoint {
            dslNameObject = '#' + ownerDatasetsModel.dslNameObject.replace(':', '_') + ':' + datasetName.replace(':', '_')
            historyTableName = ownerDatasetsModel.modelIncrementDatasetName()
            switch (incrementField.type) {
                case Field.integerFieldType: case Field.bigintFieldType: case Field.numericFieldType:
                    sourceType = HistoryPointManager.identitySourceType
                    break
                case Field.dateFieldType: case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
                    sourceType = HistoryPointManager.timestampSourceType
                    break
                default:
                    throw new ModelError(ownerDatasetsModel, '#dsl.model.datasets.invalid_increment_field_type',
                            [table: datasetName, field: incrementFieldName, type: incrementField.type])
            }

            sourceName = ownerDatasetsModel.historyPointSourceName(datasetName)
        }

        _historyPointObject = hp

        return hp
    }
}