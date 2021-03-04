package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset

/**
 * Base model dataset specification
 * @author ALexsey Konstantinov
 */
class DatasetSpec extends BaseSpec {
    DatasetSpec(DatasetsModel model, String tableName) {
        super(model)
        setDatasetName(tableName)
    }

    DatasetSpec(DatasetsModel model, Map importParams) {
        super(model, importParams)
    }

    /** Owner processing model */
    protected DatasetsModel getOwnerDatasetsModel() { ownerModel as DatasetsModel }

    /** Model dataset name */
    protected String getDatasetName() { params.datasetName as String }
    /** Model dataset name */
    protected void setDatasetName(String value) { saveParamValue('datasetName', value) }

    /** Model dataset */
    @JsonIgnore
    Dataset getModelDataset() { ownerModel.dslCreator.dataset(datasetName) }

    @Override
    String toString() { datasetName }
}