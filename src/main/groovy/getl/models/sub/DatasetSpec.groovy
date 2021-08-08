package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
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
    protected String objectNameInModel() { datasetName }

    @Override
    String toString() { datasetName }
}