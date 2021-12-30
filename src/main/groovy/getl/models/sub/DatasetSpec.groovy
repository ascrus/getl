//file:noinspection unused
package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.Field
import getl.exception.ExceptionModel
import getl.jdbc.HistoryPointManager
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

    /** Incremental manager for storing the last data capture point from the dataset */
    String getHistoryPointName() { params.historyPointName as String }
    /** Incremental manager for storing the last data capture point from the dataset */
    void setHistoryPointName(String value) { saveParamValue('historyPointName', value) }
    /** Incremental manager for storing the last data capture point from the dataset */
    @JsonIgnore
    HistoryPointManager getHistoryPoint() {
        checkGetlInstance()
        return (historyPointName != null)?ownerModel.dslCreator.historypoint(historyPointName):null
    }
    /** Incremental manager for storing the last data capture point from the dataset */
    @JsonIgnore
    void setHistoryPoint() { }
    /** Incremental manager for storing the last data capture point from the dataset */
    void useHistoryPoint(HistoryPointManager value) {
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
}