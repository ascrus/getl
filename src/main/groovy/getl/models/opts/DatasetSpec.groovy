package getl.models.opts

import getl.data.Dataset
import getl.data.FileDataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.jdbc.ViewDataset
import getl.models.sub.DatasetsModel
import getl.utils.MapUtils

import java.util.concurrent.ConcurrentHashMap

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

    @Override
    protected void initSpec() {
        super.initSpec()
        params.attrs = new ConcurrentHashMap<String, Object>()
    }

    /** Model dataset name */
    protected String getDatasetName() { params.datasetName as String }
    /** Model dataset name */
    protected void setDatasetName(String value) { params.datasetName = value }

    /** Model dataset */
    Dataset getModelDataset() { ownerModel.dslCreator.dataset(datasetName) }

    /** Mapping attributes */
    Map<String, Object> getAttrs() { params.attrs as Map<String, Object> }
    /** Mapping attributes */
    void setAttrs(Map<String, Object> value) {
        attrs.clear()
        if (value != null)
            attrs.putAll(value)
    }

    /**
     * Check attribute naming and generate an unknown error
     * @param allowAttrs list of allowed attribute names
     */
    void checkAttrs(List<String> allowAttrs) {
        if (allowAttrs == null)
            throw new ExceptionDSL('The list of attribute names in parameter "allowAttrs" is not specified!')

        def unknownKeys = MapUtils.Unknown(attrs, allowAttrs)
        if (!unknownKeys.isEmpty())
            throw new ExceptionDSL("Unknown attributes were detected in \"$datasetName\": $unknownKeys, allow attributes: $allowAttrs")
    }

    @Override
    String toString() { datasetName }
}