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
            params.map = new LinkedHashMap<String, String>()
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

    /** source table mapping rules: map.put('sourceField', 'expression') */
    LinkedHashMap<String, String> getMap() { params.map as LinkedHashMap<String, String> }
    /** source table mapping rules: map.put('sourceField', 'expression') */
    void setMap(LinkedHashMap<String, String> value) {
        map.clear()
        if (value != null)
            map.putAll(value)
    }
}