//file:noinspection unused
package getl.models.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.JDBCDataset
import getl.models.SetOfTables
import getl.models.sub.DatasetSpec
import groovy.transform.InheritConstructors

/**
 * Table specification
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class TableSpec extends DatasetSpec {
    /** Owner list tables model */
    protected SetOfTables getOwnerSetOfTables() { ownerModel as SetOfTables }

    /** Table name */
    String getSourceTableName() { datasetName }
    /** Table name */
    void setSourceTableName(String value) { datasetName = value }
    /** Table */
    @JsonIgnore
    JDBCDataset getSourceTable() { ownerModel.dslCreator.dataset(datasetName) as JDBCDataset }
}