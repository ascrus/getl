//file:noinspection unused
package getl.models.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.TableDataset
import getl.models.SetOfTables
import getl.models.sub.DatasetSpec
import groovy.transform.InheritConstructors

/**
 * Table specification
 * @author ALexsey Konstantinov
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
    TableDataset getSourceTable() { ownerModel.dslCreator.jdbcTable(datasetName) }
}