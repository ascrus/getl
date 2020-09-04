package getl.models.opts

import getl.jdbc.TableDataset
import getl.models.SetOfTables
import groovy.transform.InheritConstructors

/**
 * Table specification
 * @author ALexsey Konstantinov
 */
@InheritConstructors
class TableSpec extends DatasetSpec {
    /** Owner list tables model */
    protected SetOfTables getOwnerSetOfTables() { ownerModel as SetOfTables }

    /** Repository table name */
    String getSourceTableName() { datasetName }
    /** Repository table */
    TableDataset getSourceTable() { ownerModel.dslCreator.jdbcTable(datasetName) }
}