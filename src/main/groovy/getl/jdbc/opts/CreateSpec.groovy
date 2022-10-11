//file:noinspection unused
package getl.jdbc.opts

import getl.exception.ExceptionGETL
import getl.jdbc.JDBCDataset
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Create options for creating table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CreateSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.indexes == null)
            params.indexes = new HashMap<String, Object>()
        if (params.type == null)
            params.type = JDBCDataset.tableType
    }

    /** Create table if not exists */
    Boolean getIfNotExists() { params.ifNotExists as Boolean }
    /** Create table if not exists */
    void setIfNotExists(Boolean value) { saveParamValue('ifNotExists', value) }

    /** Create commit preserve rows for temporary table */
    Boolean getOnCommit() { params.onCommit as Boolean }
    /** Create commit preserve rows for temporary table */
    void setOnCommit(Boolean value) { saveParamValue('onCommit', value) }

    /** Indexes by table */
    Map<String, Object> getIndexes() { params.indexes as Map<String, Object> }
    /** Indexes by table */
    void setIndexes(Map<String, Object> value) {
        indexes.clear()
        if (value != null) indexes.putAll(value)
    }
    /** Indexes by table */
    void assignIndexes(Map<String, Object> value) {
        indexes.clear()
        (value as Map<String, Map<String, Object>>)?.each { indexName, indexParams ->
            def par = new HashMap<String, Object>()
            indexParams.each { name, param ->
                if (name == 'columns') {
                    if (param == null)
                        param = [] as List<String>
                    else if (!(param instanceof List)) {
                        if (param instanceof String)
                            param = (param as String).split('\\|').toList()
                        else
                            throw new ExceptionGETL("Invalid type of propery \"columns\" from \"$name\" index!")
                    }
                }
                par.put(name, param)
            }
            indexes.put(indexName, par)
        }
    }

    /** Create hash primary key */
    Boolean getHashPrimaryKey() { params.hashPrimaryKey as Boolean }
    /** Create hash primary key */
    void setHashPrimaryKey(Boolean value ) { saveParamValue('hashPrimaryKey', value) }

    /** Create field by name of native database type */
    Boolean getUseNativeDBType() { params.useNativeDBType as Boolean }
    /** Create field by name of native database type */
    void setUseNativeDBType(Boolean value) { saveParamValue('useNativeDBType', value) }

    /** JDBC dataset type */
    JDBCDataset.Type getType() { params.type as JDBCDataset.Type }
    /** JDBC dataset type */
    void setType(JDBCDataset.Type value) { saveParamValue('type', value) }

    /** Create new parameters object for create index */
    protected IndexSpec newIndexParams(Boolean useExternalParams, Map<String, Object> opts) {
        new IndexSpec(ownerObject, useExternalParams, opts)
    }

    /** Generate new parameters object for create index */
    protected IndexSpec genIndex(String name, Closure cl) {
        def indexOpts = indexes.get(name) as  Map<String, Object>
        if (indexOpts == null) {
            indexOpts = new HashMap<String, Object>()
            indexes.put(name, indexOpts)
        }
        def parent = newIndexParams(true, indexOpts)
        parent.runClosure(cl)

        return parent
    }

    /** Generate index of specified parameters */
    IndexSpec index(String name,
                    @DelegatesTo(IndexSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.IndexSpec'])
                            Closure cl = null) {
        genIndex(name, cl)
    }
}