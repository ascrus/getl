/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.jdbc.opts

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
        if (params.indexes == null) params.indexes = [:] as Map<String, Object>
    }

    /**
     * Create table if not exists
     */
    Boolean getIfNotExists() { params.ifNotExists as Boolean }
    /**
     * Create table if not exists
     */
    void setIfNotExists(Boolean value) { params.ifNotExists = value }

    /**
     * Create commit preserve rows for temporary table
     */
    Boolean getOnCommit() { params.onCommit as Boolean }
    /**
     * Create commit preserve rows for temporary table
     */
    void setOnCommit(Boolean value) { params.onCommit = value }

    /**
     * List of indexes by table
     */
    Map<String, Object> getIndexes() { params.indexes as Map<String, Object> }
    /**
     * List of indexes by table
     */
    void setIndexes(Map<String, Object> value) {
        indexes.clear()
        if (value != null) indexes.putAll(value)
    }

    /**
     * Create hash primary key
     */
    Boolean getHashPrimaryKey() { params.hashPrimaryKey as Boolean }
    /**
     * Create hash primary key
     */
    void setHashPrimaryKey(Boolean value ) { params.hashPrimaryKey = value }

    /**
     * Create field by name of native database type
     */
    Boolean getUseNativeDBType() { params.useNativeDBType as Boolean }
    /**
     * Create field by name of native database type
     */
    void setUseNativeDBType(Boolean value) { params.useNativeDBType = value }

    /**
     * Object type
     */
    JDBCDataset.Type getType() { params.type as JDBCDataset.Type }
    /**
     * Object type
     */
    void setType(JDBCDataset.Type value) { params.type = value }

    /**
     * Create new parameters object for create index
     */
    protected IndexSpec newIndexParams(Boolean useExternalParams, Map<String, Object> opts) {
        new IndexSpec(ownerObject, useExternalParams, opts)
    }

    /**
     * Generate new parameters object for create index
     */
    protected IndexSpec genIndex(String name, Closure cl) {
        def indexOpts = indexes.get(name) as  Map<String, Object>
        if (indexOpts == null) {
            indexOpts = [:] as  Map<String, Object>
            indexes.put(name, indexOpts)
        }
        def parent = newIndexParams(true, indexOpts)
        parent.runClosure(cl)

        return parent
    }

    /**
     * Generate index of specified parameters
     */
    IndexSpec index(String name,
                    @DelegatesTo(IndexSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.IndexSpec'])
                            Closure cl = null) {
        genIndex(name, cl)
    }
}