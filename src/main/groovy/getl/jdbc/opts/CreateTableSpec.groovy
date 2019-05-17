/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) 2013-2019  Alexsey Konstantonov (ASCRUS)

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

/**
 * Create options for creating table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CreateTableSpec extends BaseSpec {
    CreateTableSpec() {
        super()
        params._indexes = [:] as Map<String, IndexSpec>
    }

    /**
     * Create table if not exists
     */
    Boolean getIfNotExists() { params.ifNotExists }
    /**
     * Create table if not exists
     */
    void setIfNotExists(Boolean value) { params.ifNotExists = value }

    /**
     * Create commit preserve rows for temporary table
     */
    Boolean getOnCommit() { params.onCommit }
    /**
     * Create commit preserve rows for temporary table
     */
    void setOnCommit(Boolean value) { params.onCommit = value }

    /**
     * List of indexes by table
     */
    Map<String, IndexSpec> getIndexes() { params._indexes }

    /**
     * Create hash primary key
     */
    Boolean getHashPrimaryKey() { params.hashPrimaryKey }
    /**
     * Create hash primary key
     */
    void setHashPrimaryKey(Boolean value ) { params.hashPrimaryKey = value }

    /**
     * Create field by name of native database type
     */
    Boolean getUseNativeDBType() { params.useNativeDBType }
    /**
     * Create field by name of native database type
     */
    void setUseNativeDBType(Boolean value) { params.useNativeDBType = value }

    /**
     * This table
     */
    JDBCDataset.Type getIsTable() { JDBCDataset.Type.TABLE }

    /**
     * This global temporary table
     */
    JDBCDataset.Type getIsGlobalTemporary() { JDBCDataset.Type.GLOBAL_TEMPORARY }

    /**
     * This local temporary table
     */
    JDBCDataset.Type getIsTemporary() { JDBCDataset.Type.LOCAL_TEMPORARY }

    /**
     * Create new parameters object for create index
     */
    protected IndexSpec newIndexParams() { new IndexSpec() }

    /**
     * Generate new parameters object for create index
     */
    protected IndexSpec genIndex(IndexSpec parent, Closure cl) {
        if (parent == null) {
            parent = newIndexParams()
            parent.thisObject = parent.DetectClosureDelegate(cl)
        }
        def code = cl.rehydrate(parent.DetectClosureDelegate(cl), parent, parent.DetectClosureDelegate(cl))
        code.resolveStrategy = Closure.OWNER_FIRST
        code(parent)

        return parent
    }

    /**
     * Generate index of specified parameters
     */
    IndexSpec index(IndexSpec parent = null, @DelegatesTo(IndexSpec) Closure cl) {
        genIndex(parent, cl)
    }

    @Override
    void prepare() {
        if (!indexes.isEmpty()) {
            def i = [:] as Map<String, Object>
            params.indexes = i
            indexes.each { String name, IndexSpec idx ->
                idx.prepare()
                i.put(name, idx.params)
            }
        }
    }
}