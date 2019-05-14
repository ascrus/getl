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

import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

/**
 * Index options for creating table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class IndexSpec extends BaseSpec {
    IndexSpec() {
        super()
        params.columns = [] as List<String>
    }

    /**
     * List of column by index
     */
    List<String> getColumns() { params.columns }
    /**
     * List of column by index
     */
    void setColumns(List<String> value) { params.columns = value }

    /**
     * Create unique index
     */
    Boolean getUnique() { params.unique }
    /**
     * Create unique index
     */
    void setUnique(Boolean value) { params.unique = value }

    /**
     * Create hash index
     */
    Boolean getHash() { params.hash }
    /**
     * Create hash index
     */
    void setHash(Boolean value) { params.hash = value }

    /**
     * Create index if not exists
     */
    Boolean getIfNotExists() { params.ifNotExists }
    /**
     * Create index if not exists
     */
    void setIfNotExists(Boolean value) { params.ifNotExists = value }
}