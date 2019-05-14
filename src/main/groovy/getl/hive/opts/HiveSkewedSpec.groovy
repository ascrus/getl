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

package getl.hive.opts

import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

/**
 * Hive skewed options for creating table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class HiveSkewedSpec extends BaseSpec {
    HiveSkewedSpec() {
        super()
        params.by = [] as List<String>
        params.on = [] as List<String>
    }

    /**
     * List of "by" columns
     */
    List<String> getBy() { params.by }
    void setBy(List<String> value) { params.by = value }

    /**
     * List of "on" columns
     */
    List<String> getOn() { params.on }
    void setOn(List<String> value) { params.on = value }

    /**
     * Stored data as directories
     */
    Boolean getStoredAsDirectories() { params.storedAsDirectories }
    void setStoredAsDirectories(Boolean value) { params.storedAsDirectories = value }
}