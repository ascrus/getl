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

package getl.xml.opts

import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

/**
 * Options for reading XML file
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class XMLReadSpec extends BaseSpec {
    XMLReadSpec() {
        super()
        params.fields = [] as List<String>
    }

    XMLReadSpec(Boolean useExternalParams, Map<String, Object> importParams) {
        super(useExternalParams, importParams)
        if (params.fields == null) params.fields = [] as List<String>
    }

    /** List of fields to read
     * <br>if not specified, then all fields are taken
     */
    List<String> getFields() { params.fields as List<String> }
    /** List of fields to read
     * <br>if not specified, then all fields are taken
     */
    void setFields(List<String> value) {
        fields.clear()
        if (value != null) fields.addAll(value)
    }

    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    Closure<Boolean> getFilter() { params.filter as Closure<Boolean> }
    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    void filter(Closure<Boolean> value) { params.filter = prepareClosure(value) }

    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    Closure<Boolean> getInitAttr() { params.initAttr as Closure<Boolean> }
    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    void initAttr(Closure<Boolean> value) { params.initAttr = prepareClosure(value) }
}