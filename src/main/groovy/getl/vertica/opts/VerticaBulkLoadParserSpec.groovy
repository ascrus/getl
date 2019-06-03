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

package getl.vertica.opts

import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

/**
 * Parser options for bulk loading Vertica table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VerticaBulkLoadParserSpec extends BaseSpec {
    VerticaBulkLoadParserSpec() {
        super()
        params.options = [:] as Map<String, Object>
    }

    VerticaBulkLoadParserSpec(Boolean useExternalParams = false, Map<String, Object> importParams) {
        super(useExternalParams, importParams)
        if (params.options == null) params.options = [:] as Map<String, Object>
    }

    /**
     * Vertica parser function name
     * <br>See <a href="https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/AdministratorsGuide/BulkLoadCOPY/SpecifyCopyParser.htm">Vertica documentation</a>
     */
    String getFunction() { params.function as String }
    /**
     * Vertica parser function name
     * <br>See <a href="https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/AdministratorsGuide/BulkLoadCOPY/SpecifyCopyParser.htm">Vertica documentation</a>
     */
    void setFunction(String value) { params.function = value }

    /**
     * Options
     * <br>P.S. read about options in the Vertica documentation of the selected parser function
     */
    Map<String, Object> getOptions() { params.options as Map<String, Object> }
    /**
     * Options
     * <br>P.S. read about options in the Vertica documentation of the selected parser function
     */
    void setOptions(Map<String, Object> value) {
        options.clear()
        if (value != null) options.putAll(value)
    }

    /**
     * Use CSV file options as COPY options
     */
    Boolean getUseCsvOptions() { params.useCsvOptions as Boolean }
    /**
     * Use CSV file options as COPY options
     */
    void setUseCsvOptions(Boolean value) { params.useCsvOptions = value }
}