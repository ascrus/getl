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

package getl.data.opts

import getl.driver.FileDriver
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * File dataset retrieve objects options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FileDatasetRetrieveObjectsSpec extends BaseSpec {
    /** File location path relative to the connection path */
    String getDirectory() { params.directory as String }
    /** File location path relative to the connection path */
    void setDirectory(String value) { params.directory = value }

    /** Regular file search mask */
    String getMask() { params.mask as String?:'.*' }
    /** Regular file search mask */
    void setMask(String value) { params.mask = value }

    /** Mask search type */
    FileDriver.RetrieveObjectType getType() { params.type as FileDriver.RetrieveObjectType?:FileDriver.RetrieveObjectType.FILE}
    /** Mask search type */
    void setType(FileDriver.RetrieveObjectType value) { params.type = value }

    /** File type search by mask */
    public static final FileDriver.RetrieveObjectType fileType = FileDriver.RetrieveObjectType.FILE
    /** Catalog type search by mask */
    public static final FileDriver.RetrieveObjectType directoryType = FileDriver.RetrieveObjectType.DIR

    /** Way to sort the results */
    FileDriver.RetrieveObjectSort getSort() { params.sort as FileDriver.RetrieveObjectSort }
    /** Way to sort the results */
    void setSort(FileDriver.RetrieveObjectSort value) { params.sort = value }

    /** No sorting required */
    public static final FileDriver.RetrieveObjectSort noneSort = FileDriver.RetrieveObjectSort.NONE
    /** Sort by name */
    public static final FileDriver.RetrieveObjectSort nameSort = FileDriver.RetrieveObjectSort.NAME
    /** Sort by create date of file */
    public static final FileDriver.RetrieveObjectSort dateSort = FileDriver.RetrieveObjectSort.DATE
    /** Sort by size of file */
    public static final FileDriver.RetrieveObjectSort sizeSort = FileDriver.RetrieveObjectSort.SIZE

    /** Recursive directory processing */
    Boolean getRecursive() { params.recursive }
    /** Recursive directory processing */
    void setRecursive(Boolean value) { params.recursive = value }

    /** Custom file filtering */
    Closure<Boolean> getOnFilter() { params.filter as Closure }
    /** Custom file filtering */
    void setOnFilter(Closure<Boolean> value) { params.filter = value }
    /** Custom file filtering */
    void filter(@ClosureParams(value = SimpleType, options = ['java.io.File'])
                        Closure<Boolean> value) {
        setOnFilter(prepareClosure(value))
    }
}
