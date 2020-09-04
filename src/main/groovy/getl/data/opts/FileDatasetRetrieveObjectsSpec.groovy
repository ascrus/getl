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
    static public final FileDriver.RetrieveObjectType fileType = FileDriver.RetrieveObjectType.FILE
    /** Catalog type search by mask */
    static public final FileDriver.RetrieveObjectType directoryType = FileDriver.RetrieveObjectType.DIR

    /** Way to sort the results */
    FileDriver.RetrieveObjectSort getSort() { params.sort as FileDriver.RetrieveObjectSort }
    /** Way to sort the results */
    void setSort(FileDriver.RetrieveObjectSort value) { params.sort = value }

    /** No sorting required */
    static public final FileDriver.RetrieveObjectSort noneSort = FileDriver.RetrieveObjectSort.NONE
    /** Sort by name */
    static public final FileDriver.RetrieveObjectSort nameSort = FileDriver.RetrieveObjectSort.NAME
    /** Sort by create date of file */
    static public final FileDriver.RetrieveObjectSort dateSort = FileDriver.RetrieveObjectSort.DATE
    /** Sort by size of file */
    static public final FileDriver.RetrieveObjectSort sizeSort = FileDriver.RetrieveObjectSort.SIZE

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
        setOnFilter(value)
    }
}
