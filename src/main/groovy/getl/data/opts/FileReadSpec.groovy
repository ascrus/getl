package getl.data.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Base options for reading file
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FileReadSpec extends BaseSpec {
    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    @JsonIgnore
    Closure<Boolean> getOnFilter() { params.filter as Closure<Boolean> }
    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    void setOnFilter(Closure<Boolean> value) { saveParamValue('filter', value) }
    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    void filter(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure<Boolean> value) {
        setOnFilter(value)
    }

    /** Read no more than the specified number of rows */
    Long getLimit() { params.limit as Long }
    /** Read no more than the specified number of rows */
    void setLimit(Long value) { saveParamValue('limit', value) }
}