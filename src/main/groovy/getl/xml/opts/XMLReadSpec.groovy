package getl.xml.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Options for reading XML file
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class XMLReadSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
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

    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    @JsonIgnore
    Closure<Boolean> getOnReadAttributes() { params.initAttr as Closure<Boolean> }
    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    void setOnReadAttributes(Closure<Boolean> value) { saveParamValue('initAttr', value) }
    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    void readAttributes(@ClosureParams(value = SimpleType, options = ['getl.xml.XMLDataset']) Closure<Boolean> value) {
        setOnReadAttributes(value)
    }
}