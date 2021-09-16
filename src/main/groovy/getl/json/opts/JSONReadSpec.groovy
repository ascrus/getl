package getl.json.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.opts.FileReadSpec
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Options for reading JSON file
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class JSONReadSpec extends FileReadSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.fields == null)
            params.fields = [] as List<String>
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
    void readAttributes(@ClosureParams(value = SimpleType, options = ['getl.json.JSONDataset']) Closure<Boolean> value) {
        setOnReadAttributes(value)
    }

    /** Wrap json text in root */
    Boolean getConvertToList () { params.convertToList as Boolean }
    /** Wrap json text in root */
    void setConvertToList (Boolean value) { params.convertToList = value }
}