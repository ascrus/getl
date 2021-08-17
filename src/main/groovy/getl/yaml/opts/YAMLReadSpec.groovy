package getl.yaml.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.opts.FileReadSpec
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Options for reading YAML file
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class YAMLReadSpec extends FileReadSpec {
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
}