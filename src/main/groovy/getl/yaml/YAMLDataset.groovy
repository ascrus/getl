package getl.yaml

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.StructureFileDataset
import getl.exception.ExceptionGETL
import getl.yaml.opts.YAMLReadSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * YAML dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class YAMLDataset extends StructureFileDataset {
    @Override
    protected void initParams() {
        super.initParams()

        _driver_params = new HashMap<String, Object>()
    }

    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof YAMLConnection))
            throw new ExceptionGETL('Connection to YAMLConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    YAMLConnection useConnection(YAMLConnection value) {
        setConnection(value)
        return value
    }

    /** Current YAML connection */
    @JsonIgnore
    YAMLConnection getCurrentYAMLConnection() { connection as YAMLConnection }

    /** Read file options */
    YAMLReadSpec getReadOpts() { new YAMLReadSpec(this, true, readDirective) }

    /** Read file options */
    YAMLReadSpec readOpts(@DelegatesTo(YAMLReadSpec)
                          @ClosureParams(value = SimpleType, options = ['getl.yaml.opts.YAMLReadSpec'])
                                  Closure cl = null) {
        def parent = readOpts
        parent.runClosure(cl)

        return parent
    }
}