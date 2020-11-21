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
    @Override
    protected void initSpec() {
        super.initSpec()
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
    void setFunction(String value) { saveParamValue('function', value) }

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
    void setUseCsvOptions(Boolean value) { saveParamValue('useCsvOptions', value) }
}