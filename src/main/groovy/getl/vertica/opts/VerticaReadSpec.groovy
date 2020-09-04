package getl.vertica.opts

import getl.exception.ExceptionGETL
import getl.jdbc.opts.ReadSpec
import groovy.transform.InheritConstructors

/**
 * Options for reading vertica table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VerticaReadSpec extends ReadSpec {
    /** Label vertica hint */
    String getLabel() { params.label as String }
    /** Label vertica hint */
    void setLabel(String value) { params.label = value }

    /** The percentage of sampling returned table rows */
    Integer getTablesample() { params.tablesample as Integer }
    /** The percentage of sampling returned table rows */
    void setTablesample(Integer value) {
        if (value != null && value <= 0)
            throw new ExceptionGETL('Parameter "tablesample" must be greater than zero!')
        if (value != null && value > 100)
            throw new ExceptionGETL('Parameter "tablesample" should be no more than 100!')
        params.tablesample = value
    }
}