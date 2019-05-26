package getl.jdbc.opts

import getl.exception.ExceptionGETL
import getl.jdbc.JDBCConnection
import getl.lang.opts.BaseSpec
import getl.utils.MapUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

@InheritConstructors
@CompileStatic
class ScriptSpec extends BaseSpec {
    /**
     * Database connection
     */
    public JDBCConnection connection

    /**
     * Local variables
     */
    final public Map<String, Object> vars = [:]

    /**
     * Echo logging level
     */
    public String echoLogLevel = 'FINE'

    /**
     * Point history table connection
     */
    public JDBCConnection pointConnection

    /**
     * SQL script
     */
    public String script
    /**
     * SQL script
     */
    public void script(Closure cl) {
        cl = prepareClosure(cl)
        script = cl.call(this)
    }

    /**
     * Load sql file script
     */
    public void load(String fileName, String codePage = 'UTF8') {
        script = new File(fileName).getText(codePage)
    }

    /**
     * Last count rows
     */
    public Long countRow = 0

    /**
     * Import variables
     */
    void importVars(Map<String, Object> extVars, String prefix = null) {
        if (prefix == null) prefix = ''
        extVars?.each { String key, value ->
            if (value instanceof Map) {
                importVars(value as Map<String, Object>, prefix + key + '.')
            }
            else {
                vars.put(prefix + key, value)
            }
        }
    }
}