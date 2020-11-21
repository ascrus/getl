package getl.oracle.opts

import getl.jdbc.opts.ReadSpec
import groovy.transform.InheritConstructors

/**
 * Oracle table read options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class OracleReadSpec extends ReadSpec {
    /** Return the result on the specified transactional scn */
    Long getScn() { return params.scn as Long }
    /** Return the result on the specified transactional scn */
    void setScn(Long value) { saveParamValue('scn', value) }

    /** Return the result on the specified transactional timestamp */
    Date getTimestamp() { params.timestamp as Date }
    /** Return the result on the specified transactional timestamp */
    void setTimestamp(Date value) { saveParamValue('timestamp', value) }

    /** Using specified hints in the select statement */
    String getHints() { params.hints as String }
    /** Using specified hints in the select statement */
    void setHints(String value) { saveParamValue('hints', value) }

    /** Read data from the specified partition */
    String getUsePartition() { params.usePartition as String }
    /** Read data from the specified partition */
    void setUsePartition(String value) { saveParamValue('usePartition', value) }
}