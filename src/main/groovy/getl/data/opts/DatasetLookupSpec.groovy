package getl.data.opts

import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

/**
 * Dataset lookup options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class DatasetLookupSpec extends BaseSpec {
    /** Hashmap lookup result */
    static public final String HASH_STRATEGY = 'HASH'
    /** Treemap lookup result */
    static public final String ORDER_STRATEGY = 'ORDER'

    /** Lookup key field name */
    String getKey() { params.key as String }
    /** Lookup key field name */
    void setKey(String value) { saveParamValue('key', value) }

    /** Result lookup strategy */
    String getStrategy() { params.strategy as String }
    /** Result lookup strategy */
    void setStrategy(String value) { saveParamValue('strategy', value) }
}
