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
    final String HASH_STRATEGY = 'HASH'
    /** Treemap lookup result */
    final String ORDER_STRATEGY = 'ORDER'

    /** Lookup key field name */
    String getKey() { params.key }
    /** Lookup key field name */
    void setKey(String value) { params.key = value }

    /** Result lookup strategy */
    String getStrategy() { params.strategy }
    /** Result lookup strategy */
    void setStrategy(String value) { params.strategy = value }
}
