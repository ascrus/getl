package getl.jdbc.opts

import getl.lang.opts.BaseSpec
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors

/**
 * Create options for creating sequence
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class SequenceCreateSpec extends BaseSpec {
    /** A positive or negative integer that specifies how much to increment or decrement the sequence on each call to NEXTVAL, by default set to 1 */
    BigInteger getIncrementBy() { params.incrementBy as BigInteger }
    /** A positive or negative integer that specifies how much to increment or decrement the sequence on each call to NEXTVAL, by default set to 1 */
    void setIncrementBy(BigInteger value) { saveParamValue('incrementBy', value) }

    /** Determines the minimum value a sequence can generate */
    BigInteger getMinValue() { params.minValue as BigInteger }
    /** Determines the minimum value a sequence can generate */
    void setMinValue(BigInteger value) { saveParamValue('minValue', value) }

    /* Determines the maximum value for the sequence */
    BigInteger getMaxValue() { params.maxValue as BigInteger }
    /* Determines the maximum value for the sequence */
    void setMaxValue(BigInteger value) { saveParamValue('maxValue', value) }

    /** Sets the sequence start value to integer */
    BigInteger getStartWith() { params.startWith as BigInteger }
    /** Sets the sequence start value to integer */
    void setStartWith(BigInteger value) { saveParamValue('startWith', value) }

    /** Specifies whether to cache unique sequence numbers for faster access */
    BigInteger getCacheNumbers() { params.cacheNumbers as BigInteger }
    /** Specifies whether to cache unique sequence numbers for faster access */
    void setCacheNumbers(BigInteger value) { saveParamValue('cacheNumbers', value) }

    /** Specifies whether the sequence can wrap when its minimum or maximum values are reached */
    Boolean getIsCycle() { ConvertUtils.Object2Boolean(params.isCycle) }
    /** Specifies whether the sequence can wrap when its minimum or maximum values are reached */
    void setIsCycle(Boolean value) { saveParamValue('isCycle', value) }

    /** Only generate ddl statement witout execute */
    Boolean getDdlOnly() { ConvertUtils.Object2Boolean(params.ddlOnly) }
    /** Only generate ddl statement witout execute */
    void setDdlOnly(Boolean value) { saveParamValue('ddlOnly', value) }
}