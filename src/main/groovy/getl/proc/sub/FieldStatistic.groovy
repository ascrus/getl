package getl.proc.sub

import groovy.transform.CompileStatic

/**
 * Source field processing statistic
 * @author Alexsey Konstantinov
 */
@CompileStatic
class FieldStatistic {
    FieldStatistic(String name, Comparable value) {
        fieldName = name
        minimumValue = value
        maximumValue = value
    }

    /** Field name */
    public String fieldName
    /** Minimum field value for processed rows */
    public Comparable minimumValue
    /** Maximum field value for processed rows */
    public Comparable maximumValue
}
