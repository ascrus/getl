package getl.data.sub

import getl.data.Field

/**
 * Sub class for DetectChangeFields function in Dataset
 * @author Alexsey Konstantinov
 */
class DatasetCompareField {
    /** Dataset field */
    public Field datasetField
    /** Compared field */
    public Field comparedField

    static public final String COMPARE_FIELD_NOT_FOUND = 'field_not_found'
    static public final String COMPARE_FIELD_TYPE_NOT_COMPATIBLE = 'field_type_not_compatible'
    static public final String COMPARE_FIELD_LENGTH_NOT_COMPATIBLE = 'field_length_not_compatible'
    static public final String COMPARE_FIELD_NULL_NOT_COMPATIBLE = 'field_null_not_compatible'
    static public final String COMPARE_FIELD_KEY_NOT_COMPATIBLE = 'field_key_not_compatible'
    static public final String COMPARE_FIELD_DEFAULT_NOT_COMPATIBLE = 'field_default_not_compatible'
    static public final String COMPARE_UNNECESSARY_FIELD = 'unnecessary_field'

    @Override
    String toString() {
        def compFieldName = (comparedField != null)?"\"$comparedField\"":'<none>'
        def dsFieldName = (datasetField != null)?"\"$datasetField\"":'<none>'
        return "comparedField: $compFieldName, datasetField: $dsFieldName"
    }
}