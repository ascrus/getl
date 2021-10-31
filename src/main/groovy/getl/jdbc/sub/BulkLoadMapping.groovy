package getl.jdbc.sub

/**
 * Map for bulk loading csv file to jdbc table
 * @author Alexsey Konstantinov
 */
class BulkLoadMapping {
    /** Jdbc table field name */
    private String destinationFieldName
    /** Jdbc table field name */
    String getDestinationFieldName() { this.destinationFieldName }
    /** Jdbc table field name */
    void setDestinationFieldName(String value) { this.destinationFieldName = value?.toLowerCase() }

    /** Csv file field name*/
    private String sourceFieldName
    /** Csv file field name*/
    String getSourceFieldName() { this.sourceFieldName }
    /** Csv file field name*/
    void setSourceFieldName(String value) { this.sourceFieldName = value?.toLowerCase() }

    /** Transformation expression */
    private String expression
    /** Transformation expression */
    String getExpression() { this.expression }
    /** Transformation expression */
    void setExpression(String value) { this.expression = value }

    String toString() {
        def e = expression
        if (e == null)
            e = '<none>'
        else if (e.length() == 0)
            e = '<except>'
        return "$destinationFieldName <= $sourceFieldName | $e"
    }
}