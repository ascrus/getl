package getl.csv.proc

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext

/**
 * Parse array fields for read CSV files
 * @author Alexsey Konstantinov
 */
@CompileStatic
class CSVParseArray extends  CellProcessorAdaptor{
    CSVParseArray() {
        super()
    }

    private final JsonSlurper json = new JsonSlurper()

    @Override
    <T> T execute(final Object value, final CsvContext context) {
        validateInputNotNull(value, context)

        if (!(value instanceof String)) {
            throw new SuperCsvCellProcessorException(String, value, context, this)
        }

        final def result = json.parse(new StringReader(value as String)) as List
        return next.execute(result, context)
    }
}