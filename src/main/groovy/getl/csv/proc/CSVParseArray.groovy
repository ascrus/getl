package getl.csv.proc

import getl.exception.RequiredParameterError
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

    CSVParseArray(String openBracket, String closeBracket) {
        super()

        if (openBracket == null)
            throw new RequiredParameterError('openBracket')
        if (closeBracket == null)
            throw new RequiredParameterError('closeBracket')

        if (openBracket != '[' || closeBracket != ']') {
            this.openBracket = openBracket
            this.closeBracket = closeBracket
        }
    }

    private final JsonSlurper json = new JsonSlurper()
    private String openBracket = null
    private String closeBracket = null

    @Override
    <T> T execute(final Object value, final CsvContext context) {
        validateInputNotNull(value, context)

        if (!(value instanceof String)) {
            throw new SuperCsvCellProcessorException(String, value, context, this)
        }

        def str = value as String
        if (openBracket != null) {
            if (str.length() < openBracket.length() + closeBracket.length())
                throw new SuperCsvCellProcessorException('Invalid array field value!', context, this)

            if (str.substring(0, openBracket.length()) != openBracket)
                throw new SuperCsvCellProcessorException('Array field value does not start with an opening bracket!', context, this)

            if (str.substring(str.length() - closeBracket.length()) != closeBracket)
                throw new SuperCsvCellProcessorException('Array field value does not finish with an closing bracket!', context, this)

            str = '[' + str.substring(openBracket.length(), str.length() - closeBracket.length()) + ']'
        }

        def result = json.parse(new StringReader(str)) as List
        return next.execute(result, context)
    }
}