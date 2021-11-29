package getl.csv.proc

import getl.utils.StringUtils
import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.util.CsvContext

/**
 * Parse escaped string fields for read CSV files
 * @author Alexsey Konstantinov
 */
@CompileStatic
class CSVParseEscapeString extends CellProcessorAdaptor implements StringCellProcessor {
    CSVParseEscapeString() {
        super()
    }

    CSVParseEscapeString(StringCellProcessor next) {
        super(next)
    }

    @Override
    <T> T execute(final Object value, final CsvContext context) {
        return next.execute(StringUtils.UnescapeJava(value as String), context)
    }
}
