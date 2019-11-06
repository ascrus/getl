package getl.csv.proc

import getl.utils.StringUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext

@InheritConstructors
class CSVFmtEscapeString extends CellProcessorAdaptor implements StringCellProcessor {
    @CompileStatic
    @Override
    def <T> T execute(final Object value, final CsvContext context) {
        final def result = StringUtils.EscapeJavaWithoutUTF(value as String)
        return next.execute(result, context)
    }
}
