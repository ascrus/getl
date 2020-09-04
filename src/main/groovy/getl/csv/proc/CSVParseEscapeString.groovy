package getl.csv.proc

import getl.utils.StringUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.util.CsvContext

@InheritConstructors
class CSVParseEscapeString extends CellProcessorAdaptor implements StringCellProcessor {
    @CompileStatic
    @Override
    <T> T execute(final Object value, final CsvContext context) {
        return next.execute(StringUtils.UnescapeJava(value as String), context)
    }
}
