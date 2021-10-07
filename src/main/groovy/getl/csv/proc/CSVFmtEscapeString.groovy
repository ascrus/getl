package getl.csv.proc

import getl.utils.StringUtils
import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.util.CsvContext

@CompileStatic
class CSVFmtEscapeString extends CellProcessorAdaptor implements StringCellProcessor {
    CSVFmtEscapeString() {
        super()
    }

    CSVFmtEscapeString(StringCellProcessor next) {
        super(next)
    }

    @Override
    <T> T execute(final Object value, final CsvContext context) {
        return next.execute(StringUtils.EscapeJavaWithoutUTF(value as String), context)
    }
}