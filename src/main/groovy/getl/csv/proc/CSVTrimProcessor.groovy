package getl.csv.proc

import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.BoolCellProcessor
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.cellprocessor.ift.DateCellProcessor
import org.supercsv.cellprocessor.ift.DoubleCellProcessor
import org.supercsv.cellprocessor.ift.LongCellProcessor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.util.CsvContext

/**
 * Trim value processor
 * @author Alexsey Konstantinov
 */
@CompileStatic
class CSVTrimProcessor extends CellProcessorAdaptor implements BoolCellProcessor, DateCellProcessor, DoubleCellProcessor,
        LongCellProcessor, StringCellProcessor {
    CSVTrimProcessor() {
        super()
        this.convertEmptyToNull = false
    }

    CSVTrimProcessor(Boolean convertEmptyToNull) {
        super()
        this.convertEmptyToNull = convertEmptyToNull
    }

    CSVTrimProcessor(CellProcessor next) {
        super(next)
        this.convertEmptyToNull = false
    }

    CSVTrimProcessor(Boolean convertEmptyToNull, CellProcessor next) {
        super(next)
        this.convertEmptyToNull = convertEmptyToNull
    }

    private Boolean convertEmptyToNull

    @Override
    <T> T execute(Object value, final CsvContext context) {
        if (value != null) {
            value = value.toString().replace('\u00A0', ' ').trim()
            if (convertEmptyToNull && value.length() == 0)
                value = null
        }

        next.execute(value, context)
    }
}