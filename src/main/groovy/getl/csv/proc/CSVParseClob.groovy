package getl.csv.proc

import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext

class CSVParseClob extends CellProcessorAdaptor implements StringCellProcessor {
	CSVParseClob() {
		super()
	}

	CSVParseClob(StringCellProcessor next) {
		super(next)
	}

	@CompileStatic
	@Override
    <T> T execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof String)) {
			throw new SuperCsvCellProcessorException(String.class, value, context, this)
		}

		return next.execute(value, context)
	}

}
