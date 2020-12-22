package getl.csv.proc

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext
import getl.utils.*

class CSVParseBlob extends CellProcessorAdaptor {
	CSVParseBlob() {
		super()
	}

	@CompileStatic
	@Override
    <T> T execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof String)) {
			throw new SuperCsvCellProcessorException(String.class, value, context, this)
		}

		final def result = StringUtils.HexToRaw((String)value)
		return next.execute(result, context)
	}
}