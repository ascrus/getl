package getl.csv.proc

import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext
import getl.utils.*
import java.sql.Clob

class CSVFmtClob extends CellProcessorAdaptor implements StringCellProcessor {
	CSVFmtClob() {
		super()
	}

	CSVFmtClob(StringCellProcessor next) {
		super(next)
	}

	@CompileStatic
	@Override
    <T> T execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		String result
		if (value instanceof String) {
			result = value
		}
		else if (value instanceof GString) {
			result = value.toString()
		}
		else if (value instanceof Clob) {
			Clob text = (Clob)value
			result = (text.getSubString(1, (int)text.length()))
		}
		else {
			throw new SuperCsvCellProcessorException(String, value, context, this)
		}
		
		return next.execute(result, context)
	}
}
