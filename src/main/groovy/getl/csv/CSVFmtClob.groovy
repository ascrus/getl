package getl.csv

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext
import getl.utils.*
import java.sql.Clob

class CSVFmtClob extends CellProcessorAdaptor {
	@groovy.transform.CompileStatic
	@Override
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof Clob) && !(value instanceof String)) {
			throw new SuperCsvCellProcessorException((byte[]).class, value, context, this)
		}

		String result
		if (value instanceof Clob) {
			Clob text = (Clob)value
			result = (text.getSubString(1, (int)text.length()))
		}
		else {
			result = value
		}
		
		return next.execute(result, context)
	}
}
