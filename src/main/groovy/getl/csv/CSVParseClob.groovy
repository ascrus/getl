package getl.csv

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext
import getl.utils.*
import javax.sql.rowset.serial.SerialClob

class CSVParseClob extends CellProcessorAdaptor {
	@groovy.transform.CompileStatic
	@Override
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof String)) {
			throw new SuperCsvCellProcessorException(String.class, value, context, this)
		}

		def result = new SerialClob(((String)value).chars)
		return next.execute(result, context)
	}

}
