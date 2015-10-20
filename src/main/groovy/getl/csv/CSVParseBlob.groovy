package getl.csv

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext
import getl.utils.*

class CSVParseBlob extends CellProcessorAdaptor {
	@Override
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof String)) {
			throw new SuperCsvCellProcessorException(String.class, value, context, this);
		}

		def result = StringUtils.HexToRaw(value)
		return next.execute(result, context)
	}

}
