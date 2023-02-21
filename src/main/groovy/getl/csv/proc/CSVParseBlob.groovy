package getl.csv.proc

import getl.exception.CSVException
import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext
import getl.utils.*

/**
 * Parse blob fields for read CSV files
 * @author Alexsey Konstantinov
*/
@CompileStatic
class CSVParseBlob extends CellProcessorAdaptor {
	CSVParseBlob(Boolean pureFormat) {
		super()
		this.pureFormat = pureFormat
	}

	CSVParseBlob(Boolean pureFormat, StringCellProcessor next) {
		super(next)
		this.pureFormat = pureFormat
	}

	private Boolean pureFormat

	@Override
    <T> T execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof String))
			throw new SuperCsvCellProcessorException(String, value, context, this)

		def str = value as String
		byte[] result
		if (!pureFormat) {
			if (str.length() < 3 || str.substring(0, 2) != '\\x')
				throw new CSVException('#csv.invalid_blob')

			result = StringUtils.HexToRaw(str.substring(2))
		}
		else
			result = StringUtils.HexToRaw(str)

		return next.execute(result, context)
	}
}