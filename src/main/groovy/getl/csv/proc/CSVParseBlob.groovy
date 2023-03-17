//file:noinspection unused
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
	CSVParseBlob(Boolean pureFormat, String blobPrefix) {
		super()
		this.pureFormat = pureFormat
		this.blobPrefix = blobPrefix
		this.prefixLength = blobPrefix.length()
	}

	CSVParseBlob(Boolean pureFormat, String blobPrefix, StringCellProcessor next) {
		super(next)
		this.pureFormat = pureFormat
		this.blobPrefix = blobPrefix
		this.prefixLength = blobPrefix.length()
	}

	private Boolean pureFormat
	private String blobPrefix
	private Integer prefixLength

	@Override
    <T> T execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof String))
			throw new SuperCsvCellProcessorException(String, value, context, this)

		def str = value as String
		byte[] result
		if (!pureFormat) {
			if (str.length() <= prefixLength || str.substring(0, prefixLength) != blobPrefix)
				throw new CSVException('#csv.invalid_blob')

			result = StringUtils.HexToRaw(str.substring(prefixLength))
		}
		else
			result = StringUtils.HexToRaw(str)

		return next.execute(result, context)
	}
}