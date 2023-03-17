//file:noinspection unused
package getl.csv.proc

import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext
import getl.utils.*

/**
 * Format blob fields from write to CSV files
 * @author Alexsey Konstantinov
 */
@CompileStatic
class CSVFmtBlob extends CellProcessorAdaptor {
	CSVFmtBlob(Boolean pureFormat, String blobPrefix) {
		super()
		this.pureFormat = pureFormat
		this.blobPrefix = blobPrefix
	}

	CSVFmtBlob(Boolean pureFormat, String blobPrefix, StringCellProcessor next) {
		super(next)
		this.pureFormat = pureFormat
		this.blobPrefix = blobPrefix
	}

	private Boolean pureFormat
	private String blobPrefix

	@Override
    <T> T execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof byte[])) {
			byte[] b = []
			throw new SuperCsvCellProcessorException((b.getClass()), value, context, this)
		}
		
		String result = (!pureFormat)?blobPrefix + StringUtils.RawToHex((byte[])value):StringUtils.RawToHex((byte[])value)
		return next.execute(result, context)
	}
}