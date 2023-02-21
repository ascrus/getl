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
	CSVFmtBlob(Boolean pureFormat) {
		super()
		this.pureFormat = pureFormat
	}

	CSVFmtBlob(Boolean pureFormat, StringCellProcessor next) {
		super(next)
		this.pureFormat = pureFormat
	}

	private Boolean pureFormat

	@Override
    <T> T execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof byte[])) {
			byte[] b = []
			throw new SuperCsvCellProcessorException((b.getClass()), value, context, this)
		}
		
		String result = (!pureFormat)?'\\x' + StringUtils.RawToHex((byte[])value):StringUtils.RawToHex((byte[])value)
		return next.execute(result, context)
	}
}