package getl.csv.proc

import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext
import getl.utils.*

@CompileStatic
class CSVFmtBlob extends CellProcessorAdaptor {
	CSVFmtBlob() {
		super()
	}

	CSVFmtBlob(StringCellProcessor next) {
		super(next)
	}

	@Override
    <T> T execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof byte[])) {
			byte[] b = []
			throw new SuperCsvCellProcessorException((b.getClass()), value, context, this)
		}
		
		final String result = StringUtils.RawToHex((byte[])value)
		return next.execute(result, context)
	}
}
