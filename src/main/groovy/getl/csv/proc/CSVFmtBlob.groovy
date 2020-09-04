package getl.csv.proc

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext
import getl.utils.*

@InheritConstructors
class CSVFmtBlob extends CellProcessorAdaptor {
	@CompileStatic
	@Override
    <T> T execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof byte[])) {
			byte[] b = []
			throw new SuperCsvCellProcessorException((b.class), value, context, this)
		}
		
		final String result = StringUtils.RawToHex((byte[])value)
		return next.execute(result, context)
	}

}
