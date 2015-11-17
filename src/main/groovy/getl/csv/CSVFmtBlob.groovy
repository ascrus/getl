package getl.csv

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext
import getl.utils.*

class CSVFmtBlob extends CellProcessorAdaptor {
	@groovy.transform.CompileStatic
	@Override
	public Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof byte[])) {
			throw new SuperCsvCellProcessorException((byte[]).class, value, context, this);
		}
		
		String result = /*'0x' + */StringUtils.RawToHex((byte[])value)
		return next.execute(result, context)
	}

}
