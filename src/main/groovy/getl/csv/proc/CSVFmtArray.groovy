package getl.csv.proc

import getl.utils.ListUtils
import getl.utils.StringUtils
import groovy.json.JsonBuilder
import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext

/**
 * Format array fields from write to CSV files
 * @author Alexsey Konstantinov
 */
@CompileStatic
class CSVFmtArray extends CellProcessorAdaptor {
	CSVFmtArray() {
		super()
	}

    CSVFmtArray(StringCellProcessor next) {
		super(next)
	}

	private final JsonBuilder json = new JsonBuilder()

	@Override
    <T> T execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof List || value.class.isArray())) {
			def b = new ArrayList()
			throw new SuperCsvCellProcessorException((b.getClass()), value, context, this)
		}

		if (value.class.isArray())
			json.call(value as Object[])
		else
			json.call((value as List).toArray())
		final String result = json.toString()
		return next.execute(result, context)
	}
}