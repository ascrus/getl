package getl.csv.proc

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

import org.supercsv.cellprocessor.*
import org.supercsv.cellprocessor.ift.*
import org.supercsv.util.*

/**
 * Csv null processor
 * @author Alexsey Konstantinov
 */
class CSVConvertToNullProcessor extends CellProcessorAdaptor
						implements BoolCellProcessor, DateCellProcessor, DoubleCellProcessor, 
									LongCellProcessor, StringCellProcessor {
	
    CSVConvertToNullProcessor() {
		super()
	}

    CSVConvertToNullProcessor(String nullValue, CellProcessor next) {
		super(next)
		this.nullValue = nullValue
	}

	private String nullValue

	@CompileStatic
	@Override
    <T> T execute(Object value, final CsvContext context) {
		if (nullValue != null && value != null && value == nullValue) value = null
		
		next.execute(value, context)
	}

}
