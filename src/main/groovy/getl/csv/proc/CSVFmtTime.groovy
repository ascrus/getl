//file:noinspection DuplicatedCode
package getl.csv.proc

import getl.data.Field
import getl.exception.IncorrectParameterError
import getl.exception.RequiredParameterError
import getl.utils.ConvertUtils
import getl.utils.DateUtils
import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.DateCellProcessor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext

import java.sql.Time
import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle

/**
 * Format date field for write to CSV files by format and locale
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class CSVFmtTime extends CellProcessorAdaptor implements DateCellProcessor {
	CSVFmtTime(String format, Field.Type typeField, String locale) {
		super()

		this.format = format
		this.typeField = typeField
		this.locale = locale

		initProcessor()
	}

    CSVFmtTime(String format, Field.Type typeField, String locale, StringCellProcessor next) {
		super(next)

		this.format = format
		this.typeField = typeField

		initProcessor()
	}

	private String format
	private String locale = null
	private Field.Type typeField
	private DateTimeFormatter formatter

	private void initProcessor() {
		if( format == null )
			throw new RequiredParameterError('format')

		if( typeField == null )
			throw new RequiredParameterError('typeField')

		if (typeField != Field.timeFieldType)
			throw new IncorrectParameterError('Invalid type "{type}" for parameter {param}', 'typeField', [type: typeField.toString()])

		formatter = DateUtils.BuildTimeFormatter(format, ResolverStyle.STRICT, locale)
	}
	
	@Override
    Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		def result = formatter.format(ConvertUtils.Object2Time(value).toLocalTime())
		return next.execute(result, context)
	}
}