package getl.csv.proc

import groovy.transform.CompileStatic
import java.text.SimpleDateFormat
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.DateCellProcessor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext

import getl.utils.*

/**
 * Format date field for write to CSV files by format and locale
 * @author Alexsey Konstantinov
 *
 */
class CSVFmtDate extends CellProcessorAdaptor implements DateCellProcessor {
    CSVFmtDate(String dateFormat, String localeStr) {
		super()
		checkPreconditions(dateFormat, localeStr)
		this.dateFormat = dateFormat
		this.localeStr = localeStr
		locale = StringUtils.NewLocale(localeStr)
	}

    CSVFmtDate(String dateFormat, String localeStr, StringCellProcessor next) {
		super(next)
		checkPreconditions(dateFormat, localeStr)
		this.dateFormat = dateFormat
		this.localeStr = localeStr
		locale = StringUtils.NewLocale(localeStr)
	}

	private final String dateFormat
	private final String localeStr
	private final Locale locale
	
	static private void checkPreconditions(String dateFormat, String localeStr) {
		if( dateFormat == null ) {
			throw new NullPointerException("dateFormat should not be null")
		}
		
		StringUtils.NewLocale(localeStr)
	}
	
	@SuppressWarnings("unchecked")
	@CompileStatic
	@Override
    Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if( !(value instanceof Date) ) {
			throw new SuperCsvCellProcessorException(Date.class, value, context, this)
		}
		
		SimpleDateFormat formatter
		try {
			if (locale == null) {
				formatter = new SimpleDateFormat(dateFormat)
			}
			else {
				formatter = new SimpleDateFormat(dateFormat, locale)
			}
		}
		catch(IllegalArgumentException e) {
			throw new SuperCsvCellProcessorException(String.format("'%s' is not a valid date format", dateFormat),
				context, this, e)
		}
		catch (Exception e) {
			throw e
		}
		
		final String result = formatter.format((Date) value)
		return next.execute(result, context)
	}
}
