package getl.csv.proc

import getl.data.Field
import getl.exception.IncorrectParameterError
import getl.exception.RequiredParameterError
import getl.utils.DateUtils
import getl.utils.StringUtils
import groovy.transform.CompileStatic
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext

import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle

/**
 * Parse value from date field in CSV files by format and locale
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class CSVParseTimestamp extends CellProcessorAdaptor implements StringCellProcessor {
    CSVParseTimestamp(String format, Field.Type typeField, String locale) {
        super()

        this.format = format
        this.typeField = typeField
        this.locale = locale

        initProcessor()
    }

    CSVParseTimestamp(String format, Field.Type typeField, String locale, StringCellProcessor next) {
        super(next)

        this.format = format
        this.typeField = typeField
        this.locale = locale

        initProcessor()
    }

    private String format
    private String locale
    private Field.Type typeField
    private DateTimeFormatter formatter

    private void initProcessor() {
        if( format == null )
            throw new RequiredParameterError('format')

        if( typeField == null )
            throw new RequiredParameterError('typeField')

        if (!(typeField in [Field.datetimeFieldType, Field.timestamp_with_timezoneFieldType]))
            throw new IncorrectParameterError('Invalid type "{type}" for parameter {param}', 'typeField', [type: typeField.toString()])

        formatter = DateUtils.BuildDateTimeFormatter(format, ResolverStyle.STRICT, locale)
    }

    @Override
    <T> T execute(final Object value, final CsvContext context) {
        validateInputNotNull(value, context)

        if( !(value instanceof String) )
            throw new SuperCsvCellProcessorException(String.class, value, context, this)

        def result = DateUtils.ParseSQLTimestamp(formatter, value, false)
        return next.execute(result, context)
    }
}