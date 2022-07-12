package getl.csv.proc

import getl.utils.ListUtils
import getl.utils.StringUtils
import groovy.json.DefaultJsonGenerator
import groovy.json.JsonBuilder
import groovy.json.JsonGenerator
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import org.apache.groovy.json.internal.CharBuf
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
		json = new JsonBuilder()
	}

	CSVFmtArray(String openBracket, String closeBracket) {
		super()
		if (openBracket != '[' || closeBracket != ']')
			json = new JsonBuilder(new ArrayGenerator(new JsonGenerator.Options(), openBracket, closeBracket))
		else
			json = new JsonBuilder()
	}

    CSVFmtArray(StringCellProcessor next) {
		super(next)
		json = new JsonBuilder()
	}

	CSVFmtArray(StringCellProcessor next, String openBracket, String closeBracket) {
		super(next)
		if (openBracket != '[' || closeBracket != ']')
			json = new JsonBuilder(new ArrayGenerator(new JsonGenerator.Options(), openBracket, closeBracket))
		else
			json = new JsonBuilder()
	}

	class ArrayGenerator extends DefaultJsonGenerator {
		ArrayGenerator(Options options, String openBracket, String closeBracket) {
			super(options)

			if (openBracket == null)
				throw new NullPointerException('Required open bracket parameter!')
			if (closeBracket == null)
				throw new NullPointerException('Required close bracket parameter!')

			this.openBracket = openBracket
			this.closeBracket = closeBracket
		}

		private String openBracket
		private String closeBracket

		@Override
		protected void writeArray(Class<?> arrayClass, Object array, CharBuf buffer) {
			def tempBuffer = CharBuf.create(255)
			super.writeArray(arrayClass, array, tempBuffer)
			def str = tempBuffer.toString()
			buffer.add(openBracket + str.substring(1, str.length() - 1) + closeBracket)
		}

		@Override
		protected void writeIterator(Iterator<?> iterator, CharBuf buffer) {
			def tempBuffer = CharBuf.create(255)
			super.writeIterator(iterator, tempBuffer)
			def str = tempBuffer.toString()
			buffer.add(openBracket + str.substring(1, str.length() - 1) + closeBracket)
		}
	}

	private JsonBuilder json

	@Override
    <T> T execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if (!(value instanceof List || value.class.isArray())) {
			def b = new ArrayList()
			throw new SuperCsvCellProcessorException((b.getClass()), value as Object, context, this)
		}

		if (value.class.isArray())
			json.call(value as Object[])
		else
			json.call((value as List).toArray())
		final String result = json.toString()
		return next.execute(result, context)
	}
}