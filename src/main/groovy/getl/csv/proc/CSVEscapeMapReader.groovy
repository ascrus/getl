package getl.csv.proc

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.CsvMapReader
import org.supercsv.io.ITokenizer
import org.supercsv.prefs.CsvPreference
import getl.utils.StringUtils
import java.sql.Clob
import javax.sql.rowset.serial.SerialClob

/**
 * CSV map reader escaped string
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class CSVEscapeMapReader extends CsvMapReader {
	CSVEscapeMapReader(Reader reader, CsvPreference preferences) {
		super(reader, preferences)
	}
	
	CSVEscapeMapReader(ITokenizer tokenizer, CsvPreference preferences) {
		super(tokenizer, preferences)
	}
	
	@Override
    Map<String, Object> read(final String[] cols, final CellProcessor[] proc) throws IOException {
		Map<String, Object> res = super.read(cols, proc)
		if (res == null) return res
		res.each { String key, value ->
			if (value instanceof String) {
				res.put(key, StringUtils.UnescapeJava((String)value))
			}
			else if (value instanceof Clob) {
				Clob text = (Clob)value
				String str = (text.getSubString(1, (int)text.length()))
				str = StringUtils.UnescapeJava(str)
				res.put(key, new SerialClob(str.chars))
			}
		}

		return res
	}
}