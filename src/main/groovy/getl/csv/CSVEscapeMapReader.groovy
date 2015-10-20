package getl.csv

import groovy.transform.InheritConstructors
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.CsvMapReader
import org.supercsv.io.ITokenizer
import org.supercsv.prefs.CsvPreference
import getl.utils.StringUtils

@InheritConstructors
class CSVEscapeMapReader extends CsvMapReader {
	CSVEscapeMapReader (Reader reader, CsvPreference preferences) {
		super(reader, preferences)
	}
	
	CSVEscapeMapReader (ITokenizer tokenizer, CsvPreference preferences) {
		super(tokenizer, preferences)
	}
	
	@Override
	public Map<String, Object> read(String[] cols, CellProcessor[] proc) throws IOException {
		def res = super.read(cols, proc)
		res.each { key, value ->
			if (value instanceof String) res.put(key, StringUtils.UnescapeJava(value))
		}
		
		return res
	}
}
