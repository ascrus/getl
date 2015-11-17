package getl.csv

import groovy.transform.InheritConstructors

import org.supercsv.io.Tokenizer
import org.supercsv.prefs.CsvPreference

import getl.utils.StringUtils

@InheritConstructors
class CSVEscapeTokenizer extends Tokenizer {
	CSVEscapeTokenizer (Reader reader, CsvPreference preferences) {
		super(reader, preferences)
	}
	
	@groovy.transform.CompileStatic
	@Override
	protected String readLine() throws IOException {
		def res = super.readLine()
		if (res != null) {
			res = res.replace("\\\\", "\u0001")
			res = res.replace('\\"', '""').replace("\\'", "''")
			res = res.replace("\u0001", "\\\\")
		}
		
		res
	}
}
