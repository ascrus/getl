package getl.csv.proc

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

import org.supercsv.io.Tokenizer
import org.supercsv.prefs.CsvPreference
import java.util.regex.Pattern

import getl.utils.StringUtils
import getl.csv.CSVDriver
import getl.csv.CSVDriver.WriterParams

/**
 * CSV tokenizer escape string
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class CSVEscapeTokenizer extends Tokenizer {
	CSVEscapeTokenizer (Reader reader, CsvPreference preferences, Boolean useHeader) {
		super(reader, preferences)
		this.header = useHeader

		def quoteChar = String.valueOf(preferences.quoteChar)
		pattern2 = StringUtils.SearchPattern('\\' + quoteChar)
		replace2 = quoteChar + quoteChar
	}

	static private final Pattern pattern1 = StringUtils.SearchPattern('\\\\')
	static private final String replace1 = '\u0081'

	private Boolean header
	private Pattern pattern2
	private String replace2

	static private final Pattern pattern3 = StringUtils.SearchPattern('\u0081')
	static private final String replace3 = '\\\\'
	
	@Override
	protected String readLine() throws IOException {
		def res = super.readLine()
		if (res != null && (!header || lineNumber > 1)) {
			def sb = new StringBuilder(res)
			StringUtils.ReplaceAll(sb, pattern1, replace1)
			StringUtils.ReplaceAll(sb, pattern2, replace2)
			StringUtils.ReplaceAll(sb, pattern3, replace3)

			res = sb.toString()
		}

		return res
	}
}
