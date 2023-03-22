package getl.csv.proc

import groovy.transform.CompileStatic
import org.supercsv.io.Tokenizer
import org.supercsv.prefs.CsvPreference

class CSVTokenizer extends Tokenizer {
    CSVTokenizer(Reader reader, CsvPreference preferences) {
        super(reader, preferences)
        this.reader = reader
        assert preferences.endOfLineSymbols.length() == 1
        this.rowDelimiter = preferences.endOfLineSymbols.charAt(0)
    }

    private int curLine = 0L
    private Reader reader
    private int rowDelimiter

    @Override
    void close() {
        super.close()
        reader.close()
    }

    @CompileStatic
    @Override
    int getLineNumber() {
        return curLine
    }

    @CompileStatic
    @Override
    protected String readLine() {
        StringBuilder sb = new StringBuilder()
        int c = 0
        int i = 0
        while ((c = reader.read()) != -1) {
            i++
            if (c == rowDelimiter) {
                curLine++
                return sb.toString()
            }

            sb.append((char)c)
        }
        if (i > 0) {
            curLine++
            return sb.toString()
        }

        return null
    }
}