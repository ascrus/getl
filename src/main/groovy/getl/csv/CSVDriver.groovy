/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.csv

import groovy.transform.InheritConstructors

import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import org.supercsv.cellprocessor.constraint.*
import org.supercsv.cellprocessor.ift.*
import org.supercsv.cellprocessor.*
import org.supercsv.io.*
import org.supercsv.prefs.*
import org.supercsv.quote.*

import getl.data.*
import getl.driver.Driver
import getl.driver.FileDriver
import getl.exception.ExceptionGETL
import getl.files.FileManager
import getl.utils.*

/**
 * CSV Driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CSVDriver extends FileDriver {
	CSVDriver () {
		super()
		methodParams.register('eachRow', ['isValid', 'quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 'isSplit', 'readAsText', 
											'escaped', 'processError', 'filter', 'nullAsValue', 'escapeProcessLineChar'])
		methodParams.register('openWrite', ['batchSize', 'onSaveBatch', 'isValid', 'escaped', 'splitSize', 
								'quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 'nullAsValue', 'decimalSeparator', 
								'formatDate', 'formatTime', 'formatDateTime', 'onSplitFile', 'escapeProcessLineChar'])
	}
	
	@Override
	public List<Driver.Support> supported() { 
		[Driver.Support.WRITE, Driver.Support.AUTOLOADSCHEMA, Driver.Support.AUTOSAVESCHEMA, Driver.Support.EACHROW] 
	}

	@Override
	public List<Driver.Operation> operations () { 
		[Driver.Operation.DROP] 
	}

	class ReadParams {
		Map params = [:]
		String path
		def quoteStr
		def quote
		def fieldDelimiter
		def rowDelimiter
		boolean isHeader
		def qMode
		def isSplit
		String nullAsValue
	}

	protected ReadParams readParamDataset (Dataset dataset, Map params) {
		def p = new ReadParams()
		if (params != null) {
			def dsp = getDatasetParams(dataset, params)
			p.params.putAll(dsp)
		}
		
		p.path = p.params.fn
		CSVDataset ds = dataset as CSVDataset
		
		p.quote = ListUtils.NotNullValue([params.quoteStr, ds.quoteStr])
		p.quoteStr = p.quote.charAt(0)
		
		def fieldDelimiter = ListUtils.NotNullValue([params.fieldDelimiter, ds.fieldDelimiter])
		p.fieldDelimiter = fieldDelimiter.charAt(0)
		
		p.rowDelimiter = ListUtils.NotNullValue([params.rowDelimiter, ds.rowDelimiter])
		p.isHeader = BoolUtils.IsValue(params.header as Boolean, ds.header)
		p.qMode = datasetQuoteMode(ds)
		p.isSplit = (params.isSplit != null)?params.isSplit:false
		p.nullAsValue = ListUtils.NotNullValue([params.nullAsValue, ds.nullAsValue])
		
		return p
	}
	
	protected static QuoteMode datasetQuoteMode(Dataset dataset) {
		def QuoteMode qMode
		switch (dataset.quoteMode) {
			case CSVDataset.QuoteMode.COLUMN:
				boolean[] b = new boolean[dataset.field.size()]
                for (int i = 0; i < dataset.field.size(); i++) {
                    if (dataset.field[i].type in [Field.Type.STRING, Field.Type.TEXT] || dataset.field[i].extended?."quote") b[i] = true
                }
				qMode = new ColumnQuoteMode(b)
				break
			case CSVDataset.QuoteMode.ALWAYS:
				qMode = new AlwaysQuoteMode()
				break
			default:
				qMode = new NormalQuoteMode()
				break
		}
		qMode
	}
	
	@Override
    public
    List<Field> fields(Dataset dataset) {
		def p = readParamDataset(dataset, [:]) 
		
		def csvfile = new File(p.path)
		if (!csvfile.exists()) throw new ExceptionGETL("File \"${dataset.fileName}\" not found or invalid path \"${dataset.connection.params.path}\"")
		Reader fileReader = getFileReader(dataset, [:])
		
		def CsvPreference pref = new CsvPreference.Builder(p.quoteStr as char, (p.fieldDelimiter) as int, p.rowDelimiter as String).useQuoteMode(p.qMode as QuoteMode).build()
		def reader = new CsvListReader(fileReader, pref)
		String[] header = null
		try {
			if (p.isHeader)  {
				header = reader.getHeader(true)
				if (header == null) throw new ExceptionGETL("File \"${dataset.fileName}\" is empty")
			}
			else {
				def row = reader.read()
				if (row == null) throw new ExceptionGETL("File \"${dataset.fileName}\" is empty")
				def c = 0
				row.each {
					c++
					header += "col_${c}"
				}
			}
		}
		finally {
			reader.close()
		}
		
		return header2fields(header)
	}
	
	protected static List<Field> header2fields (String[] header) {
		List<Field> fields = []
		header.each { String name ->
			if (name == null || name.length() == 0) throw new ExceptionGETL("Detected empty field name for $header")
			fields << new Field(name: name/*.toLowerCase()*/, type: Field.Type.STRING)
		}
		return fields
	}

	protected static CellProcessor type2cellProcessor (Field field, Boolean isWrite, Boolean isEscape, String nullAsValue, String decimalSeparator,
											 			String formatDate, String formatTime, String formatDateTime, Boolean isValid) {
		CellProcessor cp
		
		if (field.type == null || (field.type in [Field.Type.STRING, Field.Type.OBJECT, Field.Type.ROWID, Field.Type.UUID])) {
			if (BoolUtils.IsValue(field.trim)) {
				cp = new Trim()
			}
			else {
				cp = new Optional()
			}
		} else if (field.type == Field.Type.INTEGER) {
			if (!isWrite) {
				cp = new ParseInt()
			}
			else {
				cp = new Optional()
			}
		} else if (field.type == Field.Type.BIGINT) {
				if (!isWrite) {
					cp = new ParseLong()
				}
				else {
					cp = new Optional()
				}
		} else if (field.type == Field.Type.NUMERIC) {
			if (!isWrite) {
				if (field.precision != null || field.format != null || decimalSeparator != null) {
					def ds = ListUtils.NotNullValue([field.decimalSeparator, decimalSeparator]) as String
					DecimalFormatSymbols dfs = new DecimalFormatSymbols()
					dfs.setDecimalSeparator(ds.value[0])
					cp = new ParseBigDecimal(dfs)
				}
				else {
					cp = new ParseBigDecimal()
				}
			}
			else {
				if (field.precision != null || field.format != null || decimalSeparator != null) {
					def ds = ListUtils.NotNullValue([field.decimalSeparator, decimalSeparator]) as String

					DecimalFormatSymbols dfs = new DecimalFormatSymbols()
					dfs.setDecimalSeparator(ds.value[0])
					
					def p = (field.precision != null)?field.precision:0
					def f = field.format
					if (f == null) {
						if (p > 0) {
							def s = ''
							(1..p).each {
								s += '0'
							}
							f = '0.' + s
						}
						else {
							f = '0'
						}
					}
					
					DecimalFormat df = new DecimalFormat(f, dfs);
					cp = new FmtNumber(df)
				}
				else {
					cp = new Optional()
				}
			}
		} else if (field.type == Field.Type.DOUBLE) {
			if (!isWrite) {
				cp = new ParseDouble()
			}
			else {
				cp = new Optional()
			}
		} else if (field.type == Field.Type.BOOLEAN) {
			String[] v = (field.format == null)?['1', '0']:field.format.toLowerCase().split('[|]')
			if (v[0] == null) v[0] = '1'
			if (v[1] == null) v[1] = '0'
			
			if (!isWrite) {
				if (field.format != null) cp = new ParseBool(v[0], v[1]) else cp = new ParseBool()
			}
			else {
				cp = new FmtBool(v[0], v[1])
			}
		} else if (field.type in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME]) {
			String df = null
			switch (field.type) {
				case Field.Type.DATE:
					df = ListUtils.NotNullValue([field.format, formatDate, 'yyyy-MM-dd'])
					break
				case Field.Type.TIME:
					df = ListUtils.NotNullValue([field.format, formatTime, 'HH:mm:ss'])
					break
				case Field.Type.DATETIME:
					df = ListUtils.NotNullValue([field.format, formatDateTime, 'yyyy-MM-dd HH:mm:ss'])
					break
			}
			def locale = field.extended.locale as String
			if (!isWrite) {
				if (locale == null) {
					cp = new ParseDate(df, true)
				}
				else {
					cp = new ParseDate(df, true, StringUtils.NewLocale(locale))
				}
			}
			else {
				if (locale == null) {
					cp = new org.supercsv.cellprocessor.FmtDate(df)
				}
				else {
					cp = new CSVFmtDate(df, locale)
				}
			}
		} else if (field.type == Field.Type.BLOB) {
			if (!isWrite) {
				cp = new CSVParseBlob()
			}
			else {
				cp = new CSVFmtBlob()
			} 
		} else if (field.type == Field.Type.TEXT) {
			if (!isWrite) {
				cp = new CSVParseClob()
			}
			else {
				cp = new CSVFmtClob()
			}
		} else {
			throw new ExceptionGETL("Type ${field.type} not supported")
		}

		if (field.isKey != null && field.isKey && isValid) cp = new UniqueHashCode(cp)
		if (field.isNull != null && !field.isNull && isValid) cp = new NotNull(cp) else cp = new Optional(cp)

		if (nullAsValue != null) {
			if (isWrite)
				cp = new ConvertNullTo(nullAsValue, cp)
			else
				cp = new CSVConvertToNullProcessor(nullAsValue, cp)
		}

		/*		
		CellProcessor c = cp
		print "${field.name}: "
		while (c != CellProcessorAdaptor.NullObjectPattern.INSTANCE) {
			print c.getClass().name + ";"
			c = c.next
		}
		println ""
		*/
		
		cp
	}
	
	@groovy.transform.CompileStatic
	public static CellProcessor[] fields2cellProcessor(Map fParams) {
		CSVDataset dataset = (CSVDataset)fParams.dataset
		ArrayList<String> fields = (ArrayList<String>)fParams.filds
		String[] header = (String[])fParams.header
		Boolean isOptional = fParams.isOptional as Boolean
		Boolean isWrite = fParams.isWrite as Boolean
		Boolean isValid = fParams.isValid as Boolean
		Boolean isEscape = fParams.isEscape as Boolean
		String nullAsValue = fParams.nullAsValue as String
		String decimalSeparator = ListUtils.NotNullValue([fParams.decimalSeparator, dataset.decimalSeparator, '.']) as String
		String formatDate = ListUtils.NotNullValue([fParams.formatDate, dataset.formatDate]) as String
		String formatTime = ListUtils.NotNullValue([fParams.formatTime, dataset.formatTime]) as String
		String formatDateTime = ListUtils.NotNullValue([fParams.formatDateTime, dataset.formatDateTime]) as String
		
		if (fields == null) fields = []
//		def quoteStr = dataset.quoteStr
		
		def cp = new ArrayList<CellProcessor>()
		header.each { String name ->
			if (isOptional) {
				cp << new Optional()
			}
			else {
				name = name.toLowerCase()
				int i = dataset.indexOfField(name)
				String fi = fields.find { (it.toLowerCase() == name.toLowerCase()) } 
				if (i == -1 || (!fields.isEmpty() && fi == null)) {
					cp << new Optional()
				} 
				else {
					Field f = dataset.field[i]
					
					CellProcessor p = type2cellProcessor(f, isWrite, isEscape, nullAsValue, decimalSeparator,
															formatDate, formatTime, formatDateTime, isValid)
					cp << p
				}
			}
		}

		return cp.toArray() as CellProcessor[]
	} 
	
	protected static String[] fields2header (List<Field> fields, List<String> writeFields) {
		if (writeFields == null) writeFields = []
		def header = []
		fields.each { v ->
			if (writeFields.isEmpty()) {
				header << v.name //.toLowerCase()
			}
			else {
				def fi = writeFields.find { (it.toLowerCase() == v.name.toLowerCase()) }
				if (fi != null) header << v.name //.toLowerCase()
			}
		} 
		return header.toArray()
	}
	
	@groovy.transform.CompileStatic
	@Override
	public
	long eachRow (Dataset dataset, Map params, Closure prepareCode, Closure code) {
		if (code == null) throw new ExceptionGETL('Required process code')
		
		CSVDataset cds = (CSVDataset)dataset
		if (cds.fileName == null) throw new ExceptionGETL('Dataset required fileName')
		
		boolean escaped = BoolUtils.IsValue(params.escaped, cds.escaped)
		boolean readAsText = BoolUtils.IsValue(params.readAsText)
		boolean ignoreHeader = BoolUtils.IsValue([params.ignoreHeader, cds.ignoreHeader], true)
		String escapeProcessLineChar = ListUtils.NotNullValue([params.escapeProcessLineChar, cds.escapeProcessLineChar])
		Closure filter = (Closure)params."filter"

		long countRec = 0
		
		def p = readParamDataset(cds, params)
		
		FileManager fm = new FileManager(rootPath: ((CSVConnection)cds.connection).path)
		fm.connect()

		def fileMask = fileMaskDataset(cds, (boolean)p.isSplit)
		def vars = [:]
		if (p.isSplit) {
			vars << [number: [type: Field.Type.INTEGER, len: 4]]
		}
		def filePath = new Path(mask: fileMask, vars: vars)
		
		fm.buildList(path: filePath)
		def filesParams = (p.isSplit)?[order: ['number']]:[:]
		def files = fm.fileList.rows(filesParams)
		if (files.isEmpty()) throw new ExceptionGETL("File(s) \"${cds.fileName}\" not found or invalid path \"${((CSVConnection)cds.connection).path}\"")
		Integer portion = 0
		Reader bufReader = getFileReader(dataset, params, (Integer)files[portion].number)
		
		Closure processError = (params.processError != null)?params.processError:null
		boolean isValid = BoolUtils.IsValue(params.isValid, false)
		CsvPreference pref = new CsvPreference.Builder((char)p.quoteStr, (int)p.fieldDelimiter, (String)p.rowDelimiter).useQuoteMode((QuoteMode)p.qMode).build()
		ICsvMapReader reader
		if (escaped) { 
			if (!readAsText) {
				reader = new CSVEscapeMapReader(new CSVEscapeTokenizer(bufReader, pref), pref, escapeProcessLineChar)
			}
			else {
				reader = new CsvMapReader(new CSVEscapeTokenizer(bufReader, pref), pref)
			}
		}
		else {
			reader = new CsvMapReader(bufReader, pref)
		}

		try {
			String[] header
			List<Field> filefields = []
			if (p.isHeader) {
				header = reader.getHeader(true)
//				String[] header_new = header*.toLowerCase()
//				header = header_new
				if (!ignoreHeader) {
					filefields = header2fields(header)
				}
				else {
					header = fields2header(dataset.field, null)
				}
			}
			else {
				header = fields2header(dataset.field, null)
			}
			header = header*.toLowerCase()

			ArrayList<String> listFields = new ArrayList<String>()
			if (prepareCode != null) {
				listFields = (ArrayList<String>)prepareCode.call(filefields)
			}
			
			CellProcessor[] cp = fields2cellProcessor([dataset: dataset, fields: listFields, header: header, isOptional: readAsText, isWrite: false, isValid: isValid, isEscape: false, nullAsValue: p.nullAsValue])
			
			long count = (params.limit != null)?(long)params.limit:0
			long cur = 0
			while (true) {
				Map row
				boolean isError = false
				try {
					cur++
					row = reader.read(header, cp)
				}
				catch (Exception e) {
					boolean isContinue = (processError != null)?processError(e, cur + 1):false
					if (!isContinue) throw e
					isError = true
				}
				if (!isError) {
					if (row == null) {
						cur--
						if (portion != null && (portion + 1) < files.size()) {
							reader.close()
							portion++
							bufReader = getFileReader(dataset, params, (Integer)files[portion].number)
							
							if (escaped) {
								if (!readAsText) {
									reader = new CSVEscapeMapReader(new CSVEscapeTokenizer(bufReader, pref), pref)
								}
								else {
									reader = new CsvMapReader(new CSVEscapeTokenizer(bufReader, pref), pref)
								}
							}
							else {
								reader = new CsvMapReader(bufReader, pref)
							}
							
							if (p.isHeader) reader.getHeader(true)
							continue
						}
						else {
							break
						}
					}
					
					if (filter != null && !filter(row)) {
						cur--
						continue
					} 

					code(row)
					if (cur == count) break
				}
			}
			countRec = cur
		}
		finally {
			reader.close()
			dataset.params.countReadPortions = portion + 1
		}
		
		countRec
	}
	
	class WriterParams {
		ICsvMapWriter writer
		CSVDefaultFileEncoder encoder
		CsvPreference pref
		String[] header
		CellProcessor[] cp
		Map params
		boolean isHeader = false
		String quote
		String nullAsValue
		boolean escaped
		String escapeProcessLineChar
		Long splitSize
		boolean formatOutput = true
		long batchSize = 1000
		List<Map> rows = new ArrayList<Map>()
		long current = 0
		long batch = 0
		long fieldDelimiterSize = 0
		long rowDelimiterSize = 0
		int countFields = 0
		Closure onSaveBatch
		Closure onSplitFile
		Writer bufWriter
		Integer portion
		long countCharacters = 0
	}
	
	@Override
	public
	void openWrite (Dataset dataset, Map params, Closure prepareCode) {
		if (dataset.fileName == null) throw new ExceptionGETL('Dataset required fileName')
		
		WriterParams wp = new WriterParams()
		dataset.driver_params = wp
		wp.formatOutput = dataset.formatOutput

        CSVDataset csv_ds = dataset as CSVDataset
		
		ReadParams p = readParamDataset(csv_ds, params)
		boolean isAppend = p.params.isAppend
		boolean isValid = (params.isValid != null)?params.isValid:false
//		boolean bulkFile = (params.bulkFile != null)?params.bulkFile:false
		boolean escaped = BoolUtils.IsValue(params.escaped, csv_ds.escaped)
		String formatDate = ListUtils.NotNullValue([params.formatDate, csv_ds.formatDate])
		String formatTime = ListUtils.NotNullValue([params.formatTime, csv_ds.formatTime])
		String formatDateTime = ListUtils.NotNullValue([params.formatDateTime, csv_ds.formatDateTime])
		String escapeProcessLineChar = ListUtils.NotNullValue([params.escapeProcessLineChar, csv_ds.escapeProcessLineChar])
		
		if (params.batchSize != null) wp.batchSize = params.batchSize
		if (params.onSaveBatch != null) wp.onSaveBatch = params.onSaveBatch
		if (params.onSplitFile != null) wp.onSplitFile = params.onSplitFile
		
		ArrayList<String> listFields = new ArrayList<String>()
		if (prepareCode != null) {
			listFields = prepareCode([])
		}
		
		def header = fields2header(csv_ds.field, listFields)
		if (header.length == 0) throw new ExceptionGETL('Required fields declare')
		wp.header = header*.toLowerCase()

		wp.params = params
		wp.cp = fields2cellProcessor(
					dataset: csv_ds, fields: listFields, header: wp.header, isOptional: false,
					isWrite: true, isValid: isValid, isEscape: escaped, nullAsValue: p.nullAsValue,
					formatDate: formatDate, formatTime: formatTime, formatDateTime: formatDateTime)
		wp.fieldDelimiterSize = p.fieldDelimiter.toString().length()
		wp.rowDelimiterSize = p.rowDelimiter.length()
		wp.countFields = listFields.size()
		wp.isHeader = p.isHeader
		wp.quote = p.quote
		wp.nullAsValue = p.nullAsValue
		wp.escaped = escaped
		wp.escapeProcessLineChar = escapeProcessLineChar
		wp.splitSize = params.splitSize
		if (wp.splitSize != null) wp.portion = 1

        csv_ds.params.writeCharacters = null
		
		File csvfile = new File(p.path)
		boolean isExistsFile = csvfile.exists()
		
		wp.bufWriter = getFileWriter(csv_ds, wp.params, wp.portion)
		
		wp.encoder = new CSVDefaultFileEncoder(csv_ds, wp)
		
		wp.pref = new CsvPreference.Builder(p.quoteStr as char, (p.fieldDelimiter) as int, p.rowDelimiter as String).useQuoteMode(p.qMode as QuoteMode).useEncoder(wp.encoder).build()
		wp.writer = new CsvMapWriter(wp.bufWriter, wp.pref)

		if ((!isAppend || !isExistsFile || wp.splitSize != null) && wp.isHeader) {
			wp.writer.writeHeader(header)
		}
	}
	
	/**
	 * Write rows batch to file
	 * @param dataset
	 * @param wp
	 */
	@groovy.transform.CompileStatic
	protected void writeRows (Dataset dataset, WriterParams wp) {
		wp.batch++
		
		try {
			if (wp.formatOutput) {
				wp.rows.each { Map row ->
					wp.writer.write(row, wp.header, wp.cp)
					dataset.writeRows++
					dataset.updateRows++
					
					if (wp.splitSize != null && wp.encoder.writeSize >= wp.splitSize) {
						boolean splitFile = true
						if (wp.onSplitFile != null) {
							splitFile = wp.onSplitFile.call(row)
						}
						if (splitFile) {
							wp.portion++
							wp.bufWriter = getFileWriter(dataset, wp.params, wp.portion)
							wp.writer.close()
							wp.countCharacters += wp.encoder.writeSize
							wp.encoder.writeSize = 0
							wp.writer = new CsvMapWriter(wp.bufWriter, wp.pref)
							if (wp.isHeader) wp.writer.writeHeader(wp.header)
						}
					}
				}
			}
			else {
				wp.rows.each { Map row ->
					wp.writer.write(row, wp.header)
					dataset.writeRows++
					dataset.updateRows++
					
					if (wp.splitSize != null && wp.encoder.writeSize >= wp.splitSize) {
						boolean splitFile = true
						if (wp.onSplitFile != null) {
							splitFile = wp.onSplitFile.call(row)
						}
						if (splitFile) {
							wp.portion++
							wp.bufWriter = getFileWriter(dataset, wp.params, wp.portion)
							wp.writer.close()
							wp.countCharacters += wp.encoder.writeSize
							wp.encoder.writeSize = 0
							wp.writer = new CsvMapWriter(wp.bufWriter, wp.pref)
							if (wp.isHeader) wp.writer.writeHeader(wp.header)
						}
					}
				}
			}
		}
		finally {
			wp.rows.clear()
			wp.current = 0
		}
		
		if (wp.onSaveBatch) wp.onSaveBatch.call(wp.batch)
	}
	
	@groovy.transform.CompileStatic
	@Override
	public void write(Dataset dataset, Map row) {
		WriterParams wp = (WriterParams)dataset.driver_params
		wp.rows << row
		wp.current++
		
		if (wp.batchSize == 0 || wp.current >= wp.batchSize) writeRows(dataset, wp)
	}
	
	@Override
	public
	void doneWrite (Dataset dataset) {
		WriterParams wp = dataset.driver_params
		if (!wp.rows.isEmpty()) writeRows(dataset, wp)
	}
	
	@Override
	public
	void closeWrite (Dataset dataset) {
		WriterParams wp = dataset.driver_params
		
		try {
			wp.writer.close()
		}
		finally {
			wp.countCharacters += wp.encoder.writeSize
			
			dataset.params.countWriteCharacters = wp.countCharacters
			dataset.params.countWritePortions = wp.portion
			dataset.driver_params = null
		}
	}
	
	protected static long readLinesCount (CSVDataset dataset) {
		if (!dataset.existsFile()) throw new ExceptionGETL("File \"${dataset.fullFileName()}\" not found")
		if (!(dataset.rowDelimiter in ['\n', '\r\n'])) throw new ExceptionGETL('Allow CSV file only standart row delimiter')
		
		BufferedReader reader
		if (dataset.isGzFile) {
			def input = new GZIPInputStream(new FileInputStream(dataset.fullFileName()))
			reader = new BufferedReader(new InputStreamReader(input, dataset.codePage))
		}
		else {
			reader = new File(dataset.fullFileName()).newReader(dataset.codePage)
		}
		
		long count = 0
		
		try {
			def str = reader.readLine(true)
			while (str != null) {
				count++
				str = reader.readLine(true)
			}
		}
		finally {
			reader.close()
		}
		
		count - ((dataset.header)?1:0)
	}
	
	public static long prepareCSVForBulk(CSVDataset target, CSVDataset source, Map<String, String> encodeTable, Closure code) {
		if (!source.existsFile()) throw new ExceptionGETL("File \"${source.fullFileName()}\" not found")
		if (!(source.rowDelimiter in ['\n', '\r\n'])) throw new ExceptionGETL('Allow convert CSV files only standart row delimiter')

		if (source.field.isEmpty() && source.autoSchema) source.loadDatasetMetadata()
		//if (source.field.isEmpty()) throw new ExceptionGETL('Required fields from source dataset')
		
		target.connection.validPath()

		if (!source.field.isEmpty()) target.setField(source.field) else target.field.clear()
		target.header = source.header
		target.nullAsValue = source.nullAsValue
		target.escaped = false
		/*
		target.rowDelimiter = '\u0001'
		target.fieldDelimiter = '\u0002'
		target.quoteStr = '\u0003'
		*/

		//int targetFieldCount = target.field.size()
//		String sourceRowDelimiter = source.rowDelimiter
		String targetRowDelimiter = target.rowDelimiter
		String sourceFieldDelimiter = source.fieldDelimiter
//		int sourceFieldDelimiterLen = sourceFieldDelimiter.length()
		String sourceFieldDelimiterLast = sourceFieldDelimiter + '\u0000'
		String targetFieldDelimiter = target.fieldDelimiter
		boolean source_escaped = source.escaped
		String sourceQuoteStr = source.quoteStr
//		int sourceQuoteLen = sourceQuoteStr.length()
		String targetQuoteStr = target.quoteStr
		
		//String lastBadQuoteExpr = '\\\\\\\\"$'
		String decodeQuote = sourceQuoteStr
		if (targetQuoteStr == sourceQuoteStr) decodeQuote += sourceQuoteStr
		Map convertMap = ['\u0004': "$decodeQuote", '\u0005': '\\\\']
		
		Map<String, String> encodeMap = [:]
		if (!source_escaped) {
			//encodeMap."$sourceQuoteStr$sourceQuoteStr$sourceQuoteStr" = '"\u0004'
			encodeMap."$sourceQuoteStr$sourceQuoteStr" = '\u0004'
			//encodeMap."\\" = '\u0005'
		}
		else {
			encodeMap."\\\\" = '\u0005'
			encodeMap."\\$sourceQuoteStr" = '\u0004'
			encodeMap."\\n" = '\n'
			encodeMap."\\t" = '\t'
			//encodeMap."\\" = '\u0005'
		}
		
		String splitFieldDelimiter = StringUtils.Delimiter2SplitExpression(sourceFieldDelimiter)

//		def fs = new File(source.fullFileName())
		def fw = new File(target.fullFileName())
		if (target.autoSchema) target.saveDatasetMetadata()
		if (BoolUtils.IsValue(target.params.deleteOnExit, false)) {
			fw.deleteOnExit()
			if (target.autoSchema) {
				File ws = new File(target.fullFileSchemaName())
				ws.deleteOnExit()
			}
		}
		
		long count = 0
		
		BufferedReader reader
		if (source.isGzFile) {
			def input = new GZIPInputStream(new FileInputStream(source.fullFileName()))
			reader = new BufferedReader(new InputStreamReader(input, source.codePage))
		}
		else {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(source.fullFileName()), source.codePage))
		}
		
		try {
			BufferedWriter writer
			if (target.isGzFile) {
				def output = new GZIPOutputStream(new FileOutputStream(target.fullFileName()))
				writer = new BufferedWriter(new OutputStreamWriter(output, target.codePage))
			}
			else {
				writer = new File(target.fullFileName()).newWriter(target.codePage, target.append)
			}
			
			try {
				StringBuilder bufStr = new StringBuilder()
				String readStr = reader.readLine(true)
				boolean isQuoteFirst = false
				List<String> values = []
				
				// Read lines
				while (readStr != null) {
					encodeTable?.each { String oldStr, String newStr -> 
						readStr = readStr.replace(oldStr, newStr)
					}
					
					// Add last virtual column from correct split last nullable columns
					readStr += sourceFieldDelimiterLast
					
					// Split columns by field delimiter
					List<String> cols = readStr.split(splitFieldDelimiter).toList()
					int colsSize = cols.size() - 1
					//boolean allFields = (colsSize == targetFieldCount)
					for (int colI = 0; colI < colsSize; colI++) {
						String value = cols[colI]
						
						// Prev column not quoted
						if (!isQuoteFirst) {
							
							// Valid first quoted by column
							isQuoteFirst = (value.length() > 0 && value.substring(0, 1) == sourceQuoteStr)
							// Add not quoted column and continue
							if (!isQuoteFirst) {
								values << value
								continue
							}
						
							// Hide all escaped chars into value
							value = value.substring(1)
							encodeMap.each { String oldStr, String newStr -> value = value.replace(oldStr, newStr) }

							// Add to buffer							
							bufStr << targetQuoteStr
							bufStr << value
						}
						else {
							// Hide all escaped chars into value
							encodeMap.each { String oldStr, String newStr -> value = value.replace(oldStr, newStr) }
							
							// Add to buffer
							bufStr << value
						}
						
						// Valid last quoted by column
						if (bufStr.substring(bufStr.length() - 1) == sourceQuoteStr) {
							values << bufStr.substring(0, bufStr.length() - 1) + targetQuoteStr
							bufStr = new StringBuilder()
							isQuoteFirst = false
							continue
						}

						if (colI < colsSize - 1) bufStr << sourceFieldDelimiter
					}
					
					if (isQuoteFirst) {
						bufStr << '\n'
						readStr = reader.readLine(true)
						continue
					}
					
					convertCSVValues(values, convertMap, code)
					writer.write(values.join(targetFieldDelimiter))
					writer.write(targetRowDelimiter)
					values.clear()
					count++

					readStr = reader.readLine(true)
				}
				
				if (bufStr.length() > 0) {
					convertCSVValues(values, convertMap, code)
					writer.write(values.join(targetFieldDelimiter))
					writer.write(targetRowDelimiter)
					count++
				}
			}
			finally {
				writer.close()
			}
		}
		finally {
			reader.close()
		}
		
		count - ((target.header)?1:0)
	}
	
	protected static void convertCSVValues (List<String> values, Map<String, String> convertMap, Closure code) {
		convertMap.each { String oldStr, String newStr ->
			for (int i = 0; i < values.size(); i++) { values[i] = values[i].replace(oldStr, newStr) }
		}

		if (code != null) code(values)
	}
	
	public static long decodeBulkCSV (CSVDataset target, CSVDataset source) {
		if (!source.existsFile()) throw new ExceptionGETL("File \"${source.fullFileName()}\" not found")
		
		if (source.field.isEmpty() && source.autoSchema) source.loadDatasetMetadata()
		if (!source.field.isEmpty()) target.setField(source.field)
		
		target.connection.validPath()
		
		target.header = source.header
		target.nullAsValue = source.nullAsValue
		String targetRowDelimiter = target.rowDelimiter
		String targetQuoteStr = target.quoteStr
		
		Map<String, String> encodeMap = [:]
		if (target.escaped) {
			encodeMap."\\" = '\\\\'
			encodeMap."\n" = '\\n' 
			encodeMap."\t" = '\\t'
			encodeMap."$targetQuoteStr" = "\\$targetQuoteStr"
		}
		else {
			encodeMap."$targetQuoteStr" = "$targetQuoteStr$targetQuoteStr"
		}
		encodeMap.putAll(['\u0001': targetRowDelimiter, '\u0002': target.fieldDelimiter, '\u0003': target.quoteStr])

		BufferedReader reader
		if (source.isGzFile) {
			def input = new GZIPInputStream(new FileInputStream(source.fullFileName()))
			reader = new BufferedReader(new InputStreamReader(input, source.codePage), 64*1024)
		}
		else {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(source.fullFileName()), source.codePage), 64*1024)
		}
		
		long count = 0

		try {
			BufferedWriter writer
			if (target.isGzFile) {
				def output = new GZIPOutputStream(new FileOutputStream(target.fullFileName()))
				writer = new BufferedWriter(new OutputStreamWriter(output, target.codePage))
			}
			else {
				writer = new File(target.fullFileName()).newWriter(target.codePage, target.append)
			}
			
			try {
				// Read lines
				char[] buf = new char[64*1024]
				def countRead = reader.read(buf)
				while (countRead > -1) {
					count += countRead
					String str = new String(buf, 0, countRead)
					encodeMap.each { String oldStr, String newStr -> str = str.replace(oldStr, newStr) }
					writer.write(str)
					
					countRead = reader.read(buf)
				}
			}
			finally {
				writer.close()
			}
		}
		finally {
			reader.close()
		}
		
		return count
	}

}
