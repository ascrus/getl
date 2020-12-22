package getl.csv

import getl.csv.proc.*
import getl.data.opts.FileWriteOpts
import getl.data.*
import getl.driver.Driver
import getl.driver.FileDriver
import getl.exception.ExceptionGETL
import getl.files.FileManager
import getl.utils.*

import groovy.transform.CompileStatic
import org.codehaus.groovy.runtime.StringBufferWriter

import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.cellprocessor.constraint.*
import org.supercsv.cellprocessor.ift.*
import org.supercsv.cellprocessor.*
import org.supercsv.io.*
import org.supercsv.prefs.*
import org.supercsv.quote.*

/**
 * CSV Driver class
 * @author Alexsey Konstantinov
 *
 */
class CSVDriver extends FileDriver {
	CSVDriver () {
		super()
		methodParams.register('eachRow', ['isValid', 'quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 'isSplit', 'readAsText', 
											'escaped', 'processError', 'filter', 'nullAsValue', 'ignoreHeader', 'skipRows', 'limit'])
		methodParams.register('openWrite', ['batchSize', 'onSaveBatch', 'isValid', 'escaped', 'splitSize', 
								'quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 'nullAsValue', 'decimalSeparator', 
								'formatDate', 'formatTime', 'formatDateTime', 'formatTimestampWithTz', 'onSplitFile'])
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		[Driver.Support.WRITE, Driver.Support.AUTOLOADSCHEMA, Driver.Support.AUTOSAVESCHEMA, Driver.Support.EACHROW]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations () {
		[Driver.Operation.DROP, Driver.Operation.RETRIEVEFIELDS]
	}

	class ReadParams {
		public Map params = [:]
		public String path
		public char quoteStr
		public String quote
		public char fieldDelimiter
		public String rowDelimiter
		public Boolean isHeader
		public org.supercsv.quote.QuoteMode qMode
		public Boolean isSplit
		public String nullAsValue
	}

	@CompileStatic
	protected ReadParams readParamDataset (Dataset dataset, Map params) {
		def p = new ReadParams()
		if (params != null) {
			def dsp = getDatasetParams(dataset as FileDataset, params)
			p.params.putAll(dsp)
		}
		
		p.path = p.params.fn
		CSVDataset ds = dataset as CSVDataset

		p.quote = ListUtils.NotNullValue([params.quoteStr, ds.quoteStr()]) as String
		p.quoteStr = p.quote.chars[0]
		
		def fieldDelimiter = ListUtils.NotNullValue([params.fieldDelimiter, ds.fieldDelimiter()]) as String
		p.fieldDelimiter = fieldDelimiter.chars[0]
		
		p.rowDelimiter = ListUtils.NotNullValue([params.rowDelimiter, ds.rowDelimiter()]) as String
		p.isHeader = BoolUtils.IsValue([params.header, ds.isHeader()])
		p.qMode = datasetQuoteMode(ds)
		p.isSplit = BoolUtils.IsValue(params.isSplit)
		p.nullAsValue = ListUtils.NotNullValue([params.nullAsValue, ds.nullAsValue()]) as String
		
		return p
	}
	
	protected static QuoteMode datasetQuoteMode(Dataset dataset) {
		QuoteMode res
		def ds = dataset as CSVDataset
		switch (ds.quoteMode()) {
			case CSVDataset.quoteColumn:
				def b = new boolean[dataset.field.size()]
                for (Integer i = 0; i < dataset.field.size(); i++) {
                    if (dataset.field[i].type in [Field.Type.STRING, Field.Type.TEXT] || dataset.field[i].extended?."quote") b[i] = true
                }
				res = new ColumnQuoteMode(b)
				break
			case CSVDataset.quoteAlways:
				res = new AlwaysQuoteMode()
				break
			default:
				res = new NormalQuoteMode()
				break
		}
		return res
	}
	
	@Override
	List<Field> fields(Dataset dataset) {
		CSVDriver.ReadParams p = readParamDataset(dataset, [:])
		
		def csvFile = new File(p.path)
		if (!csvFile.exists()) throw new ExceptionGETL("File \"${(dataset as CSVDataset).fileName}\" not found or invalid path \"${dataset.connection.params.path}\"")
		Reader fileReader = getFileReader(dataset as FileDataset, [:])

		CsvPreference pref = new CsvPreference.Builder(p.quoteStr, (int)p.fieldDelimiter, p.rowDelimiter).useQuoteMode(p.qMode).build()
		def reader = new CsvListReader(fileReader, pref)
		String[] header = null
		try {
			if (p.isHeader)  {
				header = reader.getHeader(true)
				if (header == null) throw new ExceptionGETL("File \"${(dataset as CSVDataset).fileName}\" is empty")
			}
			else {
				def row = reader.read()
				if (row == null) throw new ExceptionGETL("File \"${(dataset as CSVDataset).fileName}\" is empty")
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

	protected static CellProcessor type2cellProcessor (Field field, Boolean isWrite, Boolean isEscape, String nullAsValue,
													   String locale, String decimalSeparator, String formatDate,
													   String formatTime, String formatDateTime, String formatTimestampWithTz,
													   Boolean isValid) {
		CellProcessor cp
		if (field.type == null || (field.type in [Field.Type.STRING, Field.Type.OBJECT, Field.Type.ROWID, Field.Type.UUID])) {
			if (field.length != null && isValid)
				cp = new StrMinMax(0L, field.length.toLong())

			if (BoolUtils.IsValue(field.trim))
				cp = (cp != null)?new Trim(cp as StringCellProcessor):new Trim(new Optional())

			if (isEscape && field.type == Field.Type.STRING) {
				if (!isWrite)
					cp = (cp != null)?new CSVParseEscapeString(cp as StringCellProcessor):new CSVParseEscapeString()
			}
		} else if (field.type == Field.Type.INTEGER) {
			if (!isWrite)
				cp = new ParseInt()
		} else if (field.type == Field.Type.BIGINT) {
				if (!isWrite)
					cp = new ParseLong()
		} else if (field.type == Field.Type.NUMERIC) {
			def ds = ListUtils.NotNullValue([field.decimalSeparator, decimalSeparator]) as String
			def fieldLocale = (field.extended.locale as String)?:locale

			if (!isWrite) {
				if (field.precision != null || field.format != null || ds != null || fieldLocale != null) {
					DecimalFormatSymbols dfs = (fieldLocale == null)?
							new DecimalFormatSymbols():new DecimalFormatSymbols(StringUtils.NewLocale(fieldLocale))
					if (ds != null) dfs.setDecimalSeparator(ds.chars[0])
					cp = new ParseBigDecimal(dfs)
				}
				else {
					cp = new ParseBigDecimal()
				}
			}
			else {
				if (field.precision != null || field.format != null || ds != null || fieldLocale != null) {
					DecimalFormatSymbols dfs = (fieldLocale == null)?
							new DecimalFormatSymbols():new DecimalFormatSymbols(StringUtils.NewLocale(fieldLocale))
					if (ds != null) dfs.setDecimalSeparator(ds.chars[0])

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

					DecimalFormat df = new DecimalFormat(f, dfs)
					cp = new FmtNumber(df)
				}
			}
		} else if (field.type == Field.Type.DOUBLE) {
			if (!isWrite)
				cp = new ParseDouble()
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
		} else if (field.type in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.TIMESTAMP_WITH_TIMEZONE]) {
			String df = null
			switch (field.type) {
				case Field.Type.DATE:
					df = ListUtils.NotNullValue([field.format, formatDate])
					break
				case Field.Type.TIME:
					df = ListUtils.NotNullValue([field.format, formatTime])
					break
				case Field.Type.DATETIME:
					df = ListUtils.NotNullValue([field.format, formatDateTime])
					break
				case Field.Type.TIMESTAMP_WITH_TIMEZONE:
					df = ListUtils.NotNullValue([field.format, formatTimestampWithTz])
					break
			}
			def fieldLocale = (field.extended.locale as String)?:locale
			if (!isWrite) {
				if (fieldLocale == null) {
					cp = new ParseDate(df, true)
				}
				else {
					cp = new ParseDate(df, true, StringUtils.NewLocale(fieldLocale))
				}
			}
			else {
				if (fieldLocale == null)
					//noinspection UnnecessaryQualifiedReference
					cp = new org.supercsv.cellprocessor.FmtDate(df)
				else
					cp = new CSVFmtDate(df, fieldLocale)
			}
		} else if (field.type == Field.Type.BLOB) {
			if (!isWrite)
				cp = new CSVParseBlob()
			else
				cp = new CSVFmtBlob()
		} else if (field.type == Field.Type.TEXT) {
			if (field.length != null && isValid)
				cp = new StrMinMax(0L, field.length.toLong())

			if (BoolUtils.IsValue(field.trim))
				cp = (cp != null)?new Trim(cp as StringCellProcessor):new Trim(new Optional())

			if (isEscape)
				if (!isWrite)
					cp = (cp != null)?new CSVParseEscapeString(cp as StringCellProcessor):new CSVParseEscapeString()

			if (isWrite)
				cp = (cp != null)?new CSVFmtClob(cp as StringCellProcessor):new CSVFmtClob()
		} else {
			throw new ExceptionGETL("Type ${field.type} not supported")
		}

		if (BoolUtils.IsValue(field.isKey) && isValid)
			cp = (cp != null)?new UniqueHashCode(cp):new UniqueHashCode()

		if (!BoolUtils.IsValue(field.isNull, true) && isValid)
			cp = (cp != null)?new NotNull(cp):new NotNull()
		else
			cp = (cp != null)?new Optional(cp):new Optional()

		if (nullAsValue != null) {
			if (isWrite)
				cp = new ConvertNullTo(nullAsValue, cp)
			else
				cp = new CSVConvertToNullProcessor(nullAsValue, cp)
		}

		return cp
	}
	
	static CellProcessor[] fields2cellProcessor(Map fParams) {
		def dataset = fParams.dataset as CSVDataset
		def fields = fParams.fields as List<String>
		def header = fParams.header as String[]
		def isOptional = fParams.isOptional as Boolean
		def isWrite = fParams.isWrite as Boolean
		def isValid = fParams.isValid as Boolean
		def isEscape = fParams.isEscape as Boolean
		def nullAsValue = fParams.nullAsValue as String
		def locale = ListUtils.NotNullValue([fParams.locale, dataset.locale()]) as String
		def decimalSeparator = ListUtils.NotNullValue([fParams.decimalSeparator, dataset.decimalSeparator()]) as String
		def formatDate = ListUtils.NotNullValue([fParams.formatDate, dataset.formatDate()]) as String
		def formatTime = ListUtils.NotNullValue([fParams.formatTime, dataset.formatTime()]) as String
		def formatDateTime = ListUtils.NotNullValue([fParams.formatDateTime, dataset.formatDateTime()]) as String
		def formatTimestampWithTz = ListUtils.NotNullValue([fParams.formatTimestampWithTz, dataset.formatTimestampWithTz()]) as String
		
		if (fields == null) fields = [] as List<String>

		def cp = new ArrayList<CellProcessor>()
		header.each { String name ->
			if (isOptional) {
				cp << new Optional()
			}
			else {
				name = name.toLowerCase()
				def i = dataset.indexOfField(name)
				String fi = fields.find { (it.toLowerCase() == name.toLowerCase()) } 
				if (i == -1 || (!fields.isEmpty() && fi == null)) {
					cp << new Optional()
				} 
				else {
					Field f = dataset.field[i]
					
					CellProcessor p = type2cellProcessor(f, isWrite, isEscape, nullAsValue, locale, decimalSeparator,
															formatDate, formatTime, formatDateTime, formatTimestampWithTz, isValid)
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
		if (header.isEmpty()) throw new ExceptionGETL('Fields for processing dataset not found!')
		return header.toArray()
	}

	@CompileStatic
	@Override
	Long eachRow (Dataset dataset, Map params, Closure prepareCode, Closure code) {
		if (code == null)
			throw new ExceptionGETL('Required process code')
		
		def cds = dataset as CSVDataset
		if (cds.fileName == null)
			throw new ExceptionGETL('Dataset required fileName')
		
		def escaped = BoolUtils.IsValue([params.escaped, cds.isEscaped()])
		def readAsText = BoolUtils.IsValue(params.readAsText)
		def ignoreHeader = BoolUtils.IsValue(params.ignoreHeader, cds.isIgnoreHeader())
		def skipRows = (params.skipRows as Long)?:0L
		def limit = (params.limit as Long)?:0L

		def filter = params.filter as Closure

		CSVDriver.ReadParams p = readParamDataset(cds, params)
		def countRec = 0L
		Boolean isSplit = BoolUtils.IsValue(p.isSplit)

		def fileMask = fileMaskDataset(cds, isSplit)
		List<Map> files
		Integer portion = 0
		Reader bufReader
		if (isSplit) {
			def fm = cds.currentCsvConnection.files.cloneManager() as FileManager
//			fm.connect()

			def vars = [:]
			vars.put('number', [type: Field.Type.INTEGER, len: 4])
			def filePath = new Path([mask: fileMask, vars: vars])

			fm.buildList(path: filePath)
			def filesParams = [order: ['number']]
			files = fm.fileList.rows(filesParams)
			if (files.isEmpty())
				throw new ExceptionGETL("File(s) \"${cds.fileName}\" not found or invalid path \"${((CSVConnection) cds.connection).currentPath()}\"")
			bufReader = getFileReader(dataset as FileDataset, params, (Integer)files[portion].number)
		}
		else {
			files = [] as List<Map>
			bufReader = getFileReader(dataset as FileDataset, params, null)
		}

		Closure processError = (params.processError != null)?params.processError as Closure:null
		def isValid = BoolUtils.IsValue(params.isValid, cds.isConstraintsCheck())
		CsvPreference pref = new CsvPreference.Builder(p.quoteStr, (int)p.fieldDelimiter, (String)p.rowDelimiter).useQuoteMode((QuoteMode)p.qMode).build()
		CsvMapReader reader
		if (escaped) {
			reader = new CsvMapReader(new CSVEscapeTokenizer(bufReader, pref, p.isHeader), pref)
		}
		else {
			reader = new CsvMapReader(bufReader, pref)
		}

		try {
			String[] header
			List<Field> fileFields = []
			if (p.isHeader) {
				header = reader.getHeader(true)
				if (!ignoreHeader) {
					fileFields = header2fields(header)
				}
				else {
					header = fields2header(dataset.field, null)
				}
			}
			else {
				header = fields2header(dataset.field, null)
			}
			header = (header*.toLowerCase()).toArray(new String()[])

			if (skipRows > 0)
				(1..skipRows).each { reader.getHeader(false) }

			ArrayList<String> listFields = new ArrayList<String>()
			if (prepareCode != null) {
				listFields = (ArrayList<String>)prepareCode.call(fileFields)
			}
			
			CellProcessor[] cp = fields2cellProcessor([dataset: dataset, fields: listFields, header: header, isOptional: readAsText, isWrite: false, isValid: isValid, isEscape: escaped, nullAsValue: p.nullAsValue])
			
			def cur = 0L
			def line = 0L
			while (true) {
				Map row
				def isError = false
				try {
					cur++
					line++
					if (limit > 0 && cur > limit) break
					row = reader.read(header, cp)
				}
				catch (SuperCsvCellProcessorException e) {
					if (processError == null)
						throw e

					def c = e.csvContext
					def ex = new ExceptionGETL("Line $line column ${c.columnNumber} [${header[c.columnNumber - 1]}]: ${e.message}")

					if (!processError(ex, line))
						throw e

					isError = true
				}
				catch (Exception e) {
					def isContinue = (processError != null)?processError(e, line):false
					if (!isContinue) throw e
					isError = true
				}
				if (!isError) {
					if (row == null) {
						cur--
						if (portion != null && (portion + 1) < files.size()) {
							reader.close()
							portion++
							bufReader = getFileReader(dataset as FileDataset, params, (Integer)files[portion].number)
							
							if (escaped) {
								reader = new CsvMapReader(new CSVEscapeTokenizer(bufReader, pref, p.isHeader), pref)
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

					code.call(row)
					if (cur == limit) break
				}
			}
			countRec = cur
		}
		finally {
			reader.close()
			dataset.sysParams.countReadPortions = portion + 1
		}
		
		return countRec
	}
	
	class WriterParams {
		public ICsvMapWriter writer
		public CSVDefaultFileEncoder encoder
		public CsvPreference pref
		public String[] header
		public String headerStr
		public CellProcessor[] cp
		public Map params
		public Boolean isHeader = false
		public String quote
		public String nullAsValue
		public Boolean escaped
		public List<Integer> escapedColumns
		public Long splitSize
		public Boolean formatOutput = true
		public Long batchSize = 500L
		public List<Map> rows = new LinkedList<Map>()
		public Long current = 0L
		public Long batch = 0L
		public Long fieldDelimiterSize = 0L
		public Long rowDelimiterSize = 0L
		public Integer countFields = 0
		public Closure onSaveBatch
		public Closure onSplitFile
		public Writer bufWriter
		public Integer portion
		public Long countCharacters = 0L
		public FileWriteOpts opt

		void free() {
			writer = null
			encoder = null
			pref = null
			params = null
			rows = null
			onSaveBatch = null
			onSplitFile = null
			bufWriter = null
			opt = null
		}
	}

	@Override
	void openWrite (Dataset dataset, Map params, Closure prepareCode) {
		def csv_ds = dataset as CSVDataset
		if (csv_ds.fileName == null)
			throw new ExceptionGETL('Dataset required fileName')

		WriterParams wp = new WriterParams()
		dataset._driver_params = wp
		wp.formatOutput = csv_ds.isFormatOutput()

		ReadParams p = readParamDataset(csv_ds, params)
		Boolean isAppend = BoolUtils.IsValue(p.params.isAppend)
		Boolean isValid = BoolUtils.IsValue(params.isValid, csv_ds.isConstraintsCheck())
		Boolean escaped = BoolUtils.IsValue(params.escaped, csv_ds.isEscaped())
		String formatDate = ListUtils.NotNullValue([params.formatDate, csv_ds.formatDate()])
		String formatTime = ListUtils.NotNullValue([params.formatTime, csv_ds.formatTime()])
		String formatDateTime = ListUtils.NotNullValue([params.formatDateTime, csv_ds.formatDateTime()])
		String formatTimestampWithTz = ListUtils.NotNullValue([params.formatDate, csv_ds.formatTimestampWithTz()])

		if (params.batchSize != null) wp.batchSize = params.batchSize as Long
		if (params.onSaveBatch != null) wp.onSaveBatch = params.onSaveBatch as Closure
		if (params.onSplitFile != null) wp.onSplitFile = params.onSplitFile as Closure
		
		ArrayList<String> listFields = new ArrayList<String>()
		if (prepareCode != null) {
			listFields = prepareCode.call([]) as ArrayList
		}
		
		def header = fields2header(csv_ds.field, listFields)
		if (header.length == 0) throw new ExceptionGETL('Required fields declare')
		wp.header = header*.toLowerCase()

		wp.params = params
		wp.cp = fields2cellProcessor(
					dataset: csv_ds, fields: listFields, header: wp.header, isOptional: false,
					isWrite: true, isValid: isValid, isEscape: escaped, nullAsValue: p.nullAsValue,
					formatDate: formatDate, formatTime: formatTime, formatDateTime: formatDateTime,
				    formatTimestampWithTz: formatTimestampWithTz)

		wp.fieldDelimiterSize = p.fieldDelimiter.toString().length()
		wp.rowDelimiterSize = (p.rowDelimiter as String).length()
		wp.countFields = listFields.size()
		wp.isHeader = p.isHeader
		wp.quote = p.quote
		wp.nullAsValue = p.nullAsValue

		if (escaped) {
			wp.escapedColumns = new ArrayList<Integer>()
			def num = 0
			header.each { fieldName ->
				num++
				if (csv_ds.fieldByName(fieldName).type in [Field.Type.STRING, Field.Type.TEXT])
					wp.escapedColumns << num
			}
			wp.escaped = !(wp.escapedColumns.isEmpty())
		}

		wp.splitSize = params.splitSize as Long
		if (wp.splitSize != null || wp.onSplitFile != null) wp.portion = 1

        csv_ds.params.writeCharacters = null
		
		wp.bufWriter = getFileWriter(csv_ds, wp.params, wp.portion)
		wp.opt = (dataset as FileDataset).writedFiles[(wp.portion?:1) - 1]
		
		wp.encoder = new CSVDefaultFileEncoder(wp)
		
		wp.pref = new CsvPreference.Builder(p.quoteStr as char, (p.fieldDelimiter) as int, p.rowDelimiter as String).useQuoteMode(p.qMode as QuoteMode).useEncoder(wp.encoder).build()
		wp.writer = new CsvMapWriter(wp.bufWriter, wp.pref, false)

		if (wp.isHeader) {
			if (!isAppend) {
				wp.writer.writeHeader(header)
			}
			else {
				def sb = new StringBuffer()
				def strWriter = new StringBufferWriter(sb)
				try {
					def headerWriter = new CsvMapWriter(strWriter, wp.pref, false)
					headerWriter.writeHeader(header)
				}
				finally {
					strWriter.close()
				}
				wp.headerStr = sb.toString()
			}
		}
	}

	@Override
	protected Boolean fileHeader(FileDataset dataset) {
		return (dataset._driver_params as WriterParams).isHeader
	}

	@Override
	protected void saveHeaderToFile(FileDataset dataset, File file) {
		def wp = (dataset._driver_params as WriterParams)
		file.write(wp.headerStr, dataset.codePage, false)
	}
	
	/**
	 * Write rows batch to file
	 * @param dataset
	 * @param wp
	 */
	@CompileStatic
	protected void writeRows (Dataset dataset, WriterParams wp) {
		wp.batch++
		
		try {
			if (wp.formatOutput) {
				wp.rows.each { Map row ->
					wp.writer.write(row, wp.header, wp.cp)

					dataset.writeRows++
					dataset.updateRows++
					wp.opt.countRows = wp.opt.countRows + 1
					
					if ((wp.splitSize != null && wp.encoder.writeSize >= wp.splitSize) || (wp.splitSize == null && wp.onSplitFile != null)) {
						def splitFile = true
						if (wp.onSplitFile != null) {
							splitFile = wp.onSplitFile.call(row)
						}
						if (splitFile) {
							def ds = dataset as FileDataset

							wp.portion++
							wp.writer.close()
							wp.bufWriter = getFileWriter(ds, wp.params, wp.portion)
							wp.opt = ds.writedFiles[wp.portion - 1]
							wp.countCharacters += wp.encoder.writeSize
							wp.encoder.writeSize = 0
							wp.writer = new CsvMapWriter(wp.bufWriter, wp.pref, false)
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
					wp.opt.countRows = wp.opt.countRows + 1

					if ((wp.splitSize != null && wp.encoder.writeSize >= wp.splitSize) || (wp.splitSize == null && wp.onSplitFile != null)) {
						def splitFile = true
						if (wp.onSplitFile != null) {
							splitFile = wp.onSplitFile.call(row)
						}
						if (splitFile) {
							wp.portion++
							wp.writer.close()
							wp.bufWriter = getFileWriter(dataset as FileDataset, wp.params, wp.portion)
							wp.countCharacters += wp.encoder.writeSize
							wp.encoder.writeSize = 0
							wp.writer = new CsvMapWriter(wp.bufWriter, wp.pref, false)
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

	@CompileStatic
	@Override
	void write(Dataset dataset, Map row) {
		WriterParams wp = (dataset._driver_params as WriterParams)
		wp.rows << row
		wp.current++
		
		if (wp.batchSize == 0 || wp.current >= wp.batchSize) writeRows(dataset, wp)
	}

	@Override
	void doneWrite (Dataset dataset) {
		def wp = (dataset._driver_params as WriterParams)
		if (!wp.rows.isEmpty()) writeRows(dataset, wp)
	}

	@Override
	void closeWrite(Dataset dataset) {
		def wp = (dataset._driver_params as WriterParams)
		
		try {
			wp.writer.close()
		}
		finally {
			wp.countCharacters += wp.encoder.writeSize
			
			dataset.sysParams.countWriteCharacters = wp.countCharacters
			dataset.sysParams.countWritePortions = wp.portion

			wp.free()
		}

		try {
			super.closeWrite(dataset)
		}
		finally {
			dataset._driver_params = null
		}
	}

	@CompileStatic
	protected static Long readLinesCount(CSVDataset dataset) {
		if (!dataset.existsFile())
			throw new ExceptionGETL("File \"${dataset.fullFileName()}\" not found!")

		if (!(dataset.rowDelimiter in ['\n', '\r\n']))
			throw new ExceptionGETL('Allow CSV file only standard row delimiter!')

		LineNumberReader reader
		if (dataset.isGzFile) {
			def input = new GZIPInputStream(new FileInputStream(dataset.fullFileName()))
			reader = new LineNumberReader(new InputStreamReader(input, dataset.codePage))
		}
		else {
			reader = new LineNumberReader(new File(dataset.fullFileName()).newReader(dataset.codePage))
		}

		def count = 0L
		
		try {
			def str = reader.readLine()
			while (str != null) {
				count++
				str = reader.readLine()
			}
		}
		finally {
			reader.close()
		}
		
		count - ((dataset.header)?1:0)
	}

	@SuppressWarnings("DuplicatedCode")
	static Long prepareCSVForBulk(CSVDataset target, CSVDataset source, Map<String, String> encodeTable, Closure code) {
		if (!source.existsFile())
			throw new ExceptionGETL("File \"${source.fullFileName()}\" not found!")
		if (!(source.rowDelimiter in ['\n', '\r\n']))
			throw new ExceptionGETL('Allow convert CSV files only standard row delimiter!')

		def sourceCon = source.currentCsvConnection

		def autoSchema = BoolUtils.IsValue(source.autoSchema, sourceCon.autoSchema)
		def header = BoolUtils.IsValue(source.header, sourceCon.isHeader())
		def nullAsValue = source.nullAsValue()

		if (source.field.isEmpty() && autoSchema)
			source.loadDatasetMetadata()

		(target.connection as CSVConnection).validPath()

		if (!source.field.isEmpty()) target.setField(source.field) else target.field.clear()
		target.header = header
		target.nullAsValue = nullAsValue
		target.escaped = false

		String targetRowDelimiter = target.rowDelimiter()
		String sourceFieldDelimiter = source.fieldDelimiter()
		String sourceFieldDelimiterLast = sourceFieldDelimiter + '\u0000'
		String targetFieldDelimiter = target.fieldDelimiter()
		def source_escaped = source.isEscaped()
		String sourceQuoteStr = source.quoteStr()
		String targetQuoteStr = target.quoteStr()
		
		String decodeQuote = sourceQuoteStr
		if (targetQuoteStr == sourceQuoteStr) decodeQuote += sourceQuoteStr
		Map convertMap = ['\u0004': "$decodeQuote", '\u0005': '\\\\']
		
		Map<String, String> encodeMap = [:]
		if (!source_escaped) {
			encodeMap."$sourceQuoteStr$sourceQuoteStr" = '\u0004'
		}
		else {
			encodeMap."\\\\" = '\u0005'
			encodeMap."\\$sourceQuoteStr" = '\u0004'
			encodeMap."\\n" = '\n'
			encodeMap."\\t" = '\t'
		}
		
		String splitFieldDelimiter = StringUtils.Delimiter2SplitExpression(sourceFieldDelimiter)

		def fw = new File(target.fullFileName())
		if (target.autoSchema) target.saveDatasetMetadata()
		if (BoolUtils.IsValue(target.params.deleteOnExit, false)) {
			fw.deleteOnExit()
			if (target.autoSchema) {
				File ws = new File(target.fullFileSchemaName())
				ws.deleteOnExit()
			}
		}
		
		def count = 0L
		
		LineNumberReader reader
		if (source.isGzFile) {
			def input = new GZIPInputStream(new FileInputStream(source.fullFileName()))
			reader = new LineNumberReader(new InputStreamReader(input, source.codePage))
		}
		else {
			reader = new LineNumberReader(new InputStreamReader(new FileInputStream(source.fullFileName()), source.codePage))
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
				String readStr = reader.readLine()
				def isQuoteFirst = false
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
					def colsSize = cols.size() - 1
					for (Integer colI = 0; colI < colsSize; colI++) {
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
						readStr = reader.readLine()
						continue
					}
					
					convertCSVValues(values, convertMap, code)
					writer.write(values.join(targetFieldDelimiter))
					writer.write(targetRowDelimiter)
					values.clear()
					count++

					readStr = reader.readLine()
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
			for (Integer i = 0; i < values.size(); i++) { values[i] = values[i].replace(oldStr, newStr) }
		}

		if (code != null) code.call(values)
	}

	@SuppressWarnings("DuplicatedCode")
	static Long decodeBulkCSV (CSVDataset target, CSVDataset source) {
		if (!source.existsFile()) throw new ExceptionGETL("File \"${source.fullFileName()}\" not found")
		
		if (source.field.isEmpty() && source.autoSchema) source.loadDatasetMetadata()
		if (!source.field.isEmpty()) target.setField(source.field)
		
		(target.connection as CSVConnection).validPath()
		
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
		
		def count = 0L

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