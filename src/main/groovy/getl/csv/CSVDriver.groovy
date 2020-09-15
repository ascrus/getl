package getl.csv

import getl.csv.proc.CSVConvertToNullProcessor
import getl.csv.proc.CSVDefaultFileEncoder
import getl.csv.proc.CSVEscapeTokenizer
import getl.csv.proc.CSVFmtBlob
import getl.csv.proc.CSVFmtClob
import getl.csv.proc.CSVFmtDate
import getl.csv.proc.CSVParseBlob
import getl.csv.proc.CSVParseEscapeString
import getl.data.opts.FileWriteOpts
import groovy.transform.CompileStatic
import org.codehaus.groovy.runtime.StringBufferWriter

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
class CSVDriver extends FileDriver {
	CSVDriver () {
		super()
		methodParams.register('eachRow', ['isValid', 'quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 'isSplit', 'readAsText', 
											'escaped', 'processError', 'filter', 'nullAsValue', 'ignoreHeader', 'skipRows', 'limit'])
		methodParams.register('openWrite', ['batchSize', 'onSaveBatch', 'isValid', 'escaped', 'splitSize', 
								'quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 'nullAsValue', 'decimalSeparator', 
								'formatDate', 'formatTime', 'formatDateTime', 'onSplitFile'])
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
		Map params = [:]
		String path
		def quoteStr
		def quote
		def fieldDelimiter
		def rowDelimiter
		Boolean isHeader
		def qMode
		def isSplit
		String nullAsValue
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
		
		p.quote = ListUtils.NotNullValue([params.quoteStr, ds.quoteStr])
		p.quoteStr = (p.quote as String).charAt(0)
		
		def fieldDelimiter = ListUtils.NotNullValue([params.fieldDelimiter, ds.fieldDelimiter])
		p.fieldDelimiter = (fieldDelimiter as String).charAt(0)
		
		p.rowDelimiter = ListUtils.NotNullValue([params.rowDelimiter, ds.rowDelimiter])
		p.isHeader = BoolUtils.IsValue(params.header as Boolean, ds.header)
		p.qMode = datasetQuoteMode(ds)
		p.isSplit = (params.isSplit != null)?params.isSplit:false
		p.nullAsValue = ListUtils.NotNullValue([params.nullAsValue, ds.nullAsValue])
		
		return p
	}
	
	protected static QuoteMode datasetQuoteMode(Dataset dataset) {
		QuoteMode qMode
		switch ((dataset as CSVDataset).quoteMode) {
			case CSVDataset.QuoteMode.COLUMN:
				def b = new boolean[dataset.field.size()]
                for (Integer i = 0; i < dataset.field.size(); i++) {
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
	List<Field> fields(Dataset dataset) {
		def p = readParamDataset(dataset, [:]) 
		
		def csvfile = new File(p.path)
		if (!csvfile.exists()) throw new ExceptionGETL("File \"${(dataset as CSVDataset).fileName}\" not found or invalid path \"${dataset.connection.params.path}\"")
		Reader fileReader = getFileReader(dataset as FileDataset, [:])

		CsvPreference pref = new CsvPreference.Builder(p.quoteStr as char, (p.fieldDelimiter) as int, p.rowDelimiter as String).useQuoteMode(p.qMode as QuoteMode).build()
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
													   String formatTime, String formatDateTime, Boolean isValid) {
		CellProcessor cp
		
		if (field.type == null || (field.type in [Field.Type.STRING, Field.Type.OBJECT, Field.Type.ROWID, Field.Type.UUID])) {
			if (BoolUtils.IsValue(field.trim)) cp = new Trim()

			if (isEscape && field.type == Field.Type.STRING) {
				if (!isWrite)
					/*cp = (cp != null)?new CSVFmtEscapeString(cp):new CSVFmtEscapeString()
				else*/
					cp = (cp != null)?new CSVParseEscapeString(cp):new CSVParseEscapeString()
			}
		} else if (field.type == Field.Type.INTEGER) {
			if (!isWrite) {
				cp = new ParseInt()
			}
		} else if (field.type == Field.Type.BIGINT) {
				if (!isWrite) {
					cp = new ParseLong()
				}
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
			if (!isWrite) {
				cp = new ParseDouble()
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
		} else if (field.type in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.TIMESTAMP_WITH_TIMEZONE]) {
			String df = null
			switch (field.type) {
				case Field.Type.DATE:
					df = ListUtils.NotNullValue([field.format, formatDate, 'yyyy-MM-dd'])
					break
				case Field.Type.TIME:
					df = ListUtils.NotNullValue([field.format, formatTime, 'HH:mm:ss'])
					break
				case Field.Type.DATETIME: case Field.Type.TIMESTAMP_WITH_TIMEZONE:
					df = ListUtils.NotNullValue([field.format, formatDateTime, 'yyyy-MM-dd HH:mm:ss'])
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
				if (fieldLocale == null) {
					//noinspection UnnecessaryQualifiedReference
					cp = new org.supercsv.cellprocessor.FmtDate(df)
				}
				else {
					cp = new CSVFmtDate(df, fieldLocale)
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
			if (isWrite) {
				cp = new CSVFmtClob()
			}

			if (isEscape) {
				if (!isWrite)
					/*cp = (cp != null)?new CSVFmtEscapeString(cp):new CSVFmtEscapeString()
				else*/
					cp = (cp != null)?new CSVParseEscapeString(cp):new CSVParseEscapeString()
			}
		} else {
			throw new ExceptionGETL("Type ${field.type} not supported")
		}

		if (BoolUtils.IsValue(field.isKey) && isValid)
			cp = (cp != null)?new UniqueHashCode(cp):new UniqueHashCode()

		if (!BoolUtils.IsValue(field.isNull, true) && isValid)
			cp = (cp != null)?new NotNull(cp):new NotNull()
		else
			cp = (cp != null)?new Optional(cp):new Optional()

		if (nullAsValue != null && BoolUtils.IsValue(field.isNull, true)) {
			if (isWrite)
				cp = new ConvertNullTo(nullAsValue, cp)
			else
				cp = new CSVConvertToNullProcessor(nullAsValue, cp)
		}

		/*CellProcessor c = cp
		print "${field.name}: "
		while (c != CellProcessorAdaptor.NullObjectPattern.INSTANCE) {
			print c.getClass().name + ";"
			c = c.next
		}
		println ""*/

		return cp
	}
	
	static CellProcessor[] fields2cellProcessor(Map fParams) {
		CSVDataset dataset = (CSVDataset)fParams.dataset
		ArrayList<String> fields = (ArrayList<String>)fParams.fields
		String[] header = (String[])fParams.header
		Boolean isOptional = fParams.isOptional as Boolean
		Boolean isWrite = fParams.isWrite as Boolean
		Boolean isValid = fParams.isValid as Boolean
		Boolean isEscape = fParams.isEscape as Boolean
		String nullAsValue = fParams.nullAsValue as String
		String locale = ListUtils.NotNullValue([fParams.locale, dataset.locale]) as String
		String decimalSeparator = ListUtils.NotNullValue([fParams.decimalSeparator, dataset.decimalSeparator, '.']) as String
		String formatDate = ListUtils.NotNullValue([fParams.formatDate, dataset.formatDate]) as String
		String formatTime = ListUtils.NotNullValue([fParams.formatTime, dataset.formatTime]) as String
		String formatDateTime = ListUtils.NotNullValue([fParams.formatDateTime, dataset.formatDateTime]) as String
		
		if (fields == null) fields = [] as ArrayList<String>
//		def quoteStr = dataset.quoteStr
		
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
		
		def escaped = BoolUtils.IsValue(params.escaped, cds.escaped)
		def readAsText = BoolUtils.IsValue(params.readAsText)
		def ignoreHeader = BoolUtils.IsValue(params.ignoreHeader, true)
		def skipRows = (params.skipRows as Long)?:0L
		def limit = (params.limit as Long)?:0L

		def filter = params.filter as Closure

		def p = readParamDataset(cds, params)
		def countRec = 0L
		Boolean isSplit = BoolUtils.IsValue(p.isSplit)

		def fileMask = fileMaskDataset(cds, isSplit)
		List<Map> files
		Integer portion = 0
		Reader bufReader
		if (isSplit) {
			FileManager fm = new FileManager(rootPath: ((CSVConnection) cds.connection).currentPath())
			fm.connect()

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
		def isValid = BoolUtils.IsValue([params.isValid, cds.constraintsCheck], false)
		CsvPreference pref = new CsvPreference.Builder((char)p.quoteStr, (int)p.fieldDelimiter, (String)p.rowDelimiter).useQuoteMode((QuoteMode)p.qMode).build()
		CsvMapReader reader
		if (escaped) {
			reader = new CsvMapReader(new CSVEscapeTokenizer(bufReader, pref, p.isHeader), pref)
		}
		else {
			reader = new CsvMapReader(bufReader, pref)
		}

		try {
			String[] header
			List<Field> filefields = []
			if (p.isHeader) {
				header = reader.getHeader(true)
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
			header = (header*.toLowerCase()).toArray(new String()[])

			if (skipRows > 0)
				(1..skipRows).each { reader.getHeader(false) }

			ArrayList<String> listFields = new ArrayList<String>()
			if (prepareCode != null) {
				listFields = (ArrayList<String>)prepareCode.call(filefields)
			}
			
			CellProcessor[] cp = fields2cellProcessor([dataset: dataset, fields: listFields, header: header, isOptional: readAsText, isWrite: false, isValid: isValid, isEscape: escaped, nullAsValue: p.nullAsValue])
			
			def count = (params.limit != null)?params.limit as Long:0L
			def cur = 0L
			while (true) {
				Map row
				def isError = false
				try {
					cur++
					if (limit > 0 && cur > limit) break
					row = reader.read(header, cp)
				}
				catch (Exception e) {
					def isContinue = (processError != null)?processError(e, cur + 1):false
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
					if (cur == count) break
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
		if (csv_ds.fileName == null) throw new ExceptionGETL('Dataset required fileName')
		
		WriterParams wp = new WriterParams()
		dataset._driver_params = wp
		wp.formatOutput = csv_ds.formatOutput

		ReadParams p = readParamDataset(csv_ds, params)
		Boolean isAppend = BoolUtils.IsValue(p.params.isAppend)
		Boolean isValid = BoolUtils.IsValue([params.isValid, csv_ds.constraintsCheck], false)
		Boolean escaped = BoolUtils.IsValue(params.escaped, csv_ds.escaped)
		String formatDate = ListUtils.NotNullValue([params.formatDate, csv_ds.formatDate])
		String formatTime = ListUtils.NotNullValue([params.formatTime, csv_ds.formatTime])
		String formatDateTime = ListUtils.NotNullValue([params.formatDateTime, csv_ds.formatDateTime])

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
					formatDate: formatDate, formatTime: formatTime, formatDateTime: formatDateTime)
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
		
		wp.encoder = new CSVDefaultFileEncoder(csv_ds, wp)
		
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

	protected static Long readLinesCount(CSVDataset dataset) {
		if (!dataset.existsFile())
			throw new ExceptionGETL("File \"${dataset.fullFileName()}\" not found")

		if (!(dataset.rowDelimiter in ['\n', '\r\n']))
			throw new ExceptionGETL('Allow CSV file only standart row delimiter')
		
		BufferedReader reader
		if (dataset.isGzFile) {
			def input = new GZIPInputStream(new FileInputStream(dataset.fullFileName()))
			reader = new BufferedReader(new InputStreamReader(input, dataset.codePage))
		}
		else {
			reader = new File(dataset.fullFileName()).newReader(dataset.codePage)
		}

		def count = 0L
		
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

	@SuppressWarnings("DuplicatedCode")
	static Long prepareCSVForBulk(CSVDataset target, CSVDataset source, Map<String, String> encodeTable, Closure code) {
		if (!source.existsFile()) throw new ExceptionGETL("File \"${source.fullFileName()}\" not found")
		if (!(source.rowDelimiter in ['\n', '\r\n'])) throw new ExceptionGETL('Allow convert CSV files only standart row delimiter')

		if (source.field.isEmpty() && source.autoSchema) source.loadDatasetMetadata()
		//if (source.field.isEmpty()) throw new ExceptionGETL('Required fields from source dataset')
		
		(target.connection as CSVConnection).validPath()

		if (!source.field.isEmpty()) target.setField(source.field) else target.field.clear()
		target.header = source.header
		target.nullAsValue = source.nullAsValue
		target.escaped = false

		String targetRowDelimiter = target.rowDelimiter
		String sourceFieldDelimiter = source.fieldDelimiter
		String sourceFieldDelimiterLast = sourceFieldDelimiter + '\u0000'
		String targetFieldDelimiter = target.fieldDelimiter
		def source_escaped = source.escaped
		String sourceQuoteStr = source.quoteStr
		String targetQuoteStr = target.quoteStr
		
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