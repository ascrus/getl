package getl.csv

import getl.csv.proc.*

import getl.data.sub.FileWriteOpts
import getl.data.*
import getl.driver.Driver
import getl.driver.FileDriver
import getl.exception.ExceptionGETL
import getl.files.FileManager
import getl.utils.*
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import org.codehaus.groovy.runtime.StringBufferWriter
import java.text.DecimalFormat
import java.time.format.ResolverStyle
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
@InheritConstructors
class CSVDriver extends FileDriver {
	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('eachRow', ['isValid', 'quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header',
										  'isSplit', 'readAsText', 'escaped', 'processError', 'filter',
										  'nullAsValue', 'fieldOrderByHeader', 'skipRows', 'limit'])
		methodParams.register('openWrite', ['batchSize', 'onSaveBatch', 'isValid', 'escaped', 'splitSize',
											'quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 'nullAsValue',
											'decimalSeparator', 'formatDate', 'formatTime', 'formatDateTime',
											'formatTimestampWithTz', 'uniFormatDateTime', 'formatBoolean', 'onSplitFile'])
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		[Driver.Support.WRITE, Driver.Support.AUTOLOADSCHEMA, Driver.Support.AUTOSAVESCHEMA, Driver.Support.EACHROW, Driver.Support.PRIMARY_KEY,
		 Driver.Support.NOT_NULL_FIELD]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations () {
		[Driver.Operation.DROP, Driver.Operation.RETRIEVEFIELDS]
	}

	@SuppressWarnings('UnnecessaryQualifiedReference')
	class ReadParams {
		public Map params = new HashMap()
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

	@SuppressWarnings('UnnecessaryQualifiedReference')
	@Override
	List<Field> fields(Dataset dataset) {
		def csv = dataset as CSVDataset
		ReadParams p = readParamDataset(dataset, new HashMap())
		
		def csvFile = new File(p.path)
		if (!csvFile.exists())
			throw new ExceptionGETL("File \"${csv.fileName()}\" not found or invalid path \"${csv.currentCsvConnection?.path}\"!")
		Reader fileReader = getFileReader(csv, new HashMap())

		CsvPreference pref = new CsvPreference.Builder(p.quoteStr, (int)p.fieldDelimiter, p.rowDelimiter).useQuoteMode(p.qMode).build()
		List<Field> res = []
		try (def reader = new CsvListReader(fileReader, pref)) {
			String[] header
			if (p.isHeader)  {
				header = reader.getHeader(true)
				if (header == null)
					throw new ExceptionGETL("File \"${csv.fileName()}\" is empty!")
			}
			else {
				def row = reader.read()
				if (row == null)
					throw new ExceptionGETL("File \"${csv.fileName()}\" is empty!")
				def c = 0
				def list = [] as List<String>
				row.each {
					c++
					list.add("col_${c}".toString())
				}
				header = list.toArray(new String[0])
			}

			header.each { String name ->
				if (name == null || name.length() == 0)
					throw new ExceptionGETL("Detected empty field name for $header")

				res.add(new Field(name: name, type: Field.Type.OBJECT, isNull: false))
			}
		}

		def nullValue = csv.nullAsValue()?.toLowerCase()
		def dateFormatter = DateUtils.BuildDateFormatter(csv.uniFormatDateTime()?:csv.formatDate(), ResolverStyle.STRICT, csv.locale())
		def timeFormatter = DateUtils.BuildTimeFormatter(csv.uniFormatDateTime()?:csv.formatTime(), ResolverStyle.STRICT, csv.locale())
		def dateTimeFormatter = DateUtils.BuildDateFormatter(csv.uniFormatDateTime()?:csv.formatDateTime(), ResolverStyle.STRICT, csv.locale())
		def timestampWithTzFormatter = DateUtils.BuildDateFormatter(csv.uniFormatDateTime()?:csv.formatTimestampWithTz(), ResolverStyle.STRICT, csv.locale())
		def decimalSeparator = csv.decimalSeparator()
		def groupingSeparator = csv.groupSeparator()
		def decimalFormatSymbol = NumericUtils.BuildDecimalFormatSymbols(decimalSeparator.chars[0],
				(groupingSeparator != null)?groupingSeparator.chars[0]:null, csv.locale())
		def decimalFormat = new DecimalFormat('#,##0.#', decimalFormatSymbol)
		def booleanFormat = (csv.formatBoolean()?.toLowerCase()?:'true|false').split('[|]')

		fileReader = getFileReader(csv, new HashMap())

		List<String> row
		def lengths = res.collect { 0 }
		def useGrouping = res.collect { false }
		try (def reader = new CsvListReader(fileReader, pref)) {
			if (p.isHeader)
				reader.getHeader(true)

			while ((row = reader.read()) != null) {
				for (int i = 0; i < res.size(); i++) {
					def col = row[i]
					def field = res[i]

					if (col == null || (nullValue == null && col.length() == 0) || (nullValue != null && nullValue == col.toLowerCase())) {
						field.isNull = true
						continue
					}

					if (col.matches('^\\s+.+')) {
						col = col.trim()
						field.trim = true
					}

					switch (field.type) {
						case Field.objectFieldType:
							if (DateUtils.ParseSQLTime(timeFormatter, col, true) != null)
								field.type = Field.timeFieldType
							else if (DateUtils.ParseSQLDate(dateFormatter, col, true) != null)
								field.type = Field.dateFieldType
							else if (DateUtils.ParseSQLTimestamp(dateTimeFormatter, col, true) != null)
								field.type = Field.datetimeFieldType
							else if (DateUtils.ParseSQLTimestamp(timestampWithTzFormatter, col, true) != null)
								field.type = Field.timestamp_with_timezoneFieldType
							else if (col.matches('(?i)^(\\w){8}-(\\w){4}-(\\w){4}-(\\w){4}-(\\w){12}$'))
								field.type = Field.uuidFieldType
							else if ((NumericUtils.ParseString(decimalFormat, col, true)) != null) {
								field.type = Field.numericFieldType
								if (groupingSeparator != null && col.indexOf(groupingSeparator) != -1) {
									useGrouping[i] = true
									col = col.replace(groupingSeparator, '')
								}
								def pos = col.indexOf(decimalSeparator)
								field.precision = (pos != -1)?(col.size() - pos - 1):0
							} else if (col.toLowerCase() in booleanFormat)
								field.type = Field.booleanFieldType
							else
								field.type = Field.stringFieldType

							break
						case Field.dateFieldType:
							if (DateUtils.ParseSQLDate(dateFormatter, col, true) == null) {
								if (DateUtils.ParseSQLDate(dateTimeFormatter, col, true) != null)
									field.type = Field.datetimeFieldType
								else
									field.type = Field.stringFieldType
							}
							break
						case Field.datetimeFieldType:
							if (DateUtils.ParseSQLTimestamp(dateTimeFormatter, col, true) == null)
								field.type = Field.stringFieldType
							break
						case Field.timeFieldType:
							if (DateUtils.ParseSQLTime(timeFormatter, col, true) == null)
								field.type = Field.stringFieldType
							break
						case Field.timestamp_with_timezoneFieldType:
							if (DateUtils.ParseSQLTimestamp(timestampWithTzFormatter, col, true) == null)
								field.type = Field.stringFieldType
							break
						case Field.numericFieldType:
							if ((NumericUtils.ParseString(decimalFormat, col, true)) == null)
								field.type = Field.stringFieldType
							else {
								if (groupingSeparator != null && col.indexOf(groupingSeparator) != -1) {
									if (!useGrouping[i])
										useGrouping[i] = true
									col = col.replace(groupingSeparator, '')
								}
								def pos = col.indexOf(decimalSeparator)
								def precision = (pos != -1) ? col.size() - pos - 1 : 0
								if (precision > field.precision)
									field.precision = precision
							}
							break
						case Field.booleanFieldType:
							if (!(col.toLowerCase() in booleanFormat))
								field.type = Field.stringFieldType
							break
						case Field.uuidFieldType:
							if (!col.matches('(?i)^(\\w){8}-(\\w){4}-(\\w){4}-(\\w){4}-(\\w){12}$'))
								field.type = Field.stringFieldType
					}

					if (col.size() > lengths[i])
						lengths[i] = col.size()
				}
			}
		}

		for (int i = 0; i < res.size(); i++) {
			def field = res[i]
			def length = lengths[i]
			switch (field.type) {
				case Field.objectFieldType:
					field.type = Field.stringFieldType
					if (length > 0)
						field.length = length
					break
				case Field.stringFieldType:
					if (length > 0)
						field.length = length
					break
				case Field.dateFieldType: case Field.datetimeFieldType:
				case Field.timeFieldType: case Field.timestamp_with_timezoneFieldType:
				case Field.booleanFieldType: case Field.uuidFieldType:
					field.length = null
					field.precision = null
					break
				case Field.numericFieldType:
					if (field.precision == 0 && !useGrouping[i]) {
						if (length < Integer.MAX_VALUE.toString().size())
							field.type = Field.integerFieldType
						else if (length < Long.MAX_VALUE.toString().length())
							field.type = Field.bigintFieldType
					}
					if (field.type == Field.numericFieldType)
						field.length = length
			}
		}
		
		return res
	}

	static private List<Field> header2fields(String[] header, List<Field> listField) {
		List<Field> fields = []
		def c = 0
		def size = header.length
		for (int i = 0; i < size; i++) {
			def name = header[i]
			if (name == null || name.length() == 0)
				throw new ExceptionGETL("Detected empty field name for $header")

			def findName = name.toLowerCase()
			def field = listField.find { field -> field.name.toLowerCase() == findName }
			if (field == null) {
				c++
				field = new Field(name: "_getl_csv_col_$c".toString())
			}

			fields.add(field)
		}

		return fields
	}

	static private CellProcessor type2cellProcessor(Field field, Boolean isWrite, Boolean isEscape, String nullAsValue,
													   String locale, String decimalSeparator, String groupSeparator, String formatDate,
													   String formatTime, String formatDateTime, String formatTimestampWithTz,
													   String formatBoolean, Boolean isValid) {
		CellProcessor cp = null
		if (field.type == null || (field.type in [Field.stringFieldType, Field.objectFieldType, Field.rowidFieldType, Field.uuidFieldType])) {
			if (field.length != null && isValid)
				cp = new StrMinMax(0L, field.length.toLong())

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

			def dfs = NumericUtils.BuildDecimalFormatSymbols(ds?.chars[0],
					(groupSeparator != null)?groupSeparator.chars[0]:null, fieldLocale)
			if (!isWrite)
				cp = new ParseBigDecimal(dfs)
			else {
				def cf = (groupSeparator != null)?'#,##0':'0'
				String f = cf + '.#'
				if (field.precision != null || field.format != null) {
					def p = (field.precision != null)?field.precision:0
					if (field.format != null)
						f = field.format
					else if (p != null) {
						if (p > 0)
							f = cf + '.' + StringUtils.Replicate('0', p)
						else
							f = cf
					}
				}

				DecimalFormat df = new DecimalFormat(f, dfs)
				cp = new FmtNumber(df)
			}
		} else if (field.type == Field.Type.DOUBLE) {
			if (!isWrite)
				cp = new ParseDouble()
		} else if (field.type == Field.Type.BOOLEAN) {
			def df = (ListUtils.NotNullValue([field.format, formatBoolean, '1|0']) as String).toLowerCase()
			String[] v = df.split('[|]')
			if (v[0] == null) v[0] = '1'
			if (v[1] == null) v[1] = '0'

			String[] listTrue = [v[0]]
			String[] listFalse = [v[1]]
			
			if (!isWrite) {
				if (df != null)
					cp = new ParseBool(listTrue, listFalse, true)
				else
					cp = new ParseBool()
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

			/*if (BoolUtils.IsValue(field.trim))
				cp = (cp != null)?new Trim(cp as StringCellProcessor):new Trim(new Optional())*/

			if (isEscape)
				if (!isWrite)
					cp = (cp != null) ? new CSVParseEscapeString(cp as StringCellProcessor) : new CSVParseEscapeString()

			if (isWrite)
				cp = (cp != null) ? new CSVFmtClob(cp as StringCellProcessor) : new CSVFmtClob()
		}
		else if (field.type == Field.arrayFieldType) {
			if (!isWrite)
				cp = new CSVParseArray()
			else
				cp = new CSVFmtArray()
		} else {
			throw new ExceptionGETL("Type ${field.type} not supported")
		}

		if (BoolUtils.IsValue(field.isKey) && isValid)
			cp = (cp != null)?new UniqueHashCode(cp):new UniqueHashCode()

		if (!BoolUtils.IsValue(field.isNull, true) && isValid)
			cp = (cp != null)?new NotNull(cp):new NotNull()
		else if (cp != null)
			cp = new Optional(cp)
			//cp = (cp != null)?new Optional(cp):new Optional()

		if (!isWrite && BoolUtils.IsValue(field.trim)) {
			def isStr = field.type in [Field.stringFieldType, Field.textFieldType]
			cp = (cp != null) ? new CSVTrimProcessor(!isStr, cp) : new CSVTrimProcessor(isStr)
		}

		if (nullAsValue != null) {
			if (isWrite)
				cp = (cp != null)?new ConvertNullTo(nullAsValue, cp):new ConvertNullTo(nullAsValue)
			else
				cp = (cp != null)?new CSVConvertToNullProcessor(nullAsValue, cp):new CSVConvertToNullProcessor(nullAsValue)
		}

		return cp?:new Optional()
	}
	
	static CellProcessor[] fields2cellProcessor(Map fParams) {
		def dataset = fParams.dataset as CSVDataset
		def fields = fParams.fields as List<String>
		def header = fParams.header as String[]
		def isOptional = fParams.isOptional as Boolean
		def isWrite = fParams.isWrite as Boolean
		def isValid = fParams.isValid as Boolean
		def escaped = BoolUtils.IsValue(fParams.isEscape, dataset.escaped())
		def nullAsValue = ListUtils.NotNullValue([fParams.nullAsValue, dataset.nullAsValue()]) as String
		def locale = ListUtils.NotNullValue([fParams.locale, dataset.locale()]) as String
		def decimalSeparator = ListUtils.NotNullValue([fParams.decimalSeparator, dataset.decimalSeparator()]) as String
		def groupSeparator = ListUtils.NotNullValue([fParams.groupSeparator, dataset.groupSeparator()]) as String
		def uniFormatDateTime = ListUtils.NotNullValue([fParams.uniFormatDateTime, dataset.uniFormatDateTime()]) as String
		def formatDate = ListUtils.NotNullValue([fParams.formatDate, uniFormatDateTime, dataset.formatDate()]) as String
		def formatTime = ListUtils.NotNullValue([fParams.formatTime, uniFormatDateTime, dataset.formatTime()]) as String
		def formatDateTime = ListUtils.NotNullValue([fParams.formatDateTime, uniFormatDateTime, dataset.formatDateTime()]) as String
		def formatTimestampWithTz = ListUtils.NotNullValue([fParams.formatTimestampWithTz, uniFormatDateTime, dataset.formatTimestampWithTz()]) as String
		def formatBoolean = ListUtils.NotNullValue([fParams.formatBoolean, dataset.formatBoolean()]) as String
		
		if (fields == null)
			fields = [] as List<String>

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
					
					CellProcessor p = type2cellProcessor(f, isWrite, escaped, nullAsValue, locale, decimalSeparator, groupSeparator,
															formatDate, formatTime, formatDateTime, formatTimestampWithTz, formatBoolean, isValid)
					cp << p
				}
			}
		}

		return cp.toArray() as CellProcessor[]
	} 
	
	protected static String[] fields2header(List<Field> fields, List<String> writeFields) {
		if (writeFields == null) writeFields = []
		def header = []
		fields.each { v ->
			if (writeFields.isEmpty()) {
				header << v.name
			}
			else {
				def fi = writeFields.find { (it.toLowerCase() == v.name.toLowerCase()) }
				if (fi != null) header << v.name
			}
		}
		if (header.isEmpty())
			throw new ExceptionGETL('Fields for processing dataset not found!')

		return header.toArray()
	}

	@CompileStatic
	@Override
	Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		if (code == null)
			throw new ExceptionGETL('Required process code!')
		
		def cds = dataset as CSVDataset
		if (cds.fileName == null)
			throw new ExceptionGETL('Dataset required fileName!')

		cds.currentCsvConnection.validPath()
		
		def escaped = BoolUtils.IsValue([params.escaped, cds.escaped()])
		def readAsText = BoolUtils.IsValue(params.readAsText)
		def fieldOrderByHeader = BoolUtils.IsValue(params.fieldOrderByHeader, cds.isFieldOrderByHeader())
		def formatDate = params.formatDate as String
		def formatTime = params.formatTime as String
		def formatDateTime = params.formatDateTime as String
		def formatTimestampWithTz = params.formatDate as String
		def uniFormatDateTime = params.uniFormatDateTime as String

		def skipRows = (params.skipRows as Long)?:0L
		def limit = (params.limit as Long)?:0L

		def filter = params.filter as Closure

		//noinspection UnnecessaryQualifiedReference
		CSVDriver.ReadParams p = readParamDataset(cds, params)
		def countRec = 0L
		Boolean isSplit = BoolUtils.IsValue(p.isSplit)

		def fileMask = fileMaskDataset(cds, isSplit)
		List<Map> files
		Integer portion = 0
		Integer numPortion = null
		Reader bufReader
		if (isSplit) {
			def fm = cds.currentCsvConnection.connectionFileManager.cloneManager(null, dataset.dslCreator) as FileManager

			def vars = new HashMap()
			vars.put('number', [type: Field.Type.INTEGER, len: 4])
			def filePath = new Path([mask: fileMask, vars: vars])

			fm.buildList(path: filePath)
			def filesParams = [order: ['number']]
			files = fm.fileList.rows(filesParams)
			if (files.isEmpty())
				throw new ExceptionGETL("File(s) \"${cds.fileName()}\" not found or invalid path \"${((CSVConnection) cds.connection).currentPath()}\"!")
			numPortion = files[portion].number as Integer
		}
		else {
			files = [] as List<Map>
		}
		bufReader = getFileReader(dataset as FileDataset, params, numPortion)

		Closure processError = (params.processError != null)?params.processError as Closure:null
		def isValid = BoolUtils.IsValue(params.isValid, cds.isConstraintsCheck())
		CsvPreference pref = new CsvPreference.Builder(p.quoteStr, (int)p.fieldDelimiter, (String)p.rowDelimiter).useQuoteMode((QuoteMode)p.qMode).build()
		CsvMapReader reader
		if (escaped)
			reader = new CsvMapReader(new CSVEscapeTokenizer(bufReader, pref, p.isHeader), pref)
		else
			reader = new CsvMapReader(bufReader, pref)

		try {
			String[] header
			List<Field> fileFields
			if (p.isHeader) {
				header = reader.getHeader(true)
				if (fieldOrderByHeader)
					fileFields = header2fields(header, dataset.field)
				else {
					header = fields2header(dataset.field, null)
					fileFields = dataset.field
				}
			}
			else {
				fileFields = dataset.field
				header = fields2header(dataset.field, null)
				if (fieldOrderByHeader) {
					try (def newBufReader = getFileReader(dataset as FileDataset, params, numPortion)) {
						CsvMapReader newReader
						if (escaped)
							newReader = new CsvMapReader(new CSVEscapeTokenizer(newBufReader, pref, p.isHeader), pref)
						else
							newReader = new CsvMapReader(newBufReader, pref)

						def colHeader = newReader.getHeader(true)
						def fieldSize = fileFields.size()
						def newHeader = header.toList()
						for (int i = fieldSize; i < colHeader.length; i++) {
							def colName = "_getl_csv_col_$i".toString()
							newHeader.add(colName)
							fileFields.add(new Field(name: colName))
						}
						header = newHeader.toArray(new String()[])
					}
				}
			}

			header = (header*.toLowerCase()).toArray(new String()[])

			if (skipRows > 0)
				(1..skipRows).each { reader.getHeader(false) }

			List<String> listFields
			if (prepareCode != null)
				listFields = prepareCode.call(fileFields.findAll { field -> !field.name.matches('(?i)^[_]getl[_]csv[_]col[_]\\d+') }) as List<String>
			else
				listFields = fileFields.findAll { field -> !field.name.matches('(?i)^[_]getl[_]csv[_]col[_]\\d+') }
						.collect { field -> field.name }
			
			CellProcessor[] cp = fields2cellProcessor(dataset: dataset, fields: listFields, header: header,
					isOptional: readAsText, isWrite: false, isValid: isValid, isEscape: escaped,
					nullAsValue: p.nullAsValue, formatDate: formatDate, formatTime: formatTime, formatDateTime: formatDateTime,
					formatTimestampWithTz: formatTimestampWithTz, uniFormatDateTime: uniFormatDateTime)
			
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
				catch (IOException e) {
					throw e
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
					if (cur == limit || code.directive == Closure.DONE)
						break
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
			throw new ExceptionGETL('Dataset required fileName!')

		csv_ds.currentCsvConnection.validPath()

		WriterParams wp = new WriterParams()
		dataset._driver_params = wp
		wp.formatOutput = csv_ds.isFormatOutput()

		ReadParams p = readParamDataset(csv_ds, params)
		def isAppend = BoolUtils.IsValue(p.params.isAppend)
		def isValid = BoolUtils.IsValue(params.isValid, csv_ds.isConstraintsCheck())
		def escaped = BoolUtils.IsValue(params.escaped, csv_ds.escaped())
		def formatDate = params.formatDate as String
		def formatTime = params.formatTime as String
		def formatDateTime = params.formatDateTime as String
		def formatTimestampWithTz = params.formatDate as String
		def uniFormatDateTime = params.uniFormatDateTime as String

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
				    formatTimestampWithTz: formatTimestampWithTz, uniFormatDateTime: uniFormatDateTime)

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
		wp.opt = (dataset as FileDataset).writtenFiles[(wp.portion?:1) - 1]
		
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
		file.write(wp.headerStr, dataset.codePage(), false)
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
							wp.opt = ds.writtenFiles[wp.portion - 1]
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
		if (dataset.isGzFile()) {
			def input = new GZIPInputStream(new FileInputStream(dataset.fullFileName()))
			reader = new LineNumberReader(new InputStreamReader(input, dataset.codePage()))
		}
		else {
			reader = new LineNumberReader(new File(dataset.fullFileName()).newReader(dataset.codePage()))
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
		
		count - ((dataset.isHeader())?1:0)
	}

	@SuppressWarnings("DuplicatedCode")
	static Long prepareCSVForBulk(CSVDataset target, CSVDataset source, Map<String, String> encodeTable, Closure code) {
		if (!source.existsFile())
			throw new ExceptionGETL("File \"${source.fullFileName()}\" not found!")
		if (!(source.rowDelimiter in ['\n', '\r\n']))
			throw new ExceptionGETL('Allow convert CSV files only standard row delimiter!')

		def autoSchema = source.isAutoSchema()
		def header = source.isHeader()
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
		String sourceFieldDelimiterLast = sourceFieldDelimiter + '\u0001'
		String targetFieldDelimiter = target.fieldDelimiter()
		def source_escaped = source.escaped()
		String sourceQuoteStr = source.quoteStr()
		String targetQuoteStr = target.quoteStr()
		
		String decodeQuote = sourceQuoteStr
		if (targetQuoteStr == sourceQuoteStr) decodeQuote += sourceQuoteStr
		Map convertMap = ['\u0004': "$decodeQuote", '\u0005': '\\\\']
		
		Map<String, String> encodeMap = new HashMap<String, String>()
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
		if (target.isAutoSchema()) target.saveDatasetMetadata()
		if (BoolUtils.IsValue(target.params.deleteOnExit, false)) {
			fw.deleteOnExit()
			if (target.isAutoSchema()) {
				File ws = new File(target.fullFileSchemaName())
				ws.deleteOnExit()
			}
		}
		
		def count = 0L
		
		LineNumberReader reader
		if (source.isGzFile()) {
			def input = new GZIPInputStream(new FileInputStream(source.fullFileName()))
			reader = new LineNumberReader(new InputStreamReader(input, source.codePage()))
		}
		else {
			reader = new LineNumberReader(new InputStreamReader(new FileInputStream(source.fullFileName()), source.codePage()))
		}
		
		try {
			BufferedWriter writer
			if (target.isGzFile()) {
				def output = new GZIPOutputStream(new FileOutputStream(target.fullFileName()))
				writer = new BufferedWriter(new OutputStreamWriter(output, target.codePage()))
			}
			else {
				writer = new File(target.fullFileName()).newWriter(target.codePage(), target.isAppend())
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
		
		count - ((target.isHeader())?1:0)
	}
	
	protected static void convertCSVValues (List<String> values, Map<String, String> convertMap, Closure code) {
		convertMap.each { String oldStr, String newStr ->
			for (Integer i = 0; i < values.size(); i++) { values[i] = values[i].replace(oldStr, newStr) }
		}

		if (code != null) code.call(values)
	}

	static Long decodeBulkCSV (CSVDataset target, CSVDataset source) {
		if (!source.existsFile()) throw new ExceptionGETL("File \"${source.fullFileName()}\" not found")
		
		if (source.field.isEmpty() && source.isAutoSchema()) source.loadDatasetMetadata()
		if (!source.field.isEmpty()) target.setField(source.field)
		
		(target.connection as CSVConnection).validPath()
		
		target.header = source.isHeader()
		target.nullAsValue = source.nullAsValue()
		String targetRowDelimiter = target.rowDelimiter()
		String targetQuoteStr = target.quoteStr()
		
		Map<String, String> encodeMap = new HashMap<String, String>()
		if (target.escaped()) {
			encodeMap."\\" = '\\\\'
			encodeMap."\n" = '\\n' 
			encodeMap."\t" = '\\t'
			encodeMap."$targetQuoteStr" = "\\$targetQuoteStr"
		}
		else {
			encodeMap."$targetQuoteStr" = "$targetQuoteStr$targetQuoteStr"
		}
		encodeMap.putAll(['\u0001': targetRowDelimiter, '\u0002': target.fieldDelimiter(), '\u0003': target.quoteStr()])

		BufferedReader reader
		if (source.isGzFile()) {
			def input = new GZIPInputStream(new FileInputStream(source.fullFileName()))
			reader = new BufferedReader(new InputStreamReader(input, source.codePage()), 64*1024)
		}
		else {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(source.fullFileName()), source.codePage()), 64*1024)
		}
		
		def count = 0L

		try {
			BufferedWriter writer
			if (target.isGzFile()) {
				def output = new GZIPOutputStream(new FileOutputStream(target.fullFileName()))
				writer = new BufferedWriter(new OutputStreamWriter(output, target.codePage()))
			}
			else {
				writer = new File(target.fullFileName()).newWriter(target.codePage(), target.isAppend())
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