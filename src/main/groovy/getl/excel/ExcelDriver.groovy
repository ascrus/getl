package getl.excel

/*import com.monitorjbl.xlsx.*
import com.monitorjbl.xlsx.impl.**/
import com.github.pjfanning.xlsx.*
import com.github.pjfanning.xlsx.impl.*
import getl.data.*
import getl.driver.Driver
import getl.csv.CSVDataset
import getl.driver.FileDriver
import getl.exception.ExceptionGETL
import getl.utils.*
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.poifs.filesystem.POIFSFileSystem
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import java.sql.Time
import java.sql.Timestamp
import java.text.DecimalFormat
import java.time.format.DateTimeFormatter

/**
 * Excel Driver class
 * @author Dmitry Shaldin and Alexsey Konstantinov
 *
 */
@InheritConstructors
class ExcelDriver extends FileDriver {
    @Override
    protected void registerParameters() {
        super.registerParameters()
        methodParams.register('eachRow', ['limit', 'showWarnings', 'filter', 'prepareFilter', 'decimalSeparator', 'groupSeparator',
                                          'formatDate', 'formatDateTime', 'formatTime', 'formatBoolean', 'uniFormatDateTime'])
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        [Driver.Support.EACHROW, Driver.Support.AUTOLOADSCHEMA]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        [Driver.Operation.DROP, Driver.Operation.RETRIEVEFIELDS]
    }

    @Override
    List<Object> retrieveObjects(Map params, Closure<Boolean> filter) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    List<Field> fields(Dataset dataset) {
        if (!BoolUtils.IsValue([(dataset as ExcelDataset).header,
                                (dataset.connection as ExcelConnection).header, true])) {
            throw new ExceptionGETL('Not support this features with no field header!')
        }
        dataset.rows(limit: 1)
        return dataset.field
    }

    @Override
    @CompileStatic
    Long eachRow(Dataset source, Map params, Closure prepareCode, Closure code) {
        ExcelDataset dataset = source as ExcelDataset
        String fileName = dataset.fileName()
        String fullPath = FileUtils.ResourceFileName(dataset.fullFileName(), dataset.dslCreator)

        if (!fileName)
            throw new ExceptionGETL('Required "fileName" parameter with Excel dataset!')

        dataset.currentExcelConnection.validPath()

        if (!FileUtils.ExistsFile(fullPath))
            throw new ExceptionGETL("Excel file \"${fullPath}\" doesn't exists!")

        def header = dataset.header()

        if (dataset.field.isEmpty())
            throw new ExceptionGETL("Required fields description with $dataset!")
		
        def offsetRows = (dataset.offsetRows?:0) + ((header)?1:0)
        def offsetCells = (dataset.offsetCells?:0).shortValue()

        def prepareFilter = (params.prepareFilter as Closure)?:dataset.onPrepareFilter
        def filter = (params.filter as Closure)?:dataset.onFilter

        def locale = ListUtils.NotNullValue([params.locale, dataset.locale()]) as String
        def decimalSeparator = ListUtils.NotNullValue([params.decimalSeparator, dataset.decimalSeparator()]) as String
        def groupSeparator = ListUtils.NotNullValue([params.groupSeparator, dataset.groupSeparator()]) as String
        def dfs = NumericUtils.BuildDecimalFormatSymbols((decimalSeparator != null)?decimalSeparator.chars[0]:null,
                (groupSeparator != null)?groupSeparator.chars[0]:null, locale)
        def dfDecimal = new DecimalFormat('#,##0.#', dfs)

        def formatBoolean = ListUtils.NotNullValue([params.formatBoolean, dataset.formatBoolean(), 'true|false']) as String

        def uniFormatDateTime = ListUtils.NotNullValue([params.uniFormatDateTime, dataset.uniFormatDateTime()]) as String

        def formatDate = ListUtils.NotNullValue([params.formatDate, uniFormatDateTime, dataset.formatDate(), DateUtils.defaultDateMask]) as String
        def dfDate = DateUtils.BuildDateFormatter(formatDate, null, locale)

        def formatDateTime = ListUtils.NotNullValue([params.formatDate, uniFormatDateTime, dataset.formatDateTime(), DateUtils.defaultDateTimeMask]) as String
        def dfDateTime = DateUtils.BuildDateTimeFormatter(formatDateTime, null, locale)

        def formatTime = ListUtils.NotNullValue([params.formatTime, uniFormatDateTime, dataset.formatTime(), DateUtils.defaultTimeMask]) as String
        def dfTime = DateUtils.BuildTimeFormatter(formatTime, null, locale)

        def countRec = 0L
        if (prepareCode != null)
            prepareCode([])

        def workbook = getWorkbookType(fullPath, dataset.rowCacheSize(), dataset.bufferSize())
        Sheet sheet
        try {
            if (dataset.listName != null) {
                if (workbook.getSheetIndex(dataset.listName) == -1)
                    throw new ExceptionGETL("List \"${dataset.listName}\" not found in $dataset!")

                sheet = workbook.getSheet(dataset.listName)
                dataset.listNumber = workbook.getSheetIndex(dataset.listName)
            }
            else {
                def num = dataset.listNumber?:0
                if (workbook.getNumberOfSheets() < num)
                    throw new ExceptionGETL("List number $num not found in $dataset!")

                sheet = workbook.getSheetAt(num) as Sheet
                dataset.listName = workbook.getSheetName(num)
            }

            if (sheet == null)
                throw new ExceptionGETL("Specified workbook list not found in $dataset!")

            def limit = ListUtils.NotNullValue([ConvertUtils.Object2Int(params.limit), dataset.limit]) as Integer

            Iterator<Row> rows = sheet.rowIterator()
            if (offsetRows > 0) {
                def curRow = 0
                while (rows.hasNext() && curRow < offsetRows) {
                    def row = rows.next() as Row
                    curRow = row.rowNum + 1
                }
            }

            def fieldCount = dataset.field.size()
            while (rows.hasNext()) {
                def row = rows.next() as StreamingRow

                if (row.firstCellNum == -1.shortValue())
                    continue

                if (prepareFilter != null && !prepareFilter.call(row))
                    continue

                def updater = new LinkedHashMap<String, Object>()

                def cells = row.cellIterator()
                while (cells.hasNext()) {
                    def cell = cells.next()

                    def colNum = cell.columnIndex + 1
                    if (colNum <= offsetCells)
                        continue

                    def fieldNum = colNum - offsetCells - 1
                    if (fieldNum >= fieldCount)
                        continue

                    def field = dataset.field.get(fieldNum)
                    def fieldName = field.name
                    try {
                        def fieldValue = getCellValue(cell, dataset, field, dfDate, dfTime, dfDateTime, formatBoolean, dfDecimal)
                        updater.put(fieldName.toLowerCase(), fieldValue)
                    }
                    catch (Exception e) {
                        connection.logger.severe("Error reading field \"$fieldName\" of column $colNum of line ${row.rowNum + 1} in $dataset", e)
                        try {
                            def m = new HashMap<String, Object>()
                            row.cellMap.each { column, value ->
                                m.put("col $column".toString(), value?.stringCellValue)
                            }
                            connection.logger.dump(e, 'excel', dataset.fullFileName(), "Error reading field \"$fieldName\" of column $colNum of line ${row.rowNum + 1} in row:\n${MapUtils.ToJson(m)}")
                        }
                        catch (Exception es) {
                            connection.logger.severe("Failed to get column values for error Excel line ${row.rowNum + 1}", es)
                        }
                        finally {
                            throw e
                        }
                    }
                }

                if (filter == null || filter.call(updater)) {
                    code.call(updater)
                    countRec++
                    if ((limit != null && countRec >= limit) || code.directive == Closure.DONE)
                        break
                }
            }
        }
        finally {
            try {
                workbook.close()
            }
            catch (Exception e) {
                connection.logger.severe("Can not close workbook $dataset", e)
                throw e
            }
        }

        return countRec
    }

    @SuppressWarnings('unused')
    @CompileStatic
    static private Object getCellValue(Cell cell, FileDataset dataset, Field field, DateTimeFormatter formatDate, DateTimeFormatter formatTime,
                                       DateTimeFormatter formatDateTime, String formatBoolean, DecimalFormat df) {
        if (cell.cellType == CellType.BLANK) return null

		def res = null
        def fieldType = field.type

        switch (fieldType) {
            case Field.Type.BIGINT:
                if (cell.cellType in [CellType.NUMERIC, CellType.FORMULA])
                    res = cell.numericCellValue.toBigInteger()
                else if (cell.cellType == CellType.STRING)
                    res = (cell.stringCellValue.toBigInteger())
                else
                    throw new ExceptionGETL("Cell type ${cell.cellType} not compatible with bigint!")

                break
            case Field.Type.BOOLEAN:
                if (cell.cellType in [CellType.BOOLEAN, CellType.FORMULA])
                    res = cell.booleanCellValue
                else if (cell.cellType == CellType.NUMERIC) {
                    def val = cell.numericCellValue.toInteger()
                    if (val == 1)
                        res = true
                    else if (val == 0)
                        res = false
                    else
                        res = null
                }
                else if (cell.cellType == CellType.STRING) {
                    def format = field.format?.toLowerCase()?:formatBoolean?:'true|false'
                    def val = format.split("[|]")
                    def str = cell.stringCellValue
                    if (str != null) {
                        if (str.toLowerCase() == val[0])
                            res = true
                        else if (str.toLowerCase() == val[1])
                            res = false
                        else
                            res = null
                    }
                }
                else
                    throw new ExceptionGETL("Cell type ${cell.cellType} not compatible with boolean!")

                break
            case Field.Type.DATE:
                if (cell.cellType in [CellType.NUMERIC, CellType.FORMULA])
                    res = new java.sql.Date(cell.dateCellValue.time)
                else if (cell.cellType == CellType.STRING)
                    res = DateUtils.ParseSQLDate(formatDate, cell.stringCellValue, false)
                else
                    throw new ExceptionGETL("Cell type ${cell.cellType} not compatible with date!")

                break
            case Field.Type.DATETIME:
                if (cell.cellType in [CellType.NUMERIC, CellType.FORMULA])
                    res = new Timestamp(cell.dateCellValue.time)
                else if (cell.cellType == CellType.STRING)
                    res = DateUtils.ParseSQLDate(formatDateTime, cell.stringCellValue, false)
                else
                    throw new ExceptionGETL("Cell type ${cell.cellType} not compatible with datetime!")

                break
            case Field.Type.TIME:
                if (cell.cellType in [CellType.NUMERIC, CellType.FORMULA])
                    res = new Time(cell.dateCellValue.time)
                else if (cell.cellType == CellType.STRING)
                    res = DateUtils.ParseSQLDate(formatTime, cell.stringCellValue, false)
                else
                    throw new ExceptionGETL("Cell type ${cell.cellType} not compatible with time!")

                break
            case Field.Type.DOUBLE:
                if (cell.cellType in [CellType.NUMERIC, CellType.FORMULA])
                    res = cell.numericCellValue
                else if (cell.cellType == CellType.STRING)
                    res = df.parse(cell.stringCellValue).toDouble()
                else
                    throw new ExceptionGETL("Cell type ${cell.cellType} not compatible with double!")

                break
            case Field.Type.INTEGER:
                if (cell.cellType in [CellType.NUMERIC, CellType.FORMULA])
                    res = cell.numericCellValue.toInteger()
                else if (cell.cellType == CellType.STRING)
                    res = (cell.stringCellValue.toInteger())
                else
                    throw new ExceptionGETL("Cell type ${cell.cellType} not compatible with integer!")

                break
            case Field.Type.NUMERIC:
                if (cell.cellType in [CellType.NUMERIC, CellType.FORMULA])
                    res = cell.numericCellValue.toBigDecimal()
                else if (cell.cellType == CellType.STRING)
                    res = df.parse(cell.stringCellValue).toBigDecimal()
                else
                    throw new ExceptionGETL("Cell type ${cell.cellType} not compatible with numeric!")

                break
            case Field.Type.STRING:
                switch (cell.cellType) {
                    case CellType.STRING: case CellType.FORMULA:
                        res = cell.stringCellValue
                        break
                    case CellType.NUMERIC:
                        res = cell.numericCellValue.toString()
                        break
                    case CellType.BOOLEAN:
                        res = cell.booleanCellValue.toString()
                        break
                    default:
                        throw new ExceptionGETL("Cell type ${cell.cellType} not compatible with string!")
                }

                break
            default:
                throw new ExceptionGETL('Field type $fieldType not supported!')
        }

		return res
    }

    /** Create work book manager */
    static protected Workbook getWorkbookType(String fileName, Integer rowCacheSize = null, Integer bufferSize = null) {
        def ext = FileUtils.FileExtension(fileName).toLowerCase()
        def file = new File(fileName)
        if (!file.exists())
            throw new ExceptionGETL("File \"$fileName\" doesn't exists!")

        if (!(ext in ['xls', 'xlsx']))
            throw new ExceptionGETL("\"$ext\" extension for file \"$fileName\" is not available. Please, use 'xls' or 'xlsx' types!")

        def is = new FileInputStream(file)

        Workbook res
        switch (ext) {
            case {fileName.endsWith(ext) && ext == 'xlsx'}:
                res = StreamingReader.builder()
                        .rowCacheSize(rowCacheSize?:100)
                        .bufferSize(bufferSize?:4096)
                        .open(is)
                //res = new XSSFWorkbook(OPCPackage.open(is))
                break
            case {fileName.endsWith(ext) && ext == 'xls'}:
                res = new HSSFWorkbook(new POIFSFileSystem(is), false)
                break
            default:
                throw new ExceptionGETL("Unknown extension of Excel file \"$fileName\"!")
        }

        return res
    }

    @Override
    void doneWrite(Dataset dataset) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void closeWrite(Dataset dataset) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void openWrite(Dataset dataset, Map params, Closure prepareCode) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void write(Dataset dataset, Map row) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    Long executeCommand(String command, Map params) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    Long getSequence(String sequenceName) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void clearDataset(Dataset dataset, Map params) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void createDataset(Dataset dataset, Map params) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void startTran(Boolean useSqlOperator = false) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void commitTran(Boolean useSqlOperator = false) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void rollbackTran(Boolean useSqlOperator = false) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void connect() {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void disconnect() {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    Boolean isConnected() { false }
}