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
import java.text.DecimalFormatSymbols

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
        methodParams.register('eachRow', ['limit', 'showWarnings', 'filter', 'prepareFilter'])
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

        def decimalSeparator = dataset.decimalSeparator()
        def dfs = new DecimalFormatSymbols()
        dfs.setDecimalSeparator(decimalSeparator.chars[0])
        def df = new DecimalFormat('#,##0.0#', dfs)

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

            def limit = ListUtils.NotNullValue([params.limit, dataset.limit]) as Integer

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

                def updater = [:] as LinkedHashMap<String, Object>

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
                        def fieldValue = getCellValue(cell, dataset, field, df)
                        updater.put(fieldName.toLowerCase(), fieldValue)
                    }
                    catch (Exception e) {
                        connection.logger.severe("Error reading field \"$fieldName\" of column $colNum of line ${row.rowNum + 1} in $dataset: ${e.message}")
                        try {
                            def m = [:] as Map<String, Object>
                            row.cellMap.each { column, value ->
                                m.put("col $column".toString(), value?.stringCellValue)
                            }
                            connection.logger.dump(e, 'excel', dataset.fullFileName(), "Error reading field \"$fieldName\" of column $colNum of line ${row.rowNum + 1} in row:\n${MapUtils.ToJson(m)}")
                        }
                        catch (Exception i) {
                            connection.logger.severe("Failed to get column values for error Excel line ${row.rowNum + 1}: ${i.message}")
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
                connection.logger.severe("Can not close workbook $dataset, error: ${e.message}")
                throw e
            }
        }

        return countRec
    }

    @SuppressWarnings('unused')
    @CompileStatic
    static private Object getCellValue(Cell cell, FileDataset dataset, Field field, DecimalFormat df) {
        if (cell.cellType == CellType.BLANK) return null

		def res = null
        def fieldType = field.type

        switch (fieldType) {
            case Field.Type.BIGINT:
                if (cell.cellType == CellType.STRING)
                    res = (cell.stringCellValue.toBigInteger())
                else
                    res = cell.numericCellValue.toBigInteger()

                break
            case Field.Type.BOOLEAN:
                if (cell.cellType == CellType.STRING) {
                    def format = field.format?:dataset.formatBoolean()?:'true|false'
                    def val = format.toUpperCase().split("[|]")
                    def str = cell.stringCellValue
                    if (str != null)
                        res = (str.toUpperCase() == val[0])
                }
                else if (cell.cellType == CellType.NUMERIC) {
                    res = cell.numericCellValue.toInteger() == 1
                }
                else
                    res = cell.booleanCellValue

                break
            case Field.Type.DATE:
                res = new java.sql.Date(cell.dateCellValue.time)

                break
            case Field.Type.DATETIME: case Field.Type.TIMESTAMP_WITH_TIMEZONE:
                res = new Timestamp(cell.dateCellValue.time)

                break
            case Field.Type.TIME:
                res = new Time(cell.dateCellValue.time)

                break
            case Field.Type.DOUBLE:
                if (cell.cellType == CellType.STRING)
                    res = df.parse(cell.stringCellValue).toDouble()
                    //res = (cell.stringCellValue.toDouble())
                else
                    res = cell.numericCellValue

                break
            case Field.Type.INTEGER:
                if (cell.cellType == CellType.STRING)
                    res = (cell.stringCellValue.toInteger())
                else
                    res = cell.numericCellValue.toInteger()

                break
            case Field.Type.NUMERIC:
                if (cell.cellType == CellType.STRING)
                    res = df.parse(cell.stringCellValue).toBigDecimal()
                    //res = (cell.stringCellValue.toBigDecimal())
                else
                    res = cell.numericCellValue.toBigDecimal()

                break
            case Field.Type.STRING:
                res = cell.stringCellValue

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
    void doneWrite (Dataset dataset) {
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
    Long executeCommand (String command, Map params) {
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
    void startTran() {
        throw new ExceptionGETL('Not support this features!')

    }

    @Override
    void commitTran() {
        throw new ExceptionGETL('Not support this features!')

    }

    @Override
    void rollbackTran() {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void connect () {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    void disconnect () {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    Boolean isConnected() { false }
}