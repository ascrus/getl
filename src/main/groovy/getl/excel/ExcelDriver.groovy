package getl.excel

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.Logs
import groovy.transform.CompileStatic
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.DateUtil
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import org.codehaus.groovy.runtime.DateGroovyMethods

/**
 * Excel Driver class
 * @author Dmitry Shaldin
 *
 */
class ExcelDriver extends Driver {
    ExcelDriver () {
        methodParams.register("eachRow", [])
    }

    @Override
    List<Support> supported() {
        [Support.EACHROW, Support.AUTOLOADSCHEMA]
    }

    @Override
    List<Operation> operations() {
        [Operation.DROP]
    }

    @Override
    protected List<Object> retrieveObjects(Map params, Closure filter) { null }

    @Override
    protected List<Field> fields(Dataset dataset) { null }

    @Override
    protected long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
        String path = dataset.connection.params.path
        String fileName = dataset.connection.params.fileName
        String fullPath = FileUtils.ConvertToDefaultOSPath(path + File.separator + fileName)
        boolean warnings = params.showWarnings

        if (dataset.field.isEmpty()) throw new ExceptionGETL("Required fields description with dataset")
        if (!path) throw new ExceptionGETL("Required \"path\" parameter with connection")
        if (!fileName) throw new ExceptionGETL("Required \"fileName\" parameter with connection")
        if (!FileUtils.ExistsFile(fullPath)) throw new ExceptionGETL("File \"${fileName}\" doesn't exists in \"${path}\"")

        Map datasetParams = dataset.params

        def ln = datasetParams.listName ?: 0
        def header = datasetParams.header ?: false

        Number offsetRows = datasetParams.offset?.rows ?: 0
        Number offsetCells = datasetParams.offset?.cells ?: 0

        long countRec = 0

        if (prepareCode != null) prepareCode([])

        Workbook workbook = getWorkbookType(fullPath, dataset.connection.params.extension as String)
        Sheet sheet

        if (ln instanceof String) sheet = workbook.getSheet(ln as String)
        else {
            sheet = workbook.getSheetAt(ln)
            dataset.params.listName = workbook.getSheetName(ln)
            if (warnings) Logs.Warning("Parameter listName not found. Using list name: '${dataset.params.listName}'")
        }

        def limit = datasetParams.limit ?: sheet.lastRowNum

        Iterator rows = sheet.rowIterator()

        if (header) rows.next()
        if (offsetRows != 0) offsetRows.times { rows.next() }
        int additionalRows = limit + offsetRows + (header ? 1 as int : 0 as int)

        rows.each { Row row ->
            if (row.rowNum >= additionalRows) return
            Iterator cells = row.cellIterator()
            Map updater = [:]

            if (offsetCells != 0) offsetCells.times { cells.next() }

            cells.each { Cell cell ->
                updater."${dataset.field.get(cell.columnIndex).name}" = getCellValue(cell, dataset)
            }

            code(updater)
            countRec++
        }

        countRec
    }

    private static getCellValue(final Cell cell, final Dataset dataset) {
        try{
            Field.Type fieldType = dataset.field.get(cell.columnIndex).type

            switch (fieldType) {
                case Field.Type.BIGINT:
                    if (cell.cellType == Cell.CELL_TYPE_STRING) (cell.stringCellValue as BigInteger)
                    else (cell.numericCellValue as BigInteger)
                    break
                case Field.Type.BOOLEAN:
                    cell.booleanCellValue
                    break
                case Field.Type.DATE:
                    cell.dateCellValue
                    break
                case Field.Type.DATETIME:
                    cell.dateCellValue
                    break
                case Field.Type.DOUBLE:
                    if (cell.cellType == Cell.CELL_TYPE_STRING) (cell.stringCellValue as Double)
                    else (cell.numericCellValue as Double)
                    break
                case Field.Type.INTEGER:
                    if (cell.cellType == Cell.CELL_TYPE_STRING) (cell.stringCellValue as Integer)
                    else (cell.numericCellValue as Integer)
                    break
                case Field.Type.NUMERIC:
                    if (cell.cellType == Cell.CELL_TYPE_STRING) (cell.stringCellValue as BigDecimal)
                    else (cell.numericCellValue as BigDecimal)
                    break
                case Field.Type.STRING:
                    cell.stringCellValue
                    break
                default:
                    throw new ExceptionGETL('Default field type not supported.')
            }
        } catch (e) {
            Logs.Warning("Error in ${cell.rowIndex}")
            Logs.Exception(e)
        }
    }

    private static getWorkbookType(final String fileName, final String extension) {
        def ext = extension ?: FileUtils.FileExtension(fileName)
        if (!(new File(fileName).exists())) throw new ExceptionGETL("File '$fileName' doesn't exists")
        if (!(ext in ['xls', 'xlsx'])) throw new ExceptionGETL("'$extension' is not available. Please, use 'xls' or 'xlsx'.")

        switch (ext) {
            case {fileName.endsWith(ext) && ext == 'xlsx'}:
                new XSSFWorkbook(new FileInputStream(fileName))
                break
            case {fileName.endsWith(ext) && ext == 'xls'}:
                new HSSFWorkbook(new FileInputStream(fileName))
                break
            default:
                throw new ExceptionGETL("Something went wrong")
        }
    }

    @Override
    protected void doneWrite (Dataset dataset) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    protected void closeWrite(Dataset dataset) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    protected void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    protected void openWrite(Dataset dataset, Map params, Closure prepareCode) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    protected void write(Dataset dataset, Map row) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    protected long executeCommand (String command, Map params) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    public long getSequence(String sequenceName) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    protected void clearDataset(Dataset dataset, Map params) {
        throw new ExceptionGETL("Not supported")

    }

    @Override
    protected void createDataset(Dataset dataset, Map params) {
        throw new ExceptionGETL("Not supported")

    }

    @Override
    protected void startTran() {
        throw new ExceptionGETL("Not supported")

    }

    @Override
    protected void commitTran() {
        throw new ExceptionGETL("Not supported")

    }

    @Override
    protected void rollbackTran() {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    protected void connect () {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    protected void disconnect () {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    protected boolean isConnect () {
        throw new ExceptionGETL("Not supported")
    }
}
