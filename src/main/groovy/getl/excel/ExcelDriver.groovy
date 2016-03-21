package getl.excel

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.DateUtil
import org.apache.poi.xssf.usermodel.XSSFCell
import org.apache.poi.xssf.usermodel.XSSFRow
import org.apache.poi.xssf.usermodel.XSSFSheet
import org.apache.poi.xssf.usermodel.XSSFWorkbook

/**
 * Excel Driver class
 * @author Dmitry Shaldin
 *
 */
class ExcelDriver extends Driver {
    ExcelDriver () {
        methodParams.register("eachRow", ["fields", "filter"])
    }

    @Override
    List<Driver.Support> supported() {
        [Driver.Support.EACHROW, Driver.Support.AUTOLOADSCHEMA]
    }

    @Override
    List<Driver.Operation> operations() {
        [Driver.Operation.DROP]
    }

    @Override
    protected List<Object> retrieveObjects(Map params, Closure filter) {
        return null
    }

    @Override
    protected List<Field> fields(Dataset dataset) {
        return null
    }

    @Override
    protected long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
        if (dataset.field.isEmpty()) throw new ExceptionGETL("Required fields description with dataset")
        if (dataset.connection.params.path == null) throw new ExceptionGETL("Required \"path\" parameter with connection")
        if (dataset.connection.params.fileName == null) throw new ExceptionGETL("Required \"fileName\" parameter with connection")
        if (dataset.params.listName == null) throw new ExceptionGETL("Required \"listName\" parameter with dataset")

        long countRec = 0
        String fn = "${dataset.connection.params.path}/${dataset.connection.params.fileName}"
        def ln = dataset.params.listName ?: 0

        XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream(fn))
        def sheet = workbook.getSheet(ln)
        Iterator rows = sheet.rowIterator()

        rows.each { XSSFRow row ->
            Iterator cells = row.cellIterator()

            cells.each { XSSFCell cell ->
                Map updater = [:]
                def value

                switch(cell.cellType) {
                    case Cell.CELL_TYPE_NUMERIC:
                        if(DateUtil.isCellDateFormatted(cell)) {
                            value = cell.dateCellValue
                        } else {
                            value = !cell.numericCellValue ? 0 : cell.numericCellValue.toBigDecimal()
                        }
                        break
                    case Cell.CELL_TYPE_BOOLEAN:
                        value = cell.booleanCellValue
                        break
                    default:
                        value = cell.stringCellValue
                        break
                }

                updater."${dataset.field.get(cell.columnIndex).name}" = value

                countRec++
                code(updater)
            }
        }

        countRec
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
