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

package getl.excel

import getl.data.*
import getl.driver.Driver
import getl.csv.CSVDataset
import getl.exception.ExceptionGETL
import getl.utils.*
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.openxml4j.opc.OPCPackage
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook

/**
 * Excel Driver class
 * @author Dmitry Shaldin
 *
 */
@groovy.transform.InheritConstructors
class ExcelDriver extends Driver {
    ExcelDriver () {
        methodParams.register("eachRow", ["header", "offset", "limit", "showWarnings"])
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
    public List<Object> retrieveObjects(Map params, Closure filter) { throw new ExceptionGETL("Not supported") }

    @Override
    public
    List<Field> fields(Dataset dataset) { throw new ExceptionGETL("Not supported") }

    @Override
    public
    long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
        String path = dataset.connection.params.path
        String fileName = dataset.connection.params.fileName
        String fullPath = FileUtils.ConvertToDefaultOSPath(path + File.separator + fileName)
        boolean warnings = params.showWarnings

        if (!path) throw new ExceptionGETL("Required \"path\" parameter with connection")
        if (!fileName) throw new ExceptionGETL("Required \"fileName\" parameter with connection")
        if (!FileUtils.ExistsFile(fullPath)) throw new ExceptionGETL("File \"${fileName}\" doesn't exists in \"${path}\"")

        Map datasetParams = dataset.params

        def ln = datasetParams.listName?:0
        def header = BoolUtils.IsValue([params.header, datasetParams.header], false)
        if (dataset.field.isEmpty() && !header) throw new ExceptionGETL("Required fields description with dataset")
		
		def offset = (params.offset?:datasetParams.offset) as Map

        Number offsetRows = offset?.rows?:0
        Number offsetCells = offset?.cells?:0

        long countRec = 0

        if (prepareCode != null) prepareCode([])

        Workbook workbook = getWorkbookType(fullPath)
        Sheet sheet

        if (ln instanceof String) sheet = workbook.getSheet(ln as String)
        else {
            sheet = workbook.getSheetAt(ln)
            dataset.params.listName = workbook.getSheetName(ln)
            if (warnings) Logs.Warning("Parameter listName not found. Using list name: '${dataset.params.listName}'")
        }

        def limit = ListUtils.NotNullValue([params.limit, datasetParams.limit, sheet.lastRowNum])

        Iterator rows = sheet.rowIterator()

        if (offsetRows != 0) 1..offsetRows.each { rows.next() }
        int additionalRows = limit + offsetRows + (header?(1 as int):(0 as int))

		def excelFields = [] as List<String>
		def requiedParseField = dataset.field.isEmpty()
		if (header) {
			Row row = rows.next()
			if (requiedParseField) {
				Iterator cells = row.cellIterator()
				def colNum = 0
				cells.each { Cell cell ->
					colNum++
					if (offsetCells >= colNum) return
					if (cell.cellType != cell.CELL_TYPE_STRING) throw new ExceptionGETL("Not string field name in header by $colNum col")
					def fieldName = cell.stringCellValue?.trim()
					if (fieldName == null || fieldName == '') throw new ExceptionGETL("Required field name in header by $colNum col")
					excelFields << cell.stringCellValue
				}

				if (excelFields.isEmpty()) throw new ExceptionGETL("Required fields description with dataset")
			}
		}

		def rowNum = 0
        rows.each { Row row ->
			rowNum++
            if (row.rowNum >= additionalRows) return

            Iterator cells = row.cellIterator()
			def colNum = 0
			if (offsetCells != 0) 1..offsetCells.each {
				colNum++
				cells.next()
			}

			if (requiedParseField) {
				def types = [] as List<String>
				cells.each { Cell cell ->
					colNum++
					switch (cell.cellType) {
						case cell.CELL_TYPE_STRING:
							types << 'STRING'
							break
						case cell.CELL_TYPE_BOOLEAN:
							types << 'BOOLEAN'
							break
						case cell.CELL_TYPE_NUMERIC:
							types << 'NUMERIC'
							break
						default:
							throw new ExceptionGETL("Unknown type cell from $rowNum row $colNum col")
					}
				}
				if (types.size() != excelFields.size()) throw new ExceptionGETL("The number of fields in the header and in the next data line does not match")
				for (int i = 0; i < excelFields.size(); i++) {
					dataset.field << new Field(name: excelFields[i], type: types[i])
				}
				requiedParseField = false

				cells = row.cellIterator()
				colNum = 0
				if (offsetCells != 0) 1..offsetCells.each {
					colNum++
					cells.next()
				}
			}

            LinkedHashMap<String, Object> updater = [:]
            cells.each { Cell cell ->
				colNum++
                int columnIndex = cell.columnIndex - offsetCells
                if (columnIndex >= dataset.field.size()) return
                updater."${dataset.field.get(columnIndex).name}" = getCellValue(cell, dataset, columnIndex)
            }

            code(updater)
            countRec++
        }

        countRec
    }

    private static def getCellValue(final Cell cell, final Dataset dataset, final int columnIndex) {
		def res
        try{
            Field.Type fieldType = dataset.field.get(columnIndex).type


            switch (fieldType) {
                case Field.Type.BIGINT:
                    if (cell.cellType == Cell.CELL_TYPE_STRING)
						res = (cell.stringCellValue.toBigInteger())
                    else
						res = cell.numericCellValue.toBigInteger()

                    break
                case Field.Type.BOOLEAN:
                    res = cell.booleanCellValue

                    break
                case Field.Type.DATE:
                    res = cell.dateCellValue

                    break
                case Field.Type.DATETIME:
                    res = cell.dateCellValue

                    break
                case Field.Type.DOUBLE:
                    if (cell.cellType == Cell.CELL_TYPE_STRING)
						res = (cell.stringCellValue.toDouble())
                    else
						res = cell.numericCellValue

                    break
                case Field.Type.INTEGER:
                    if (cell.cellType == Cell.CELL_TYPE_STRING)
						res = (cell.stringCellValue.toInteger())
                    else
						res = cell.numericCellValue.toInteger()

                    break
                case Field.Type.NUMERIC:
                    if (cell.cellType == Cell.CELL_TYPE_STRING)
						res = (cell.stringCellValue.toBigDecimal())
                    else
						res = cell.numericCellValue.toBigDecimal()
                    break
                case Field.Type.STRING:
                    res = cell.stringCellValue
                    break
                default:
                    throw new ExceptionGETL('Default field type not supported.')
            }
        } catch (e) {
            Logs.Warning("Error in ${cell.rowIndex} row")
            Logs.Exception(e)
			throw e
        }

		return res
    }

    private static getWorkbookType(final String fileName) {
        def ext = FileUtils.FileExtension(fileName)
        if (!(new File(fileName).exists())) throw new ExceptionGETL("File '$fileName' doesn't exists")
        if (!(ext in ['xls', 'xlsx'])) throw new ExceptionGETL("'$ext' is not available. Please, use 'xls' or 'xlsx'.")

        switch (ext) {
            case {fileName.endsWith(ext) && ext == 'xlsx'}:
                new XSSFWorkbook(OPCPackage.open(new File(fileName)))
                break
            case {fileName.endsWith(ext) && ext == 'xls'}:
                new HSSFWorkbook(new NPOIFSFileSystem(new File(fileName)))
                break
            default:
                throw new ExceptionGETL("Something went wrong")
        }
    }

    @Override
    public
    void doneWrite (Dataset dataset) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    public
    void closeWrite(Dataset dataset) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    public
    void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    public
    void openWrite(Dataset dataset, Map params, Closure prepareCode) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    public
    void write(Dataset dataset, Map row) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    public
    long executeCommand (String command, Map params) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    public long getSequence(String sequenceName) {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    public
    void clearDataset(Dataset dataset, Map params) {
        throw new ExceptionGETL("Not supported")

    }

    @Override
    public
    void createDataset(Dataset dataset, Map params) {
        throw new ExceptionGETL("Not supported")

    }

    @Override
    public
    void startTran() {
        throw new ExceptionGETL("Not supported")

    }

    @Override
    public
    void commitTran() {
        throw new ExceptionGETL("Not supported")

    }

    @Override
    public
    void rollbackTran() {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    public
    void connect () {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    public
    void disconnect () {
        throw new ExceptionGETL("Not supported")
    }

    @Override
    public boolean isConnected() {
        throw new ExceptionGETL("Not supported")
    }
}