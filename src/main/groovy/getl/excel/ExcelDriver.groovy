/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) EasyData Company LTD

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
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.openxml4j.opc.OPCPackage
import org.apache.poi.poifs.filesystem.POIFSFileSystem
import org.apache.poi.ss.usermodel.CellType
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
@InheritConstructors
class ExcelDriver extends Driver {
    ExcelDriver () {
        methodParams.register("eachRow", ["header", "offset", "limit", "showWarnings"])


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
    List<Object> retrieveObjects(Map params, Closure<Boolean> filter) { throw new ExceptionGETL('Not support this features!') }

    @Override
    List<Field> fields(Dataset dataset) {
        if (!BoolUtils.IsValue([(dataset as ExcelDataset).header,
                                (dataset.connection as ExcelConnection).header, true])) {
            throw new ExceptionGETL('Not support this features with no field header!')
        }
        dataset.rows(limit: 1)
        return dataset.field
    }

    /** Full file name */
    static String fullFileNameDataset(ExcelDataset dataset) {
        if (dataset.connection == null) throw new ExceptionGETL("Required connection for excel dataset!")
        String path = (dataset.connection as ExcelConnection).path
        String fileName = (dataset.connection as ExcelConnection).fileName
        (path != null)?
                FileUtils.ConvertToDefaultOSPath(path + File.separator + fileName):
                FileUtils.ConvertToDefaultOSPath(fileName)
    }

    @Override
    @CompileStatic
    long eachRow(Dataset source, Map params, Closure prepareCode, Closure code) {
        ExcelDataset dataset = source as ExcelDataset
        String fileName = dataset.currentExcelConnection.fileName
        String fullPath = dataset.fullFileName()
        boolean warnings = BoolUtils.IsValue([params.showWarnings, dataset.showWarnings,
                                              dataset.currentExcelConnection.showWarnings, false])

        if (!fileName) throw new ExceptionGETL("Required \"fileName\" parameter with connection")
        if (!FileUtils.ExistsFile(fullPath)) throw new ExceptionGETL("File \"${fullPath}\" doesn't exists!")

        def ln = dataset.listName?:0
        def header = BoolUtils.IsValue([params.header, dataset.header,
                                        (dataset.connection as ExcelConnection).header], true)
        if (dataset.field.isEmpty() && !header) throw new ExceptionGETL("Required fields description with dataset")
		
		def offset = (params.offset?:dataset.offset) as Map

        Number offsetRows = (offset?.rows as Number)?:0
        Number offsetCells = (offset?.cells as Number)?:0

        long countRec = 0

        if (prepareCode != null) prepareCode([])

        def workbook = getWorkbookType(fullPath) as Workbook

        try {
            Sheet sheet

            if (dataset.listName != null) {
                if (workbook.getSheetIndex(dataset.listName) == -1)
                    throw new ExceptionGETL("List \"${dataset.listName}\" not found!")

                sheet = workbook.getSheet(dataset.listName)
                dataset.listNumber = workbook.getSheetIndex(dataset.listName)
            }
            else {
                def num = dataset.listNumber?:0
                if (workbook.getNumberOfSheets() < num)
                    throw new ExceptionGETL("List №$num not found!")

                sheet = workbook.getSheetAt(num) as Sheet
                dataset.listName = workbook.getSheetName(num)
            }
            if (sheet == null)
                throw new ExceptionGETL("Specified list not found!")

            def limit = ListUtils.NotNullValue([params.limit, dataset.limit, sheet.lastRowNum]) as Integer

            Iterator rows = sheet.rowIterator()

            if (offsetRows != 0) 1..offsetRows.each { rows.next() }
            int additionalRows = limit + offsetRows + (header ? (1 as int) : (0 as int))

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
                        if (cell.cellType != CellType.STRING) throw new ExceptionGETL("Not string field name in header by $colNum col")
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
                if (row.rowNum >= additionalRows) {
//                    directive = Closure.DONE
                    return
                }

                Iterator cells = row.cellIterator()
                def colNum = 0
                if (offsetCells != 0) 1..offsetCells.each {
                    colNum++
                    cells.next()
                }

                if (requiedParseField) {
                    def types = [] as List<Field.Type>
                    cells.each { Cell cell ->
                        colNum++
                        switch (cell.cellType) {
                            case CellType.STRING:
                                types << Field.stringFieldType
                                break
                            case CellType.BOOLEAN:
                                types << Field.booleanFieldType
                                break
                            case CellType.NUMERIC:
                                types << Field.numericFieldType
                                break
                            default:
                                throw new ExceptionGETL("Unknown type cell from $rowNum row $colNum col")
                        }
                    }
                    if (types.size() != excelFields.size())
                        throw new ExceptionGETL("The number of fields in the header and in the next data line does not match")

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

                def updater = [:] as LinkedHashMap<String, Object>
                cells.each { Cell cell ->
                    colNum++
                    int columnIndex = cell.columnIndex - offsetCells
                    if (columnIndex >= dataset.field.size()) return
                    updater.put("${dataset.field.get(columnIndex).name}".toString(), getCellValue(cell, dataset, columnIndex))
                }

                code.call(updater)
                countRec++
            }
        }
        finally {
            workbook.close()
        }

        return countRec
    }

    private static def getCellValue(final Cell cell, final Dataset dataset, final int columnIndex) {
		def res
        try{
            Field.Type fieldType = dataset.field.get(columnIndex).type


            switch (fieldType) {
                case Field.Type.BIGINT:
                    if (cell.cellType == CellType.STRING)
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
                    if (cell.cellType == CellType.STRING)
						res = (cell.stringCellValue.toDouble())
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
                new HSSFWorkbook(new POIFSFileSystem(new File(fileName)))
                break
            default:
                throw new ExceptionGETL("Something went wrong")
        }
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
    long executeCommand (String command, Map params) {
        throw new ExceptionGETL('Not support this features!')
    }

    @Override
    long getSequence(String sequenceName) {
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
    boolean isConnected() {
        throw new ExceptionGETL('Not support this features!')
    }
}