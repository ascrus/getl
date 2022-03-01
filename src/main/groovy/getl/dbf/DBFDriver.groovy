package getl.dbf

import com.linuxense.javadbf.DBFDataType
import com.linuxense.javadbf.DBFField
import com.linuxense.javadbf.DBFReader
import com.linuxense.javadbf.DBFRow
import com.linuxense.javadbf.DBFUtils
import getl.data.Dataset
import getl.data.Field
import getl.dbf.proc.ReadBigint
import getl.dbf.proc.ReadBlob
import getl.dbf.proc.ReadBoolean
import getl.dbf.proc.ReadDate
import getl.dbf.proc.ReadDatetime
import getl.dbf.proc.ReadDouble
import getl.dbf.proc.ReadFloat
import getl.dbf.proc.ReadInteger
import getl.dbf.proc.ReadNumeric
import getl.dbf.proc.ReadString
import getl.dbf.sub.ReadProcessor
import getl.driver.Driver
import getl.driver.FileDriver
import getl.exception.ExceptionGETL
import getl.utils.FileUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import java.nio.charset.Charset

@InheritConstructors
class DBFDriver extends FileDriver {
    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        [Driver.Support.EACHROW, Driver.Support.NOT_NULL_FIELD]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations () {
        [Driver.Operation.DROP, Driver.Operation.RETRIEVEFIELDS]
    }


    /** Convert DBF field to Getl field */
    static Field DBFField2Field(DBFField dbfField) {
        Field res = new Field(name: dbfField.name, typeName: dbfField.type.toString())
        switch (dbfField.type) {
            case DBFDataType.CHARACTER:
                res.type = Field.stringFieldType
                res.length = dbfField.length
                break
            case DBFDataType.VARCHAR:
                res.type = Field.blobFieldType
                res.length = dbfField.length
                break
            case DBFDataType.VARBINARY: case DBFDataType.BINARY: case DBFDataType.BLOB:
            case DBFDataType.PICTURE: case DBFDataType.GENERAL_OLE:
                res.type = Field.blobFieldType
                res.length = dbfField.length
                break
            case DBFDataType.DATE:
                res.type = Field.dateFieldType
                break
            case DBFDataType.DOUBLE:
                res.type = Field.doubleFieldType
                break
            case DBFDataType.LOGICAL:
                res.type = Field.booleanFieldType
                break
            case DBFDataType.MEMO:
                res.type = Field.textFieldType
                res.length = dbfField.length
                break
            case DBFDataType.NUMERIC: case DBFDataType.FLOATING_POINT:
                if (res.precision > 0) {
                    res.type = Field.numericFieldType
                    res.length = dbfField.length
                    res.precision = dbfField.decimalCount
                }
                else if (res.length < 10)
                    res.type = Field.integerFieldType
                else
                    res.type = Field.bigintFieldType
                break
            case DBFDataType.LONG:
                res.type = Field.integerFieldType
                break
            case DBFDataType.AUTOINCREMENT:
                res.type = Field.integerFieldType
                res.isAutoincrement = true
                break
            case DBFDataType.CURRENCY:
                res.type = Field.numericFieldType
                res.length = dbfField.length
                res.precision = dbfField.decimalCount
                break
            case DBFDataType.TIMESTAMP: case DBFDataType.TIMESTAMP_DBASE7:
                res.type = Field.datetimeFieldType
                break
            default:
                throw new ExceptionGETL("Not support DBF type \"${dbfField.type}\"!")
        }

        return res
    }

    @Override
    List<Field> fields(Dataset dataset) {
        def ds = dataset as DBFDataset
        if (ds.fileName == null)
            throw new ExceptionGETL('Dataset required fileName!')

        ds.currentDbfConnection.validPath()

        def res = [] as List<Field>
        DBFReader reader = null
        try {
            reader = new DBFReader(getFileInputStream(ds, new HashMap()), Charset.forName(ds.codePage()))
            def numberOfFields = reader.fieldCount
            for (int i = 0; i < numberOfFields; i++) {
                res.add(DBFField2Field(reader.getField(i)))

            }
        }
        finally {
            DBFUtils.close(reader)
        }

        return res
    }

    @CompileStatic
    @Override
    Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
        if (code == null)
            throw new ExceptionGETL('Required process code!')

        def ds = dataset as DBFDataset
        if (ds.fileName == null)
            throw new ExceptionGETL('Dataset required fileName!')

        ds.currentDbfConnection.validPath()

        if (ds.field.isEmpty()) {
            ds.retrieveFields()
            if (ds.field.isEmpty())
                throw new ExceptionGETL("Unable to get list of fields for DBF file \"$ds\"!")
        }

        def procs = [] as List<ReadProcessor>
        ds.field.each { field ->
            ReadProcessor proc
            switch (field.type) {
                case Field.stringFieldType: case Field.textFieldType:
                    proc = new ReadString(field)
                    break
                case Field.integerFieldType:
                    proc = new ReadInteger(field)
                    break
                case Field.bigintFieldType:
                    proc = new ReadBigint(field)
                    break
                case Field.numericFieldType:
                    proc = new ReadNumeric(field)
                    break
                case Field.doubleFieldType:
                    if (field.typeName == 'FLOATING_POINT')
                        proc = new ReadFloat(field)
                    else
                        proc = new ReadDouble(field)
                    break
                case Field.booleanFieldType:
                    proc = new ReadBoolean(field)
                    break
                case Field.dateFieldType:
                    proc = new ReadDate(field)
                    break
                case Field.datetimeFieldType:
                    proc = new ReadDatetime(field)
                    break
                case Field.blobFieldType:
                    proc = new ReadBlob(field)
                    break
                default:
                    throw new ExceptionGETL("Read does not support field type \"${field.type} [${field.typeName}]\"!")
            }
            procs.add(proc)
        }

        def limit = (params.limit as Long)?:0L
        def filter = params.filter as Closure<Boolean>
        def res = 0L

        DBFReader reader = null
        try {
            reader = new DBFReader(getFileInputStream(ds, new HashMap()), Charset.forName(ds.codePage()))
            def dbfFileName = FileUtils.FilenameWithoutExtension(ds.fullFileName())
            def dbtFile = new File("${dbfFileName}.${ds.fileMemoExtension()}")
            if (dbtFile.exists())
                reader.setMemoFile(dbtFile)

            DBFRow row
            def cur = 0L
            while ((row = reader.nextRow()) != null) {
                def destRow = new HashMap<String, Object>()
                for (proc in procs)
                    proc.read(row, destRow)

                if (filter != null && !filter(destRow))
                    continue

                cur++
                code.call(destRow)
                if (cur == limit || code.directive == Closure.DONE)
                    break
            }
        }
        finally {
            DBFUtils.close(reader)
        }

        return res
    }
}
