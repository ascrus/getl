package getl.yaml

import getl.data.Dataset
import getl.data.Field
import getl.data.FileDataset
import getl.driver.Driver
import getl.driver.FileDriver
import getl.exception.ExceptionGETL
import getl.utils.GenerationUtils
import getl.utils.StringUtils
import groovy.transform.CompileStatic
import groovy.yaml.YamlSlurper

/**
 * JSON driver class
 * @author Alexsey Konstantinov
 *
 */
class YAMLDriver extends FileDriver {
    YAMLDriver() {
        super()
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        [Driver.Support.EACHROW, Driver.Support.AUTOLOADSCHEMA]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        [Driver.Operation.DROP]
    }

    @Override
    List<Field> fields(Dataset dataset) {
        return null
    }

    /**
     * Read attributes and rows from dataset
     * @param dataset source dataset
     * @param listFields list of read fields
     * @param rootNode start read node name
     * @param limit limit read rows (0 for unlimited)
     * @param data json data object
     * @param initAttr attributes initialization code
     * @param code row process code
     */
    @CompileStatic
    protected void readRows (YAMLDataset dataset, List<String> listFields, String rootNode, Long limit, def data, Closure code) {
        StringBuilder sb = new StringBuilder()
        sb << "{ getl.yaml.YAMLDataset dataset, Closure code, Object data, Long limit ->\n"

        sb << "Long cur = 0\n"
        sb << 'data' + ((rootNode != ".")?(".${StringUtils.ProcessObjectName(rootNode, true, true)}"):'') + ".each { struct ->\n"
        sb << """
if (limit > 0) {
	cur++
	if (cur > limit) {
		directive = Closure.DONE
		return
	}
}
"""
        sb << '	Map row = [:]\n'
        def c = 0
        dataset.field.each { Field d ->
            c++
            if (listFields.isEmpty() || listFields.find { it.toLowerCase() == d.name.toLowerCase() }) {
                Field s = d.copy()
                if (s.type in [Field.Type.DATETIME, Field.Type.DATE, Field.Type.TIME, Field.Type.TIMESTAMP_WITH_TIMEZONE])
                    s.type = Field.Type.STRING

                String path = GenerationUtils.Field2Alias(d)
                sb << "	row.'${d.name.toLowerCase()}' = "
                sb << GenerationUtils.GenerateConvertValue(d, s, d.format?:'yyyy-MM-dd\'T\'HH:mm:ss', "struct.${path}", false)

                sb << "\n"
            }
        }
        sb << "	code.call(row)\n"
        sb << "}\n}"

        def script = sb.toString()
        def hash = script.hashCode()
        Closure cl
        def driverParams = (dataset._driver_params as Map<String, Object>)
        if (((driverParams.hash_code_read as Integer)?:0) != hash) {
            cl = GenerationUtils.EvalGroovyClosure(script)
            driverParams.code_read = cl
            driverParams.hash_code_read = hash
        }
        else {
            cl = driverParams.code_read as Closure
        }

        cl.call(dataset, code, data, limit)
    }

    /**
     * Read YAML data from file
     * @param dataset source dataset
     * @param params process parameters
     */
    @CompileStatic
    protected def readData(YAMLDataset dataset, Map params) {
        def parser = new YamlSlurper()
        def data = null

        def reader = getFileReader(dataset as FileDataset, params)
        try {
            data = parser.parse(reader)
        }
        finally {
            reader.close()
        }

        return data
    }

    /**
     * Read and process YAML file
     * @param dataset processed file
     * @param params process parameters
     * @param prepareCode prepare field code
     * @param code process row code
     */
    @CompileStatic
    protected void doRead(YAMLDataset dataset, Map params, Closure prepareCode, Closure code) {
        if (dataset.field.isEmpty())
            throw new ExceptionGETL("Required fields description with dataset!")
        if (dataset.rootNode == null)
            throw new ExceptionGETL("Required \"rootNode\" parameter with dataset!")
        String rootNode = dataset.rootNode

        def fn = fullFileNameDataset(dataset)
        if (fn == null)
            throw new ExceptionGETL("Required \"fileName\" parameter with dataset!")
        File f = new File(fn)
        if (!f.exists())
            throw new ExceptionGETL("File \"${fn}\" not found!")

        Long limit = (params.limit != null)?(params.limit as Long):0

        def data = readData(dataset, params)

        List<String> fields = []
        if (prepareCode != null) {
            prepareCode.call(fields)
        }
        else if (params.fields != null)
            fields = params.fields as List<String>

        readRows(dataset, fields, rootNode, limit, data, code)
    }

    @Override
    @CompileStatic
    Long eachRow (Dataset dataset, Map params, Closure prepareCode, Closure code) {
        Closure<Boolean> filter = params."filter" as Closure<Boolean>

        def countRec = 0L
        doRead(dataset as YAMLDataset, params, prepareCode) { Map row ->
            if (filter != null && !(filter.call(row))) return

            countRec++
            code.call(row)
        }

        return countRec
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
    void closeWrite(Dataset dataset) {
        throw new ExceptionGETL('Not support this features!')
    }
}