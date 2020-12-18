package getl.yaml

import getl.data.Dataset
import getl.data.Field
import getl.data.FileDataset
import getl.driver.Driver
import getl.driver.FileDriver
import getl.exception.ExceptionGETL
import getl.utils.GenerationUtils
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
    protected void readRows (YAMLDataset dataset, List<String> listFields, Long limit, def data, Closure code) {
        StringBuilder sb = new StringBuilder()
        sb << "{ getl.yaml.YAMLDataset dataset, Closure code, Object data, Long limit ->\n"
        sb << 'proc(dataset, code, data, limit)\n'
        sb << '}\n'
        sb << '@groovy.transform.CompileStatic\n'
        sb << 'void proc(getl.yaml.YAMLDataset dataset, Closure code, Object data, Long limit) {\n'

        def genScript = GenerationUtils.GenerateConvertFromBuilderMap(dataset, listFields,
                'Map', true, dataset.dataNode, 'struct',
                'row', 0, 1)
        sb << genScript.head

        sb << "def cur = 0L\n"
        def rootNode = dataset.rootNode
        if (rootNode != '.') {
            def sect = GenerationUtils.GenerateRootSections('(data as Map)', rootNode, 'Map')
            sb << sect.join('\n')
            sb << '\n'
            sb << "def rootList = _getl_root_${sect.size() - 1}"
        }
        else {
            sb << 'def rootList = data as List<Map>'
        }
        sb << '\n'
        sb << 'rootList?.each { Map struct ->\n'
        sb << """	if (limit > 0) {
	cur++
	if (cur > limit) {
		directive = Closure.DONE
		return
	}
}
"""
        sb << '	Map<String, Object> row = [:]\n'
        sb << genScript.body
        sb << "	code.call(row)\n"
        sb << "}\n}"
//		println sb.toString()

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

        readRows(dataset, fields, limit, data, code)
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