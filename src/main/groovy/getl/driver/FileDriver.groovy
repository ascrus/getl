package getl.driver

import getl.data.sub.FileWriteOpts
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

import java.util.concurrent.locks.Lock
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import getl.data.*
import getl.csv.CSVDataset
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * Base file driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FileDriver extends Driver {
	/** Retrieve object type  */
	static enum RetrieveObjectType {FILE, DIR}
	/** Retrieve sort method */
	static enum RetrieveObjectSort {NONE, NAME, DATE, SIZE}

	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('retrieveObjects', ['directory', 'mask', 'type', 'sort', 'recursive'])
		methodParams.register('eachRow', ['append', 'codePage'])
		methodParams.register('openWrite', ['append', 'codePage', 'createPath', 'deleteOnEmpty',
											'availableAfterWrite'])
	}

	@Override
	List<Support> supported() { [] as List<Support> }

	@Override
	List<Operation> operations() { [Operation.DROP] }

	@Override
	List<Object> retrieveObjects (Map params, Closure<Boolean> filter) {
		def path = (connection as FileConnection).currentPath()
		if (path == null) throw new ExceptionGETL('Path not setting!')
		
		if (params.directory != null) path += (connection as FileConnection).fileSeparator() + params.directory
		def match = (params.mask != null)?(params.mask as String):'.*'
		RetrieveObjectType type = (params.type == null)?RetrieveObjectType.FILE:(params.type as RetrieveObjectType)
		RetrieveObjectSort sort = (params.sort == null)?RetrieveObjectSort.NONE:(params.sort as RetrieveObjectSort)
		def recursive = BoolUtils.IsValue(params.recursive)
		
		def list = []
		
		def f = new File(path)
		if (!f.exists()) return list
		
		def add = { File file ->
			if (file.name.matches(match)) {
				if (filter == null || filter(file)) list << file
			}
		}

		if (type == RetrieveObjectType.FILE) {
			if (!recursive)
				f.eachFile(add)
			else
				f.eachFileRecurse(add)
		} else if (type == RetrieveObjectType.DIR) {
			if (!recursive)
				f.eachDir(add)
			else
				f.eachDirRecurse(add)
		}
		
		if (sort != RetrieveObjectSort.NONE) {
			switch (sort) {
				case RetrieveObjectSort.NAME:
					list = list.sort { (it as File).name }
					break
				case RetrieveObjectSort.DATE:
					list = list.sort { (it as File).lastModified() }
					break
				case RetrieveObjectSort.SIZE:
					list = list.sort { (it as File).length() }
					break
			}
		} 

		return list
	}

	@Override
	List<Field> fields(Dataset dataset) {
		return null
	}

	/**
	 * Fill file name without GZ extension
	 * @param dataset
	 * @return
	 */
	@SuppressWarnings("GrMethodMayBeStatic")
	String fullFileNameDatasetWithoutGZ(FileDataset dataset) {
		String fn = dataset.fileName()
		if (fn == null)
			return null
		def con = dataset.fileConnection
		if (dataset.extension() != null) fn += ".${dataset.extension()}"
		if (con.path != null) {
			if (FileUtils.IsResourceFileName(con.currentPath()))
				fn = FileUtils.ConvertToUnixPath(con.currentPath()) + '/' + fn
			else
				fn = FileUtils.ConvertToDefaultOSPath(con.currentPath()) + con.fileSeparator() + fn
		}

		return FileUtils.ResourceFileName(fn, dataset.dslCreator)
	}
	
	/**
	 * Clear file extension by file name	
	 * @param dataset
	 * @return
	 */
	@SuppressWarnings("GrMethodMayBeStatic")
	String fileNameWithoutExtension(FileDataset dataset) {
		def fn = dataset.fileName()
		if (fn == null)
			return null

		def extDs = dataset.extension()
		def extFile = FileUtils.FileExtension(fn)?.toLowerCase()
		if (dataset.isGzFile() && extFile == "gz")
			fn = FileUtils.ExcludeFileExtension(fn)
		if (extDs != null && extFile == extDs.toLowerCase())
			fn = FileUtils.ExcludeFileExtension(fn)

		return fn
	}
	
	/**
	 * Full file name with number of split portion 
	 * @param dataset
	 * @param portion
	 * @return
	 */
	String fullFileNameDataset(FileDataset dataset, Integer portion = null) {
		String fn = fileNameWithoutExtension(dataset)
		if (fn == null)
			return null
		def con = dataset.fileConnection

		if (con.path != null) {
			if (FileUtils.IsResourceFileName(con.currentPath()))
				fn = FileUtils.ConvertToUnixPath(con.currentPath()) + '/' + fn
			else
				fn = FileUtils.ConvertToDefaultOSPath(con.currentPath()) + con.fileSeparator() + fn
		}

		if (portion != null)
			fn = "${fn}.${StringUtils.AddLedZeroStr(portion.toString(), 4)}"
		if (dataset.extension() != null)
			fn += ".${dataset.extension()}"
		if (dataset.isGzFile())
			fn += ".gz"
		
		return FileUtils.ResourceFileName(fn, dataset.dslCreator)
	}
	
	/**
	 * Full file schema name
	 * @param dataset
	 * @return
	 */
	@Override
	String fullFileNameSchema(Dataset dataset) {
		return (dataset.schemaFileName != null)?FileUtils.ResourceFileName(dataset.schemaFileName, dataset.dslCreator):
				(fullFileNameDatasetWithoutGZ(dataset as FileDataset) + ".schema")
	}
	
	/**
	 * File mask name with number of split portion
	 * @param dataset
	 * @param isSplit
	 * @return
	 */
	String fileMaskDataset(FileDataset dataset, Boolean isSplit) {
		String fn = fileNameWithoutExtension(dataset)

		if (isSplit)
			fn += ".{number}"
		if (dataset.extension() != null) fn += ".${dataset.extension()}"
		
		def isGzFile = dataset.isGzFile()
		if (isGzFile) fn += ".gz"

		return fn
	}

	@Override
	void dropDataset(Dataset dataset, Map params) {
		def ds = dataset as FileDataset
        if (params.portions == null) {
            def f = new File(fullFileNameDataset(ds))
            if (f.exists()) {
                f.delete()
            } else if (BoolUtils.IsValue(params.validExist, false)) {
                throw new ExceptionGETL("File ${fullFileNameDataset(ds)} not found")
            }
        }
        else {
            (1..(params.portions as Integer)).each { Integer num ->
                def f = new File(fullFileNameDataset(ds, num))
                if (f.exists()) {
                    f.delete()
                } else if (BoolUtils.IsValue(params.validExist, false)) {
                    throw new ExceptionGETL("File ${fullFileNameDataset(ds)} not found")
                }
            }
        }

        super.dropDataset(dataset, params)
	}

	@Override
	Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		(dataset.connection as FileConnection).validPath()
		return 0L
	}

	@Override
	void openWrite(Dataset dataset, Map params, Closure prepareCode) { }

	@Override
	void write(Dataset dataset, Map row) { }

	/**
	 * Create path to file
	 * @param filePath
	 * @return
	 */
	protected static Boolean createPath(String filePath) {
		return FileUtils.ValidFilePath(filePath)
	}
	
	/**
	 * Get dataset file parameters 
	 * @param dataset
	 * @param params
	 * @param portion
	 * @return
	 */
	@CompileStatic
	protected Map getDatasetParams(FileDataset dataset, Map params, Integer portion = null) {
		def res = new HashMap()
		res.fn = fullFileNameDataset(dataset, portion)
		res.isGzFile = dataset.isGzFile()
		res.codePage = ListUtils.NotNullValue([params.codePage, dataset.codePage()])
		res.isAppend = BoolUtils.IsValue(params.append, dataset.isAppend())
		res.autoSchema = BoolUtils.IsValue(params.autoSchema, dataset.isAutoSchema())
		res.createPath = BoolUtils.IsValue(params.createPath, dataset.isCreatePath())
		res.deleteOnEmpty = BoolUtils.IsValue(params.deleteOnEmpty, dataset.isDeleteOnEmpty())
		
		return res
	}
	
	/**
	 * Get input stream to file
	 * @param dataset
	 * @param params
	 * @param portion
	 * @return
	 */
	@CompileStatic
	InputStream getFileInputStream(FileDataset dataset, Map params = [:], Integer portion = null) {
		if (params == null)
			params = [:]
		def wp = getDatasetParams(dataset, params, portion)
		
		def fn = wp.fn as String
		def isGzFile = BoolUtils.IsValue(wp.isGzFile)
		def isAppend = BoolUtils.IsValue(wp.isAppend)

		if (isAppend && isGzFile)
			throw new ExceptionGETL("The append operation is not allowed for gzip file \"$fn\"!")
		
		InputStream res
		if (isGzFile) {
			res = new GZIPInputStream(new FileInputStream(fn))
		}
		else {
			res = new FileInputStream(fn)
		}
		
		return res
	}

	/**
	 * Get reader file
	 * @param dataset
	 * @param params
	 * @param portion
	 * @return
	 */
	@CompileStatic
	Reader getFileReader(FileDataset dataset, Map params = [:], Integer portion = null) {
		if (params == null)
			params = [:]
		def wp = getDatasetParams(dataset, params, portion)
		def codePage = wp.codePage as String
		return new BufferedReader(new InputStreamReader(getFileInputStream(dataset, params, portion), codePage), dataset.bufferSize())
	}

	/**
	 * Get writer file
	 * @param dataset
	 * @param params
	 * @param portion
	 * @return
	 */
	@CompileStatic
	protected Writer getFileWriter(FileDataset dataset, Map params = [:], Integer portion = null) {
		if (params == null)
			params = [:]
		def wp = getDatasetParams(dataset, params, portion)

		if (BoolUtils.IsValue(params.availableAfterWrite) && (portion?:0) > 1) {
			def opt = dataset.writtenFiles[portion - 2]
			fileLocking.lockObject(dataset.fullFileName()) {
				fixTempFile(dataset, opt)
			}
		}

		def isGzFile = BoolUtils.IsValue(wp.isGzFile)
		def codePage = wp.codePage as String
		def isAppend = BoolUtils.IsValue(wp.isAppend)
		def removeEmptyFile = BoolUtils.IsValue(wp.deleteOnEmpty)

		def orig_fn = ((!dataset.isTemporaryFile)?"${wp.fn}.getltemp":(wp.fn as String)) as String
		def fn = orig_fn
		if (isAppend)
			fn += ".${FileUtils.UniqueFileName()}"
		def writeOpt = new FileWriteOpts()
		writeOpt.tap {
			fileName = wp.fn as String //(!isAppend)?(wp.fn as String):(wp.fn as String)
			tempFileName = fn
			partNumber = portion
			append = isAppend
			deleteOnEmpty = removeEmptyFile
			encode = codePage
		}
		dataset.writtenFiles << writeOpt

		if (BoolUtils.IsValue(wp.createPath))
			createPath(fn)
		
		def writer
		OutputStream output
		def file = new File(fn)

		if (dataset.isTemporaryFile) {
			file.deleteOnExit()
			if (isAppend)
				new File(orig_fn).deleteOnExit()
			if (dataset.isAutoSchema())
				new File(dataset.fullFileSchemaName()).deleteOnExit()
		}

		if (isGzFile) {
			output = new GZIPOutputStream(new FileOutputStream(file, isAppend))
		}
		else {
			output = new FileOutputStream(file, isAppend)
		}
		
		writer = new BufferedWriter(new OutputStreamWriter(output, codePage), dataset.bufferSize())
		
		processWriteFile(wp.fn as String, file)
		
		return writer
	}
	
	/**
	 * Process file when open for write
	 * @param fileName
	 * @param fileTemp
	 */
	protected void processWriteFile(String fileName, File fileTemp) {  }

	/** Lock file manager for synchronize write temporary files to persistent file in multi-threads */
	static private final LockManager fileLocking = new LockManager()

	/**
	 * Fixing temporary files to persistent file or deleting
	 * @param dataset
	 * @param isDelete
	 */
	@CompileStatic
	void fixTempFiles(FileDataset dataset) {
		fileLocking.lockObject(dataset.fullFileName()) {
			def deleteOnEmpty = dataset.writtenFiles[0].deleteOnEmpty
			try {
				dataset.writtenFiles.each { opt ->
					fixTempFile(dataset, opt)
				}
			}
			catch (Exception e) {
				removeTempFiles(dataset)
				throw e
			}

			if (deleteOnEmpty)
				dataset.writtenFiles.removeAll { opt -> opt.countRows == 0 }

			if (deleteOnEmpty && dataset.isAutoSchema() && dataset.writeRows == 0) {
				def s = new File(dataset.fullFileSchemaName())
				if (s.exists())
					s.delete()
			}
		}
	}
	
	/**
	 * Fixing temporary files to persistent file or deleting 
	 * @param dataset
	 * @param isDelete
	 */
	@CompileStatic
	void fixTempFile(FileDataset dataset, FileWriteOpts opt) {
		if (opt.readyFile) return

		def dsFile = new File(opt.fileName as String)
		def tempFile = new File(opt.tempFileName as String)

		if (opt.fileName != opt.tempFileName) {
			if (!opt.append) {
				def isExistsFile = dsFile.exists()
				if (isExistsFile && !dsFile.delete())
					throw new ExceptionGETL("Failed to remove file \"${opt.fileName}\"!")

				if (BoolUtils.IsValue(opt.deleteOnEmpty) && opt.countRows == 0) {
					if (!tempFile.delete())
						connection.logger.severe("Failed to remove file \"${opt.tempFileName}\"!")
				} else if (!tempFile.renameTo(dsFile)) {
					throw new ExceptionGETL("Failed rename temp file \"${opt.tempFileName}\" to \"${opt.fileName}\"!")
				}
			}
			else {
				FileUtils.LockFile(dsFile) {
					def isExistsFile = dsFile.exists()

					def isHeader = fileHeader(dataset)

					if (!isExistsFile && !isHeader) {
						if (!tempFile.renameTo(dsFile))
							throw new ExceptionGETL("Failed rename temp file \"${opt.tempFileName}\" to \"${opt.fileName}\"!")
					} else {
						if (!isExistsFile)
							saveHeaderToFile(dataset, dsFile)

						/*def tempData = t.newInputStream()
						try {
							f.append(tempData)

						}
						finally {
							tempData.close()
						}*/
						FileUtils.AppendToFile(tempFile, dsFile)

						if (!tempFile.delete())
							connection.logger.severe("Failed to remove file \"${opt.tempFileName}\"!")
					}
				}
			}
		}
		else if (BoolUtils.IsValue(opt.deleteOnEmpty) && opt.countRows == 0) {
			if (!dsFile.delete())
				connection.logger.severe("Failed to remove file \"${opt.fileName}\"!")
		}

		opt.tempFileName = null
		opt.readyFile = true
	}

	/**
	 * Remove temporary files
	 * @param dataset
	 */
	@SuppressWarnings("GrMethodMayBeStatic")
	@CompileStatic
	void removeTempFiles(FileDataset dataset) {
		dataset.writtenFiles.each { opt ->
			if (!opt.readyFile && opt.fileName != opt.tempFileName) {
				if (!FileUtils.DeleteFile(opt.tempFileName))
					connection.logger.severe("Failed to remove file \"${opt.tempFileName}\"!")
			}
			opt.fileName = null
			opt.tempFileName = null
		}

		dataset.writtenFiles.clear()
	}
	
	@Override
	void doneWrite (Dataset dataset) {	}

	@Override
	void closeWrite(Dataset dataset) {
		if (dataset.isWriteError) {
			removeTempFiles(dataset as FileDataset)
		} else {
			fixTempFiles(dataset as FileDataset)
		}
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
	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
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
	void connect () {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override
	void disconnect () {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override
	Boolean isConnected() { false }

	/**
	 * Required writing header to file
	 * @param dataset dataset object
	 * @return header
	 */
	protected Boolean fileHeader(FileDataset dataset) { return false }

	/**
	 * Save header to file
	 * @param dataset dataset object
	 * @param file file descriptor
	 */
	protected void saveHeaderToFile(FileDataset dataset, File file) { }
}