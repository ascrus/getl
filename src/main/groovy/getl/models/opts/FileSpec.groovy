package getl.models.opts

import getl.models.sub.FilesModel

/**
 * Base model file specification
 * @author Alexsey Konstantinov
 */
class FileSpec extends BaseSpec {
    FileSpec(FilesModel model, String fileName) {
        super(model)
        setFilePath(fileName)
    }

    FileSpec(FilesModel model, Map importParams) {
        super(model, importParams)
    }

    /** Owner processing model */
    protected FilesModel getOwnerFilesModel() { ownerModel as FilesModel }

    /** Path to file */
    String getFilePath() { params.filePath as String }
    /** Path to file */
    void setFilePath(String value) { params.filePath = value }

    @Override
    String toString() {
        return filePath
    }
}