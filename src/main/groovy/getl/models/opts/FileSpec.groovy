package getl.models.opts

import getl.models.sub.FilesModel

/**
 * Base model file specification
 * @author Alexsey Konstantinov
 */
class FileSpec extends BaseSpec {
    FileSpec(FilesModel model, String fileName) {
        super(model)
        params.filePath = fileName
    }

    FileSpec(FilesModel model, Map importParams) {
        super(model, importParams)
    }

    /** Owner processing model */
    protected FilesModel getOwnerFilesModel() { ownerModel as FilesModel }

    /** Path to file */
    String getFilePath() { params.filePath as String }

    @Override
    String toString() {
        return filePath
    }
}