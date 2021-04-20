package getl.proc.sub

import getl.proc.FileProcessing
import getl.utils.NumericUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * File processing build manager class
 * @author Alexsey Konstantinov
 */
@InheritConstructors
@CompileStatic
class FileProcessingBuild extends FileListProcessingBuild {
    /** File processing owner */
    FileProcessing getOwnerProcessing() { owner as FileProcessing }

    /** Segmented columns */
    public List<String> threadGroupColumns

    @Override
    void init() {
        super.init()

        threadGroupColumns = ownerProcessing.threadGroupColumns*.toLowerCase()
    }

    @Override
    Boolean prepare(Map file) {
        if (!super.prepare(file)) return false
        if (file.filetype != 'FILE') return true

        if (!threadGroupColumns.isEmpty()) {
            def  l = []
            threadGroupColumns.each { l.add(file.get(it)) }
            file.put('_hash_', NumericUtils.Hash(l))
        }
        else {
            file.put('_hash_', NumericUtils.Hash(file.filepath))
        }

        return true
    }
}