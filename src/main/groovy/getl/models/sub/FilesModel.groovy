//file:noinspection unused
package getl.models.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionModel
import getl.files.Manager
import getl.models.opts.ReferenceFileSpec
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized

/**
 * Source files model
 * @author Alexsey Konstantinov
 */
@CompileStatic
@InheritConstructors
class FilesModel<T extends FileSpec> extends BaseModel {
    /** Source file manager name for model */
    String getSourceManagerName() { params.sourceManagerName as String }
    /** Source file manager name for model */
    void setSourceManagerName(String value) { useSourceManager(value) }

    /** Source file manager for model */
    @JsonIgnore
    Manager getSourceManager() { dslCreator.filemanager(sourceManagerName) }
    /** Source file manager for model */
    @JsonIgnore
    void setSourceManager(Manager value) { useSourceManager(value) }

    /** Specify the source file manager for the model */
    void useSourceManager(String managerName) {
        if (managerName == null)
            throw new ExceptionModel('File manager name required!')

        dslCreator.filemanager(managerName)
        saveParamValue('sourceManagerName', managerName)
    }
    /** Specify the source file manager for the model */
    void useSourceManager(Manager manager) {
        if (manager == null)
            throw new ExceptionModel('File manager required!')
        if (manager.dslNameObject == null)
            throw new ExceptionModel('File manager not registered in Getl repository!')

        saveParamValue('sourceManagerName', manager.dslNameObject)
    }

    /** Create new model file */
    FileSpec newFile(String fileName) {
        def modelSpecClass = usedModelClass()
        def constr = modelSpecClass.getConstructor([FilesModel, String].toArray([] as Class[]))
        def res = constr.newInstance(this, fileName) as T

        return res
    }

    /**
     * Add file to model
     * @param filePath path to file
     * @return model of file
     */
    protected T modelFile(String filePath, Closure cl) {
        if (filePath == null)
            throw new ExceptionModel("The file path is not specified!")

        checkModel(false)

        def parent = ((usedObjects as List<ReferenceFileSpec>).find { modelFile -> (modelFile.filePath == filePath) }) as T
        if (parent == null)
            //parent = createSpec(filePath) as T
            parent = addSpec(newFile(filePath)) as T

        parent.runClosure(cl)

        return parent
    }

    private final Object synchModel = synchObjects

    /**
     * Check model
     * @param checkObjects check model object parameters
     * @param checkNodeCode additional validation code for model objects
     */
    @Synchronized('synchModel')
    protected void checkModel(Boolean checkObjects = true, Closure checkNodeCode = null) {
        if (sourceManagerName == null)
            throw new ExceptionModel("The source manager name is not specified!")

        if (checkObjects) {
            def isCon = sourceManager.connected
            if (!isCon)
                sourceManager.connect()
            try {
                super.checkModel(checkObjects, checkNodeCode)
            }
            finally {
                if (!isCon)
                    sourceManager.disconnect()
            }
        }
    }

    @Synchronized('synchModel')
    @Override
    protected void checkObject(BaseSpec obj) {
        super.checkObject(obj)
        def modelFile = obj as FileSpec
        if (!sourceManager.existsFile(modelFile.filePath))
            throw new ExceptionModel("Source file \"${modelFile.filePath}\" not found!")
    }

    @Override
    void doneModel() {
        super.doneModel()
        if (sourceManagerName != null && sourceManager.connected)
            sourceManager.disconnect()
    }
}