package getl.lang.sub

import getl.files.*
import getl.utils.FileUtils
import groovy.transform.InheritConstructors

/**
 * Repository files manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
@SuppressWarnings('SpellCheckingInspection')
class RepositoryFilemanagers extends RepositoryObjects<Manager> {
    static public final String FILEMANAGER = 'getl.files.FileManager'
    static public final String FTPMANAGER = 'getl.files.FTPManager'
    static public final String HDFSMANAGER = 'getl.files.HDFSManager'
    static public final String SFTPMANAGER = 'getl.files.SFTPManager'
    static public final String RESOURCEMANAGER = 'getl.files.ResourceManager'

    /** List of allowed file manager classes */
    static public final List<String> LISTFILEMANAGERS = [FILEMANAGER, FTPMANAGER, HDFSMANAGER, SFTPMANAGER, RESOURCEMANAGER]

    @Override
    List<String> getListClasses() {
        return LISTFILEMANAGERS
    }

    @Override
    protected Manager createObject(String className) {
        return Manager.CreateManager(manager: className)
    }

    @Override
    Map exportConfig(GetlRepository repObj) {
        return [manager: repObj.getClass().name] + ((repObj as Manager).params)
    }

    @Override
    GetlRepository importConfig(Map config, GetlRepository existObject, String objectName) {
        return (existObject != null)?(existObject as Manager).importParams(config):Manager.CreateManager(config)
    }

    @Override
    Boolean needEnvConfig() { true }

    @Override
    protected void processUnregisteringObject(Manager obj) {
        super.processUnregisteringObject(obj)

        if (obj.connected)
            obj.disconnect()
    }
}