package getl.lang.opts

import getl.exception.ExceptionDSL
import getl.files.Manager
import getl.lang.Getl
import getl.proc.FileCleaner
import getl.proc.FileCopier
import getl.proc.FileProcessing
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * File manager class
 */
@InheritConstructors
class FilemanSpec extends BaseSpec {
    /** Getl instance */
    protected Getl getGetl() { _owner as Getl }

    /**
     * Copying files according to the specified rules between file systems
     * @param source source file manager
     * @param destinations list of destination file manager
     * @param cl process parameter setting code
     * @return file copier instance
     */
    FileCopier copier(Manager source, List<Manager> destinations,
                      @DelegatesTo(FileCopier)
                          @ClosureParams(value = SimpleType, options = ['getl.proc.FileCopier']) Closure cl) {
        if (source == null)
            throw new ExceptionDSL('Required source file manager!')
        if (destinations == null && destinations.isEmpty())
            throw new ExceptionDSL('Required destination file manager!')

        def parent = new FileCopier()
        parent.dslCreator = getl
        parent.source = source
        parent.destinations = destinations

        def ptName = "Copy files from [$source]"
        def pt = getGetl().startProcess(ptName, 'file')
        try {
            parent.ConnectTo([source] + destinations, 1, 1)
            runClosure(parent, cl)
            parent.process()
        }
        finally {
            parent.DisconnectFrom([source] + destinations)
        }
        getGetl().finishProcess(pt, parent.countFiles)

        return parent
    }

    /**
     * Copying files according to the specified rules between file systems
     * @param source source file manager
     * @param destination destination file manager
     * @param cl process parameter setting code
     * @return file copier instance
     */
    FileCopier copier(Manager source, Manager destination,
                      @DelegatesTo(FileCopier)
                          @ClosureParams(value = SimpleType, options = ['getl.proc.FileCopier']) Closure cl) {
        copier(source, [destination], cl)
    }

    /**
     * Cleaner files according to the specified rules from source file system
     * @param source source file manager
     * @param cl process parameter setting code
     * @return file cleaner instance
     */
    FileCleaner cleaner(Manager source,
                        @DelegatesTo(FileCleaner)
                            @ClosureParams(value = SimpleType, options = ['getl.proc.FileCleaner']) Closure cl) {
        if (source == null)
            throw new ExceptionDSL('Required source file manager!')

        def parent = new FileCleaner()
        parent.dslCreator = getl
        parent.source = source

        def ptName = "Remove files from [$source]"
        def pt = getGetl().startProcess(ptName, 'file')
        try {
            parent.ConnectTo([source], 1, 1)
            runClosure(parent, cl)
            parent.process()
        }
        finally {
            parent.DisconnectFrom([source])
        }
        getGetl().finishProcess(pt, parent.countFiles)

        return parent
    }

    /**
     * Processing files according to the specified rules from source file system
     * @param source source file manager
     * @param archiveStorage archive file storage
     * @param cl process parameter setting code
     * @return file processing instance
     */
    FileProcessing processing(Manager source,
                              @DelegatesTo(FileProcessing)
                                  @ClosureParams(value = SimpleType, options = ['getl.proc.FileProcessing']) Closure cl) {
        if (source == null)
            throw new ExceptionDSL('Required source file manager!')

        def parent = new FileProcessing()
        parent.dslCreator = getl
        parent.source = source

        def ptName = "Processing files from [$source]"
        def pt = getGetl().startProcess(ptName, 'file')
        try {
            parent.ConnectTo([source], 1, 1)
            runClosure(parent, cl)
            parent.process()
        }
        finally {
            parent.DisconnectFrom([source])
        }
        getGetl().finishProcess(pt, parent.countFiles)

        return parent
    }
}