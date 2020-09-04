package getl.lang.opts

import getl.data.Dataset
import getl.exception.ExceptionDSL
import getl.lang.Getl
import getl.proc.opts.FlowCopySpec
import getl.proc.opts.FlowProcessSpec
import getl.proc.opts.FlowWriteManySpec
import getl.proc.opts.FlowWriteSpec
import getl.stat.ProcessTime
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/** Etl specification manager */
@InheritConstructors
class EtlSpec extends BaseSpec {
    /** Getl instance */
    protected Getl getGetl() { _owner as Getl }

    /**
     * Copy rows from source to destination dataset
     * <br>Closure gets two parameters: source and destination datasets
     */
    void copyRows(Dataset source, Dataset destination,
                  @DelegatesTo(FlowCopySpec)
                  @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowCopySpec']) Closure cl = null) {
        if (source == null)
            throw new ExceptionDSL('Source dataset cannot be null!')
        if (destination == null)
            throw new ExceptionDSL('Destination dataset cannot be null!')

        ProcessTime pt = getGetl().startProcess("Copy rows from $source to $destination")
        def parent = new FlowCopySpec(getl)
        parent.source = source
        parent.destination = destination
        runClosure(parent, cl)
        if (!parent.isProcessed) parent.copyRow(null)
        getGetl().finishProcess(pt, parent.countRow)
    }

    /** Write rows to destination dataset */
    void rowsTo(Dataset destination,
                @DelegatesTo(FlowWriteSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteSpec']) Closure cl) {
        if (destination == null)
            throw new ExceptionDSL('Destination dataset cannot be null!')
        if (cl == null)
            throw new ExceptionDSL('Required closure code!')

        def pt = getGetl().startProcess("Write rows to $destination")
        def parent = new FlowWriteSpec(getl)
        parent.destination = destination
        runClosure(parent, cl)
        getGetl().finishProcess(pt, parent.countRow)
    }

    /** Write rows to destination dataset */
    void rowsTo(@DelegatesTo(FlowWriteSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteSpec']) Closure cl) {
        if (cl == null)
            throw new ExceptionDSL('Required closure code!')
        def destination = DetectClosureDelegate(cl)
        if (destination == null || !(destination instanceof Dataset))
            throw new ExceptionDSL('Can not detect destination dataset!')

        rowsTo(destination, cl)
    }

    /** Write rows to many destination datasets */
    void rowsToMany(Map destinations,
                    @DelegatesTo(FlowWriteManySpec)
                    @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteManySpec']) Closure cl) {
        if (destinations == null || destinations.isEmpty())
            throw new ExceptionDSL('Destination datasets cannot be null or empty!')
        if (cl == null)
            throw new ExceptionDSL('Required closure code!')

        def destNames = [] as List<String>
        (destinations as Map<String, Dataset>).each { destName, ds -> destNames.add("$destName: ${ds.toString()}".toString()) }
        def pt = getGetl().startProcess("Write rows to $destNames")
        def parent = new FlowWriteManySpec(getl)
        parent.destinations = destinations
        runClosure(parent, cl)
        getGetl().finishProcess(pt)
    }

    /** Process rows from source dataset */
    void rowsProcess(Dataset source,
                     @DelegatesTo(FlowProcessSpec)
                     @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowProcessSpec']) Closure cl) {
        if (source == null)
            throw new ExceptionDSL('Source dataset cannot be null!')
        if (cl == null)
            throw new ExceptionDSL('Required closure code!')
        def pt = getGetl().startProcess("Read rows from $source")
        def parent = new FlowProcessSpec(getl)
        parent.source = source
        runClosure(parent, cl)
        getGetl().finishProcess(pt, parent.countRow)
    }

    /** Process rows from source dataset */
    void rowsProcess(@DelegatesTo(FlowProcessSpec)
                     @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowProcessSpec']) Closure cl) {
        if (cl == null)
            throw new ExceptionDSL('Required closure code!')
        def source = DetectClosureDelegate(cl)
        if (source == null || !(source instanceof Dataset))
            throw new ExceptionDSL('Can not detect source dataset!')
        rowsProcess(source, cl)
    }
}