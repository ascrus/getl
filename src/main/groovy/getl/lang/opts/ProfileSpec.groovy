package getl.lang.opts

import getl.lang.Getl
import getl.stat.ProcessTime
import getl.utils.Logs
import groovy.transform.InheritConstructors

import java.util.logging.Level

/**
 * Profiler options
 * @author Alexsey Konstantinov
 *
 */
class ProfileSpec extends BaseSpec {
    ProfileSpec(Object owner, String name, String objectName = null, Boolean isProfile = false) {
        super(owner)
        this.isProfile = isProfile
        statistics = new ProcessTime(
                name: name,
                logLevel: (isProfile)?Level.INFO:((owner as Getl).options().processTimeLevelLog),
                debug: (isProfile)?true:((owner as Getl).options().processTimeDebug),
                objectName: objectName?:'row',
                abbrName: (isProfile)?'<PROFILE>':'<STAT>'
        )
    }

    /** Getl profile block */
    Boolean isProfile = false

    /** Profile statistics object */
    ProcessTime statistics

    /** Start profiling process */
    void startProfile() {
        statistics.countRow = null
    }

    /** Finish profiling process */
    void finishProfile() {
        statistics.finish(statistics.countRow)
    }

    /** Profile name */
    String getProfileName() { statistics.name }
    /** Profile name */
    void setProfileName(String value) { statistics.name = value }

    /** Count processed row */
    Long getCountRow() { statistics.countRow }
    /** Count processed row */
    void setCountRow(Long value) { statistics.countRow = value }
}