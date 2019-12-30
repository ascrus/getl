/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

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
@InheritConstructors
class ProfileSpec extends BaseSpec {
    ProfileSpec(def ownerObject, def thisObject, String name, String objectName = null, boolean isProfile = false) {
        super(ownerObject, thisObject)
        this.isProfile = isProfile
        statistics = new ProcessTime(
                name: name,
                logLevel: (isProfile)?Level.INFO:(geltOwner().langOpts.processTimeLevelLog),
                debug: (isProfile)?true:(geltOwner().langOpts.processTimeDebug),
                objectName: objectName?:'row',
                abbrName: (isProfile)?'<PROFILE>':'<STAT>'
        )
    }

    /** Getl profile block */
    boolean isProfile = false

    /** Getl main class */
    protected Getl geltOwner() { ownerObject as Getl }

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
    Integer getCountRow() { statistics.countRow }
    /** Count processed row */
    void setCountRow(Integer value) { statistics.countRow = value }
}