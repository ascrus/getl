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
package getl.lang.sub

import getl.exception.ExceptionDSL
import getl.files.*
import getl.utils.MapUtils
import groovy.transform.InheritConstructors

/**
 * Repository files manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositoryFilemanagers extends RepositoryObjects<Manager> {
    public static final String FILEMANAGER = 'getl.files.FileManager'
    public static final String FTPMANAGER = 'getl.files.FTPManager'
    public static final String HDFSMANAGER = 'getl.files.HDFSManager'
    public static final String SFTPMANAGER = 'getl.files.SFTPManager'

    /** List of allowed file manager classes */
    public static final List<String> LISTFILEMANAGERS = [FILEMANAGER, FTPMANAGER, HDFSMANAGER, SFTPMANAGER]

    @Override
    List<String> getListClasses() {
        return LISTFILEMANAGERS
    }

    @Override
    protected Manager createObject(String className) {
        return Manager.CreateManager(manager: className)
    }

    @Override
    Map exportConfig(String name) {
        def obj = find(name)
        if (obj == null)
            throw new ExceptionDSL("File manager \"$name\" not found!")

        return [manager: obj.class.name] + obj.params
    }

    @Override
    GetlRepository importConfig(Map config) {
        return Manager.CreateManager(config)
    }
}