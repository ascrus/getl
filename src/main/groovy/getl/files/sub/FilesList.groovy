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
package getl.files.sub

import getl.files.Manager
import groovy.transform.CompileStatic

/**
 * List of files for files managers
 * @author Alexsey Konstantinov
 */
class FilesList extends FileManagerList {
    public File[] listFiles

    @CompileStatic
    @Override
    Integer size () {
        listFiles.length
    }

    @CompileStatic
    @Override
    Map item (int index) {
        File f = listFiles[index]

        Map<String, Object> m =  new HashMap<String, Object>()
        m.filename = f.name
        m.filedate = new Date(f.lastModified())
        m.filesize = f.length()
        if (f.isDirectory()) m.type = Manager.TypeFile.DIRECTORY else m.type = Manager.TypeFile.FILE

        return m
    }

    @CompileStatic
    @Override
    void clear () {
        listFiles = []
    }
}