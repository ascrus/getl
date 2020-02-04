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

package getl.utils

/**
 * Clone objects utilite library
 * @author Alexsey Konstantinov
 */
class CloneUtils {
    /**
     * Clone object
     * @param obj object for cloning
     * @param cloneChildObjects cloning children objects
     * @return new object
     */
    static Object CloneObject(Object obj, boolean cloneChildObjects = true) {
        if (obj == null) return null

        Object res
        if (obj instanceof Map) {
            res = CloneMap(obj as Map, cloneChildObjects)
        }
        else if (obj instanceof List) {
            res = CloneList(obj as List, cloneChildObjects)
        }
        else if (cloneChildObjects && obj instanceof Cloneable) {
            res = obj.clone()
        }
        else {
            res = obj
        }

        return res
    }


    /**
     * Clone map
     * @param map map for cloning
     * @param cloneChildObjects cloning children objects
     * @return new map
     */
    static Map CloneMap (Map obj, boolean cloneChildObjects = true) {
        if (obj == null) return null

        def res = obj.getClass().newInstance() as Map
        obj.each { k, v ->
            res.put(k, CloneObject(v, cloneChildObjects))
        }

        return res
    }

    /**
     * Clone list
     * @param list list for cloning
     * @param cloneChildObjects cloning children objects
     * @return new list
     */
    static List CloneList(List obj, boolean cloneChildObjects = true) {
        if (obj == null) return null

        def res = obj.getClass().newInstance() as List
        for (int i = 0; i < obj.size(); i++) {
            res << CloneObject(obj[i], cloneChildObjects)
        }

        return res
    }

    /**
     * Clone object by stream
     * @param obj object for cloning
     * @return new object
     */
    static Object StreamClone (Object obj) {
        if (obj == null) return null

        Object res

        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        ObjectOutputStream oos = new ObjectOutputStream(bos)
        try {
            oos.writeObject(obj)
            oos.flush()
        }
        finally {
            oos.close()
            bos.close()
        }
        byte[] byteData = bos.toByteArray()

        ByteArrayInputStream bais = new ByteArrayInputStream(byteData)
        res = new ObjectInputStream(bais).readObject()

        res
    }
}
