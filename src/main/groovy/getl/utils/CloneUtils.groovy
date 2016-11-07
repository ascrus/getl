/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

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

import getl.exception.ExceptionGETL
import groovy.json.internal.LazyMap

/**
 * Clone objects utilite library
 * @author Alexsey Konstantinov
 */
class CloneUtils {
    public static def CloneObject(def obj) {
        if (obj == null) return null

        def res = null
        if (obj instanceof Map) {
            res = CloneMap(obj as Map)
        }
        else if (obj instanceof List) {
            res = CloneList(obj as List)
        }
        else {
            if (obj instanceof String) {
                res = new String(obj as String)
            }
            else if (obj instanceof Integer) {
                res = new Integer((obj as Integer).intValue())
            }
            else if (obj instanceof Long) {
                res = new Long((obj as Long).longValue())
            }
            else if (obj instanceof BigInteger) {
                res = new BigInteger((obj as BigInteger).toByteArray())
            }
            else if (obj instanceof BigDecimal) {
                res = new BigDecimal((obj as BigDecimal).doubleValue())
            }
            else if (obj instanceof Double) {
                res = new Double((obj as Double).doubleValue())
            }
            else if (obj instanceof Date) {
                res = new Date((obj as Date).time)
            }
            else if (obj instanceof Boolean) {
                res = new Boolean((obj as Boolean).booleanValue())
            }
            else if (obj instanceof Cloneable) {
                res = obj.clone()
            }
        }

        return res
    }


    /**
     * Clone map
     * @param map
     * @return
     */
    public static Map CloneMap (Map obj) {
        if (obj == null) return null
        if (obj instanceof LazyMap) obj = new TreeMap(obj)

        if (!(obj instanceof Cloneable)) return null

        def res = obj.clone() as Map
        res.each { k, v ->
            res.put(k, CloneObject(v))
        }

        return res
    }

    /**
     * Clone list
     * @param list
     * @return
     */
    public static List CloneList(List obj) {
        if (obj == null) return null
        if (!(obj instanceof Cloneable)) return null

        List res = obj.clone() as List
        for (int i = 0; i < res.size(); i++) {
            res[i] = CloneObject(res[i])
        }

        return res
    }
}
