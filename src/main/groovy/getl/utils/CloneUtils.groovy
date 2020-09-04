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
    static Object CloneObject(Object obj, Boolean cloneChildObjects = true) {
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
    static Map CloneMap (Map obj, Boolean cloneChildObjects = true) {
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
    static List CloneList(List obj, Boolean cloneChildObjects = true) {
        if (obj == null) return null

        def res = obj.getClass().newInstance() as List
        for (Integer i = 0; i < obj.size(); i++) {
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
