package getl.utils

/**
 * Clone objects utilities library
 * @author Alexsey Konstantinov
 */
class CloneUtils {
    /**
     * Clone object
     * @param obj object for cloning
     * @param cloneChildObjects cloning children objects
     * @param ignoreClasses ignore objects of specified class names
     * @return new object
     */
    static Object CloneObject(Object obj, Boolean cloneChildObjects = true, List<String> ignoreClasses = null) {
        if (obj == null)
            return null

        if (ignoreClasses != null && obj.getClass().name in ignoreClasses)
            return null

        Object res
        if (obj instanceof Map) {
            res = CloneMap(obj as Map, cloneChildObjects, ignoreClasses)
        }
        else if (obj instanceof Collection) {
            res = CloneList(obj as Collection, cloneChildObjects, ignoreClasses)
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
     * @param ignoreClasses ignore objects of specified class names
     * @return new map
     */
    static Map CloneMap(Map obj, Boolean cloneChildObjects = true, List<String> ignoreClasses = null) {
        if (obj == null)
            return null

        def res = obj.getClass().newInstance() as Map
        obj.each { k, v ->
            if (v == null)
                res.put(k, v)
            else if (ignoreClasses == null || !(v.getClass().name in ignoreClasses))
                res.put(k, CloneObject(v, cloneChildObjects, ignoreClasses))
        }

        return res
    }

    /**
     * Clone list
     * @param list list for cloning
     * @param cloneChildObjects cloning children objects
     * @param ignoreClasses ignore objects of specified class names
     * @return new list
     */
    static List CloneList(Collection obj, Boolean cloneChildObjects = true, List<String> ignoreClasses = null) {
        if (obj == null)
            return null

        def res = obj.getClass().newInstance() as Collection
        for (Integer i = 0; i < obj.size(); i++) {
            def v = obj[i]
            if (v == null)
                res << v
            else if (ignoreClasses == null || !(v.getClass().name in ignoreClasses))
                res << CloneObject(v, cloneChildObjects)
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

        ByteArrayInputStream s = new ByteArrayInputStream(byteData)
        res = new ObjectInputStream(s).readObject()

        return res
    }
}
