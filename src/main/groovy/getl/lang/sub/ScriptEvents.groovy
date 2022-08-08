//file:noinspection unused
package getl.lang.sub

import groovy.transform.Synchronized

/**
 * Events manager
 * @author Alexsey Konstantinov
 */
class ScriptEvents extends HashMap<String, HashMap<String, Closure>> {
    /** Name of all objects identification */
    static public final ALL_OBJECTS = '#ALL#'

    /**
     * Add event on object
     * @param  objectName object name
     * @param eventName event name
     * @param cl event code
     */
    @Synchronized
    void event(String objectName, String eventName, Closure cl) {
        if (eventName == null)
            throw new NullPointerException('Required event name!')

        if (objectName == null)
            objectName = ALL_OBJECTS

        def objectEvents = get(objectName)
        if (objectEvents == null) {
            objectEvents = new HashMap<String, Closure>()
            put(objectName, objectEvents)
        }
        objectEvents.put(eventName, cl)
    }

    /**
     * Add event on all objects
     * @param eventName event name
     * @param cl event code
     */
    void event(String eventName, Closure cl) {
        event(null, eventName, cl)
    }

    /**
     * Get list of events by specified object
     * @param objectName object name
     * @return list of events
     */
    @Synchronized
    Map<String, Closure> objectEvents(String objectName) {
        def res = (containsKey(ALL_OBJECTS))?get(ALL_OBJECTS):new HashMap<String, Closure>()
        if (containsKey(objectName))
            res.putAll(get(objectName))

        return res
    }

    /**
     * Get event by specified object
     * @param objectName object name
     * @param eventName event name
     * @return event code
     */
    @Synchronized
    Closure eventFromObject(String objectName, String eventName) {
        return objectEvents(objectName).get(eventName)
    }

    /**
     * Get event from all objects
     * @param eventName event name
     * @return event code
     */
    @Synchronized
    Closure eventFromAll(String eventName) {
        def res = (containsKey(ALL_OBJECTS))?get(ALL_OBJECTS):new HashMap<String, Closure>()
        return res.get(eventName)
    }

    /**
     * Get list of event registered
     */
    List<String> getListEvents() {
        def res = [] as List<String>
        each { name, event ->
            res.addAll(event.keySet().toList())
        }

        return res.unique().sort(true)
    }
}