package getl.utils

import groovy.transform.Synchronized

/**
 * Synchronized object
 * @author Alexsey Konstantinov
 */
class SynchronizeObject {
	private Long count = 0

	/** Current counter value */
	@Synchronized
	Long getCount() { count }

	/** Set new counter value */
	@Synchronized
	setCount(Long value) { count = value?:0 }

	/** Clear counter value */
	@Synchronized
	clear() {
		count = 0
		text = null
		date = null
		list.clear()
	}

	/** Increase counter value */
	@Synchronized
	Long nextCount() {
		count++
		
		return count
	}

	/** Decrease counter value */
	@Synchronized
	Long prevCount() {
		count--
		
		return count
	}

	/** Add number to counter value */
	@Synchronized
	Long addCount(Long value) {
		if (value != null)
			count += value

		return count
	}

	/** Text value */
	private String text

	/** Text value */
	@Synchronized
	String getText() { text }

	/** Text value */
	@Synchronized
	setText (String value) { text = value }

	/** Date value */
	private Date date = DateUtils.ParseDate('1900-01-01')

	/** Date value */
	@Synchronized
	Date getDate() { date }

	/** Date value */
	@Synchronized
	void setDate(Date value) { date = value }

	/** Array list */
	private final List list = []

	/** Array list node by index */
	@Synchronized
	def getList(Integer index) {
		list[index]
	}

	/** Add node to array list by index */
	@Synchronized
	void addToList (Integer index, def value) {
		if (value != null)
			list.add(index, value)
	}

	/** Append node to array list */
	@Synchronized
	Boolean addToList(def value) {
		if (value != null)
			return list.add(value)

		return false
	}

	/** Append list to array list */
	@Synchronized
	static Boolean addAllToList (List list) {
		if (list != null)
			return false

		return list.addAll(list)
	}

	/** Append list to array list by index */
	@Synchronized
	static Boolean addAllToList (Integer index, List list) {
		if (list != null)
			return false

		return list.addAll(index, list)
	}

	/** Clear array list */
	@Synchronized
	void clearList () {
		list.clear()
	}

	/** Check empty array list */
	@Synchronized
	Boolean isEmptyList() {
		list.isEmpty()
	}

	/** Array list */
	@Synchronized
	List getList() { list }

	/** Index the item list by value */
	@Synchronized
	def indexOfListItem(def item) { list.indexOf(item) }

	/** Find item list */
	@Synchronized
	def findListItem(Closure cl) { list.find(cl) }
}