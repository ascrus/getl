package getl.data.sub

import getl.data.Dataset
import getl.data.Field
import getl.exception.ExceptionGETL

import java.util.function.Predicate

/**
 * List of field from datasets
 * @author Alexsey Konstantinov
 */
class FieldList extends ArrayList<Field> {
    FieldList(Dataset ownerDataset) {
        super()
        if (ownerDataset == null)
            throw new NullPointerException('Required "owner" parameter value!')
        this.ownerDataset = ownerDataset
    }

    /** Owner dataset */
    private Dataset ownerDataset
    /** Owner dataset */
    Dataset getOwner() { this.ownerDataset }

    /** Allow change list fields */
    Boolean getAllowChange() { this.ownerDataset.allowChangeFields() }

    void checkChange() {
        if (!allowChange)
            throw new ExceptionGETL("Dataset \"$ownerDataset\" does not allow changing the field set!")
    }

    @Override
    void clear() {
        checkChange()
        super.clear()
    }

    @Override
    boolean add(Field value) {
        checkChange()
        return super.add(value)
    }

    @Override
    void add(int pos, Field value) {
        checkChange()
        super.add(pos, value)
    }

    @Override
    Field remove(int pos) {
        checkChange()
        super.remove(pos)
    }

    @Override
    boolean remove(Object field) {
        checkChange()
        return super.remove(field)
    }

    @Override
    boolean removeAll(Collection list) {
        checkChange()
        return super.removeAll(list)
    }

    @Override
    boolean removeIf(Predicate<? super Field> filter) {
        checkChange()
        return super.removeIf(filter)
    }

    @Override
    boolean addAll(Collection list) {
        checkChange()
        super.addAll(list)
    }

    @Override
    boolean addAll(int pos, Collection list) {
        checkChange()
        super.addAll(pos, list)
    }

    void setFields(List<Field> value) {
        if (!allowChange && size() > 0 && (value == null || value.size() != size()))
            throw new ExceptionGETL("The list of dataset \"$ownerDataset\" fields should contain only ${size()} fields!")

        super.clear()
        value?.each { Field f ->
            def name = f.name.toLowerCase()
            if (this.find {it.name.toLowerCase() == name } != null )
                return

            Field n = f.copy()
            if (ownerDataset.connection != null)
                ownerDataset.connection.driver.prepareField(n)

            super.add(n)
        }
    }
}