package getl.data.sub

import getl.data.Dataset
import getl.data.Field
import getl.exception.DatasetError
import getl.exception.RequiredParameterError

import java.util.function.Predicate

/**
 * List of field from datasets
 * @author Alexsey Konstantinov
 */
class FieldList extends ArrayList<Field> {
    FieldList(Dataset ownerDataset) {
        super()
        if (ownerDataset == null)
            throw new RequiredParameterError('ownerDataset')

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
            throw new DatasetError(ownerDataset, '#dataset.deny_fields_change')
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
            throw new DatasetError(ownerDataset, '#dataset.fields_incorrect_count_params', [
                    countFieldDataset: size(), countFieldParam: value?.size()?:0])

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