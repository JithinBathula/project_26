package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return Arrays.asList(tdItems).iterator();
    }

    private static final long serialVersionUID = 1L;
    private TDItem[] tdItems; // Just declaration, not assignment
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        tdItems = new TDItem[typeAr.length];

        for (int i = 0; i < typeAr.length; i++) {
            tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        tdItems = new TDItem[typeAr.length];

        for (int i = 0; i < typeAr.length; i++) {
            tdItems[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= tdItems.length) {
            throw new NoSuchElementException("invalid index: " + i);
        }
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= tdItems.length) {
            throw new NoSuchElementException("invalid index: " + i);
        }

        return tdItems[i].fieldType;

    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) {
            throw new NoSuchElementException("null is not a valid field name");
        }

        for (int i = 0; i < tdItems.length; i++) {
            if (name.equals(tdItems[i].fieldName)) {
                return i;
            }
        }

        throw new NoSuchElementException("No Field with name " + name + " is found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;

        for (int i = 0; i < tdItems.length; i++){
            size += tdItems[i].fieldType.getLen();
        }

        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int td1Length = td1.numFields();
        int td2Length = td2.numFields();
        int totalFields = td1Length + td2Length;
        Type[] types = new Type[totalFields];
        String[] strings = new String[totalFields];

        for (int i = 0; i < td1Length; i++){
            types[i] = td1.getFieldType(i);
            strings[i] = td1.getFieldName(i);
        }

        for (int i = 0; i < td2Length; i++){
            types[i + td1Length] = td2.getFieldType(i);
            strings[i + td1Length] = td2.getFieldName(i);
        }

        return new TupleDesc(types, strings);

    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)){
            return false;
        }

        TupleDesc other = (TupleDesc) o;

        if (tdItems.length != other.numFields()){
            return false;
        }
        for (int i = 0; i < tdItems.length; i++){
            if (!tdItems[i].fieldType.equals(other.getFieldType(i))){
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        int hash = 0;                                                                          
        for (int i = 0; i < tdItems.length; i++) {
            hash = hash * 31 + tdItems[i].fieldType.hashCode();
        }
        return hash;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String s = "";

        for (int i = 0; i < tdItems.length; i++){
            s += tdItems[i].fieldType;
            s += "(" + tdItems[i].fieldName + ")";
            if (i < tdItems.length - 1) {
                s += ", ";
            }
        }

        return s;
    }
}
