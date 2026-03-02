package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            int offset = pid.getPageNumber() * BufferPool.getPageSize();
            RandomAccessFile raf = new RandomAccessFile(f, "r"); // Used to read from anywhere from the file.
            byte[] data = new byte[BufferPool.getPageSize()];
            raf.seek(offset); // Jump to the offset byte
            raf.read(data); // Populate the byte array with the data from the file.
            raf.close();

            // Have to typecast because HeapPage 
            // constructor specifically wants the id to be in heappage.
            HeapPageId heapPageId = (HeapPageId) pid; 

            return new HeapPage(heapPageId, data);
        } catch (IOException e) {
            throw new IllegalArgumentException("Page not found: " + pid);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    private class HeapFileIterator extends AbstractDbFileIterator {
        private TransactionId tid;
        private int currentPage;
        private Iterator<Tuple> tupleIterator;

        public HeapFileIterator(TransactionId tid) {
          this.tid = tid;
        }

        public void open() throws DbException, TransactionAbortedException {
            currentPage = 0;
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), currentPage), Permissions.READ_ONLY);
            tupleIterator = page.iterator();
        }


        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) return null;

            if (tupleIterator.hasNext()) {
                return tupleIterator.next();
            }

            currentPage++;
            while (currentPage < numPages()) {
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), currentPage), Permissions.READ_ONLY);
                tupleIterator = page.iterator();
                if (tupleIterator.hasNext()) {
                    return tupleIterator.next();
                }
                currentPage++;
            }

            return null;
        }

        public void close() {
            super.close();
            tupleIterator = null;
        }

        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);

    }

}

