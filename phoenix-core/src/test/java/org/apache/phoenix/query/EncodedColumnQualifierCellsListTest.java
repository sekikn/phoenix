/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.tuple.EncodedColumnQualiferCellsList;
import org.apache.phoenix.schema.types.PInteger;
import org.junit.Test;

public class EncodedColumnQualifierCellsListTest {
    
    private static final byte[] row = Bytes.toBytes("row");
    private static final byte[] cf = Bytes.toBytes("cf");

    
    @Test
    public void testIterator() {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        Cell[] cells = new Cell[7];
        int i = 0;
        populateListAndArray(list, cells);
        Iterator itr = list.iterator();
        assertTrue(itr.hasNext());
        
        // test itr.next()
        i = 0;
        while (itr.hasNext()) {
            assertEquals(cells[i++], itr.next());
        }
        
        assertEquals(7, list.size());
        
        // test itr.remove()
        itr = list.iterator();
        i = 0;
        int numRemoved = 0;
        try {
            itr.remove();
            fail("Remove not allowed till next() is called");
        } catch (IllegalStateException expected) {}
        
        while (itr.hasNext()) {
            assertEquals(cells[i++], itr.next());
            itr.remove();
            numRemoved++;
        }
        assertEquals("Number of elements removed should have been the size of the list", 7, numRemoved);
    }
    
    @Test
    public void testSize() {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        assertEquals(0, list.size());
        
        populateList(list);
        
        assertEquals(7, list.size());
        int originalSize = list.size();
        
        Iterator itr = list.iterator();
        while (itr.hasNext()) {
            itr.next();
            itr.remove();
            assertEquals(--originalSize, list.size());
        }
    }
    
    @Test
    public void testIsEmpty() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        assertTrue(list.isEmpty());
        populateList(list);
        assertFalse(list.isEmpty());
        Iterator itr = list.iterator();
        while (itr.hasNext()) {
            itr.next();
            itr.remove();
            if (itr.hasNext()) {
                assertFalse(list.isEmpty());
            }
        }
        assertTrue(list.isEmpty());
    }
    
    @Test
    public void testContains() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        Cell[] cells = new Cell[7];
        populateListAndArray(list, cells);
        
        for (Cell c : cells) {
            assertTrue(list.contains(c));
        }
        assertFalse(list.contains(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(13))));
    }
    
    @Test
    public void testToArrayWithParam() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        Cell[] cells = new Cell[7];
        populateListAndArray(list, cells);
        Cell[] array = list.toArray(new Cell[0]);
        assertTrue(Arrays.equals(cells, array));
    }
    
    @Test
    public void testToArrayWithoutParam() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        Cell[] cells = new Cell[7];
        populateListAndArray(list, cells);
        Object[] array = list.toArray();
        assertTrue(Arrays.equals(cells, array));
    }
    
    @Test
    public void testRemove() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        Cell[] cells = new Cell[7];
        populateListAndArray(list, cells);
        assertTrue(list.remove(cells[0]));
        assertEquals(6, list.size());
        assertTrue(list.remove(cells[6]));
        assertEquals(5, list.size());
        assertTrue(list.remove(cells[3]));
        assertEquals(4, list.size());
        assertFalse(list.remove(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(13))));
        assertEquals(4, list.size());
    }
    
    @Test
    public void testContainsAll() throws Exception {
        EncodedColumnQualiferCellsList list1 = new EncodedColumnQualiferCellsList(11, 16);
        populateList(list1);
        EncodedColumnQualiferCellsList list2 = new EncodedColumnQualiferCellsList(11, 16);
        populateList(list2);
        assertTrue(list1.containsAll(list2));
        list2.remove(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(11)));
        assertTrue(list1.containsAll(list2));
        assertFalse(list2.containsAll(list1));
        list2.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(13)));
        assertFalse(list1.containsAll(list2));
        assertFalse(list2.containsAll(list1));
        List<Cell> arrayList = new ArrayList<>();
        populateList(arrayList);
        assertTrue(list1.containsAll(arrayList));
    }
    
    @Test
    public void testAddAll() throws Exception {
        EncodedColumnQualiferCellsList list1 = new EncodedColumnQualiferCellsList(11, 16);
        populateList(list1);
        EncodedColumnQualiferCellsList list2 = new EncodedColumnQualiferCellsList(11, 16);
        populateList(list2);
        /* 
         * Note that we don't care about equality of the element being added with the element already
         * present at the index.
         */
        assertTrue(list1.addAll(list2));
    }
    
    @Test
    public void testAddAllAtIndexFails() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        populateList(list);
        try {
            list.addAll(0, new ArrayList<Cell>());
        } catch (UnsupportedOperationException expected) {
        }
    }
    
    @Test
    public void testRemoveAll() throws Exception {
        EncodedColumnQualiferCellsList list1 = new EncodedColumnQualiferCellsList(11, 16);
        populateList(list1);
        ArrayList<Cell> list2 = new ArrayList<>();
        populateList(list2);
        assertTrue(list1.removeAll(list2));
        assertTrue(list1.isEmpty());
        assertFalse(list2.isEmpty());
    }
    
    @Test
    public void testRetainAll() throws Exception {
        EncodedColumnQualiferCellsList list1 = new EncodedColumnQualiferCellsList(11, 16);
        populateList(list1);
        EncodedColumnQualiferCellsList list2 = new EncodedColumnQualiferCellsList(11, 16);
        populateList(list2);
        // retainAll won't be modifying the list1 since they both have the same elements equality wise
        assertFalse(list1.retainAll(list2));
        list2.remove(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(12)));
        assertTrue(list1.retainAll(list2));
        assertEquals(list1.size(), list2.size());
        for (Cell c : list1) {
            assertTrue(list2.contains(c));
        }
    }
    
    @Test
    public void testClear() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        populateList(list);
        list.clear();
        assertTrue(list.isEmpty());
        assertEquals(0, list.size());
    }
    
    @Test
    public void testGetIndex() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        Cell[] cells = new Cell[7];
        populateListAndArray(list, cells);
        for (int i = 0; i < cells.length; i++) {
            assertEquals(cells[i], list.get(i));
        }
    }
    
    @Test
    public void testIndexOf() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        Cell[] cells = new Cell[7];
        populateListAndArray(list, cells);
        for (int i = 0; i < cells.length; i++) {
            assertEquals(i, list.indexOf(cells[i]));
        }
    }
    
    @Test
    public void testLastIndexOf() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        Cell[] cells = new Cell[7];
        populateListAndArray(list, cells);
        for (int i = 0; i < cells.length; i++) {
            assertEquals(i, list.lastIndexOf(cells[i]));
        }
    }
    
    @Test
    public void testListIterator() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        Cell[] cells = new Cell[7];
        int i = 0;
        populateListAndArray(list, cells);
        ListIterator<Cell> itr = list.listIterator();
        assertTrue(itr.hasNext());
        
        // test itr.next()
        i = 0;
        while (itr.hasNext()) {
            assertEquals(cells[i++], itr.next());
        }
        
        assertEquals(7, list.size());
        
        // test itr.remove()
        itr = list.listIterator();
        i = 0;
        int numRemoved = 0;
        try {
            itr.remove();
            fail("Remove not allowed till next() is called");
        } catch (IllegalStateException expected) {}
        
        while (itr.hasNext()) {
            assertEquals(cells[i++], itr.next());
            itr.remove();
            numRemoved++;
        }
        assertEquals("Number of elements removed should have been the size of the list", 7, numRemoved);
        assertTrue(list.isEmpty());
    }
    
    @Test
    public void testListIteratorSet() {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        Cell[] array = new Cell[7];
        populateListAndArray(list, array);
        ListIterator<Cell> itr = list.listIterator();
        // This cell is KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(12))
        final Cell validCell = array[4];
        // This cell is KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(14))
        final Cell invalidCell = array[5];
        String validCellName = "Valid Cell";
        String invalidCellName = "Invalid Cell";
        Cell validReplacementCell = new DelegateCell(validCell, validCellName);
        Cell invalidReplacementCell = new DelegateCell(invalidCell, invalidCellName);
        int i = 0;
        while (itr.hasNext()) {
            Cell c = itr.next();
            if (i == 4) {
                itr.set(validReplacementCell);
            }
            if (i == 6) {
                try {
                    itr.set(invalidReplacementCell);
                    fail("This should have failed since " + invalidReplacementCell + " cannot be added where " + c + " is.");
                } catch (IllegalArgumentException expected) {
                }
            }
            i++;
        }
        itr = list.listIterator();
        i = 0;
        // Assert that the valid cell was added and invalid cell wasn't.
        while (itr.hasNext()) {
            Cell c = itr.next();
            if (i == 4) {
                assertEquals(validCellName, c.toString());
            }
            if (i == 6) {
                assertNotEquals(invalidCellName, c.toString());
            }
            i++;
        }
    }
    
    @Test
    public void testListIteratorNextAndPrevious()  throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        Cell[] array = new Cell[7];
        populateListAndArray(list, array);
        ListIterator<Cell> itr = list.listIterator();
        try {
            itr.previous();
            fail("Call to itr.previous() should have failed since the iterator hasn't been moved forward yet");
        } catch (NoSuchElementException expected) {
            
        }
        Cell c = itr.next();
        Cell d = itr.previous();
        Cell e = itr.next();
        Cell f = itr.previous();
        assertTrue(c.equals(d) && c.equals(f) && c.equals(e));
        itr = list.listIterator();
        int i = 0;
        assertEquals(array[i++], itr.next());
        assertEquals(array[i++], itr.next()); 
        assertEquals(array[i++], itr.next());
        assertEquals(array[--i], itr.previous());
        assertEquals(array[--i], itr.previous());
        assertEquals(array[i++], itr.next());
        
        // move itr forward till next() is exhausted
        while (itr.hasNext()) {
            itr.next();
        }
        i = 6;
        while (itr.hasPrevious()) {
            assertEquals(array[i--], itr.previous());
        }
        assertEquals("Not all elements navigated using previous()", -1, i);
        // now that previous is exhausted, move itr() forward till next() is exhausted
        i = 0;
        while (itr.hasNext()) {
            assertEquals(array[i++], itr.next());
        }
        assertEquals("Not all elements navigated using next()", 7, i);
    }
    
    @Test
    public void testSetNull() throws Exception {
        EncodedColumnQualiferCellsList list = new EncodedColumnQualiferCellsList(11, 16);
        try {
            list.add(null);
            fail("Adding null elements to the list is not allowed");
        } catch (NullPointerException expected) {
            
        }
    }
    
    private void populateListAndArray(List<Cell> list, Cell[] cells) {
        // add elements in reserved range
        list.add(cells[0] = KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(0)));
        list.add(cells[1] = KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(5)));
        list.add(cells[2] = KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(10)));

        // add elements in qualifier range
        list.add(cells[6] = KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(16)));
        list.add(cells[4] = KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(12)));
        list.add(cells[5] = KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(14)));
        list.add(cells[3] = KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(11)));
    }

    private void populateList(List<Cell> list) {
        // add elements in reserved range
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(0)));
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(5)));
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(10)));

        // add elements in qualifier range
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(16)));
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(12)));
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(14)));
        list.add(KeyValue.createFirstOnRow(row, cf, PInteger.INSTANCE.toBytes(11)));
    }
    
    private class DelegateCell implements Cell {
        private final Cell delegate;
        private final String name;
        public DelegateCell(Cell delegate, String name) {
            this.delegate = delegate;
            this.name = name;
        }

        @Override
        public int getValueOffset() {
            return delegate.getValueOffset();
        }

        @Override
        public int getValueLength() {
            return delegate.getValueLength();
        }

        @Override
        public byte[] getValueArray() {
            return delegate.getValueArray();
        }

        @Override
        public byte[] getValue() {
            return delegate.getValue();
        }

        @Override
        public byte getTypeByte() {
            return delegate.getTypeByte();
        }

        @Override
        public long getTimestamp() {
            return delegate.getTimestamp();
        }

        @Override
        public int getTagsOffset() {
            return delegate.getTagsOffset();
        }

        @Override
        public int getTagsLengthUnsigned() {
            return delegate.getTagsLengthUnsigned();
        }

        @Override
        public short getTagsLength() {
            return delegate.getTagsLength();
        }

        @Override
        public byte[] getTagsArray() {
            return delegate.getTagsArray();
        }

        @Override
        public int getRowOffset() {
            return delegate.getRowOffset();
        }

        @Override
        public short getRowLength() {
            return delegate.getRowLength();
        }

        @Override
        public byte[] getRowArray() {
            return delegate.getRowArray();
        }

        @Override
        public byte[] getRow() {
            return delegate.getRow();
        }

        @Override
        public int getQualifierOffset() {
            return delegate.getQualifierOffset();
        }

        @Override
        public int getQualifierLength() {
            return delegate.getQualifierLength();
        }

        @Override
        public byte[] getQualifierArray() {
            return delegate.getQualifierArray();
        }

        @Override
        public byte[] getQualifier() {
            return delegate.getQualifier();
        }

        @Override
        public long getMvccVersion() {
            return delegate.getMvccVersion();
        }

        @Override
        public int getFamilyOffset() {
            return delegate.getFamilyOffset();
        }

        @Override
        public byte getFamilyLength() {
            return delegate.getFamilyLength();
        }

        @Override
        public byte[] getFamilyArray() {
            return delegate.getFamilyArray();
        }

        @Override
        public byte[] getFamily() {
            return delegate.getFamily();
        }
        
        @Override
        public String toString() {
            return name;
        }

    }
    
}
