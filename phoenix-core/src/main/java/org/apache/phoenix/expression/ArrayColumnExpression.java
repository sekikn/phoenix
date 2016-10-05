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
package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.EncodedColumnsUtil;

/**
 * 
 * Class to access a column that is stored in a KeyValue that contains all
 * columns for a given column family (stored in an array)
 *
 */
public class ArrayColumnExpression extends ColumnExpression {
    
    private String displayName; // client-side only
    private int index;
    // expression that represents the array (where all cols are stored in a single key value)
    private KeyValueColumnExpression arrayExpression;
    // expression that represents this column if (it were stored as a regular key value) 
    private KeyValueColumnExpression origKVExpression;
    
    public ArrayColumnExpression() {
    }
    
    public ArrayColumnExpression(PDatum column, byte[] cf, int index) {
        super(column);
        this.index = index;
        this.arrayExpression = new KeyValueColumnExpression(column, cf, cf);
    }
    
    public ArrayColumnExpression(PColumn column, String displayName, boolean encodedColumnName) {
        super(column);
        // array indexes are 1-based TODO: samarth think about the case when the encodedcolumn qualifier is null. Probably best to add a check here for encodedcolumnname to be true
        this.index = column.getEncodedColumnQualifier() + 1;
        byte[] cf = column.getFamilyName().getBytes();
        this.arrayExpression = new KeyValueColumnExpression(column, cf, cf);
        this.origKVExpression = new KeyValueColumnExpression(column, displayName, encodedColumnName);
        this.displayName = displayName;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        return PArrayDataType.positionAtArrayElement(tuple, ptr, index, arrayExpression, PVarbinary.INSTANCE, null);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        index = WritableUtils.readVInt(input);
        arrayExpression = new KeyValueColumnExpression();
        arrayExpression.readFields(input);
        origKVExpression = new KeyValueColumnExpression();
        origKVExpression.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, index);
        arrayExpression.write(output);
        origKVExpression.write(output);
    }
    
    public KeyValueColumnExpression getArrayExpression() {
        return arrayExpression;
    }
    
    public KeyValueColumnExpression getKeyValueExpression() {
        return origKVExpression;
    }
    
    @Override
    public String toString() {
        return displayName;
    }

}
