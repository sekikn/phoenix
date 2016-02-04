package org.apache.phoenix.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Implementation of Calcite's {@link Schema} SPI for Phoenix.
 * 
 * TODO
 * 1) change this to non-caching mode??
 * 2) how to deal with define indexes and views since they require a CalciteSchema
 *    instance??
 *
 */
public class PhoenixSchema implements Schema {
    public static final Factory FACTORY = new Factory();

    public final PhoenixConnection pc;
    
    protected final String name;
    protected final String schemaName;
    protected final SchemaPlus parentSchema;
    protected final MetaDataClient client;
    
    protected final Map<String, Schema> subSchemas;
    protected final Map<String, Table> tables;
    protected final Map<String, Function> views;
    protected final Set<PTable> viewTables;
    
    protected PhoenixSchema(String name, String schemaName,
            SchemaPlus parentSchema, PhoenixConnection pc) {
        this.name = name;
        this.schemaName = schemaName;
        this.parentSchema = parentSchema;
        this.pc = pc;
        this.client = new MetaDataClient(pc);
        this.subSchemas = Maps.newHashMap();
        this.tables = Maps.newHashMap();
        this.views = Maps.newHashMap();
        this.viewTables = Sets.newHashSet();
    }

    private static Schema create(SchemaPlus parentSchema,
            String name, Map<String, Object> operand) {
        String url = (String) operand.get("url");
        final Properties properties = new Properties();
        for (Map.Entry<String, Object> entry : operand.entrySet()) {
            properties.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
        }
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            final Connection connection =
                DriverManager.getConnection(url, properties);
            final PhoenixConnection phoenixConnection =
                connection.unwrap(PhoenixConnection.class);
            return new PhoenixSchema(name, null, parentSchema, phoenixConnection);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Table getTable(String name) {
        Table table = tables.get(name);
        if (table != null) {
            return table;
        }
        
        try {
            ColumnResolver x = FromCompiler.getResolver(
                    NamedTableNode.create(
                            null,
                            TableName.create(schemaName, name),
                            ImmutableList.<ColumnDef>of()), pc);
            final List<TableRef> tables = x.getTables();
            assert tables.size() == 1;
            PTable pTable = tables.get(0).getTable();
            if (!isView(pTable)) {
                pTable = fixTableMultiTenancy(pTable);
                table = new PhoenixTable(pc, pTable);
            }
        } catch (TableNotFoundException e) {
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        if (table == null) {
            table = resolveSequence(name);
        }
        
        if (table != null) {
            tables.put(name, table);
        }
        return table;
    }

    @Override
    public Set<String> getTableNames() {
        return tables.keySet();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        Function func = views.get(name);
        if (func != null) {
            return ImmutableList.of(func);
        }
        
        try {
            ColumnResolver x = FromCompiler.getResolver(
                    NamedTableNode.create(
                            null,
                            TableName.create(schemaName, name),
                            ImmutableList.<ColumnDef>of()), pc);
            final List<TableRef> tables = x.getTables();
            assert tables.size() == 1;
            PTable pTable = tables.get(0).getTable();
            if (isView(pTable)) {
                String viewSql = pTable.getViewStatement();
                if (viewSql == null) {
                    viewSql = "select * from "
                            + SchemaUtil.getEscapedFullTableName(
                                    pTable.getPhysicalName().getString());
                }
                SchemaPlus schema = parentSchema.getSubSchema(this.name);
                SchemaPlus viewSqlSchema =
                        this.schemaName == null ? schema : parentSchema;
                func = ViewTable.viewMacro(schema, viewSql,
                        CalciteSchema.from(viewSqlSchema).path(null),
                        pTable.getViewType() == ViewType.UPDATABLE);
                views.put(name, func);
                viewTables.add(pTable);
            }
        } catch (TableNotFoundException e) {
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        return func == null ? Collections.<Function>emptyList() : ImmutableList.of(func);
    }

    @Override
    public Set<String> getFunctionNames() {
        return views.keySet();
    }

    @Override
    public Schema getSubSchema(String name) {
        if (schemaName != null) {
            return null;
        }
        
        Schema schema = subSchemas.get(name);
        if (schema != null) {
            return schema;
        }
        
        schema = new PhoenixSchema(name, name, parentSchema.getSubSchema(this.name), pc);
        subSchemas.put(name, schema);
        return schema;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return subSchemas.keySet();
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public boolean contentsHaveChangedSince(long lastCheck, long now) {
        return false;
    }
    
    public void defineIndexesAsMaterializations() {
        SchemaPlus schema = parentSchema.getSubSchema(this.name);
        SchemaPlus viewSqlSchema =
                this.schemaName == null ? schema : parentSchema;
        CalciteSchema calciteSchema = CalciteSchema.from(schema);
        List<String> path = CalciteSchema.from(viewSqlSchema).path(null);
        try {
            for (Table table : tables.values()) {
                if (table instanceof PhoenixTable) {
                    PTable pTable = ((PhoenixTable) table).pTable;
                    for (PTable index : pTable.getIndexes()) {
                        addMaterialization(index, path, calciteSchema);
                    }
                }
            }
            for (PTable pTable : viewTables) {
                for (PTable index : pTable.getIndexes()) {
                    if (index.getParentName().equals(pTable.getName())) {
                        addMaterialization(index, path, calciteSchema);
                    }
                }                
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void addMaterialization(PTable index, List<String> path,
            CalciteSchema calciteSchema) throws SQLException {
        index = fixTableMultiTenancy(index);
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT");
        for (PColumn column : PhoenixTable.getMappedColumns(index)) {
            String indexColumnName = column.getName().getString();
            String dataColumnName = IndexUtil.getDataColumnName(indexColumnName);
            sb.append(",").append(SchemaUtil.getEscapedFullColumnName(dataColumnName));
            sb.append(" ").append(SchemaUtil.getEscapedFullColumnName(indexColumnName));
        }
        sb.setCharAt(6, ' '); // replace first comma with space.
        sb.append(" FROM ").append(SchemaUtil.getEscapedFullTableName(index.getParentName().getString()));
        MaterializationService.instance().defineMaterialization(
                calciteSchema, null, sb.toString(), path, index.getTableName().getString(), true, true);        
    }
    
    private boolean isView(PTable table) {
        return table.getType() == PTableType.VIEW
                && table.getViewType() != ViewType.MAPPED;
    }
    
    private PTable fixTableMultiTenancy(PTable table) throws SQLException {
        if (pc.getTenantId() != null || !table.isMultiTenant()) {
            return table;
        }
        return PTableImpl.makePTable(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), table.getTimeStamp(),
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), PTableImpl.getColumnsToClone(table), table.getParentSchemaName(), table.getParentTableName(),
                table.getIndexes(), table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                table.isWALDisabled(), false, table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(),
                table.rowKeyOrderOptimizable(), table.isTransactional(), table.getUpdateCacheFrequency(), table.getTableStats(), table.getBaseColumnCount());
    }
    
    private PhoenixSequence resolveSequence(String name) {
        try {
            // FIXME: Do this the same way as resolving a table after PHOENIX-2489.
            String tenantId = pc.getTenantId() == null ? null : pc.getTenantId().getString();
            String q = "select 1 from " + PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_ESCAPED
                    + " where " + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA
                    + (schemaName == null ? " is null" : " = '" + schemaName + "'")
                    + " and " + PhoenixDatabaseMetaData.SEQUENCE_NAME
                    + " = '" + name + "'"
                    + " and " + PhoenixDatabaseMetaData.TENANT_ID
                    + (tenantId == null ? " is null" : " = '" + tenantId + "'");
            ResultSet rs = pc.createStatement().executeQuery(q);
            if (rs.next()) {
                return new PhoenixSequence(schemaName, name, pc);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        return null;
    }

    /** Schema factory that creates a
     * {@link org.apache.phoenix.calcite.PhoenixSchema}.
     * This allows you to create a Phoenix schema inside a model.json file.
     *
     * <pre>{@code
     * {
     *   version: '1.0',
     *   defaultSchema: 'HR',
     *   schemas: [
     *     {
     *       name: 'HR',
     *       type: 'custom',
     *       factory: 'org.apache.phoenix.calcite.PhoenixSchema.Factory',
     *       operand: {
     *         url: "jdbc:phoenix:localhost",
     *         user: "scott",
     *         password: "tiger"
     *       }
     *     }
     *   ]
     * }
     * }</pre>
     */
    public static class Factory implements SchemaFactory {
        public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
            return PhoenixSchema.create(parentSchema, name, operand);
        }
    }
}