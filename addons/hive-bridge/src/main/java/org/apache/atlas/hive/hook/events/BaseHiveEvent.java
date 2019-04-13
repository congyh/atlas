/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hive.hook.events;

import org.apache.atlas.hive.hook.AtlasHiveHookContext;
import org.apache.atlas.hive.hook.HiveLineageTableInfo;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.utils.HdfsNameServiceResolver;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DependencyKey;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.hive.hook.AtlasHiveHookContext.QNAME_SEP_CLUSTER_NAME;
import static org.apache.atlas.hive.hook.AtlasHiveHookContext.QNAME_SEP_ENTITY_NAME;
import static org.apache.atlas.hive.hook.AtlasHiveHookContext.QNAME_SEP_PROCESS;

public abstract class BaseHiveEvent {
    private static final Logger LOG = LoggerFactory.getLogger(BaseHiveEvent.class);

    public static final String HIVE_TYPE_DB             = "hive_db";
    public static final String HIVE_TYPE_TABLE          = "hive_table";
    public static final String HIVE_TYPE_STORAGEDESC    = "hive_storagedesc";
    public static final String HIVE_TYPE_COLUMN         = "hive_column";
    public static final String HIVE_TYPE_PROCESS        = "hive_process";
    public static final String HIVE_TYPE_COLUMN_LINEAGE = "hive_column_lineage";
    public static final String HIVE_TYPE_SERDE          = "hive_serde";
    public static final String HIVE_TYPE_ORDER          = "hive_order";
    public static final String HDFS_TYPE_PATH           = "hdfs_path";
    public static final String HBASE_TYPE_TABLE         = "hbase_table";
    public static final String HBASE_TYPE_NAMESPACE     = "hbase_namespace";
    public static final String AWS_S3_BUCKET            = "aws_s3_bucket";
    public static final String AWS_S3_PSEUDO_DIR        = "aws_s3_pseudo_dir";
    public static final String AWS_S3_OBJECT            = "aws_s3_object";

    public static final String SCHEME_SEPARATOR         = "://";
    public static final String S3_SCHEME                = "s3" + SCHEME_SEPARATOR;
    public static final String S3A_SCHEME               = "s3a" + SCHEME_SEPARATOR;

    public static final String ATTRIBUTE_QUALIFIED_NAME            = "qualifiedName";
    public static final String ATTRIBUTE_NAME                      = "name";
    public static final String ATTRIBUTE_DESCRIPTION               = "description";
    public static final String ATTRIBUTE_OWNER                     = "owner";
    public static final String ATTRIBUTE_CLUSTER_NAME              = "clusterName";
    public static final String ATTRIBUTE_LOCATION                  = "location";
    public static final String ATTRIBUTE_PARAMETERS                = "parameters";
    public static final String ATTRIBUTE_OWNER_TYPE                = "ownerType";
    public static final String ATTRIBUTE_COMMENT                   = "comment";
    public static final String ATTRIBUTE_CREATE_TIME               = "createTime";
    public static final String ATTRIBUTE_LAST_ACCESS_TIME          = "lastAccessTime";
    public static final String ATTRIBUTE_VIEW_ORIGINAL_TEXT        = "viewOriginalText";
    public static final String ATTRIBUTE_VIEW_EXPANDED_TEXT        = "viewExpandedText";
    public static final String ATTRIBUTE_TABLE_TYPE                = "tableType";
    public static final String ATTRIBUTE_TEMPORARY                 = "temporary";
    public static final String ATTRIBUTE_RETENTION                 = "retention";
    public static final String ATTRIBUTE_DB                        = "db";
    public static final String ATTRIBUTE_STORAGEDESC               = "sd";
    public static final String ATTRIBUTE_PARTITION_KEYS            = "partitionKeys";
    public static final String ATTRIBUTE_COLUMNS                   = "columns";
    public static final String ATTRIBUTE_INPUT_FORMAT              = "inputFormat";
    public static final String ATTRIBUTE_OUTPUT_FORMAT             = "outputFormat";
    public static final String ATTRIBUTE_COMPRESSED                = "compressed";
    public static final String ATTRIBUTE_BUCKET_COLS               = "bucketCols";
    public static final String ATTRIBUTE_NUM_BUCKETS               = "numBuckets";
    public static final String ATTRIBUTE_STORED_AS_SUB_DIRECTORIES = "storedAsSubDirectories";
    public static final String ATTRIBUTE_TABLE                     = "table";
    public static final String ATTRIBUTE_SERDE_INFO                = "serdeInfo";
    public static final String ATTRIBUTE_SERIALIZATION_LIB         = "serializationLib";
    public static final String ATTRIBUTE_SORT_COLS                 = "sortCols";
    public static final String ATTRIBUTE_COL_TYPE                  = "type";
    public static final String ATTRIBUTE_COL_POSITION              = "position";
    public static final String ATTRIBUTE_PATH                      = "path";
    public static final String ATTRIBUTE_NAMESERVICE_ID            = "nameServiceId";
    public static final String ATTRIBUTE_INPUTS                    = "inputs";
    public static final String ATTRIBUTE_OUTPUTS                   = "outputs";
    public static final String ATTRIBUTE_OPERATION_TYPE            = "operationType";
    public static final String ATTRIBUTE_START_TIME                = "startTime";
    public static final String ATTRIBUTE_USER_NAME                 = "userName";
    public static final String ATTRIBUTE_QUERY_TEXT                = "queryText";
    public static final String ATTRIBUTE_QUERY_ID                  = "queryId";
    public static final String ATTRIBUTE_QUERY_PLAN                = "queryPlan";
    public static final String ATTRIBUTE_END_TIME                  = "endTime";
    public static final String ATTRIBUTE_RECENT_QUERIES            = "recentQueries";
    public static final String ATTRIBUTE_QUERY                     = "query";
    public static final String ATTRIBUTE_DEPENDENCY_TYPE           = "depenendencyType";
    public static final String ATTRIBUTE_EXPRESSION                = "expression";
    public static final String ATTRIBUTE_ALIASES                   = "aliases";
    public static final String ATTRIBUTE_URI                       = "uri";
    public static final String ATTRIBUTE_STORAGE_HANDLER           = "storage_handler";
    public static final String ATTRIBUTE_NAMESPACE                 = "namespace";
    public static final String ATTRIBUTE_OBJECT_PREFIX             = "objectPrefix";
    public static final String ATTRIBUTE_BUCKET                    = "bucket";

    public static final String HBASE_STORAGE_HANDLER_CLASS         = "org.apache.hadoop.hive.hbase.HBaseStorageHandler";
    public static final String HBASE_DEFAULT_NAMESPACE             = "default";
    public static final String HBASE_NAMESPACE_TABLE_DELIMITER     = ":";
    public static final String HBASE_PARAM_TABLE_NAME              = "hbase.table.name";
    public static final long   MILLIS_CONVERT_FACTOR               = 1000;

    public static final Map<Integer, String> OWNER_TYPE_TO_ENUM_VALUE = new HashMap<>();


    static {
        OWNER_TYPE_TO_ENUM_VALUE.put(1, "USER");
        OWNER_TYPE_TO_ENUM_VALUE.put(2, "ROLE");
        OWNER_TYPE_TO_ENUM_VALUE.put(3, "GROUP");
    }

    protected final AtlasHiveHookContext context;


    protected BaseHiveEvent(AtlasHiveHookContext context) {
        this.context = context;
    }

    public AtlasHiveHookContext getContext() {
        return context;
    }

    public List<HookNotification> getNotificationMessages() throws Exception {
        return null;
    }

    public static long getTableCreateTime(Table table) {
        return table.getTTable() != null ? (table.getTTable().getCreateTime() * MILLIS_CONVERT_FACTOR) : System.currentTimeMillis();
    }

    public static String getTableOwner(Table table) {
        return table.getTTable() != null ? (table.getOwner()): "";
    }

    public static AtlasObjectId getObjectId(AtlasEntity entity) {
        String        qualifiedName = (String) entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME);
        AtlasObjectId ret           = new AtlasObjectId(entity.getGuid(), entity.getTypeName(), Collections.singletonMap(ATTRIBUTE_QUALIFIED_NAME, qualifiedName));

        return ret;
    }

    public static List<AtlasObjectId> getObjectIds(List<AtlasEntity> entities) {
        final List<AtlasObjectId> ret;

        if (CollectionUtils.isNotEmpty(entities)) {
            ret = new ArrayList<>(entities.size());

            for (AtlasEntity entity : entities) {
                ret.add(getObjectId(entity));
            }
        } else {
            ret = Collections.emptyList();
        }

        return ret;
    }


    protected void addProcessedEntities(AtlasEntitiesWithExtInfo entitiesWithExtInfo) {
        for (AtlasEntity entity : context.getEntities()) {
            entitiesWithExtInfo.addReferredEntity(entity);
        }

        entitiesWithExtInfo.compact();

        context.addToKnownEntities(entitiesWithExtInfo.getEntities());

        if (entitiesWithExtInfo.getReferredEntities() != null) {
            context.addToKnownEntities(entitiesWithExtInfo.getReferredEntities().values());
        }
    }

    protected AtlasEntity getInputOutputEntity(Entity entity, AtlasEntityExtInfo entityExtInfo) throws Exception {
        AtlasEntity ret = null;

        switch(entity.getType()) {
            case TABLE:
            case PARTITION:
            case DFS_DIR: {
                ret = toAtlasEntity(entity, entityExtInfo);
            }
            break;
        }

        return ret;
    }

    protected AtlasEntity toAtlasEntity(Entity entity, AtlasEntityExtInfo entityExtInfo) throws Exception {
        AtlasEntity ret = null;

        switch (entity.getType()) {
            case DATABASE: {
                Database db = getHive().getDatabase(entity.getDatabase().getName());

                ret = toDbEntity(db);
            }
            break;

            case TABLE:
            case PARTITION: {
                Table table = getHive().getTable(entity.getTable().getDbName(), entity.getTable().getTableName());

                ret = toTableEntity(table, entityExtInfo);
            }
            break;

            case DFS_DIR: {
                URI location = entity.getLocation();

                if (location != null) {
                    ret = getPathEntity(new Path(entity.getLocation()), entityExtInfo);
                }
            }
            break;

            default:
            break;
        }

        return ret;
    }

    protected AtlasEntity toDbEntity(Database db) throws Exception {
        String  dbQualifiedName = getQualifiedName(db);
        boolean isKnownDatabase = context.isKnownDatabase(dbQualifiedName);

        AtlasEntity ret = context.getEntity(dbQualifiedName);

        if (ret == null) {
            ret = new AtlasEntity(HIVE_TYPE_DB);

            // if this DB was sent in an earlier notification, set 'guid' to null - which will:
            //  - result in this entity to be not included in 'referredEntities'
            //  - cause Atlas server to resolve the entity by its qualifiedName
            if (isKnownDatabase) {
                ret.setGuid(null);
            }

            ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, dbQualifiedName);
            ret.setAttribute(ATTRIBUTE_NAME, db.getName().toLowerCase());
            ret.setAttribute(ATTRIBUTE_DESCRIPTION, db.getDescription());
            ret.setAttribute(ATTRIBUTE_OWNER, db.getOwnerName());

            ret.setAttribute(ATTRIBUTE_CLUSTER_NAME, getClusterName());
            ret.setAttribute(ATTRIBUTE_LOCATION, HdfsNameServiceResolver.getPathWithNameServiceID(db.getLocationUri()));
            ret.setAttribute(ATTRIBUTE_PARAMETERS, db.getParameters());

            if (db.getOwnerType() != null) {
                ret.setAttribute(ATTRIBUTE_OWNER_TYPE, OWNER_TYPE_TO_ENUM_VALUE.get(db.getOwnerType().getValue()));
            }

            context.putEntity(dbQualifiedName, ret);
        }

        return ret;
    }

    protected AtlasEntityWithExtInfo toTableEntity(Table table) throws Exception {
        AtlasEntityWithExtInfo ret = new AtlasEntityWithExtInfo();

        AtlasEntity entity = toTableEntity(table, ret);

        ret.setEntity(entity);

        return ret;
    }

    protected AtlasEntity toTableEntity(Table table, AtlasEntitiesWithExtInfo entities) throws Exception {
        AtlasEntity ret = toTableEntity(table, (AtlasEntityExtInfo) entities);

        entities.addEntity(ret);

        return ret;
    }

    protected AtlasEntity toTableEntity(Table table, AtlasEntityExtInfo entityExtInfo) throws Exception {
        AtlasEntity dbEntity = toDbEntity(getHive().getDatabase(table.getDbName()));

        if (entityExtInfo != null) {
            if (dbEntity != null) {
                entityExtInfo.addReferredEntity(dbEntity);
            }
        }

        AtlasEntity ret = toTableEntity(getObjectId(dbEntity), table, entityExtInfo);

        return ret;
    }

    protected AtlasEntity toTableEntity(AtlasObjectId dbId, Table table, AtlasEntityExtInfo entityExtInfo) throws Exception {
        String  tblQualifiedName = getQualifiedName(table);
        boolean isKnownTable     = context.isKnownTable(tblQualifiedName);

        AtlasEntity ret = context.getEntity(tblQualifiedName);

        if (ret == null) {
            ret = new AtlasEntity(HIVE_TYPE_TABLE);

            // if this table was sent in an earlier notification, set 'guid' to null - which will:
            //  - result in this entity to be not included in 'referredEntities'
            //  - cause Atlas server to resolve the entity by its qualifiedName
            if (isKnownTable && !isAlterTableOperation()) {
                ret.setGuid(null);
            }

            long createTime     = getTableCreateTime(table);
            long lastAccessTime = table.getLastAccessTime() > 0 ? (table.getLastAccessTime() * MILLIS_CONVERT_FACTOR) : createTime;

            ret.setAttribute(ATTRIBUTE_DB, dbId);
            ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, tblQualifiedName);
            ret.setAttribute(ATTRIBUTE_NAME, table.getTableName().toLowerCase());
            ret.setAttribute(ATTRIBUTE_OWNER, table.getOwner());
            ret.setAttribute(ATTRIBUTE_CREATE_TIME, createTime);
            ret.setAttribute(ATTRIBUTE_LAST_ACCESS_TIME, lastAccessTime);
            ret.setAttribute(ATTRIBUTE_RETENTION, table.getRetention());
            ret.setAttribute(ATTRIBUTE_PARAMETERS, table.getParameters());
            ret.setAttribute(ATTRIBUTE_COMMENT, table.getParameters().get(ATTRIBUTE_COMMENT));
            ret.setAttribute(ATTRIBUTE_TABLE_TYPE, table.getTableType().name());
            ret.setAttribute(ATTRIBUTE_TEMPORARY, table.isTemporary());

            if (table.getViewOriginalText() != null) {
                ret.setAttribute(ATTRIBUTE_VIEW_ORIGINAL_TEXT, table.getViewOriginalText());
            }

            if (table.getViewExpandedText() != null) {
                ret.setAttribute(ATTRIBUTE_VIEW_EXPANDED_TEXT, table.getViewExpandedText());
            }

            AtlasObjectId     tableId       = getObjectId(ret);
            AtlasEntity       sd            = getStorageDescEntity(tableId, table);
            List<AtlasEntity> partitionKeys = getColumnEntities(tableId, table, table.getPartitionKeys());
            List<AtlasEntity> columns       = getColumnEntities(tableId, table, table.getCols());

            if (entityExtInfo != null) {
                entityExtInfo.addReferredEntity(sd);

                if (partitionKeys != null) {
                    for (AtlasEntity partitionKey : partitionKeys) {
                        entityExtInfo.addReferredEntity(partitionKey);
                    }
                }

                if (columns != null) {
                    for (AtlasEntity column : columns) {
                        entityExtInfo.addReferredEntity(column);
                    }
                }
            }

            ret.setAttribute(ATTRIBUTE_STORAGEDESC, getObjectId(sd));
            ret.setAttribute(ATTRIBUTE_PARTITION_KEYS, getObjectIds(partitionKeys));
            ret.setAttribute(ATTRIBUTE_COLUMNS, getObjectIds(columns));

            context.putEntity(tblQualifiedName, ret);
        }

        return ret;
    }

    protected AtlasEntity getStorageDescEntity(AtlasObjectId tableId, Table table) {
        String  sdQualifiedName = getQualifiedName(table, table.getSd());
        boolean isKnownTable    = tableId.getGuid() == null;

        AtlasEntity ret = context.getEntity(sdQualifiedName);

        if (ret == null) {
            ret = new AtlasEntity(HIVE_TYPE_STORAGEDESC);

            // if sd's table was sent in an earlier notification, set 'guid' to null - which will:
            //  - result in this entity to be not included in 'referredEntities'
            //  - cause Atlas server to resolve the entity by its qualifiedName
            if (isKnownTable) {
                ret.setGuid(null);
            }

            StorageDescriptor sd = table.getSd();

            ret.setAttribute(ATTRIBUTE_TABLE, tableId);
            ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, sdQualifiedName);
            ret.setAttribute(ATTRIBUTE_PARAMETERS, sd.getParameters());
            ret.setAttribute(ATTRIBUTE_LOCATION, HdfsNameServiceResolver.getPathWithNameServiceID(sd.getLocation()));
            ret.setAttribute(ATTRIBUTE_INPUT_FORMAT, sd.getInputFormat());
            ret.setAttribute(ATTRIBUTE_OUTPUT_FORMAT, sd.getOutputFormat());
            ret.setAttribute(ATTRIBUTE_COMPRESSED, sd.isCompressed());
            ret.setAttribute(ATTRIBUTE_NUM_BUCKETS, sd.getNumBuckets());
            ret.setAttribute(ATTRIBUTE_STORED_AS_SUB_DIRECTORIES, sd.isStoredAsSubDirectories());

            if (sd.getBucketCols().size() > 0) {
                ret.setAttribute(ATTRIBUTE_BUCKET_COLS, sd.getBucketCols());
            }

            if (sd.getSerdeInfo() != null) {
                AtlasStruct serdeInfo   = new AtlasStruct(HIVE_TYPE_SERDE);
                SerDeInfo   sdSerDeInfo = sd.getSerdeInfo();

                serdeInfo.setAttribute(ATTRIBUTE_NAME, sdSerDeInfo.getName());
                serdeInfo.setAttribute(ATTRIBUTE_SERIALIZATION_LIB, sdSerDeInfo.getSerializationLib());
                serdeInfo.setAttribute(ATTRIBUTE_PARAMETERS, sdSerDeInfo.getParameters());

                ret.setAttribute(ATTRIBUTE_SERDE_INFO, serdeInfo);
            }

            if (CollectionUtils.isNotEmpty(sd.getSortCols())) {
                List<AtlasStruct> sortCols = new ArrayList<>(sd.getSortCols().size());

                for (Order sdSortCol : sd.getSortCols()) {
                    AtlasStruct sortcol = new AtlasStruct(HIVE_TYPE_ORDER);

                    sortcol.setAttribute("col", sdSortCol.getCol());
                    sortcol.setAttribute("order", sdSortCol.getOrder());

                    sortCols.add(sortcol);
                }

                ret.setAttribute(ATTRIBUTE_SORT_COLS, sortCols);
            }

            context.putEntity(sdQualifiedName, ret);
        }

        return ret;
    }

    protected List<AtlasEntity> getColumnEntities(AtlasObjectId tableId, Table table, List<FieldSchema> fieldSchemas) {
        List<AtlasEntity> ret          = new ArrayList<>();
        boolean           isKnownTable = tableId.getGuid() == null;

        // 预留1000个字段上限
        int positionInterval = 1000;
        for (int i = 0; i < fieldSchemas.size(); i++) {

            HiveLineageTableInfo lineageTableInfo = context.getHiveLineageTableInfo();
            FieldSchema fieldSchema = fieldSchemas.get(i);
            List<String> colQualifiedNames = getQualifiedName(table, fieldSchema);

            for (int j = 0; j < colQualifiedNames.size(); j++) {
                String colQualifiedName = colQualifiedNames.get(j);
                AtlasEntity column = context.getEntity(colQualifiedName);

                if (column == null) { // 全局entityMap中自动对partition列进行了去重操作
                    column = new AtlasEntity(HIVE_TYPE_COLUMN);

                    // if column's table was sent in an earlier notification, set 'guid' to null - which will:
                    //  - result in this entity to be not included in 'referredEntities'
                    //  - cause Atlas server to resolve the entity by its qualifiedName
                    if (isKnownTable) {
                        column.setGuid(null);
                    }

                    column.setAttribute(ATTRIBUTE_TABLE, tableId);
                    column.setAttribute(ATTRIBUTE_QUALIFIED_NAME, colQualifiedName);

                    String colName = null;

                    // 如果有血缘分区列
                    if (lineageTableInfo.isLineagePartitioned(table)) {
                        String lineagePartitionName = lineageTableInfo.getLineagePartitionName(table);
                        List<String> lineagePartitionValues = lineageTableInfo.getLineagePartitionValues(table);
                        List<FieldSchema> partCols = table.getPartCols();

                        if (partCols.contains(fieldSchema)) { // 如果是分区列, 不改写名字.
                            colName = fieldSchema.getName();
                        } else {
                            colName = getRewrittenColumnName(
                                    fieldSchema.getName(), lineagePartitionName, lineagePartitionValues.get(j));
                        }
                    } else {
                        colName = fieldSchema.getName();
                    }

                    column.setAttribute(ATTRIBUTE_NAME, colName);
                    column.setAttribute(ATTRIBUTE_OWNER, table.getOwner());
                    column.setAttribute(ATTRIBUTE_COL_TYPE, fieldSchema.getType());

                    column.setAttribute(ATTRIBUTE_COL_POSITION, i + positionInterval * j);
                    column.setAttribute(ATTRIBUTE_COMMENT, fieldSchema.getComment());

                    context.putEntity(colQualifiedName, column);
                }

                ret.add(column);
            }

        }

        return ret;
    }

    public String getRewrittenColumnName(String columnName,
                                         String lineagePartitionName,
                                         String lineagePartitionValue) {
        return columnName + "^" + lineagePartitionName + "=" + lineagePartitionValue;
    }

    protected AtlasEntity getPathEntity(Path path, AtlasEntityExtInfo extInfo) {
        AtlasEntity ret;
        String      strPath = path.toString().toLowerCase();

        if (isS3Path(strPath)) {
            String      bucketName          = path.toUri().getAuthority();
            String      bucketQualifiedName = (path.toUri().getScheme() + SCHEME_SEPARATOR + path.toUri().getAuthority() + QNAME_SEP_CLUSTER_NAME).toLowerCase() + getClusterName();
            String      pathQualifiedName   = (strPath + QNAME_SEP_CLUSTER_NAME).toLowerCase() + getClusterName();
            AtlasEntity bucketEntity        = context.getEntity(bucketQualifiedName);

            ret = context.getEntity(pathQualifiedName);

            if (ret == null) {
                if (bucketEntity == null) {
                    bucketEntity = new AtlasEntity(AWS_S3_BUCKET);

                    bucketEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, bucketQualifiedName);
                    bucketEntity.setAttribute(ATTRIBUTE_NAME, bucketName);

                    context.putEntity(bucketQualifiedName, bucketEntity);
                }

                extInfo.addReferredEntity(bucketEntity);

                ret = new AtlasEntity(AWS_S3_PSEUDO_DIR);

                ret.setAttribute(ATTRIBUTE_BUCKET, getObjectId(bucketEntity));
                ret.setAttribute(ATTRIBUTE_OBJECT_PREFIX, Path.getPathWithoutSchemeAndAuthority(path).toString().toLowerCase());
                ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, pathQualifiedName);
                ret.setAttribute(ATTRIBUTE_NAME, Path.getPathWithoutSchemeAndAuthority(path).toString().toLowerCase());

                context.putEntity(pathQualifiedName, ret);
            }
        } else {
            String nameServiceID     = HdfsNameServiceResolver.getNameServiceIDForPath(strPath);
            String attrPath          = StringUtils.isEmpty(nameServiceID) ? strPath : HdfsNameServiceResolver.getPathWithNameServiceID(strPath);
            String pathQualifiedName = getQualifiedName(attrPath);

            ret = context.getEntity(pathQualifiedName);

            if (ret == null) {
                ret = new AtlasEntity(HDFS_TYPE_PATH);

                if (StringUtils.isNotEmpty(nameServiceID)) {
                    ret.setAttribute(ATTRIBUTE_NAMESERVICE_ID, nameServiceID);
                }

                ret.setAttribute(ATTRIBUTE_PATH, attrPath);
                ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, pathQualifiedName);
                ret.setAttribute(ATTRIBUTE_NAME, Path.getPathWithoutSchemeAndAuthority(path).toString().toLowerCase());
                ret.setAttribute(ATTRIBUTE_CLUSTER_NAME, getClusterName());

                context.putEntity(pathQualifiedName, ret);
            }
        }

        return ret;
    }

    protected AtlasEntity getHiveProcessEntity(List<AtlasEntity> inputs, List<AtlasEntity> outputs) throws Exception {
        AtlasEntity ret         = new AtlasEntity(HIVE_TYPE_PROCESS);
        HookContext hookContext = getHiveContext();
        String      queryStr    = hookContext.getQueryPlan().getQueryStr();

        if (queryStr != null) {
            queryStr = queryStr.toLowerCase().trim();
        }

        ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, getQualifiedName(inputs, outputs));
        ret.setAttribute(ATTRIBUTE_INPUTS, getObjectIds(inputs));
        ret.setAttribute(ATTRIBUTE_OUTPUTS,  getObjectIds(outputs));
        ret.setAttribute(ATTRIBUTE_NAME, queryStr);
        ret.setAttribute(ATTRIBUTE_OPERATION_TYPE, hookContext.getOperationName());
        ret.setAttribute(ATTRIBUTE_START_TIME, hookContext.getQueryPlan().getQueryStartTime());
        ret.setAttribute(ATTRIBUTE_END_TIME, System.currentTimeMillis());
        ret.setAttribute(ATTRIBUTE_USER_NAME, getUserName());
        ret.setAttribute(ATTRIBUTE_QUERY_TEXT, queryStr);
        ret.setAttribute(ATTRIBUTE_QUERY_ID, hookContext.getQueryPlan().getQuery().getQueryId());
        ret.setAttribute(ATTRIBUTE_QUERY_PLAN, "Not Supported");
        ret.setAttribute(ATTRIBUTE_RECENT_QUERIES, Collections.singletonList(queryStr));

        return ret;
    }

    protected String getClusterName() {
        return context.getClusterName();
    }

    protected Hive getHive() {
        return context.getHive();
    }

    protected HookContext getHiveContext() {
        return context.getHiveContext();
    }

    protected String getUserName() {
        String ret = getHiveContext().getUserName();

        if (StringUtils.isEmpty(ret)) {
            UserGroupInformation ugi = getHiveContext().getUgi();

            if (ugi != null) {
                ret = ugi.getShortUserName();
            }

            if (StringUtils.isEmpty(ret)) {
                try {
                    ret = UserGroupInformation.getCurrentUser().getShortUserName();
                } catch (IOException e) {
                    LOG.warn("Failed for UserGroupInformation.getCurrentUser() ", e);
                    ret = System.getProperty("user.name");
                }
            }
        }


        return ret;
    }

    protected String getQualifiedName(Entity entity) throws Exception {
        switch (entity.getType()) {
            case DATABASE:
                return getQualifiedName(entity.getDatabase());

            case TABLE:
            case PARTITION:
                return getQualifiedName(entity.getTable());

            case DFS_DIR:
                return getQualifiedName(entity.getLocation());
        }

        return null;
    }

    protected String getQualifiedName(Database db) {
        return context.getQualifiedName(db);
    }

    protected String getQualifiedName(Table table) {
        return context.getQualifiedName(table);
    }

    protected String getQualifiedName(Table table, StorageDescriptor sd) {
        return getQualifiedName(table) + "_storage";
    }

    protected List<String> getQualifiedName(Table table, FieldSchema column) {
        List<String> ret = new ArrayList<>();
        String tblQualifiedName = getQualifiedName(table);
        int sepPos = tblQualifiedName.lastIndexOf(QNAME_SEP_CLUSTER_NAME);

        List<String> rewrittenColumnNames = getRewrittenColumnNames(table, column);

        for (String columnName: rewrittenColumnNames) {
            String colQualifiedName = null;
            if (sepPos == -1) {
                colQualifiedName = tblQualifiedName + QNAME_SEP_ENTITY_NAME + columnName.toLowerCase();
            } else {
                colQualifiedName = tblQualifiedName.substring(0, sepPos)
                        + QNAME_SEP_ENTITY_NAME + columnName.toLowerCase() + tblQualifiedName.substring(sepPos);
            }
            ret.add(colQualifiedName);
        }
        return ret;
    }

    protected List<String> getRewrittenColumnNames(Table table, FieldSchema column) {
        List<String> ret = new ArrayList<>();
        Set<String> knownColumns = new HashSet<>();
        HiveLineageTableInfo lineageTableInfo = context.getHiveLineageTableInfo();

        if (lineageTableInfo.isLineagePartitioned(table)) {
            String lineagePartitionName = lineageTableInfo.getLineagePartitionName(table);
            List<String> lineagePartitionValues = lineageTableInfo.getLineagePartitionValues(table);
            List<FieldSchema> partCols = table.getPartCols();

            for (String lineagePartitionValue: lineagePartitionValues) {
                String colName = null;
                if (partCols.contains(column)) {
                    colName = column.getName();
                } else {
                    colName = getRewrittenColumnName(
                            column.getName(), lineagePartitionName, lineagePartitionValue);
                }

                if (knownColumns.add(colName)) {
                    ret.add(colName);
                }
            }
        } else {
            if (knownColumns.add(column.getName())) {
                ret.add(column.getName());
            }
        }

        return ret;
    }

    /**
     * 列级别血统输出列列名改写
     *
     * 对于输出列而言, 由于分区表插入肯定会插入某个具体的分区, 所以不会出现某个血统分区具有多个值的情况,
     * 对比输入列比较好处理
     *
     * @param column 输出列
     * @return 改写后的输出列名
     */
    protected String getQualifiedName(DependencyKey column) {
        String dbName    = column.getDataContainer().getTable().getDbName();
        String tableName = column.getDataContainer().getTable().getTableName();

        HiveLineageTableInfo lineageTableInfo = context.getHiveLineageTableInfo();
        org.apache.hadoop.hive.metastore.api.Table table = column.getDataContainer().getTable();

        String colName = null;
        // 因为是lineage partitioned, 所以首先一定是partitioned
        if (lineageTableInfo.isLineagePartitioned(table)) {
            String lineagePartitionName = lineageTableInfo.getLineagePartitionName(table);
            Partition part = column.getDataContainer().getPartition();

            List<FieldSchema> partCols = getPartCols(table);

            String partitionValue = null;
            for (int i = 0; i < partCols.size(); i++) {
                // 我们只需要获取血统分区列的值就行了
                if (partCols.get(i).getName().equals(lineagePartitionName)) {
                    partitionValue = part.getValues().get(i);
                    break;
                }
            }

            if (partCols.contains(column.getFieldSchema())) { // 如果是分区列, 不改写名字.
                colName = column.getFieldSchema().getName();
            } else {
                colName = getRewrittenColumnName(
                        column.getFieldSchema().getName(), lineagePartitionName, partitionValue);
            }
        } else {
            colName = column.getFieldSchema().getName();
        }

        return getQualifiedName(dbName, tableName, colName);
    }

    public List<FieldSchema> getPartCols(org.apache.hadoop.hive.metastore.api.Table table) {
        if (table.getPartitionKeys() != null && table.getPartitionKeys().size() > 0) {
            return table.getPartitionKeys();
        } else {
            return new ArrayList<>();
        }
    }

    protected List<String> getQualifiedName(BaseColumnInfo column,
                                            Map<String, List<String>> lineagePartitionValuesMap) {
        String dbName    = column.getTabAlias().getTable().getDbName();
        String tableName = column.getTabAlias().getTable().getTableName();
        String colName   = null;

        FieldSchema fieldSchema = column.getColumn();
        List<String> ret = new ArrayList<>();

        List<String> colNames = new ArrayList<>();

        if (fieldSchema != null) {
            HiveLineageTableInfo lineageTableInfo = context.getHiveLineageTableInfo();
            org.apache.hadoop.hive.metastore.api.Table table = column.getTabAlias().getTable();

            if (lineageTableInfo.isLineagePartitioned(table)) {
                String tableFullName = lineageTableInfo.getTableFullName(table);
                String lineagePartitionName = lineageTableInfo.getLineagePartitionName(table);
                List<String> lineagePartitionValues = lineagePartitionValuesMap.get(tableFullName);
                List<FieldSchema> partCols = getPartCols(table);

                if (partCols.contains(fieldSchema)) { // 如果是分区列, 不改写名字.
                    colName = fieldSchema.getName();
                    colNames.add(colName);
                } else {
                    // TODO: 当前在输入分区没有值的情况下, lineagePartitionValues为空. 需要升级
                    for (String lineagePartitionValue: lineagePartitionValues) {
                        colName = getRewrittenColumnName(
                                fieldSchema.getName(), lineagePartitionName, lineagePartitionValue);
                        colNames.add(colName);
                    }
                }
            } else {
                colName = fieldSchema.getName();
                colNames.add(colName);
            }

        }

        if (colName == null) {
            ret.add((dbName + QNAME_SEP_ENTITY_NAME + tableName + QNAME_SEP_CLUSTER_NAME).toLowerCase() + getClusterName());
        } else {
            for (String name: colNames) {
                ret.add((dbName + QNAME_SEP_ENTITY_NAME + tableName + QNAME_SEP_ENTITY_NAME + name + QNAME_SEP_CLUSTER_NAME).toLowerCase() + getClusterName());
            }
        }

        return ret;
    }

    protected String getQualifiedName(String dbName, String tableName, String colName) {
        return (dbName + QNAME_SEP_ENTITY_NAME + tableName + QNAME_SEP_ENTITY_NAME + colName + QNAME_SEP_CLUSTER_NAME).toLowerCase() + getClusterName();
    }

    protected String getQualifiedName(URI location) {
        String strPath       = new Path(location).toString().toLowerCase();
        String nameServiceID = HdfsNameServiceResolver.getNameServiceIDForPath(strPath);
        String attrPath      = StringUtils.isEmpty(nameServiceID) ? strPath : HdfsNameServiceResolver.getPathWithNameServiceID(strPath);

        return getQualifiedName(attrPath);
    }

    protected String getQualifiedName(String path) {
        if (path.startsWith(HdfsNameServiceResolver.HDFS_SCHEME)) {
            return (path + QNAME_SEP_CLUSTER_NAME).toLowerCase() + getClusterName();
        }

        return path.toLowerCase();
    }

    protected String getColumnQualifiedName(String tblQualifiedName, String columnName) {
        int sepPos = tblQualifiedName.lastIndexOf(QNAME_SEP_CLUSTER_NAME);

        if (sepPos == -1) {
            return tblQualifiedName + QNAME_SEP_ENTITY_NAME + columnName.toLowerCase();
        } else {
            return tblQualifiedName.substring(0, sepPos) + QNAME_SEP_ENTITY_NAME + columnName.toLowerCase() + tblQualifiedName.substring(sepPos);
        }

    }

    protected String getQualifiedName(List<AtlasEntity> inputs, List<AtlasEntity> outputs) throws Exception {
        HiveOperation operation = context.getHiveOperation();

        if (operation == HiveOperation.CREATETABLE ||
            operation == HiveOperation.CREATETABLE_AS_SELECT ||
            operation == HiveOperation.CREATEVIEW ||
            operation == HiveOperation.ALTERVIEW_AS ||
            operation == HiveOperation.ALTERTABLE_LOCATION) {
            List<? extends Entity> sortedEntities = new ArrayList<>(getHiveContext().getOutputs());

            Collections.sort(sortedEntities, entityComparator);

            for (Entity entity : sortedEntities) {
                if (entity.getType() == Entity.Type.TABLE) {
                    Table table = entity.getTable();

                    table = getHive().getTable(table.getDbName(), table.getTableName());

                    long createTime = getTableCreateTime(table);

                    return getQualifiedName(table) + QNAME_SEP_PROCESS + createTime;
                }
            }
        }

        StringBuilder sb = new StringBuilder(getHiveContext().getOperationName());

        boolean ignoreHDFSPaths = ignoreHDFSPathsinProcessQualifiedName();

        addToProcessQualifiedName(sb, getHiveContext().getInputs(), ignoreHDFSPaths);
        sb.append("->");
        addToProcessQualifiedName(sb, getHiveContext().getOutputs(), ignoreHDFSPaths);

        return sb.toString();
    }

    protected AtlasEntity toReferencedHBaseTable(Table table, AtlasEntitiesWithExtInfo entities) {
        AtlasEntity    ret            = null;
        HBaseTableInfo hBaseTableInfo = new HBaseTableInfo(table);
        String         hbaseNameSpace = hBaseTableInfo.getHbaseNameSpace();
        String         hbaseTableName = hBaseTableInfo.getHbaseTableName();

        if (hbaseTableName != null) {
            AtlasEntity nsEntity = new AtlasEntity(HBASE_TYPE_NAMESPACE);
            nsEntity.setAttribute(ATTRIBUTE_NAME, hbaseNameSpace);
            nsEntity.setAttribute(ATTRIBUTE_CLUSTER_NAME, getClusterName());
            nsEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, getHBaseNameSpaceQualifiedName(getClusterName(), hbaseNameSpace));

            ret = new AtlasEntity(HBASE_TYPE_TABLE);

            ret.setAttribute(ATTRIBUTE_NAME, hbaseTableName);
            ret.setAttribute(ATTRIBUTE_URI, hbaseTableName);
            ret.setAttribute(ATTRIBUTE_NAMESPACE, getObjectId(nsEntity));
            ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, getHBaseTableQualifiedName(getClusterName(), hbaseNameSpace, hbaseTableName));

            entities.addReferredEntity(nsEntity);
            entities.addEntity(ret);
        }

        return ret;
    }

    protected boolean isHBaseStore(Table table) {
        boolean             ret        = false;
        Map<String, String> parameters = table.getParameters();

        if (MapUtils.isNotEmpty(parameters)) {
            String storageHandler = parameters.get(ATTRIBUTE_STORAGE_HANDLER);

            ret = (storageHandler != null && storageHandler.equals(HBASE_STORAGE_HANDLER_CLASS));
        }

        return ret;
    }

    private static String getHBaseTableQualifiedName(String clusterName, String nameSpace, String tableName) {
        return String.format("%s:%s@%s", nameSpace.toLowerCase(), tableName.toLowerCase(), clusterName);
    }

    private static String getHBaseNameSpaceQualifiedName(String clusterName, String nameSpace) {
        return String.format("%s@%s", nameSpace.toLowerCase(), clusterName);
    }

    private boolean ignoreHDFSPathsinProcessQualifiedName() {
        switch (context.getHiveOperation()) {
            case LOAD:
            case IMPORT:
                return hasPartitionEntity(getHiveContext().getOutputs());
            case EXPORT:
                return hasPartitionEntity(getHiveContext().getInputs());
            case QUERY:
                return true;
        }

        return false;
    }

    private boolean hasPartitionEntity(Collection<? extends Entity> entities) {
        if (entities != null) {
            for (Entity entity : entities) {
                if (entity.getType() == Entity.Type.PARTITION) {
                    return true;
                }
            }
        }

        return false;
    }

    private void addToProcessQualifiedName(StringBuilder processQualifiedName, Collection<? extends Entity> entities, boolean ignoreHDFSPaths) {
        if (entities == null) {
            return;
        }

        List<? extends Entity> sortedEntities = new ArrayList<>(entities);

        Collections.sort(sortedEntities, entityComparator);

        Set<String> dataSetsProcessed = new HashSet<>();

        for (Entity entity : sortedEntities) {
            if (ignoreHDFSPaths && (Entity.Type.DFS_DIR.equals(entity.getType()) || Entity.Type.LOCAL_DIR.equals(entity.getType()))) {
                continue;
            }

            String qualifiedName = null;
            long   createTime    = 0;

            try {
                if (entity.getType() == Entity.Type.PARTITION || entity.getType() == Entity.Type.TABLE) {
                    Table table = getHive().getTable(entity.getTable().getDbName(), entity.getTable().getTableName());

                    if (table != null) {
                        createTime    = getTableCreateTime(table);
                        qualifiedName = getQualifiedName(table);
                    }
                } else {
                    qualifiedName = getQualifiedName(entity);
                }
            } catch (Exception excp) {
                LOG.error("error while computing qualifiedName for process", excp);
            }

            if (qualifiedName == null || !dataSetsProcessed.add(qualifiedName)) {
                continue;
            }

            if (entity instanceof WriteEntity) { // output entity
                WriteEntity writeEntity = (WriteEntity) entity;

                if (writeEntity.getWriteType() != null && HiveOperation.QUERY.equals(context.getHiveOperation())) {
                    boolean addWriteType = false;

                    switch (((WriteEntity) entity).getWriteType()) {
                        case INSERT:
                        case INSERT_OVERWRITE:
                        case UPDATE:
                        case DELETE:
                            addWriteType = true;
                        break;

                        case PATH_WRITE:
                            addWriteType = !Entity.Type.LOCAL_DIR.equals(entity.getType());
                        break;
                    }

                    if (addWriteType) {
                        processQualifiedName.append(QNAME_SEP_PROCESS).append(writeEntity.getWriteType().name());
                    }
                }
            }

            processQualifiedName.append(QNAME_SEP_PROCESS).append(qualifiedName.toLowerCase().replaceAll("/", ""));

            if (createTime != 0) {
                processQualifiedName.append(QNAME_SEP_PROCESS).append(createTime);
            }
        }
    }

    private boolean isAlterTableOperation() {
        switch (context.getHiveOperation()) {
            case ALTERTABLE_FILEFORMAT:
            case ALTERTABLE_CLUSTER_SORT:
            case ALTERTABLE_BUCKETNUM:
            case ALTERTABLE_PROPERTIES:
            case ALTERTABLE_SERDEPROPERTIES:
            case ALTERTABLE_SERIALIZER:
            case ALTERTABLE_ADDCOLS:
            case ALTERTABLE_REPLACECOLS:
            case ALTERTABLE_PARTCOLTYPE:
            case ALTERTABLE_LOCATION:
            case ALTERTABLE_RENAME:
            case ALTERTABLE_RENAMECOL:
            case ALTERVIEW_PROPERTIES:
            case ALTERVIEW_RENAME:
            case ALTERVIEW_AS:
                return true;
        }

        return false;
    }

    private boolean isS3Path(String strPath) {
        return strPath != null && (strPath.startsWith(S3_SCHEME) || strPath.startsWith(S3A_SCHEME));
    }


    static final class EntityComparator implements Comparator<Entity> {
        @Override
        public int compare(Entity entity1, Entity entity2) {
            String name1 = entity1.getName();
            String name2 = entity2.getName();

            if (name1 == null || name2 == null) {
                name1 = entity1.getD().toString();
                name2 = entity2.getD().toString();
            }

            return name1.toLowerCase().compareTo(name2.toLowerCase());
        }
    }

    static final Comparator<Entity> entityComparator = new EntityComparator();

    static final class HBaseTableInfo {
        String hbaseNameSpace = null;
        String hbaseTableName = null;

         HBaseTableInfo(Table table) {
            Map<String, String> parameters = table.getParameters();

            if (MapUtils.isNotEmpty(parameters)) {
                hbaseNameSpace = HBASE_DEFAULT_NAMESPACE;
                hbaseTableName = parameters.get(HBASE_PARAM_TABLE_NAME);

                if (hbaseTableName != null) {
                    if (hbaseTableName.contains(HBASE_NAMESPACE_TABLE_DELIMITER)) {
                        String[] hbaseTableInfo = hbaseTableName.split(HBASE_NAMESPACE_TABLE_DELIMITER);

                        if (hbaseTableInfo.length > 1) {
                            hbaseNameSpace = hbaseTableInfo[0];
                            hbaseTableName = hbaseTableInfo[1];
                        }
                    }
                }
            }
        }

        public String getHbaseNameSpace() {
            return hbaseNameSpace;
        }

        public String getHbaseTableName() {
            return hbaseTableName;
        }
    }
}
