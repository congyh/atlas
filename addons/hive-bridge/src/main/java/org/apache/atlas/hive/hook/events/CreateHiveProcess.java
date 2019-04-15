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

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.atlas.hive.hook.AtlasHiveHookContext;
import org.apache.atlas.hive.hook.LineageInfoParser;
import org.apache.atlas.hive.hook.LineageParser;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DependencyKey;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;


public class CreateHiveProcess extends BaseHiveEvent {
    private static final Logger LOG = LoggerFactory.getLogger(CreateHiveProcess.class);

    public CreateHiveProcess(AtlasHiveHookContext context) {
        super(context);
    }

    @Override
    public List<HookNotification> getNotificationMessages() throws Exception {
        List<HookNotification>   ret      = null;
        AtlasEntitiesWithExtInfo entities = getEntities();

        if (entities != null && CollectionUtils.isNotEmpty(entities.getEntities())) {
            ret = Collections.singletonList(new EntityCreateRequestV2(getUserName(), entities));
        }

        return ret;
    }

    public AtlasEntitiesWithExtInfo getEntities() throws Exception {
        AtlasEntitiesWithExtInfo ret = null;

        if (!skipProcess()) {
            List<AtlasEntity> inputs         = new ArrayList<>();
            List<AtlasEntity> outputs        = new ArrayList<>();
            HookContext       hiveContext    = getHiveContext();
            Set<String>       processedNames = new HashSet<>();

            ret = new AtlasEntitiesWithExtInfo();

            if (hiveContext.getInputs() != null) {
                for (ReadEntity input : hiveContext.getInputs()) {
                    String qualifiedName = getQualifiedName(input);

                    if (qualifiedName == null || !processedNames.add(qualifiedName)) {
                        continue;
                    }

                    AtlasEntity entity = getInputOutputEntity(input, ret);

                    if (!input.isDirect()) {
                        continue;
                    }

                    if (entity != null) {
                        inputs.add(entity);
                    }
                }
            }

            if (hiveContext.getOutputs() != null) {
                for (WriteEntity output : hiveContext.getOutputs()) {
                    String qualifiedName = getQualifiedName(output);

                    if (qualifiedName == null || !processedNames.add(qualifiedName)) {
                        continue;
                    }

                    AtlasEntity entity = getInputOutputEntity(output, ret);

                    if (entity != null) {
                        outputs.add(entity);
                    }
                }
            }

            if (!inputs.isEmpty() || !outputs.isEmpty()) {
                AtlasEntity process = getHiveProcessEntity(inputs, outputs);

                ret.addEntity(process);

                processColumnLineage(process, ret);

                addProcessedEntities(ret);
            } else {
                ret = null;
            }
        }

        return ret;
    }

    private void processColumnLineage(AtlasEntity hiveProcess, AtlasEntitiesWithExtInfo entities) {
        // 外层调用已经保证了inputs不为空.
        Set<ReadEntity> inputs = context.getHiveContext().getInputs();
        LineageParser parser = new LineageParser(context.getHiveLineageTableInfo(), inputs);
        // hive1.2.1版本的HookContext是没有Index的, 所以需要升级atlas依赖的hive版本
        LineageCtx.Index index = context.getHiveContext().getIndex();
        Map<String, ObjectPair<SelectOperator,
                Table>> finalSelOps = index.getFinalSelectOps();
        final String dbType = JdbcConstants.HIVE;
        List<SQLExpr> exprs = new ArrayList<>();
        for (ObjectPair<SelectOperator,
                org.apache.hadoop.hive.ql.metadata.Table> pair: finalSelOps.values()) {
            SelectOperator finalSelOp = pair.getFirst();
            Set<LineageInfo.Predicate> conds = index.getPredicates(finalSelOp);
            if (conds != null && !conds.isEmpty()) {
                for (LineageInfo.Predicate cond: conds) {
                    exprs.add(SQLUtils.toSQLExpr(cond.getExpr(), dbType));
                }
            }
        }

        for (SQLExpr expr: exprs) {
            parser.getColumnValuePair(expr);
        }
        parser.makeUpActualLineagePartitionValues();

        Map<String, Set<String>> lineagePartitionValuesMap = parser.getActualLineagePartVals();

        LineageInfo lineageInfo = getHiveContext().getLinfo();

        if (lineageInfo == null || CollectionUtils.isEmpty(lineageInfo.entrySet())) {
            return;
        }

        for (Map.Entry<DependencyKey, Dependency> entry : lineageInfo.entrySet()) {
            String      outputColName = getQualifiedName(entry.getKey());
            AtlasEntity outputColumn  = context.getEntity(outputColName);

            if (outputColumn == null) {
                LOG.warn("column-lineage: non-existing output-column {}", outputColName);

                continue;
            }

            List<AtlasEntity> inputColumns = new ArrayList<>();

            for (BaseColumnInfo baseColumn : getBaseCols(entry.getValue())) {
                List<String>      inputColNames = getQualifiedName(baseColumn, lineagePartitionValuesMap);
                for (String inputColName: inputColNames) {
                    AtlasEntity inputColumn  = context.getEntity(inputColName);

                    if (inputColumn == null) {
                        LOG.warn("column-lineage: non-existing input-column {} for output-column={}", inputColNames, outputColName);

                        continue;
                    }

                    inputColumns.add(inputColumn);
                }
            }

            if (inputColumns.isEmpty()) {
                continue;
            }

            AtlasEntity columnLineageProcess = new AtlasEntity(HIVE_TYPE_COLUMN_LINEAGE);

            columnLineageProcess.setAttribute(ATTRIBUTE_NAME, hiveProcess.getAttribute(ATTRIBUTE_NAME) + ":" + outputColumn.getAttribute(ATTRIBUTE_NAME));
            columnLineageProcess.setAttribute(ATTRIBUTE_QUALIFIED_NAME, hiveProcess.getAttribute(ATTRIBUTE_QUALIFIED_NAME) + ":" + outputColumn.getAttribute(ATTRIBUTE_NAME));
            columnLineageProcess.setAttribute(ATTRIBUTE_INPUTS, getObjectIds(inputColumns));
            columnLineageProcess.setAttribute(ATTRIBUTE_OUTPUTS, Collections.singletonList(getObjectId(outputColumn)));
            columnLineageProcess.setAttribute(ATTRIBUTE_QUERY, getObjectId(hiveProcess));
            columnLineageProcess.setAttribute(ATTRIBUTE_DEPENDENCY_TYPE, entry.getValue().getType());
            columnLineageProcess.setAttribute(ATTRIBUTE_EXPRESSION, entry.getValue().getExpr());

            entities.addEntity(columnLineageProcess);
        }
    }

    private Collection<BaseColumnInfo> getBaseCols(Dependency lInfoDep) {
        Collection<BaseColumnInfo> ret = Collections.emptyList();

        if (lInfoDep != null) {
            try {
                Method getBaseColsMethod = lInfoDep.getClass().getMethod("getBaseCols");

                Object retGetBaseCols = getBaseColsMethod.invoke(lInfoDep);

                if (retGetBaseCols != null) {
                    if (retGetBaseCols instanceof Collection) {
                        ret = (Collection) retGetBaseCols;
                    } else {
                        LOG.warn("{}: unexpected return type from LineageInfo.Dependency.getBaseCols(), expected type {}",
                                retGetBaseCols.getClass().getName(), "Collection");
                    }
                }
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ex) {
                LOG.warn("getBaseCols()", ex);
            }
        }

        return ret;
    }


    private boolean skipProcess() {
        Set<ReadEntity>  inputs  = getHiveContext().getInputs();
        Set<WriteEntity> outputs = getHiveContext().getOutputs();

        boolean ret = CollectionUtils.isEmpty(inputs) && CollectionUtils.isEmpty(outputs);

        if (!ret) {
            if (getContext().getHiveOperation() == HiveOperation.QUERY) {
                // Select query has only one output
                if (outputs.size() == 1) {
                    WriteEntity output = outputs.iterator().next();

                    if (output.getType() == Entity.Type.DFS_DIR || output.getType() == Entity.Type.LOCAL_DIR) {
                        if (output.getWriteType() == WriteEntity.WriteType.PATH_WRITE && output.isTempURI()) {
                            ret = true;
                        }
                    }

                }
            }
        }

        return ret;
    }
}
