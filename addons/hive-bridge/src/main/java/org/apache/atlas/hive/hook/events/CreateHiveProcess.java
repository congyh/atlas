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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DependencyKey;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class CreateHiveProcess extends BaseHiveEvent {
    private static final Logger LOG = LoggerFactory.getLogger(CreateHiveProcess.class);

    public CreateHiveProcess(AtlasHiveHookContext context) {
        super(context);
    }

    /**
     * Note: 本方法在初步从LineageInfo中解析出需要解析的下游表列及其依赖信息后, 转交给notification方法进行传递
     * @return
     * @throws Exception
     */
    @Override
    public List<HookNotification> getNotificationMessages() throws Exception {
        List<HookNotification>   ret      = null;
        // Note: 这里能拿到初步需要解析的所有血统信息
        AtlasEntitiesWithExtInfo entities = getEntities();

        if (entities != null && CollectionUtils.isNotEmpty(entities.getEntities())) {
            // Note: 这里将原来的初步解析的LineageInfo信息, 即下游表列(table, column)对->dependency的包装类
            // 重新拆装成了HookNotification类型(EntityCreateRequestV2是其子类)
            ret = Collections.singletonList(new EntityCreateRequestV2(getUserName(), entities));
        }

        return ret;
    }

    public AtlasEntitiesWithExtInfo getEntities() throws Exception {
        AtlasEntitiesWithExtInfo ret = null;

        if (!skipProcess()) {
            List<AtlasEntity> inputs         = new ArrayList<>();
            List<AtlasEntity> outputs        = new ArrayList<>();
            // Note: 在这里拿到了HiveContext, 里面有LineageInfo信息
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
                //  Note: 获取一个Hive实例的抽象
                AtlasEntity process = getHiveProcessEntity(inputs, outputs);

                ret.addEntity(process);

                // Note: 这里抽取下游表列的Lineage信息
                processColumnLineage(process, ret);

                addProcessedEntities(ret);
            } else {
                ret = null;
            }
        }

        return ret;
    }

    /**
     * Note: 这里是解析列lineage的核心方法
     *
     * 会从Hive传过来的Context中取出LineageInfo, 然后进行解析
     *
     * 核心存储结构:
     *
     * final List<AtlasEntity> columnLineages      = new ArrayList<>();
     *
     * 通过run中遍历Hive的LineageInfo中的map: 一个以下游(table, column)为key, dependency信息为value的map取出所有依赖信息,
     * 然后保存到这里面.
     *
     * @param hiveProcess
     * @param entities
     */
    private void processColumnLineage(AtlasEntity hiveProcess, AtlasEntitiesWithExtInfo entities) {
        LineageInfo lineageInfo = getHiveContext().getLinfo();

        // Note: 验证LineageInfo的entrySet不为空, 也就是有(table, column)的Dependency信息.
        if (lineageInfo == null || CollectionUtils.isEmpty(lineageInfo.entrySet())) {
            return;
        }

        final List<AtlasEntity> columnLineages      = new ArrayList<>();
        int                     lineageInputsCount  = 0;
        final Set<String>       processedOutputCols = new HashSet<>();

        // Note: 这里是核心列级别血统的遍历解析部分.
        for (Map.Entry<DependencyKey, Dependency> entry : lineageInfo.entrySet()) {
            // Note: 通过字符串拼接的方式获取column的全名
            String      outputColName = getQualifiedName(entry.getKey());
            // Note: 根据全名获取到column实体.
            AtlasEntity outputColumn  = context.getEntity(outputColName);

            // Note: 这里的开关是log4j的开关
            if (LOG.isDebugEnabled()) {
                LOG.debug("processColumnLineage(): DependencyKey={}; Dependency={}", entry.getKey(), entry.getValue());
            }

            // Note: 如果找不到下游表列信息, 那么不解析直接跳过
            if (outputColumn == null) {
                LOG.warn("column-lineage: non-existing output-column {}", outputColName);

                continue;
            }

            // Note: 是否重复解析了下游表列的Lineage, 如果已经解析过了直接跳过
            if (processedOutputCols.contains(outputColName)) {
                LOG.warn("column-lineage: duplicate for output-column {}", outputColName);

                continue;
            } else { // Note: 先标记为待解析, 然后开始解析
                processedOutputCols.add(outputColName);
            }

            // Note: 存储所有上游存储列的信息.
            List<AtlasEntity> inputColumns = new ArrayList<>();

            // Note: 首先取出上游的基础列(对比派生列的概念)
            for (BaseColumnInfo baseColumn : getBaseCols(entry.getValue())) {
                String      inputColName = getQualifiedName(baseColumn);
                AtlasEntity inputColumn  = context.getEntity(inputColName);

                if (inputColumn == null) {
                    LOG.warn("column-lineage: non-existing input-column {} for output-column={}", inputColName, outputColName);

                    continue;
                }

                inputColumns.add(inputColumn);
            }

            // Note: 如果下游表列不是从上游生成的, 无需做解析
            if (inputColumns.isEmpty()) {
                continue;
            }

            lineageInputsCount += inputColumns.size();

            AtlasEntity columnLineageProcess = new AtlasEntity(HIVE_TYPE_COLUMN_LINEAGE);

            // Note: 以下都是在往AtlasEntity对象中设置属性
            // 每个AtlasEntity都有一个名为attribute的map, 作为可扩展的属性保存.
            columnLineageProcess.setAttribute(ATTRIBUTE_NAME, hiveProcess.getAttribute(ATTRIBUTE_QUALIFIED_NAME) + ":" + outputColumn.getAttribute(ATTRIBUTE_NAME));
            columnLineageProcess.setAttribute(ATTRIBUTE_QUALIFIED_NAME, hiveProcess.getAttribute(ATTRIBUTE_QUALIFIED_NAME) + ":" + outputColumn.getAttribute(ATTRIBUTE_NAME));
            columnLineageProcess.setAttribute(ATTRIBUTE_INPUTS, getObjectIds(inputColumns));
            columnLineageProcess.setAttribute(ATTRIBUTE_OUTPUTS, Collections.singletonList(getObjectId(outputColumn)));
            columnLineageProcess.setAttribute(ATTRIBUTE_QUERY, getObjectId(hiveProcess));
            columnLineageProcess.setAttribute(ATTRIBUTE_DEPENDENCY_TYPE, entry.getValue().getType());
            columnLineageProcess.setAttribute(ATTRIBUTE_EXPRESSION, entry.getValue().getExpr());

            // Note: 初步遍历完成, 筛选掉了不需要解析的下游表列依赖.
            columnLineages.add(columnLineageProcess);
        }

        float   avgInputsCount    = columnLineages.size() > 0 ? (((float) lineageInputsCount) / columnLineages.size()) : 0;
        boolean skipColumnLineage = context.getSkipHiveColumnLineageHive20633() && avgInputsCount > context.getSkipHiveColumnLineageHive20633InputsThreshold();

        if (!skipColumnLineage) {
            // Note: 这里对初步筛选完成的信息加入到context中, 传递给其他方法.
            for (AtlasEntity columnLineage : columnLineages) {
                // Note: entities是一个AtlasEntitiesWithExtInfo类型.
                entities.addEntity(columnLineage);
            }
        } else {
            LOG.warn("skipped {} hive_column_lineage entities. Average # of inputs={}, threshold={}, total # of inputs={}", columnLineages.size(), avgInputsCount, context.getSkipHiveColumnLineageHive20633InputsThreshold(), lineageInputsCount);
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
