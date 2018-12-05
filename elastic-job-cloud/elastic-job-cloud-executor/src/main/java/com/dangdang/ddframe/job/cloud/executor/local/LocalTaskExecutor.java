/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.cloud.executor.local;

import com.dangdang.ddframe.job.api.ElasticJob;
import com.dangdang.ddframe.job.api.JobType;
import com.dangdang.ddframe.job.api.dataflow.DataflowJob;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.cloud.executor.CloudJobFacade;
import com.dangdang.ddframe.job.cloud.executor.JobConfigurationContext;
import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.dataflow.DataflowJobConfiguration;
import com.dangdang.ddframe.job.config.script.ScriptJobConfiguration;
import com.dangdang.ddframe.job.event.JobEventBus;
import com.dangdang.ddframe.job.exception.JobSystemException;
import com.dangdang.ddframe.job.executor.AbstractElasticJobExecutor;
import com.dangdang.ddframe.job.executor.ShardingContexts;
import com.dangdang.ddframe.job.executor.type.DataflowJobExecutor;
import com.dangdang.ddframe.job.executor.type.ScriptJobExecutor;
import com.dangdang.ddframe.job.executor.type.SimpleJobExecutor;
import com.dangdang.ddframe.job.util.config.ShardingItemParameters;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import lombok.RequiredArgsConstructor;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.HashMap;
import java.util.Map;

/**
 * 本地作业执行器.
 * 
 * @author gaohongtao
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class LocalTaskExecutor {
    
    private final LocalCloudJobConfiguration localCloudJobConfiguration;
    
    /**
     * 本地执行作业.
     */
    @SuppressWarnings("unchecked")
    public void execute() {
        AbstractElasticJobExecutor jobExecutor;
        CloudJobFacade jobFacade = new CloudJobFacade(getShardingContexts(), getJobConfigurationContext(), new JobEventBus());
        switch (localCloudJobConfiguration.getTypeConfig().getJobType()) {
            case SIMPLE:
                jobExecutor = new SimpleJobExecutor(getJobInstance(SimpleJob.class), jobFacade);
                break;
            case DATAFLOW:
                jobExecutor = new DataflowJobExecutor(getJobInstance(DataflowJob.class), jobFacade);
                break;
            case SCRIPT:
                jobExecutor = new ScriptJobExecutor(jobFacade);
                break;
            default:
                throw new UnsupportedOperationException(localCloudJobConfiguration.getTypeConfig().getJobType().name());
        }
        jobExecutor.execute();
    }
    
    private ShardingContexts getShardingContexts() {
        JobCoreConfiguration coreConfig = localCloudJobConfiguration.getTypeConfig().getCoreConfig();
        Map<Integer, String> shardingItemMap = new HashMap<>(1, 1);
        shardingItemMap.put(localCloudJobConfiguration.getShardingItem(),
                new ShardingItemParameters(coreConfig.getShardingItemParameters()).getMap().get(localCloudJobConfiguration.getShardingItem()));
        return new ShardingContexts(Joiner.on("@-@").join(localCloudJobConfiguration.getJobName(), localCloudJobConfiguration.getShardingItem(), "READY", "foo_slave_id", "foo_uuid"),
                localCloudJobConfiguration.getJobName(), coreConfig.getShardingTotalCount(), coreConfig.getJobParameter(), shardingItemMap);
    }
    
    private JobConfigurationContext getJobConfigurationContext() {
        Map<String, String> jobConfigurationMap = new HashMap<>();
        jobConfigurationMap.put("jobClass", localCloudJobConfiguration.getTypeConfig().getJobClass());
        jobConfigurationMap.put("jobType", localCloudJobConfiguration.getTypeConfig().getJobType().name());
        jobConfigurationMap.put("jobName", localCloudJobConfiguration.getJobName());
        jobConfigurationMap.put("beanName", localCloudJobConfiguration.getBeanName());
        jobConfigurationMap.put("applicationContext", localCloudJobConfiguration.getApplicationContext());
        if (JobType.DATAFLOW == localCloudJobConfiguration.getTypeConfig().getJobType()) {
            jobConfigurationMap.put("streamingProcess", Boolean.toString(((DataflowJobConfiguration) localCloudJobConfiguration.getTypeConfig()).isStreamingProcess()));
        } else if (JobType.SCRIPT == localCloudJobConfiguration.getTypeConfig().getJobType()) {
            jobConfigurationMap.put("scriptCommandLine", ((ScriptJobConfiguration) localCloudJobConfiguration.getTypeConfig()).getScriptCommandLine());
        }
        return new JobConfigurationContext(jobConfigurationMap);
    }
    
    private <T extends ElasticJob> T getJobInstance(final Class<T> clazz) {
        Object result;
        if (Strings.isNullOrEmpty(localCloudJobConfiguration.getApplicationContext())) {
            String jobClass = localCloudJobConfiguration.getTypeConfig().getJobClass();
            try {
                result = Class.forName(jobClass).newInstance();
            } catch (final ReflectiveOperationException ex) {
                throw new JobSystemException("Elastic-Job: Class '%s' initialize failure, the error message is '%s'.", jobClass, ex.getMessage());
            }
        } else {
            result = new ClassPathXmlApplicationContext(localCloudJobConfiguration.getApplicationContext()).getBean(localCloudJobConfiguration.getBeanName());
        }
        return clazz.cast(result);
    }
}
