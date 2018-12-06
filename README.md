# Elastic-Job - distributed scheduled job solution

#### ElasticJob 完善
2018.12.5
-   增加默认对oracle的支持
    - 增加对oracle数据源的判断，主要是修改控制台分页查询的mysql与oracle的差异com.dangdang.ddframe.job.event.rdb.JobEventRdbSearch 
    - 对官方使用的sql写法有些不是很规范的DML语句做了修改，修改oracle新增主键的方式com.dangdang.ddframe.job.event.rdb.JobEventRdbStorage
2018.12.6
-   更新依赖
     - com.google.guava:guava
        *该原先的version`18.0`升级到`25.0-jre`*
     - org.apache.curator
        *替换是因为curator-client中引用了低版本的guava，当guava升级到`25.0-j re`后有些方法废弃了，所以需要由原来的`2.10.0`升级到`4.0.1`*
     - org.apache.zookeeper
       *因为替换了curator依赖，其默认的版本比较低，所以将原来的`3.5.3-beta`升级到`3.4.9`，必须替换否则会报错，可以参考[博客](https://www.jianshu.com/p/384442962e86)*
     - com.fasterxml.jackson.core
       *github 官方检测有安全漏洞问题，所以由原先的`2.4.4`升级为`2.8.10`*
     - org.apache.mesos
       *github 官方检测有安全漏洞问题，所以由原先的`1.1.0`升级为`1.5.0`*

[![Build Status](https://secure.travis-ci.org/dangdangdotcom/elastic-job.png?branch=master)](https://travis-ci.org/dangdangdotcom/elastic-job)
[![Maven Status](https://maven-badges.herokuapp.com/maven-central/com.dangdang/elastic-job/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.dangdang/elastic-job)
[![Coverage Status](https://coveralls.io/repos/dangdangdotcom/elastic-job/badge.svg?branch=master&service=github)](https://coveralls.io/github/dangdangdotcom/elastic-job?branch=master)
[![GitHub release](https://img.shields.io/github/release/dangdangdotcom/elastic-job.svg)](https://github.com/dangdangdotcom/elastic-job/releases)
[![Hex.pm](http://dangdangdotcom.github.io/elastic-job/elastic-job-lite/img/license.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

# [Elastic-Job-Lite中文主页](http://dangdangdotcom.github.io/elastic-job/elastic-job-lite)
# [Elastic-Job-Cloud中文主页](http://dangdangdotcom.github.io/elastic-job/elastic-job-cloud)
# [Elastic-Job 1.x中文主页(已废弃)](http://dangdangdotcom.github.io/elastic-job/elastic-job-lite-1.x)

# Overview

Elastic-Job is a distributed scheduled job solution. Elastic-Job is composited from 2 independent sub projects: Elastic-Job-Lite and Elastic-Job-Cloud.

Elastic-Job-Lite is a centre-less solution, use lightweight jar to coordinate distributed jobs.
Elastic-Job-Cloud is a Mesos framework which use Mesos + Docker(todo) to manage and isolate resources and processes.

Elastic-Job-Lite and Elastic-Job-Cloud provide unified API. Developers only need code one time, then decide to deploy Lite or Cloud as you want.

# Features

## 1. Elastic-Job-Lite

* Distributed schedule job coordinate
* Elastic scale in and scale out supported
* Failover
* Misfired jobs refire
* Sharding consistently, same sharding item for a job only one running instance
* Self diagnose and recover when distribute environment unstable
* Parallel scheduling supported
* Job lifecycle operation
* Lavish job types
* Spring integrated and namespace supported
* Web console

## 2. Elastic-Job-Cloud
* All Elastic-Job-Lite features included
* Application distributed automatically
* Fenzo based resources allocated elastically
* Docker based processes isolation support (TBD)

# Architecture

## Elastic-Job-Lite

![Elastic-Job-Lite Architecture](http://dangdangdotcom.github.io/elastic-job/elastic-job-lite/img/architecture/elastic_job_lite.png)
***

## Elastic-Job-Cloud

![Elastic-Job-Cloud Architecture](http://dangdangdotcom.github.io/elastic-job/elastic-job-cloud/img/architecture/elastic_job_cloud.png)


# [Release Notes](https://github.com/dangdangdotcom/elastic-job/releases)

# [Roadmap](ROADMAP.md)

# Quick Start

## Elastic-Job-Lite

### Add maven dependency

```xml
<!-- import elastic-job lite core -->
<dependency>
    <groupId>com.dangdang</groupId>
    <artifactId>elastic-job-lite-core</artifactId>
    <version>${lasted.release.version}</version>
</dependency>

<!-- import other module if need -->
<dependency>
    <groupId>com.dangdang</groupId>
    <artifactId>elastic-job-lite-spring</artifactId>
    <version>${lasted.release.version}</version>
</dependency>
```
### Job development

```java
public class MyElasticJob implements SimpleJob {
    
    @Override
    public void execute(ShardingContext context) {
        switch (context.getShardingItem()) {
            case 0: 
                // do something by sharding item 0
                break;
            case 1: 
                // do something by sharding item 1
                break;
            case 2: 
                // do something by sharding item 2
                break;
            // case n: ...
        }
    }
}
```

### Job configuration

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:reg="http://www.dangdang.com/schema/ddframe/reg"
    xmlns:job="http://www.dangdang.com/schema/ddframe/job"
    xsi:schemaLocation="http://www.springframework.org/schema/beans
                        http://www.springframework.org/schema/beans/spring-beans.xsd
                        http://www.dangdang.com/schema/ddframe/reg
                        http://www.dangdang.com/schema/ddframe/reg/reg.xsd
                        http://www.dangdang.com/schema/ddframe/job
                        http://www.dangdang.com/schema/ddframe/job/job.xsd
                        ">
    <!--configure registry center -->
    <reg:zookeeper id="regCenter" server-lists="yourhost:2181" namespace="dd-job" base-sleep-time-milliseconds="1000" max-sleep-time-milliseconds="3000" max-retries="3" />

    <!--configure job -->
    <job:simple id="myElasticJob" class="xxx.MyElasticJob" registry-center-ref="regCenter" cron="0/10 * * * * ?"   sharding-total-count="3" sharding-item-parameters="0=A,1=B,2=C" />
</beans>
```

***

## Elastic-Job-Cloud

### Add maven dependency

```xml
<!-- import elastic-job cloud executor -->
<dependency>
    <groupId>com.dangdang</groupId>
    <artifactId>elastic-job-cloud-executor</artifactId>
    <version>${lasted.release.version}</version>
</dependency>
```

### Job development

Same with `Elastic-Job-Lite`

### Job App configuration

```shell
curl -l -H "Content-type: application/json" -X POST -d '{"appName":"yourAppName","appURL":"http://app_host:8080/foo-job.tar.gz","cpuCount":0.1,"memoryMB":64.0,"bootstrapScript":"bin/start.sh","appCacheEnable":true}' http://elastic_job_cloud_host:8899/api/app
```

### Job configuration

```shell
curl -l -H "Content-type: application/json" -X POST -d '{"jobName":"foo_job","appName":"yourAppName","jobClass":"yourJobClass","jobType":"SIMPLE","jobExecutionType":"TRANSIENT","cron":"0/5 * * * * ?","shardingTotalCount":5,"cpuCount":0.1,"memoryMB":64.0,"failover":true,"misfire":true,"bootstrapScript":"bin/start.sh"}' http://elastic_job_cloud_host:8899/api/job/register
```
