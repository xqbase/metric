# XQBase Metric

A lightweight metric framework for aggregating, collecting and showing metric data.

For basic concepts of metric, see [Metric and Dashboard](http://www.slideshare.net/auntyellow/metric-and-dashboard) in SlideShare (uploaded in 2015), or download [here](https://github.com/xqbase/metric/raw/master/doc/Metric%20and%20Dashboard.ppt).

## Deploy Server

Metric server did 2 big changes since 2015:

1. It supports SQL Databases, [VMStore](http://www.h2database.com/html/mvstore.html) or File as metric storage, except for MongoDB;
2. Dashboard service is embedded in Metric Collector.

So Metric Collector now has 4 projects:

- [collector-file](https://github.com/xqbase/metric/tree/master/collector-file) with embedded File storage and embedded Dashboard
- [collector-mongo](https://github.com/xqbase/metric/tree/master/collector-mongo) with MongoDB and embedded Dashboard. Should use an external MongoDB 
- [collector-mvstore](https://github.com/xqbase/metric/tree/master/collector-mvstore) with embedded MVStore and embedded Dashboard
- **[collector-sql](https://github.com/xqbase/metric/tree/master/collector-sql)** with SQL database and embedded Dashboard. It uses [embedded H2 database](http://www.h2database.com/html/features.html#connection_modes) by default (and recommended), and also supports other databases like Derby, SQLite, MySQL, PostgreSQL, etc

Choose a project and build with `mvn package` and get a jar file (e.g. `metric-collector-sql.jar`), then put it into a folder (e.g. `/usr/local/xqbase-metric/lib`), then run it with `java -jar`, e.g.

    /usr/bin/java -Xms256m -Xmx512m -XX:MaxMetaspaceSize=32m -jar /usr/local/xqbase-metric/lib/metric-collector-sql.jar &

It opens UDP 5514 as metric collector port and TCP 5514 as dashboard web port. If running with embedded H2 database, it also opens TCP 5513 as H2 data port.

If you want to deploy a standalone Dashboard server, build one of the following 4 projects with `mvn package` and get a war file, then put it inside a servlet container like Tomcat:

- [dashboard-file](https://github.com/xqbase/metric/tree/master/dashboard-file) shows metrics stored in files
- [dashboard-mongo](https://github.com/xqbase/metric/tree/master/dashboard-mongo) shows metrics stored in MongoDB 
- [dashboard-mvstore](https://github.com/xqbase/metric/tree/master/dashboard-mvstore) shows metrics stored in MVStore
- [dashboard-sql](https://github.com/xqbase/metric/tree/master/dashboard-sql) shows metrics stored in SQL database

## Generate Metrics

Import the metric-client library as a maven dependency:

```xml
<dependency>
	<groupId>com.xqbase</groupId>
	<artifactId>xqbase-metric-client</artifactId>
	<version>0.2.13</version>
</dependency>
```

For a webapp project, add a filter into web.xml:

```xml
<filter>
	<filter-name>MetricFilter</filter-name>
	<filter-class>com.xqbase.metric.client.MetricFilter</filter-class>
	<init-param>
		<param-name>addresses</param-name>
		<param-value>${metric.collectors}</param-value>
	</init-param>
	<init-param>
		<param-name>prefix</param-name>
		<param-value>${metric.prefix}</param-value>
	</init-param>
	<init-param> <!-- this parameter is optional -->
		<param-name>tags</param-name>
		<param-value>${metric.tags}</param-value>
	</init-param>
</filter>
<filter-mapping>
	<filter-name>MetricFilter</filter-name>
	<url-pattern>/*</url-pattern>
</filter-mapping>
```

For a spring boot project, use `@Configuration` instead of web.xml:

```java
@Configuration
public class FilterConfig {
    @Bean
    public FilterRegistrationBean<MetricFilter> metricFilterRegistrationBean() {
        FilterRegistrationBean<MetricFilter> bean = new FilterRegistrationBean<>();
        MetricFilter filter = new MetricFilter();
        bean.setFilter(filter);
        bean.addUrlPatterns("/*");
        bean.addInitParameter("addresses", ${metric.collectors});
        bean.addInitParameter("prefix", ${metric.prefix});
        bean.addInitParameter("tags", ${metric.tags}); // this line is optional
        return bean;
    }
}
```

For a standalone application, use the following codes:

```java
// Start aggregating metrics
MetricClient.startup(collectors);
...
// Log an event
Metric.put(metricName, metricValue, tagName1, tagValue1, tagName2, tagValue2, ...);
...
// Stop aggregating metrics
MetricClient.shutdown();
```

If `MetricFilter` or `ManagementMonitor` used, the following metrics are automatically generated:

- CPU Usage：`${metric.prefix}.server.cpu`
- GC Elapses：`${metric.prefix}.server.gc`
- Memory Usage：`${metric.prefix}.server.memory.mb`
- Memory Usage in Percentage：`${metric.prefix}.server.memory.percent`
- Memory Pool Usage：`${metric.prefix}.server.memory_pool.mb`
- Memory Pool Usage in Percentage：`${metric.prefix}.server.memory_pool.percent`
- Threads：`${metric.prefix}.server.threads`
- Web Request Concurrency：`${metric.prefix}.webapp.connections` (only work with `MetricFilter`)
- Web Request Elapses：`${metric.prefix}.webapp.request_time` (only work with `MetricFilter`)

Other metrics can be generated by `Metric.put()` method. See [P6Factory.java](https://github.com/xqbase/metric/blob/master/collector-sql/src/main/java/com/xqbase/metric/util/P6Factory.java) as an example to get metrics of SQL elapses and affected rows.

## View Metrics

Dashboard has a Tab bar on top:

- All Metrics: list all metrics collected. Metrics from metric collector are prefixed with `metric.`, and metrics from clients are prefixed with `${metric.prefix}`.
- Other tabs: shortcuts for important metrics.

For each dashboard for a given metric, we can choose the following conditions to view chart:

- tags
- method (SUM, COUNT, AVG, MAX, etc)
- group by (which tag)
- time interval
- time range

Then get a chart like running this SQL:

    SELECT <group-by tag>, <time aggr. by interval>, SUM<or other aggr. method>(value) WHERE <tags conditions> AND <time range> GROUP BY <group-by tag>, <time aggr. by interval>

Dashboard service reads [config.js](https://github.com/xqbase/metric/blob/master/dashboard-sql/src/main/webapp/config.js) file (`../webapp/config.js` relative to jar file if dashboard service is embedded in metric collector) to show shortcuts in Tab bar.

In INDEX_NAV, each line corresponds to a tab. For each item in a tab:

- Title: Tab title
- Metric Name: Metric name (should be prefixed with `${metric.prefix}` because we focus metrics from our clients only)
- Method: Aggregate method, such as sum, count, avg or max
- Group By: Tag to group-by
- Query: Where conditions
- Interval: Time interval. By default dashboard shows 100 dots. So "1" (1 minute) can show 100 minutes' metric data; "60" (1 hour) can show 4 days' data
- Until Midnight: By default dashboard shows metric data until now. If "Until Midnight" is set, dashboard shows metric data until last midnight. This is useful to get a day trend chart when "Interval" is set to "1440" (1 day)
- Active: "true" if this tab is the default tab. If all tabs are "false", "All Metrics" tab will be the default tab

## Data Storage and Dashboard API

In SQL database storage, metric meta data is stored in table `metric_name` and metric value is stored in table `metric_minute` and `metric_quarter`.

Every 15 minutes (2nd, 17th, 32nd, 47th minute in each hour) metric collector aggregates last quarter's metric data from `metric_minute` into `metric_quarter`.

By default `metric_minute` keeps 2 days (2880 minutes) and `metric_quarter` keeps 30 days (2880 quarters). So metric value can be kept 2 days at minute precision, and kept 30 days at quarter precision.

And there are 3 APIs to get metric meta data and values (regardless of storage type):

- List metric names: `http://<metric-server>:5514/api/name`
- List tags for a metric: `http://<metric-server>:5514/api/tags/<metric-name>`
- Get values: `http://<metric-server>:5514/api/<aggr-method>/<metric-name>?_end=<until time>&_interval=<interval>&_length=<how many dots>&_group_by=<group-by tag>&<tags conditions>`

Here `_end` is the minute or quarter count to epoch (`1970-01-01T00:00:00Z`). It can be omitted if query until now.

If <metric-name> begins with `_quarter.`, it loads data from `metric_quarter` instead of `metric_minute`. In this case, unit of `_end` and `_interval` is "quarter" instead of "minute".

See [Dashboard.java](https://github.com/xqbase/metric/blob/master/collector-sql/src/main/java/com/xqbase/metric/Dashboard.java) for more details.

Dashboard API is useful for monitoring and alerting. For example, the following request returns system CPU load percentage in last 2 minutes (last 1 may be zero because data may not be collected at that time):

    http://<metric-server>:5514/api/metric.server.cpu/max?_length=2&_interval=1&type=system

It is easy to write a bash or python script to alert if CPU load is greater than a value (e.g. 50%) and configure it into cron or Jenkins.
