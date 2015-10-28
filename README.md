# XQBase Metric

A lightweight metric framework for aggregating, collecting and showing metric data.

For basic concepts of metric, see [Metric and Dashboard](http://www.slideshare.net/auntyellow/metric-and-dashboard) in SlideShare, or download [here](https://github.com/xqbase/metric/raw/master/doc/Metric%20and%20Dashboard.ppt).

## How to use Metric in server codes?

Import the metric-client library as a maven dependency:

```xml
<dependency>
	<groupId>com.xqbase</groupId>
	<artifactId>xqbase-metric-client</artifactId>
	<version>0.2.5</version>
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
</filter>
<filter-mapping>
	<filter-name>MetricFilter</filter-name>
	<url-pattern>/*</url-pattern>
</filter-mapping>
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

## How to deploy a Metric server?

- Install MongoDB
- Deploy Metric Collector
- Deploy and Configure Dashboard
