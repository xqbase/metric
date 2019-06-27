var DASHBOARD_HEIGHT = 600;
var DASHBOARD_LENGTH = 100;
var DASHBOARD_SERIES = 10;
var DASHBOARD_TAGS_TOP = true;
var DASHBOARD_CHART_TYPE = "line";
var DASHBOARD_MARKER = false;
var DASHBOARD_API = "api/";

var INDEX_NAV = [
	// [Title, Metric Name, Method, Group By, Query, Interval, Until Midnight, Active],
	["Size", "metric.size", "max", "name", "", 15, false, true],
	["Rows", "metric.rows", "sum", "name", "", 15],
	["Throughput", "metric.throughput", "sum", "remote_addr", "", 15],
	["Tag Values", "metric.tags.values", "sum", "name", "", 15],
	["Tag Combinations", "metric.tags.combinations", "sum", "name", "", 15],
];