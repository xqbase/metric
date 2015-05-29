var DASHBOARD_HEIGHT = 600;
var DASHBOARD_LENGTH = 100;
var DASHBOARD_SERIES = 10;
var DASHBOARD_TAGS_TOP = true;
var DASHBOARD_CHART_TYPE = "line";
var DASHBOARD_MARKER = false;
var DASHBOARD_API = "api/";

var INDEX_NAV = [
	["Size", "dashboard.html?_r=s#_name=metric.size&_method=max&_interval=15&_group_by=name", true],
	["Rows", "dashboard.html?_r=r#_name=metric.rows&_method=sum&_interval=15&_group_by=name"],
	["Throughput", "dashboard.html?_r=t#_name=metric.throughput&_method=sum&_interval=15&_group_by=remote_addr"],
];