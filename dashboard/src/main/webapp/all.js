var METHOD_NAME = ["sum", "count", "avg", "max", "min", "std"];

$("#tbody").html("Loading Meta Data ...");
var xhr = new XMLHttpRequest();
try {
	xhr.withCredentials = true;
} catch (e) {
	// Ignore
}
xhr.onload = function() {
	var html = "";
	var metricNames = [];
	for (var metricName in eval("(" + xhr.responseText + ")")) {
		metricNames.push(metricName.substring(0, 9) == "_quarter." ? metricName.substring(9) : metricName);
	}
	metricNames.sort();
	var lastName = "";
	for (var i = 0; i < metricNames.length; i ++) {
		var metricName = metricNames[i];
		if (metricName == lastName) {
			continue;
		}
		lastName = metricName;
		html += "<tr><td>" + metricName + "</td><td>";
		for (var j = 0; j < METHOD_NAME.length; j ++) {
			var methodName = METHOD_NAME[j];
			html += "<a class=\"label label-info\" title=\"" + metricName + "/" + methodName +
					"\" href=\"dashboard.html#_name=" + encodeURIComponent(metricName) + "&_method=" +
					methodName + "\" target=\"_blank\">" + methodName.toUpperCase() + "</a> ";
		}
		html += "</td></tr>";
	}
	$("#tbody").html(html);
};
xhr.open("GET", DASHBOARD_API + "metric.size/count?_group_by=name&_length=2&_r=" + Math.random(), true);
xhr.send(null);