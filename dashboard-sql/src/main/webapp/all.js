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
	var metricNames = eval("(" + xhr.responseText + ")");
	for (var i = 0; i < metricNames.length; i ++) {
		var metricName = metricNames[i];
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
xhr.open("GET", DASHBOARD_API + "_/names?_r=" + Math.random(), true);
xhr.send(null);