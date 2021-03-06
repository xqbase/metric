var MINUTE = 60000;
var METHOD_NAME = ["sum", "count", "avg", "max", "min", "std"];
var METHOD_COMPARATOR = [
	function(tag1, tag2) {
		return tag2.sum - tag1.sum;
	},
	function(tag1, tag2) {
		return tag2.count - tag1.count;
	},
	function(tag1, tag2) {
		return tag2.sum / tag2.count - tag1.sum / tag1.count;
	},
	function(tag1, tag2) {
		return tag2.max - tag1.max;
	},
	function(tag1, tag2) {
		return tag2.min - tag1.min;
	},
	function(tag1, tag2) {
		var base1 = tag1.sqr * tag1.count - tag1.sum * tag1.sum;
		base1 = base1 < 0 ? 0 : Math.sqrt(base1) / tag1.count;
		var base2 = tag2.sqr * tag2.count - tag2.sum * tag2.sum;
		base2 = base2 < 0 ? 0 : Math.sqrt(base2) / tag2.count;
		return base2 - base1;
	},
];
var INTERVAL = [1, 5, 15, 60, 360, 1440];
var INTERVAL_TEXT = ["1 Minute", "5 Minutes", "15 Minutes", "1 Hour", "6 Hours", "1 Day"];

var DIV_CHART = DASHBOARD_TAGS_TOP ? "#divChartBottom" : "#divChartTop";
var HIGHCHARTS_OPTIONS = {
	chart: {
		animation: false,
		renderTo: DIV_CHART.substring(1),
		type: DASHBOARD_CHART_TYPE,
	},
	credits: {
		enabled: false,
	},
	legend: {
		align: "right",
		verticalAlign: "top",
		layout: "vertical",
	},
	plotOptions: {
		series: {
			animation: false,
			marker: {
				enabled: DASHBOARD_MARKER,
			},
		},
	},
	tooltip: {
		formatter: function() {
			return (groupKeys.length == 1 ? "" : "<b>" + this.series.name + "</b><br/>") +
					Highcharts.dateFormat("%Y-%m-%d %H:%M:%S", this.x) + "<br/><b>" +
					Highcharts.numberFormat(this.y, 2) + "</b>";
		}
	},
	xAxis: {
		type: "datetime",
	},
};

function APPEND_HTML(html, line) {
	return DASHBOARD_TAGS_TOP ? html + line : line + html;
}

function HTML_ENTITIES(s) {
	return s.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function METRIC_NAME_TEXT() {
	if (typeof DASHBOARD_TEXT != "object") {
		return metricName;
	}
	var o = DASHBOARD_TEXT[metricName];
	if (typeof o != "object") {
		return metricName;
	}
	var text = o["_" + METHOD_NAME[method]];
	if (typeof text != "string") {
		text = o._text;
	}
	return typeof text == "string" ? text : metricName;
}

function TAG_NAME_TEXT(tagName) {
	if (typeof DASHBOARD_TEXT != "object") {
		return tagName;
	}
	var o = DASHBOARD_TEXT[metricName];
	if (typeof o != "object") {
		return tagName;
	}
	o = o[tagName];
	if (typeof o != "object") {
		return tagName;
	}
	o = o._text;
	return typeof o == "string" ? o : tagName;
}

function TAG_VALUE_TEXT(tagName, tagValue) {
	if (typeof DASHBOARD_TEXT != "object") {
		return tagValue;
	}
	var o = DASHBOARD_TEXT[metricName];
	if (typeof o != "object") {
		return tagValue;
	}
	o = o[tagName];
	if (typeof o != "object") {
		return tagValue;
	}
	o = o[tagValue];
	return typeof o == "string" ? o : tagValue;
}

var DROPDOWN_DIV = ["Method", "Group", "Interval", "Hour", "Minute"];
var DROPDOWN_CLASS = DASHBOARD_TAGS_TOP ? "dropdown" : "dropup";

var paramMap, metricName, method, interval, now, until, apiUrl;
var timer = null, chart = null, groupKeys = [];
var selectedMethod, selectedGroup, selectedInterval, selectedNow, selectedHour, selectedMinute, selectedTags;

function loadParams(reload) {
	var hash = location.hash;
	if (hash.charAt(0) == "#") {
		hash = hash.substring(1);
	}
	paramMap = {};
	var params = hash.split("&");
	for (var i = 0; i < params.length; i ++) {
		var param = params[i];
		var j = param.indexOf("=");
		if (j < 0) {
			continue;
		}
		paramMap[decodeURIComponent(param.substring(0, j))] = decodeURIComponent(param.substring(j + 1));
	}

	metricName = paramMap._name;
	if (typeof metricName == "undefined") {
		location.href = "index.html";
		return;
	}

	method = paramMap._method;
	method = typeof method == "undefined" ? "sum" : method;
	method = METHOD_NAME.indexOf(method);
	method = method < 0 ? 0 : method;
	selectedMethod = method;
	$("#spnMethod").text(METHOD_NAME[method].toUpperCase());

	interval = paramMap._interval;
	interval = typeof interval == "undefined" ? "1" : interval;
	interval = parseInt(interval);
	var index = Math.max(0, INTERVAL.indexOf(interval));
	interval = INTERVAL[index];
	selectedInterval = index < 0 ? 0 : index;
	$("#spnInterval").text(INTERVAL_TEXT[index]);

	apiUrl = DASHBOARD_API + (interval < 15 ? "" : "_quarter.") + encodeURIComponent(metricName) + "/" + METHOD_NAME[method] +
			"?_length=" + DASHBOARD_LENGTH + "&_interval=" + (interval < 15 ? interval : interval / 15);

	now = true;
	var date = new Date();
	until = Math.floor(date.getTime() / MINUTE);
	var until_ = paramMap._until;
	if (typeof until_ != "undefined") {
		until_ = parseInt(until_);
		if (!isNaN(until_)) {
			now = false;
			until = until_;
			date.setTime(until * MINUTE);
		}
	}
	if (now) {
		$("#btnReal").button("toggle");
		selectedNow = true;
	} else {
		$("#btnHist").button("toggle");
		selectedNow = false;
	}
	clickNow();

	$("#txtDate").val(date.getFullYear() + "-" + (date.getMonth() + 1) + "-" + date.getDate());
	selectedHour = date.getHours();
	$("#spnHour").text(("" + (100 + selectedHour)).substring(1));
	selectedMinute = Math.floor(date.getMinutes() / 15);
	$("#spnMinute").text(("" + (100 + selectedMinute * 15)).substring(1));

	if (reload) {
		loadParams2();
		return;
	}
	$(DIV_CHART).html("<br>Loading Meta Data ...");
	var xhr = new XMLHttpRequest();
	try {
		xhr.withCredentials = true;
	} catch (e) {
		// Ignore
	}
	xhr.onload = function() {
		showTags(eval("(" + xhr.responseText + ")"));
		loadParams2();
	};
	xhr.open("GET", DASHBOARD_API + encodeURIComponent(metricName) + "/tags?_r=" + Math.random(), true);
	xhr.send(null);
}

function showTags(tagMap) {
	selectedTags = {};
	var groupHtml = "<li value=\"_\"><a>===NONE===</a></li>";
	var tagsHtml = "";
	var methodComparator = METHOD_COMPARATOR[method];
	for (var tagName in tagMap) {
		selectedTags[tagName] = "_";
		groupHtml = APPEND_HTML(groupHtml, "<li value=\"" + tagName + "\"><a>" + TAG_NAME_TEXT(tagName) + "</a></li>");
		var tagsObj = tagMap[tagName];
		var tags = [];
		for (var tagValue in tagsObj) {
			var tag = tagsObj[tagValue];
			tag.value = tagValue;
			tags.push(tag);
		}
		tags.sort(methodComparator);
		tagsHtml +=
				"<div class=\"btn-group\">" +
					"<button type=\"button\" class=\"btn btn-danger disabled\">" + TAG_NAME_TEXT(tagName) + "</button>" +
					"<div class=\"btn-group " + DROPDOWN_CLASS + "\">" +
						"<button type=\"button\" class=\"btn btn-info dropdown-toggle\" data-toggle=\"dropdown\">" +
							"<span id=\"spnTag_" + tagName + "\"></span>" +
							"<span class=\"caret\"></span>" +
						"</button>" +
						"<ul class=\"dropdown-menu\" role=\"menu\" value=\"" + tagName + "\">";
		var valuesHtml = "<li value=\"_\"><a>===ALL===</a></li>";
		for (var i = 0; i < tags.length; i ++) {
			tagValue = tags[i].value;
			valuesHtml = APPEND_HTML(valuesHtml, "<li value=\"" + HTML_ENTITIES(tagValue) + "\"><a>" + HTML_ENTITIES(TAG_VALUE_TEXT(tagName, tagValue)) + "</a></li>");
		}
		tagsHtml += valuesHtml +
						"</ul>" +
					"</div>" +
				"</div> ";
	}

	$("#ulGroup").html(groupHtml);
	$("#ulGroup li").click(function() {
		selectedGroup = $(this).attr("value");
		$("#spnGroup").text(selectedGroup == "_" ? "===NONE===" : TAG_NAME_TEXT(selectedGroup));
	});

	$("#divTags").html(tagsHtml);
	$("#divTags li").click(function() {
		var tagName = $(this).parent().attr("value");
		var tagValue = $(this).attr("value");
		$("#spnTag_" + tagName).text(tagValue == "_" ? "===ALL===" : TAG_VALUE_TEXT(tagName, tagValue));
		selectedTags[tagName] = tagValue;
	});
}

function loadParams2() {
	var groupBy = paramMap._group_by;
	selectedGroup = (typeof groupBy == "undefined" ? "_" : groupBy);
	$("#spnGroup").text(selectedGroup == "_" ? "===NONE===" : TAG_NAME_TEXT(selectedGroup));
	if (selectedGroup != "_") {
		apiUrl += "&_group_by=" + groupBy;
	}
	for (var tagName in selectedTags) {
		var tagValue = paramMap[tagName];
		tagValue = (typeof tagValue == "undefined" ? "_" : tagValue);
		selectedTags[tagName] = tagValue;
		$("#spnTag_" + tagName).text(tagValue == "_" ? "===ALL===" : TAG_VALUE_TEXT(tagName, tagValue));
	}
	for (var tagName in paramMap) {
		if (tagName.charAt(0) != "_") {
			var tagValue = paramMap[tagName];
			if (tagValue != "_") {
				apiUrl += "&" + encodeURIComponent(tagName) + "=" + encodeURIComponent(tagValue);
			}
		}
	}
	apiUrl += "&_end=";

	chart = null;
	if (timer != null) {
		clearInterval(timer);
	}
	timer = now ? setInterval(requestApi, interval < 15 ? MINUTE : MINUTE * 15) : null;
	$(DIV_CHART).html("<br>Loading Metric Data ...");
	requestApi();
}

function requestApi() {
	var xhr = new XMLHttpRequest();
	try {
		xhr.withCredentials = true;
	} catch (e) {
		// Ignore
	}
	xhr.onload = function() {
		drawChart(eval("(" + xhr.responseText + ")"));
		chart.redraw();
		if (interval < 15) {
			until ++;
		} else {
			until += 15;
		}
	};
	xhr.open("GET", apiUrl + (interval < 15 ? until : Math.floor(until / 15)) + "&_r=" + Math.random(), true);
	xhr.send(null);
}

function drawChart(data) {
	var pointStart = until - (DASHBOARD_LENGTH - 1) * interval;
	pointStart = interval < 15 ? pointStart * MINUTE : Math.floor(pointStart / 15) * MINUTE * 15;
	var pointInterval = interval * MINUTE;
	if (chart != null) {
		for (var i = 0; i < groupKeys.length; i ++) {
			chart.series[i].update({
				pointStart: pointStart,
				pointInterval: pointInterval,
				data: data[groupKeys[i]],
			}, false);
		}
		return;
	}
	groupKeys = [];
	var valueMap = {};
	for (var key in data) {
		var line = data[key];
		var value = 0;
		for (var i = 0; i < line.length; i ++) {
			value += line[i];
		}
		groupKeys.push(key);
		valueMap[key] = value;
	}
	groupKeys.sort(function(key1, key2) {
		return valueMap[key2] - valueMap[key1];
	});
	chart = new Highcharts.Chart(HIGHCHARTS_OPTIONS);
	if (groupKeys.length == 1 && groupKeys[0] == "_") {
		chart.options.legend.enabled = false;
	}
	chart.setTitle({text: METRIC_NAME_TEXT()}, null, false);
	chart.yAxis[0].setTitle({text: METHOD_NAME[method].toUpperCase()}, false);
	for (var i = 0; i < groupKeys.length; i ++) {
		var key = groupKeys[i];
		chart.addSeries({
			name: TAG_VALUE_TEXT(selectedGroup, key),
			pointStart: pointStart,
			pointInterval: pointInterval,
			data: data[key],
			zIndex: groupKeys.length - i,
			visible: i < DASHBOARD_SERIES,
		}, false);
	}
}

function clickNow() {
	$("#txtDate").prop("disabled", selectedNow);
	if (selectedNow) {
		$("#btnHour").addClass("disabled");
		$("#btnMinute").addClass("disabled");
	} else {
		$("#btnHour").removeClass("disabled");
		$("#btnMinute").removeClass("disabled");
	}
}

for (var i = 0; i < DROPDOWN_DIV.length; i ++) {
	$("#div" + DROPDOWN_DIV[i]).addClass(DROPDOWN_CLASS);
}

$(DIV_CHART).css({
	height: DASHBOARD_HEIGHT + "px",
});

$(DASHBOARD_TAGS_TOP ? "#divChartTop" : "#divChartBottom").hide();

Highcharts.setOptions({
	global: {
		useUTC: false,
	},
});

var methodHtml = "";
for (var i = 0; i < METHOD_NAME.length; i ++) {
	methodHtml = APPEND_HTML(methodHtml, "<li value=\"" + i + "\"><a>" + METHOD_NAME[i].toUpperCase() + "</a></li>");
}
$("#ulMethod").html(methodHtml);
$("#ulMethod li").click(function() {
	selectedMethod = parseInt($(this).attr("value"));
	$("#spnMethod").text(METHOD_NAME[selectedMethod].toUpperCase());
});

var intervalHtml = "";
for (var i = 0; i < INTERVAL_TEXT.length; i ++) {
	intervalHtml = APPEND_HTML(intervalHtml, "<li value=\"" + i + "\"><a>" + INTERVAL_TEXT[i] + "</a></li>");
}
$("#ulInterval").html(intervalHtml);
$("#ulInterval li").click(function() {
	selectedInterval = parseInt($(this).attr("value"));
	$("#spnInterval").text(INTERVAL_TEXT[selectedInterval]);
});

$("#txtDate").datepicker({
	autoclose: true,
	format: "yyyy-mm-dd",
	orientation: DASHBOARD_TAGS_TOP ? "top" : "bottom",
});

var hourHtml = "";
for (var i = 0; i < 24; i ++) {
	hourHtml = APPEND_HTML(hourHtml, "<li value=\"" + i + "\"><a>" + ("" + (100 + i)).substring(1) + "</a></li>");
}
$("#ulHour").html(hourHtml);
$("#ulHour li").click(function() {
	selectedHour = parseInt($(this).attr("value"));
	$("#spnHour").text(("" + (100 + selectedHour)).substring(1));
});

var minuteHtml = "";
for (var i = 0; i < 4; i ++) {
	minuteHtml = APPEND_HTML(minuteHtml, "<li value=\"" + i + "\"><a>" + ("" + (100 + i * 15)).substring(1) + "</a></li>");
}
$("#ulMinute").html(minuteHtml);
$("#ulMinute li").click(function() {
	selectedMinute = parseInt($(this).attr("value"));
	$("#spnMinute").text(("" + (100 + selectedMinute * 15)).substring(1));
});

$("#btnReal").click(function() {
	selectedNow = true;
	clickNow();
});
$("#btnHist").click(function() {
	selectedNow = false;
	clickNow();
});

$("#btnSubmit").click(function() {
	var hash = "#_name=" + encodeURIComponent(metricName) + "&_method=" + METHOD_NAME[selectedMethod] +
			"&_interval=" + INTERVAL[selectedInterval];
	if (!selectedNow) {
		var ymd = $("#txtDate").val().split("-");
		var time = (ymd.length == 3 ? new Date(ymd[0], ymd[1] - 1, ymd[2]) : new Date()).getTime();
		hash += "&_until=" + (Math.floor(time / MINUTE) +
				selectedHour * 60 + selectedMinute * 15);
	}
	if (selectedGroup != "_") {
		hash += "&_group_by=" + selectedGroup;
	}
	for (var tagName in selectedTags) {
		var tagValue = selectedTags[tagName];
		if (typeof tagValue != "undefined" && tagValue != "_") {
			hash += "&" + encodeURIComponent(tagName) + "=" + encodeURIComponent(tagValue);
		}
	}
	location.href = hash;
	loadParams(true);
});

loadParams(false);