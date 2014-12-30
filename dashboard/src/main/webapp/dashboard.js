var MINUTE = 60000;
var METHOD_NAME = ["sum", "count", "avg", "max", "min", "std"];
var METHOD_COMPARATOR = [
	function(tag1, tag2) {
		return tag2._sum - tag1._sum;
	},
	function(tag1, tag2) {
		return tag2._count - tag1._count;
	},
	function(tag1, tag2) {
		return tag2._sum / tag2._count - tag1._sum / tag1._count;
	},
	function(tag1, tag2) {
		return tag2._max - tag1._max;
	},
	function(tag1, tag2) {
		return tag2._min - tag1._min;
	},
	function(tag1, tag2) {
		var base1 = tag1._sqr * tag1._count - tag1._sum * tag1._sum;
		base1 = base1 < 0 ? 0 : Math.sqrt(base1) / tag1._count;
		var base2 = tag2._sqr * tag2._count - tag2._sum * tag2._sum;
		base2 = base2 < 0 ? 0 : Math.sqrt(base2) / tag2._count;
		return base2 - base1;
	},
];
var INTERVAL = [1, 5, 15, 60, 360, 1440];
var INTERVAL_TEXT = ["1 Minute", "5 Minutes", "15 Minutes", "1 Hour", "6 Hours", "1 Day"];

var HIGHCHARTS_OPTIONS = {
	chart: {
		animation: false,
		renderTo: "divChart",
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
				enabled: false,
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

var paramMap, metricName, method, interval, now, until, tagMap, apiUrl, html;
var timer = null, chart = null, groupKeys = [];
var selectedMethod, selectedGroup, selectedInterval, selectedNow, selectedHour, selectedMinute;

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
		paramMap[unescape(param.substring(0, j))] = unescape(param.substring(j + 1));
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
	$("#spnMethod").html(METHOD_NAME[method].toUpperCase());

	interval = paramMap._interval;
	interval = typeof interval == "undefined" ? "1" : interval;
	interval = parseInt(interval);
	var index = Math.max(0, INTERVAL.indexOf(interval));
	interval = INTERVAL[index];
	selectedInterval = index < 0 ? 0 : index;
	$("#spnInterval").html(INTERVAL_TEXT[index]);

	apiUrl = DASHBOARD_API + (interval < 15 ? "" : "_quarter.") + escape(metricName) + "/" + METHOD_NAME[method] +
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
		$("#btnNow").addClass("active");
		selectedNow = true;
	} else {
		$("#btnNow").removeClass("active");
		selectedNow = false;
	}
	clickNow();

	$("#txtDate").val(date.getFullYear() + "-" + (date.getMonth() + 1) + "-" + date.getDate());
	selectedHour = date.getHours();
	$("#spnHour").html(("" + (100 + selectedHour)).substring(1));
	selectedMinute = Math.floor(date.getMinutes() / 15);
	$("#spnMinute").html(("" + (100 + selectedMinute * 15)).substring(1));

	if (reload) {
		showTags();
		return;
	}
	var xhr = new XMLHttpRequest();
	try {
		xhr.withCredentials = true;
	} catch (e) {
		// Ignore
	}
	xhr.onload = function() {
		tagMap = eval("(" + xhr.responseText + ")");
		showTags();
	};
	xhr.open("GET", DASHBOARD_API + escape(metricName) + "/tags?_r=" + Math.random(), true);
	xhr.send(null);
}

function showTags() {
	var html = "";
	var groupBy = paramMap._group_by;
	var groupHtml = "<li onclick=\"clickGroup('_')\"><a>===NONE===</a></li>";
	$("#spnGroup").html("===NONE===");

	var methodComparator = METHOD_COMPARATOR[method];
	for (var tagName in tagMap) {
		var tags = tagMap[tagName];
		tags.sort(methodComparator);
		html += tagName + "=<select id=\"tag_" + tagName + "\">";
		var selectedTag = paramMap[tagName];
		html += "<option ";
		if (typeof selectedTag == "undefined") {
			html += "selected ";
		}
		html += "value=\"_\">===ALL===</option>";
		for (var i = 0; i < tags.length; i ++) {
			var tag = tags[i];
			html += "<option ";
			if (selectedTag == tag._value) {
				apiUrl += "&" + escape(tagName) + "=" + escape(selectedTag);
				html += "selected ";
			}
			html += "value=\"" + tag._value + "\">" + tag._value + "</option>";
		}
		html += "</select>\n";

		groupHtml = "<li onclick=\"clickGroup('" + tagName + "')\"><a>" + tagName + "</a></li>" + groupHtml;
		if (groupBy == tagName) {
			apiUrl += "&_group_by=" + groupBy;
			$("#spnGroup").html(groupBy);
		}
	}
	apiUrl += "&_end=";

	divTags.innerHTML = html;
	$("#ulGroup").html(groupHtml);

	chart = null;
	if (timer != null) {
		clearInterval(timer);
	}
	timer = now ? setInterval(requestApi, interval < 15 ? MINUTE : MINUTE * 15) : null;
	requestApi();
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
	chart.setTitle({text: metricName}, null, false);
	chart.yAxis[0].setTitle({text: METHOD_NAME[method].toUpperCase()}, false);
	for (var i = 0; i < groupKeys.length; i ++) {
		var key = groupKeys[i];
		chart.addSeries({
			name: key,
			pointStart: pointStart,
			pointInterval: pointInterval,
			data: data[key],
			zIndex: groupKeys.length - i,
			visible: i < DASHBOARD_SERIES,
		}, false);
	}
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

function clickMethod(i) {
	selectedMethod = i;
	$("#spnMethod").html(METHOD_NAME[i].toUpperCase());
}

function clickGroup(groupBy) {
	selectedGroup = groupBy;
	$("#spnGroup").html(groupBy == "_" ? "===NONE===" : groupBy);
}

function clickInterval(i) {
	selectedInterval = i;
	$("#spnInterval").html(INTERVAL_TEXT[i]);
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

function clickHour(i) {
	selectedHour = i;
	$("#spnHour").html(("" + (100 + i)).substring(1));
}

function clickMinute(i) {
	selectedMinute = i;
	$("#spnMinute").html(("" + (100 + i * 15)).substring(1));
}

$("#divChart").css({
	height: DASHBOARD_HEIGHT + "px",
});

Highcharts.setOptions({
	global: {
		useUTC: false,
	},
});

html = "";
for (var i = 0; i < METHOD_NAME.length; i ++) {
	html = "<li onclick=\"clickMethod(" + i + ")\"><a>" + METHOD_NAME[i].toUpperCase() + "</a></li>" + html;
}
$("#ulMethod").html(html);

html = "";
for (var i = 0; i < INTERVAL_TEXT.length; i ++) {
	html = "<li onclick=\"clickInterval(" + i + ")\"><a>" + INTERVAL_TEXT[i] + "</a></li>" + html;
}
$("#ulInterval").html(html);

$("#txtDate").datepicker({
	autoclose: true,
	format: "yyyy-mm-dd",
	orientation: "bottom",
});

html = "";
for (var i = 0; i < 24; i ++) {
	html = "<li onclick=\"clickHour(" + i + ")\"><a>" + ("" + (100 + i)).substring(1) + "</a></li>" + html;
}
$("#ulHour").html(html);

html = "";
for (var i = 0; i < 4; i ++) {
	html = "<li onclick=\"clickMinute(" + i + ")\"><a>" + ("" + (100 + i * 15)).substring(1) + "</a></li>" + html;
}
$("#ulMinute").html(html);

$("#btnNow").click(function() {
	selectedNow = !selectedNow;
	clickNow();
});

$("#btnSubmit").click(function() {
	var hash = "#_name=" + escape(metricName) + "&_method=" + METHOD_NAME[selectedMethod] +
			"&_interval=" + INTERVAL[selectedInterval];
	if (selectedGroup != "_") {
		hash += "&_group_by=" + selectedGroup;
	}
	if (!selectedNow) {
		var ymd = $("#txtDate").val().split("-");
		var time = (ymd.length == 3 ? new Date(ymd[0], ymd[1] - 1, ymd[2]) : new Date()).getTime(); 
		hash += "&_until=" + (Math.floor(time / MINUTE) +
				selectedHour * 60 + selectedMinute * 15);
	}
	for (var tagName in tagMap) {
		var sel = window["tag_" + tagName];
		var value = sel.options[sel.selectedIndex].value;
		if (value != "_") {
			hash += "&" + escape(tagName) + "=" + escape(value);
		}
	}
	location.href = hash;
	loadParams(true);
});

loadParams(false);