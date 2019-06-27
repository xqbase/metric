var html = "<li><a href=\"#\" src=\"all.html\">All Metrics</a></li>";
var navLen = INDEX_NAV.length;
for (var i = 0; i < INDEX_NAV.length; i ++) {
	var entry = INDEX_NAV[i];
	if (entry.length <= 1) {
		continue;
	}
	var src = "dashboard.html?_r=" + i + "#_name=" + entry[1] +
			"&_method=" + (entry.length <= 2 ? "sum" : entry[2]) +
			(entry.length <= 3 || entry[3] == "" ? "" : "&_group_by=" + entry[3]) +
			(entry.length <= 4 || entry[4] == "" ? "" : "&" + entry[4]) +
			(entry.length <= 5 ? "" : "&_interval=" + entry[5]);
	if (entry.length > 6 && entry[6]) {
		var today = new Date();
		src += "&_until=" + (new Date(today.getFullYear(), today.getMonth(), today.getDate()).getTime() / 60000 + 1440);
	}
	var active = entry.length > 7 && entry[7];
	html += "<li" + (active ? " class=\"active\" " : "") + "><a href=\"#\" src=\"" + src + "\">" + entry[0] + "</a></li>";
	if (active) {
		$("#dashboard iframe").attr("src", src);
	}
}
$("#nav .nav").html(html);
$("#nav a").click(function() {
	$(this).tab("show");
	$("#dashboard iframe").attr("src", $(this).attr("src"));
});