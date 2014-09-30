<?php
$metric_servers = array();
$metric_rows = "";

// void metric_add_server(string $host, int $port)
function metric_add_server($host, $port) {
	global $metric_servers;
	$metric_servers[] = array($host, $port);
}

// void metric_append(string $name, string $value, string... $tags)
function metric_append() {
	global $metric_rows;
	$numargs = func_num_args();
	$args = func_get_args();
	if ($numargs < 2) {
		return;
	}
	$row = urlencode($args[0]) . "/" . $args[1];
	if ($numargs >= 4) {
		$row .= "?";
		for ($i = 2; $i < $numargs - 1; $i += 2) {
			$row .= urlencode($args[$i]) . "=" . urlencode($args[$i + 1]) . "&";
		}
		$row = substr($row, 0, strlen($row) - 1);
	}
	$metric_rows .= $row . "\n";
}

function metric_send() {
	global $metric_servers, $metric_rows;
	$packet = zlib_encode($metric_rows, ZLIB_ENCODING_DEFLATE);
	foreach ($metric_servers as $server) {
		// $socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
		// socket_sendto($socket, $packet, strlen($packet), 0, $host, $port);
		// socket_close($socket);
		$fp = fsockopen("udp://" . $server[0], $server[1]);
		fwrite($fp, $packet);
		fclose($fp);
	} 
}
?>