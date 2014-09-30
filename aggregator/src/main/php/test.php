<?php
require_once "./metric.php";
metric_add_server("localhost", 5514);
metric_append("test", 1, "r", "_");
metric_append("test", 2, "r", "_");
metric_append("test", 3, "r", "_");
metric_append("test", 4, "r", "_");
metric_append("test", 5, "r", "_");
metric_send();
?>