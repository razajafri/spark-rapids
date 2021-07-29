---
layout: page
title: Supported Operators
nav_order: 6
---

# Execution for Spark 3.0.1-databricks
<table>
<tr>
<th>Executor</th>
<th>Description</th>
<th>Notes</th>
<th>BOOLEAN</th>
<th>BYTE</th>
<th>SHORT</th>
<th>INT</th>
<th>LONG</th>
<th>FLOAT</th>
<th>DOUBLE</th>
<th>DATE</th>
<th>TIMESTAMP</th>
<th>STRING</th>
<th>DECIMAL</th>
<th>NULL</th>
<th>BINARY</th>
<th>CALENDAR</th>
<th>ARRAY</th>
<th>MAP</th>
<th>STRUCT</th>
<th>UDT</th>
</tr>
<tr>
<td>RunningWindowFunctionExec</td>
<td>Databricks-specific window function exec, for "running" windows, i.e. (UNBOUNDED PRECEDING TO CURRENT ROW)</td>
<td>None</td>
<td>S</td>
<td>S</td>
<td>S</td>
<td>S</td>
<td>S</td>
<td>S</td>
<td>S</td>
<td>S</td>
<td>S*</td>
<td>S</td>
<td>S*</td>
<td>S</td>
<td><b>NS</b></td>
<td><b>NS</b></td>
<td><em>PS* (missing nested NULL, BINARY, CALENDAR, MAP, UDT)</em></td>
<td><b>NS</b></td>
<td><em>PS* (missing nested NULL, BINARY, CALENDAR, MAP, UDT)</em></td>
<td><b>NS</b></td>
</tr> 
</table>
/table>
