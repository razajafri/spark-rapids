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

# Expressions for Spark 3.0.1-databricks
<table>
<tr>
<th>Expression</th>
<th>SQL Functions(s)</th>
<th>Description</th>
<th>Notes</th>
<th>Context</th>
<th>Param/Output</th>
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
<td rowSpan="8">RegExpReplace</td>
<td rowSpan="8">`regexp_replace`</td>
<td rowSpan="8">RegExpReplace support for string literal input patterns</td>
<td rowSpan="8">None</td>
<td rowSpan="4">project</td>
<td>str</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td>S</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td>regex</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td><em>PS (very limited regex support; Literal value only)</em></td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td>rep</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td><em>PS (Literal value only)</em></td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td>result</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td>S</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td rowSpan="4">lambda</td>
<td>str</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td><b>NS</b></td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td>regex</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td><b>NS</b></td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td>rep</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td><b>NS</b></td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td>result</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td><b>NS</b></td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td rowSpan="6">TimeSub</td>
<td rowSpan="6"> </td>
<td rowSpan="6">Subtracts interval from timestamp</td>
<td rowSpan="6">None</td>
<td rowSpan="3">project</td>
<td>start</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td>S*</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td>interval</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td><em>PS (months not supported; Literal value only)</em></td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td>result</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td>S*</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td rowSpan="3">lambda</td>
<td>start</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td><b>NS</b></td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td>interval</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td><b>NS</b></td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
<tr>
<td>result</td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td><b>NS</b></td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
<td> </td>
</tr>
</table>

