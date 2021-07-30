### Expressions for Spark 3.0.1-databricks

Name | SQL Function(s) | Description | Default Value | Notes
-----|-----------------|-------------|---------------|------
<a name="sql.expression.If"></a>spark.rapids.sql.expression.If|`if`|IF expression|true|None|
<a name="sql.expression.StringLocate"></a>spark.rapids.sql.expression.StringLocate|`position`, `locate`|Substring search operator|true|None|
<a name="sql.expression.TimeSub"></a>spark.rapids.sql.expression.TimeSub| |Subtracts interval from timestamp|true|None|

### Execution for Spark 3.0.1-databricks

Name | Description | Default Value | Notes
-----|-------------|---------------|------------------
<a name="sql.exec.RunningWindowFunctionExec"></a>spark.rapids.sql.exec.RunningWindowFunctionExec|Databricks-specific window function exec, for "running" windows, i.e. (UNBOUNDED PRECEDING TO CURRENT ROW)|true|None|

