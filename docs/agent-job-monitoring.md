# Agent Job Monitoring

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

Every edition monitors currently running SQL Agent jobs and flags jobs running longer than expected.

| Metric | How It Works |
|---|---|
| **Current duration** | Elapsed time since the job started |
| **Average duration** | Historical mean from successful completions in `msdb.dbo.sysjobhistory` |
| **p95 duration** | 95th percentile from historical completions |
| **Running long flag** | Set when current duration exceeds the p95 threshold |

Lite and Darling query `msdb` directly on each collection cycle; all editions expose this data through the MCP `get_running_jobs` tool. Gracefully skipped on Azure SQL Database, AWS RDS for SQL Server, and environments without SQL Server Agent. On Azure SQL Database and AWS RDS for SQL Server the MCP reads (`get_running_jobs`, and `get_agent_alerts` in Darling) answer `not_collected` with the reason, because those collectors never run there, the Darling Viewer's Running Jobs grid and Agent Alerts panel and the Lite Running Jobs tab say the same, and fleet `get_collection_health` lists the server as not applicable with the same reason.
