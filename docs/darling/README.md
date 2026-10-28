# Darling guides

Audience: operators.

Read these pages in this order. After the install, each page is complete on its own.

1. [Install Darling](install.md): from a downloaded zip to a service that collects data.
2. [The WPF viewer](viewer.md): the desktop app, on the service host or on another machine.
3. [The web dashboard](web-dashboard.md): the browser view that the service serves.
4. [MCP server](../mcp-server.md#darling): the endpoint for an LLM client.
5. [Running Darling day to day](admin.md): add or remove a server, change a password, rotate a token, pause, upgrade, uninstall, and where everything is.
6. [Command line reference](cli.md): every verb of the service exe, with exit codes.
7. [Darling and your monitored servers](monitored-servers.md): what the service does on each SQL Server, its Extended Events sessions, and the permissions it needs.
8. [PostgreSQL Targets](postgresql-targets.md): monitor a PostgreSQL server.
9. [Managed Bundled PostgreSQL](managed-postgresql.md): the store that the service runs for itself.
10. [Configuration Reference](configuration.md): where each setting lives, `postgres`, `servers`, `peers`, `heartbeat` and `reads`.
11. [Alert and notification settings](alerts-and-notifications.md): the `alerts`, `analysis`, `smtp` and `webhooks` blocks, and the notification routes.
12. [Agents and alert sources](agents-and-alert-sources.md): hand an alert to your own agent, practice requests, and logins for systems that send alerts.
13. [MCP configuration](configuration-mcp.md): the `mcp` block.
14. [MCP tools and their store tables](mcp-tool-tables.md): every MCP tool and the store tables that it reads or changes.
15. [Web dashboard configuration](configuration-web.md): the `web` block.
16. [Collector schedules and switches](collector-settings.md): why there are no schedule settings, and the verbs and tools that change a collector.
17. [Operations](operations.md): the store, TimescaleDB, retention, the logs, restarts and watching the service from outside.
18. [Security and agent changes](security.md): the store's schemas and roles, the agent change list and its undo, and Level 2.
19. [Opt-in Network Endpoints (LAN)](network-endpoints.md): reach the store, MCP and the web dashboard from another machine.
20. [Troubleshooting](../troubleshooting.md#darling): a table of what you see, the likely cause and the fix, for Darling and Lite.
21. [Darling troubleshooting](troubleshooting.md): every service log line and what it means.

The [Darling operator guide](../../Darling/README.md) is now a short index. Its old headings link to these pages.
