using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.AspNetCore;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// Background service that hosts an MCP server over Streamable HTTP transport.
/// Allows LLM clients to discover and call monitoring tools via http://localhost:{port}.
/// </summary>
public sealed class McpHostService : BackgroundService
{
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly MuteRuleService _muteRuleService;
    private readonly DuckDbInitializer _duckDb;
    private readonly int _port;
    private WebApplication? _app;

    /// <summary>
    /// The port this host was built to listen on. Exposed so a caller reporting the endpoint's state asks
    /// the running host rather than re-reading settings.json, which can have become unreadable since the
    /// host started and would then answer with a default that is not the live port (#2431).
    /// </summary>
    public int Port => _port;

    public McpHostService(LocalDataService dataService, ServerManager serverManager, MuteRuleService muteRuleService, DuckDbInitializer duckDb, int port)
    {
        _dataService = dataService;
        _serverManager = serverManager;
        _muteRuleService = muteRuleService;
        _duckDb = duckDb;
        _port = port;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var builder = WebApplication.CreateBuilder();

            builder.WebHost.ConfigureKestrel(options =>
            {
                options.ListenLocalhost(_port);
            });

            /* Suppress ASP.NET Core console logging — route to app logger instead */
            builder.Logging.ClearProviders();
            builder.Logging.SetMinimumLevel(LogLevel.Warning);

            /* Register services that MCP tools need via dependency injection */
            builder.Services.AddSingleton(_dataService);
            builder.Services.AddSingleton(_serverManager);
            builder.Services.AddSingleton(_muteRuleService);
            var planFetcher = new SqlPlanFetcher(_serverManager);
            builder.Services.AddSingleton(new AnalysisService(_duckDb, planFetcher));

            /* Register MCP server with all tool classes */
            builder.Services
                .AddMcpServer(options =>
                {
                    options.ServerInfo = new()
                    {
                        Name = "PerformanceMonitorLite",
                        Version = "1.2.0"
                    };
                    options.ServerInstructions = McpInstructions.Text;
                })
                /* Stateless mode: each request is self-contained (no Mcp-Session-Id round-trip).
                   Required for clients like Google Antigravity that don't echo the session id,
                   which otherwise connect but list zero tools (issue #1074). */
                .WithHttpTransport(options => options.Stateless = true)
                /* WithGeminiCompatibleTools (not the SDK's WithTools) rewrites parameter schemas into
                   the subset Gemini/Antigravity accepts — collapsing nullable type unions and
                   dropping the default keyword. The companion to stateless transport for issue #1074. */
                .WithGeminiCompatibleTools<McpDiscoveryTools>()
                .WithGeminiCompatibleTools<McpHealthTools>()
                .WithGeminiCompatibleTools<McpWaitTools>()
                .WithGeminiCompatibleTools<McpBlockingTools>()
                /* #2028 get_plan_corrections — automatic plan correction activity + per-database
                   FORCE_LAST_GOOD_PLAN enablement; twin of Darling's DarlingMcpPlanCorrectionTools. */
                .WithGeminiCompatibleTools<McpPlanCorrectionTools>()
                /* #2029 get_pvs_stats — the ADR persistent version store; twin of Darling's DarlingMcpPvsTools. */
                .WithGeminiCompatibleTools<McpPvsTools>()
                .WithGeminiCompatibleTools<McpLongQueryTools>()
                .WithGeminiCompatibleTools<McpQueryTools>()
                .WithGeminiCompatibleTools<McpCpuTools>()
                .WithGeminiCompatibleTools<McpMemoryTools>()
                .WithGeminiCompatibleTools<McpIoTools>()
                .WithGeminiCompatibleTools<McpTempDbTools>()
                .WithGeminiCompatibleTools<McpPerfmonTools>()
                .WithGeminiCompatibleTools<McpAlertTools>()
                .WithGeminiCompatibleTools<McpJobTools>()
                .WithGeminiCompatibleTools<McpPlanTools>()
                .WithGeminiCompatibleTools<McpConfigTools>()
                .WithGeminiCompatibleTools<McpServerInfoTools>()
                .WithGeminiCompatibleTools<McpSessionTools>()
                .WithGeminiCompatibleTools<McpObjectStatsTools>()
                .WithGeminiCompatibleTools<McpLatchSpinlockTools>()
                .WithGeminiCompatibleTools<McpPlanCacheSchedulerTools>()
                .WithGeminiCompatibleTools<McpConfigHistoryTools>()
                .WithGeminiCompatibleTools<McpDefaultTraceTools>()
                .WithGeminiCompatibleTools<McpHealthParserTools>()
                .WithGeminiCompatibleTools<McpAnalysisTools>();

            _app = builder.Build();

            /* DNS-rebinding guard (#1648) — the FIRST middleware, running the SAME shared decision Darling's
               web dashboard and MCP host install (HostHeaderGuard; never a second copy of the rule). This host
               is loopback-only and tokenless, so a browser on this machine that loads attacker content could be
               rebound to 127.0.0.1:{port} and reach every tool same-origin; application/json does not force a
               CORS preflight under a rebind, because the browser considers the request same-origin. Loopback-
               only means no configured listen IP, so ONLY loopback Hosts (localhost / 127.0.0.1 / ::1) and an
               absent Host pass — a rebound foreign hostname is rejected 400 before MapMcp or any tool handler.
               Severity here is informational (single-user desktop app, no service-account privilege), but the
               guard is shared code and covers both apps rather than only the one that was exploitable. */
            _app.Use(async (context, next) =>
            {
                if (!HostHeaderGuard.IsAllowedHost(context.Request.Host.Host, networkListenIp: null))
                {
                    context.Response.StatusCode = StatusCodes.Status400BadRequest;
                    return;
                }

                await next(context);
            });

            _app.MapMcp();

            AppLogger.Info("MCP", $"Starting MCP server on http://localhost:{_port}");

            await _app.RunAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* Normal shutdown */
        }
        catch (Exception ex)
        {
            AppLogger.Error("MCP", $"MCP server failed: {ex.Message}");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_app != null)
        {
            AppLogger.Info("MCP", "Stopping MCP server");
            await _app.StopAsync(cancellationToken);
            await _app.DisposeAsync();
            _app = null;
        }

        await base.StopAsync(cancellationToken);
    }
}
