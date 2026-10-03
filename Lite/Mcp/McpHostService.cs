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
    private readonly ScheduleManager? _scheduleManager;
    private readonly int _port;
    private WebApplication? _app;

    /// <summary>
    /// The port this host was built to listen on. Exposed so a caller reporting the endpoint's state asks
    /// the running host rather than re-reading settings.json, which can have become unreadable since the
    /// host started and would then answer with a default that is not the live port (#2431).
    /// </summary>
    public int Port => _port;

    /// <param name="scheduleManager">#3896: the collector schedules, so the analysis tools bound each
    /// latest-value read by the cadence its collector runs at. Null bounds by the shipped defaults.</param>
    public McpHostService(LocalDataService dataService, ServerManager serverManager, MuteRuleService muteRuleService, DuckDbInitializer duckDb, int port, ScheduleManager? scheduleManager = null)
    {
        _dataService = dataService;
        _serverManager = serverManager;
        _muteRuleService = muteRuleService;
        _duckDb = duckDb;
        _port = port;
        _scheduleManager = scheduleManager;

        /* #4999: the health read judges each collector by the interval it is scheduled at on that server, the
           same answer the analysis tools bound their reads by (see RegisterAnalysisService below), not by the
           cadence it shipped with. Null leaves this instance on the app-wide answer every LocalDataService reads
           (LocalDataService.DefaultCollectorFrequencyMinutes), which is none when nothing set one, so every row
           then keeps its shipped cadence. */
        dataService.CollectorFrequencyMinutes = scheduleManager is null
            ? null
            : (serverId, collector) => scheduleManager.GetFrequencyForStorageServer(serverManager, serverId, collector);
    }

    /// <summary>
    /// #4726: registers the analysis service TRANSIENT, so every MCP call gets its own instance. One shared instance
    /// answered a second, overlapping analyze_server call with an empty list (its busy check) and the tool then read
    /// the FIRST call's running state, so the second server got "No significant findings" for a server it never
    /// analyzed. The scheduler and the Recommendations tab keep their own instances. Every instance is handed the
    /// store's ONE shared baseline tier (#3941), so an analysis hour a scheduled pass already computed reads no
    /// 30-day baseline. Extracted so a test resolves the service from the production registration instead of a
    /// hand-copied one that could drift from it.
    /// </summary>
    internal static void RegisterAnalysisService(
        IServiceCollection services,
        DuckDbInitializer duckDb,
        IPlanFetcher planFetcher,
        ServerManager serverManager,
        ScheduleManager? schedules)
    {
        services.AddTransient<AnalysisService>(_ => new AnalysisService(
            duckDb,
            planFetcher,
            collectorFrequencyMinutes: schedules is null
                ? null
                : (serverId, collector) => schedules.GetFrequencyForStorageServer(serverManager, serverId, collector),
            baselineCache: BaselineCache.For(duckDb)));
    }

    /// <summary>
    /// #4938: registers the run-time source get_collection_health reads: the collector schedules the sweep reads, and the
    /// server list a storage id is looked up in. The tool takes the service as an optional parameter, so a host that left
    /// this registration out would not fail on a call: the parameter would keep its null default and every collector would
    /// read as having no run time. Extracted so a test resolves the service from the production registration instead of a
    /// hand-built one that could drift from it.
    /// </summary>
    internal static void RegisterCollectorRunTimes(IServiceCollection services, ScheduleManager? schedules, ServerManager serverManager)
    {
        services.AddSingleton(new McpCollectorRunTimes(schedules, serverManager));
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
            /* #4938: get_collection_health shows each collector's run time and next run, from the schedule the sweep reads.
               Registered through the method a test also calls (see RegisterCollectorRunTimes). */
            RegisterCollectorRunTimes(builder.Services, _scheduleManager, _serverManager);
            var planFetcher = new SqlPlanFetcher(_serverManager);
            /* #4726: registered PER CALL, through the method a test also calls (see RegisterAnalysisService). */
            RegisterAnalysisService(builder.Services, _duckDb, planFetcher, _serverManager, _scheduleManager);

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
                .WithGeminiCompatibleTools<McpAnalysisTools>()
                /* #3898 D1: get_tool_guide serves the reading guides tools/list leaves out (the tails split off
                   at McpToolGuide.Marker in WithGeminiCompatibleTools) and the cross-tool topics. Darling twin:
                   DarlingMcpToolGuideTools. */
                .WithGeminiCompatibleTools<McpToolGuideTools>()
                /* The unknown-argument guard (#3870): ONE call-tool filter, registered once, covering
                   every tool with no per-tool change. A call carrying an argument no tool parameter
                   declares is refused before dispatch — the refusal names the key and lists what the
                   tool accepts — instead of being run with the key silently dropped. The SDK binds
                   arguments by name and ignores the rest, so a misremembered parameter used to produce
                   an answer to a different question with nothing anywhere saying so; for a surface whose
                   callers are language models, a silently dropped key is a confidently wrong answer.
                   The SAME filter object Darling's host registers (shared from
                   PerformanceMonitor.Common), so the two SKUs cannot refuse differently.

                   #4198 ruled out a second filter here that trimmed any oversized result after the
                   fact: it would make a tool's own truncated/*_returned fields wrong, cut calls that
                   explicitly asked for more rows, and drop the newest rows of anything sorted
                   oldest-first. Each tool sizes its own defaults to fit instead — see
                   McpResponseBudget.DefaultBytes, the one shared size target both SKUs read. */
                .WithRequestFilters(filters => filters
                    .AddCallToolFilter(McpUnknownArgumentGuard.Instance));

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
