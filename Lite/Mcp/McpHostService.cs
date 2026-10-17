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
    private readonly bool _allowWrites;
    private readonly bool _allowInvestigations;
    private readonly McpDataDetail _dataDetail;
    private readonly IInvestigationRequestStore _investigationStore;
    private WebApplication? _app;

    /// <summary>
    /// The port this host was built to listen on. Exposed so a caller reporting the endpoint's state asks
    /// the running host rather than re-reading settings.json, which can have become unreadable since the
    /// host started and would then answer with a default that is not the live port (#2431).
    /// </summary>
    public int Port => _port;

    /// <param name="investigationStore">#5820: the one investigation store, required: <c>get_notification_routes</c> is served in every
    /// configuration and takes it as a service parameter, so a host without it would advertise it as a tool argument nobody can fill.</param>
    /// <param name="scheduleManager">#3896: the collector schedules, so the analysis tools bound each
    /// latest-value read by the cadence its collector runs at. Null bounds by the shipped defaults.</param>
    /// <param name="allowWrites">Whether the write tools are served (<see cref="McpSettings.AllowWrites"/>, the
    /// <c>mcp_allow_writes</c> setting). Default off: a write tool is then absent from the served set and a call
    /// to it fails as an unknown tool. See <see cref="McpWritePolicy"/>.</param>
    public McpHostService(LocalDataService dataService, ServerManager serverManager, MuteRuleService muteRuleService, DuckDbInitializer duckDb, int port, IInvestigationRequestStore investigationStore, ScheduleManager? scheduleManager = null, bool allowWrites = false,
        bool allowInvestigations = false, McpDataDetail dataDetail = McpDataDetail.Full)
    {
        _dataService = dataService;
        _serverManager = serverManager;
        _muteRuleService = muteRuleService;
        _duckDb = duckDb;
        _port = port;
        _scheduleManager = scheduleManager;
        _allowWrites = allowWrites;
        _allowInvestigations = allowInvestigations;
        _dataDetail = dataDetail;
        _investigationStore = investigationStore ?? throw new ArgumentNullException(nameof(investigationStore));

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

    /// <summary>
    /// A POST must carry a JSON Content-Type; any other type, or none, is answered 415 before <c>MapMcp</c>.
    /// The guard is installed right after the Host guard. A charset parameter is fine. GET (the event stream)
    /// and DELETE carry no body to type and pass. This is the twin of the gate in Darling's MCP host
    /// (<c>DarlingMcpHostService</c>); both call the one shared test, <see cref="JsonContentType.IsJson"/>.
    /// </summary>
    internal static void UseJsonPostGuard(IApplicationBuilder app)
    {
        app.Use(async (context, next) =>
        {
            if (HttpMethods.IsPost(context.Request.Method)
                && !JsonContentType.IsJson(context.Request.ContentType))
            {
                context.Response.StatusCode = StatusCodes.Status415UnsupportedMediaType;
                return;
            }

            await next(context);
        });
    }

    /// <summary>
    /// The write switch (<see cref="McpWritePolicy"/>) and the SDK's MCP server with every tool class, filter and
    /// transport option the host serves. Extracted so the tests that drive <c>tools/call</c> through a real web
    /// host register the SAME tools and the SAME policy production does, not a hand-built set.
    /// </summary>
    internal static void RegisterMcpServer(IServiceCollection services, bool allowWrites,
        bool allowInvestigations = false, McpDataDetail dataDetail = McpDataDetail.Full)
    {
        /* #5820: the four investigation tools are served only with mcp_allow_investigations on AND the writes off
           (McpInvestigationPolicy). Registered BEFORE the write policy so its instructions census (built there) counts
           only the tools this start offers. */
        services.AddMcpInvestigationPolicy(McpInvestigationPolicy.Served(allowInvestigations, allowWrites));

        /* #5820: mcp_data_detail, read once per start. McpDataDetailFilter (AddCallToolFilters) reads this singleton on
           every call; Full passes everything through. */
        services.AddSingleton(new McpDataDetailSetting(dataDetail));

        /* The write switch (McpWritePolicy): once the SDK has gathered every registered tool, drop the writes
           unless mcp_allow_writes is on and stamp the annotation hints. Registered here, ahead of AddMcpServer,
           so it is a PostConfigure over the SDK's own options and sees the full tool set. */
        services.AddMcpWritePolicy(allowWrites);

        /* Register MCP server with all tool classes */
        services
            .AddMcpServer(options =>
            {
                options.ServerInfo = new()
                {
                    Name = "PerformanceMonitorLite",
                    Version = "1.2.0"
                };
                /* ServerInstructions is set by AddMcpWritePolicy above, from the served set (#5789). */
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
            /* #5992 get_resource_use_by_tag; twin of Darling's DarlingMcpQueryTagRollupTools. */
            .WithGeminiCompatibleTools<McpQueryTagUsageTools>()
            .WithGeminiCompatibleTools<McpClientTimeoutTools>()
            .WithGeminiCompatibleTools<McpQueryTools>()
            .WithGeminiCompatibleTools<McpCpuTools>()
            .WithGeminiCompatibleTools<McpMemoryTools>()
            .WithGeminiCompatibleTools<McpIoTools>()
            .WithGeminiCompatibleTools<McpTempDbTools>()
            .WithGeminiCompatibleTools<McpPerfmonTools>()
            .WithGeminiCompatibleTools<McpAlertTools>()
            /* #5820: the Level 1 investigation tools, served only by the mcp_allow_investigations gate (McpInvestigationPolicy). */
            .WithGeminiCompatibleTools<McpInvestigationTools>()
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
               McpResponseBudget.DefaultBytes, the one shared size target both SKUs read.

               The statement filter (#4348, SensitiveStatementOutputFilter) is registered LAST, as Darling's
               host does: the first filter added is the outermost, so the last one sits next to the tool and
               reads each result as the tool's own JSON. It covers every tool with no per-tool change, error
               results included. The list lives in AddCallToolFilters so a test runs the REAL registration. */
            .WithRequestFilters(AddCallToolFilters);
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
            /* #5820: the investigation brief's reader and composer, for the claim tool (the composer's own file registers both). */
            PerformanceMonitorLite.Investigations.LiteInvestigationServices.AddInvestigationBrief(builder.Services, _duckDb);
            /* #5820: the one investigation store MainWindow built, as a singleton, for the four tools and get_notification_routes. */
            builder.Services.AddSingleton<IInvestigationRequestStore>(_investigationStore);

            /* The write switch and the SDK's tool registration live in RegisterMcpServer, the method the tests that
               drive tools/call also use (see there). */
            RegisterMcpServer(builder.Services, _allowWrites, _allowInvestigations, _dataDetail);

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

            UseJsonPostGuard(_app);

            _app.MapMcp();

            AppLogger.Info("MCP", $"Starting MCP server on http://localhost:{_port}");

            /* One line at every start, because an install that used the mute tool before the switch existed loses
               it on upgrade until the operator turns the setting on. */
            if (_allowWrites)
            {
                AppLogger.Warn("MCP",
                    $"MCP write tools are ON ({McpWritePolicy.WritesSetting}): the write tools (mute_analysis_finding, unmute_analysis_finding, set_notification_route_enabled, delete_notification_route) are served to any client that reaches this port.");
            }
            else
            {
                AppLogger.Info("MCP",
                    $"MCP write tools are off (the default): the write tools (mute_analysis_finding, unmute_analysis_finding, set_notification_route_enabled, delete_notification_route) are not served. To serve them set {McpWritePolicy.WritesSetting} to true in settings.json and restart.");
            }

            var gateLine = McpInvestigationPolicy.StartupLine(_allowInvestigations, _allowWrites);
            if (gateLine is { } line)
            {
                if (line.Level == LogLevel.Critical)
                {
                    AppLogger.Error(LogLevel.Critical, "MCP", line.Message, null);
                }
                else
                {
                    AppLogger.Info("MCP", line.Message);
                }
            }

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

    /// <summary>
    /// The call-tool filters, in registration order: the unknown-argument guard (#3870), the data-detail filter
    /// (#5820, <c>mcp_data_detail</c>), then the statement filter (#4348). The same objects Darling's host registers, in
    /// its order, from <c>PerformanceMonitor.Common</c>, so the two SKUs cannot answer differently. Lite's write and
    /// investigation gates are not filters: an unserved tool is absent from the served collection. Registered through this one method so a test can start a server with the REAL list.
    /// </summary>
    internal static void AddCallToolFilters(IMcpRequestFilterBuilder filters) => filters
        .AddCallToolFilter(McpUnknownArgumentGuard.Instance)
        .AddCallToolFilter(McpDataDetailFilter.Instance)
        .AddCallToolFilter(SensitiveStatementOutputFilter.Instance);

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
