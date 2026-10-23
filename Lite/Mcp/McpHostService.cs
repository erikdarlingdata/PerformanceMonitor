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
    private readonly McpServing _serving;
    private string? _currentToken;
    private readonly McpDataDetail _dataDetail;
    private readonly IPseudonymKeySource? _pseudonymKeys;
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
    /// <param name="bearerToken">The MCP token every request must carry. Required: null or empty is refused (an
    /// <see cref="ArgumentException"/>) unless <paramref name="allowTokenlessLoopback"/> is on, so no caller can start the host with
    /// no gate by leaving it out.</param>
    /// <param name="allowTokenlessLoopback">The explicit opt-out (<see cref="McpSettings.AllowTokenlessLoopback"/>): no gate is
    /// installed, only the read tools are served, and <paramref name="bearerToken"/> is ignored. See <see cref="DecideServing"/>.</param>
    /// <param name="allowWrites">Whether the write tools are asked for (<see cref="McpSettings.AllowWrites"/>, the
    /// <c>mcp_allow_writes</c> setting). Default off: a write tool is then absent from the served set and a call
    /// to it fails as an unknown tool. They are served only with a usable token too. See <see cref="McpWritePolicy"/>.</param>
    public McpHostService(LocalDataService dataService, ServerManager serverManager, MuteRuleService muteRuleService, DuckDbInitializer duckDb, int port, IInvestigationRequestStore investigationStore,
        string? bearerToken, bool allowTokenlessLoopback, ScheduleManager? scheduleManager = null, bool allowWrites = false,
        bool allowInvestigations = false, McpDataDetail dataDetail = McpDataDetail.Full,
        IPseudonymKeySource? pseudonymKeys = null)
    {
        _serving = DecideServing(bearerToken, allowTokenlessLoopback, allowWrites, allowInvestigations);
        _currentToken = _serving.Token;
        _pseudonymKeys = pseudonymKeys;
        _dataService = dataService;
        _serverManager = serverManager;
        _muteRuleService = muteRuleService;
        _duckDb = duckDb;
        _port = port;
        _scheduleManager = scheduleManager;
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

    /// <summary>What a start serves, decided once from the token and the settings by <see cref="DecideServing"/>.</summary>
    /// <param name="Token">The token every request must carry, or null with the opt-out on (no gate).</param>
    /// <param name="Writes">The write tools are served.</param>
    /// <param name="Investigations">The investigation tools are served.</param>
    /// <param name="AskedWrites">The settings asked for the write tools.</param>
    /// <param name="AskedInvestigations">The settings asked for the investigation tools.</param>
    internal readonly record struct McpServing(string? Token, bool Writes, bool Investigations, bool AskedWrites, bool AskedInvestigations)
    {
        /// <summary>The compiler-made <c>ToString</c> would print the token, so it shows only whether there is one.</summary>
        private bool PrintMembers(System.Text.StringBuilder builder)
        {
            builder.Append("Token = ").Append(Token is null ? "<none>" : "<set>")
                .Append(", Writes = ").Append(Writes)
                .Append(", Investigations = ").Append(Investigations)
                .Append(", AskedWrites = ").Append(AskedWrites)
                .Append(", AskedInvestigations = ").Append(AskedInvestigations);
            return true;
        }
    }

    /// <summary>
    /// The one decision of what a start serves, the same rule as Darling's (<c>DarlingMcpWritePolicy.ResolveForStart</c>). With
    /// <paramref name="allowTokenlessLoopback"/> on there is no token and only the read tools are served. Otherwise the token is
    /// required (empty throws). The write tools need <paramref name="allowWrites"/> and a usable token (<see cref="McpTokenStore.IsUsableToken"/>:
    /// at least <see cref="McpTokenStore.MinimumTokenLength"/> characters and at least <see cref="McpTokenStore.MinimumDistinctTokenCharacters"/>
    /// different ones). The investigation tools need their setting, a usable token and
    /// <paramref name="allowWrites"/> off (the writes SETTING, not whether they are served). It lives here, in the host's own
    /// constructor path, so no caller can skip it.
    /// </summary>
    internal static McpServing DecideServing(string? bearerToken, bool allowTokenlessLoopback, bool allowWrites, bool allowInvestigations)
    {
        if (!allowTokenlessLoopback && string.IsNullOrEmpty(bearerToken))
        {
            throw new ArgumentException(
                $"The MCP server needs a token; pass one, or turn on {McpSettings.AllowTokenlessLoopbackSetting} to serve the read tools without one.",
                nameof(bearerToken));
        }

        var token = allowTokenlessLoopback ? null : bearerToken;
        var usable = McpTokenStore.IsUsableToken(token);
        return new McpServing(
            token,
            Writes: allowWrites && usable,
            Investigations: allowInvestigations && usable && !allowWrites,
            AskedWrites: allowWrites,
            AskedInvestigations: allowInvestigations);
    }

    /// <summary>
    /// The token gate: every request must carry <c>Authorization: Bearer &lt;token&gt;</c> with the right token, or it is
    /// answered 401 with <c>WWW-Authenticate: Bearer</c> before <c>MapMcp</c>. The compare is constant-time
    /// (<see cref="McpTokenStore.IsAuthorized"/>). This is Darling's gate for its loopback listener. <paramref name="currentToken"/>,
    /// when given, is read on every request, so a replaced token takes effect at once. A null or empty
    /// <paramref name="token"/> installs nothing (<c>mcp_allow_tokenless_loopback</c> is on, and only the read tools are served).
    /// </summary>
    internal static void UseBearerTokenGate(IApplicationBuilder app, string? token, Func<string?>? currentToken = null)
    {
        if (string.IsNullOrEmpty(token))
        {
            return;
        }

        app.Use(async (context, next) =>
        {
            /* currentToken: the host's live token, which New token replaces while the server runs. Empty never authorizes. */
            var expected = currentToken?.Invoke() ?? token;
            if (!McpTokenStore.IsAuthorized(context.Request.Headers.Authorization.ToString(), expected))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                context.Response.Headers.WWWAuthenticate = "Bearer";
                return;
            }

            await next(context);
        });
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
        bool allowInvestigations = false, McpDataDetail dataDetail = McpDataDetail.Full, IPseudonymKeySource? pseudonymKeys = null)
    {
        /* #6066: Lite's pseudonym key (Credential Manager), for the data-detail filter and the brief composer at metadataOnly. None: logins are numbered per answer. */
        if (pseudonymKeys is not null) services.AddSingleton<IPseudonymKeySource>(pseudonymKeys);
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
            /* #6114 get_cpu_by_database, get_load_by_caller and get_query_pileups; twins of Darling's DarlingMcpWhoDroveItTools (#6072). */
            .WithGeminiCompatibleTools<McpWhoDroveItTools>()
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
            /* #5991: the query-tag and query-tag-rule tools; twin of Darling's DarlingMcpQueryTagTools and DarlingMcpQueryTagRuleTools. */
            .WithGeminiCompatibleTools<McpQueryTagTools>()
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

    /// <summary>Makes <paramref name="newToken"/> the token every request must carry, from the next request on; the old one is refused
    /// at once. False (nothing changes) when this host serves without a token or the new token is empty.</summary>
    internal bool ReplaceToken(string newToken)
    {
        if (_serving.Token is null || string.IsNullOrEmpty(newToken))
        {
            return false;
        }

        Volatile.Write(ref _currentToken, newToken);
        return true;
    }

    /// <summary>
    /// The request pipeline, in order: the Host guard (#1648), the token gate, the JSON POST guard, then <c>MapMcp</c>. Extracted from
    /// <c>ExecuteAsync</c> so a test drives the SAME pipeline production runs, not a hand-built copy.
    ///
    /// <para>The Host guard runs the SAME shared decision Darling's web dashboard and MCP host install (<see cref="HostHeaderGuard"/>;
    /// never a second copy of the rule). This host listens on loopback only, so a browser on this machine that loads hostile
    /// content could be rebound to 127.0.0.1:{port} and reach every tool same-origin; application/json does not force a CORS
    /// preflight under a rebind, because the browser considers the request same-origin. Loopback-only means no configured listen
    /// IP, so ONLY loopback Hosts (localhost / 127.0.0.1 / ::1) and an absent Host pass; a rebound foreign hostname is rejected 400
    /// before the token gate, <c>MapMcp</c> or any tool handler.</para>
    ///
    /// <para>After the Host guard and before the JSON guard and <c>MapMcp</c>, a request with no token or a wrong one is refused 401
    /// before anything else looks at it.</para>
    /// </summary>
    internal static void ConfigurePipeline(WebApplication app, McpServing serving, Func<string?>? currentToken = null)
    {
        app.Use(async (context, next) =>
        {
            if (!HostHeaderGuard.IsAllowedHost(context.Request.Host.Host, networkListenIp: null))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            await next(context);
        });

        UseBearerTokenGate(app, serving.Token, currentToken);

        UseJsonPostGuard(app);

        app.MapMcp();
    }

    /// <summary>
    /// The lines a start logs about what it serves: one Warning when there is no token, one line for the write tools (a Warning when
    /// served, one Critical when they were asked for and stay off, an Information when not asked for), and the investigation gate's
    /// line (<see cref="McpInvestigationPolicy.StartupLine"/>). Every tool group that was asked for and stays off gets one Critical line.
    /// </summary>
    internal static List<(LogLevel Level, string Message)> StartupLines(McpServing serving)
    {
        var lines = new List<(LogLevel, string)>();
        var writeTools = $"{McpWritePolicy.WriteTools.Count} write tools";
        if (serving.Token is null)
        {
            lines.Add((LogLevel.Warning,
                $"{McpSettings.AllowTokenlessLoopbackSetting} is on: the MCP server serves http://localhost with no token, and only the read tools. Turn it off to require a token."));
        }

        var usable = McpTokenStore.IsUsableToken(serving.Token);
        if (serving.Writes)
        {
            lines.Add((LogLevel.Warning,
                $"MCP write tools are ON ({McpWritePolicy.WritesSetting}): the {writeTools} are served to any client that has the MCP token."));
        }
        else if (serving.AskedWrites)
        {
            lines.Add((LogLevel.Critical,
                $"{McpWritePolicy.WritesSetting} is on but the MCP write tools stay OFF: they need {McpTokenStore.TokenRequirement}, "
                + $"and {McpSettings.AllowTokenlessLoopbackSetting} must be off. The {writeTools} are not served."));
        }
        else
        {
            lines.Add((LogLevel.Information,
                $"MCP write tools are off (the default): the {writeTools} are not served. To serve them set {McpWritePolicy.WritesSetting} to true in settings.json (a token is needed too)."));
        }

        if (McpInvestigationPolicy.StartupLine(serving.AskedInvestigations, serving.AskedWrites, usable) is { } line)
        {
            lines.Add(line);
        }

        return lines;
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
            PerformanceMonitorLite.Investigations.LiteInvestigationServices.AddInvestigationBrief(builder.Services, _duckDb,
                InvestigationServerNameLookup.FromServerManager(_serverManager));
            /* #5820: the one investigation store MainWindow built, as a singleton, for the four tools and get_notification_routes. */
            builder.Services.AddSingleton<IInvestigationRequestStore>(_investigationStore);

            /* The write switch and the SDK's tool registration live in RegisterMcpServer, the method the tests that
               drive tools/call also use (see there). */
            RegisterMcpServer(builder.Services, _serving.Writes, _serving.Investigations, _dataDetail, _pseudonymKeys);

            _app = builder.Build();

            ConfigurePipeline(_app, _serving, () => Volatile.Read(ref _currentToken));

            AppLogger.Info("MCP", $"Starting MCP server on http://localhost:{_port}");
            foreach (var (level, message) in StartupLines(_serving))
            {
                if (level == LogLevel.Critical)
                {
                    AppLogger.Error(LogLevel.Critical, "MCP", message, null);
                }
                else if (level == LogLevel.Warning)
                {
                    AppLogger.Warn("MCP", message);
                }
                else
                {
                    AppLogger.Info("MCP", message);
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
