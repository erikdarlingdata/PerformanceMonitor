using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.AspNetCore;
using PerformanceMonitor.Analysis;
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
                .WithGeminiCompatibleTools<McpAnalysisTools>();

            _app = builder.Build();
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
