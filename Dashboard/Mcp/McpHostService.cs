using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.AspNetCore;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Mcp;

/// <summary>
/// Background service that hosts an MCP server over Streamable HTTP transport.
/// Allows LLM clients to discover and call monitoring tools via http://localhost:{port}.
/// </summary>
public sealed class McpHostService : BackgroundService
{
    private readonly ServerManager _serverManager;
    private readonly ICredentialService _credentialService;
    private readonly MuteRuleService _muteRuleService;
    private readonly UserPreferencesService _preferencesService;
    private readonly int _port;
    private WebApplication? _app;
    private DatabaseServiceRegistry? _registry;

    public McpHostService(ServerManager serverManager, ICredentialService credentialService, MuteRuleService muteRuleService, UserPreferencesService preferencesService, int port)
    {
        _serverManager = serverManager;
        _credentialService = credentialService;
        _muteRuleService = muteRuleService;
        _preferencesService = preferencesService;
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

            /* Suppress ASP.NET Core console logging */
            builder.Logging.ClearProviders();
            builder.Logging.SetMinimumLevel(LogLevel.Warning);

            /* Register services that MCP tools need via dependency injection */
            _registry = new DatabaseServiceRegistry(_serverManager, _credentialService);
            builder.Services.AddSingleton(_serverManager);
            builder.Services.AddSingleton(_registry);
            builder.Services.AddSingleton(_muteRuleService);
            builder.Services.AddSingleton(_preferencesService);

            /* Register MCP server with all tool classes */
            builder.Services
                .AddMcpServer(options =>
                {
                    options.ServerInfo = new()
                    {
                        Name = "PerformanceMonitorDashboard",
                        Version = "1.2.0"
                    };
                    options.ServerInstructions = McpInstructions.Text;
                })
                .WithHttpTransport()
                .WithTools<McpDiscoveryTools>()
                .WithTools<McpHealthTools>()
                .WithTools<McpWaitTools>()
                .WithTools<McpBlockingTools>()
                .WithTools<McpQueryTools>()
                .WithTools<McpCpuTools>()
                .WithTools<McpMemoryTools>()
                .WithTools<McpIoTools>()
                .WithTools<McpTempDbTools>()
                .WithTools<McpPerfmonTools>()
                .WithTools<McpAlertTools>()
                .WithTools<McpJobTools>();

            _app = builder.Build();
            _app.MapMcp();

            Logger.Info($"[MCP] Starting MCP server on http://localhost:{_port}");

            await _app.RunAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* Normal shutdown */
        }
        catch (Exception ex)
        {
            Logger.Error($"[MCP] MCP server failed: {ex.Message}", ex);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_app != null)
        {
            Logger.Info("[MCP] Stopping MCP server");
            await _app.StopAsync(cancellationToken);
            await _app.DisposeAsync();
            _app = null;
        }

        if (_registry != null)
        {
            await _registry.DisposeAsync();
            _registry = null;
        }

        await base.StopAsync(cancellationToken);
    }
}
