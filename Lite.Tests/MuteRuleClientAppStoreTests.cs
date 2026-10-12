using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5640: Lite's mute table round-trips <c>client_app_pattern</c> through DuckDbMuteRuleStore (insert, update,
/// clear, a rule that never set it), and the pattern matches like the other text patterns. The column itself
/// is created by the DuckDB initializer (the attention rung owns that DDL).
/// </summary>
public class MuteRuleClientAppStoreTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;

    public MuteRuleClientAppStoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    [Fact]
    public async Task DuckDbMuteRuleStore_RoundTripsClientAppPattern_ThroughInsertUpdateAndClear()
    {
        using var initializer = new PerformanceMonitorLite.Database.DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();
        var store = new DuckDbMuteRuleStore(initializer);

        var withApp = new MuteRule { Id = Guid.NewGuid().ToString(), ServerName = "srv", MetricName = "Client Timeouts", ClientAppPattern = "ssms" };
        var without = new MuteRule { Id = Guid.NewGuid().ToString(), ServerName = "srv", MetricName = "High CPU" };
        await store.InsertAsync(withApp);
        await store.InsertAsync(without);

        var loaded = await store.LoadAllAsync();
        Assert.Equal("ssms", loaded.Single(r => r.Id == withApp.Id).ClientAppPattern);
        Assert.Null(loaded.Single(r => r.Id == without.Id).ClientAppPattern);

        withApp.ClientAppPattern = "OrdersApi";
        without.ClientAppPattern = "ssms";
        await store.UpdateAsync(withApp);
        await store.UpdateAsync(without);
        loaded = await store.LoadAllAsync();
        Assert.Equal("OrdersApi", loaded.Single(r => r.Id == withApp.Id).ClientAppPattern);
        Assert.Equal("ssms", loaded.Single(r => r.Id == without.Id).ClientAppPattern);

        withApp.ClientAppPattern = null;
        await store.UpdateAsync(withApp);
        Assert.Null((await store.LoadAllAsync()).Single(r => r.Id == withApp.Id).ClientAppPattern);
    }

    [Theory]
    [InlineData("Management Studio", "Microsoft SQL Server Management Studio", true)]
    [InlineData("management studio", "Microsoft SQL Server Management Studio", true)]
    [InlineData("Management Studio", "OrdersApi", false)]
    [InlineData("Management Studio", null, false)]
    public void ClientAppPattern_IsACaseInsensitiveContains_AndNeverMatchesAMissingApp(string pattern, string? app, bool expected)
    {
        var rule = new MuteRule { ClientAppPattern = pattern };
        var context = new AlertMuteContext { ServerName = "srv", MetricName = "Client Timeouts", ClientAppName = app };

        Assert.Equal(expected, rule.MatchesAt(context, new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Fact]
    public void LiteEditorAndMcpMuteRuleOutput_CarryTheClientAppField()
    {
        var mcp = File.ReadAllText(Path.Combine(FindRepoRoot(), "Lite", "Mcp", "McpAlertTools.cs"));
        Assert.Contains("client_app_pattern = r.ClientAppPattern,", mcp, StringComparison.Ordinal);
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
            dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("repo root not found");
    }
}
