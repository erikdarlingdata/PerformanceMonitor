using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace UiSmokeWpf;

/// <summary>
/// The Darling viewer walk (#5745 part 2): the same four checks as the Lite walk (<see cref="WalkBase"/>), on the real
/// viewer exe reading a throwaway PostgreSQL store that the workflow has migrated and seeded with
/// <c>tools/ui-smoke/seed.sql</c> (the SQL Server target <c>ui-sql</c>, with waits and queries). The viewer finds the
/// store through <c>DARLING_CONFIG</c>, which the workflow sets for this step.
/// </summary>
public sealed class ViewerWalkTests : WalkBase
{
    protected override string ServerName => "ui-sql";

    // Nothing to seed here: the workflow started the cluster, ran the service once to migrate it and applied seed.sql.
    protected override Task SeedAsync()
    {
        var cfg = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        Assert.True(!string.IsNullOrEmpty(cfg) && File.Exists(cfg), "DARLING_CONFIG must name the smoke darling.json; the workflow sets it");
        return Task.CompletedTask;
    }

    [Fact]
    public void Walk_OverviewWaitStatsAndQueries_ShowTheSeededServer() => Walk();

    protected override string FindExe()
    {
        var fromEnv = Environment.GetEnvironmentVariable("VIEWER_EXE");
        if (!string.IsNullOrEmpty(fromEnv) && File.Exists(fromEnv)) return fromEnv;

        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !Directory.Exists(Path.Combine(dir.FullName, "Darling")))
        {
            dir = dir.Parent;
        }
        if (dir is not null)
        {
            foreach (var cfg in new[] { "Release", "Debug" })
            {
                var p = Path.Combine(dir.FullName, "Darling", "PerformanceMonitor.Darling.Viewer", "bin", cfg, "net10.0-windows",
                    "PerformanceMonitor.Darling.Viewer.exe");
                if (File.Exists(p)) return p;
            }
        }
        throw new FileNotFoundException("PerformanceMonitor.Darling.Viewer.exe not found; set VIEWER_EXE");
    }
}
