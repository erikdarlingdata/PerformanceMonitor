using System;
using System.IO;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Tests;

/* Shared DuckDB fixture for FinOpsTests. xUnit instantiates this once per
   test class (via IClassFixture<>), so all tests in the class share a
   single DuckDB file. Each test seeder calls ClearTestDataAsync first
   (via the *Async helpers in TestDataSeeder), so cross-test pollution
   is prevented without paying the schema-init cost on every test. */
public sealed class FinOpsDuckDbFixture : IDisposable
{
    public string TempDir { get; }
    public string DbPath { get; }
    public DuckDbInitializer DuckDb { get; }

    public FinOpsDuckDbFixture()
    {
        TempDir = Path.Combine(Path.GetTempPath(), "FinOpsTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(TempDir);
        DbPath = Path.Combine(TempDir, "test.duckdb");
        DuckDb = new DuckDbInitializer(DbPath);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(TempDir))
                Directory.Delete(TempDir, recursive: true);
        }
        catch { /* Best-effort cleanup */ }
    }
}
