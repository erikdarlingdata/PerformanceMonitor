using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5711: the long-query trace's <c>query_hash</c> action is 0 on every batch-level completion (measured on SQL Server
/// 2016, 2017 and 2019: normal and aborted alike, and on the attention event). A reader that groups, filters or labels
/// by the raw value puts every such row in one bucket. These pin the shared rule (<see cref="QueryHashIdentity"/>) and
/// the Lite reader that applies it.
/// </summary>
public sealed class LongQueryUnknownHashTests : IDisposable
{
    private const int ServerId = 5711;
    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);

    public LongQueryUnknownHashTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

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

    [Theory]
    [InlineData(null, true)]
    [InlineData("", true)]
    [InlineData("   ", true)]
    [InlineData("0", true)]
    [InlineData("00", true)]
    [InlineData("0x0000000000000000", true)]
    [InlineData("0X0", true)]
    [InlineData("0x1A2B3C4D5E6F7A8B", false)]
    [InlineData("0x0000000000000001", false)]
    [InlineData("10", false)]
    public void IsUnknown_TreatsNullEmptyAndAllZeroAsUnknown(string? hash, bool unknown)
    {
        Assert.Equal(unknown, QueryHashIdentity.IsUnknown(hash));
        Assert.Equal(unknown, QueryHashIdentity.KnownHash(hash) is null);
    }

    [Fact]
    public void Identity_KeysUnknownHashRowsByTheirOwnStatement_NotByOneSharedValue()
    {
        var a = QueryHashIdentity.Identity("0", "SELECT * FROM dbo.Orders");
        var b = QueryHashIdentity.Identity("0", "SELECT * FROM dbo.Customers");
        var c = QueryHashIdentity.Identity("0x0000000000000000", "  select *   from DBO.orders ");

        Assert.NotNull(a);
        Assert.NotNull(b);
        Assert.NotEqual(a, b);
        Assert.Equal(a, c); /* same statement, case and whitespace aside: one group */
        Assert.StartsWith(QueryHashIdentity.FingerprintPrefix, a);
    }

    [Fact]
    public void Identity_PrefersARealHash_AndLeavesNoIdentityForARowWithNeitherHashNorText()
    {
        Assert.Equal("0x1a2b3c4d5e6f7a8b", QueryHashIdentity.Identity("0x1A2B3C4D5E6F7A8B", "SELECT 1"));
        Assert.Null(QueryHashIdentity.Identity("0", null));
        Assert.Null(QueryHashIdentity.Identity(null, "   "));
        Assert.Null(QueryHashIdentity.Identity("0", SensitiveStatements.PlaceholderText));
    }

    /* The Lite grid's rebound column is the user-visible half and no compile step sees XAML bindings: pins the Long
       Queries grid's last column to QueryIdentity (binding and filter tag) under the "Query ID" header, like Darling's
       twin in its LongQueryUnknownHashTests. Putting the raw QueryHash back fails here. */
    [Fact]
    public void TheLiteGrid_BindsTheIdentity_UnderTheQueryIdHeader()
    {
        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Controls", "ServerTab.xaml"));
        var grid = xaml[xaml.IndexOf("x:Name=\"LongQueryCompletionsGrid\"", StringComparison.Ordinal)..];
        var bindAt = grid.IndexOf("{Binding QueryIdentity}", StringComparison.Ordinal);
        Assert.True(bindAt >= 0, "the Long Queries grid no longer binds QueryIdentity");
        var column = grid[bindAt..grid.IndexOf("</DataGridTextColumn>", bindAt, StringComparison.Ordinal)];
        Assert.Contains("Tag=\"QueryIdentity\"", column, StringComparison.Ordinal);
        Assert.Contains("Text=\"Query ID\"", column, StringComparison.Ordinal);
        Assert.DoesNotContain("{Binding QueryHash}", grid[..grid.IndexOf("</DataGrid>", StringComparison.Ordinal)], StringComparison.Ordinal);

        var tool = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Mcp", "McpLongQueryTools.cs"));
        Assert.Contains("query_identity = r.McpQueryIdentity", tool, StringComparison.Ordinal);
        Assert.Contains("query_hash = QueryHashIdentity.KnownHash(r.QueryHash)", tool, StringComparison.Ordinal);
    }

    private static string RepoRoot([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    private async Task<LocalDataService> StageAsync()
    {
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        var initializer = new DuckDbInitializer(dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();
        var now = DateTime.UtcNow;
        using var connection = new DuckDBConnection($"Data Source={dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        var rows = new (string Hash, string Type, string Result, string Text)[]
        {
            ("0", "sql_batch_completed", "Abort", "WAITFOR DELAY ''00:00:09''; SELECT 1"),
            ("0", "sql_batch_completed", "Abort", "SELECT COUNT_BIG(*) FROM sys.all_columns a CROSS JOIN sys.all_columns b"),
            ("0x0000000000000000", "sql_batch_completed", "OK", "SELECT TOP (5) name FROM sys.columns ORDER BY name"),
            ("0x1A2B3C4D5E6F7A8B", "rpc_completed", "OK", "EXEC dbo.RealHash"),
        };
        for (var i = 0; i < rows.Length; i++)
        {
            using var cmd = connection.CreateCommand();
            var stamp = $"TIMESTAMP '{now.AddMinutes(-5):yyyy-MM-dd HH:mm:ss.ffffff}'";
            cmd.CommandText =
                "INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, "
                + "event_type, result, query_hash, statement_text, duration_microseconds) VALUES "
                + $"({i + 1}, {stamp}, {ServerId}, 'S', {stamp}, '{rows[i].Type}', '{rows[i].Result}', '{rows[i].Hash}', '{rows[i].Text}', {5_000_000 - i})";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        return new LocalDataService(initializer);
    }

    /* The reader, end to end: three rows stored with query_hash 0 (two spellings) read back with no hash and three
       different identities, so a group or filter on the identity does not collapse them. Reverting the reader to the raw
       value gives QueryHash "0" and an empty-hash identity bucket, and fails here. */
    [Fact]
    public async Task TheLiteReader_DoesNotCollapseZeroHashRowsOfDifferentStatements_IntoOneQuery()
    {
        var service = await StageAsync();
        var now = DateTime.UtcNow;
        var rows = await service.GetRecentLongQueryCompletionsAsync(ServerId, 1, now.AddHours(-1), now);

        Assert.Equal(4, rows.Count);
        var unknown = rows.Where(r => r.StatementText != "EXEC dbo.RealHash").ToList();
        Assert.Equal(3, unknown.Count);
        Assert.All(unknown, r => Assert.Equal("", r.QueryHash));
        Assert.Equal(3, unknown.Select(r => r.QueryIdentity).Distinct().Count());
        Assert.All(unknown, r => Assert.StartsWith(QueryHashIdentity.FingerprintPrefix, r.QueryIdentity));

        var real = rows.Single(r => r.StatementText == "EXEC dbo.RealHash");
        Assert.Equal("0x1A2B3C4D5E6F7A8B", real.QueryHash);
        Assert.Equal("0x1a2b3c4d5e6f7a8b", real.QueryIdentity);
    }
}
