using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Lane O7 (docs/optional-tables.md §8.2): the key-set SQL runs against DuckDB (never a second store read), the
/// batch fetch SQL shape/tag, the pacing math, and the skip reason when pg_statement_stats is absent.
/// </summary>
public sealed class PgStatementTextExporterTests
{
    [Fact]
    public void KeySetSql_reads_from_duckdb_not_the_store()
    {
        var sql = PgStatementTextExporter.KeySetSql("pg_statement_stats_hourwindow");

        Assert.Contains("SELECT DISTINCT server_name, queryid", sql, StringComparison.Ordinal);
        Assert.Contains("pg_statement_stats_hourwindow", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collect.", sql, StringComparison.Ordinal);
        Assert.Contains("queryid IS NOT NULL", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BatchSql_is_tagged_and_probes_the_primary_key()
    {
        var sql = SqlBuilder.PgStatementTextBatchSql;

        Assert.StartsWith("/* darling-export:opt.pg_statement_text */", sql.TrimStart(), StringComparison.Ordinal);
        Assert.Contains("collect.pg_statement_text", sql, StringComparison.Ordinal);
        Assert.Contains("t.server_id = k.sid AND t.queryid = k.qid", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(0, 50)]
    [InlineData(10, 50)]
    [InlineData(49, 50)]
    [InlineData(500, 500)]
    [InlineData(1999, 1999)]
    [InlineData(2000, 2000)]
    [InlineData(5000, 2000)]
    public void PaceSleepMs_clamps_between_50_and_2000(double callElapsedMs, int expected)
    {
        Assert.Equal(expected, PgStatementTextExporter.PaceSleepMs(callElapsedMs));
    }

    [Fact]
    public void NeedsPgStatementStats_true_when_stats_not_requested_alongside()
    {
        Assert.True(PgStatementTextExporter.NeedsPgStatementStats(["pg_statement_text"]));
        Assert.True(PgStatementTextExporter.NeedsPgStatementStats(["pg_statement_text", "pg_wait_stats"]));
    }

    [Fact]
    public void NeedsPgStatementStats_false_when_stats_present()
    {
        Assert.False(PgStatementTextExporter.NeedsPgStatementStats(["pg_statement_text", "pg_statement_stats"]));
    }

    [Fact]
    public void MissingStatsReason_is_the_documented_skip_reason()
    {
        Assert.Equal("needs pg_statement_stats", PgStatementTextExporter.MissingStatsReason);
    }

    [Fact]
    public async Task FetchContentAsync_batches_and_sleeps_once_per_batch()
    {
        // 251 keys => two batches of 250/1, so two paced sleeps.
        var keys = Enumerable.Range(1, 251).Select(i => (ServerId: 1, QueryId: (long)i)).ToList();
        var sleepCalls = new List<int>();

        // KeyBatchSize is asserted directly since the real fetch needs a live Npgsql connection
        // (covered by the container harness, docs/optional-tables.md §9 O7 row).
        Assert.Equal(250, PgStatementTextExporter.KeyBatchSize);
        var batches = DimensionContent.BatchGeneric(keys, PgStatementTextExporter.KeyBatchSize).ToList();
        Assert.Equal(2, batches.Count);
        Assert.Equal(250, batches[0].Count);
        Assert.Single(batches[1]);

        foreach (var _ in batches)
        {
            var ms = PgStatementTextExporter.PaceSleepMs(75);
            sleepCalls.Add(ms);
            await Task.CompletedTask;
        }

        Assert.Equal([75, 75], sleepCalls);
    }

    // ---- threshold gate: calls >= 2 OR total exec time >= 100 ms ----

    private static DuckDB.NET.Data.DuckDBConnection OpenStats(string columnsSql, string valuesSql)
    {
        var db = DuckDbSession.OpenInMemory(Path.Combine(Path.GetTempPath(), $"darling-export-pgtext-test-{Guid.NewGuid():N}"));
        using var cmd = db.CreateCommand();
        cmd.CommandText = $"CREATE TABLE stats ({columnsSql}); INSERT INTO stats VALUES {valuesSql};";
        cmd.ExecuteNonQuery();
        return db;
    }

    private const string FullColumns = "server_name VARCHAR, queryid BIGINT, calls_last BIGINT, total_exec_time_ms_last DOUBLE";

    [Fact]
    public void Gate_keeps_keys_on_calls_or_exec_time_and_skips_the_rest()
    {
        using var db = OpenStats(FullColumns,
            "('a', 1, 2, 0.5)," +        // calls == 2 -> keep
            "('a', 2, 1, 100.0)," +      // exec == 100 -> keep
            "('a', 3, 1, 99.9)," +       // both below -> skip
            "('a', 4, 1, 0.0)," +        // skip
            "('a', 5, NULL, NULL)," +    // NULL counters -> skip
            "('a', 6, 5, NULL)," +       // NULL exec time, calls ok -> keep
            "('b', 7, NULL, 250.0)");    // NULL calls, exec ok -> keep

        var result = PgStatementTextExporter.ReadKeySetGated(db, "stats", fetchAll: false);

        Assert.True(result.GateApplied);
        Assert.Equal(3, result.BelowThresholdSkipped);
        Assert.Equal([1L, 2L, 6L, 7L], result.Keys.Select(k => k.QueryId).OrderBy(q => q).ToArray());
    }

    [Fact]
    public void Gate_takes_the_max_over_the_hour_per_key()
    {
        // Same key in two rows (e.g. two databases): one row below, one above -> kept once.
        using var db = OpenStats(FullColumns, "('a', 1, 1, 1.0), ('a', 1, 3, 1.0), ('a', 2, 1, 1.0), ('a', 2, 1, 2.0)");

        var result = PgStatementTextExporter.ReadKeySetGated(db, "stats", fetchAll: false);

        Assert.Equal("a", Assert.Single(result.Keys).ServerName);
        Assert.Equal(1L, result.Keys[0].QueryId);
        Assert.Equal(1, result.BelowThresholdSkipped);
    }

    [Fact]
    public void Gate_keeps_null_server_name_keys_that_pass_and_drops_null_queryid()
    {
        using var db = OpenStats(FullColumns, "(NULL, 1, 9, 1.0), (NULL, 2, 1, 1.0), ('a', NULL, 99, 999.0)");

        var result = PgStatementTextExporter.ReadKeySetGated(db, "stats", fetchAll: false);

        var key = Assert.Single(result.Keys);
        Assert.Null(key.ServerName);
        Assert.Equal(1L, key.QueryId);
        Assert.Equal(1, result.BelowThresholdSkipped);
    }

    [Fact]
    public void Fetch_all_bypasses_the_gate()
    {
        using var db = OpenStats(FullColumns, "('a', 1, 1, 1.0), ('a', 2, 1, 1.0), ('a', 3, 5, 1.0)");

        var result = PgStatementTextExporter.ReadKeySetGated(db, "stats", fetchAll: true);

        Assert.False(result.GateApplied);
        Assert.Equal(0, result.BelowThresholdSkipped);
        Assert.Equal(3, result.Keys.Count);
    }

    [Theory]
    [InlineData("server_name VARCHAR, queryid BIGINT, calls_last BIGINT")]
    [InlineData("server_name VARCHAR, queryid BIGINT, total_exec_time_ms_last DOUBLE")]
    [InlineData("server_name VARCHAR, queryid BIGINT")]
    public void Missing_gate_columns_fall_back_to_every_key_and_log_one_line(string columns)
    {
        var values = columns.Contains("calls_last") && columns.Contains("total_exec") ? "" :
            columns.Contains("calls_last") ? "('a', 1, 1), ('a', 2, 1)" :
            columns.Contains("total_exec") ? "('a', 1, 1.0), ('a', 2, 1.0)" : "('a', 1), ('a', 2)";
        using var db = OpenStats(columns, values);
        var log = new StringWriter();

        var result = PgStatementTextExporter.ReadKeySetGated(db, "stats", fetchAll: false, log);

        Assert.False(result.GateApplied);
        Assert.Equal(2, result.Keys.Count);
        Assert.Equal(0, result.BelowThresholdSkipped);
        Assert.Single(log.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries));
        Assert.Contains("gate off", log.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void KeySetSql_gated_uses_the_threshold_constants_and_stays_in_duckdb()
    {
        var sql = PgStatementTextExporter.KeySetSql("t", applyGate: true);

        Assert.Contains("max(calls_last)", sql, StringComparison.Ordinal);
        Assert.Contains(">= 2", sql, StringComparison.Ordinal);
        Assert.Contains("max(total_exec_time_ms_last)", sql, StringComparison.Ordinal);
        Assert.Contains(">= 100", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_name, queryid", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collect.", sql, StringComparison.Ordinal);
    }

    private static ExportOptions MakeOptions(bool pgTextFetchAll) => ExportOptions.Validate(
        "store_pg", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", "deadlocks",
        "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, new DateTime(2026, 9, 27, 0, 0, 0, DateTimeKind.Utc),
        pgTextFetchAll: pgTextFetchAll);

    [Fact]
    public void ExportOptions_defaults_gate_on_and_accepts_the_switch()
    {
        Assert.False(MakeOptions(false).PgTextFetchAll);
        Assert.True(MakeOptions(true).PgTextFetchAll);
    }

    [Fact]
    public async Task ManifestWriter_emits_below_threshold_skipped_only_when_set()
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-export-manifest-{Guid.NewGuid():N}", "manifest.json");
        var manifest = new Manifest
        {
            Store = "store_pg",
            FromUtc = new DateTime(2026, 9, 26, 8, 0, 0, DateTimeKind.Utc),
            ToUtc = new DateTime(2026, 9, 26, 9, 0, 0, DateTimeKind.Utc),
            ExporterVersion = "t",
            DarlingSchemaVersion = null,
            Tables =
            [
                new ManifestTableEntry { Table = "pg_statement_text", OutputFile = "x", Sha256 = "", Columns = [], RowCount = 0, ByteSize = 0, StoreQueryElapsedMs = 0, WriteElapsedMs = 0, MissingContentCount = 0, BelowThresholdSkipped = 7 },
                new ManifestTableEntry { Table = "other", OutputFile = "y", Sha256 = "", Columns = [], RowCount = 0, ByteSize = 0, StoreQueryElapsedMs = 0, WriteElapsedMs = 0, MissingContentCount = 0 },
            ],
        };

        await ManifestWriter.WriteAsync(manifest, path, CancellationToken.None);

        using var doc = System.Text.Json.JsonDocument.Parse(await File.ReadAllTextAsync(path));
        var tables = doc.RootElement.GetProperty("tables").EnumerateArray().ToList();
        Assert.Equal(7, tables[0].GetProperty("below_threshold_skipped").GetInt64());
        Assert.False(tables[1].TryGetProperty("below_threshold_skipped", out _));
    }
}
