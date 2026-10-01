using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The archive views over the event tables that a cycle after the 512 MB reset
/// (ArchiveService.ArchiveAllAndResetAsync) could store again: blocked process reports, system_health events,
/// long query completions and memory pressure events. Builds that read the watermark from the live table alone
/// fetched the collector's fallback window again after a reset, so the archive holds the first copy of an event
/// and a later file or the hot table a second one; a watermark read that fails still does. Each view shows one
/// row per exact identity, keeps the earliest collected copy, and never collapses a row that has no usable
/// identity.
/// </summary>
public class ArchiveViewEventDedupTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archivePath;

    public ArchiveViewEventDedupTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archivePath = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archivePath);
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

    /* One table's insert shape. Row(id, collection time, event time or null, payload or null) returns one VALUES
       tuple; the payload is the event's own text (report XML, event XML, statement text) or, for memory
       pressure events, the notification that tells two events apart. */
    private sealed record Shape(string Columns, Func<int, string, string?, string?, string> Row, string TimeColumn, bool HasText);

    private static string Ts(string? value) => value is null ? "NULL" : $"TIMESTAMP '{value}'";

    private static string Str(string? value) => value is null ? "NULL" : $"'{value.Replace("'", "''")}'";

    private static readonly Dictionary<string, Shape> Shapes = new(StringComparer.Ordinal)
    {
        ["blocked_process_reports"] = new(
            "(blocked_report_id, collection_time, server_id, server_name, event_time, blocked_process_report_xml)",
            (id, ct, et, p) => $"({id}, {Ts(ct)}, 1, 'S1', {Ts(et)}, {Str(p)})",
            "event_time", HasText: true),
        ["system_health_events"] = new(
            "(system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)",
            (id, ct, et, p) => $"({id}, {Ts(ct)}, 1, 'S1', {Ts(et)}, 'sp_server_diagnostics_component_result', {Str(p)})",
            "event_time", HasText: true),
        ["long_query_completions"] = new(
            "(long_query_completion_id, collection_time, server_id, server_name, event_time, database_name, session_id, event_sequence, statement_text)",
            (id, ct, et, p) => $"({id}, {Ts(ct)}, 1, 'S1', {Ts(et)}, 'db1', 55, 7, {Str(p)})",
            "event_time", HasText: true),
        ["memory_pressure_events"] = new(
            "(collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system)",
            (id, ct, et, p) => $"({id}, {Ts(ct)}, 1, 'S1', {Ts(et)}, {Str(p)}, 2, 0)",
            "sample_time", HasText: false),
    };

    /* Payloads per table: the three XE tables carry event text, memory pressure events a notification. */
    private static string P(string table, int n) => Shapes[table].HasText
        ? $"<event n=\"{n}\"/>"
        : $"RESOURCE_MEMPHYSICAL_LOW_{n}";

    private const string Archived = "2026-01-01 00:00:00";
    private const string Recollected = "2026-06-01 00:00:00";
    private const string T1 = "2026-05-31 23:51:00";
    private const string T2 = "2026-05-31 23:54:00";
    private const string T3 = "2026-05-31 23:58:00";

    private async Task<DuckDBConnection> OpenAsync()
    {
        var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var result = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return result == DBNull.Value ? null : result;
    }

    /// <summary>
    /// Archive, reset, collect again, the way ArchiveService does it: the archived rows go to a *_{table}.parquet
    /// file the view's glob reads, the hot table is emptied, and the rows collected after the reset go into it
    /// with new ids and a later collection time. Returns an open connection after the views are rebuilt.
    /// </summary>
    private async Task<DuckDBConnection> StageAsync(string table, IEnumerable<string> archived, IEnumerable<string> recollected)
    {
        var shape = Shapes[table];
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();
        using (var connection = await OpenAsync())
        {
            await ExecuteAsync(connection, $"INSERT INTO {table} {shape.Columns} VALUES {string.Join(", ", archived)}");
            var parquetPath = Path.Combine(_archivePath, $"20260601_0000_{table}.parquet").Replace("\\", "/");
            await ExecuteAsync(connection, $"COPY {table} TO '{parquetPath}' (FORMAT PARQUET)");
            await ExecuteAsync(connection, $"DELETE FROM {table}");
            await ExecuteAsync(connection, $"INSERT INTO {table} {shape.Columns} VALUES {string.Join(", ", recollected)}");
        }

        await initializer.CreateArchiveViewsAsync();
        return await OpenAsync();
    }

    public static TheoryData<string> AllTables() => new(Shapes.Keys);

    public static TheoryData<string> TextTables() => new(Shapes.Where(s => s.Value.HasText).Select(s => s.Key));

    [Theory]
    [MemberData(nameof(AllTables))]
    public async Task AStoredEventCollectedAgainAfterTheReset_ReadsOnce_KeepingTheEarliestCopy(string table)
    {
        var row = Shapes[table].Row;
        var time = Shapes[table].TimeColumn;
        using var connection = await StageAsync(table,
            archived: [row(1, Archived, T1, P(table, 1)), row(2, Archived, T2, P(table, 2))],
            recollected: [row(11, Recollected, T2, P(table, 2)), row(12, Recollected, T3, P(table, 3))]);

        /* Three events, not four: the copy of the second one collapses. */
        Assert.Equal(3L, Convert.ToInt64(await ScalarAsync(connection, $"SELECT COUNT(*) FROM v_{table}")));
        /* The surviving row is the first one stored, from the archive. */
        Assert.Equal(DateTime.Parse(Archived), (DateTime)(await ScalarAsync(connection,
            $"SELECT collection_time FROM v_{table} WHERE {time} = TIMESTAMP '{T2}'"))!);
        /* The archive-only event and the new event are both there. */
        Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, $"SELECT COUNT(*) FROM v_{table} WHERE {time} = TIMESTAMP '{T1}'")));
        Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, $"SELECT COUNT(*) FROM v_{table} WHERE {time} = TIMESTAMP '{T3}'")));
    }

    [Theory]
    [MemberData(nameof(AllTables))]
    public async Task TwoDifferentEventsAtTheSameTime_BothRead(string table)
    {
        var row = Shapes[table].Row;
        using var connection = await StageAsync(table,
            archived: [row(1, Archived, T1, P(table, 1))],
            recollected: [row(11, Recollected, T1, P(table, 2))]);

        Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(connection, $"SELECT COUNT(*) FROM v_{table}")));
    }

    /* memory_pressure_events.sample_time is NOT NULL, so only the three XE tables can store a row with no time. */
    [Theory]
    [MemberData(nameof(TextTables))]
    public async Task RowsWithNoTime_AreNeverCollapsed(string table)
    {
        var row = Shapes[table].Row;
        using var connection = await StageAsync(table,
            archived: [row(1, Archived, null, P(table, 1)), row(2, Archived, null, P(table, 1))],
            recollected: [row(11, Recollected, null, P(table, 1)), row(12, Recollected, null, P(table, 1))]);

        Assert.Equal(4L, Convert.ToInt64(await ScalarAsync(connection, $"SELECT COUNT(*) FROM v_{table}")));
    }

    [Theory]
    [MemberData(nameof(TextTables))]
    public async Task RowsWithNoEventText_AreNeverCollapsed(string table)
    {
        var row = Shapes[table].Row;
        using var connection = await StageAsync(table,
            archived: [row(1, Archived, T1, null), row(2, Archived, T1, null)],
            recollected: [row(11, Recollected, T1, null), row(12, Recollected, T1, "")]);

        Assert.Equal(4L, Convert.ToInt64(await ScalarAsync(connection, $"SELECT COUNT(*) FROM v_{table}")));
    }

    /* Every Lite reader of these tables goes through the v_ views, so the dedup above reaches it: analysis and
       anomaly counts, the grids and charts, MCP and alerts. A reader on the bare hot table would see neither the
       archive nor the dedup. cpu_utilization_stats has no key but is swept too: on the bare table, a reader
       misses every sample archived before the last reset. The collectors' own watermark and archive paths name the table through a variable
       ({tableName}, {table}), so they never match. Comments are blanked first (they name these tables in
       prose), and the match spans line breaks, so a FROM on one line and the table on the next is caught. */
    private static readonly Regex BlockComment = new(@"/\*\s.*?\*/", RegexOptions.Singleline | RegexOptions.CultureInvariant);

    [Fact]
    public void NoLiteReaderReadsTheseTablesBare()
    {
        var bare = new Regex(@"\b(?:FROM|JOIN)\s+(?:main\.)?(?:" + string.Join("|", Shapes.Keys.Append("cpu_utilization_stats")) + @")\b",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        var offenders = new List<string>();
        foreach (var path in Directory.EnumerateFiles(Path.Combine(RepoRoot(), "Lite"), "*.cs", SearchOption.AllDirectories))
        {
            if (path.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                || path.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                continue;

            /* "/*" followed by a space opens a comment; a glob such as "/*_table.parquet" does not. Each comment
               keeps its line breaks, so line numbers still match the file. */
            var text = BlockComment.Replace(File.ReadAllText(path).Replace("\r\n", "\n"),
                comment => new string('\n', comment.Value.Count(c => c == '\n')));
            text = string.Join("\n", text.Split('\n')
                .Select(line => line.TrimStart().StartsWith("//", StringComparison.Ordinal) ? "" : line));
            foreach (Match match in bare.Matches(text))
            {
                var lineNumber = text.AsSpan(0, match.Index).Count('\n') + 1;
                offenders.Add($"{Path.GetFileName(path)}:{lineNumber}: {match.Value}");
            }
        }

        Assert.Empty(offenders);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.Collectors")))
            {
                return dir.FullName;
            }
        }

        throw new InvalidOperationException("Could not find the repository root from " + thisFile);
    }
}
