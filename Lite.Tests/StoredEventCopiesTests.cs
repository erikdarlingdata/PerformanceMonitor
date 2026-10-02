using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// <see cref="StoredEventCopies"/>: the reads over the event tables that a cycle after the 512 MB reset
/// (ArchiveService.ArchiveAllAndResetAsync) could store again. Builds that read the watermark from the live table
/// alone fetched the collector's fallback window again after a reset, so the archive holds the first copy of an
/// event and a later file or the hot table a second one; a watermark read that fails still does. Each read shows
/// every row of the first batch that stored an identity, drops the copies a later batch stored, and never
/// collapses a row that has no usable identity.
/// </summary>
public class StoredEventCopiesTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archivePath;

    public StoredEventCopiesTests()
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

    /* One table's insert shape. Row(id, collection time, event time or null, event text or null) returns one
       VALUES tuple; the event text is the event's own XML or statement text. Read(collectedFrom) is the relation
       a reader of the table queries, for server 1, with the collection_time lower bound when one is given. */
    private sealed record Shape(string Columns, Func<int, string, string?, string?, string> Row, Func<string?, string> Read);

    private static string Ts(string? value) => value is null ? "NULL" : $"TIMESTAMP '{value}'";

    private static string Str(string? value) => value is null ? "NULL" : $"'{value.Replace("'", "''")}'";

    private static readonly Dictionary<string, Shape> Shapes = new(StringComparer.Ordinal)
    {
        ["blocked_process_reports"] = new(
            "(blocked_report_id, collection_time, server_id, server_name, event_time, blocked_process_report_xml)",
            (id, ct, et, p) => $"({id}, {Ts(ct)}, 1, 'S1', {Ts(et)}, {Str(p)})",
            from => StoredEventCopies.BlockedProcessReports("server_id = 1", from)),
        ["long_query_completions"] = new(
            "(long_query_completion_id, collection_time, server_id, server_name, event_time, database_name, session_id, event_sequence, statement_text)",
            (id, ct, et, p) => $"({id}, {Ts(ct)}, 1, 'S1', {Ts(et)}, 'db1', 55, 7, {Str(p)})",
            from => StoredEventCopies.LongQueryCompletions("server_id = 1", from)),
        ["system_health_events"] = new(
            "(system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)",
            (id, ct, et, p) => $"({id}, {Ts(ct)}, 1, 'S1', {Ts(et)}, 'sp_server_diagnostics_component_result', {Str(p)})",
            from => StoredEventCopies.SystemHealthEvents("server_id = 1", from)),
    };

    private static string P(int n) => $"<event n=\"{n}\"/>";

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

    private static async Task<long> CountAsync(DuckDBConnection connection, string table, string where = "TRUE", string? collectedFrom = null) =>
        Convert.ToInt64(await ScalarAsync(connection, $"SELECT COUNT(*) FROM {Shapes[table].Read(collectedFrom)} AS r WHERE {where}"));

    [Theory]
    [MemberData(nameof(AllTables))]
    public async Task AStoredEventCollectedAgainAfterTheReset_ReadsOnce_KeepingTheEarliestCopy(string table)
    {
        var row = Shapes[table].Row;
        using var connection = await StageAsync(table,
            archived: [row(1, Archived, T1, P(1)), row(2, Archived, T2, P(2))],
            recollected: [row(11, Recollected, T2, P(2)), row(12, Recollected, T3, P(3))]);

        /* Three events, not four: the copy of the second one collapses. */
        Assert.Equal(3L, await CountAsync(connection, table));
        /* The surviving row is the first one stored, from the archive. */
        Assert.Equal(DateTime.Parse(Archived), (DateTime)(await ScalarAsync(connection,
            $"SELECT collection_time FROM {Shapes[table].Read(null)} AS r WHERE event_time = TIMESTAMP '{T2}'"))!);
        /* The archive-only event and the new event are both there. */
        Assert.Equal(1L, await CountAsync(connection, table, $"event_time = TIMESTAMP '{T1}'"));
        Assert.Equal(1L, await CountAsync(connection, table, $"event_time = TIMESTAMP '{T3}'"));
    }

    /* A collector stores identical rows in one batch when the source returns the same event twice in one read.
       That is not a reset copy, which always comes from a later batch, so those rows all stay. */
    [Theory]
    [MemberData(nameof(AllTables))]
    public async Task IdenticalRowsFromOneBatch_AllRead_WhileTheSameRowFromALaterBatchIsDropped(string table)
    {
        var row = Shapes[table].Row;
        using var connection = await StageAsync(table,
            archived: [row(1, Archived, T1, P(1)), row(2, Archived, T1, P(1))],
            recollected: [row(11, Recollected, T1, P(1)), row(12, Recollected, T3, P(3)), row(13, Recollected, T3, P(3))]);

        /* Both archived rows of the first event stay, and its copy from the later batch goes. */
        Assert.Equal(2L, await CountAsync(connection, table, $"event_time = TIMESTAMP '{T1}'"));
        Assert.Equal(0L, await CountAsync(connection, table, $"event_time = TIMESTAMP '{T1}' AND collection_time = TIMESTAMP '{Recollected}'"));
        /* Two identical rows that only one batch stored both stay. */
        Assert.Equal(2L, await CountAsync(connection, table, $"event_time = TIMESTAMP '{T3}'"));
    }

    /* Two events that differ only in their text, stored by different batches, both stay. The texts are the same
       length and differ in one character, so the text's part of the key has to tell them apart: a key without it,
       or one coarser than its hash, such as its length, merges them. */
    [Theory]
    [MemberData(nameof(AllTables))]
    public async Task TwoDifferentEventsAtTheSameTime_BothRead(string table)
    {
        var row = Shapes[table].Row;
        using var connection = await StageAsync(table,
            archived: [row(1, Archived, T1, P(1))],
            recollected: [row(11, Recollected, T1, P(2))]);

        Assert.Equal(2L, await CountAsync(connection, table));
    }

    [Theory]
    [MemberData(nameof(AllTables))]
    public async Task RowsWithNoTime_AreNeverCollapsed(string table)
    {
        var row = Shapes[table].Row;
        using var connection = await StageAsync(table,
            archived: [row(1, Archived, null, P(1)), row(2, Archived, null, P(1))],
            recollected: [row(11, Recollected, null, P(1)), row(12, Recollected, null, P(1))]);

        Assert.Equal(4L, await CountAsync(connection, table));
    }

    [Theory]
    [MemberData(nameof(AllTables))]
    public async Task RowsWithNoEventText_AreNeverCollapsed(string table)
    {
        var row = Shapes[table].Row;
        using var connection = await StageAsync(table,
            archived: [row(1, Archived, T1, null), row(2, Archived, T1, null)],
            recollected: [row(11, Recollected, T1, null), row(12, Recollected, T1, "")]);

        Assert.Equal(4L, await CountAsync(connection, table));
    }

    public static TheoryData<string, string?> AllTablesWithNoText()
    {
        var data = new TheoryData<string, string?>();
        foreach (var table in Shapes.Keys)
        {
            data.Add(table, null);
            data.Add(table, "");
        }
        return data;
    }

    /* A row with no text stays apart from its twin in a later batch, though the two match on every other part. The
       parts that keep it apart test the raw text, not the text's key: hash(NULL) is not NULL, and hash('') is one
       value, so a test on the key would merge each pair. */
    [Theory]
    [MemberData(nameof(AllTablesWithNoText))]
    public async Task ARowWithNoText_AndItsTwinFromALaterBatch_BothRead(string table, string? text)
    {
        var row = Shapes[table].Row;
        using var connection = await StageAsync(table,
            archived: [row(1, Archived, T1, text)],
            recollected: [row(11, Recollected, T1, text)]);

        Assert.Equal(2L, await CountAsync(connection, table));
    }

    /* The key covers the whole text: of two 13,000-character texts that differ in one character in the middle,
       both stay, while the unchanged text's later copy goes. */
    [Theory]
    [MemberData(nameof(AllTables))]
    public async Task TwoLongEventsThatDifferInOneMiddleCharacter_BothRead(string table)
    {
        var text = string.Concat(Enumerable.Repeat("<a b=\"x\"/>", 1300));
        var changed = text[..6500] + "Q" + text[6501..];
        var row = Shapes[table].Row;
        using var connection = await StageAsync(table,
            archived: [row(1, Archived, T1, text)],
            recollected: [row(11, Recollected, T1, changed), row(12, Recollected, T1, text)]);

        Assert.Equal(2L, await CountAsync(connection, table));
        Assert.Equal(1L, await CountAsync(connection, table, $"collection_time = TIMESTAMP '{Recollected}'"));
    }

    /* A long query completion can lack its database, session or event sequence. NULL there is still part of one
       identity: each completion's first batch reads once and its copy goes, and completions that differ only in
       which of those columns is NULL stay apart. Each is first stored by its own batch, so a merge would leave one. */
    [Fact]
    public async Task ALongQueryCompletionWithNullDatabaseSessionOrSequence_ReadsOnce()
    {
        static string Lqc(int id, string collected, string database, string session, string sequence) =>
            $"({id}, {Ts(collected)}, 1, 'S1', {Ts(T1)}, {database}, {session}, {sequence}, 'SELECT 1')";

        using var connection = await StageAsync("long_query_completions",
            archived:
            [
                Lqc(1, "2026-01-01 00:00:00", "NULL", "55", "7"), Lqc(2, "2026-01-01 00:01:00", "'db1'", "NULL", "7"),
                Lqc(3, "2026-01-01 00:02:00", "'db1'", "55", "NULL"), Lqc(4, "2026-01-01 00:03:00", "NULL", "NULL", "NULL"),
            ],
            recollected:
            [
                Lqc(11, Recollected, "NULL", "55", "7"), Lqc(12, Recollected, "'db1'", "NULL", "7"),
                Lqc(13, Recollected, "'db1'", "55", "NULL"), Lqc(14, Recollected, "NULL", "NULL", "NULL"),
            ]);

        Assert.Equal(4L, await CountAsync(connection, "long_query_completions"));
        Assert.Equal(0L, await CountAsync(connection, "long_query_completions", $"collection_time = TIMESTAMP '{Recollected}'"));
    }

    /* A read whose window starts after an event's first copy was stored, but before its later copy, shows neither:
       the read looks one fallback window back past its start, finds the first copy there, drops the later one, and
       the first copy stays outside the window. */
    [Theory]
    [MemberData(nameof(AllTables))]
    public async Task AWindowStartingBetweenAnEventsFirstCopyAndItsLaterCopy_ReadsNeither(string table)
    {
        const string firstStored = "2026-05-31 23:57:00";
        const string windowStart = "TIMESTAMP '2026-05-31 23:59:00'";
        var row = Shapes[table].Row;
        using var connection = await StageAsync(table,
            archived: [row(1, firstStored, T1, P(1))],
            recollected: [row(11, Recollected, T1, P(1)), row(12, Recollected, T3, P(3))]);

        /* Only the event first stored inside the window. */
        Assert.Equal(1L, await CountAsync(connection, table, collectedFrom: windowStart));
        Assert.Equal(1L, await CountAsync(connection, table, $"event_time = TIMESTAMP '{T3}'", windowStart));
    }

    /* The read looks back exactly as far as a collector with no watermark does: both come from one shared value. */
    [Fact]
    public void EveryEventCollectorsFallback_IsTheWindowTheReadLooksBack()
    {
        var collectionTime = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);
        var collectors = new (string Name, Func<CollectorContext, CollectorQuery> BuildQuery)[]
        {
            ("blocked_process_reports", BlockedProcessReportCollector.Instance.BuildQuery),
            ("long_query_completions", LongQueryCompletionsCollector.Instance.BuildQuery),
            ("system_health_events", SystemHealthEventsCollector.Instance.BuildQuery),
        };

        foreach (var (name, buildQuery) in collectors)
        {
            var query = buildQuery(new CollectorContext
            {
                ServerId = 1,
                ServerName = "S1",
                CollectionTime = collectionTime,
                Deltas = new RecordingCollectorDeltaCalculator(),
            });

            var cutoff = (DateTime)query.Parameters.Single(p => p.Name == "@cutoff_time").Value!;
            Assert.True(collectionTime - CollectorContext.EventFallbackWindow == cutoff,
                $"{name}: a first run reads back to {cutoff:O}, not one EventFallbackWindow before {collectionTime:O}");
            Assert.Contains($"INTERVAL {(long)CollectorContext.EventFallbackWindow.TotalSeconds} SECOND",
                Shapes[name].Read("$2"), StringComparison.Ordinal);
        }
    }
}
