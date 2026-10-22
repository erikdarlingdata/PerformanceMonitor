/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6441 part 2: the system_health collector's REAL batch, run against a live SQL Server's own system_health event_file
/// target. <see cref="SystemHealthEventFileTests"/> feeds <c>ReadAsync</c> canned result sets; this class is what proves
/// the T-SQL itself: the first read with no bookmark, a bookmark read that returns only newer events, a bookmark at a
/// file that is gone and one at an offset that is not a buffer start (both still return rows and move to a fresh
/// bookmark), the cap, and the version branch. It also pins down what the server answers to a bad bookmark (error 25717
/// for a missing file, 25722 for a bad offset: neither is an empty result).
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server. It only reads: SELECTs
/// and the event file read, nothing that alters the server. Written against sql2019; the batch it builds follows the
/// connected server's major version, so it also runs against 2017+ and 2016.</para>
/// </summary>
[Collection("timing")]
public sealed class SystemHealthEventFileLiveSqlTests
{
    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live system_health event_file tests.";

    /// <summary>An old cutoff: every event in the file is newer than it.</summary>
    private static readonly DateTime s_longAgo = DateTime.UtcNow.AddYears(-5);

    private sealed record Server(SqlConnection Connection, int Major, string CurrentFile, string FilePattern);

    private sealed record Pass(List<SystemHealthEventsCollector.Row> Rows, CollectorContext Context, TimeSpan Elapsed);

    private static string? ConnectionString()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host))
        {
            return null;
        }

        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "pm-6441-system-health-event-file-live-test",
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    /// <summary>Opens the server and finds the file system_health is writing now, the way the batch does.</summary>
    private static async Task<Server> OpenAsync()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);

        int major;
        string? name;
        await using (var command = new SqlCommand(
            @"SELECT
    major = CONVERT(integer, SERVERPROPERTY('ProductMajorVersion')),
    file_name = TRY_CAST(xet.target_data AS xml).value('(EventFileTarget/File/@name)[1]', 'nvarchar(260)')
FROM sys.dm_xe_session_targets AS xet
JOIN sys.dm_xe_sessions AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'system_health'
AND   xet.target_name = N'event_file';", connection))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            Assert.SkipUnless(await reader.ReadAsync(ct), "This server has no system_health event_file target.");
            major = reader.GetInt32(0);
            name = reader.IsDBNull(1) ? null : reader.GetString(1);
        }

        Assert.SkipWhen(string.IsNullOrEmpty(name), "The system_health event_file target names no file.");
        var stem = name![..^4];
        var pattern = stem[..stem.LastIndexOf('_')] + "_*.xel";
        return new Server(connection, major, name, pattern);
    }

    private static CollectorContext MakeContext(
        int major, IReadOnlyDictionary<string, string>? state, DateTime? watermark)
        => new()
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = new RecordingCollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { SqlMajorVersion = major },
            Watermark = watermark,
            State = state ?? CollectorContext.NoState,
        };

    private static SqlParameter Bind(CollectorParameter parameter) =>
        parameter.Type switch
        {
            CollectorParameterType.DateTime2 => new SqlParameter(parameter.Name, SqlDbType.DateTime2)
            {
                Scale = 7,
                Value = parameter.Value ?? DBNull.Value,
            },
            CollectorParameterType.NVarChar260 => new SqlParameter(parameter.Name, SqlDbType.NVarChar, 260)
            {
                Value = parameter.Value ?? DBNull.Value,
            },
            CollectorParameterType.BigInt => new SqlParameter(parameter.Name, SqlDbType.BigInt)
            {
                Value = parameter.Value ?? DBNull.Value,
            },
            CollectorParameterType.Int32 => new SqlParameter(parameter.Name, SqlDbType.Int)
            {
                Value = parameter.Value ?? DBNull.Value,
            },
            _ => throw new NotSupportedException(parameter.Type.ToString()),
        };

    /// <summary>One pass of the real batch through the collector's own BuildQuery and ReadAsync.</summary>
    private static async Task<Pass> RunPassAsync(
        Server server,
        IReadOnlyDictionary<string, string>? state,
        DateTime? watermark,
        int? maxEvents = null,
        int? asMajor = null)
    {
        var ct = TestContext.Current.CancellationToken;
        var context = MakeContext(asMajor ?? server.Major, state, watermark);
        var query = SystemHealthEventsCollector.Instance.BuildQuery(context);

        await using var command = new SqlCommand(query.Text, server.Connection) { CommandTimeout = 120 };
        foreach (var parameter in query.Parameters)
        {
            var bound = parameter.Name == "@max_events" && maxEvents is { } cap
                ? parameter with { Value = cap }
                : parameter;
            command.Parameters.Add(Bind(bound));
        }

        var watch = Stopwatch.StartNew();
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = await SystemHealthEventsCollector.Instance.ReadAsync(reader, context, ct);
        watch.Stop();
        return new Pass(rows, context, watch.Elapsed);
    }

    private static long? Measured(CollectorContext context, string label) =>
        context.Measurements.Where(m => m.Label == label).Select(m => (long?)m.Value).FirstOrDefault();

    private static string KeyOf(SystemHealthEventsCollector.Row row) =>
        string.Concat(row.SourceFile, "|", row.SourceOffset, "|", row.EventTime?.ToString("o", CultureInfo.InvariantCulture), "|", row.EventXml);

    /// <summary>The rows' keys in a fixed order: rows of one buffer share its offset, and the server does not order them among themselves.</summary>
    private static List<string> Keys(IEnumerable<SystemHealthEventsCollector.Row> rows) =>
        rows.Select(KeyOf).Order(StringComparer.Ordinal).ToList();

    private static Dictionary<string, string> StateFrom(CollectorContext context) =>
        new(context.PendingState, StringComparer.Ordinal);

    private static string MissingFileInDirectoryOf(string file) =>
        file[..(file.LastIndexOf('_') + 1)] + "1234567.xel";

    /// <summary>Runs the read function against a bookmark that cannot work and returns the error the server raised.</summary>
    private static async Task<int> BadBookmarkErrorAsync(Server server, string bookmarkFile, long offset)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var command = new SqlCommand(
            "SELECT COUNT_BIG(*) FROM sys.fn_xe_file_target_read_file(@pattern, NULL, @file, @offset);", server.Connection);
        command.Parameters.Add(new SqlParameter("@pattern", SqlDbType.NVarChar, 260) { Value = server.FilePattern });
        command.Parameters.Add(new SqlParameter("@file", SqlDbType.NVarChar, 260) { Value = bookmarkFile });
        command.Parameters.Add(new SqlParameter("@offset", SqlDbType.BigInt) { Value = offset });
        var ex = await Assert.ThrowsAsync<SqlException>(() => command.ExecuteScalarAsync(ct));
        return ex.Number;
    }

    [Fact]
    public async Task AFirstRead_WithNoBookmark_ReadsTheCurrentFileOnly_InFileOrder_AndLeavesABookmark()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        var pass = await RunPassAsync(server, state: null, watermark: s_longAgo);

        Assert.SkipWhen(pass.Rows.Count == 0, "The current system_health file holds none of the six captured events yet.");
        Assert.All(pass.Rows, row => Assert.Equal(server.CurrentFile, row.SourceFile, ignoreCase: true));
        Assert.True(pass.Rows.Count <= SystemHealthEventsCollector.MaxEventsPerPass);
        Assert.Equal(pass.Rows.Count, Measured(pass.Context, "event_file_events"));
        Assert.Null(Measured(pass.Context, "event_file_fallback"));
        Assert.Null(Measured(pass.Context, "event_file_error"));

        /* Rows arrive in file order, so the later row of a tie is the later one in the file. */
        Assert.Equal(
            pass.Rows.OrderBy(r => r.SourceFile, StringComparer.OrdinalIgnoreCase).ThenBy(r => r.SourceOffset).Select(KeyOf),
            pass.Rows.Select(KeyOf));

        var bookmark = pass.Context.PendingState[SystemHealthEventsCollector.BookmarkStateKey];
        Assert.Contains(server.CurrentFile, bookmark, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(SystemHealthEventsCollector.TryUseBookmark(
            pass.Context.PendingState, pass.Rows.Max(r => r.EventTime!.Value)));
    }

    [Fact]
    public async Task ABookmarkRead_ReturnsOnlyTheEventsPastTheBookmark()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        var full = await RunPassAsync(server, state: null, watermark: s_longAgo);
        Assert.SkipWhen(full.Rows.Count < 20, "The current system_health file holds too few events to bookmark in.");

        /* Bookmark in the middle of the file: the buffer of the row half-way through, with that row's time as the watermark. */
        var middle = full.Rows[full.Rows.Count / 2];
        var state = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SystemHealthEventsCollector.BookmarkStateKey] =
                SystemHealthEventsCollector.FormatBookmark(middle.EventTime!.Value, middle.SourceOffset!.Value, middle.SourceFile!),
        };

        var resumed = await RunPassAsync(server, state, watermark: middle.EventTime!.Value);

        Assert.Null(Measured(resumed.Context, "event_file_fallback"));
        Assert.All(resumed.Rows, row =>
        {
            Assert.True(row.SourceOffset > middle.SourceOffset, "a bookmark read returned an event from the bookmark's own buffer or before it");
            Assert.True(row.EventTime >= middle.EventTime, "a bookmark read returned an event older than the watermark");
        });

        /* Exactly the full read's rows past the bookmark, with the time filter inclusive on a bookmark read. */
        var expected = full.Rows.Where(r => r.SourceOffset > middle.SourceOffset && r.EventTime >= middle.EventTime);
        Assert.Equal(Keys(expected), Keys(resumed.Rows));
    }

    [Fact]
    public async Task AMissingBookmarkFile_RaisesError25717_AndABadOffsetRaises25722_NeitherIsAnEmptyResult()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        Assert.Equal(25717, await BadBookmarkErrorAsync(server, MissingFileInDirectoryOf(server.CurrentFile), 98304));
        Assert.Equal(25722, await BadBookmarkErrorAsync(server, server.CurrentFile, 1));
    }

    [Fact]
    public async Task ABookmarkAtAMissingFile_StillReturnsRows_AndMovesToAFreshBookmark()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        var stale = SystemHealthEventsCollector.FormatBookmark(s_longAgo, 98304, MissingFileInDirectoryOf(server.CurrentFile));
        var state = new Dictionary<string, string>(StringComparer.Ordinal) { [SystemHealthEventsCollector.BookmarkStateKey] = stale };

        var pass = await RunPassAsync(server, state, watermark: s_longAgo);

        Assert.SkipWhen(pass.Rows.Count == 0, "The current system_health file holds none of the six captured events yet.");
        Assert.Null(Measured(pass.Context, "event_file_fallback"));
        var fresh = pass.Context.PendingState[SystemHealthEventsCollector.BookmarkStateKey];
        Assert.NotEqual(stale, fresh);
        Assert.Contains(server.CurrentFile, fresh, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ABookmarkAtANonBufferOffset_StillReturnsRows_AndMovesToAFreshBookmark()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        var stale = SystemHealthEventsCollector.FormatBookmark(s_longAgo, 1, server.CurrentFile);
        var state = new Dictionary<string, string>(StringComparer.Ordinal) { [SystemHealthEventsCollector.BookmarkStateKey] = stale };

        var pass = await RunPassAsync(server, state, watermark: s_longAgo);

        Assert.SkipWhen(pass.Rows.Count == 0, "The current system_health file holds none of the six captured events yet.");
        Assert.Null(Measured(pass.Context, "event_file_fallback"));
        var fresh = pass.Context.PendingState[SystemHealthEventsCollector.BookmarkStateKey];
        Assert.NotEqual(stale, fresh);
        Assert.StartsWith(pass.Rows.Max(r => r.EventTime!.Value).ToString("o", CultureInfo.InvariantCulture), fresh, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AQuietPass_WithABookmarkAtAMissingFile_DropsTheBookmark()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        /* A watermark in the future: nothing is newer, so the current-file read is quiet and could not replace the bookmark. */
        var future = DateTime.UtcNow.AddDays(1);
        var stale = SystemHealthEventsCollector.FormatBookmark(future, 98304, MissingFileInDirectoryOf(server.CurrentFile));
        var state = new Dictionary<string, string>(StringComparer.Ordinal) { [SystemHealthEventsCollector.BookmarkStateKey] = stale };

        var pass = await RunPassAsync(server, state, watermark: future);

        Assert.Empty(pass.Rows);
        Assert.Equal(0, Measured(pass.Context, "event_file_events"));
        Assert.Equal(string.Empty, pass.Context.PendingState[SystemHealthEventsCollector.BookmarkStateKey]);
    }

    [Fact]
    public async Task AQuietPass_WithAGoodBookmark_KeepsIt()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        var full = await RunPassAsync(server, state: null, watermark: s_longAgo);
        Assert.SkipWhen(full.Rows.Count == 0, "The current system_health file holds none of the six captured events yet.");

        var state = StateFrom(full.Context);
        var newest = full.Rows.Max(r => r.EventTime!.Value);
        var quiet = await RunPassAsync(server, state, watermark: newest.AddDays(1));

        Assert.Empty(quiet.Rows);
        Assert.DoesNotContain(SystemHealthEventsCollector.BookmarkStateKey, quiet.Context.PendingState.Keys);
    }

    [Fact]
    public async Task TheCap_HoldsEachPassToTheCap_AndTheNextPassContinuesWithoutRepeatingAnEvent()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        var full = await RunPassAsync(server, state: null, watermark: s_longAgo);
        Assert.SkipWhen(full.Rows.Count < 40, "The current system_health file holds too few events to chain capped passes over.");

        /* The same file, a cap of 200 events a pass: each pass keeps its rows and its bookmark, and the next one goes on from
           it, the way the host feeds the stored watermark and the state back. */
        const int Cap = 200;
        var seen = new List<SystemHealthEventsCollector.Row>();
        Dictionary<string, string>? state = null;
        DateTime? watermark = s_longAgo;
        string? previousBookmark = null;
        var passes = 0;

        while (passes < 400)
        {
            var pass = await RunPassAsync(server, state, watermark, maxEvents: Cap);
            passes++;
            Assert.True(pass.Rows.Count <= Cap, $"pass {passes} returned {pass.Rows.Count} rows against a cap of {Cap}");
            if (pass.Rows.Count == 0)
            {
                break;
            }

            seen.AddRange(pass.Rows);
            var bookmark = pass.Context.PendingState[SystemHealthEventsCollector.BookmarkStateKey];
            Assert.NotEqual(previousBookmark, bookmark);
            previousBookmark = bookmark;
            state = StateFrom(pass.Context);
            watermark = seen.Max(r => r.EventTime!.Value);
        }

        TestContext.Current.SendDiagnosticMessage($"chained capped passes: {passes} passes, {seen.Count} events");
        Assert.True(passes > 1, "the cap never bound, so the chain proved nothing");

        /* The uncapped read is the truth: no event was stored more often than the file holds it (two events can be
           identical, so this counts per key rather than asking for distinct rows), and the buffer the cap cut did not
           lose the events after it. */
        var uncapped = (await RunPassAsync(server, state: null, watermark: s_longAgo, maxEvents: 1_000_000)).Rows;
        var holds = uncapped.GroupBy(KeyOf, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
        foreach (var group in seen.GroupBy(KeyOf, StringComparer.Ordinal))
        {
            Assert.True(holds.TryGetValue(group.Key, out var held) && group.Count() <= held, "an event was stored more often than the file holds it");
        }

        Assert.True(seen.Count >= uncapped.Count * 0.99, $"the chained capped passes read {seen.Count} of the uncapped read's {uncapped.Count} events");
    }

    [Fact]
    public async Task ABacklogRead_FromTheStartOfTheLargestFile_StopsAtTheCap_UnderTheCommandTimeout()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;
        var ct = TestContext.Current.CancellationToken;

        /* The largest system_health file on disk, bookmarked at its first buffer: the biggest backlog a pass can meet. */
        string? largest = null;
        long size = 0;
        var directory = server.FilePattern[..(server.FilePattern.LastIndexOf('\\') + 1)];
        if (server.Major >= 14)
        {
            await using var list = new SqlCommand(
                @"SELECT TOP (1) fs.full_filesystem_path, fs.size_in_bytes
FROM sys.dm_os_enumerate_filesystem(@directory, N'system_health_0_*.xel') AS fs
ORDER BY fs.size_in_bytes DESC;", connection);
            list.Parameters.Add(new SqlParameter("@directory", SqlDbType.NVarChar, 260) { Value = directory });
            await using var reader = await list.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                largest = reader.GetString(0);
                size = reader.GetInt64(1);
            }
        }

        Assert.SkipWhen(largest is null || size < 20_000_000, "No system_health file of 20 MB or more to read a backlog from.");

        var state = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SystemHealthEventsCollector.BookmarkStateKey] = SystemHealthEventsCollector.FormatBookmark(s_longAgo, 98304, largest!),
        };
        var pass = await RunPassAsync(server, state, watermark: s_longAgo);

        TestContext.Current.SendDiagnosticMessage(
            $"backlog pass: {pass.Rows.Count} events from {size:N0} bytes of {largest} in {pass.Elapsed.TotalSeconds:0.00} s (major {server.Major})");

        /* The cap bound (less the buffer it cut), and the pass finished inside the 30 s command timeout Lite runs it under. */
        Assert.InRange(pass.Rows.Count, SystemHealthEventsCollector.MaxEventsPerPass * 9 / 10, SystemHealthEventsCollector.MaxEventsPerPass);
        Assert.True(pass.Elapsed < TimeSpan.FromSeconds(30), $"a capped backlog pass took {pass.Elapsed.TotalSeconds:0.0} s");
        Assert.Null(Measured(pass.Context, "event_file_fallback"));
    }

    [Fact]
    public async Task TheBatchForTheServersVersion_Runs_AndTheOtherVersionsBatchReadsTheSameRows()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        /* The version the connected server reports picks the batch; on 2017+ the 2016 batch (no timestamp_utc) must run too,
           and both must read the same events. */
        var own = await RunPassAsync(server, state: null, watermark: s_longAgo);
        Assert.Null(Measured(own.Context, "event_file_fallback"));
        Assert.Equal(
            server.Major >= SystemHealthEventsCollector.TimestampUtcMajorVersion,
            SystemHealthEventsCollector.Instance
                .BuildQuery(MakeContext(server.Major, null, s_longAgo)).Text.Contains("timestamp_utc", StringComparison.Ordinal));

        if (server.Major >= SystemHealthEventsCollector.TimestampUtcMajorVersion)
        {
            var asOld = await RunPassAsync(server, state: null, watermark: s_longAgo, asMajor: 13);
            Assert.Null(Measured(asOld.Context, "event_file_fallback"));
            Assert.Equal(Keys(own.Rows), Keys(asOld.Rows));

            /* A watermark inside the file: the timestamp_utc filter before the XML cast and the XML time filter agree. */
            Assert.SkipWhen(own.Rows.Count < 20, "The current system_health file holds too few events to cut a watermark in.");
            var cut = own.Rows[own.Rows.Count / 2].EventTime!.Value;
            var newer = await RunPassAsync(server, state: null, watermark: cut);
            var newerOld = await RunPassAsync(server, state: null, watermark: cut, asMajor: 13);
            Assert.NotEmpty(newer.Rows);
            Assert.All(newer.Rows, row => Assert.True(row.EventTime > cut));
            Assert.Equal(Keys(newer.Rows), Keys(newerOld.Rows));
        }
    }
}
