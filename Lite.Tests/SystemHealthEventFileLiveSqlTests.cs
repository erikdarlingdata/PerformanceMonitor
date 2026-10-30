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

    /// <summary>A cap no read reaches, for the reference reads the capped ones are compared with.</summary>
    private const int Uncapped = 1_000_000;

    /// <summary>An old cutoff: every event in the file is newer than it.</summary>
    private static readonly DateTime s_longAgo = DateTime.UtcNow.AddYears(-5);

    private sealed record Server(SqlConnection Connection, int Major, string CurrentFile, string FilePattern);

    private sealed record Pass(List<SystemHealthEventsCollector.Row> Rows, CollectorContext Context, TimeSpan Elapsed);

    /// <summary>One pass of a chain: the watermark it read past, whether it read from a bookmark, where its bookmark ended.</summary>
    private sealed record ChainPass(DateTime Watermark, bool FromBookmark, string? EndFile, long EndOffset, int Rows);

    private sealed record Chain(List<SystemHealthEventsCollector.Row> Seen, List<ChainPass> Passes);

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
        int major, IReadOnlyDictionary<string, string>? state, DateTime? watermark, bool managedInstance = false)
        => new()
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = new RecordingCollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { SqlMajorVersion = major, IsAzureManagedInstance = managedInstance },
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
        int? asMajor = null,
        bool managedInstance = false)
    {
        var ct = TestContext.Current.CancellationToken;
        var context = MakeContext(asMajor ?? server.Major, state, watermark, managedInstance);
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

    [Theory]
    [InlineData(null)]
    [InlineData(13)]
    public async Task AFirstRead_WithNoBookmark_ReadsTheCurrentFileOnly_InFileOrder_AndLeavesABookmark(int? asMajor)
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        /* The 2016 batch too: its read with no bookmark is capped like every other read (a current file of more than
           MaxEventsPerPass events used to be read whole). */
        var pass = await RunPassAsync(server, state: null, watermark: s_longAgo, asMajor: asMajor);

        Assert.SkipWhen(pass.Rows.Count == 0, "The current system_health file holds none of the six captured events yet.");
        Assert.All(pass.Rows, row => Assert.Equal(server.CurrentFile, row.SourceFile, ignoreCase: true));
        Assert.True(pass.Rows.Count <= SystemHealthEventsCollector.MaxEventsPerPass);
        /* A pass the cap stopped writes no count (it did not read to the end); one it did not stop counts its rows. */
        if (Measured(pass.Context, "event_file_events") is { } counted)
        {
            Assert.Equal(pass.Rows.Count, counted);
        }

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
    public async Task ManagedInstanceMode_ReadsTheBarePattern_ForTheFreshReadAndTheBookmarkResume()
    {
        /* #6897: Managed Instance refuses a directory path and the _0_ pattern, so its batch reads system_health*.xel with
           the bookmark's file_name as the function returned it. A box resolves the bare pattern in its default log folder,
           where its system_health files are, so the same batch is proven here: both reads reach the file, neither falls back.
           The bare pattern opens the OLDEST matching file first, while the ordinary path opens the file the target is writing
           now, so a rollover file older than the current one tells the two apart: a batch that stopped using the override
           would start the fresh read in the current file. */
        var server = await OpenAsync();
        await using var connection = server.Connection;
        var ct = TestContext.Current.CancellationToken;

        Assert.SkipWhen(server.Major < 14, "sys.dm_os_enumerate_filesystem needs SQL Server 2017 or later.");
        var directory = server.FilePattern[..(server.FilePattern.LastIndexOf('\\') + 1)];
        string? oldest = null;
        long oldestSize = 0;
        await using (var list = new SqlCommand(
            @"SELECT TOP (1) fs.full_filesystem_path, fs.size_in_bytes
FROM sys.dm_os_enumerate_filesystem(@directory, N'system_health_0_*.xel') AS fs
ORDER BY fs.full_filesystem_path;", connection))
        {
            list.Parameters.Add(new SqlParameter("@directory", SqlDbType.NVarChar, 260) { Value = directory });
            await using var reader = await list.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                oldest = reader.GetString(0);
                oldestSize = reader.GetInt64(1);
            }
        }

        Assert.SkipWhen(
            oldest is null || string.Equals(oldest, server.CurrentFile, StringComparison.OrdinalIgnoreCase),
            "No rollover system_health file older than the current one, so the bare pattern and the exact path start in the same file.");
        Assert.SkipWhen(oldestSize < 1_000_000, "The oldest system_health file is too small to hold the 50 events the fresh read is capped at.");

        var fresh = await RunPassAsync(server, state: null, watermark: s_longAgo, maxEvents: 50, managedInstance: true);
        Assert.SkipWhen(
            Measured(fresh.Context, "event_file_fallback") is not null,
            "This server's system_health files are not in its default log folder, where the bare pattern looks.");
        Assert.SkipWhen(fresh.Rows.Count == 0, "The system_health files hold none of the six captured events yet.");
        Assert.Null(Measured(fresh.Context, "event_file_error"));

        /* The fresh read starts in the oldest file and the cap stops it there: no row comes from the current file. */
        Assert.All(fresh.Rows, row => Assert.Equal(oldest, row.SourceFile, ignoreCase: true));

        var resumed = await RunPassAsync(
            server, StateFrom(fresh.Context), fresh.Rows.Max(r => r.EventTime!.Value), managedInstance: true);
        Assert.Null(Measured(resumed.Context, "event_file_fallback"));
        Assert.Null(Measured(resumed.Context, "event_file_error"));
    }

    [Fact]
    public async Task ABookmarkRead_ReturnsOnlyTheEventsPastTheBookmark()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        var full = await RunPassAsync(server, state: null, watermark: s_longAgo, maxEvents: Uncapped);
        Assert.SkipWhen(full.Rows.Count < 20, "The current system_health file holds too few events to bookmark in.");

        /* Bookmark in the middle of the file: the buffer of the row half-way through, with that row's time as the watermark. */
        var middle = full.Rows[full.Rows.Count / 2];
        var state = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SystemHealthEventsCollector.BookmarkStateKey] =
                SystemHealthEventsCollector.FormatBookmark(middle.EventTime!.Value, middle.SourceOffset!.Value, middle.SourceFile!),
        };

        var resumed = await RunPassAsync(server, state, watermark: middle.EventTime!.Value, maxEvents: Uncapped);

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

    /// <summary>#6544: a bookmark in the file the target is writing reads that file by its exact path; a bookmark in an older file reads every rollover file, and no bookmark reads the current file alone by its exact path.</summary>
    [Fact]
    public async Task ABookmarkInTheCurrentFile_ReadsItByItsExactPath_AndOneInAnOlderFileReadsEveryRolloverFile()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;
        var ct = TestContext.Current.CancellationToken;

        var full = await RunPassAsync(server, state: null, watermark: s_longAgo, maxEvents: Uncapped);
        Assert.SkipWhen(full.Rows.Count < 20, "The current system_health file holds too few events to bookmark in.");

        /* No bookmark: the current file alone, by its exact path. */
        Assert.Null(Measured(full.Context, "event_file_wildcard"));

        /* A bookmark in the current file: still the exact path, and the same rows as before. */
        var middle = full.Rows[full.Rows.Count / 2];
        var current = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SystemHealthEventsCollector.BookmarkStateKey] =
                SystemHealthEventsCollector.FormatBookmark(middle.EventTime!.Value, middle.SourceOffset!.Value, middle.SourceFile!),
        };
        var resumed = await RunPassAsync(server, current, watermark: middle.EventTime!.Value, maxEvents: Uncapped);
        Assert.Null(Measured(resumed.Context, "event_file_wildcard"));
        Assert.Equal(
            Keys(full.Rows.Where(r => r.SourceOffset > middle.SourceOffset && r.EventTime >= middle.EventTime)),
            Keys(resumed.Rows));

        /* A bookmark in an older file (the file has rolled over): every rollover file is read, as it has to be to reach the
           current one. */
        string? older = null;
        if (server.Major >= 14)
        {
            var directory = server.FilePattern[..(server.FilePattern.LastIndexOf('\\') + 1)];
            await using var list = new SqlCommand(
                @"SELECT TOP (1) fs.full_filesystem_path
FROM sys.dm_os_enumerate_filesystem(@directory, N'system_health_0_*.xel') AS fs
WHERE fs.size_in_bytes >= 1000000
AND   fs.full_filesystem_path <> @current
ORDER BY fs.size_in_bytes DESC;", connection);
            list.Parameters.Add(new SqlParameter("@directory", SqlDbType.NVarChar, 260) { Value = directory });
            list.Parameters.Add(new SqlParameter("@current", SqlDbType.NVarChar, 260) { Value = server.CurrentFile });
            older = await list.ExecuteScalarAsync(ct) as string;
        }

        Assert.SkipWhen(older is null, "No older system_health file of 1 MB or more to put a bookmark in.");
        var rolled = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SystemHealthEventsCollector.BookmarkStateKey] = SystemHealthEventsCollector.FormatBookmark(s_longAgo, 98304, older!),
        };
        var crossed = await RunPassAsync(server, rolled, watermark: s_longAgo);
        Assert.Equal(1, Measured(crossed.Context, "event_file_wildcard"));
        Assert.Null(Measured(crossed.Context, "event_file_fallback"));
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

        /* Uncapped: a current-file read the cap stops would leave its scan position as the new bookmark instead. */
        var pass = await RunPassAsync(server, state, watermark: future, maxEvents: Uncapped);

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

    private static int ComparePosition(string file, long offset, string otherFile, long otherOffset)
    {
        var byFile = string.Compare(file, otherFile, StringComparison.OrdinalIgnoreCase);
        return byFile != 0 ? byFile : offset.CompareTo(otherOffset);
    }

    private static (string File, long Offset) PositionOf(SystemHealthEventsCollector.Row row) =>
        (row.SourceFile!, row.SourceOffset!.Value);

    /// <summary>
    /// Capped passes chained the way the host runs them: each pass gets the state the last one left and the stored watermark
    /// (the newest event stored, never below the one the chain started from), until a pass returns nothing and moves no
    /// bookmark. A pass holds to the cap, except one that read a single buffer whole.
    /// </summary>
    private static async Task<Chain> ChainAsync(Server server, DateTime watermark, int cap, int? asMajor)
    {
        var seen = new List<SystemHealthEventsCollector.Row>();
        var passes = new List<ChainPass>();
        var elapsed = TimeSpan.Zero;
        Dictionary<string, string>? state = null;
        string? previousBookmark = null;
        string? endFile = null;
        long endOffset = 0;

        while (passes.Count < 400)
        {
            var fromBookmark = state is not null && SystemHealthEventsCollector.TryUseBookmark(state, watermark) is not null;
            var pass = await RunPassAsync(server, state, watermark, maxEvents: cap, asMajor: asMajor);
            elapsed += pass.Elapsed;
            var number = passes.Count + 1;
            Assert.True(
                pass.Rows.Count <= cap || pass.Rows.Select(PositionOf).Distinct().Count() == 1,
                $"pass {number} returned {pass.Rows.Count} rows from more than one buffer against a cap of {cap}");
            Assert.Null(Measured(pass.Context, "event_file_fallback"));
            Assert.Null(Measured(pass.Context, "event_file_buffer_cut"));

            pass.Context.PendingState.TryGetValue(SystemHealthEventsCollector.BookmarkStateKey, out var bookmark);
            var moved = bookmark is not null && !string.Equals(bookmark, previousBookmark, StringComparison.Ordinal);
            if (moved)
            {
                var parts = bookmark!.Split('|', 3);
                endOffset = long.Parse(parts[1], CultureInfo.InvariantCulture);
                endFile = parts[2];
            }

            passes.Add(new ChainPass(watermark, fromBookmark, endFile, endOffset, pass.Rows.Count));
            if (!moved)
            {
                Assert.True(pass.Rows.Count == 0, $"pass {number} returned {pass.Rows.Count} rows and left the bookmark where it was");
                break;
            }

            seen.AddRange(pass.Rows);
            previousBookmark = bookmark;
            state = StateFrom(pass.Context);
            if (pass.Rows.Count > 0)
            {
                var newest = pass.Rows.Max(r => r.EventTime!.Value);
                watermark = newest > watermark ? newest : watermark;
            }
        }

        TestContext.Current.SendDiagnosticMessage(
            $"chained capped passes (cap {cap}, major {asMajor?.ToString(CultureInfo.InvariantCulture) ?? "own"}): {passes.Count} passes, {seen.Count} events, {elapsed.TotalSeconds:0.0} s");
        return new Chain(seen, passes);
    }

    /// <summary>
    /// The chain stored exactly the events it must have, compared with an uncapped read of the same file taken before it
    /// (events written after that read are left out on both sides). Each event belongs to the first pass whose bookmark
    /// reached its buffer (the last pass takes the rest), and is stored when it is newer than that pass's watermark, or at
    /// it on a read from a bookmark, which keeps the cutoff inclusive. The one loss the design accepts is an event older
    /// than the newest event stored from the buffers before it; nothing else may be missing or stored twice.
    /// </summary>
    private static void AssertChainStoredExactly(List<SystemHealthEventsCollector.Row> reference, Chain chain)
    {
        var end = reference.Select(PositionOf).Aggregate((a, b) => ComparePosition(a.File, a.Offset, b.File, b.Offset) >= 0 ? a : b);
        var expected = reference.Where(row =>
        {
            var (file, offset) = PositionOf(row);
            var pass = chain.Passes.FirstOrDefault(p => p.EndFile is not null && ComparePosition(file, offset, p.EndFile, p.EndOffset) <= 0)
                ?? chain.Passes[^1];
            return pass.FromBookmark ? row.EventTime >= pass.Watermark : row.EventTime > pass.Watermark;
        });
        var stored = chain.Seen.Where(row => ComparePosition(row.SourceFile!, row.SourceOffset!.Value, end.File, end.Offset) <= 0);

        Assert.Equal(Keys(expected), Keys(stored));
    }

    [Fact]
    public async Task TheCap_HoldsEachPassToTheCap_AndTheChainedPassesStoreExactlyTheEventsTheFileHolds()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        var reference = (await RunPassAsync(server, state: null, watermark: s_longAgo, maxEvents: Uncapped)).Rows;
        Assert.SkipWhen(reference.Count < 40, "The current system_health file holds too few events to chain capped passes over.");

        /* A cap of 200, or one under the fullest buffer's event count (not below 50), so a pass meets a buffer that holds
           more events than the cap: that buffer is read whole, not cut where the cap fell. */
        var fullest = reference.GroupBy(PositionOf).Max(g => g.Count());
        var cap = Math.Clamp(fullest - 1, 50, 200);
        var chain = await ChainAsync(server, s_longAgo, cap, asMajor: null);

        Assert.True(chain.Passes.Count > 2, "the cap never bound, so the chain proved nothing");
        AssertChainStoredExactly(reference, chain);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(13)]
    public async Task AFirstRead_WithAWatermarkMidFile_StopsAtTheCap_AndTheChainMovesPastTheOlderEvents(int? asMajor)
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;
        Assert.SkipWhen(
            asMajor is null && server.Major < SystemHealthEventsCollector.TimestampUtcMajorVersion,
            "The server's own batch is the 2016 batch, which the asMajor: 13 case runs.");

        const int Cap = 200;
        var reference = (await RunPassAsync(server, state: null, watermark: s_longAgo, maxEvents: Uncapped, asMajor: asMajor)).Rows;
        Assert.SkipWhen(reference.Count < 4 * Cap, "The current system_health file holds too few events to put a watermark past two caps' worth.");

        /* The watermark: the newest event in the buffers up to the middle of the file, so at least two caps' worth of
           events, the first pass's among them, are at or before it. */
        var middle = PositionOf(reference[(reference.Count / 2) - 1]);
        var watermark = reference
            .Where(r => ComparePosition(r.SourceFile!, r.SourceOffset!.Value, middle.File, middle.Offset) <= 0)
            .Max(r => r.EventTime!.Value);

        var chain = await ChainAsync(server, watermark, Cap, asMajor);

        /* The first read with no bookmark counts the older events toward the cap (2016 has no timestamp_utc; on 2017+ the
           time test sits in the CASE, not in the WHERE inside the TOP), so it stops after Cap raw events with no row to
           store, and still moves the bookmark past them. */
        Assert.Equal(0, chain.Passes[0].Rows);
        Assert.NotNull(chain.Passes[0].EndFile);
        Assert.Contains(chain.Passes, p => p.Rows > 0);
        AssertChainStoredExactly(reference, chain);
    }

    [Fact]
    public async Task ABufferHoldingMoreEventsThanTheCap_IsReadWhole_AndOneBeyondTheReReadMultipleOfTheCapIsCutAndRecorded()
    {
        var server = await OpenAsync();
        await using var connection = server.Connection;

        /* The fullest buffer that has an earlier buffer to bookmark: a pass from that bookmark starts at it. */
        var reference = (await RunPassAsync(server, state: null, watermark: s_longAgo, maxEvents: Uncapped)).Rows;
        var buffers = reference.GroupBy(PositionOf).ToList();
        var fullest = buffers.Skip(1).OrderByDescending(g => g.Count()).FirstOrDefault();
        Assert.SkipWhen(fullest is null || fullest.Count() < 2, "The current system_health file has no later buffer holding two or more of the captured events.");
        var before = buffers[buffers.IndexOf(fullest!) - 1].Key;
        var state = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SystemHealthEventsCollector.BookmarkStateKey] = SystemHealthEventsCollector.FormatBookmark(s_longAgo, before.Offset, before.File),
        };

        /* A cap one under the buffer's count: every capped event is in that buffer, so it is read again and kept whole, and
           the bookmark moves to it. */
        var whole = await RunPassAsync(server, state, s_longAgo, maxEvents: fullest!.Count() - 1);
        Assert.Equal(Keys(fullest!), Keys(whole.Rows));
        Assert.Null(Measured(whole.Context, "event_file_buffer_cut"));
        var moved = SystemHealthEventsCollector.TryUseBookmark(whole.Context.PendingState, DateTime.MaxValue);
        Assert.NotNull(moved);
        Assert.Equal(fullest.Key.File, moved.Value.File, ignoreCase: true);
        Assert.Equal(fullest.Key.Offset, moved.Value.Offset);

        /* A cap whose re-read multiple (BufferReReadFactor) is still under the buffer's count: the pass keeps the events it read, all from that buffer,
           and records the cut. */
        Assert.SkipWhen(fullest.Count() <= SystemHealthEventsCollector.BufferReReadFactor, "The fullest buffer holds too few events to outgrow the re-read's multiple of a cap of one.");
        var cutCap = (fullest.Count() - 1) / SystemHealthEventsCollector.BufferReReadFactor;
        var cut = await RunPassAsync(server, state, s_longAgo, maxEvents: cutCap);
        Assert.Equal(cutCap * SystemHealthEventsCollector.BufferReReadFactor, cut.Rows.Count);
        Assert.All(cut.Rows, row => Assert.Equal(fullest.Key.Offset, row.SourceOffset));
        Assert.Equal(1, Measured(cut.Context, "event_file_buffer_cut"));
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
        var own = await RunPassAsync(server, state: null, watermark: s_longAgo, maxEvents: Uncapped);
        Assert.Null(Measured(own.Context, "event_file_fallback"));
        Assert.Equal(
            server.Major >= SystemHealthEventsCollector.TimestampUtcMajorVersion,
            SystemHealthEventsCollector.Instance
                .BuildQuery(MakeContext(server.Major, null, s_longAgo)).Text.Contains("timestamp_utc", StringComparison.Ordinal));

        if (server.Major >= SystemHealthEventsCollector.TimestampUtcMajorVersion)
        {
            var asOld = await RunPassAsync(server, state: null, watermark: s_longAgo, maxEvents: Uncapped, asMajor: 13);
            Assert.Null(Measured(asOld.Context, "event_file_fallback"));
            /* The file keeps growing between the reads, so compare what both could see: events up to the first read's newest. */
            var bound = own.Rows.Max(r => r.EventTime!.Value);
            Assert.Equal(Keys(own.Rows), Keys(asOld.Rows.Where(r => r.EventTime <= bound)));

            /* A watermark inside the file: the timestamp_utc filter before the XML cast and the XML time filter agree. */
            Assert.SkipWhen(own.Rows.Count < 20, "The current system_health file holds too few events to cut a watermark in.");
            var cut = own.Rows[own.Rows.Count / 2].EventTime!.Value;
            var newer = await RunPassAsync(server, state: null, watermark: cut, maxEvents: Uncapped);
            var newerOld = await RunPassAsync(server, state: null, watermark: cut, maxEvents: Uncapped, asMajor: 13);
            Assert.NotEmpty(newer.Rows);
            Assert.All(newer.Rows, row => Assert.True(row.EventTime > cut));
            Assert.Equal(Keys(newer.Rows.Where(r => r.EventTime <= bound)), Keys(newerOld.Rows.Where(r => r.EventTime <= bound)));
        }
    }
}
