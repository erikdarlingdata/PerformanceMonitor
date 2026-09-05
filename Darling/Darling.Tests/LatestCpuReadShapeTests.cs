/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Darling reads the newest CPU sample for one server in three places, and both halves of the shape they
/// share are load-bearing, so both are pinned — across the whole tree rather than per constant, because the
/// defect this guards was three copies of one query and nothing stopping a fourth.
///
/// <para><b>Order on the partition column.</b> <c>cpu_utilization_stats</c> is a hypertable partitioned on
/// <c>collection_time</c>: <c>TimescaleSupport.CreateHypertableSql</c> takes the definition's
/// <c>PrefixTimeColumnName</c> and <c>CpuUtilizationCollector</c> keeps the <c>collection_time</c> default, so
/// <c>sample_time</c> is an ordinary payload column with neither an index nor partition affinity. Ordering on
/// the dimension earns TimescaleDB's ORDERED ChunkAppend, which walks chunks newest-first and stops at the
/// first that yields a row; ordering on <c>sample_time</c> appends every chunk and top-N sorts the server's
/// whole retained history to return one row. Measured against a seeded thirty-chunk store: 47,752 rows and
/// 839 buffers per call versus 6 buffers, and the alert sweep makes this call once per server per tick.</para>
///
/// <para><b>Carry no store-clock predicate.</b> <c>sample_time</c> is the monitored server's LOCAL wall clock
/// on the ring-buffer arm (#1262, pinned by <see cref="CollectorTimestampFrameTests"/>) and naive UTC on the
/// Azure SQL DB arm, so the obvious-looking <c>sample_time &gt; now() - INTERVAL '1 hour'</c> compares two
/// clock frames. It returns ZERO rows for every server behind the store's clock, and zero rows here reads as
/// "this server has no CPU data" rather than as a failure — no exception, no log line, CPU alerting silently
/// off. <see cref="LatestCpuReadShapeLivePostgresTests"/> demonstrates that against seeded data instead of
/// arguing it from the shape.</para>
/// </summary>
public sealed class LatestCpuReadShapeSqlTests
{
    /// <summary>The required ordering, as the substring common to the per-server reads
    /// (<c>ORDER BY collection_time DESC, ...</c>) and the fleet-wide one
    /// (<c>ORDER BY server_id, collection_time DESC, ...</c>): the partition column ahead of
    /// <c>sample_time</c>, which is only the within-batch tiebreak.</summary>
    private const string RequiredOrdering = "collection_time DESC, sample_time DESC";

    /// <summary>
    /// The number of latest-CPU reads in <c>Darling/</c>: the alert sweep's, the viewer's server-summary
    /// card, the MCP health reader's, and the MCP fleet reader's per-server-newest <c>DISTINCT ON</c>. A hard
    /// floor rather than an exact count so adding a fifth read is allowed; what is not allowed is the scan
    /// silently finding NOTHING and passing vacuously, which is the only way this whole class could stop
    /// guarding without going red.
    ///
    /// <para>It was 3 while the extraction keyed on <c>LIMIT 1</c> alone, and the fleet read — the only one
    /// of the four with no <c>server_id</c> filter, so the one where a mis-framed bound would take the whole
    /// fleet — was the copy the guard could not see.</para>
    /// </summary>
    private const int KnownLatestCpuReadCount = 4;

    [Fact]
    public void EveryLatestCpuRead_OrdersOnThePartitionColumnWithASampleTimeTiebreak()
    {
        var reads = LatestCpuReads();

        Assert.True(
            reads.Count >= KnownLatestCpuReadCount,
            $"Found {reads.Count} latest-CPU read(s) under Darling/, expected at least "
            + $"{KnownLatestCpuReadCount}. The scan below is the enforcement, so finding nothing is a broken "
            + "scan rather than a clean tree — fix the extraction, do not lower the floor.");

        foreach (var (where, sql) in reads)
        {
            Assert.Contains(RequiredOrdering, sql, StringComparison.Ordinal);

            /* sample_time must not be the LEADING sort key by any route: that is the shape being replaced,
               and it costs the server's whole retained history plus a sort to return one row. */
            /* sample_time must not be the leading TIME key by any route - not on its own, and not behind
               the fleet read's server_id. That is the shape being replaced, and it is also the shape that
               invites a bound on a local-clock column. */
            Assert.False(
                Regex.IsMatch(sql, @"ORDER\s+BY\s+(?:server_id\s*,\s*)?sample_time"),
                $"{where}: leads its ordering on sample_time, which has no index, is not the partition "
                + $"column, and is not in the store's clock frame. Put \"{RequiredOrdering}\" ahead of it "
                + "— see DarlingWorker.LatestCpuSql.");
        }
    }

    [Fact]
    public void EveryLatestCpuRead_ComparesNoColumnToAStoreClock()
    {
        var reads = LatestCpuReads();

        Assert.True(reads.Count >= KnownLatestCpuReadCount, "broken scan — see the ordering test's message.");

        foreach (var (where, sql) in reads)
        {
            /* now() / CURRENT_TIMESTAMP / LOCALTIMESTAMP are all the STORE's clock, and none of them shares a
               frame with sample_time. A bound on collection_time WOULD be frame-correct, but it buys nothing
               once the ordering prunes to one chunk, and it drops a server that has stopped reporting out of
               alerting altogether — so these reads carry no time predicate at all and the guard is absolute
               rather than per-column. */
            foreach (var clock in new[] { "now(", "CURRENT_TIMESTAMP", "LOCALTIMESTAMP" })
            {
                Assert.False(
                    sql.Contains(clock, StringComparison.OrdinalIgnoreCase),
                    $"{where}: compares a column to the store clock ({clock}). sample_time is the monitored "
                    + "server's local wall clock, so that returns zero rows — read as \"no CPU data\", not as "
                    + "an error — for every server behind the store. See DarlingWorker.LatestCpuSql.");
            }
        }
    }

    [Fact]
    public void LatestCpuSql_ReadsTheRawTableForOneServerAndReturnsOneRow()
    {
        Assert.Contains("FROM cpu_utilization_stats", DarlingWorker.LatestCpuSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", DarlingWorker.LatestCpuSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", DarlingWorker.LatestCpuSql, StringComparison.Ordinal);
        Assert.Contains(RequiredOrdering, DarlingWorker.LatestCpuSql, StringComparison.Ordinal);
        Assert.Contains(
            "SELECT sqlserver_cpu_utilization, other_process_cpu_utilization",
            DarlingWorker.LatestCpuSql,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The scan's own positive control. The extraction below is the enforcement mechanism, so a change that
    /// quietly stops it matching would disarm both tests above while leaving them green; this pins that it
    /// finds the shape it is looking for and rejects the two it must not confuse with it.
    /// </summary>
    [Theory]
    /* A latest-row read: table, LIMIT 1, and the ordering under test. */
    [InlineData(true, "const string S = @\"\nSELECT a FROM cpu_utilization_stats\nWHERE server_id = $1\nORDER BY collection_time DESC, sample_time DESC\nLIMIT 1\";")]
    /* The fleet-wide shape: newest-per-server via DISTINCT ON, so no LIMIT 1 and no $1 at all. Keying on
       LIMIT 1 alone left this one invisible, which is how a fourth copy already existed under a guard whose
       own doc worried about a fourth copy being added. */
    [InlineData(true, "const string S = @\"\nSELECT DISTINCT ON (server_id)\n    server_id, a\nFROM v_cpu_utilization_stats\nORDER BY server_id, collection_time DESC, sample_time DESC\";")]
    /* DISTINCT ON something OTHER than server_id is not a per-server-newest read. */
    [InlineData(false, "const string S = @\"\nSELECT DISTINCT ON (database_name)\n    database_name, a\nFROM v_cpu_utilization_stats\nORDER BY database_name, collection_time DESC\";")]
    /* The v_ passthrough view counts too — the viewer and MCP reads go through it. */
    [InlineData(true, "const string S = @\"\nSELECT a FROM v_cpu_utilization_stats\nWHERE server_id = $1\nORDER BY collection_time DESC, sample_time DESC\nLIMIT 1\";")]
    /* A WINDOWED read is a different query under different rules — it legitimately bounds collection_time,
       and bounding is the correct answer there — so the second parameter must keep it out rather than
       dragging it under the no-predicate rule. */
    [InlineData(false, "const string S = @\"\nSELECT a FROM cpu_utilization_stats\nWHERE server_id = $1\nAND collection_time >= $2\nORDER BY collection_time\";")]
    /* The shape that made this discriminator necessary: PgAnomalyDetector.CpuWindowSql picks the PEAK
       sample inside a bounded window, so it is a windowed read that happens to LIMIT 1 and orders on the
       CPU value rather than on time. A LIMIT-1-only rule pulled it in and demanded a fix it does not
       need. */
    [InlineData(false, "const string S = @\"\nSELECT (SELECT collection_time FROM v_cpu_utilization_stats\nWHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3\nORDER BY sqlserver_cpu_utilization DESC LIMIT 1) AS peak_time\nFROM v_cpu_utilization_stats\nWHERE server_id = $1\";")]
    /* A different table entirely. */
    [InlineData(false, "const string S = @\"\nSELECT a FROM memory_stats\nWHERE server_id = $1\nORDER BY collection_time DESC\nLIMIT 1\";")]
    public void TheScanFindsLatestRowCpuReadsAndNothingElse(bool expected, string source)
    {
        Assert.Equal(expected, ExtractLatestCpuReads(source, "synthetic").Count == 1);
    }

    /// <summary>
    /// Every latest-CPU read under <c>Darling/</c>, as (location, SQL) pairs. Reads the SOURCE TREE rather
    /// than the constants so it reaches the WPF viewer without this net10.0-windows test assembly having to
    /// resolve a WPF project reference, and so a fourth copy anywhere under Darling/ is covered the moment it
    /// is written. This test project is skipped: it holds the replaced shape and the trap predicate on purpose,
    /// as the oracle and the positive control the live tests compare against.
    /// </summary>
    private static List<(string Where, string Sql)> LatestCpuReads()
    {
        var darling = Path.Combine(RepoRoot(), "Darling");
        var found = new List<(string, string)>();

        foreach (var file in Directory.EnumerateFiles(darling, "*.cs", SearchOption.AllDirectories))
        {
            var segments = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (segments.Contains("bin") || segments.Contains("obj") || segments.Contains("Darling.Tests"))
            {
                continue;
            }

            found.AddRange(ExtractLatestCpuReads(File.ReadAllText(file), Path.GetFileName(file)));
        }

        return found;
    }

    /// <summary>
    /// One file's latest-row CPU reads. Comment spans go first (this codebase's SQL carries its reasoning in
    /// <c>/* ... */</c> blocks that quote the very predicates matched here), then each mention of the table is
    /// taken out to the end of its verbatim literal and kept if that span is a latest-row read in either
    /// shape: per-server (<c>LIMIT 1</c> with <c>$1</c> as its only parameter) or fleet-wide
    /// (<c>DISTINCT ON (server_id)</c>, which has neither).
    ///
    /// <para>The single-parameter test is what separates this class from a WINDOWED read. A windowed read
    /// takes its bounds as <c>$2</c>/<c>$3</c> and bounding <c>collection_time</c> is the right answer there;
    /// <c>PgAnomalyDetector.CpuWindowSql</c> is one, and a <c>LIMIT 1</c>-only rule swept it in and demanded a
    /// change it does not need. Both directions are pinned by
    /// <see cref="TheScanFindsLatestRowCpuReadsAndNothingElse"/>.</para>
    /// </summary>
    private static List<(string Where, string Sql)> ExtractLatestCpuReads(string text, string where)
    {
        var code = Regex.Replace(text, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        code = Regex.Replace(code, @"//[^\r\n]*", " ");

        var reads = new List<(string, string)>();
        foreach (Match match in Regex.Matches(code, @"FROM\s+v?_?cpu_utilization_stats\b"))
        {
            /* The enclosing verbatim literal, both ends: DISTINCT ON and the select list sit BEFORE the
               table name, so a forward-only span would miss the fleet shape entirely. A read that lacks
               either end is not a SQL literal and is left alone rather than guessed at. */
            var close = code.IndexOf("\";", match.Index, StringComparison.Ordinal);
            var open = code.LastIndexOf("@\"", match.Index, StringComparison.Ordinal);
            if (close < 0 || open < 0)
            {
                continue;
            }

            var span = code[open..close];
            var perServerNewest = span.Contains("LIMIT 1", StringComparison.Ordinal)
                                  && !span.Contains("$2", StringComparison.Ordinal);
            var fleetNewest = span.Contains("DISTINCT ON (server_id)", StringComparison.Ordinal);
            if (perServerNewest || fleetNewest)
            {
                reads.Add((where, span));
            }
        }

        return reads;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the latest-CPU read against the real hypertable. Serialized
/// through the "live-postgres" collection; uses negative sentinel server_ids and cleans up in finally, the
/// same contract as the sibling live suites.
///
/// <para>What a shape pin cannot show is that the shipped ordering returns the SAME ROW the unbounded
/// <c>sample_time</c> order returned, for servers whose local clock sits at different offsets from the
/// store's. These seed exactly that: two servers behind UTC, two ahead (one on a half-hour offset), and one
/// in the store's own frame — that last being the single case a broken frame comparison still passes, so it
/// is here as the control rather than as the whole test.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class LatestCpuReadShapeLivePostgresTests
{
    /// <summary>Sentinel ids, negative so they cannot collide with a real registry entry.</summary>
    private const int FirstServerId = -9151;

    /// <summary>Offsets from UTC for the seeded servers, in minutes: behind, behind, at, ahead on a half
    /// hour, ahead.</summary>
    private static readonly int[] s_offsetMinutes = { -420, -240, 0, 330, 600 };

    /// <summary>The unbounded <c>sample_time</c> order, kept as the oracle the shipped read must match
    /// row-for-row. Slow by construction — it appends every chunk and sorts — which is why it is the oracle
    /// and not the read.</summary>
    private const string OracleSql = @"
SELECT sqlserver_cpu_utilization, other_process_cpu_utilization
FROM cpu_utilization_stats
WHERE server_id = $1
ORDER BY sample_time DESC
LIMIT 1";

    /// <summary>The predicate that looks like the fix and is not. Anchored on <c>now() AT TIME ZONE 'UTC'</c>
    /// so the assertion does not itself depend on the store session's TimeZone, and still empty for a server
    /// behind the store.</summary>
    private const string TrapSql = @"
SELECT sqlserver_cpu_utilization, other_process_cpu_utilization
FROM cpu_utilization_stats
WHERE server_id = $1
AND   sample_time > (now() AT TIME ZONE 'UTC') - INTERVAL '1 hour'
ORDER BY sample_time DESC
LIMIT 1";

    [Fact]
    public async Task LatestCpuRead_ReturnsTheOracleRow_AtEveryUtcOffset_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-CPU read test.");

        var cancellationToken = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await PgMigrations.MigrateAsync(connection, cancellationToken);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, cancellationToken);

            /* Three polls per server one minute apart, each carrying one new ring-buffer sample: the steady
               state at the collector's one-minute frequency. The CPU values carry the poll ordinal so the row
               that comes back is identifiable rather than merely equal to the oracle's. */
            var newestUtc = TruncateToSeconds(DateTime.UtcNow);
            for (var i = 0; i < s_offsetMinutes.Length; i++)
            {
                for (var poll = 0; poll < 3; poll++)
                {
                    var collectionTime = newestUtc.AddMinutes(-poll);
                    await InsertSampleAsync(
                        connection,
                        FirstServerId - i,
                        collectionTime,
                        collectionTime.AddMinutes(s_offsetMinutes[i]),
                        sqlCpu: 10 - poll,
                        otherCpu: 20 - poll,
                        cancellationToken);
                }
            }

            for (var i = 0; i < s_offsetMinutes.Length; i++)
            {
                var serverId = FirstServerId - i;
                var shipped = await ReadAsync(connection, DarlingWorker.LatestCpuSql, serverId, cancellationToken);
                var oracle = await ReadAsync(connection, OracleSql, serverId, cancellationToken);

                Assert.NotNull(shipped);
                Assert.Equal(oracle, shipped);

                /* Not merely agreement with the oracle — the newest poll's own values, so a shape that agreed
                   with a broken oracle would still fail here. */
                Assert.Equal((10, 20), shipped);
            }

            bodySucceeded = true;
        }
        finally
        {
            /* #1794/#1902: teardown gets its own freshly-opened connection, because the body's is the one
               thing the failure being reported may have destroyed — and a throw from finally would
               replace the body's exception with connection noise. */
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    [Fact]
    public async Task TheSampleTimePredicate_LosesEveryServerBehindTheStoresClock_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-CPU trap test.");

        var cancellationToken = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await PgMigrations.MigrateAsync(connection, cancellationToken);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, cancellationToken);

            var newestUtc = TruncateToSeconds(DateTime.UtcNow);

            /* One server behind the store by more than the window, one in the store's own frame. */
            await InsertSampleAsync(connection, FirstServerId, newestUtc, newestUtc.AddHours(-6), 33, 44, cancellationToken);
            await InsertSampleAsync(connection, FirstServerId - 1, newestUtc, newestUtc, 55, 66, cancellationToken);

            /* The shipped read is indifferent to the offset: both servers report. */
            Assert.Equal((33, 44), await ReadAsync(connection, DarlingWorker.LatestCpuSql, FirstServerId, cancellationToken));
            Assert.Equal((55, 66), await ReadAsync(connection, DarlingWorker.LatestCpuSql, FirstServerId - 1, cancellationToken));

            /* The predicate loses the server behind the store entirely, and not as an error — as an empty
               result the alert engine reads as "no CPU data". The same-frame server is the positive control
               proving the predicate is well-formed and the empty result above is not a typo. */
            Assert.Null(await ReadAsync(connection, TrapSql, FirstServerId, cancellationToken));
            Assert.Equal((55, 66), await ReadAsync(connection, TrapSql, FirstServerId - 1, cancellationToken));

            bodySucceeded = true;
        }
        finally
        {
            /* #1794/#1902: teardown gets its own freshly-opened connection, because the body's is the one
               thing the failure being reported may have destroyed — and a throw from finally would
               replace the body's exception with connection noise. */
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    [Fact]
    public async Task LatestCpuRead_PicksTheNewestSampleInsideOneBatch_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-CPU tiebreak test.");

        var cancellationToken = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await PgMigrations.MigrateAsync(connection, cancellationToken);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, cancellationToken);

            /* One poll catching up on a ring-buffer backlog: thirty samples under ONE collection_time. They
               go in NEWEST-sample-first, so heap order runs opposite to sample order and a scan of the tied
               group hands back the OLDEST sample unless the tiebreak sorts it. */
            var collectionTime = TruncateToSeconds(DateTime.UtcNow);
            for (var minutesAgo = 0; minutesAgo < 30; minutesAgo++)
            {
                await InsertSampleAsync(
                    connection,
                    FirstServerId,
                    collectionTime,
                    collectionTime.AddHours(-4).AddMinutes(-minutesAgo),
                    sqlCpu: minutesAgo,
                    otherCpu: minutesAgo,
                    cancellationToken);
            }

            /* The newest sample in the batch is minutesAgo == 0; an unsorted tied group returns one of the
               others, up to 29 minutes stale. */
            Assert.Equal((0, 0), await ReadAsync(connection, DarlingWorker.LatestCpuSql, FirstServerId, cancellationToken));

            bodySucceeded = true;
        }
        finally
        {
            /* #1794/#1902: teardown gets its own freshly-opened connection, because the body's is the one
               thing the failure being reported may have destroyed — and a throw from finally would
               replace the body's exception with connection noise. */
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    private static async Task<(int SqlCpu, int OtherCpu)?> ReadAsync(
        NpgsqlConnection connection, string sql, int serverId, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = LiveReadTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return (reader.GetInt32(0), reader.GetInt32(1));
    }

    private static async Task InsertSampleAsync(
        NpgsqlConnection connection,
        int serverId,
        DateTime collectionTimeUtc,
        DateTime sampleTime,
        int sqlCpu,
        int otherCpu,
        CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name,
     sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection) { CommandTimeout = LiveReadTimeoutSeconds };
        command.Parameters.AddWithValue((long)Math.Abs(serverId));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(string.Create(CultureInfo.InvariantCulture, $"sentinel{serverId}"));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(sampleTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(sqlCpu);
        command.Parameters.AddWithValue(otherCpu);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var lowest = FirstServerId - s_offsetMinutes.Length;
        using var cleanup = new NpgsqlCommand(
            string.Create(
                CultureInfo.InvariantCulture,
                $"DELETE FROM cpu_utilization_stats WHERE server_id <= {FirstServerId} AND server_id >= {lowest};"),
            connection) { CommandTimeout = LiveReadTimeoutSeconds };
        await cleanup.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Every command in this class is a sentinel-scoped seed, read or cleanup against a dev store —
    /// seconds of work. Sixty is slack for a cold container, not a budget.</summary>
    private const int LiveReadTimeoutSeconds = 60;

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}
