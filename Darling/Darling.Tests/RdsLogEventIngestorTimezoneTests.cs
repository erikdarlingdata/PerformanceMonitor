/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.RDS;
using Amazon.RDS.Model;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="RdsLogEventIngestor"/>'s <c>logTimezoneIsUtc</c> parameter (#4046 part 1b) — the managed
/// route's twin of #4049's self-hosted fix, and <see cref="RdsDeadlockIngestorTests"/>'s sibling suite for
/// the other log-tail transport.
/// </summary>
public sealed class RdsLogEventIngestorTimezoneTests
{
    private const string DeadStore =
        "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private const string Host = "solo.abc123.us-east-1.rds.amazonaws.com";

    private const string UtcLine =
        "2026-08-26 22:25:24.100 UTC [1549] LOG:  connection authorized: user=app database=appdb\n";

    private static readonly string NonUtcLine = UtcLine.Replace(" UTC ", " EST ", StringComparison.Ordinal);

    private sealed class FakeRds : AmazonRDSClient
    {
        public FakeRds() : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"), Amazon.RegionEndpoint.USEast1) { }

        public string FirstBody { get; init; } = UtcLine;

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, System.Threading.CancellationToken cancellationToken = default)
            => Task.FromResult(new DescribeDBLogFilesResponse
            {
                DescribeDBLogFiles = new List<DescribeDBLogFilesDetails>
                {
                    new() { LogFileName = "error/postgresql.log.2026-08-26-22", LastWritten = 9999 },
                },
            });

        public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
            DownloadDBLogFilePortionRequest request, System.Threading.CancellationToken cancellationToken = default)
            => Task.FromResult(new DownloadDBLogFilePortionResponse
            {
                LogFileData = FirstBody,
                Marker = "MARKER-1",
                AdditionalDataPending = false,
            });
    }

    /// <summary>
    /// A window whose only line is stamped in a foreign zone is skipped and counted rather than refused
    /// (#4046 part 1b), the same trade the self-hosted route already makes. Zero survivors means
    /// <c>StoreAsync</c> returns before ever opening the store, so this runs over the same dead store the
    /// deadlock suite uses for the equivalent case.
    /// </summary>
    [Fact]
    public async Task LogTimezoneIsUtcTrue_SkipsAndCountsAForeignZoneLine_RatherThanRefusing()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var logs = new RdsLogSource(_ => new FakeRds { FirstBody = NonUtcLine });
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        var outcome = await ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true);

        Assert.Equal(0, outcome.Rows);
        Assert.Equal(1, outcome.ForeignZoneLines);
        Assert.True(outcome.SourceReached);
    }

    /// <summary>
    /// With <c>logTimezoneIsUtc: false</c> (today's default), a foreign-zone line is still refused rather
    /// than silently skipped — #4046 part 1b only changes behavior for a target whose own setting was read
    /// as UTC.
    /// </summary>
    [Fact]
    public async Task LogTimezoneIsUtcFalse_KeepsTodaysRefusal()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var logs = new RdsLogSource(_ => new FakeRds { FirstBody = NonUtcLine });
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(
            () => ingestor.IngestAsync(1, "target-a", Host));
    }

    /// <summary>
    /// A UTC-only window still stores normally under the new parameter — the happy path is unchanged.
    /// Proven the way <c>RdsDeadlockIngestorTests</c> proves "reaches the write": the dead store turns a
    /// non-empty batch into a throw that is not <see cref="RdsLogUnavailableException"/>.
    /// </summary>
    [Fact]
    public async Task LogTimezoneIsUtcTrue_AUtcLineStillReachesTheWrite()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);
        var logs = new RdsLogSource(_ => new FakeRds { FirstBody = UtcLine });
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true));

        Assert.IsNotType<RdsLogUnavailableException>(failure);
    }

    /// <summary>
    /// #4501 round 2: a K1-shaped line — a client field quoting <c>ERROR:  </c> in text it sends — must
    /// still reach the write over this transport. Before this fix the ingestor called the 3-arg
    /// <c>Classify</c> overload, which applies the forgery rule with NO separator check — the fallback for a
    /// prefix that was "not yet collected" — and refused this exact shape even though RDS's prefix is fixed
    /// and known (<see cref="RdsLogEventIngestor.DefaultLogLinePrefix"/>).
    ///
    /// <para>The line is at WARNING, not LOG: a bare LOG-severity statement line is classified into an entry
    /// either way, but no family parser recognises it (<see cref="PgErrorEventParser"/> only claims WARNING
    /// or worse), so it produces zero events and reaches no write on EITHER prefix path — that shape cannot
    /// distinguish the fix. WARNING is the lowest severity the error family parser claims, so this is the
    /// smallest change that still proves the write is reached only when the fixed prefix is passed.</para>
    ///
    /// <para>Proven through <see cref="RdsLogEventIngestor.IngestAsync"/> itself, the product's own call
    /// path, the same way the happy-path test above proves "reaches the write": the dead store turns a
    /// non-empty batch into a throw that is not <see cref="RdsLogUnavailableException"/>.</para>
    /// </summary>
    [Fact]
    public async Task AK1ShapedStatementLine_StillReachesTheWrite()
    {
        var k1 = "2026-08-26 22:25:24.100 UTC:192.0.2.10(52345):app_rw@app_db:[1549]:WARNING:  statement: SELECT 'ERROR:  x'\n";

        await using var store = NpgsqlDataSource.Create(DeadStore);
        var logs = new RdsLogSource(_ => new FakeRds { FirstBody = k1 });
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true));

        Assert.IsNotType<RdsLogUnavailableException>(failure);
    }

    /// <summary>
    /// #4501 round 2: a real server message carrying a libpq error inside it (a logical-replication worker's
    /// own line) must still reach the write over this transport. RDS/Aurora's prefix is fixed and known
    /// (<see cref="RdsLogEventIngestor.DefaultLogLinePrefix"/>), so passing it — rather than falling back to
    /// the no-separator-check overload the 3-arg <c>Classify</c> call used before this fix — keeps this line
    /// as ERROR (ERROR and FATAL agree) instead of refusing it as a forgery. Proven through
    /// <see cref="RdsLogEventIngestor.IngestAsync"/> itself, the product's own call path, the same way the
    /// happy-path test above proves "reaches the write": the dead store turns a non-empty batch into a throw
    /// that is not <see cref="RdsLogUnavailableException"/>.
    /// </summary>
    [Fact]
    public async Task AnErrorCarryingAFatal_StillReachesTheWrite()
    {
        var line = "2026-08-26 22:25:24.100 UTC:192.0.2.10(52345):app_rw@app_db:[1549]:ERROR:  could not connect to the publisher: FATAL:  password authentication failed\n";

        await using var store = NpgsqlDataSource.Create(DeadStore);
        var logs = new RdsLogSource(_ => new FakeRds { FirstBody = line });
        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs);

        var failure = await Assert.ThrowsAnyAsync<Exception>(
            () => ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true));

        Assert.IsNotType<RdsLogUnavailableException>(failure);
    }
}
