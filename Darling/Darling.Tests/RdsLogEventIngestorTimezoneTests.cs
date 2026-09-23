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
}
