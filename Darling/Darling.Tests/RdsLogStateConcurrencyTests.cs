/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.RDS;
using Amazon.RDS.Model;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5003: a store with several RDS targets runs the same collector for each of them at once, and every one of those
/// runs goes through ONE ingestor, which owns ONE <see cref="RdsLogSource"/>. A <c>pg_log_events</c> run and a
/// <c>pg_deadlocks</c> run each failed once with .NET's "Collection was modified; enumeration operation may not
/// execute", and both landed on the hour, when RDS rotates its log and every target adds a position for the new file.
///
/// <para>These tests drive that sharing the way the ingestors do: one source, a different instance id for each
/// thread, all of them started together. They assert nothing about timing. A run that tears the shared tables throws
/// out of the thread that was caught enumerating, or leaves a position or a debounce stamp or a carry missing, and
/// both are failures here. They are bounded stress tests rather than a fixed interleaving, because the source has no
/// hook to pause an enumeration part way and one is not worth adding to production code for this: the table is seeded
/// with a few hundred other targets' positions so a scan is long, and every round adds a key, which is what breaks a
/// scan that is already running on another thread.</para>
/// </summary>
public class RdsLogStateConcurrencyTests
{
    /// <summary>Threads that share the one source or book, each with its own instance id.</summary>
    private const int Workers = 4;

    /// <summary>Other targets' positions already in the table. A prune or a position read walks all of them, so the
    /// window a thread can be caught in is wide enough to hit inside a bounded run.</summary>
    private const int IdleInstances = 600;

    /// <summary>Rotations each thread makes: a new log file every round, as the hourly rotation does.</summary>
    private const int Rotations = 1500;

    /// <summary>Rounds for the books and the debounce table, whose operations are short.</summary>
    private const int ShortRounds = 20_000;

    private static string FileFor(int hour) => "error/postgresql.log.2026-10-03-" + hour.ToString("D5");

    private static string HostFor(string instance) => instance + ".abc123.us-east-1.rds.amazonaws.com";

    /// <summary>
    /// A client that answers from the instance id on each request, so one object can serve every thread. The source
    /// disposes the client it is handed after each read, which a client shared by four threads must survive.
    /// </summary>
    private sealed class SharedRds : AmazonRDSClient
    {
        public SharedRds() : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"), Amazon.RegionEndpoint.USEast1) { }

        /// <summary>The newest log file's hour for each instance. The listing also carries the hour before it, as RDS
        /// does until the older file ages out.</summary>
        public ConcurrentDictionary<string, int> Hour { get; } = new();

        /// <summary>Instances whose stderr file reads ten minutes newer than their csv file right now.</summary>
        public ConcurrentDictionary<string, bool> StaleCsv { get; } = new();

        protected override void Dispose(bool disposing)
        {
            /* Shared by every thread, and the source disposes whatever the factory hands it. */
        }

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
        {
            var instance = request.DBInstanceIdentifier;
            var hour = Hour.TryGetValue(instance, out var h) ? h : 1;
            var stale = StaleCsv.TryGetValue(instance, out var s) && s;

            var files = new List<DescribeDBLogFilesDetails>
            {
                new() { LogFileName = FileFor(hour), LastWritten = hour * 10L + (stale ? 10 * 60 * 1000 : 0) },
                new() { LogFileName = FileFor(hour) + ".csv", LastWritten = hour * 10L },
            };

            if (hour > 1)
            {
                files.Add(new() { LogFileName = FileFor(hour - 1), LastWritten = (hour - 1) * 10L });
            }

            return Task.FromResult(new DescribeDBLogFilesResponse { DescribeDBLogFiles = files });
        }

        public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
            DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DownloadDBLogFilePortionResponse
            {
                LogFileData = string.Empty,
                Marker = "m-" + request.LogFileName,
                AdditionalDataPending = false,
            });
    }

    /// <summary>Runs <paramref name="body"/> on a thread of its own for each of <paramref name="workers"/> workers, all
    /// released together, and completes when every one has finished. A worker that throws fails the run with its
    /// exception.</summary>
    private static async Task RunTogetherAsync(int workers, Action<int> body)
    {
        using var barrier = new Barrier(workers);

        var tasks = Enumerable.Range(0, workers)
            .Select(worker => Task.Factory.StartNew(
                () =>
                {
                    barrier.SignalAndWait();
                    body(worker);
                },
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default))
            .ToArray();

        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));
    }

    /// <summary>
    /// Four targets read their position and commit a chunk against one source while every one of them rotates onto a
    /// new file each round: the hourly rotation, run until it collides. A commit adds the new file's key and prunes the
    /// old one by walking the table, and a position read walks it too, so one thread's add lands inside another
    /// thread's walk. Nothing may throw, and afterwards every target still holds exactly the position it last
    /// committed (and the targets that were idle all along still hold theirs).
    /// </summary>
    [Fact]
    public async Task TargetsRotatingOntoNewLogFilesAtOnceEachKeepTheirOwnPosition()
    {
        var client = new SharedRds();
        var source = new RdsLogSource(_ => client);

        for (var i = 0; i < IdleInstances; i++)
        {
            Assert.True(source.RestorePosition(RdsLogSource.LogFileKind.Stderr, "idle-" + i, FileFor(0), "0"));
        }

        var failed = 0;

        await RunTogetherAsync(Workers, worker =>
        {
            var instance = "live-" + worker;

            try
            {
                for (var hour = 1; hour <= Rotations && Volatile.Read(ref failed) == 0; hour++)
                {
                    client.Hour[instance] = hour;

                    var chunk = source.ReadNewestAsync(HostFor(instance), RdsLogSource.LogFileKind.Stderr)
                        .GetAwaiter().GetResult();

                    source.CommitResume(chunk!.Value.Resume);
                }
            }
            catch
            {
                Volatile.Write(ref failed, 1);
                throw;
            }
        });

        for (var worker = 0; worker < Workers; worker++)
        {
            var instance = "live-" + worker;

            Assert.True(source.HasMarkerForKey(instance + "|" + FileFor(Rotations)), instance + " lost its last position.");
            Assert.False(source.HasMarkerForKey(instance + "|" + FileFor(Rotations - 1)), instance + " kept a position for a file it had moved past.");
        }

        for (var i = 0; i < IdleInstances; i++)
        {
            Assert.True(source.HasMarkerForKey("idle-" + i + "|" + FileFor(0)), "idle-" + i + " lost its position.");
        }
    }

    /// <summary>
    /// The csv route keeps, for each instance, when the stale-csv condition was first seen, and clears it the moment
    /// the condition goes. Four targets flip between the two states against one source. Afterwards each of them is
    /// stale again, so once the debounce has passed every one of them must raise <see cref="PgNoCsvlogFileException"/>;
    /// a stamp lost to a torn table restarts the debounce instead, and the read answers as if csvlog were fine.
    /// </summary>
    [Fact]
    public async Task TargetsFlippingTheStaleCsvConditionAtOnceEachKeepTheirOwnDebounce()
    {
        var client = new SharedRds();
        var start = new DateTime(2026, 10, 3, 5, 0, 0, DateTimeKind.Utc);
        var now = start;
        var source = new RdsLogSource(_ => client, () => now);

        var failed = 0;

        await RunTogetherAsync(Workers, worker =>
        {
            var instance = "live-" + worker;

            try
            {
                /* An even round is stale and an odd one is not, and the last round is even: the stamp exists at the end. */
                for (var round = 0; round <= ShortRounds && Volatile.Read(ref failed) == 0; round++)
                {
                    client.StaleCsv[instance] = round % 2 == 0;

                    source.ReadNewestAsync(HostFor(instance), RdsLogSource.LogFileKind.Csv).GetAwaiter().GetResult();
                }
            }
            catch
            {
                Volatile.Write(ref failed, 1);
                throw;
            }
        });

        now = start.AddMinutes(6);

        for (var worker = 0; worker < Workers; worker++)
        {
            var instance = "live-" + worker;

            await Assert.ThrowsAsync<PgNoCsvlogFileException>(
                () => source.ReadNewestAsync(HostFor(instance), RdsLogSource.LogFileKind.Csv));
        }
    }

    /// <summary>
    /// The csvlog partial-record carry is one book for the ingestor, keyed by instance, and every target's run goes
    /// through it. Four targets set and clear their own entries at once; afterwards each must still hold the carry it
    /// set last.
    /// </summary>
    [Fact]
    public async Task TargetsCommittingCsvlogCarriesAtOnceEachKeepTheirOwnCarry()
    {
        var book = new RdsCsvlogCarryBook();
        var failed = 0;

        await RunTogetherAsync(Workers, worker =>
        {
            var instance = "live-" + worker;

            try
            {
                /* An even round holds a partial record, an odd one finishes it (the entry is cleared), and the last round is even. */
                for (var round = 0; round <= ShortRounds && Volatile.Read(ref failed) == 0; round++)
                {
                    var (_, key, dropped, file) = book.CarryFor(instance + "|" + FileFor(round), startsAtFileStart: false);

                    var next = round % 2 == 0
                        ? new RdsCsvlogCarry.CsvCarry("partial-" + round, StartKnown: true)
                        : new RdsCsvlogCarry.CsvCarry(string.Empty, StartKnown: false);

                    book.Commit(key, file, next, dropped);
                }
            }
            catch
            {
                Volatile.Write(ref failed, 1);
                throw;
            }
        });

        for (var worker = 0; worker < Workers; worker++)
        {
            var (carry, _, _, _) = book.CarryFor("live-" + worker + "|" + FileFor(ShortRounds), startsAtFileStart: false);

            Assert.Equal("partial-" + ShortRounds, carry.Partial);
        }
    }

    /// <summary>The same as the csvlog book, for the deadlock report held back until the rest of it arrives.</summary>
    [Fact]
    public async Task TargetsCommittingHeldDeadlockReportsAtOnceEachKeepTheirOwnReport()
    {
        var book = new RdsDeadlockCarryBook();
        var failed = 0;

        await RunTogetherAsync(Workers, worker =>
        {
            var instance = "live-" + worker;

            try
            {
                for (var round = 0; round <= ShortRounds && Volatile.Read(ref failed) == 0; round++)
                {
                    var (_, key, file, _) = book.CarryFor(instance + "|" + FileFor(round));

                    book.Commit(key, file, round % 2 == 0 ? "report-" + round : string.Empty);
                }
            }
            catch
            {
                Volatile.Write(ref failed, 1);
                throw;
            }
        });

        for (var worker = 0; worker < Workers; worker++)
        {
            var (held, _, _, _) = book.CarryFor("live-" + worker + "|" + FileFor(ShortRounds));

            Assert.Equal("report-" + ShortRounds, held);
        }
    }

    /// <summary>
    /// The runner is one object for every target, and it builds each RDS ingestor the first time a target needs it.
    /// A plain <c>??=</c> lets two targets' first runs each build one and each keep their own, so one of the two
    /// ingestors (and the positions it holds) is dropped while its run is still going. Each is published once instead.
    /// </summary>
    [Fact]
    public void TheRunnerPublishesOneIngestorForEachRdsCollector()
    {
        var runner = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");

        foreach (var field in new[] { "_rdsPlans", "_rdsDeadlocks", "_rdsLogEvents", "_rdsCpu" })
        {
            Assert.DoesNotContain(field + " ??=", runner, StringComparison.Ordinal);
            Assert.Matches(@"LazyInitializer\.EnsureInitialized\(\s*ref " + field + ",", runner);
        }
    }
}
