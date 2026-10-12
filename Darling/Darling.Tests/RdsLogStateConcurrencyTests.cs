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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.RDS;
using Amazon.RDS.Model;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
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

    /// <summary>Entries each writer adds in the read tests: enough that the book's table is rebuilt several times, up to a
    /// size where one rebuild takes long enough for a reader to be inside it.</summary>
    private const int GrowthKeysPerWriter = 20_000;

    /// <summary>Fresh books each read test grows from empty, so every one of them goes through the rebuilds again.</summary>
    private const int GrowthRepeats = 5;

    /// <summary>
    /// One run of the read tests against one book, through the two operations the readers and writers use:
    /// <paramref name="store"/> holds a value for a target by its resume key, and <paramref name="read"/> returns what the
    /// book holds for a resume key ("" for nothing). Each of <see cref="Workers"/> readers has an entry stored before the
    /// run starts and reads it in a loop until every writer is done. The same number of writers add thousands of entries
    /// for other targets, which grows the table and rebuilds it again and again. A reader's entry is never touched, so
    /// every read must find it holding what was stored.
    /// </summary>
    private static async Task ReadsKeepFindingTheirEntryWhileOthersAddTheirsAsync(
        Action<string, string> store, Func<string, string> read)
    {
        for (var reader = 0; reader < Workers; reader++)
        {
            store("reader-" + reader + "|" + FileFor(1), "held-" + reader);
        }

        var writersLeft = Workers;
        var failed = 0;

        await RunTogetherAsync(Workers * 2, worker =>
        {
            try
            {
                if (worker < Workers)
                {
                    var resumeKey = "reader-" + worker + "|" + FileFor(1);
                    var expected = "held-" + worker;

                    while (Volatile.Read(ref writersLeft) > 0 && Volatile.Read(ref failed) == 0)
                    {
                        var held = read(resumeKey);

                        if (!string.Equals(held, expected, StringComparison.Ordinal))
                        {
                            Assert.Equal(expected, held);
                        }
                    }
                }
                else
                {
                    for (var i = 0; i < GrowthKeysPerWriter && Volatile.Read(ref failed) == 0; i++)
                    {
                        store("writer-" + worker + "-" + i + "|" + FileFor(1), "added");
                    }
                }
            }
            catch
            {
                Volatile.Write(ref failed, 1);
                throw;
            }
            finally
            {
                if (worker >= Workers)
                {
                    Interlocked.Decrement(ref writersLeft);
                }
            }
        });
    }

    /// <summary>
    /// A read of the csvlog carry book takes the book's lock like a write does. A read that does not can walk the table
    /// while another thread is rebuilding it larger and answer "nothing held" for an entry that has been there all along
    /// (or throw), and a carry that is missed is a partial record whose tail is never glued back. The other tests of the
    /// book read only the entry their own thread writes, in a table that never grows, which a missing lock does not show.
    /// </summary>
    [Fact]
    public async Task TargetsReadingCsvlogCarriesWhileOthersAddTheirsStillFindTheirOwn()
    {
        for (var repeat = 0; repeat < GrowthRepeats; repeat++)
        {
            var book = new RdsCsvlogCarryBook();

            await ReadsKeepFindingTheirEntryWhileOthersAddTheirsAsync(
                (resumeKey, held) =>
                {
                    var (_, key, dropped, file) = book.CarryFor(resumeKey, startsAtFileStart: false);

                    book.Commit(key, file, new RdsCsvlogCarry.CsvCarry(held, StartKnown: true), dropped);
                },
                resumeKey => book.CarryFor(resumeKey, startsAtFileStart: false).Carry.Partial);
        }
    }

    /// <summary>The same as the csvlog book, for the deadlock report held back until the rest of it arrives.</summary>
    [Fact]
    public async Task TargetsReadingHeldDeadlockReportsWhileOthersAddTheirsStillFindTheirOwn()
    {
        for (var repeat = 0; repeat < GrowthRepeats; repeat++)
        {
            var book = new RdsDeadlockCarryBook();

            await ReadsKeepFindingTheirEntryWhileOthersAddTheirsAsync(
                (resumeKey, held) =>
                {
                    var (_, key, file, _) = book.CarryFor(resumeKey);

                    book.Commit(key, file, held);
                },
                resumeKey => book.CarryFor(resumeKey).Held);
        }
    }

    /// <summary>Threads that ask one runner for its ingestors at once.</summary>
    private const int PublishThreads = 8;

    /// <summary>
    /// The runner is one object for every target, and it builds each RDS ingestor the first time a target needs it. Two
    /// targets' first runs get there together: a plain <c>??=</c> lets each build an ingestor and keep its own, so one of
    /// the two (and the positions and carries it holds) is dropped while its run is still going. Eight threads ask a fresh
    /// runner for every ingestor at the same moment, over and over for a second, and all eight must be handed the same
    /// object of each kind every time.
    /// </summary>
    [Fact]
    public async Task TheRunnerHandsEveryTargetTheSameIngestorForEachRdsCollector()
    {
        await using var store = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused;Username=unused");
        var logHashKey = new PgLogHashKey(Enumerable.Repeat((byte)7, PgLogHashKey.KeyLength).ToArray());
        var clock = Stopwatch.StartNew();
        var rounds = 0;

        while (clock.Elapsed < TimeSpan.FromSeconds(1))
        {
            var runner = new DarlingCollectorRunner(store, new CollectorDeltaCalculator());
            var plans = new object[PublishThreads];
            var deadlocks = new object[PublishThreads];
            var logEvents = new object[PublishThreads];
            var cpus = new object[PublishThreads];

            await RunTogetherAsync(PublishThreads, worker =>
            {
                plans[worker] = runner.PublishedRdsPlanIngestor();
                deadlocks[worker] = runner.PublishedRdsDeadlockIngestor();
                logEvents[worker] = runner.PublishedRdsLogEventIngestor(logHashKey);
                cpus[worker] = runner.PublishedRdsCpuIngestor();
            });

            AssertOneInstance("plan", plans);
            AssertOneInstance("deadlock", deadlocks);
            AssertOneInstance("log event", logEvents);
            AssertOneInstance("cpu", cpus);

            rounds++;
        }

        Assert.True(rounds > 0);
    }

    private static void AssertOneInstance(string collector, object[] handed)
        => Assert.True(
            handed.All(item => ReferenceEquals(item, handed[0])),
            "The " + collector + " ingestor: the threads of one runner were handed "
            + handed.Distinct(ReferenceEqualityComparer.Instance).Count() + " different objects.");
}
