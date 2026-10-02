/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon.RDS;
using Amazon.RDS.Model;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A fake RDS log API for the rotation and restart pins (#4708): files are listed oldest first (the last one is the
/// newest), every download answers an empty body with a fresh marker (<c>M1</c>, <c>M2</c>, ...), and a file named in
/// <see cref="Pending"/> always reports that more data is waiting. Empty bodies keep the ingestors off the store, so a
/// closed-port data source is enough for the pins that do not test the store write.
/// </summary>
internal sealed class RdsRotationFakeRds : AmazonRDSClient
{
    private int _markers;

    public RdsRotationFakeRds(params string[] listed)
        : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"), Amazon.RegionEndpoint.USEast1)
    {
        Listed = listed;
    }

    public string[] Listed { get; set; }
    public HashSet<string> Pending { get; } = new();
    public List<DownloadDBLogFilePortionRequest> Downloads { get; } = new();
    public string Body { get; set; } = string.Empty;

    public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
        DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new DescribeDBLogFilesResponse
        {
            DescribeDBLogFiles = Listed
                .Select((name, i) => new DescribeDBLogFilesDetails { LogFileName = name, LastWritten = 1000L * (i + 1) })
                .ToList(),
        });

    public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
        DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
    {
        Downloads.Add(request);
        return Task.FromResult(new DownloadDBLogFilePortionResponse
        {
            LogFileData = Body,
            Marker = "M" + (++_markers),
            AdditionalDataPending = Pending.Contains(request.LogFileName),
        });
    }
}

/// <summary>An in-memory stand-in for <c>collect.collector_state</c>, shaped as the runner's two state helpers.</summary>
internal sealed class RdsFakeCollectorState
{
    public Dictionary<(int Server, string Collector), Dictionary<string, string>> Rows { get; } = new();
    public int Loads { get; private set; }
    public int Saves { get; private set; }

    public Task<Dictionary<string, string>> LoadAsync(int serverId, string collector, CancellationToken cancellationToken)
    {
        Loads++;
        return Task.FromResult(Rows.TryGetValue((serverId, collector), out var row)
            ? new Dictionary<string, string>(row)
            : new Dictionary<string, string>());
    }

    public Task SaveAsync(
        int serverId, string collector, IReadOnlyDictionary<string, string> state, CancellationToken cancellationToken)
    {
        Saves++;

        if (!Rows.TryGetValue((serverId, collector), out var row))
        {
            Rows[(serverId, collector)] = row = new Dictionary<string, string>();
        }

        foreach (var entry in state)
        {
            row[entry.Key] = entry.Value;
        }

        return Task.CompletedTask;
    }

    public RdsResumeStore Store(string collector) => new(collector, LoadAsync, SaveAsync);

    public string? Value(string collector, string key)
        => Rows.TryGetValue((1, collector), out var row) && row.TryGetValue(key, out var value) ? value : null;
}

/// <summary>
/// RDS log positions across a restart, a rotation inside one cycle, and the measurements a rotation leaves (#4708).
/// Each pin runs against all three ingestors, because each keeps its own copy of the read, store and commit sequence.
/// </summary>
public sealed class RdsResumePersistenceTests
{
    private const string Host = "solo.abc123.us-east-1.rds.amazonaws.com";

    private const string DeadStore =
        "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private const string Oldest = "error/postgresql.log.2026-08-25-16";
    private const string Older = "error/postgresql.log.2026-08-25-17";
    private const string Newest = "error/postgresql.log.2026-08-25-18";

    private static readonly Dictionary<string, string> CollectorOf = new()
    {
        ["plans"] = "pg_plan_capture",
        ["deadlocks"] = "pg_deadlocks",
        ["events"] = "pg_log_events",
    };

    public static TheoryData<string> Ingestors => new() { "plans", "deadlocks", "events" };

    /// <summary>One ingestor, kept for the life of the returned call so its source and its restored-once state persist
    /// between cycles the way the runner's do. A new call to this method is a new process.</summary>
    private static Func<Task<RdsIngestOutcome>> Ingestor(
        string which, RdsRotationFakeRds client, RdsFakeCollectorState state, bool csv = false)
    {
        var store = NpgsqlDataSource.Create(DeadStore);
        var logs = new RdsLogSource(_ => client);
        var resume = state.Store(CollectorOf[which]);

        switch (which)
        {
            case "plans":
            {
                var ingestor = new RdsPlanIngestor(store, logs, resume: resume);
                return () => ingestor.IngestAsync(1, "s", Host, csv, CancellationToken.None);
            }

            case "deadlocks":
            {
                var ingestor = new RdsDeadlockIngestor(store, logs, resume: resume);
                return () => ingestor.IngestAsync(1, "s", Host, false, csv, CancellationToken.None);
            }

            default:
            {
                var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, logs, resume: resume);
                return () => ingestor.IngestAsync(1, "s", Host, false, csv, CancellationToken.None);
            }
        }
    }

    /// <summary>A restart resumes from the file and marker the last process saved, on the file it was reading, instead of
    /// the newest file's last lines. Fails on an ingestor that keeps its position in memory only.</summary>
    [Theory]
    [MemberData(nameof(Ingestors))]
    public async Task ARestartResumesFromThePositionTheLastProcessSaved(string which)
    {
        var client = new RdsRotationFakeRds(Newest);
        var state = new RdsFakeCollectorState();

        await Ingestor(which, client, state)();

        Assert.Equal("rds|1|solo|M1|" + Newest, state.Value(CollectorOf[which], "log_resume"));

        client.Downloads.Clear();
        await Ingestor(which, client, state)();

        Assert.Equal(Newest, client.Downloads[0].LogFileName);
        Assert.Equal("M1", client.Downloads[0].Marker);
        Assert.Equal(0, client.Downloads[0].NumberOfLines);
    }

    /// <summary>The csv file's position is kept under its own key, and the stderr key is left alone.</summary>
    [Fact]
    public async Task ACsvPositionIsSavedUnderItsOwnKey()
    {
        var client = new RdsRotationFakeRds(Newest, Newest + ".csv");
        var state = new RdsFakeCollectorState();

        await Ingestor("plans", client, state, csv: true)();

        Assert.Equal("rds|1|solo|M1|" + Newest + ".csv", state.Value("pg_plan_capture", "log_resume_csv"));
        Assert.Null(state.Value("pg_plan_capture", "log_resume"));
    }

    /// <summary>A saved file RDS no longer lists cannot be resumed: the newest file's last lines are read, the outcome
    /// says the file was missing, and the saved position is replaced.</summary>
    [Fact]
    public async Task ASavedFileThatRdsNoLongerListsIsReportedMissingAndTheTailIsRead()
    {
        var state = new RdsFakeCollectorState();
        state.Rows[(1, "pg_plan_capture")] = new() { ["log_resume"] = "rds|1|solo|OLD|error/postgresql.log.2026-01-01-00" };
        var client = new RdsRotationFakeRds(Newest);

        var outcome = await Ingestor("plans", client, state)();

        Assert.True(outcome.ResumeFileMissing);
        Assert.Null(client.Downloads[0].Marker);
        Assert.Equal(RdsLogSource.FirstReadLines, client.Downloads[0].NumberOfLines);
        Assert.Equal("rds|1|solo|M1|" + Newest, state.Value("pg_plan_capture", "log_resume"));
    }

    /// <summary>A saved value that is not an RDS position of this version - the self-hosted tail's
    /// <c>offset|file</c>, another version, noise - is ignored: a first read of the newest file, and nothing reported
    /// missing.</summary>
    [Theory]
    [InlineData("52428800|/var/lib/postgresql/log/postgresql-2026-08-25_180000.log")]
    [InlineData("rds|2|solo|OLD|error/postgresql.log.2026-08-25-18")]
    [InlineData("rds|1|solo|OLD")]
    [InlineData("garbage")]
    public async Task AValueThatIsNotAnRdsPositionIsIgnored(string saved)
    {
        var state = new RdsFakeCollectorState();
        state.Rows[(1, "pg_plan_capture")] = new() { ["log_resume"] = saved };
        var client = new RdsRotationFakeRds(Newest);

        var outcome = await Ingestor("plans", client, state)();

        Assert.False(outcome.ResumeFileMissing);
        Assert.Null(client.Downloads[0].Marker);
        Assert.Equal(RdsLogSource.FirstReadLines, client.Downloads[0].NumberOfLines);
    }

    /// <summary>The other direction: the self-hosted tail reads no position out of an RDS value.</summary>
    [Fact]
    public void TheSelfHostedTailIgnoresAnRdsValue()
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "s",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            State = new Dictionary<string, string> { ["log_resume"] = "rds|1|solo|M1|" + Newest },
        };

        var query = PgServerLogTail.WithResume("SELECT 1", context);

        Assert.All(query.Parameters, p => Assert.Null(p.Value));
    }

    [Fact]
    public void AnRdsPositionRoundTripsAndOnlyOneOfThisVersionIsRead()
    {
        var text = RdsResumeState.Format("solo", "M1", "error/postgresql.log.2026-08-25-18|odd");

        Assert.Equal("rds|1|solo|M1|error/postgresql.log.2026-08-25-18|odd", text);
        Assert.True(RdsResumeState.TryParse(text, out var instance, out var marker, out var file));
        Assert.Equal(("solo", "M1", "error/postgresql.log.2026-08-25-18|odd"), (instance, marker, file));

        Assert.Null(RdsResumeState.Format("so|lo", "M1", "f"));
        Assert.Null(RdsResumeState.Format("solo", "M|1", "f"));
        Assert.False(RdsResumeState.TryParse("12345|postgresql.log", out _, out _, out _));
        Assert.False(RdsResumeState.TryParse("rds|2|solo|M1|f", out _, out _, out _));
        Assert.False(RdsResumeState.TryParse("rds|1|solo|M1", out _, out _, out _));
    }

    /// <summary>The position is saved after the chunk's rows are stored. A store write that fails saves nothing, so the
    /// next start re-reads the window.</summary>
    [Fact]
    public async Task APositionIsNotSavedWhenTheStoreWriteFails()
    {
        var plan = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "auto_explain_real_block.txt"));
        var client = new RdsRotationFakeRds(Newest) { Body = plan };
        var state = new RdsFakeCollectorState();

        await Assert.ThrowsAnyAsync<Exception>(() => Ingestor("plans", client, state)());

        Assert.Equal(0, state.Saves);
    }

    [Fact]
    public async Task TheSavedPositionsAreLoadedOncePerServer()
    {
        var client = new RdsRotationFakeRds(Newest);
        var state = new RdsFakeCollectorState();
        var ingest = Ingestor("plans", client, state);

        await ingest();
        await ingest();

        Assert.Equal(1, state.Loads);
    }

    /// <summary>A rotation between two cycles is finished and the newest file opened from its first line in ONE call, not
    /// the old file on one cycle and the new one on the next.</summary>
    [Theory]
    [MemberData(nameof(Ingestors))]
    public async Task ARotatedFileIsFinishedAndTheNewestReadInOneCycle(string which)
    {
        var client = new RdsRotationFakeRds(Older);
        var ingest = Ingestor(which, client, new RdsFakeCollectorState());
        await ingest();

        client.Listed = new[] { Older, Newest };
        client.Downloads.Clear();

        await ingest();

        Assert.Equal(2, client.Downloads.Count);
        Assert.Equal(Older, client.Downloads[0].LogFileName);
        Assert.Equal("M1", client.Downloads[0].Marker);
        Assert.Equal(Newest, client.Downloads[1].LogFileName);
        Assert.Equal("0", client.Downloads[1].Marker);
    }

    /// <summary>A rotated file with more data than a call returns keeps the cycle reading it, but only up to the pass
    /// bound; the position stays on that file for the next cycle.</summary>
    [Theory]
    [MemberData(nameof(Ingestors))]
    public async Task AnOldFileWithMoreToReadIsReadAtMostFourTimesInACycle(string which)
    {
        var client = new RdsRotationFakeRds(Older);
        var ingest = Ingestor(which, client, new RdsFakeCollectorState());
        await ingest();

        client.Listed = new[] { Older, Newest };
        client.Pending.Add(Older);
        client.Downloads.Clear();

        await ingest();

        Assert.Equal(4, RdsLogSource.MaxPassesPerCycle);
        Assert.Equal(RdsLogSource.MaxPassesPerCycle, client.Downloads.Count);
        Assert.All(client.Downloads, d => Assert.Equal(Older, d.LogFileName));
    }

    /// <summary>A file that rotated away between two reads is counted on the outcome, not silently skipped.</summary>
    [Theory]
    [MemberData(nameof(Ingestors))]
    public async Task TheOutcomeCarriesTheFilesARotationSkipped(string which)
    {
        var client = new RdsRotationFakeRds(Oldest);
        var ingest = Ingestor(which, client, new RdsFakeCollectorState());
        await ingest();

        client.Listed = new[] { Oldest, Older, Newest };

        var outcome = await ingest();

        Assert.Equal(1, outcome.FilesSkipped);
        Assert.False(outcome.ResumeFileMissing);
    }

    [Fact]
    public void TheManagedRouteRecordsTheSelfHostedResumeLabelsOnlyWhenTheyHappened()
    {
        var quiet = NewContext();
        DarlingCollectorRunner.MeasureRdsLogResume(quiet, 0, false);
        Assert.Empty(quiet.Measurements);

        var busy = NewContext();
        DarlingCollectorRunner.MeasureRdsLogResume(busy, 2, true);
        Assert.Contains(new CollectorMeasurement("log_files_skipped_by_rotation", 2), busy.Measurements);
        Assert.Contains(new CollectorMeasurement("log_resume_file_missing", 1), busy.Measurements);
    }

    [Fact]
    public void TheManagedRouteGetsItsOwnResumeSentences()
    {
        var result = new CollectorRunResult(1, 0, 0, new[]
        {
            new CollectorMeasurement(PgServerLogTail.FilesSkippedByRotationMeasurement, 2),
            new CollectorMeasurement(PgServerLogTail.ResumeFileMissingMeasurement, 1),
        });

        var rds = DarlingCollectorRunner.WithLogResumeNotes(result, managedRoute: true).HostNote;

        Assert.Contains(DarlingCollectorRunner.RdsLogResumeLostNote, rds, StringComparison.Ordinal);
        Assert.Contains(DarlingCollectorRunner.RdsLogFilesSkippedNote, rds, StringComparison.Ordinal);
        Assert.DoesNotContain("log_directory", rds, StringComparison.Ordinal);
        Assert.DoesNotContain("4 MB", rds, StringComparison.Ordinal);
        Assert.Contains(RdsLogSource.FirstReadLines.ToString("N0", CultureInfo.InvariantCulture),
            DarlingCollectorRunner.RdsLogResumeLostNote, StringComparison.Ordinal);

        var selfHosted = DarlingCollectorRunner.WithLogResumeNotes(result).HostNote;

        Assert.Contains(PgServerLogTail.LogResumeLostNote, selfHosted, StringComparison.Ordinal);
    }

    /// <summary>Each RDS ingest passes the outcome's two new fields to the measurements, and the worker asks for the RDS
    /// wording on an Aurora or RDS target.</summary>
    [Fact]
    public void TheRunnerAndWorkerCarryTheOutcomeToTheRow()
    {
        var service = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service");
        var runner = File.ReadAllText(Path.Combine(service, "DarlingCollectorRunner.cs"));
        var worker = File.ReadAllText(Path.Combine(service, "DarlingWorker.cs"));

        Assert.Equal(3, Regex.Matches(runner,
            @"filesSkipped: outcome\.FilesSkipped, resumeFileMissing: outcome\.ResumeFileMissing").Count);
        Assert.Contains(
            "WithLogResumeNotes(result, runtime.Target.IsAurora || runtime.Target.IsAwsRds)", worker, StringComparison.Ordinal);
    }

    private static CollectorContext NewContext() => new()
    {
        ServerId = 1,
        ServerName = "s",
        CollectionTime = DateTime.UtcNow,
        Deltas = new CollectorDeltaCalculator(),
    };
}

/// <summary>
/// The store half of #4708 against a real store: the runner's own state helpers write and read the RDS position, so a
/// new process resumes from what the last one saved. The RDS API stays faked.
/// </summary>
[Collection("live-postgres")]
public sealed class RdsResumeStoreLiveTests
{
    private const int TestServerId = -470801;
    private const string Host = "solo.abc123.us-east-1.rds.amazonaws.com";
    private const string Newest = "error/postgresql.log.2026-08-25-18";

    [Fact]
    public async Task APositionSavedThroughTheRunnerSurvivesANewProcess()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live RDS position test.");

        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
            await DeleteTestRowsAsync(connection, TestContext.Current.CancellationToken);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var bodySucceeded = false;

        try
        {
            var client = new RdsRotationFakeRds(Newest);
            var first = new RdsPlanIngestor(
                postgres, new RdsLogSource(_ => client), resume: runner.RdsResumeStoreFor("pg_plan_capture"));

            await first.IngestAsync(TestServerId, "s", Host, false, TestContext.Current.CancellationToken);

            using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync(TestContext.Current.CancellationToken);
                using var check = new NpgsqlCommand(
                    "SELECT collector_name, state_key, state_value FROM collect.collector_state WHERE server_id = $1", connection);
                check.Parameters.AddWithValue(TestServerId);
                using var reader = await check.ExecuteReaderAsync(TestContext.Current.CancellationToken);

                Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
                Assert.Equal("pg_plan_capture", reader.GetString(0));
                Assert.Equal("log_resume", reader.GetString(1));
                Assert.Equal("rds|1|solo|M1|" + Newest, reader.GetString(2));
                Assert.False(await reader.ReadAsync(TestContext.Current.CancellationToken));
            }

            client.Downloads.Clear();
            var restarted = new RdsPlanIngestor(
                postgres, new RdsLogSource(_ => client), resume: runner.RdsResumeStoreFor("pg_plan_capture"));

            await restarted.IngestAsync(TestServerId, "s", Host, false, TestContext.Current.CancellationToken);

            Assert.Equal(Newest, client.Downloads[0].LogFileName);
            Assert.Equal("M1", client.Downloads[0].Marker);
            Assert.Equal(0, client.Downloads[0].NumberOfLines);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(
                connectionString!,
                bodySucceeded,
                (connection, cancellationToken) => DeleteTestRowsAsync(connection, cancellationToken));
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        using var delete = new NpgsqlCommand("DELETE FROM collect.collector_state WHERE server_id = $1", connection);
        delete.Parameters.AddWithValue(TestServerId);
        await delete.ExecuteNonQueryAsync(cancellationToken);
    }
}
