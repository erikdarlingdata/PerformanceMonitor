/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public sealed partial class DarlingCollectorRunner
{
    private readonly PgLogHashKey? _logHashKey;

    private readonly Func<string?> _installId;

    /// <summary>
    /// This install's id (#4961), or null when the runner was built without one. The long-query session's name is made
    /// from it: the lifecycle names the session it creates and drops from it, and the read names the session it reads.
    /// </summary>
    internal string? InstallId => _installId();

    /// <summary>
    /// The name of this install's long-query completions session, or null when there is no install id to make it from
    /// (#4961). Null means no session exists to create, drop or read, and the caller says why as a fault.
    /// </summary>
    internal string? LongQuerySessionName() =>
        LongQueryCompletionsCollector.TryXeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallId);

    /// <summary>The store's log-hash key this runner was given (#4004), null when the service could not load one. The
    /// hourly deadlock re-mask reads it here (#4012's review), the one instance every log-hashing run shares.</summary>
    internal PgLogHashKey? LogHashKey => _logHashKey;

    /* One ingestor for the process, so the resume marker survives between cycles - it is per-file and held in
       memory (#2538), and a fresh instance every cycle would silently re-read the same tail forever while looking
       like it was making progress. Each ingestor also saves its position to collect.collector_state after the rows
       it covers are stored, and restores it before a server's first read (#4708, RdsResumeStore), so a restart
       resumes instead of re-reading only the newest file's last lines. */
    private RdsPlanIngestor? _rdsPlans;

    /* A SEPARATE ingestor from _rdsPlans, deliberately: RdsLogSource's resume marker is consumed per
       (instance, file) on every read, so sharing one instance between plan capture and deadlock ingestion
       would starve whichever of the two runs second in a cycle. Each needs its own marker to independently
       read "the same bounded tail of the same file" - matching the pg_read_file route, where the two
       collectors' SQL queries already read that tail independently rather than sharing a cursor. */
    private RdsDeadlockIngestor? _rdsDeadlocks;
    private RdsLogEventIngestor? _rdsLogEvents;

    /* Own ingestor for the same "one instance per transport" reason as the two above — see
       RdsCpuIngestor's own doc comment for why it does NOT need the marker-survival treatment
       _rdsPlans/_rdsDeadlocks get: its resume watermark lives in the store, not in this field. */
    private RdsCpuIngestor? _rdsCpu;

    /* One verifier for the four RDS readers: a server's endpoint verdict and its fresh login are made once per lifetime,
       not once per collector, and ForgetRdsVerdicts clears both when the server's definition changes. */
    private readonly RdsEndpointVerifier _rdsVerifier = new();

    /* #5452: the assumed-role credentials of every server that names its own AWS role, one session per (role, external ID)
       however many servers share it. Built beside the verifier and handed to the four RDS readers; the allow list it checks
       is AwsRoleAllowlist.Current, which the worker sets at start. A server with no role never touches it. */
    private readonly Targets.AwsRoleCredentialCache _awsRoles;

    /// <summary>Drops the assumed-role sessions that no enabled server names any more (#5452). Called after each reload of the server list.</summary>
    internal void RetainAwsRoles(IEnumerable<Targets.AwsRoleKey> liveKeys) => _awsRoles.Retain(liveKeys);

    /// <summary>The role-credential cache the RDS readers use, for the tests (#5452).</summary>
    internal Targets.AwsRoleCredentialCache AwsRoles => _awsRoles;

    /// <summary>Forgets the endpoint verdicts and the fresh login held for <paramref name="serverId"/>: its definition changed.</summary>
    internal void ForgetRdsVerdicts(int serverId) => _rdsVerifier.ClearServer(serverId);

    /// <summary>
    /// #4708: where an RDS log ingestor keeps its positions across a restart - <c>collect.collector_state</c>, under
    /// the collector <paramref name="collectorName"/> and the keys the self-hosted tail already declares
    /// (<see cref="PgServerLogTail.ResumeStateKeys"/>), through this runner's own state helpers.
    /// </summary>
    internal RdsResumeStore RdsResumeStoreFor(string collectorName)
        => new(collectorName, GetCollectorStateAsync, SaveCollectorStateAsync);

    /* #5003: the four accessors below publish an RDS ingestor once, however many targets' first runs reach them together.
       A plain ??= lets each of those runs build an ingestor and keep its own, and the ingestor holds the positions and
       carries the other run was using. EnsureInitialized publishes with a compare-exchange, so a run that loses the race
       builds an ingestor it throws away and every caller gets the one that was published. */

    /// <summary>The one plan ingestor of this runner, built the first time a target needs it.</summary>
    internal RdsPlanIngestor PublishedRdsPlanIngestor()
        => LazyInitializer.EnsureInitialized(
            ref _rdsPlans,
            () => new RdsPlanIngestor(_postgres, logger: _logger, resume: RdsResumeStoreFor(PgPlanCaptureCollector.Instance.Name), verifier: _rdsVerifier, roles: _awsRoles));

    /// <summary>The one deadlock ingestor of this runner, built the first time a target needs it.</summary>
    internal RdsDeadlockIngestor PublishedRdsDeadlockIngestor()
        => LazyInitializer.EnsureInitialized(
            ref _rdsDeadlocks,
            () => new RdsDeadlockIngestor(_postgres, logger: _logger, resume: RdsResumeStoreFor(PgDeadlocksCollector.Instance.Name), verifier: _rdsVerifier, roles: _awsRoles));

    /// <summary>The one log-event ingestor of this runner, built the first time a target needs it with the store's
    /// log-hash key <paramref name="logHashKey"/>.</summary>
    internal RdsLogEventIngestor PublishedRdsLogEventIngestor(PgLogHashKey logHashKey)
        => LazyInitializer.EnsureInitialized(
            ref _rdsLogEvents,
            () => new RdsLogEventIngestor(_postgres, logHashKey, logger: _logger, resume: RdsResumeStoreFor(PgLogEventsCollector.Instance.Name), verifier: _rdsVerifier, roles: _awsRoles));

    /// <summary>The one CPU ingestor of this runner, built the first time a target needs it.</summary>
    internal RdsCpuIngestor PublishedRdsCpuIngestor()
        => LazyInitializer.EnsureInitialized(ref _rdsCpu, () => new RdsCpuIngestor(_postgres, logger: _logger, verifier: _rdsVerifier, roles: _awsRoles));

    /// <summary>
    /// <paramref name="result"/> with <see cref="PgServerLogTail.ForeignZoneLinesNote"/> merged into its host note
    /// when the run's definition recorded <see cref="PgServerLogTail.ForeignZoneLinesMeasurement"/> (#4046), so the
    /// row that carries the count also says what it counts and names the issue; otherwise <paramref name="result"/>
    /// unchanged. Host note, not the composed <see cref="CollectorRunResult.Note"/>, which would lose the count.
    /// </summary>
    internal static CollectorRunResult WithForeignZoneLinesNote(CollectorRunResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return result.Measurements.Any(m =>
                string.Equals(m.Label, PgServerLogTail.ForeignZoneLinesMeasurement, StringComparison.Ordinal) && m.Value > 0)
            ? result with { HostNote = EnumeratedCollectorDriver.MergeNotes(result.HostNote, PgServerLogTail.ForeignZoneLinesNote) }
            : result;
    }

    /// <summary>The sentence beside <see cref="PgServerLogTail.ResumeFileMissingMeasurement"/> on the RDS route (#4708):
    /// the self-hosted sentence names <c>log_directory</c> and a 4 MB read, neither of which exists there.</summary>
    internal const string RdsLogResumeLostNote =
        "The RDS log file this collector had read up to is no longer listed, so this read fell back to the newest "
        + "file's last 10,000 lines and whatever the old file received after the previous read was not collected (#4708)";

    /// <summary>The sentence beside <see cref="PgServerLogTail.FilesSkippedByRotationMeasurement"/> on the RDS route (#4708).</summary>
    internal const string RdsLogFilesSkippedNote =
        "More than one RDS log rotation happened between two reads: this read finished the file the previous read stopped "
        + "in and read the newest one, and log_files_skipped_by_rotation counts the files between them that no read "
        + "opened (#4708)";

    /// <summary>
    /// <paramref name="result"/> with the resume notes of the stderr log tail merged into its host note for each
    /// resume measurement the run recorded above zero (#4699); otherwise unchanged. <paramref name="managedRoute"/> is
    /// true for an Aurora or RDS target, whose log comes through the AWS API: the two measurements it records get their
    /// own sentences (#4708) instead of the self-hosted ones, which describe a log directory and a 4 MB window.
    /// </summary>
    internal static CollectorRunResult WithLogResumeNotes(CollectorRunResult result, bool managedRoute = false)
    {
        ArgumentNullException.ThrowIfNull(result);

        var hostNote = result.HostNote;

        foreach (var (label, note) in new[]
        {
            (PgServerLogTail.ResumeFileMissingMeasurement, managedRoute ? RdsLogResumeLostNote : PgServerLogTail.LogResumeLostNote),
            (PgServerLogTail.ResumeFileRecycledMeasurement, PgServerLogTail.LogResumeLostNote),
            (PgServerLogTail.FilesSkippedByRotationMeasurement, managedRoute ? RdsLogFilesSkippedNote : PgServerLogTail.LogFilesSkippedNote),
            (PgServerLogTail.BytesSkippedMeasurement, PgServerLogTail.LogBytesSkippedNote),
            (PgServerLogTail.MatchesLimitedMeasurement, PgServerLogTail.LogMatchesLimitedNote),
        })
        {
            if (result.Measurements.Any(m => string.Equals(m.Label, label, StringComparison.Ordinal) && m.Value > 0))
            {
                hostNote = EnumeratedCollectorDriver.MergeNotes(hostNote, note);
            }
        }

        return ReferenceEquals(hostNote, result.HostNote) ? result : result with { HostNote = hostNote };
    }

    /// <summary>
    /// <paramref name="result"/> with <see cref="PgPlanCaptureCollector.ForgedCaptureNote"/> merged into its host
    /// note when the run's definition recorded <see cref="PgPlanCaptureCollector.ForgedCaptureMeasurement"/>
    /// (#4058 L1), following the exact pattern <see cref="WithForeignZoneLinesNote"/> sets above; otherwise
    /// <paramref name="result"/> unchanged.
    /// </summary>
    internal static CollectorRunResult WithForgedCaptureNote(CollectorRunResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return result.Measurements.Any(m =>
                string.Equals(m.Label, PgPlanCaptureCollector.ForgedCaptureMeasurement, StringComparison.Ordinal) && m.Value > 0)
            ? result with { HostNote = EnumeratedCollectorDriver.MergeNotes(result.HostNote, PgPlanCaptureCollector.ForgedCaptureNote) }
            : result;
    }

    /// <summary>
    /// <paramref name="result"/> with <see cref="PgDeadlocksCollector.RaiseShapedDeadlocksSkippedNote"/> merged
    /// into its host note when the run's definition recorded
    /// <see cref="PgDeadlocksCollector.RaiseShapedDeadlocksSkippedMeasurement"/> (#4058 item 1), following the
    /// exact pattern <see cref="WithForgedCaptureNote"/> sets above; otherwise <paramref name="result"/> unchanged.
    /// </summary>
    internal static CollectorRunResult WithRaiseShapedDeadlocksSkippedNote(CollectorRunResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return result.Measurements.Any(m =>
                string.Equals(m.Label, PgDeadlocksCollector.RaiseShapedDeadlocksSkippedMeasurement, StringComparison.Ordinal) && m.Value > 0)
            ? result with { HostNote = EnumeratedCollectorDriver.MergeNotes(result.HostNote, PgDeadlocksCollector.RaiseShapedDeadlocksSkippedNote) }
            : result;
    }

    /// <summary>
    /// <paramref name="result"/> with <see cref="PgReadBinaryFileAdvisory.Sentence"/> merged into its host
    /// note when <paramref name="server"/> is still on the text route AND has not been noted inside
    /// <see cref="PgReadBinaryFileAdvisory.NoteInterval"/> (#4046); otherwise <paramref name="result"/>
    /// unchanged. The caller (<c>DarlingWorker.RunOneAsync</c>) gates this to <c>pg_log_events</c> only, so a
    /// target gets one note a day rather than whichever of the three log-tail collectors happens to run and
    /// win the gate. Reads <see cref="PgReadBinaryFileCapability"/>'s cache rather than probing again — the
    /// server-scoped path already resolved it for this exact target this cycle, for every collector that
    /// reads <see cref="PgServerLogTail"/>. A managed target was never entered into that cache at all (it reaches
    /// its log through the RDS API, never through <see cref="PgReadBinaryFileCapability.IsGrantedAsync"/>), so
    /// <c>TryGetCachedVerdict</c> answers false for it and this never fires there. It answers false for a
    /// database that is neither UTF8 nor SQL_ASCII too, where the grant would change nothing (#4051 review L4).
    /// It takes the <see cref="ServerRuntime"/> rather than a key, like the other two cache helpers, so all three
    /// key the cache the same way (#4051 round-2 review).
    /// </summary>
    internal static CollectorRunResult WithReadBinaryFileAdvisoryNote(CollectorRunResult result, ServerRuntime server)
    {
        ArgumentNullException.ThrowIfNull(result);
        ArgumentNullException.ThrowIfNull(server);

        var targetKey = ReadBinaryFileCacheKey(server);

        return PgReadBinaryFileCapability.TryGetCachedVerdict(targetKey, out var granted)
            && !granted
            && PgReadBinaryFileAdvisory.ShouldNote(targetKey)
            ? result with { HostNote = EnumeratedCollectorDriver.MergeNotes(result.HostNote, PgReadBinaryFileAdvisory.Sentence) }
            : result;
    }

    /// <summary>
    /// Plan capture for Aurora and RDS, where the log is only reachable through the AWS API (#2538).
    ///
    /// <para>Reported as a normal <see cref="CollectorRunResult"/> so the cycle accounts for it exactly like
    /// a collector: same <c>collection_log</c> row, same rows-collected number, same health surface. The
    /// TRANSPORT differs; the bookkeeping should not, or an operator would have to know which route a
    /// target used before they could read its collection history.</para>
    /// </summary>
    public async Task<CollectorRunResult> IngestRdsPlansAsync(
        ServerRuntime server, CancellationToken cancellationToken)
    {
        var rdsPlans = PublishedRdsPlanIngestor();

        var host = new NpgsqlConnectionStringBuilder(server.ConnectionString).Host ?? string.Empty;

        /* #4053 part c3: the same target-configuration read IngestRdsDeadlocksAsync and IngestRdsLogEventsAsync
           make, for the same reason — this ingestor reaches the log through the AWS API, not SQL, so it
           carries no target connection of its own to probe with. */
        var pgLogUsesCsvlog = await TryReadPgLogUsesCsvlogAsync(server, cancellationToken);

        var started = Stopwatch.GetTimestamp();

        var outcome = await rdsPlans.IngestAsync(
            server.ServerId, server.StorageName, host, pgLogUsesCsvlog, server.ConnectionString, server.Config.AwsRoleKey, cancellationToken);

        var elapsedMs = (long)Stopwatch.GetElapsedTime(started).TotalMilliseconds;

        var measurements = MeasurementsFor(server, foreignZoneLines: 0, outcome.CsvRecordsDiscarded,
            outcome.ForgedCaptures, filesSkipped: outcome.FilesSkipped, resumeFileMissing: outcome.ResumeFileMissing);

        /* Counted as STORAGE time rather than SQL time: no query ran against the monitored server, and
           filing an HTTPS round trip under sql_duration_ms would make one target's numbers mean something
           different from every other target's. */
        return new CollectorRunResult(outcome.Rows, 0, elapsedMs, measurements,
            RdsIngestNote(outcome, RdsPlanLogNotReachedNote, RdsPlanLogEmptyNote));
    }

    /// <summary>
    /// Deadlock capture for Aurora and RDS, where the log is only reachable through the AWS API — the
    /// dispatch this collector was missing (it dispatched unconditionally to the <c>pg_read_file</c> route
    /// even against a managed target, which has no filesystem to read; a 100% failure across the whole
    /// Aurora fleet, since no grant fixes what does not exist there). Mirrors
    /// <see cref="IngestRdsPlansAsync"/> exactly, including the STORAGE-time accounting rationale.
    /// </summary>
    public async Task<CollectorRunResult> IngestRdsDeadlocksAsync(
        ServerRuntime server, CancellationToken cancellationToken)
    {
        var rdsDeadlocks = PublishedRdsDeadlockIngestor();

        var host = new NpgsqlConnectionStringBuilder(server.ConnectionString).Host ?? string.Empty;

        /* #4046 part 1b: read on the target's own connection each cycle, since a sighup setting takes
           effect with no reconnect and #3604-style connect-time caching would miss that. */
        var (logTimezoneIsUtc, zoneCheck) = await TryReadLogTimezoneAsync(server, cancellationToken);

        /* #4053 part c2: the same target-configuration read IngestRdsLogEventsAsync makes, for the same
           reason — this ingestor reaches the log through the AWS API, not SQL, so it carries no target
           connection of its own to probe with. */
        var pgLogUsesCsvlog = await TryReadPgLogUsesCsvlogAsync(server, cancellationToken);

        var started = Stopwatch.GetTimestamp();

        var outcome = await rdsDeadlocks.IngestAsync(
            server.ServerId, server.StorageName, host, logTimezoneIsUtc, pgLogUsesCsvlog, server.ConnectionString, server.Config.AwsRoleKey, zoneCheck,
            cancellationToken);

        var elapsedMs = (long)Stopwatch.GetElapsedTime(started).TotalMilliseconds;

        var measurements = MeasurementsFor(
            server, outcome.ForeignZoneLines, outcome.CsvRecordsDiscarded, raiseShapedSkipped: outcome.RaiseShapedSkipped,
            filesSkipped: outcome.FilesSkipped, resumeFileMissing: outcome.ResumeFileMissing);

        return new CollectorRunResult(outcome.Rows, 0, elapsedMs, measurements,
            RdsIngestNote(outcome, RdsDeadlockLogNotReachedNote, RdsDeadlockLogEmptyNote));
    }

    /// <summary>
    /// #4046 part 1b: the managed route's own read of <c>log_timezone</c>, over a throwaway connection to
    /// the target — the RDS ingestors reach the log through the AWS API, not SQL, so they carry no target
    /// connection of their own. False (today's refusal on a foreign-zone line) when the setting cannot be
    /// read, matching the self-hosted route's NULL-column fallback in <see cref="PgServerLogTail.LogTimezoneIsUtc"/>.
    /// </summary>
    private async Task<(bool IsUtc, PgLogZoneCheck? ZoneCheck)> TryReadLogTimezoneAsync(
        ServerRuntime server, CancellationToken cancellationToken)
    {
        try
        {
            var provider = TargetProviders.For(server.Target);
            await using var connection = provider.CreateConnection(server.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            var setting = await ReadLogTimezoneSettingAsync(connection, cancellationToken);
            var isUtc = PgDeadlockLogParser.IsUtcLogTimezoneSetting(setting);

            /* A read that works again resets the throttle below, so the next failure warns at once. */
            _logTimezoneReadWarnedUtc.TryRemove(server.ServerId, out _);

            /* #6866: a setting that is not UTC may be a named local zone. The managed route has no connection of its
               own to ask on while it parses (the log text came from the AWS API), so the check opens one when it
               has stamps to prove, the same way the read above does. */
            return (isUtc, isUtc || string.IsNullOrWhiteSpace(setting)
                ? null
                : new PgLogZoneCheck(setting!, CreatePgLogZoneResolver(server)));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Warning, not Debug: a read that fails every cycle turns part 1b off for this target, and at Debug
               that went unseen while every call failed (#4051 review H1). At most once an hour per server,
               though, with Debug in between (#4051 round-2 review, L-2). */
            _logger?.Log(LogTimezoneReadFailureLevel(_logTimezoneReadWarnedUtc, server.ServerId, DateTime.UtcNow), ex,
                "Could not read log_timezone for '{Server}' (#4046 part 1b) — a foreign-zone "
                + "line this cycle is refused as before.", server.Config.DisplayName);
            return (false, null);
        }
    }

    /// <summary>How long a server's failed <c>log_timezone</c> reads stay at Debug after one logs at Warning.</summary>
    internal static readonly TimeSpan LogTimezoneReadWarningInterval = TimeSpan.FromHours(1);

    /// <summary>
    /// #4051 round-2 review, L-2: the level for a failed <c>log_timezone</c> read on <paramref name="serverId"/>.
    /// Warning for the first failure, and again once <see cref="LogTimezoneReadWarningInterval"/> has passed since
    /// the last Warning. Debug in between. A read that fails every cycle stays visible, without a stack trace on
    /// every cycle (576 a day per managed target). The Warning is recorded on the same call that returns it.
    /// </summary>
    internal static LogLevel LogTimezoneReadFailureLevel(
        ConcurrentDictionary<int, DateTime> warnedUtc, int serverId, DateTime nowUtc)
    {
        ArgumentNullException.ThrowIfNull(warnedUtc);

        /* #4732: a stamp ahead of the clock (it stepped back since the Warning) is replaced by this reading. */
        if (LastFiredStamp.TryGet(warnedUtc, serverId, nowUtc, out var last) && nowUtc - last < LogTimezoneReadWarningInterval)
        {
            return LogLevel.Debug;
        }

        warnedUtc[serverId] = nowUtc;
        return LogLevel.Warning;
    }

    /// <summary>
    /// #4046 part 1b: the managed route's <c>log_timezone</c> read as a whole statement.
    /// <see cref="PgServerLogTail.LogTimezoneSql"/> is an expression the self-hosted readers select beside the
    /// tail, so on its own it needs the SELECT. Without it every call failed with 42601 and part 1b never took
    /// effect on an RDS or Aurora target (#4051 review H1).
    /// </summary>
    internal const string LogTimezoneReadSql = "SELECT " + PgServerLogTail.LogTimezoneSql;

    /// <summary>
    /// Whether the target's <c>log_timezone</c> renders UTC, read on <paramref name="connection"/>, which must
    /// already be open. Its own method so a live test can run the exact statement the managed route sends.
    /// </summary>
    internal static async Task<bool> ReadLogTimezoneIsUtcAsync(DbConnection connection, CancellationToken cancellationToken) =>
        PgDeadlockLogParser.IsUtcLogTimezoneSetting(await ReadLogTimezoneSettingAsync(connection, cancellationToken));

    /// <summary>The target's <c>log_timezone</c> as written, read on <paramref name="connection"/> (open), or null.</summary>
    internal static async Task<string?> ReadLogTimezoneSettingAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        using var command = connection.CreateCommand();
        command.CommandTimeout = 10;
        command.CommandText = LogTimezoneReadSql;
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    /// <summary>
    /// How a log reader proves the time of lines stamped in a named local zone (#6866): a connection of its own to the
    /// target, because the log read holds the main one open while it parses, running <see cref="PgLogNamedZone"/>'s one
    /// batched statement with the command timeout the log tail read uses (<see cref="CommandTimeoutSeconds"/>). The
    /// pool makes the extra connection cheap, and the read only happens when a named zone has stamps to prove.
    /// </summary>
    internal static PgLogZoneResolver CreatePgLogZoneResolver(ServerRuntime server) =>
        async (setting, pairs, cancellationToken) =>
        {
            var provider = TargetProviders.For(server.Target);
            await using var connection = provider.CreateConnection(server.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            return await PgLogNamedZone.ResolveAsync(connection, setting, pairs, CommandTimeoutSeconds, cancellationToken);
        };

    /// <summary>#4046 part 1b: the managed route's foreign-zone count, on a throwaway context so the RDS ingest
    /// path can report it the same way <see cref="PgServerLogTail.MeasureForeignZoneLines"/> does for the
    /// self-hosted collectors.</summary>
    private IReadOnlyList<CollectorMeasurement> MeasurementsFor(
        ServerRuntime server, int foreignZoneLines, int csvRecordsDiscarded = 0, int forgedCaptures = 0,
        int raiseShapedSkipped = 0, int filesSkipped = 0, bool resumeFileMissing = false)
    {
        if (foreignZoneLines <= 0 && csvRecordsDiscarded <= 0 && forgedCaptures <= 0 && raiseShapedSkipped <= 0
            && filesSkipped <= 0 && !resumeFileMissing)
        {
            return CollectorContext.NoMeasurements;
        }

        var context = new CollectorContext
        {
            ServerId = server.ServerId,
            ServerName = server.StorageName,
            CollectionTime = DateTime.UtcNow,
            Deltas = _deltas,
        };
        PgServerLogTail.MeasureForeignZoneLines(context, foreignZoneLines);

        /* #4053 part c1: the csvlog route's own discard count, same measurement label the self-hosted
           PgLogEventsCollector.ReadAsync records for its own csvlog route (CsvRecordsDiscardedMeasurement),
           reported only when > 0, following that method's exact pattern. */
        if (csvRecordsDiscarded > 0)
        {
            context.Measure(PgLogEventsCollector.CsvRecordsDiscardedMeasurement, csvRecordsDiscarded);
        }

        /* #4053 part c3: the RDS plan ingestor's own forged-capture count, same measurement label the
           self-hosted PgPlanCaptureCollector.ReadCsvAsync/ReadAsync record for their own forged skips
           (ForgedCaptureMeasurement), reported only when > 0. DarlingWorker's WithForgedCaptureNote reads
           this label off any collector's measurements, so this alone is what attaches the sentence to the
           managed route's row. */
        if (forgedCaptures > 0)
        {
            context.Measure(PgPlanCaptureCollector.ForgedCaptureMeasurement, forgedCaptures);
        }

        /* #4058 item 1: the RDS deadlock ingestor's own RAISE-shaped skip count, same measurement label the
           self-hosted PgDeadlocksCollector.ReadCsvRow records for its own csvlog route
           (RaiseShapedDeadlocksSkippedMeasurement), reported only when > 0. */
        if (raiseShapedSkipped > 0)
        {
            context.Measure(PgDeadlocksCollector.RaiseShapedDeadlocksSkippedMeasurement, raiseShapedSkipped);
        }

        MeasureRdsLogResume(context, filesSkipped, resumeFileMissing);

        return context.Measurements;
    }

    /// <summary>
    /// #4708: the managed route's log-resume disclosures, under the labels the self-hosted tail records for the same
    /// two facts (<see cref="PgServerLogTail.FilesSkippedByRotationMeasurement"/> and
    /// <see cref="PgServerLogTail.ResumeFileMissingMeasurement"/>), each only when it happened.
    /// </summary>
    internal static void MeasureRdsLogResume(CollectorContext context, int filesSkipped, bool resumeFileMissing)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (filesSkipped > 0)
        {
            context.Measure(PgServerLogTail.FilesSkippedByRotationMeasurement, filesSkipped);
        }

        if (resumeFileMissing)
        {
            context.Measure(PgServerLogTail.ResumeFileMissingMeasurement, 1);
        }
    }

    /// <summary>
    /// Classified log events for Aurora and RDS Postgres, read from the RDS log API (#3601) — the managed
    /// half of the log-event pipeline, dispatched where <c>pg_log_events</c>' <c>pg_read_file</c> route
    /// cannot run. Same shape, same outcome vocabulary and same note pair as
    /// <see cref="IngestRdsDeadlocksAsync"/>; the classifier it feeds is the one the self-hosted collector
    /// feeds, so the two transports store identical rows for identical text.
    /// </summary>
    public async Task<CollectorRunResult> IngestRdsLogEventsAsync(
        ServerRuntime server, CancellationToken cancellationToken)
    {
        /* #4004: no key, no hashing - the same refusal the pg_read_file route's BuildQuery makes. */
        var logHashKey = _logHashKey ?? throw new InvalidOperationException(PgLogHashKey.UnavailableMessage);
        var rdsLogEvents = PublishedRdsLogEventIngestor(logHashKey);

        var host = new NpgsqlConnectionStringBuilder(server.ConnectionString).Host ?? string.Empty;

        /* #4046 part 1b: see IngestRdsDeadlocksAsync's TryReadLogTimezoneIsUtcAsync call for why this reads
           the target fresh each cycle rather than caching it at connect. */
        var (logTimezoneIsUtc, zoneCheck) = await TryReadLogTimezoneAsync(server, cancellationToken);

        /* #4053 part c1: the same target-configuration read the self-hosted pg_read_file route makes through
           ResolvePgReadBinaryFileGrantAsync, on the managed route's own throwaway connection for the same reason
           TryReadLogTimezoneIsUtcAsync above needs one — this ingestor reaches the log through the AWS API, not
           SQL, so it carries no target connection of its own to probe with. Cached the same hour by
           PgLogFormatCapability itself, keyed the same way as the binary-file grant (ReadBinaryFileCacheKey), so
           this is a cache hit on every cycle but the first and the one after a switch. */
        var pgLogUsesCsvlog = await TryReadPgLogUsesCsvlogAsync(server, cancellationToken);

        var started = Stopwatch.GetTimestamp();

        var outcome = await rdsLogEvents.IngestAsync(
            server.ServerId, server.StorageName, host, logTimezoneIsUtc, pgLogUsesCsvlog, server.ConnectionString, server.Config.AwsRoleKey, zoneCheck,
            cancellationToken);

        var elapsedMs = (long)Stopwatch.GetElapsedTime(started).TotalMilliseconds;

        var measurements = MeasurementsFor(server, outcome.ForeignZoneLines, outcome.CsvRecordsDiscarded,
            filesSkipped: outcome.FilesSkipped, resumeFileMissing: outcome.ResumeFileMissing);

        return new CollectorRunResult(outcome.Rows, 0, elapsedMs, measurements,
            RdsIngestNote(outcome, RdsLogEventsNotReachedNote, RdsLogEventsEmptyNote));
    }

    /// <summary>
    /// #4053 part c1: whether this target's <c>log_destination</c> includes <c>csvlog</c>, read the same way
    /// <see cref="TryReadLogTimezoneIsUtcAsync"/> reads <c>log_timezone</c> — over a throwaway connection, since
    /// the RDS ingestors carry none of their own.
    ///
    /// <para>#4053 review round 1 (item 5): on a probe failure, this returns the LAST GOOD verdict this
    /// runner saw for the server (<see cref="_lastPgLogUsesCsvlogVerdict"/>), not a hardcoded false. False
    /// used to be the fallback unconditionally, which is right for a target this runner has never read
    /// successfully — today's stderr route, unchanged — but wrong for one it already knows is on csvlog:
    /// flipping that verdict on a transient connection blip would make the ingestor read the stderr file
    /// from ITS OWN marker and re-store rows the csvlog route already committed under different text. The
    /// throttled Warning-then-Debug logging (#4051 round-2 review, L-2) is unchanged: a target whose probe
    /// fails every cycle still logs once loudly and then quietly, on its own <see cref="_pgLogUsesCsvlogReadWarnedUtc"/>
    /// throttle.</para>
    /// </summary>
    private async Task<bool> TryReadPgLogUsesCsvlogAsync(ServerRuntime server, CancellationToken cancellationToken)
    {
        try
        {
            var provider = TargetProviders.For(server.Target);
            await using var connection = provider.CreateConnection(server.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            var usesCsvlog = await PgLogFormatCapability.IsCsvlogEnabledAsync(
                connection, ReadBinaryFileCacheKey(server), cancellationToken);

            /* A read that works again resets the throttle below, so the next failure warns at once. */
            _pgLogUsesCsvlogReadWarnedUtc.TryRemove(server.ServerId, out _);

            /* #4053 review round 1 (item 5): recorded on every SUCCESSFUL probe, so the catch below always
               has this cycle's predecessor to fall back to — not just the last time the probe happened to
               fail. */
            _lastPgLogUsesCsvlogVerdict[server.ServerId] = usesCsvlog;
            return usesCsvlog;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.Log(LogTimezoneReadFailureLevel(_pgLogUsesCsvlogReadWarnedUtc, server.ServerId, DateTime.UtcNow), ex,
                "Could not read log_destination for '{Server}' (#4053 part c1) — every RDS csvlog-aware "
                + "route reads the {Fallback} file this cycle, its last known-good verdict.", server.Config.DisplayName,
                _lastPgLogUsesCsvlogVerdict.TryGetValue(server.ServerId, out var lastGood) && lastGood ? "csv" : "stderr");

            /* #4053 review round 1 (item 5): the last good verdict this runner saw for this server, not a
               hardcoded false. A connection or probe failure flipping a known-csvlog target to false would
               make the ingestor read the stderr file from ITS marker instead, re-storing rows the csvlog
               route already committed under different text. A target this runner has never successfully
               probed has no last-good verdict to fall back to, so false (today's stderr route, unchanged) is
               still the answer for first contact — the one case this dictionary cannot help with, because
               there is nothing yet to remember. */
            if (_lastPgLogUsesCsvlogVerdict.TryGetValue(server.ServerId, out var fallback))
            {
                return fallback;
            }
            return false;
        }
    }

    /// <summary>
    /// Instance CPU for Aurora and RDS Postgres, read from the AWS Performance Insights API (#2719) — the
    /// third "reach the target a different way" collector alongside <see cref="IngestRdsPlansAsync"/> and
    /// <see cref="IngestRdsDeadlocksAsync"/>, and unlike either of those, the ONLY route: PostgreSQL exposes
    /// no instance-level CPU signal at all, so there is no <c>pg_read_file</c>-style fallback for a
    /// self-hosted target to fall back to (see <see cref="PgCpuUtilizationCollector"/>'s doc comment).
    /// Reported as a normal <see cref="CollectorRunResult"/> for the same reason the other two are.
    /// </summary>
    public async Task<CollectorRunResult> IngestPgCpuAsync(
        ServerRuntime server, CancellationToken cancellationToken)
    {
        var rdsCpu = PublishedRdsCpuIngestor();

        var host = new NpgsqlConnectionStringBuilder(server.ConnectionString).Host ?? string.Empty;

        var started = Stopwatch.GetTimestamp();

        var outcome = await rdsCpu.IngestAsync(
            server.ServerId, server.StorageName, host, server.ConnectionString, server.Config.AwsRoleKey, cancellationToken);

        var elapsedMs = (long)Stopwatch.GetElapsedTime(started).TotalMilliseconds;

        /* Counted as STORAGE time rather than SQL time, matching the other two RDS-API ingestors: no query
           ran against the monitored server, and filing an HTTPS round trip under sql_duration_ms would make
           one target's numbers mean something different from every other target's. */
        return new CollectorRunResult(outcome.Rows, 0, elapsedMs, CollectorContext.NoMeasurements,
            RdsIngestNote(outcome, PiCpuNotReachedNote, PiCpuEmptyNote));
    }

    /* ── #3017: the notes an AWS-API ingest cycle leaves in collection_log ──────────────────────────────
       Six constants, in pairs, because the two members of each pair are the whole point: one says the
       source was read and held nothing, the other says the source was never asked. Before this there was
       one sentence per collector and it was the first of the two, stamped on both outcomes — a claim about
       the contents of a log nobody opened.

       Named constants rather than inline strings so RdsIngestNote can be pinned without a store or an AWS
       client, and so the pair for each collector can be asserted DIFFERENT — the failure mode of a fix like
       this is the two arms converging back onto one sentence during a later edit, which no compiler
       notices.

       No host is interpolated into any of them. collection_log is read by operators and shipped in support
       bundles, and the fact worth recording is the KIND of target, not its name. */

    /// <summary>The plan log was read and no auto_explain block in it was new.</summary>
    internal const string RdsPlanLogEmptyNote = "no new auto_explain plans in the RDS log window";

    /// <summary>The plan log was never opened: this host is not an RDS or Aurora endpoint.</summary>
    internal const string RdsPlanLogNotReachedNote =
        "this target's host is not an RDS or Aurora endpoint, so no RDS log was requested and no plan "
        + "capture was attempted - this cycle did not look";

    /// <summary>The deadlock log was read and held no new deadlock.</summary>
    internal const string RdsDeadlockLogEmptyNote = "no new deadlocks in the RDS log window";

    /// <summary>The deadlock log was never opened: this host is not an RDS or Aurora endpoint.</summary>
    internal const string RdsDeadlockLogNotReachedNote =
        "this target's host is not an RDS or Aurora endpoint, so no RDS log was requested and no deadlock "
        + "capture was attempted - this cycle did not look";

    /// <summary>The log was read and no line in it classified to a family (#3601).</summary>
    internal const string RdsLogEventsEmptyNote = "no new classifiable log events in the RDS log window";

    /// <summary>The log was never opened: this host is not an RDS or Aurora endpoint.</summary>
    internal const string RdsLogEventsNotReachedNote =
        "this target's host is not an RDS or Aurora endpoint, so no RDS log was requested and no log-event "
        + "capture was attempted - this cycle did not look";

    /// <summary>Performance Insights answered and had no new CPU sample.</summary>
    internal const string PiCpuEmptyNote = "no new Performance Insights CPU samples this cycle";

    /// <summary>Performance Insights was never queried: this host is not an RDS or Aurora endpoint.</summary>
    internal const string PiCpuNotReachedNote =
        "this target's host is not an RDS or Aurora endpoint, so Performance Insights was not queried and "
        + "no instance CPU was attempted - this cycle did not look";

    /// <summary>
    /// The <c>collection_log</c> note for one AWS-API ingest cycle (#3017) — the three-way rendering the
    /// bare row count could not express.
    ///
    /// <para><b>Not reached is asked FIRST and answers on its own.</b> A cycle that never made an AWS call
    /// knows nothing about the source, so <paramref name="nothingNewNote"/> — which is a claim about what
    /// the source held — must not be reachable from it. That ordering is the fix; the rest is unchanged
    /// behaviour.</para>
    ///
    /// <para><b>A productive cycle still leaves no note</b>, exactly as before: the rows themselves are the
    /// statement, and a note on every successful cycle would fill the column an operator scans for the
    /// unusual.</para>
    ///
    /// <para>Pure and <c>internal static</c> so the three-way decision can be pinned directly, with no
    /// store, no <c>ServerRuntime</c> and no AWS client — the shape #2633's own pins already use for this
    /// family.</para>
    /// </summary>
    internal static string? RdsIngestNote(RdsIngestOutcome outcome, string notReachedNote, string nothingNewNote) =>
        !outcome.SourceReached ? notReachedNote
        : outcome.Rows == 0 ? nothingNewNote
        : null;

    /// <summary>
    /// The per-database phase line for a database that FAULTED (#2896) — the case the #2855 split was added
    /// to attribute and the one case it could not print, because its only log site sat on the success path
    /// past the flush.
    ///
    /// <para><b>One method rather than two call sites</b> because both fault arms need it and the three
    /// things that are easy to get wrong here are all decided once inside it: the null test, the read-once
    /// rule, and the gate. A copy in each arm is how the budget arm and the generic arm would drift, and a
    /// drifted decomposition is worse than an absent one — it still looks precise.</para>
    ///
    /// <para><b>The gate is the flag, and that is sufficient here rather than merely conventional.</b>
    /// <c>PerDatabasePhasesMeasured</c> is set only from the connect's <c>finally</c>, which is downstream of
    /// both the per-iteration clear and the stopwatch, so a true flag implies the stamps are this database's
    /// own AND that the slice was started. A false flag means the fault landed before the connect — the
    /// watermark read, the adaptive shrink's store write, <c>BuildQuery</c> — where there is no split to
    /// print and no parent to print it against, and nothing is the honest answer.</para>
    ///
    /// <para><b>The stopwatch is read exactly once</b>, the #2472 rule this path already carries on its
    /// success side. It is still running in the handler, so a second read a few statements later returns a
    /// larger number: the gate and the line would then disagree about the parent, the residual would have
    /// been computed against the smaller one, and the printed terms would not sum to the printed total. A
    /// line that does not add up is the one outcome worse than no line.</para>
    ///
    /// <para><b>Same decomposition substring as the success line</b>, deliberately. These lines are parsed
    /// by tooling outside this repo (#2811/#2851), so a fault split an existing parser already matches beats
    /// a second shape it has to learn. Only the tail differs, and it has to: there are no rows and there was
    /// no flush, so borrowing the success tail would print <c>(0 rows, pg:0ms)</c> and make a failed database
    /// read as a quiet one.</para>
    ///
    /// <para><b>The level is the caller's</b>, not this method's. The budget arm is a Warning because a
    /// collector that could not finish is not routine; the generic arm is Debug because one database being
    /// offline or mid-restore is. A phase line louder than the error it decomposes would put a timing on the
    /// default log with no reason beside it.</para>
    ///
    /// <para>Nothing here is persisted and nothing here is read back — a per-database split is N:1 against
    /// <c>collection_log</c> and shaping that is the open decision in #2860. <c>internal</c> only so the pin
    /// can call the shipped method instead of asserting on its source text.</para>
    /// </summary>
    internal static void LogPerDatabaseFaultSplit(
        ILogger? logger,
        LogLevel level,
        CollectorContext context,
        Stopwatch? databaseSqlSlice,
        string displayName,
        string collectorName,
        string databaseName,
        string outcome)
    {
        if (databaseSqlSlice is null)
        {
            return;
        }

        var databaseSqlMs = databaseSqlSlice.ElapsedMilliseconds;
        if (context.PerDatabasePhasesFrom(databaseSqlMs) is not { } phases)
        {
            return;
        }

        logger?.Log(
            level,
            "  [{Server}] {Collector} [{Database}] {Outcome} after sql:{SqlMs}ms = connect:{ConnectMs}ms + open:{OpenMs}ms + drain:{DrainMs}ms + other:{OtherMs}ms (nothing stored)",
            displayName, collectorName, databaseName, outcome, databaseSqlMs,
            phases.ConnectMs, phases.OpenMs, phases.DrainMs, phases.OtherMs);
    }

    /// <summary>
    /// The three collectors that open <see cref="PgServerLogTail.TailCteSql"/> (#4046 part 1c) — the set
    /// eligible for the <see cref="PgReadBinaryFileCapability"/> check and the binary route, and the same
    /// set <c>ReadsServerLogWithPgReadFile</c> in <c>DarlingWorker</c> names for the fault message, kept as
    /// its own copy here rather than shared across the two classes' current visibility boundary.
    /// </summary>
    private static bool ReadsPgServerLogTail(string collectorName) =>
        collectorName is "pg_log_events" or "pg_deadlocks" or "pg_plan_capture";

    /// <summary>
    /// #4046 part 1c: sets <see cref="CollectorContext.PgReadBinaryFileGranted"/> for a PostgreSQL log-tail
    /// collector from <see cref="PgReadBinaryFileCapability"/> (its own hourly cache), and leaves every other
    /// collector on the default text route with no round trip. Its own method so the gate is testable; the
    /// server-scoped path that calls it needs a live target.
    /// </summary>
    internal static async ValueTask ResolvePgReadBinaryFileGrantAsync(
        CollectorContext context,
        string collectorName,
        DbConnection targetConnection,
        ServerRuntime server,
        CancellationToken cancellationToken)
    {
        if (server.Target.Engine == CollectorTargetEngine.PostgreSql && ReadsPgServerLogTail(collectorName))
        {
            /* #6866: a named local log_timezone is proved against the target by a connection of its own, since the
               tail read below holds this one open while the collector parses. */
            context.PgLogZoneResolver = CreatePgLogZoneResolver(server);

            var targetKey = ReadBinaryFileCacheKey(server);
            context.PgReadBinaryFileGranted = await PgReadBinaryFileCapability.IsGrantedAsync(
                targetConnection, targetKey, cancellationToken);

            /* #4062: the binary route decodes in the connected database's own server_encoding, never a
               default, so PgLogEncoding is only ever set alongside a true grant — see the property's own
               remarks on CollectorContext. */
            context.PgLogEncoding = context.PgReadBinaryFileGranted
                && PgReadBinaryFileCapability.TryGetCachedEncoding(targetKey, out var encoding)
                    ? encoding
                    : null;
        }

        /* #4053 parts a1b, b1 and b2: the collectors in RoutedCollectors read the csvlog tail once this is on (each
           collector's own change is in its collector file). Checked on the same
           connection, before BuildQuery decides which tail this collector opens with, the same shape as the
           grant check just above. RoutedCollectors (#4053 review L1 round 2) is the single source for this
           set, shared with the invalidation arms in DarlingWorker, so the probe and the drop can never
           disagree about which collectors this cache answers for. */
        if (server.Target.Engine == CollectorTargetEngine.PostgreSql
            && PgLogFormatCapability.RoutedCollectors.Contains(collectorName))
        {
            /* #4053 part a2: one probe answers both flags (PgLogFormatCapability's own cache), so asking
               for jsonlog right after csvlog on a cache miss still costs one round trip, not two. */
            context.PgLogUsesCsvlog = await PgLogFormatCapability.IsCsvlogEnabledAsync(
                targetConnection, ReadBinaryFileCacheKey(server), cancellationToken);
            context.PgLogUsesJsonlog = await PgLogFormatCapability.IsJsonlogEnabledAsync(
                targetConnection, ReadBinaryFileCacheKey(server), cancellationToken);
        }
    }

    /// <summary>
    /// #6143: every extension in the connected database and the schema it was created in, then each Aurora built-in
    /// function (<see cref="CollectorContext.AuroraFunctionNames"/>) found in pg_catalog. The one function the
    /// pg_stat_statements extension ships (<see cref="CollectorContext.StatementsExtensionAuroraFunction"/>) is also
    /// returned from the schema the extension was created in, but only when <c>pg_depend</c> shows it is a member of
    /// that extension and its argument types are the ones the collector calls it with: anyone with CREATE on that schema
    /// can make a function of the same name there. No other schema is read, and
    /// <see cref="CollectorContext.ChoosePgFunctionSchemas"/> checks the same rule again. Run on the monitoring
    /// connection, whose search_path is pinned to pg_catalog; every relation and operator is named by schema all the same.
    /// </summary>
    internal const string PgExtensionSchemasSql = @"SELECT 'extension'::text, e.extname::text, n.nspname::text, e.extversion::text
FROM pg_catalog.pg_extension AS e
JOIN pg_catalog.pg_namespace AS n ON n.oid OPERATOR(pg_catalog.=) e.extnamespace
UNION ALL
SELECT 'function'::text, p.proname::text, n.nspname::text, NULL::text
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS n ON n.oid OPERATOR(pg_catalog.=) p.pronamespace
WHERE p.proname::text OPERATOR(pg_catalog.=) ANY (@functions)
AND n.nspname OPERATOR(pg_catalog.=) 'pg_catalog'
UNION ALL
SELECT 'function'::text, p.proname::text, n.nspname::text, NULL::text
FROM pg_catalog.pg_extension AS x
JOIN pg_catalog.pg_depend AS d
  ON d.refclassid OPERATOR(pg_catalog.=) 'pg_catalog.pg_extension'::pg_catalog.regclass
 AND d.refobjid OPERATOR(pg_catalog.=) x.oid
 AND d.classid OPERATOR(pg_catalog.=) 'pg_catalog.pg_proc'::pg_catalog.regclass
 AND d.deptype OPERATOR(pg_catalog.=) 'e'
JOIN pg_catalog.pg_proc AS p
  ON p.oid OPERATOR(pg_catalog.=) d.objid
 AND p.pronamespace OPERATOR(pg_catalog.=) x.extnamespace
JOIN pg_catalog.pg_namespace AS n ON n.oid OPERATOR(pg_catalog.=) p.pronamespace
WHERE x.extname OPERATOR(pg_catalog.=) 'pg_stat_statements'
AND p.proname::text OPERATOR(pg_catalog.=) '" + CollectorContext.StatementsExtensionAuroraFunction + @"'
AND p.proname::text OPERATOR(pg_catalog.=) ANY (@functions)
AND pg_catalog.oidvectortypes(p.proargtypes) OPERATOR(pg_catalog.=) '" + CollectorContext.StatementsExtensionAuroraFunctionArguments + "'";

    /// <summary>#6143: a catalog read of a handful of rows; the other connection-time reads beside it use 5 s too.</summary>
    internal const int PgExtensionSchemasTimeoutSeconds = 5;

    /// <summary>
    /// #6143: sets <see cref="CollectorContext.PgExtensionSchemas"/> (and, on Aurora,
    /// <see cref="CollectorContext.PgFunctionSchemas"/>) from <paramref name="targetConnection"/>, for a PostgreSQL
    /// collector that declares an extension it reads (<see cref="ICollectorSchemaInfo.RequiredPgExtensions"/>) or calls
    /// Aurora built-ins (<see cref="ICollectorSchemaInfo.ReadsAuroraFunctions"/>), so its SQL names each object by the
    /// schema it is in. Clears both first, so a per-database loop never carries one database's answer into the next.
    /// Returns whether it read the catalog.
    /// <para>Not best-effort: when the read itself fails or times out, this throws, and the run (on the per-database
    /// path, that database) fails with a message that says so. Guessing a schema instead could send the collector's SQL
    /// to a same-named object in the guessed schema.</para>
    /// </summary>
    internal static async ValueTask<bool> ResolvePgExtensionSchemasAsync(
        CollectorContext context,
        ICollectorSchemaInfo definition,
        DbConnection targetConnection,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        context.PgExtensionSchemas = null;
        context.PgExtensionVersions = null;
        context.PgFunctionSchemas = null;
        var readsFunctions = definition.ReadsAuroraFunctions && context.Target.IsAurora;
        if (definition.TargetEngine != CollectorTargetEngine.PostgreSql
            || (definition.RequiredPgExtensions.Count == 0 && !readsFunctions))
        {
            return false;
        }

        var schemas = new Dictionary<string, string>(StringComparer.Ordinal);
        var versions = new Dictionary<string, string>(StringComparer.Ordinal);
        var functions = new List<(string Function, string Schema)>();
        try
        {
            using var command = targetConnection.CreateCommand();
            command.CommandTimeout = PgExtensionSchemasTimeoutSeconds;
            command.CommandText = PgExtensionSchemasSql;
            var parameter = command.CreateParameter();
            parameter.ParameterName = "functions";
            parameter.Value = readsFunctions ? CollectorContext.AuroraFunctionNames.ToArray() : Array.Empty<string>();
            command.Parameters.Add(parameter);
            using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    if (string.Equals(reader.GetString(0), "extension", StringComparison.Ordinal))
                    {
                        schemas[reader.GetString(1)] = reader.GetString(2);
                        /* #6206: the extension's own catalog version, from the same read - no second probe. */
                        if (!reader.IsDBNull(3))
                        {
                            versions[reader.GetString(1)] = reader.GetString(3);
                        }
                    }
                    else
                    {
                        functions.Add((reader.GetString(1), reader.GetString(2)));
                    }
                }
            }
        }
        catch (DbException ex)
        {
            logger?.LogDebug(ex, "Reading extension schemas for {Collector} did not succeed (#6143)", definition.Name);
            throw new InvalidOperationException(
                $"{definition.Name} did not run: reading the schema of each extension it uses from pg_catalog.pg_extension "
                + $"did not succeed, and the collector does not guess a schema. {ex.Message}",
                ex);
        }

        context.PgExtensionSchemas = schemas;
        context.PgExtensionVersions = versions;
        if (readsFunctions)
        {
            var statementsSchema = schemas.TryGetValue("pg_stat_statements", out var found) ? found : null;
            /* #6143: a function pg_proc did not show is called in pg_catalog. This read is fresh on every run, so the
               one function the extension ships is used from the extension's schema only while pg_depend shows it is a
               member there. */
            context.PgFunctionSchemas = CollectorContext.ChoosePgFunctionSchemas(functions, statementsSchema);
        }

        /* #6888: the read above succeeded and a declared extension the query reads is not created in this database, so
           the answer is already known and the query is not sent. Sending it would fail with 42883 / 42P01, which the host
           classifies as a missing extension, and PostgreSQL writes that failure into the MONITORED server's own log, where
           a DBA reads it as an error from the monitor. Raised as the same 42P01 the query would have raised, so the
           worker's classification (PostgresFaultOutcome) records the same EXTENSION_MISSING outcome with the same advice,
           on the server-scoped path and, stamped with the database, on the per-database path. A failed read threw above
           and still fails the run. A collector that runs without the extension on this target (ReadsPgExtension false)
           keeps running. */
        foreach (var dependency in definition.RequiredPgExtensions)
        {
            if (!schemas.ContainsKey(dependency.ExtensionName) && definition.ReadsPgExtension(dependency, context))
            {
                throw new PostgresException(
                    $"extension \"{dependency.ExtensionName}\" is not created in this database",
                    "ERROR", "ERROR", PostgresErrorCodes.UndefinedTable);
            }
        }

        return true;
    }

    /// <summary>
    /// #4251: sets <see cref="CollectorContext.PgFileSettingsReadable"/> for <c>pg_server_config</c> from
    /// <see cref="PgFileSettingsCapability"/> (its own hourly cache), and leaves every other collector
    /// untouched — pg_file_settings only matters to this one read. Its own method, the
    /// <see cref="ResolvePgReadBinaryFileGrantAsync"/> shape, so the gate is testable without a live target.
    /// Logs ONCE per target at Information when the view turns out to be unreadable
    /// (<see cref="PgFileSettingsCapability.ShouldLogUnreadable"/>) rather than on every cycle the fallback
    /// query runs — the collector itself keeps collecting <c>pg_settings</c> either way, so this is
    /// informational, never a fault.
    ///
    /// <para><b>#4251 round-1 review, H1(a).</b> Both <see cref="CollectorContext.PgFileSettingsReadable"/>
    /// and the log line are gated on the SAME probe's Windows fact, not merely on the grant. By PostgreSQL's
    /// process model the stale <c>pending_restart</c> this capability exists to fix only happens on Windows:
    /// on Unix the postmaster itself applies the reload and every backend it forks inherits the flag, so
    /// <c>pg_settings</c> alone is already correct there, granting these two objects fixes nothing, and the
    /// collector keeps today's plain query and stays silent — asking an operator to widen the role for no
    /// gain would be the wrong trade on every non-Windows target, which is most of them (Linux, RDS, Aurora,
    /// Azure, Cloud SQL).</para>
    /// </summary>
    internal static async ValueTask ResolvePgFileSettingsReadableAsync(
        CollectorContext context,
        string collectorName,
        DbConnection targetConnection,
        ServerRuntime server,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        if (server.Target.Engine != CollectorTargetEngine.PostgreSql || collectorName != "pg_server_config")
        {
            return;
        }

        var targetKey = ReadBinaryFileCacheKey(server);
        var readable = await PgFileSettingsCapability.IsReadableAsync(targetConnection, targetKey, cancellationToken);
        PgFileSettingsCapability.TryGetCachedVerdict(targetKey, out _, out var isWindowsTarget);

        /* Windows-gated (H1(a) above): on a non-Windows target this stays false even when the grants ARE
           present, so PgServerConfigCollector.BuildQuery always keeps the plain query there — the enhanced
           one buys nothing when pending_restart is already correct without it. */
        context.PgFileSettingsReadable = readable && isWindowsTarget;

        if (isWindowsTarget && !readable && PgFileSettingsCapability.ShouldLogUnreadable(targetKey))
        {
            logger?.LogInformation(
                "[{Server}] pg_file_settings is not readable by the monitoring role (#4251), so pending_restart "
                + "is computed from pg_settings alone, which can read false on this Windows target for a "
                + "setting that changed and reloaded but is still pending a restart: a connection opened after "
                + "pg_reload_conf() reads pending_restart = false, and this collector opens a new connection "
                + "every cycle. Two grants fix it: GRANT SELECT ON pg_file_settings and GRANT EXECUTE ON "
                + "FUNCTION pg_show_all_file_settings() to the monitoring role. Both are superuser-only by "
                + "default for a reason: they show every line of every configuration file, including lines "
                + "that are not the running value (a superseded or misspelled primary_conninfo with its "
                + "password, for example) and each file's path — grant them only if that exposure is "
                + "acceptable for this role.",
                server.Config.DisplayName);
        }
    }

    /// <summary>
    /// The one place <see cref="PgReadBinaryFileCapability"/>'s cache key comes from (#4051 round-2 review). The
    /// helper that fills the cache, the one that reads it for the advisory note, and the one that drops a stale
    /// verdict all take the <see cref="ServerRuntime"/> and key through here. None of them can drift to a
    /// different key and leave an invalidation that never hits.
    /// </summary>
    internal static string ReadBinaryFileCacheKey(ServerRuntime server) => server.StorageName;

    /// <summary>
    /// #4046 part 1c (#4051 review L1): drops <paramref name="targetKey"/>'s cached
    /// <see cref="PgReadBinaryFileCapability"/> verdict when a log-tail collector's fault says it is stale, so the
    /// next cycle re-checks instead of waiting out the hour.
    /// <list type="bullet">
    /// <item>A 22021 is the text route meeting a planted byte, and its message tells the operator to grant
    /// pg_read_binary_file. A grant made in answer should take effect on the next cycle.</item>
    /// <item>A 42501 while the verdict says granted means the grant was revoked, or the login changed, since the
    /// check. Every binary-route read would fail until the hour ran out.</item>
    /// </list>
    /// A 42501 on the text route leaves the verdict alone, so a target that never granted pg_read_file is not
    /// re-probed every cycle. A 22021 on a database whose encoding the binary route does not serve leaves it
    /// alone too, since no grant changes that answer. A proven write to the STORE leaves it alone (#4051 round-2
    /// review), because that fault says nothing about the target's log. The checks run cheapest first and
    /// allocate nothing for a fault that is not a <see cref="PostgresException"/>, because the general handler
    /// that calls this is also the OutOfMemoryException landing pad.
    /// </summary>
    internal static void ForgetStaleReadBinaryFileVerdict(string collectorName, Exception ex, ServerRuntime server)
    {
        if (ex is not PostgresException pg
            || !ReadsPgServerLogTail(collectorName)
            || CollectorFaultCopyPhase.IsProvenStoreWrite(ex))
        {
            return;
        }

        var targetKey = ReadBinaryFileCacheKey(server);

        if ((pg.SqlState == "22021" && !PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding(targetKey))
            || (pg.SqlState == "42501" && PgReadBinaryFileCapability.TryGetCachedVerdict(targetKey, out var granted) && granted))
        {
            PgReadBinaryFileCapability.Invalidate(targetKey);
        }
    }

    /// <summary>
    /// #4251 round-1 review, M1: drops a stale "readable" pg_file_settings verdict when pg_server_config's
    /// file-settings query is refused (42501: the grant was revoked, or the login changed, since the check),
    /// so the next cycle re-probes and falls back to the pg_settings-only query instead of failing until the
    /// hour runs out — without this, every pg_server_config cycle lost the WHOLE pg_settings snapshot for up
    /// to <see cref="PgFileSettingsCapability.CacheTtl"/>, the one outcome <see cref="PgServerConfigCollector"/>
    /// must never have.
    /// </summary>
    internal static void ForgetStaleFileSettingsVerdict(string collectorName, Exception ex, ServerRuntime server)
    {
        if (ex is not PostgresException { SqlState: "42501" }
            || collectorName != "pg_server_config"
            || CollectorFaultCopyPhase.IsProvenStoreWrite(ex))
        {
            return;
        }

        var targetKey = ReadBinaryFileCacheKey(server);
        if (PgFileSettingsCapability.TryGetCachedVerdict(targetKey, out var readable, out _) && readable)
        {
            PgFileSettingsCapability.Invalidate(targetKey);
        }
    }
}
