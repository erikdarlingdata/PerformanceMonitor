/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.RDS;
using Amazon.RDS.Model;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// Fetches PostgreSQL server-log text from the RDS API, for targets with no filesystem to read (#2538).
///
/// <para><b>Why this exists at all.</b> <c>auto_explain</c> writes plans to the server log and nowhere else.
/// On a self-hosted server the collector reads that log with <c>pg_read_file</c>; on Aurora and RDS there is
/// no filesystem, <c>pg_read_server_files</c> is not grantable, and the log is only reachable through
/// <c>DownloadDBLogFilePortion</c>. Same text, different transport — which is exactly why the parsing and
/// redaction were moved into <c>PgPlanLogParser</c> first.</para>
///
/// <para><b>This is the only code in the product that reaches a monitored target other than through a
/// database connection.</b> It holds no credentials of its own: the SDK's default chain finds the EC2
/// instance profile the service already runs under, which is how the monitoring hosts reach every other AWS
/// API today. Nothing is stored, so nothing can leak from config — and a host with no role simply fails the
/// call and the collector degrades, rather than the product asking anyone to paste keys into a file.</para>
///
/// <para><b>The marker is held in memory and saved beside the rows it covers.</b> RDS returns a position to
/// resume from, and this source keeps it per process; the ingestors save it to <c>collect.collector_state</c>
/// (<see cref="RdsResumeStore"/>, the store the self-hosted tail keeps its own marker in, #4704) after each chunk's
/// rows are stored, and restore it before a server's first read, so a restart resumes where the last cycle stopped.
/// A position that cannot be resumed (the file is no longer listed, or the saved value is not an RDS position)
/// falls back to the last <see cref="FirstReadLines"/> lines of the newest file, and the chunk says so
/// (<see cref="LogChunk.ResumeFileMissing"/>). Re-reading is harmless here: plan rows dedup on (queryid,
/// plan_hash), deadlocks on <c>deadlock_hash</c> and log events on <c>raw_line_hash</c>, so an overlapping window
/// produces the same rows rather than duplicates - the same property the <c>pg_read_file</c> route relies on.</para>
///
/// <para><b>A log rotation is followed, not jumped.</b> RDS rotates the log (hourly by default), and the
/// position names ONE file. When the newest file is no longer the marked one, the read finishes the marked
/// file from its marker first (<see cref="LogChunk.ReadAgain"/>), then moves the position to the start of
/// the newest file (<see cref="ResumeMarker.NextKey"/>), so the lines the old file received after the last
/// read are collected and a new file is read from its first line rather than from its last
/// <see cref="FirstReadLines"/>. Files that rotated in between are counted, not read
/// (<see cref="LogChunk.FilesSkipped"/>), as the self-hosted tail does.</para>
///
/// <para><b>The marker only moves when the caller says so</b> (<see cref="CommitResume"/>), because that
/// re-read tolerance is the whole reason it is safe to prefer a repeat over a loss. The transport is
/// consume-once — <c>DownloadDBLogFilePortion</c> will not hand the same bytes out twice — so a marker that
/// advanced inside the fetch turned any later failure into permanent data loss, while a marker that
/// advances after the write can at worst re-store a window the store already tolerates. The saved position
/// follows the same order: it is written after the store write, so a crash re-reads a window rather than losing
/// one.</para>
/// </summary>
public sealed class RdsLogSource
{
    /// <summary>
    /// How much of a log file to take on the FIRST read of a target, before any marker exists. Bounded for
    /// the same reason the file route reads a tail: #2565 measured 772 MB of log in twenty seconds at
    /// capture-everything, and an unbounded first read would pull all of it across the network.
    /// </summary>
    internal const int FirstReadLines = 10_000;

    /// <summary>
    /// #4708: the most reads one ingest cycle makes of a target while <see cref="LogChunk.ReadAgain"/> is set - the
    /// rest of a rotated file, then the newest file from its first line. Bounded so a large old file cannot hold a
    /// cycle: the position stays where the last committed pass left it and the next cycle carries on.
    /// </summary>
    public const int MaxPassesPerCycle = 4;

    /// <summary>
    /// #4708: the most pages of <c>DescribeDBLogFiles</c> one listing reads. The API answers in pages once an instance
    /// holds more log files than one page carries, and every page but the last names the next in a Marker. Bounded so an
    /// instance with an unreasonable number of files, or a service that never stops handing out new Markers, cannot hold
    /// a cycle: the read goes on with the files it has and logs a Warning that the listing was cut short.
    /// </summary>
    internal const int MaxLogListingPages = 20;

    /// <summary>
    /// #4708: runs <paramref name="pass"/> - one fetch, store and commit - and again while it reports
    /// <see cref="LogChunk.ReadAgain"/>, at most <see cref="MaxPassesPerCycle"/> times, adding the outcomes. A pass
    /// that throws ends the cycle exactly as a single read did: the passes before it are stored and committed, and the
    /// position is where the last of them left it.
    /// </summary>
    public static async Task<RdsIngestOutcome> RunPassesAsync(Func<Task<(RdsIngestOutcome Outcome, bool ReadAgain)>> pass)
    {
        ArgumentNullException.ThrowIfNull(pass);

        var total = RdsIngestOutcome.NotReached;

        for (var attempt = 1; attempt <= MaxPassesPerCycle; attempt++)
        {
            var (outcome, readAgain) = await pass();
            total = attempt == 1 ? outcome : total.Plus(outcome);

            if (!readAgain)
            {
                break;
            }
        }

        return total;
    }

    /// <summary>
    /// #4053 review round 1 (item 4): how much newer the newest stderr file's <c>LastWritten</c> (in ms
    /// since epoch, the SDK's own unit) has to be than the newest <c>.csv</c>'s before the csvlog route
    /// treats the <c>.csv</c> listing as stale and throws <see cref="PgNoCsvlogFileException"/> rather than
    /// reading it.
    /// </summary>
    private const long StaleCsvThresholdMs = 5 * 60 * 1000;

    /// <summary>
    /// Which of an instance's PostgreSQL log files a read is for (#4053 part c1). RDS writes a target's
    /// <c>csvlog</c> output as the stderr file's own name with <c>.csv</c> appended, so the two share one
    /// <c>DescribeDBLogFiles</c> listing and differ only in which name <see cref="LogFilesNewestFirstAsync"/>
    /// picks out of it. The resume marker needs no separate design for this: it is already keyed by
    /// (instance, file name), so the csv file gets its own marker the first time anything asks for it.
    /// </summary>
    public enum LogFileKind
    {
        /// <summary>The default stderr-format file — today's only caller, and every caller but the
        /// csvlog-aware log-event ingestor.</summary>
        Stderr,

        /// <summary>The <c>.csv</c> sibling <c>csvlog</c> writes beside the stderr file, read only when
        /// the caller already knows the target's <c>log_destination</c> includes it.</summary>
        Csv,
    }

    private readonly Dictionary<string, string> _markers = new(StringComparer.Ordinal);

    /// <summary>Test-only visibility into which marker keys survive a commit (#4053 review round 2, item 3):
    /// whether <paramref name="key"/> (an "instance|file" pair) still holds a marker.</summary>
    internal bool HasMarkerForKey(string key) => _markers.ContainsKey(key);

    private readonly Func<string, IAmazonRDS> _clientFactory;

    private readonly Func<DateTime> _clock;

    private readonly ILogger? _logger;

    /// <summary>#4053 review round 2 (item 2): per-instance, when the stale-csv condition (stderr more than
    /// <see cref="StaleCsvThresholdMs"/> newer than the newest .csv) was FIRST seen. Cleared the moment the
    /// condition is gone, so a single wide-but-transient gap between two files rolled by the same syslogger
    /// — the false positive this debounce exists for — cannot throw on its own; only a gap that HOLDS across
    /// this many minutes of real time does.</summary>
    private readonly Dictionary<string, DateTime> _staleSinceUtc = new(StringComparer.Ordinal);

    /// <summary>How long the stale-csv condition has to hold, continuously, before
    /// <see cref="LogFilesNewestFirstAsync"/> throws <see cref="PgNoCsvlogFileException"/> — the same 5 minutes as
    /// <see cref="StaleCsvThresholdMs"/> itself, but measured in wall-clock cycles rather than the two files'
    /// own timestamps.</summary>
    private static readonly TimeSpan StaleCsvDebounce = TimeSpan.FromMinutes(5);

    /// <summary>#4708: per instance, when the Warning that a listing hit <see cref="MaxLogListingPages"/> last went
    /// out, so an instance that stays over the cap says so once an hour instead of on every cycle. One table for each
    /// logger rather than one for each source: the plan, deadlock and log-event ingestors each own a source and all
    /// log to the runner's one logger, so a table on the source alone would report an instance up to three times an
    /// hour. A table for each logger also keeps two loggers' reports apart, which is what lets a test read it.</summary>
    private static readonly ConditionalWeakTable<ILogger, Dictionary<string, DateTime>> ListingCapWarnedUtc = new();

    private static readonly TimeSpan ListingCapWarnInterval = TimeSpan.FromHours(1);

    public RdsLogSource(Func<string, IAmazonRDS>? clientFactory = null, Func<DateTime>? clock = null, ILogger? logger = null)
    {
        _clientFactory = clientFactory
            ?? (region => new AmazonRDSClient(RegionEndpoint.GetBySystemName(region)));
        _clock = clock ?? (() => DateTime.UtcNow);
        _logger = logger;
    }

    /// <param name="Text">Raw log text, to be handed to <c>PgPlanLogParser.Extract</c> unchanged.</param>
    /// <param name="MoreAvailable">RDS had more than one call's worth. The caller decides whether to keep
    /// pulling; this type does not loop, so one cycle cannot spend unbounded time on one target.</param>
    /// <param name="Resume">Where the NEXT read should start, once <see cref="Text"/> has actually reached
    /// the store. Handed back rather than recorded on the way out — see
    /// <see cref="CommitResume"/>.</param>
    /// <param name="StartsAtFileStart">#4053 review round 1: true when this read was requested from offset 0
    /// of a NEW file — a rotation, where no marker existed for this file's key but one existed for the same
    /// instance under a different (older) csvlog file name. The forward-only route's only source of a known
    /// start: <see cref="Text"/> begins at the file's first byte, so parity is exact from it without any
    /// forward walk having to prove it.</param>
    /// <param name="ReadAgain">#4708: true when this chunk came from a file that is no longer the newest, so the
    /// caller commits it and reads again in the same cycle — the rest of that file first, then the newest file
    /// from its first line. False for a read of the newest file, which is one chunk per cycle as before.</param>
    /// <param name="FilesSkipped">#4708: log files that rotated between the marked file and the newest and that no
    /// read opened. Set on the chunk that finishes the marked file; the self-hosted tail's
    /// <c>log_files_skipped_by_rotation</c>.</param>
    /// <param name="ResumeFileMissing">#4708: true when a position was held but its file is gone from the RDS log
    /// listing, so this read fell back to the newest file's last <see cref="FirstReadLines"/> lines; the
    /// self-hosted tail's <c>log_resume_file_missing</c>.</param>
    public readonly record struct LogChunk(
        string Text, bool MoreAvailable, ResumeMarker Resume, bool StartsAtFileStart = false,
        bool ReadAgain = false, int FilesSkipped = 0, bool ResumeFileMissing = false);

    /// <summary>
    /// A position this source can resume from, and the file it belongs to. Opaque to the caller: the
    /// marker is a service token, not an offset, so there is nothing to compute with — a caller's only
    /// move is to hand it back once the chunk it came with is durable.
    /// </summary>
    /// <param name="Key">The (instance, file) this marker belongs to.</param>
    /// <param name="Marker">RDS's own resume token, or null when the response carried none.</param>
    /// <param name="NextKey">#4708: set on the chunk that reached the end of a file that is no longer the newest.
    /// Committing it moves the position to the start of that newer (instance, file), and only then is the old
    /// file's key dropped, so a failed store write leaves the old file's position in place.</param>
    public readonly record struct ResumeMarker(string? Key, string? Marker, string? NextKey = null);

    /// <summary>
    /// Advance this source past a chunk whose rows are in the store.
    ///
    /// <para><b>This is separate from the read on purpose, and it is the whole point of the type.</b> The
    /// marker used to be recorded inside <see cref="ReadNewestAsync"/>, before the caller had done anything
    /// with the text. On a consume-once transport that made every failure between the fetch and a committed
    /// COPY a permanent loss: <c>DownloadDBLogFilePortion</c> does not hand the same bytes out twice, the
    /// marker lives in this process, and the next call resumed past a chunk nobody stored. A parse fault, a
    /// COPY that tripped its deadline, a dropped store connection and a cancelled cycle all lost every
    /// report in that window with no error naming the loss.</para>
    ///
    /// <para>Doing nothing on an unset marker is deliberate rather than defensive: it lets a caller commit
    /// unconditionally on its success path without first asking whether the read produced a token, which is
    /// the shape that keeps the commit next to the write it depends on.</para>
    /// </summary>
    public void CommitResume(ResumeMarker resume)
    {
        if (!string.IsNullOrEmpty(resume.Key) && !string.IsNullOrEmpty(resume.Marker))
        {
            CommitKey(resume.Key, resume.Marker);
        }

        /* #4708: the chunk finished a file that is no longer the newest, so the position moves to the start of
           the newer file. This runs AFTER the store write like the marker above, and it is what drops the old
           file's key: CommitKey prunes every other key of the same kind for the instance. */
        if (!string.IsNullOrEmpty(resume.NextKey))
        {
            CommitKey(resume.NextKey, StartOfFileMarker);
        }
    }

    /// <summary>The RDS marker that asks for a file from its first byte (<c>Marker "0"</c>).</summary>
    internal const string StartOfFileMarker = "0";

    /// <summary>
    /// #4708: seeds a position a previous process saved, for <paramref name="instanceId"/>'s <paramref name="kind"/> file
    /// <paramref name="file"/> at <paramref name="marker"/>. Returns false, and changes nothing, when this source
    /// already holds a position for that instance and kind (the in-process position is newer than any saved one) or
    /// when the file name is not of <paramref name="kind"/>. The next read treats the seeded position like any other,
    /// including reporting it missing when RDS no longer lists the file.
    /// </summary>
    public bool RestorePosition(LogFileKind kind, string instanceId, string file, string marker)
    {
        if (string.IsNullOrEmpty(instanceId) || string.IsNullOrEmpty(file) || string.IsNullOrEmpty(marker)
            || IsCsvFileName(file) != (kind == LogFileKind.Csv)
            || FindHeldPosition(instanceId, kind) is not null)
        {
            return false;
        }

        _markers[instanceId + "|" + file] = marker;
        return true;
    }

    /// <summary>
    /// #4708: the position <see cref="CommitResume"/> leaves this source at for <paramref name="resume"/> - the start of
    /// the newer file when the chunk finished an older one, else the chunk's own file and marker - or null when the
    /// chunk carries none. What the ingestors save after the chunk's rows are stored.
    /// </summary>
    public static (string Instance, string File, string Marker)? CommittedPosition(ResumeMarker resume)
    {
        var moved = !string.IsNullOrEmpty(resume.NextKey);
        var key = moved ? resume.NextKey : resume.Key;
        var marker = moved ? StartOfFileMarker : resume.Marker;

        var instance = InstanceKey(key);
        var file = ResumeFileName(key);

        return string.IsNullOrEmpty(instance) || string.IsNullOrEmpty(file) || string.IsNullOrEmpty(marker)
            ? null
            : (instance, file, marker);
    }

    private void CommitKey(string key, string marker)
    {
        /* Keyed by FILE as well as instance, so a log rotation starts a fresh marker instead of resuming a
           new file at an old file's offset. */
        _markers[key] = marker;

        /* Once THIS file's position has committed, every other key this same instance holds under the same
           KIND (csv vs non-csv, going by the file name's own ".csv" suffix) is dead weight — a rotation never
           resumes the old file, so its entry would otherwise sit in this dictionary forever. The old file's
           key is therefore dropped only when the position moves to the newer file (the chunk's NextKey), which
           happens after the old file's remainder has been stored (#4708), and not when the newer file is first
           SEEN: pruning at detection would forget the position the drain read needs.

           A csv → stderr → csv switch (csvlog toggled off and back on) is the one case this still leaves
           imperfect: the middle stderr commit prunes only the instance's other STDERR keys, so the original
           csv key survives it. The csv route then finds that old key, finishes that file if RDS still lists
           it, and moves on to the newest csv file from its first line; events the stderr route already stored
           for the same window can be stored again, and the identity hashes keep them from duplicating. */
        var instanceId = InstanceKey(key);
        var isCsv = IsCsvFileName(ResumeFileName(key));

        if (instanceId is null)
        {
            return;
        }

        var prefix = instanceId + "|";
        List<string>? toRemove = null;

        foreach (var existingKey in _markers.Keys)
        {
            if (string.Equals(existingKey, key, StringComparison.Ordinal)
                || !existingKey.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            if (IsCsvFileName(existingKey[prefix.Length..]) == isCsv)
            {
                (toRemove ??= new List<string>()).Add(existingKey);
            }
        }

        if (toRemove is not null)
        {
            foreach (var stale in toRemove)
            {
                _markers.Remove(stale);
            }
        }
    }

    /// <summary>Whether a log file name is the <c>.csv</c> kind rather than the stderr kind —
    /// <see cref="CommitResume"/>'s own marker-pruning check (#4053 review round 2, item 3).</summary>
    private static bool IsCsvFileName(string? fileName)
        => !string.IsNullOrEmpty(fileName) && fileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);

    /// <summary>The instance half of a marker key ("instance|file"), or null when the key itself is
    /// null/empty — <see cref="CommitResume"/>'s own copy of the same split
    /// <see cref="RdsLogEventIngestor"/> keeps privately for its carry key.</summary>
    private static string? InstanceKey(string? resumeKey)
    {
        if (string.IsNullOrEmpty(resumeKey))
        {
            return null;
        }

        var separator = resumeKey.IndexOf('|');
        return separator < 0 ? resumeKey : resumeKey[..separator];
    }

    /// <summary>The file half of a marker key, or null when the key itself is null/empty.</summary>
    private static string? ResumeFileName(string? resumeKey)
    {
        if (string.IsNullOrEmpty(resumeKey))
        {
            return null;
        }

        var separator = resumeKey.IndexOf('|');
        return separator < 0 ? null : resumeKey[(separator + 1)..];
    }

    /// <summary>
    /// The newest PostgreSQL log file's unread portion, or null when this target is not RDS at all.
    ///
    /// <para>A cluster endpoint is resolved to its WRITER, because <c>DownloadDBLogFilePortion</c> takes an
    /// instance identifier and because the writer is where the workload worth capturing runs. A READER
    /// endpoint is refused rather than guessed at: it round-robins across replicas, so the instance behind
    /// it is not stable between calls and plans captured through one would be attributed to whichever
    /// replica answered.</para>
    /// </summary>
    public Task<LogChunk?> ReadNewestAsync(string host, CancellationToken cancellationToken = default)
        => ReadNewestAsync(host, LogFileKind.Stderr, cancellationToken);

    /// <summary>
    /// <see cref="ReadNewestAsync(string, CancellationToken)"/>, but for the csvlog sibling rather than the
    /// stderr file (#4053 part c1) when <paramref name="kind"/> is <see cref="LogFileKind.Csv"/>. Every other
    /// behaviour — the writer resolution, the marker discipline, the bounded first read — is unchanged; only
    /// which file name <see cref="LogFilesNewestFirstAsync"/> picks differs.
    /// </summary>
    public async Task<LogChunk?> ReadNewestAsync(string host, LogFileKind kind, CancellationToken cancellationToken = default)
    {
        var endpoint = RdsEndpoint.TryParse(host);

        if (endpoint is null)
        {
            return null;
        }

        var parsed = endpoint.Value;

        if (parsed.Kind is RdsEndpointKind.ClusterReader or RdsEndpointKind.ClusterCustom)
        {
            throw new InvalidOperationException(
                $"'{host}' is an Aurora {(parsed.Kind == RdsEndpointKind.ClusterReader ? "reader" : "custom")} "
                + "endpoint, which does not resolve to a stable instance — it moves between replicas "
                + "call to call. Point the target at the cluster writer endpoint or at a specific instance "
                + "so captured plans belong to a server that can be named.");
        }

        using var client = _clientFactory(parsed.Region);

        var instanceId = parsed.Kind == RdsEndpointKind.ClusterWriter
            ? await ResolveWriterAsync(client, parsed.Identifier, cancellationToken)
            : parsed.Identifier;

        var files = await LogFilesNewestFirstAsync(client, instanceId, kind, cancellationToken);
        var newest = files[0];

        var held = FindHeldPosition(instanceId, kind);

        /* #4708: the position names a file that is no longer the newest - RDS rotated the log since the last
           read. Finish THAT file from its marker before anything else. Reading only the newest file, as this
           did before, dropped whatever the old file received after the last read, and started a new file at
           its last FirstReadLines lines instead of its first. The position moves to the newest file (from its
           first byte) only when the old file's remainder reaches the store: the chunk carries NextKey and
           CommitResume applies it, so a failed write re-reads the old file's remainder next time. */
        var heldFileIndex = held is null ? -1 : files.IndexOf(held.Value.File);

        if (held is not null && heldFileIndex > 0)
        {
            var drain = await client.DownloadDBLogFilePortionAsync(
                new DownloadDBLogFilePortionRequest
                {
                    DBInstanceIdentifier = instanceId,
                    LogFileName = held.Value.File,
                    Marker = held.Value.Marker,
                    NumberOfLines = 0,
                },
                cancellationToken);

            var morePending = drain.AdditionalDataPending == true;

            /* The end of the old file was reached: the next position is the newest file's first byte, and the
               files strictly between the two (listed newest first, so indexes 1 .. heldFileIndex - 1) are the
               ones no read opens. */
            var nextKey = morePending ? null : instanceId + "|" + newest;
            var skipped = morePending ? 0 : heldFileIndex - 1;

            return new LogChunk(
                drain.LogFileData ?? string.Empty,
                morePending,
                new ResumeMarker(held.Value.Key, drain.Marker, nextKey),
                StartsAtFileStart: string.Equals(held.Value.Marker, StartOfFileMarker, StringComparison.Ordinal),
                ReadAgain: true,
                FilesSkipped: skipped);
        }

        /* A position was held but its file is gone from the listing (RDS keeps a bounded number of files):
           nothing can be resumed, so this is a first contact with the newest file, and the chunk says so. */
        var resumeFileMissing = held is not null && heldFileIndex < 0;

        var key = instanceId + "|" + newest;
        var hasMarkerForThisFile = held is not null && heldFileIndex == 0;
        string? requestedMarker = hasMarkerForThisFile ? held!.Value.Marker : null;
        var requestedLines = hasMarkerForThisFile ? 0 : FirstReadLines;

        var response = await client.DownloadDBLogFilePortionAsync(
            new DownloadDBLogFilePortionRequest
            {
                DBInstanceIdentifier = instanceId,
                LogFileName = newest,
                Marker = requestedMarker,
                NumberOfLines = requestedLines,
            },
            cancellationToken);

        /* The marker is RETURNED, not recorded. Recording it here would advance this source past text the
           caller has not looked at yet, and on a consume-once transport that is a permanent loss rather
           than a repeated read - see CommitResume. */

        /* AdditionalDataPending is bool? in the SDK. Treated as false when null: claiming more is
           pending when the API did not say so would make a caller loop for data that is not there. */
        return new LogChunk(
            response.LogFileData ?? string.Empty,
            response.AdditionalDataPending == true,
            new ResumeMarker(key, response.Marker),
            StartsAtFileStart: string.Equals(requestedMarker, StartOfFileMarker, StringComparison.Ordinal),
            ResumeFileMissing: resumeFileMissing);
    }

    /// <summary>
    /// The position this source holds for <paramref name="instanceId"/> under <paramref name="kind"/> - at most
    /// one, because <see cref="CommitKey"/> keeps a single key per instance and kind - or null on a first
    /// contact. The kind test is the file-name rule <see cref="LogFilesNewestFirstAsync"/> lists by (csv
    /// against everything that is not csv), so a stderr position is never matched by the csv route.
    /// </summary>
    private (string Key, string File, string Marker)? FindHeldPosition(string instanceId, LogFileKind kind)
    {
        var prefix = instanceId + "|";

        foreach (var (existingKey, marker) in _markers)
        {
            if (!existingKey.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            var fileName = existingKey[prefix.Length..];

            if (IsCsvFileName(fileName) == (kind == LogFileKind.Csv))
            {
                return (existingKey, fileName, marker);
            }
        }

        return null;
    }

    /* An AWS SDK response collection is NULL when the service omitted it, not an empty list, so the two
       null tests below are the ordinary path rather than defence: DescribeDBClusters answers with no
       DBClusters element when it matched nothing, and DBClusterMembers is absent on a cluster reporting no
       members. LINQ over either raises ArgumentNullException, whose entire message is "Value cannot be
       null. (Parameter 'source')" — seven words naming neither the call nor the branch, and it lands in
       collection_log as a raw ERROR.

       Each null is therefore routed into the message that already says what THAT branch means, rather than
       coalesced to an empty sequence: "this cluster does not exist" is a target pointed somewhere wrong and
       "this cluster has no writer" is a failover in progress, and the two have opposite responses. */
    private static async Task<string> ResolveWriterAsync(
        IAmazonRDS client, string clusterId, CancellationToken cancellationToken)
    {
        var clusters = await client.DescribeDBClustersAsync(
            new DescribeDBClustersRequest { DBClusterIdentifier = clusterId }, cancellationToken);

        var cluster = clusters.DBClusters?.FirstOrDefault()
            ?? throw new InvalidOperationException(
                $"Aurora cluster '{clusterId}' was not found: DescribeDBClusters returned no cluster for it.");

        var writer = cluster.DBClusterMembers?.FirstOrDefault(m => m.IsClusterWriter == true)
            ?? throw new InvalidOperationException(
                $"Aurora cluster '{clusterId}' reports no writer among its members. That is a real state "
                + "during a failover, so this is worth retrying rather than treating as a configuration "
                + "error.");

        return writer.DBInstanceIdentifier;
    }

    /// <summary>
    /// The PostgreSQL log files of one kind, newest first (#4708: the first entry is the file every read used to
    /// open; the rest are the older files a rotation leaves behind, which the read finishes before it moves on).
    /// Filtered by name because an instance's log list also carries
    /// upgrade and other logs, and sorted by last-written rather than by name — the filename embeds a
    /// timestamp, but sorting text would order 2026-08-9 after 2026-08-10.
    ///
    /// <para><b>Reads every page of the listing</b> (#4708). <c>DescribeDBLogFiles</c> answers in pages when an
    /// instance holds more log files than one page carries - three days of hourly stderr and csvlog files is about
    /// 144 - and the newest file, or the file a saved position names, can be on a later page. <see cref="ListLogFilesAsync"/>
    /// follows the Marker to the last page, up to <see cref="MaxLogListingPages"/>, and every choice below (the
    /// name filter, the newest file, the held file, the stale-csv check) is made over the whole list.</para>
    ///
    /// <para><b>Also filtered to exclude <c>.csv</c>/<c>.json</c> siblings</b> (#3997), the same defect and
    /// the same fix as the self-hosted tail's <c>newest</c> CTE (<see cref="PerformanceMonitor.Collectors.PgServerLogTail"/>).
    /// RDS for PostgreSQL writes a target's <c>csvlog</c> output as the stderr file's own name with
    /// <c>.csv</c> appended — <c>error/postgresql.log.2026-08-25-18</c> beside
    /// <c>error/postgresql.log.2026-08-25-18.csv</c> — so <c>FilenameContains = "postgresql"</c> matches
    /// both and <c>OrderByDescending(LastWritten)</c> alone can return either one, exactly as
    /// <c>pg_ls_logdir()</c>'s mtime ordering can on the self-hosted route. <c>PgPlanLogParser</c> and
    /// <c>PgDeadlockLogParser</c> read whatever text this hands them as stderr-format regardless of
    /// transport, so a csvlog file chosen here fails the same way a csvlog file chosen there does.</para>
    ///
    /// <para><b>An instance with no openable PostgreSQL log file raises rather than answering
    /// "nothing".</b> A caller can act on two outcomes — the log was opened and held nothing new, or no log
    /// was opened — and only the first licenses the "no new … in the RDS log window" note the runner stamps
    /// on a zero-row cycle, which is a claim about the log's CONTENTS. Answering with a silent empty read
    /// is the #2633 confusion arriving by a second route. A stopped instance, one still being created, and
    /// one whose logs have just rotated all answer this way and clear on the first cycle that finds a
    /// log, and the message says so rather than sending anyone to look for a grant. It stays a loud
    /// ERROR either way — the store's rule for an unclassified failure, and the band a target nobody
    /// can read should carry, because the alternative on this fleet is the quiet blindness #2994 is
    /// about.</para>
    ///
    /// <para>Total, therefore, rather than nullable: an empty list, an omitted one, and a newest entry
    /// carrying no filename are one fact — there is nothing here to open — and a null return would have the
    /// caller decide that again, which is where the silent empty read came from.</para>
    /// </summary>
    private async Task<List<string>> LogFilesNewestFirstAsync(
        IAmazonRDS client, string instanceId, LogFileKind kind, CancellationToken cancellationToken)
    {
        var files = await ListLogFilesAsync(client, instanceId, cancellationToken);

        /* Guarding the omitted collection is load bearing, as in ResolveWriterAsync above: the SDK omits the
           collection entirely on an answer that carried no file, so ordering a null raises
           ArgumentNullException and buries this branch behind "Value cannot be null. (Parameter
           'source')". ListLogFilesAsync reads an omitted collection as no files, so absent and empty both
           arrive here as an empty list and reach the one message below.

           The name filter picks the file this READ wants (#4053 part c1): the stderr route excludes the
           .csv/.json siblings (#3997) — see the method's own remarks — while the csvlog route (LogFileKind.Csv)
           selects the newest name that ENDS with .csv, its own mirror of that same defect. AWS's own docs state
           that enabling csvlog on RDS for PostgreSQL always writes stderr alongside it, so the stderr filter is
           not expected to ever empty the list on its own; a target where it somehow did, on either route, falls
           through to the same "no log file" refusal below, which is the honest answer either way. */
        Func<string?, bool> matchesKind = kind == LogFileKind.Csv
            ? name => !string.IsNullOrEmpty(name) && name.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)
            : name => !string.IsNullOrEmpty(name)
                && !name.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)
                && !name.EndsWith(".json", StringComparison.OrdinalIgnoreCase);

        if (kind == LogFileKind.Csv)
        {
            /* #4053 review round 1 (item 4): csvlog turned OFF leaves the cached "csvlog is on" verdict
               (PgLogFormatCapability, an hour's TTL) reading a .csv file that has gone stale — the syslogger
               keeps writing the stderr file every cycle but stopped rolling .csv ones, and without this check
               the route would keep re-reading the same old .csv file's unread tail (nothing, since nothing
               new ever arrives) rather than surfacing that the format actually in use changed. A newest-stderr
               timestamp more than 5 minutes ahead of the newest .csv's is that signal: on an instance actually
               taking write traffic, one round trip's worth of drift between two files rolled by the same
               syslogger is not this large; a target so idle neither file moves within 5 minutes produces no
               difference to flag at all, so idleness is not a false-positive path. */
            var newestStderr = files
                .Where(f => !string.IsNullOrEmpty(f.LogFileName)
                    && !f.LogFileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)
                    && !f.LogFileName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                .Select(f => f.LastWritten)
                .Where(w => w.HasValue)
                .Select(w => w!.Value)
                .OrderByDescending(w => w)
                .FirstOrDefault();

            var newestCsv = files
                .Where(f => !string.IsNullOrEmpty(f.LogFileName)
                    && f.LogFileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
                .Select(f => f.LastWritten)
                .Where(w => w.HasValue)
                .Select(w => w!.Value)
                .OrderByDescending(w => w)
                .FirstOrDefault();

            /* #4053 review round 2 (item 2): the timestamp gap alone was a false positive on a target that
               merely straddled one slow round trip through DescribeDBLogFiles — a single stale-looking read
               is not the same fact as csvlog having actually been turned off. staleSinceUtc (per instance)
               makes the throw wait for the SAME instance to read stale on every call across a real 5-minute
               window, not just the one call that happened to see the gap; the moment a call sees the gap
               close, the instance's entry is cleared and the debounce starts over from nothing. */
            var staleNow = newestStderr - newestCsv > StaleCsvThresholdMs;

            if (staleNow)
            {
                if (!_staleSinceUtc.TryGetValue(instanceId, out var since))
                {
                    _staleSinceUtc[instanceId] = _clock();
                }
                else if (_clock() - since >= StaleCsvDebounce)
                {
                    throw new PgNoCsvlogFileException();
                }
            }
            else
            {
                _staleSinceUtc.Remove(instanceId);
            }
        }

        var ordered = files
            .OrderByDescending(f => f.LastWritten)
            .Select(f => f.LogFileName)
            .Where(name => matchesKind(name))
            .ToList();

        return ordered is { Count: > 0 }
            ? ordered
            : throw new InvalidOperationException(
                kind == LogFileKind.Csv
                    ? $"RDS listed no PostgreSQL csvlog file for instance '{instanceId}': DescribeDBLogFiles "
                        + "filtered on 'postgresql' returned nothing it could name ending '.csv' (#4053 part c1). "
                        + "NO LOG WAS OPENED this cycle, so this is not an empty log — whatever this window held is "
                        + "unread. This records as a collection ERROR and not as a permissions skip, because no "
                        + "grant fixes it: an instance that only just added csvlog to log_destination and has not "
                        + "rolled a .csv file yet answers this way and clears itself on the first cycle that finds "
                        + "one, while one that keeps answering this way is a target nobody can read and wants a "
                        + "decision rather than silence."
                    : $"RDS listed no PostgreSQL server log file for instance '{instanceId}': DescribeDBLogFiles "
                        + "filtered on 'postgresql' returned nothing it could name that was not a csvlog/jsonlog "
                        + "sibling (#3997). NO LOG WAS OPENED this cycle, "
                        + "so this is not an empty log — whatever this window held is unread. This records as a "
                        + "collection ERROR and not as a permissions skip, because no grant fixes it: an instance "
                        + "that is stopped, still being created, or has just rotated its logs answers this way and "
                        + "clears itself on the first cycle that finds a log, while one that keeps answering this "
                        + "way is a target nobody can read and wants a decision rather than silence.");
    }

    /// <summary>
    /// #4708: every page of the <c>DescribeDBLogFiles</c> listing for <paramref name="instanceId"/>, gathered into one
    /// list. Each request repeats the instance and the <c>postgresql</c> name filter and adds only the Marker the
    /// previous answer returned.
    ///
    /// <para>The loop ends when an answer carries no Marker, when it carries the Marker it was asked with (a service
    /// that keeps answering the same thing must not keep the read asking), or after <see cref="MaxLogListingPages"/>
    /// pages. A page with no files but a Marker is followed. At the cap the read goes on with the files read so far,
    /// and <see cref="WarnListingCapReached"/> says so.</para>
    /// </summary>
    private async Task<List<DescribeDBLogFilesDetails>> ListLogFilesAsync(
        IAmazonRDS client, string instanceId, CancellationToken cancellationToken)
    {
        var files = new List<DescribeDBLogFilesDetails>();
        string? marker = null;

        for (var page = 1; ; page++)
        {
            var response = await client.DescribeDBLogFilesAsync(
                new DescribeDBLogFilesRequest
                {
                    DBInstanceIdentifier = instanceId,
                    FilenameContains = "postgresql",
                    Marker = marker,
                },
                cancellationToken);

            /* The SDK omits the collection on an answer that carried no file (see LogFilesNewestFirstAsync), and
               a page like that can still carry a Marker, so an omitted collection adds nothing and the loop goes on. */
            if (response.DescribeDBLogFiles is not null)
            {
                files.AddRange(response.DescribeDBLogFiles);
            }

            var next = response.Marker;

            if (string.IsNullOrEmpty(next) || string.Equals(next, marker, StringComparison.Ordinal))
            {
                return files;
            }

            if (page >= MaxLogListingPages)
            {
                WarnListingCapReached(instanceId);
                return files;
            }

            marker = next;
        }
    }

    /// <summary>
    /// #4708: the listing for <paramref name="instanceId"/> still had a Marker after <see cref="MaxLogListingPages"/>
    /// pages, so the read went on with the files it had. There is no measurement for this on the collection_log row,
    /// so the disclosure is a Warning, at most once an hour for each instance (by this source's clock, so a test can
    /// move it): a target that stays over the cap would otherwise repeat it every cycle. Without a logger there is
    /// nothing to say it to.
    /// </summary>
    private void WarnListingCapReached(string instanceId)
    {
        if (_logger is null)
        {
            return;
        }

        var now = _clock();
        var warned = ListingCapWarnedUtc.GetOrCreateValue(_logger);

        lock (warned)
        {
            /* #4732: a stamp ahead of the clock (it stepped back since the warning) is replaced by this reading. */
            if (PerformanceMonitor.Common.LastFiredStamp.TryGet(warned, instanceId, now, out var last) && now - last < ListingCapWarnInterval)
            {
                return;
            }

            warned[instanceId] = now;
        }

        _logger.LogWarning(
            "RDS instance '{InstanceId}' lists more PostgreSQL log files than {PageCap} pages of DescribeDBLogFiles return. "
            + "The log read used the files from those pages only, so it may open an older file than the newest one. "
            + "This is logged at most once an hour for each instance.",
            instanceId,
            MaxLogListingPages);
    }
}
