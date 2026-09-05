/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// Stores <c>auto_explain</c> plans fetched from the RDS log API into <c>collect.pg_plan_capture</c>
/// (#2538) — the managed-PostgreSQL half of plan capture.
///
/// <para><b>Why this is not a collector.</b> Every collector in this product is
/// <c>BuildQuery</c> → <c>DbDataReader</c> → rows. There is no reader here: the text arrives from an AWS
/// API over HTTPS, with no database connection to the target involved at all. Forcing that through a
/// SQL-shaped framework would mean a fake reader wrapping an HTTP response, which is more machinery and
/// less honesty than a small component that says what it is.</para>
///
/// <para><b>What it deliberately does NOT duplicate.</b> Parsing, redaction and hashing come from
/// <c>PgPlanLogParser</c>, shared with the <c>pg_read_file</c> route — the redaction living in one place is
/// the whole reason that type exists. The WRITE goes through <c>PgCollectorRowWriter</c> and
/// <c>PgPlanCaptureCollector</c>'s own definition, so the column order, the COPY command and the standard
/// prefix are the collector's rather than a second opinion about them. This adds a source, not a schema.</para>
///
/// <para><b>Failure is per target and non-fatal.</b> A missing IAM permission, a cluster mid-failover, or a
/// reader endpoint someone pointed at by mistake are all ordinary states, and none of them should stop the
/// other targets — or the rest of the cycle — from collecting.</para>
/// </summary>
public sealed class RdsPlanIngestor
{
    private readonly NpgsqlDataSource _postgres;
    private readonly RdsLogSource _logs;
    private readonly ILogger? _logger;

    public RdsPlanIngestor(NpgsqlDataSource postgres, RdsLogSource? logs = null, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logs = logs ?? new RdsLogSource();
        _logger = logger;
    }

    /// <param name="host">The target's connection host. A non-RDS host means this transport does not apply
    /// to that target — it uses the <c>pg_read_file</c> route instead — and the outcome says so rather than
    /// reporting an empty log (#3017).</param>
    /// <returns>Rows stored and whether the log was reached at all. Reaching it and finding nothing is a
    /// real statement about the log; not reaching it is not, and
    /// <see cref="RdsIngestOutcome.SourceReached"/> is what keeps the runner from making one.</returns>
    public async Task<RdsIngestOutcome> IngestAsync(
        int serverId,
        string storageName,
        string host,
        CancellationToken cancellationToken = default)
    {
        RdsLogSource.LogChunk? chunk;

        try
        {
            chunk = await _logs.ReadNewestAsync(host, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2633: RETHROWN, not returned as zero rows. The warning below stays — it names the target and
               carries the AWS message — but the app log is not where collection health is read. Returning 0
               made the runner write SUCCESS with "no new auto_explain plans in the RDS log window", which
               is a claim that the log was opened. On the monitoring host the truth was
               rds:DescribeDBLogFiles denied: nothing was opened at all, and the row said the collector was
               fine.

               Still tolerated at the cycle level — the runner degrades an authorization refusal to
               PERMISSIONS and moves on, exactly as the pg_read_file route already does for its own 42501.
               What changes is that the cycle now says WHICH kind of nothing it found. */
            _logger?.LogWarning(
                "RDS plan log unavailable for {Server}: {Message} — plan capture is skipped for this target "
                + "this cycle; every other collector is unaffected.",
                storageName, ex.Message);

            throw new RdsLogUnavailableException(
                ex.Message, RdsLogUnavailableException.IsAuthorizationRefusal(ex), ex);
        }

        if (chunk is null)
        {
            /* #3017: NOT_REACHED, not zero rows — the same distinction, and the same single cause, as
               RdsDeadlockIngestor. ReadNewestAsync answers null only when RdsEndpoint.TryParse declined the
               host, which means no AWS call was made and nothing is known about the log. */
            return RdsIngestOutcome.NotReached;
        }

        var written = await StoreAsync(serverId, storageName, chunk.Value.Text, cancellationToken);

        /* THE MARKER MOVES HERE AND NOWHERE ELSE — the same order, and for the same reason, as
           RdsDeadlockIngestor (#3008). Reaching this line means everything the chunk held is either in the
           store or was nothing to store; anything else threw out of StoreAsync and left the marker where it
           was, so the next cycle asks RDS for the same window again rather than resuming past it.

           Plan rows dedup on (queryid, plan_hash), so the repeat this can cause costs a re-store of shapes
           the store already has. The loss it replaces was unbounded and silent. */
        _logs.CommitResume(chunk.Value.Resume);

        return RdsIngestOutcome.Read(written);
    }

    /// <summary>
    /// Parse a chunk and store what it held, or throw. Split out so the resume marker has exactly one
    /// commit point above it: every way this can decline to store rows — empty text, a slab no plan
    /// threshold was crossed in — is a legitimate zero that loses nothing, and every way it can FAIL leaves
    /// via an exception rather than a zero the caller would have to tell apart from those.
    /// </summary>
    private async Task<int> StoreAsync(
        int serverId,
        string storageName,
        string text,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(text))
        {
            return 0;
        }

        var plans = PgPlanLogParser.Extract(text);

        if (plans.Count == 0)
        {
            /* A log slab with no plans in it is the ordinary case on a server whose threshold nothing
               crossed. Not worth a log line every cycle. */
            return 0;
        }

        var rows = new List<PgPlanCaptureCollector.Row>(plans.Count);

        foreach (var plan in plans)
        {
            rows.Add(new PgPlanCaptureCollector.Row(
                QueryId: plan.QueryId,
                PlanHash: plan.PlanHash,
                DurationMs: plan.DurationMs,
                NodeCount: plan.NodeCount,
                TopNodeType: plan.TopNodeType,
                PlanJson: plan.PlanJson));
        }

        return await WriteAsync(serverId, storageName, rows, cancellationToken);
    }

    /// <summary>
    /// The same binary COPY the collector runner uses, driven by the collector's own definition — so the
    /// column order and COPY command come from one place and cannot drift from the table.
    /// </summary>
    private async Task<int> WriteAsync(
        int serverId,
        string storageName,
        IReadOnlyList<PgPlanCaptureCollector.Row> rows,
        CancellationToken cancellationToken)
    {
        var definition = PgPlanCaptureCollector.Instance;

        /* Naive UTC, the store's convention for every collector timestamp: the columns are `timestamp`
           without a zone, and letting Kind=Utc through makes Npgsql infer timestamptz and shift the value
           by the store session's offset. */
        var collectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

        var writer = new PgCollectorRowWriter();
        var written = 0;

        using (var importer = await connection.BeginBinaryImportAsync(
            PgCollectorRowWriter.CopyCommandFor(definition), cancellationToken))
        {
            /* #2874: the COPY's own deadline, on NpgsqlBinaryImporter.Timeout — a TimeSpan on a different type
               from the rest of the regime, invisible to a command-shaped regex, and inherited from the
               connection's CommandTimeout (30 s) when left unset. Same constant, same regime, and the same
               narrows-not-closes caveat about the Begin phase as DarlingCollectorRunner.WriteBatchAsync. */
            importer.Timeout = TimeSpan.FromSeconds(ServiceCommandDeadlines.CollectionSweepSeconds);

            writer.Importer = importer;

            foreach (var row in rows)
            {
                await importer.StartRowAsync(cancellationToken);

                if (definition.IncludesCollectionId)
                {
                    writer.Value(CollectionIdGenerator.Next());
                }

                writer.Value(collectionTime)
                      .Value(serverId)
                      .Value(storageName);

                writer.BeginPayload();
                definition.WritePayload(row, writer, NullContext(serverId, storageName, collectionTime));
                writer.EndPayload(definition.PayloadColumns.Count);
                written++;
            }

            await importer.CompleteAsync(cancellationToken);
        }

        _logger?.LogInformation(
            "Stored {Count} auto_explain plan(s) for {Server} from the RDS log API.", written, storageName);

        return written;
    }

    /* WritePayload takes a context for the collectors that consult deltas or watermarks. This one reads
       none of it - the rows are already fully formed by the parser - so the context exists to satisfy the
       signature rather than to carry anything. */
    private static CollectorContext NullContext(int serverId, string storageName, DateTime collectionTime)
        => new()
        {
            ServerId = serverId,
            ServerName = storageName,
            CollectionTime = collectionTime,
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        };
}
