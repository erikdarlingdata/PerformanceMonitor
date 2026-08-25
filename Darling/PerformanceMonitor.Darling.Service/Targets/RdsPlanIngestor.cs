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

    /// <param name="host">The target's connection host. A non-RDS host is skipped silently — that target
    /// uses the <c>pg_read_file</c> route instead, and there is nothing to report.</param>
    /// <returns>Rows stored, or zero when this target is not RDS or had nothing new.</returns>
    public async Task<int> IngestAsync(
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
            /* Warn, not Error, and the message names the target. An IAM gap or a failing-over cluster is a
               state an operator can act on; taking the cycle down for it would be the wrong trade. */
            _logger?.LogWarning(
                "RDS plan log unavailable for {Server}: {Message} — plan capture is skipped for this target "
                + "this cycle; every other collector is unaffected.",
                storageName, ex.Message);
            return 0;
        }

        if (chunk is null || string.IsNullOrEmpty(chunk.Value.Text))
        {
            return 0;
        }

        var plans = PgPlanLogParser.Extract(chunk.Value.Text);

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
