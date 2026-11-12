/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
/// The binary COPY the RDS ingestors (log events, auto_explain plans, deadlocks, Performance Insights CPU)
/// share, driven by the collector's own definition so the column order and COPY command come from one place
/// and cannot drift from the table (#7222). It was four character-identical copies, one per ingestor; the
/// deadline-and-phase discipline in it (#2874, #3008) now lives here once.
///
/// <para>What stays with each ingestor on purpose: the definition it passes, the log wording, and the commit
/// that follows the write. The commit blocks differ (the deadlock ingestor commits a second carry, and the
/// tuples each <c>StoreAsync</c> returns have different shapes), so only the write is shared.</para>
/// </summary>
internal static class RdsCopyWriter
{
    public static async Task<int> WriteAsync<TRow>(
        PostgresCollectorDefinitionBase<TRow> definition,
        NpgsqlDataSource postgres,
        ILogger? logger,
        int serverId,
        string storageName,
        IReadOnlyList<TRow> rows,
        string storedWhat,
        string storedFrom,
        CancellationToken cancellationToken)
    {
        /* Naive UTC, the store's convention for every collector timestamp: the columns are `timestamp`
           without a zone, and letting Kind=Utc through makes Npgsql infer timestamptz and shift the value
           by the store session's offset. */
        var collectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        var writer = new PgCollectorRowWriter();
        var written = 0;

        /* The start phase's deadline. It is separate from the importer's because the importer does not
           exist until Begin returns, and the await that returns it runs under the connection's
           CommandTimeout, which Npgsql exposes read-only. StoreCopyStartDeadline carries the value, the
           stop-versus-deadline discrimination and the fault shape — see it for why a breach is re-raised
           as a TimeoutException rather than left as the cancellation Npgsql threw. */
        using var startDeadline = StoreCopyStartDeadline.Start(cancellationToken);

        /* Which COPY phase a fault came out of, on the same terms as
           DarlingCollectorRunner.CopyBatchOnceAsync — see CollectorFaultCopyPhase, which is the authority
           for what each value means and what it authorises.

           Start until Begin returns, and the transition sits INSIDE the block for that reason: Start has to
           mean strictly "the importer never came back". A fault anywhere past that line may have sent rows,
           so it must read as Data even where it happens to have sent none. */
        var copyPhase = StoreCopyPhase.Start;

        try
        {
            using (var importer = await connection.BeginBinaryImportAsync(
                PgCollectorRowWriter.CopyCommandFor(definition), startDeadline.Token))
            {
                copyPhase = StoreCopyPhase.Data;

                /* #2874: the COPY's own deadline, on NpgsqlBinaryImporter.Timeout — a TimeSpan on a different type
                   from the rest of the regime, invisible to a command-shaped regex, and inherited from the
                   connection's CommandTimeout (30 s) when left unset. Same constant, same regime, and it reaches
                   the row loop only — startDeadline above bounds the Begin that returns the importer, on the same
                   terms as DarlingCollectorRunner.CopyBatchOnceAsync. */
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
        }
        /* The start phase's deadline, re-raised as the shape a client-side deadline has here, on the same
           terms as DarlingCollectorRunner.CopyBatchOnceAsync. A throw from a catch arm leaves the whole
           try, so this fault is stamped HERE rather than by the arm below; the phase term in the filter is
           what keeps the arm unreachable once the row loop has begun, whatever Npgsql throws from inside
           it, so a data-phase fault cannot be relabelled as the one a re-attempt trusts. */
        catch (OperationCanceledException cancellation)
            when (copyPhase == StoreCopyPhase.Start && startDeadline.Breached())
        {
            var breach = StoreCopyStartDeadline.Breach(cancellation);
            CollectorFaultCopyPhase.Stamp(breach, copyPhase);
            throw breach;
        }
        /* Stamped, then rethrown bare, for CopyBatchOnceAsync's reason: the fault keeps its own type,
           message and inner chain, so every classification arm upstream sees exactly what it sees without
           this. OperationCanceledException is excluded because a stopping token says the service is shutting
           down, not which protocol exchange was in flight. */
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            CollectorFaultCopyPhase.Stamp(ex, copyPhase);
            throw;
        }

        logger?.LogInformation(
            "Stored {Count} {What} for {Server} from {Source}.", written, storedWhat, storageName, storedFrom);

        return written;
    }

    /* WritePayload takes a context for the collectors that consult deltas or watermarks. The RDS ingestors
       read none of it - their rows are already fully formed by the parser or the API response - so the
       context exists to satisfy the signature rather than to carry anything. */
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
