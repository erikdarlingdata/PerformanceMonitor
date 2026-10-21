/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// The lead-blocker tree of one blocked-process row for the web (#6292), built exactly as the Viewer builds it
/// (<c>ViewerServerTab.BlockChain.cs</c>): the same pair-row read around the row's own event time, the same
/// reconstruction (<see cref="BlockingChainReconstructor"/> scoped by monitor_loop, so two scans never merge), the same
/// pick of the ONE chain that holds the clicked session, and the same shared <see cref="BlockingChainTreeBuilder"/>.
/// There is no second tree builder: the page draws the model this returns.
/// </summary>
internal static class PgBlockChainRead
{
    /// <summary>Minutes either side of the clicked row's event time the pair rows are read from (the Viewer's window).</summary>
    public const int ChainWindowMinutes = 5;

    /// <summary>The Viewer's reconstruction limits.</summary>
    public const int MaxDepth = 50, MaxPairs = 5000, StepBudget = 100_000;

    /// <summary>The chain's model, or null when the session is in no reconstructed chain in the window.</summary>
    public static async Task<BlockingChainModel?> ReadModelAsync(
        NpgsqlDataSource postgres, int serverId, DateTime eventTimeUtc, int spid, int ecid, int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        var start = eventTimeUtc.AddMinutes(-ChainWindowMinutes);
        var end = eventTimeUtc.AddMinutes(ChainWindowMinutes);
        var rows = new List<BlockingPairRow>();

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        await using (var command = connection.CreateCommand())
        {
            command.CommandTimeout = commandTimeoutSeconds;
            command.CommandText = PgBlockingPairRowQuery.WindowRowsSql;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(start) });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                rows.Add(PgBlockingPairRowQuery.Read(reader));
        }

        /* The always-on DMV snapshot stands in when the blocked-process-report event captured nothing. */
        await PgBlockingPairRowQuery.AppendDmvSnapshotRowsAsync(
            () =>
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = commandTimeoutSeconds;
                return command;
            },
            rows, serverId, start, end, cancellationToken);

        /* The clicked row's monitor_loop scopes the match, as the Viewer's row carries it. The web row does not, so it is found
           here by its own key (event time, blocked session); a row that cannot be found falls back to the Viewer's
           monitor_loop-agnostic match. The lowest monitor_loop wins a tie so the answer does not depend on read order. */
        var monitorLoop = rows
            .Where(r => r.EventTime == eventTimeUtc && r.BlockedSpid == spid && r.BlockedEcid == ecid && r.MonitorLoop.HasValue)
            .Select(r => r.MonitorLoop)
            .OrderBy(m => m)
            .FirstOrDefault();

        var reconstruction = BlockingChainReconstructor.Reconstruct(
            rows, MaxDepth, MaxPairs, StepBudget, scopeByMonitorLoop: true);
        var chain = BlockingChainReconstructor.FindChainForSession(reconstruction, monitorLoop, spid, ecid);
        if (chain == null)
            return null;

        return BlockingChainTreeBuilder.Build(
            new[] { ToInput(chain) },
            reconstruction.CycleDetected,
            reconstruction.DepthCapped,
            reconstruction.TraversalTruncated);
    }

    private static BlockingChainInput ToInput(ReconstructedChain c) => new()
    {
        ApexSpid = c.ApexSpid,
        ApexEcid = c.ApexEcid,
        ApexTranStarted = c.ApexTranStarted,
        ApexSleeping = c.ApexSleeping,
        Magnitude = c.Magnitude,
        Edges = c.Levels.Select(static l => new BlockingEdgeInput
        {
            Level = l.Level,
            BlockingSpid = l.BlockingSpid,
            BlockingEcid = l.BlockingEcid,
            BlockingTranStarted = l.BlockingTranStarted,
            BlockedSpid = l.BlockedSpid,
            BlockedEcid = l.BlockedEcid,
            BlockedTranStarted = l.BlockedTranStarted,
            LockMode = l.LockMode,
            WaitTimeMs = l.WaitTimeMs,
            DatabaseName = l.DatabaseName,
            BlockingSqlText = l.BlockingSqlText,
            BlockedSqlText = l.BlockedSqlText,
            BlockedLoginName = l.BlockedLoginName,
            BlockedHostName = l.BlockedHostName,
            BlockedClientApp = l.BlockedClientApp,
            BlockingLoginName = l.BlockingLoginName,
            BlockingHostName = l.BlockingHostName,
            BlockingClientApp = l.BlockingClientApp,
            ContentiousObject = l.ContentiousObject
        }).ToList()
    };
}
