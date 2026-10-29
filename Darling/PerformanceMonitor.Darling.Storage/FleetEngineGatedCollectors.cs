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
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #6912: the collectors a server's engine does not run, by server, for the collection-health count surfaces. A gated collector
/// writes no new row, so the rows it logged before the gate existed (the 315 SESSION_MISSING runs an Amazon RDS Web or Express
/// target holds for <c>attention</c> after #6893) stay inside the seven-day window and band it FAILING. The answer is the same
/// gate that makes the collector's read answer <c>not_collected</c>, <see cref="CollectorEngineCapability.NotCollectedMessage"/>,
/// so the health list, the fleet card, the Viewer card and the status bar cannot disagree with the reads.
/// </summary>
public static class FleetEngineGatedCollectors
{
    /// <summary>The registry's engine facts for every server, the columns <c>DarlingEngineCapability.ServerEngineSql</c> reads for one.</summary>
    public const string Sql = @"
SELECT server_id, sql_engine_edition, engine_kind, is_aws_rds, is_aws_rds_web_or_express
FROM servers";

    /// <summary>
    /// The catalog collectors that do not run on a server with these engine facts. Empty when the facts make no claim (an unknown
    /// edition, no kind, no RDS fact), so a server nobody has connected to yet is judged by its log as before.
    /// </summary>
    public static HashSet<string> For(int engineEdition, string? engineKind, AwsRdsKind awsRdsKind)
    {
        var gated = new HashSet<string>(StringComparer.Ordinal);
        foreach (var definition in CollectorCatalog.All)
        {
            if (CollectorEngineCapability.NotCollectedMessage(string.Empty, engineEdition, engineKind, definition.Name, awsRdsKind) is not null)
            {
                gated.Add(definition.Name);
            }
        }

        return gated;
    }

    /// <summary>
    /// The gated collectors of every server that has any, read in one statement. A failed read answers none, as
    /// <see cref="FleetAlwaysOnOff"/> does: with no evidence the log-driven bands stand.
    /// </summary>
    public static async Task<Dictionary<int, HashSet<string>>> ReadAsync(NpgsqlDataSource postgres, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var byServer = new Dictionary<int, HashSet<string>>();
        try
        {
            await using var command = postgres.CreateCommand(Sql);
            command.CommandTimeout = commandTimeoutSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var edition = reader.IsDBNull(1) ? CollectorEngineCapability.UnknownEngineEdition : reader.GetInt32(1);
                var kind = reader.IsDBNull(2) ? null : reader.GetString(2);
                var rds = CollectorEngineCapability.AwsRdsKindOf(!reader.IsDBNull(3) && reader.GetBoolean(3), !reader.IsDBNull(4) && reader.GetBoolean(4));
                var gated = For(edition, kind, rds);
                if (gated.Count > 0)
                {
                    byServer[reader.GetInt32(0)] = gated;
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            byServer.Clear();
        }

        return byServer;
    }
}
