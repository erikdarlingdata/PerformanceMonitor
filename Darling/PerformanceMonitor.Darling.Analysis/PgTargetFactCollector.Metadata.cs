/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The registry's two PostgreSQL facts for one server — the major (V100, #2653) and the engine KIND (V82,
    /// #2530). The same shape the MCP capability helper reads; declared here because this assembly cannot
    /// reference the service's. <c>$1</c> server_id. Reads <c>servers</c>, the one non-collector table an
    /// analysis read may name.
    /// </summary>
    public const string PgTargetServerMetadataSql = @"
SELECT postgres_major_version, engine_kind
FROM servers
WHERE server_id = $1";

    /// <summary>
    /// The PostgreSQL twin of <c>CollectServerMetadataFactsAsync</c>'s <c>SERVER_MAJOR_VERSION</c>: one
    /// point-in-time context fact, <see cref="PgTargetFactKeys.ServerMajorVersion"/>, Value = the probed major,
    /// metadata <c>is_aurora</c> decoded from the registry kind through <see cref="MonitoredEngineKind"/> rather
    /// than by string comparison. Base severity 0 — it roots nothing and amplifies nothing; it is the fact the
    /// D6 flavour disclosure ("CPU: not collected on this flavour") will hang on, and until the content lanes
    /// land it is the one fact a skeleton pass can honestly emit.
    ///
    /// <para>Emitted only when the registry KNOWS the major: a NULL is a row no connect has stamped since V100
    /// landed, and it makes no claim (#2530) — rendering it as version 0 would be the "fabricated zero" the
    /// #3541 A12 sweep just finished removing elsewhere.</para>
    /// </summary>
    private async partial Task CollectServerMetadataFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetServerMetadataSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            if (reader.IsDBNull(0)) return;
            var major = Convert.ToInt32(reader.GetValue(0));
            if (major <= 0) return;

            var kind = reader.IsDBNull(1) ? null : reader.GetString(1);
            facts.Add(new Fact
            {
                Source = PgTargetSources.ConfigSource,
                Key = PgTargetFactKeys.ServerMajorVersion,
                Value = major,
                ServerId = context.ServerId,
                Metadata = { ["is_aurora"] = MonitoredEngineKind.IsAurora(kind) ? 1 : 0 },
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* postgres_major_version arrived in V100 — a pre-migration store raises 42703 here, which the
               reporter classifies quiet. An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }
}
