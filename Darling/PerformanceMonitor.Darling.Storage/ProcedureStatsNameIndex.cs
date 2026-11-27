/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #8570: <c>idx_procedure_stats_server_db_name_time</c> on <c>collect.procedure_stats (server_id, database_name, the procedure's full
/// name, collection_time)</c>, the index the one-procedure duration trend seeks its procedure's rows through
/// (<see cref="DurationTrendRouting.BuildBucketedRawTrendSql"/> with <c>procedureRowsByName</c>, and the WHERE of
/// <see cref="DurationTrendRouting.BuildProcedureAvgCpuSql"/>). The third key is the expression the reads compare
/// (<see cref="DurationTrendRouting.ProcedureFullNameSql"/>), so the planner matches the predicate to the key. Every other index on the
/// table leads on the server and the time, or on the time alone, so without this one a read of one procedure fetches every row the
/// server stored in the window and keeps a few of them.
///
/// <para>#9280: the top-procedures read's mid route uses it the same way. Its wide pass seeks each winner's rows through this index on the
/// uncompressed chunks (<c>DarlingDataReader.TopProceduresMidSeekWideSql</c>) instead of reading every row the server wrote there, and
/// keeps its one-statement join where this index is missing.</para>
///
/// <para><b>What it holds.</b> A compressed chunk keeps no rows in a B-tree, so the index holds the uncompressed chunks' rows and its
/// size follows the uncompressed window, not the table. The read takes a compressed chunk through its batches, as every read of the
/// table does. The service's start path builds it before the collectors start (<see cref="PgTableTuning.Statements"/>), and
/// <see cref="IsPresentAsync"/> is how a reader learns that the store it is talking to has it.</para>
/// </summary>
public static class ProcedureStatsNameIndex
{
    /// <summary>The index's name, in the <c>collect</c> schema.</summary>
    public const string IndexName = "idx_procedure_stats_server_db_name_time";

    /// <summary>
    /// The idempotent build, on one line in the shape <see cref="PgTableTuning"/> reads an index's name and table from. Valid on a
    /// plain table and on a hypertable.
    /// </summary>
    public const string CreateSql =
        "CREATE INDEX IF NOT EXISTS " + IndexName + " ON " + PgSchemaGenerator.CollectSchema + ".procedure_stats (server_id, database_name, ("
        + DurationTrendRouting.ProcedureFullNameSql + "), collection_time)";

    /// <summary>
    /// One boolean: the index exists and is valid. <c>to_regclass</c> gives NULL for a name that resolves to nothing, so a store
    /// that has no such index answers false and raises nothing.
    /// </summary>
    public const string PresentSql =
        "SELECT COALESCE((SELECT i.indisvalid FROM pg_index AS i WHERE i.indexrelid = to_regclass('"
        + PgSchemaGenerator.CollectSchema + "." + IndexName + "')), FALSE)";

    /// <summary>Deadline for the catalog probe, in seconds.</summary>
    public const int ProbeTimeoutSeconds = 5;

    private static readonly object s_found = new();

    /// <summary>The data sources whose store was seen to have the index.</summary>
    private static readonly ConditionalWeakTable<NpgsqlDataSource, object> s_present = new();

    /// <summary>
    /// Whether the store behind <paramref name="postgres"/> has the index. A store that has it is remembered for the life of the data
    /// source: nothing in the service drops the index, and a read that expects it and finds it gone returns the same rows, slower.
    /// A store that lacks it is asked again on the next call, one catalog lookup, so an index built after the first read (by another
    /// instance's start, or by hand) is picked up without a restart. A probe that fails answers false, which is the read that needs no index.
    /// </summary>
    public static async ValueTask<bool> IsPresentAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        if (s_present.TryGetValue(postgres, out _))
        {
            return true;
        }

        try
        {
            await using var command = postgres.CreateCommand(PresentSql);
            command.CommandTimeout = ProbeTimeoutSeconds;
            if (await command.ExecuteScalarAsync(cancellationToken) is not true)
            {
                return false;
            }
        }
        catch (NpgsqlException) when (!cancellationToken.IsCancellationRequested)
        {
            return false;
        }

        s_present.TryAdd(postgres, s_found);
        return true;
    }
}
