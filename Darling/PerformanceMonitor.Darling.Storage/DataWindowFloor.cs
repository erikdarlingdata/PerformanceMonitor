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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Where a read's data really starts: the oldest row each server in scope holds at or before the window's end, and
/// the oldest of those. A chart over 30 days of a table that keeps 7 draws 7 days; this is the instant its caller
/// compares with the window's start (<see cref="RawWindowFloor.IsTruncated"/>) to say so. The cause does not matter
/// here: retention, a purge that ran late, and a server added last week all look the same, and none is guessed.
///
/// <para><b>Why unbounded below.</b> <see cref="RawWindowFloor"/> asks for the oldest row INSIDE the window, which
/// is right for the dense per-collection tables it serves. On a sparse table it is wrong: waiting_tasks holds a row
/// only while something waits, and deadlocks only when one happens. A quiet first hour, or a week with one deadlock
/// near its end, puts the window's own oldest row far past its start while the store still holds older rows for the
/// same server, so nothing is missing. Asking for the oldest row at or before the window's end answers "does the
/// data reach back to the start" for dense and sparse tables alike.</para>
///
/// <para><b>Why per server, through <c>collect.servers</c>.</b> Every collect table is indexed on
/// <c>(server_id, time)</c>, and every continuous aggregate a panel reads keeps <c>(server_id, bucket)</c>. A
/// LATERAL per registered server reads the first index entry of each, one descent per chunk until a chunk holds
/// that server's rows. Filtering the fact table by <c>server_name</c>, as a panel's own read does, has no index to
/// use and reads every row the table holds below the window's end.</para>
///
/// <para><b>Why this lives in Storage.</b> The web viewer's Custom Views runner and the desktop viewer's server tab
/// both ask it, and the viewer does not reference the service assembly (#1661 / #2530), the same reason
/// <see cref="RawWindowFloor"/> lives here.</para>
/// </summary>
public static class DataWindowFloor
{
    /// <summary>
    /// One relation the probe reads, with the column its rows are stamped on. Built only by the factories, which
    /// check the name against the collector catalog or the rollup registry, so nothing outside those two lists is
    /// ever spliced into the probe's SQL.
    /// </summary>
    public sealed class Source
    {
        private Source(string relation, string timeColumn, bool endExclusive)
        {
            Relation = relation;
            TimeColumn = timeColumn;
            EndExclusive = endExclusive;
        }

        /// <summary>The unqualified <c>collect.*</c> relation name.</summary>
        public string Relation { get; }

        /// <summary>The column the probe orders and bounds on.</summary>
        public string TimeColumn { get; }

        /// <summary>
        /// True for a rollup: a bucket is stamped at its START, so a bucket at the window's end lies after the
        /// window, the same end-exclusive rule the Custom Views compiler applies to a rollup read.
        /// </summary>
        public bool EndExclusive { get; }

        /// <summary>
        /// The raw collector table <paramref name="table"/>, or false when the catalog does not list it or its
        /// index does not lead with <c>(server_id, time)</c> (<see cref="PgSchemaGenerator.CreateIndex"/>). The
        /// config snapshots have no index, and index_object_stats orders a server's rows by object before time,
        /// so the probe would read every row the server holds; neither is probed.
        /// </summary>
        public static bool TryForCollectorTable(string table, out Source source)
        {
            source = null!;
            var schema = CollectorCatalog.All.FirstOrDefault(c => string.Equals(c.TargetTable, table, StringComparison.Ordinal));
            if (schema is null
                || PgSchemaGenerator.CreateIndex(schema)?.EndsWith($"(server_id, {schema.PrefixTimeColumnName});", StringComparison.Ordinal) != true)
            {
                return false;
            }

            source = new Source(schema.TargetTable, schema.PrefixTimeColumnName, endExclusive: false);
            return true;
        }

        /// <summary><see cref="TryForCollectorTable"/>, throwing for a table it refuses.</summary>
        public static Source ForCollectorTable(string table) =>
            TryForCollectorTable(table, out var source)
                ? source
                : throw new ArgumentException($"'{table}' is not a collector table the data-start probe can read.", nameof(table));

        /// <summary>
        /// The continuous aggregate <paramref name="view"/>, or false when <see cref="RollupAvailability"/> does not
        /// know the name. Every rollup a panel reads keeps a <c>(server_id, bucket)</c> index.
        /// </summary>
        public static bool TryForRollup(string view, out Source source)
        {
            source = null!;
            if (string.IsNullOrEmpty(view) || !RollupAvailability.All.Has(view))
            {
                return false;
            }

            source = new Source(view, "bucket", endExclusive: true);
            return true;
        }
    }

    /// <summary>Which servers the probe asks about.</summary>
    public enum Scope
    {
        /// <summary>Every registered server: a fleet-wide panel.</summary>
        Fleet,

        /// <summary>The servers named in <c>$2</c> (<c>text[]</c>): a panel scoped by name.</summary>
        ServerNames,

        /// <summary>The one server in <c>$2</c> (<c>int</c>): the desktop viewer's server tab.</summary>
        ServerId,
    }

    /// <summary>
    /// The probe over <paramref name="sources"/>: $1 the window's end (naive UTC), $2 the scope's servers when
    /// <paramref name="scope"/> names any. Several sources (the two halves of a stitched rollup read) are probed
    /// side by side and the oldest answer wins.
    /// </summary>
    public static string FloorSql(IReadOnlyList<Source> sources, Scope scope)
    {
        ArgumentNullException.ThrowIfNull(sources);
        if (sources.Count == 0)
        {
            throw new ArgumentException("the data-start probe needs at least one source.", nameof(sources));
        }

        var scopeClause = scope switch
        {
            Scope.Fleet => null,
            Scope.ServerNames => "WHERE s.server_name = ANY($2)",
            Scope.ServerId => "WHERE s.server_id = $2",
            _ => throw new ArgumentOutOfRangeException(nameof(scope), scope, "unknown data-start scope"),
        };

        var sql = new StringBuilder();
        sql.Append("SELECT MIN(u.t)\nFROM\n(\n");
        for (var i = 0; i < sources.Count; i++)
        {
            var source = sources[i];
            if (i > 0)
            {
                sql.Append("    UNION ALL\n");
            }

            sql.Append("    SELECT l.t\n");
            sql.Append("    FROM collect.servers AS s\n");
            sql.Append("    CROSS JOIN LATERAL\n");
            sql.Append("    (\n");
            sql.Append("        SELECT f.").Append(source.TimeColumn).Append(" AS t\n");
            sql.Append("        FROM ").Append(PgSchemaGenerator.CollectSchema).Append('.').Append(source.Relation).Append(" AS f\n");
            sql.Append("        WHERE f.server_id = s.server_id\n");
            sql.Append("        AND   f.").Append(source.TimeColumn).Append(source.EndExclusive ? " < $1\n" : " <= $1\n");
            sql.Append("        ORDER BY f.").Append(source.TimeColumn).Append('\n');
            sql.Append("        LIMIT 1\n");
            sql.Append("    ) AS l\n");
            if (scopeClause is not null)
            {
                sql.Append("    ").Append(scopeClause).Append('\n');
            }
        }

        sql.Append(") AS u");
        return sql.ToString();
    }

    /// <summary>
    /// Where the data in <paramref name="sources"/> starts for the servers named in <paramref name="serverNames"/>,
    /// or for every registered server when it is null or empty. Null when none of them holds a row at or before
    /// <paramref name="endUtc"/>, which the caller reads as "nothing to report": an empty panel already says so.
    /// </summary>
    public static async Task<DateTime?> GetAsync(
        NpgsqlDataSource postgres, IReadOnlyList<Source> sources, IReadOnlyList<string>? serverNames, DateTime endUtc,
        int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var scoped = serverNames is { Count: > 0 };
        await using var command = postgres.CreateCommand(FloorSql(sources, scoped ? Scope.ServerNames : Scope.Fleet));
        command.CommandTimeout = commandTimeoutSeconds;
        AddEnd(command, endUtc);
        if (scoped)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = serverNames!.ToArray() });
        }

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime dt ? dt : null;
    }

    /// <summary>
    /// Where the data in <paramref name="source"/> starts for one server: the desktop viewer's server-tab form of
    /// <see cref="GetAsync"/>.
    /// </summary>
    public static async Task<DateTime?> GetForServerAsync(
        NpgsqlDataSource postgres, Source source, int serverId, DateTime endUtc,
        int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(source);

        await using var command = postgres.CreateCommand(FloorSql([source], Scope.ServerId));
        command.CommandTimeout = commandTimeoutSeconds;
        AddEnd(command, endUtc);
        command.Parameters.AddWithValue(serverId);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime dt ? dt : null;
    }

    /* SpecifyKind(Unspecified), not the bare value: Npgsql infers timestamptz from Kind=Utc and PostgreSQL then
       zone-shifts the bound against the store's NAIVE timestamp columns (RawWindowFloor.GetAsync says the same). */
    private static void AddEnd(NpgsqlCommand command, DateTime endUtc) =>
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
}
