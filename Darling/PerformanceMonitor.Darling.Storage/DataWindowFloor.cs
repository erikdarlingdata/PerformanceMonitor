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
/// Where a read's coverage starts: the instant from which the store holds everything the table's collector wrote for
/// the servers in scope. A chart over 30 days of a table that keeps 7 draws 7 days; this is the instant its caller
/// compares with the window's start (<see cref="RawWindowFloor.IsTruncated"/>) to say so. The cause does not matter
/// here: retention and a server added last week look the same, and none is guessed.
///
/// <para><b>Why coverage, not the oldest row.</b> waiting_tasks and query_snapshots hold a row only while something
/// waits or runs. A server idle overnight, whose older rows the purge dropped, has its first row hours after a 7-day
/// window starts, and a server whose first waiting task came 3 days ago has none before that, though the store
/// covered both windows whole. So each server's coverage starts at the later of two instants, and its rows only
/// ever move that earlier:</para>
/// <list type="bullet">
/// <item><b>Its first collection:</b> the registry's <c>created_date</c>, the server's first successful connect. The
/// service writes it once and no purge touches <c>collect.servers</c>, so it outlives every table. collection_log is
/// not used for it: it keeps 60 days, and a fleet override can keep a table longer, so the log can forget a first
/// run the table still covers. Every collector runs after the first connect, so this never comes after the real
/// first collection; a collector turned on later reads as covered from the connect (no notice, never a false
/// one).</item>
/// <item><b>The table's retention edge:</b> the purge's cutoff, the probe's clock less the collector's fleet
/// retention days (the fleet override in <c>config.config_collector_schedules</c>, else
/// <see cref="CollectorScheduleDefaults"/>, as <see cref="DarlingRetentionHorizons.ResolveFleetRetentionDays"/>
/// resolves it). The purge applies one horizon to the whole table (per-server overrides never reach it), so this
/// is the same for every server, and it does not depend on a row or a chunk surviving near it. None for a rollup
/// (the tier notice measures each tier's reach), for the raw relations the gated purge owns, and for the
/// collectors whose purge horizon is floored at the baseline window: those read coverage from the first
/// collection and the rows alone.</item>
/// <item><b>Its first row in the window:</b> a row proves the store covered the server from that row on.</item>
/// </list>
///
/// <para><b>Which servers count.</b> A server counts when it holds a row in [start, end] or, for a collector
/// table, when collection_log records a run of the collector for it in [start, end]. A server stopped or removed
/// before the window (its registry row and history stay) does not count, so its old rows cannot hide a newer
/// server's late start on a fleet panel. The answer is the earliest coverage start among the servers that count,
/// and null when none counts: the window holds nothing, and an empty panel already says so.</para>
///
/// <para><b>Why per server, through <c>collect.servers</c>.</b> Every collect table is indexed on
/// <c>(server_id, time)</c>, every continuous aggregate a panel reads keeps <c>(server_id, bucket)</c>, and
/// collection_log keeps <c>(server_id, collector_name, collection_time)</c>. A LATERAL per registered server reads
/// the first index entry inside the window, so only the window's chunks are probed. Filtering the fact table by
/// <c>server_name</c>, as a panel's own read does, has no index to use.</para>
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
        private Source(string relation, string timeColumn, bool endExclusive, string? collectorName = null, int? retentionDefaultDays = null)
        {
            Relation = relation;
            TimeColumn = timeColumn;
            EndExclusive = endExclusive;
            CollectorName = collectorName;
            RetentionDefaultDays = retentionDefaultDays;
        }

        /// <summary>
        /// The collector that writes the table, whose runs collection_log records; null for a rollup, which no
        /// collector writes.
        /// </summary>
        public string? CollectorName { get; }

        /// <summary>
        /// The collector's default retention days, when the probe reads the table's retention edge from the
        /// schedule; null when the edge is not read (a rollup, a raw relation the gated purge owns, a collector
        /// whose purge horizon is floored at the baseline window).
        /// </summary>
        public int? RetentionDefaultDays { get; }

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

            /* The schedule's horizon is the purge's only where the daily sweep applies it as written: the gated
               purge owns the raw relations under TimescaleDB, and the baseline-serving collectors are floored at
               the baseline window. */
            int? retentionDefaultDays =
                !TimescaleSupport.RawRelations.Contains(schema.TargetTable)
                && !DarlingRetentionHorizons.BaselineServingRawCollectors.Contains(schema.Name)
                && CollectorScheduleDefaults.All.TryGetValue(schema.Name, out var schedule)
                    ? schedule.RetentionDays
                    : null;
            source = new Source(schema.TargetTable, schema.PrefixTimeColumnName, endExclusive: false, schema.Name, retentionDefaultDays);
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

        /// <summary>The servers named in <c>$4</c> (<c>text[]</c>): a panel scoped by name.</summary>
        ServerNames,

        /// <summary>The one server in <c>$4</c> (<c>int</c>): the desktop viewer's server tab.</summary>
        ServerId,
    }

    /// <summary>
    /// The probe over <paramref name="sources"/>: $1 the window's end, $2 its start, $3 the probe's clock (all naive
    /// UTC), $4 the scope's servers when <paramref name="scope"/> names any. Several sources (the two halves of a
    /// stitched rollup read) are probed side by side and the earliest answer wins.
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
            Scope.ServerNames => "s.server_name = ANY($4)",
            Scope.ServerId => "s.server_id = $4",
            _ => throw new ArgumentOutOfRangeException(nameof(scope), scope, "unknown data-start scope"),
        };

        var schema = PgSchemaGenerator.CollectSchema;
        var sql = new StringBuilder();
        sql.Append("SELECT MIN(u.t)\nFROM\n(\n");
        for (var i = 0; i < sources.Count; i++)
        {
            var source = sources[i];
            if (i > 0)
            {
                sql.Append("    UNION ALL\n");
            }

            /* The collector name and the default days come from the collector catalog and its schedule table,
               never from a caller, so splicing them is safe (Source's factories are the only constructors). */
            var collector = source.CollectorName?.ToLowerInvariant();
            var coverage = source.RetentionDefaultDays is int days && collector is not null
                ? $"GREATEST($3 - make_interval(days => COALESCE((SELECT o.retention_days FROM config.config_collector_schedules AS o WHERE o.server_id IS NULL AND lower(o.collector_name) = '{collector}' AND o.retention_days >= 1 LIMIT 1), {days.ToString(System.Globalization.CultureInfo.InvariantCulture)})), s.created_date)"
                : "s.created_date";

            sql.Append("    SELECT LEAST(").Append(coverage).Append(", w.t) AS t\n");
            sql.Append("    FROM ").Append(schema).Append(".servers AS s\n");
            sql.Append("    LEFT JOIN LATERAL\n");
            sql.Append("    (\n");
            sql.Append("        SELECT f.").Append(source.TimeColumn).Append(" AS t\n");
            sql.Append("        FROM ").Append(schema).Append('.').Append(source.Relation).Append(" AS f\n");
            sql.Append("        WHERE f.server_id = s.server_id\n");
            sql.Append("        AND   f.").Append(source.TimeColumn).Append(" >= $2\n");
            sql.Append("        AND   f.").Append(source.TimeColumn).Append(source.EndExclusive ? " < $1\n" : " <= $1\n");
            sql.Append("        ORDER BY f.").Append(source.TimeColumn).Append('\n');
            sql.Append("        LIMIT 1\n");
            sql.Append("    ) AS w ON TRUE\n");
            sql.Append("    WHERE (w.t IS NOT NULL");
            if (collector is not null)
            {
                sql.Append("\n           OR EXISTS (SELECT 1 FROM ").Append(schema).Append(".collection_log AS c WHERE c.server_id = s.server_id AND c.collector_name = '")
                    .Append(collector).Append("' AND c.collection_time >= $2 AND c.collection_time <= $1)");
            }

            sql.Append(")\n");
            if (scopeClause is not null)
            {
                sql.Append("    AND   ").Append(scopeClause).Append('\n');
            }
        }

        sql.Append(") AS u");
        return sql.ToString();
    }

    /// <summary>
    /// Where coverage starts for the servers named in <paramref name="serverNames"/>, or for every registered server
    /// when it is null or empty: the earliest coverage start among the servers that hold a row, or logged a run, in
    /// [<paramref name="startUtc"/>, <paramref name="endUtc"/>]. At or before the start means the window is covered,
    /// whether or not it holds rows. Null when no server in scope counts, which the caller reads as "nothing to
    /// report": an empty panel already says so.
    /// </summary>
    public static async Task<DateTime?> GetAsync(
        NpgsqlDataSource postgres, IReadOnlyList<Source> sources, IReadOnlyList<string>? serverNames, DateTime startUtc, DateTime endUtc,
        int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var scoped = serverNames is { Count: > 0 };
        await using var command = postgres.CreateCommand(FloorSql(sources, scoped ? Scope.ServerNames : Scope.Fleet));
        command.CommandTimeout = commandTimeoutSeconds;
        AddWindow(command, startUtc, endUtc);
        if (scoped)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = serverNames!.ToArray() });
        }

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime dt ? dt : null;
    }

    /// <summary>
    /// Where coverage of <paramref name="source"/> starts for one server: the desktop viewer's server-tab form of
    /// <see cref="GetAsync"/>, with the same answers.
    /// </summary>
    public static async Task<DateTime?> GetForServerAsync(
        NpgsqlDataSource postgres, Source source, int serverId, DateTime startUtc, DateTime endUtc,
        int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(source);

        await using var command = postgres.CreateCommand(FloorSql([source], Scope.ServerId));
        command.CommandTimeout = commandTimeoutSeconds;
        AddWindow(command, startUtc, endUtc);
        command.Parameters.AddWithValue(serverId);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime dt ? dt : null;
    }

    /* SpecifyKind(Unspecified), not the bare value: Npgsql infers timestamptz from Kind=Utc and PostgreSQL then
       zone-shifts the bounds against the store's NAIVE timestamp columns (RawWindowFloor.GetAsync says the same). */
    private static void AddWindow(NpgsqlCommand command, DateTime startUtc, DateTime endUtc)
    {
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
    }
}
