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
/// <item><b>The table's edge:</b> where the table's own rows start. For a collector table the schedule governs
/// as written, it is the purge's cutoff: the probe's clock less the collector's fleet retention days (the fleet
/// override in <c>config.config_collector_schedules</c>, else <see cref="CollectorScheduleDefaults"/>, as
/// <see cref="DarlingRetentionHorizons.ResolveFleetRetentionDays"/> resolves it). The purge applies one horizon
/// to the whole table (per-server overrides never reach it), so this is the same for every server, and it does
/// not depend on a row or a chunk surviving near it. The schedule gives no edge to three kinds of source: a
/// rollup (the tier notice measures each tier's reach), the raw relations the gated purge owns, and the
/// collectors whose purge horizon is floored at the baseline window
/// (<see cref="DarlingRetentionHorizons.BaselineServingRawCollectors"/>). For those the edge is the oldest row
/// the server holds at or before the window's end, the rule this probe applied before it read coverage, and it
/// stands for the server's whole coverage (a row proves the store held the server from that row on; the first
/// collection serves only a server with no row at all). That is right for these tables because they are dense:
/// every collection, or every hour of a rollup, writes a row for a server that is running, so the oldest row is
/// where the table's coverage starts and a quiet start is not in play. Reading coverage from the first
/// collection alone would give an old server no notice at all, however long before anything the table keeps the
/// window starts; and on the web the tier notice covers a rollup route on a TimescaleDB store, but not these
/// collectors, and not a store without TimescaleDB, so these panels would lose the notice they had. That keeps
/// a walk to the table's oldest chunk (see <see cref="FloorSql"/>).</item>
/// <item><b>Its first row in the window:</b> a row proves the store covered the server from that row on.</item>
/// </list>
///
/// <para><b>Which servers count.</b> A server counts when it holds a row in [start, end] or, for a collector
/// table, when collection_log records a run of the collector for it in [start, end]. A server stopped or removed
/// before the window (its registry row and history stay) does not count, so its old rows cannot hide a newer
/// server's late start on a fleet panel. The answer is the earliest coverage start among the servers that count,
/// and null when none counts: the window holds nothing, and an empty panel already says so. It is null too when
/// that start is at or after the window's end (<see cref="GetAsync"/>): collection_log outlives a table the purge
/// keeps shorter, so a logged run makes a server count in a window whose rows are gone, and its coverage, the
/// purge's edge, starts after the window ended.</para>
///
/// <para><b>Why per server, through <c>collect.servers</c>.</b> Every collect table a panel reads is indexed on
/// <c>(server_id, time)</c> (the two config snapshot tables are the exception: they hold one snapshot per server a
/// day and are read bounded by the window), every continuous aggregate a panel reads keeps <c>(server_id, bucket)</c>,
/// and collection_log keeps <c>(server_id, collection_time)</c>. A LATERAL per registered server reads
/// the first index entry inside the window, so only the window's chunks are probed; the walk to a source with no
/// schedule edge runs only for a server that counts, and stops at the oldest chunk that holds the server.
/// Filtering the fact table by <c>server_name</c>, as a panel's own read does, has no index to use. So the probe
/// answers per server identity, through the registry's current name and <c>server_id</c>, while the panel's read
/// filters fact rows by their stored <c>server_name</c>: for a renamed server, rows written before the rename
/// count toward coverage even though the panel's filter misses them.</para>
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
        private Source(
            string relation, string timeColumn, bool endExclusive, string? collectorName = null, int? retentionDefaultDays = null,
            DateTime? lowerBoundUtc = null)
        {
            Relation = relation;
            TimeColumn = timeColumn;
            EndExclusive = endExclusive;
            CollectorName = collectorName;
            RetentionDefaultDays = retentionDefaultDays;
            LowerBoundUtc = lowerBoundUtc;
        }

        /// <summary>
        /// The instant (naive UTC) from which a panel reads this relation, when it reads only from there up: the
        /// successor half of a stitched rollup read, which the stitched clause takes from the stitch boundary up.
        /// Null when the panel reads the relation whole. The window read and the oldest-row walk both start at the
        /// bound, so a bucket the relation holds below it (a wide refresh after the boundary was measured), which the
        /// panel did not read, moves neither the answer nor a server's coverage. Bound as a parameter, never spliced
        /// (<see cref="FloorSql"/>). Only a rollup carries one (<see cref="TryForRollup"/>), and a rollup has no
        /// schedule edge, so the bound always applies to the walk.
        /// </summary>
        public DateTime? LowerBoundUtc { get; }

        /// <summary>
        /// The collector that writes the table, whose runs collection_log records; null for a rollup, which no
        /// collector writes, and for the collection log itself.
        /// </summary>
        public string? CollectorName { get; }

        /// <summary>
        /// The collector's default retention days, when the probe reads the table's retention edge from the
        /// schedule, or the collection log's fixed horizon; null when the schedule gives no edge (a rollup, a raw
        /// relation the gated purge owns, a collector whose purge horizon is floored at the baseline window), and the
        /// probe walks to the oldest row the server holds at or before the window's end instead.
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
        /// The two config snapshot tables the probe reads without an index: they hold one snapshot per server a day,
        /// and every read of them here carries the window's bounds, so TimescaleDB reads only the window's chunks and
        /// the oldest-row walk (which would read the whole table) never applies to them.
        /// </summary>
        private static readonly HashSet<string> s_configSnapshotTables =
            new HashSet<string>(StringComparer.Ordinal) { "server_config", "database_config" };

        /// <summary>
        /// The raw collector table <paramref name="table"/>, or false when the catalog does not list it or its
        /// index does not lead with <c>(server_id, time)</c> (<see cref="PgSchemaGenerator.CreateIndex"/>), unless it
        /// is a config snapshot table (<c>server_config</c>, <c>database_config</c>), which has no index and is read
        /// bounded by the window. index_object_stats orders a server's rows by object before time, so the probe
        /// would read every row the server holds; it is not probed. The collection log is not a collector table
        /// (<see cref="ForCollectionLog"/>).
        /// </summary>
        public static bool TryForCollectorTable(string table, out Source source)
        {
            source = null!;
            var schema = CollectorCatalog.All.FirstOrDefault(c => string.Equals(c.TargetTable, table, StringComparison.Ordinal));
            if (schema is null
                || (PgSchemaGenerator.CreateIndex(schema)?.EndsWith($"(server_id, {schema.PrefixTimeColumnName});", StringComparison.Ordinal) != true
                    && !s_configSnapshotTables.Contains(schema.TargetTable)))
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
        /// The collection log itself, the run record every collector writes a row to and the Collection Log grids show.
        /// It is not a collector table, so <see cref="TryForCollectorTable"/> refuses it and this is its factory. Its
        /// edge is its own fixed horizon (<see cref="DarlingRetentionHorizons.CollectionLogRetentionDays"/>, the
        /// purge's constant, which no schedule row moves), and a server counts by its own rows in the window, since
        /// no other log records the log's runs. The window read rides <c>idx_collection_log_time (server_id,
        /// collection_time)</c>: one index descent per server, never a scan of the log.
        /// </summary>
        public static Source ForCollectionLog() =>
            new("collection_log", "collection_time", endExclusive: false, retentionDefaultDays: DarlingRetentionHorizons.CollectionLogRetentionDays);

        /// <summary>
        /// The continuous aggregate <paramref name="view"/>, or false when <see cref="RollupAvailability"/> does not
        /// know the name. Every rollup a panel reads keeps a <c>(server_id, bucket)</c> index.
        /// <paramref name="lowerBoundUtc"/> is set for a rollup the panel reads only from that instant up
        /// (<see cref="LowerBoundUtc"/>), and null for one it reads whole.
        /// </summary>
        public static bool TryForRollup(string view, out Source source, DateTime? lowerBoundUtc = null)
        {
            source = null!;
            if (string.IsNullOrEmpty(view) || !RollupAvailability.All.Has(view))
            {
                return false;
            }

            source = new Source(view, "bucket", endExclusive: true, lowerBoundUtc: lowerBoundUtc);
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
    /// stitched rollup read) are probed side by side and the earliest answer wins. One arm per source, over the
    /// servers that count (a row in [$2, $1], or a logged run of the collector there). For a source the schedule
    /// governs: the later of the purge cutoff and the server's first collection, moved earlier by its first row in
    /// the window. For one it does not (see <see cref="Source.RetentionDefaultDays"/>): the oldest row the server
    /// holds at or before the window's end, a walk unbounded below that runs only for a server that counts, and
    /// the server's first collection when it holds no row at all.
    ///
    /// <para>A source that reads its relation only from an instant up (<see cref="Source.LowerBoundUtc"/>) takes that
    /// instant as a further parameter, naive UTC, numbered after the scope's ($4 for a fleet probe, $5 otherwise, then
    /// one more for each such source in order), and starts both its window read and its walk there. Binding them is
    /// the caller's: <see cref="GetAsync"/> and <see cref="GetForServerAsync"/> add them in that order.</para>
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
        var nextBoundParameter = scope == Scope.Fleet ? 4 : 5;
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
            var time = source.TimeColumn;
            var endBound = source.EndExclusive ? " < $1" : " <= $1";

            /* A relation the panel reads only from an instant up starts there, in the window read and in the walk, so
               a row it holds below that instant (one the panel did not read) moves neither. The instant is a bind
               parameter, numbered after the scope's; the caller adds the values in the same order. */
            var lowerBound = source.LowerBoundUtc is null
                ? null
                : "$" + (nextBoundParameter++).ToString(System.Globalization.CultureInfo.InvariantCulture);

            string coverage;
            if (source.RetentionDefaultDays is int days)
            {
                /* The table's edge is the purge cutoff: the schedule's horizon is the purge's, as written (the
                   collection log's is its own fixed constant, with no schedule row to ask). The later of it and the
                   server's first collection is where coverage starts; a row older than it moves the answer earlier. */
                var horizon = collector is null
                    ? days.ToString(System.Globalization.CultureInfo.InvariantCulture)
                    : $"COALESCE((SELECT o.retention_days FROM config.config_collector_schedules AS o WHERE o.server_id IS NULL AND lower(o.collector_name) = '{collector}' AND o.retention_days >= 1 LIMIT 1), {days.ToString(System.Globalization.CultureInfo.InvariantCulture)})";
                coverage = $"GREATEST($3 - make_interval(days => {horizon}), s.created_date)";
            }
            else
            {
                /* No schedule edge (a rollup, a raw relation the gated purge owns, a baseline-floored collector): the
                   edge is the oldest row the server holds at or before the window's end, the rule before this probe
                   read coverage. Right for these dense tables, where a running server writes a row every collection
                   (every hour, for a rollup), so the oldest row IS where the table's coverage starts and a quiet
                   start is not in play; and these panels would otherwise lose the notice they had (the tier notice
                   covers a rollup route on a TimescaleDB store, not these collectors, and not a store without
                   TimescaleDB). Unbounded below, so the walk starts at the table's oldest chunk. It sits in the select
                   list, so it runs only for a server that survives the WHERE below (one that counts). With no row at
                   all, coverage falls back to the server's first collection. */
                var walkLowerBound = lowerBound is null ? string.Empty : $" AND h.{time} >= {lowerBound}";
                coverage = $"COALESCE((SELECT h.{time} FROM {schema}.{source.Relation} AS h WHERE h.server_id = s.server_id AND h.{time}{endBound}{walkLowerBound} ORDER BY h.{time} LIMIT 1), s.created_date)";
            }

            sql.Append("    SELECT LEAST(").Append(coverage).Append(", w.t) AS t\n");
            sql.Append("    FROM ").Append(schema).Append(".servers AS s\n");
            sql.Append("    LEFT JOIN LATERAL\n");
            sql.Append("    (\n");
            sql.Append("        SELECT f.").Append(time).Append(" AS t\n");
            sql.Append("        FROM ").Append(schema).Append('.').Append(source.Relation).Append(" AS f\n");
            sql.Append("        WHERE f.server_id = s.server_id\n");
            sql.Append("        AND   f.").Append(time).Append(" >= $2\n");
            if (lowerBound is not null)
            {
                sql.Append("        AND   f.").Append(time).Append(" >= ").Append(lowerBound).Append('\n');
            }

            sql.Append("        AND   f.").Append(time).Append(endBound).Append('\n');
            sql.Append("        ORDER BY f.").Append(time).Append('\n');
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
    ///
    /// <para>Null too when that start is at or after <paramref name="endUtc"/>: the window lies wholly before the
    /// coverage, so there is none to report. The run log outlives a table the purge keeps shorter (a schedule-edge
    /// source drops its rows after days, <c>collection_log</c> keeps weeks more), so a window whose rows are all
    /// purged still counts for a server through the runs logged in it, and the coverage found then is the purge
    /// edge, which comes after the window ends. A start past the window's own end would read as a notice that
    /// "covers" a span running backwards, and as a "Showing since" banner naming a time after its own range.</para>
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

        AddLowerBounds(command, sources);

        return CoverageWithin(await command.ExecuteScalarAsync(cancellationToken), endUtc);
    }

    /// <summary>
    /// Where coverage of <paramref name="source"/> starts for one server: the desktop viewer's server-tab form of
    /// <see cref="GetAsync"/>, with the same answers, null included for a window wholly before the coverage (the run
    /// log outlives the table, so a logged run there makes the server count though its rows are gone).
    /// </summary>
    public static async Task<DateTime?> GetForServerAsync(
        NpgsqlDataSource postgres, Source source, int serverId, DateTime startUtc, DateTime endUtc,
        int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(source);

        /* #4966: a window no longer than the truncation slack can never get a coverage note. The answer is at or before the window's
           end, and RawWindowFloor.IsTruncated needs it MORE than the slack after the window's start, so the tab's banner could not
           show it. The probe would read the store for nothing: it starts no query. */
        if (endUtc - startUtc <= DurationTrendRouting.TruncationSlack)
        {
            return null;
        }

        await using var command = postgres.CreateCommand(FloorSql([source], Scope.ServerId));
        command.CommandTimeout = commandTimeoutSeconds;
        AddWindow(command, startUtc, endUtc);
        command.Parameters.AddWithValue(serverId);
        AddLowerBounds(command, [source]);

        return CoverageWithin(await command.ExecuteScalarAsync(cancellationToken), endUtc);
    }

    /* The probe's answer when it starts before the window's end, null otherwise (and for no answer at all). */
    private static DateTime? CoverageWithin(object? value, DateTime endUtc) =>
        value is DateTime start && start < endUtc ? start : null;

    /* SpecifyKind(Unspecified), not the bare value: Npgsql infers timestamptz from Kind=Utc and PostgreSQL then
       zone-shifts the bounds against the store's NAIVE timestamp columns (RawWindowFloor.GetAsync says the same). */
    private static void AddWindow(NpgsqlCommand command, DateTime startUtc, DateTime endUtc)
    {
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
    }

    /* One parameter per source that reads its relation only from an instant up, in source order, after the scope's:
       the numbering FloorSql gave them. Unspecified for the same reason as the window's bounds. */
    private static void AddLowerBounds(NpgsqlCommand command, IReadOnlyList<Source> sources)
    {
        foreach (var source in sources)
        {
            if (source.LowerBoundUtc is DateTime lowerBound)
            {
                command.Parameters.AddWithValue(DateTime.SpecifyKind(lowerBound, DateTimeKind.Unspecified));
            }
        }
    }
}
