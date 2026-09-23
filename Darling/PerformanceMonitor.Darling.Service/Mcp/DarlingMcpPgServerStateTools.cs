/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for four server-state reads that had a Viewer panel and nothing else (#2629): what is
/// resident in the buffer pool, which extensions the server has, what locking was sampled, and the
/// checkpoint / WAL write picture.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgServerStateTools
{
    /// <summary>
    /// The major that removed <c>buffers_backend</c> and <c>buffers_backend_fsync</c> from
    /// <c>pg_stat_bgwriter</c> (#2653). They moved nowhere in that view - the fact lives in
    /// <c>pg_stat_io</c> from 17 on - so <see cref="PgWriteStatsCollector"/> writes NULL for both at this
    /// major and above, and this read names that rather than letting a structural absence read as a missing
    /// measurement.
    /// </summary>
    private const int BuffersBackendRemovedInMajor = 17;

    [McpServerTool(Name = "get_pg_buffer_usage"), Description("Gets what is actually resident in the PostgreSQL shared buffer pool, per relation, from the pg_buffercache extension: how many buffers each table or index occupies, how many of those are dirty, and the average usage count that PostgreSQL's clock-sweep eviction reads. This answers which objects the cache is actually spent on, which is a different question from which objects are read most - a small hot table and a large one scanned once can produce similar read counts and completely different residency. High dirty counts concentrated in one relation point at write pressure. Note that scanning pg_buffercache takes a lock on the buffer mapping, so the collector runs it sparingly.")]
    public static async Task<string> GetPgBufferUsage(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingPgBufferUsageReader.GetPgBufferUsageAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_buffer_usage")
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_buffer_usage")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No buffer pool contents recorded for {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). This needs the pg_buffercache extension in the database "
                        + "the collector connects to.");
            }

            var newest = rows[0];

            var relations = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                relation_name = r.RelationName,
                relation_kind = r.RelationKind,
                buffers = r.Buffers,
                buffer_mb = Math.Round(r.Buffers * 8.0 / 1024.0, 1),
                dirty_buffers = r.DirtyBuffers,
                pct_dirty = Math.Round(r.PctDirty, 1),
                pct_of_pool = Math.Round(r.PctOfPool, 1),
                /* The eviction signal: PostgreSQL's clock sweep decrements this on each pass and evicts at
                   zero, so a high average means the relation is being touched faster than it decays. */
                avg_usage_count = r.AvgUsageCount,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                relation_count = rows.Count,
                /* 8 kB is PostgreSQL's near-universal block size but it IS a compile-time setting, so the
                   buffer COUNT is the measurement and the megabyte figure is a convenience beside it. */
                pool_buffers_total = newest.PoolBuffersTotal,
                pool_buffers_used = newest.PoolBuffersUsed,
                note = "Residency, not read volume — a small hot table and a large one scanned once can "
                     + "read alike and occupy the pool completely differently. avg_usage_count is what the "
                     + "clock-sweep eviction reads; buffer sizes assume the standard 8 kB block.",
                relations,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_buffer_usage", ex);
        }
    }

    [McpServerTool(Name = "get_pg_extensions"), Description("Gets which PostgreSQL extensions are installed, outdated, merely available, or absent, per database. Use this to answer why another read is empty: pg_stat_statements, pg_wait_sampling, pg_stat_kcache, pg_qualstats, pgstattuple, pg_buffercache and hypopg each back a specific tool, and 'absent' here is the reason that tool has nothing to show. State is one of installed, outdated, available or absent - 'available' means the files are on the server and CREATE EXTENSION would work, which is a different situation from absent and usually a one-line fix. IMPORTANT: extensions are per-DATABASE, so installed_version reflects the database the row names and not the cluster; an extension can be installed in the application database and absent from postgres. Rows are therefore the PRODUCT of databases and extension names - 102 extension names per database on the measured fleet - so a ten-database host needs 1,020 rows against a 1000-row maximum and cannot be completed in one call. Do NOT read a truncated row list as an install census: the ordering puts non-relevant 'installed' rows behind every non-relevant 'available' one, so plpgsql, which is installed in every database of every server, has no row at all once a host reaches ten databases. Read install_census instead - one row per extension created anywhere on the server with databases_installed against databases_total, aggregated so no row limit touches it - and pass database_name to complete one database's 102 rows at a time. The census and reach fields report the population and whether a larger limit would help; sampling this tool at a small limit returns exactly the 8 monitoring-relevant extensions per database and looks like a complete per-database answer.")]
    public static async Task<string> GetPgExtensions(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (7 days) - this collector runs daily.")] int hours_back = 168,
        [Description("Maximum rows to return. Default 50.")] int limit = 50,
        [Description("One database, or omit for every database on the server. Rows are one per (database, "
            + "extension), so the row count is databases x extension names - 102 extension names per database "
            + "on the measured fleet, which puts a ten-database host past the 1000-row maximum and a "
            + "sixteen-database one at 1,632. ONE database's slice is 102 rows, so this is what makes such a "
            + "host completable: sixteen calls that each finish instead of one that cannot. OMIT it for an "
            + "install census: install_census is aggregated per extension and no row limit touches it, so "
            + "one unfiltered call answers what is installed where. It follows this filter when you supply "
            + "one, so a filtered response describes that database and nothing else - census, install_census "
            + "and reach all share the rows' scope, and the echoed database_name names it.")]
            string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);

            /* NORMALISED ONCE, and everything downstream uses THIS value - the reads, the echo, and the
               empty-path message. The reader's own bind applies the same helper, so the query and the label
               cannot disagree: a whitespace-only database_name from the web surface produces server-wide
               rows, and echoing the caller's raw string beside them claimed a scope the response did not
               have. That is the "one response, one scope" guarantee the census and reach blocks rest on,
               contradicted by its own label. */
            var scope = DarlingPgExtensionAvailabilityReader.NormalizeDatabaseFilter(database_name);

            var fetched = await DarlingPgExtensionAvailabilityReader.GetPgExtensionAvailabilityAsync(
                postgres, resolved.ServerId, windowStart, windowEnd, limit + 1, scope);

            if (fetched.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_extension_availability")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No extension inventory for {resolved.ServerName} in the last {hours_back} "
                        + "hour(s). This collector runs DAILY, so a short window can be empty on a healthy "
                        + "server — widen it before concluding anything."
                        + (scope is null
                            ? string.Empty
                            : $" You filtered to database_name '{scope}': check the spelling and "
                              + "that the collector enumerates it, because a name that matches nothing is "
                              + "indistinguishable here from a server with no inventory. Re-run without the "
                              + "filter to see which databases are reported."));
            }

            /* The result is capped at `limit`, so a summary count taken over the returned rows describes
               the PAGE and reads as a fact about the SERVER. Measured while writing this: at limit=50 the
               tool reported "installed: 10" for a server with more than that, because 50 was all it had
               looked at. Suppressed rather than renamed — "installed_in_this_page" is a number nobody
               wants.

               #3653 (the #3541 A3 class): truncation is OBSERVED off the limit + 1 fetch above, not inferred
               from a page that happens to be exactly `limit` long — a one-database host whose slice is
               exactly the limit is complete, and the old `>= limit` withheld its state totals for it. */
            var truncated = fetched.Count > limit;
            var rows = truncated ? fetched.Take(limit).ToList() : fetched;

            /* THE POPULATION FIGURES AND THE INSTALL CENSUS, from their own query that no row limit touches
               (#3425) — the #3278 pattern, applied to a read whose truncation removes precisely the rows a
               census needs. Rows here are databases x extension names, so the row list outgrows any cap;
               this is aggregated per extension, so it is bounded by what the PostgreSQL build offers and
               does not grow as databases are added.

               Asked on the POPULATED path too, and not only when truncated. A reader with 50 rows of a
               1,632-row product has the same wrong impression as one with 1,000 of them, and a census that
               only appeared once the response was capped would be missing exactly when somebody was
               sampling.

               SAME SCOPE AS THE ROWS: database_name goes to both, so one response describes one
               population. Found in review of this change: a server-wide census beside one database's rows
               put 1,632 against a complete 102-row answer and the reach classifier correctly answered
               Unreachable, telling a filtered caller they could not see what they were holding. */
            var census = await DarlingPgExtensionAvailabilityReader.GetInstallCensusAsync(
                postgres, resolved.ServerId, windowStart, windowEnd, scope);

            /* FROM THE CENSUS, not from the rows. Every row carries the same two scalars — they hang off a
               one-row relation the per-extension groups join to — so the first row is the whole answer, and
               an empty census still produces that row. Zero here means the census read nothing rather than
               that the server has no databases, which is why it is reported rather than divided by. */
            var databasesTotal = census.Count > 0 ? census[0].DatabasesTotal : 0;
            var extensionNameCount = census.Count > 0 ? census[0].ExtensionNameCount : 0;
            var rowsAvailable = census.Count > 0 ? census[0].RowsAvailable : 0;

            /* WHICH DATABASES THE PAGE ACTUALLY FINISHED. A truncated response mixes complete databases
               with ones cut to their first few rows and nothing per row says which, so this counts them:
               measured on a sixteen-database host, nine databases complete, one at 43 rows and six holding
               only their 8 monitoring-relevant rows, all in the same payload.

               COMPLETE means "as many rows as the server has extension names", and when collection was
               uneven that test calls a genuinely complete database partial. That is the direction to be
               wrong in — see PgCappedReach on failing toward the worse label — and it is why
               databases_complete is reported beside databases_in_page rather than as a share of it. */
            var perDatabase = rows
                .GroupBy(r => r.DatabaseName, StringComparer.Ordinal)
                .ToList();
            var databasesComplete = extensionNameCount > 0
                ? perDatabase.Count(group => group.Count() >= extensionNameCount)
                : 0;

            /* THE CREATED ROWS ARE WHAT THE ORDERING PUTS LAST, so they are what a cap removes: every
               non-relevant AVAILABLE row of every database sorts ahead of every non-relevant installed one,
               and plpgsql is the only non-relevant installed extension on the measured fleet. So the
               population a reader of this tool most often wants — what is actually created — is precisely
               the one the truncation takes, and `reach` is the arithmetic for whether it was reachable at
               all.

               COMPUTED ONCE and passed to both arguments through a local. Two copies of this subtraction
               would have to agree, and a disagreement's failure is a verdict whose own figures contradict
               each other — a read that half works, for a reason no reader could see.

               `rowsAvailable - createdRows` IS A CONSERVATIVE BOUND on the leading block, and deliberately
               so. It counts every non-created row as being ahead of the created ones, while the non-relevant
               ABSENT rows in fact sort behind them — so it over-states the block by exactly that count and
               the verdict can only ever be pessimistic. On the measured fleet the error is zero: of 102
               extension names per database, 93 non-relevant rows are `available`, one is `installed`
               (plpgsql) and none is non-relevant `absent`, giving a bound of 100 against an exact 100.

               The alternative is a second copy of PgExtensionAvailabilitySql's ORDER BY inside a window
               function, to position the last created row exactly. That trades a documented bound that
               cannot flatter for two ordering expressions that have to agree and whose disagreement nothing
               would catch — the worse hazard, and the one this file's SQL comments already warn about.

               Under database_name the product collapses to one database's slice, which is two orders of
               magnitude under the cap, so nothing displaces anything and the page is that database's whole
               answer.

               GROUPED, and it is a property of this read's ORDER BY rather than a judgement (#3435). The
               order is relevance band, then state band, then database and extension name - a partition,
               not a ranking of the created rows by anything a reader would act on. So a cut here leaves
               SOME DATABASES AND NOT OTHERS, and RankedTail's claim that the withheld rows rank below the
               returned ones is false on this surface at every row count.

               Declared rather than left to the figures, because the figures cannot carry it: the arm turns
               on whether anything sorts ahead of the created rows, which is a count this server's contents
               decide. A declaration makes the false claim unreachable at every count. */
            var createdRows = census.Sum(row => row.DatabasesInstalled + row.DatabasesOutdated);

            var reach = PgCappedRead.Classify(
                rowsAhead: rowsAvailable - createdRows,
                wantedRows: createdRows,
                returnedRows: rows.Count,
                limit: limit,
                maxLimit: McpHelpers.MaxTop,
                order: PgOrderSemantics.Grouped,
                remedy: "install_census in this response answers what is created where, aggregated per "
                      + "extension and unaffected by any row limit. For the per-database ROWS, pass "
                      + "database_name: one database is "
                      + extensionNameCount.ToString(CultureInfo.InvariantCulture)
                      + " row(s) here, so a host is that many completable calls.");

            var extensions = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                extension_name = r.ExtensionName,
                state = r.State,
                installed_version = r.InstalledVersion,
                default_version = r.DefaultVersion,
                /* Whether THIS PRODUCT can use it, not merely whether it exists — the distinction that
                   makes this list actionable rather than an inventory. */
                monitoring_relevant = r.IsMonitoringRelevant,
                comment = r.Comment,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                extension_count = rows.Count,
                truncated,
                /* ECHOED NORMALISED, so a saved payload says which population it actually describes: one
                   database's complete 102 rows and a 102-row slice of a sixteen-database product look
                   identical otherwise, and a blank filter that the query ignored must not come back looking
                   like a filter that applied. */
                database_name = scope,
                installed = truncated
                    ? (int?)null
                    : rows.Count(r => string.Equals(r.State, "installed", StringComparison.OrdinalIgnoreCase)),
                available_not_installed = truncated
                    ? (int?)null
                    : rows.Count(r => string.Equals(r.State, "available", StringComparison.OrdinalIgnoreCase)),

                /* THE POPULATION, measured by its own query and unaffected by the row limit (#3425). The
                   two figures whose PRODUCT is what a caller's limit is really being compared against, and
                   which no reader could obtain from the rows: a capped response names some databases and
                   not others, so counting the ones it shows answers a question about the page. */
                census = new
                {
                    databases_total = databasesTotal,
                    extension_name_count = extensionNameCount,
                    rows_available = rowsAvailable,
                    /* The product beside the count, because they differ when a database's collection was
                       partial and the difference is the only evidence of it available here. */
                    rows_expected_if_uniform = databasesTotal * extensionNameCount,
                    databases_in_page = perDatabase.Count,
                    /* Complete for the databases it names, not complete for the server. Counted rather
                       than shared, and pessimistic when collection was uneven - see the comment above. */
                    databases_complete_in_page = databasesComplete,
                    databases_absent_from_page = Math.Max(0, databasesTotal - perDatabase.Count),
                },

                /* WHETHER THE CREATED ROWS COULD APPEAR AT ALL, which `truncated` cannot say. The ordering
                   puts non-relevant installed rows behind every non-relevant available one, so `installed`
                   is the state a cap removes first - and once the rows ahead of it reach the surface
                   maximum, no limit shows a single one of them. */
                reach = new
                {
                    arm = reach.Reach.ToString(),
                    /* WHAT THE ORDER MEANS, and on this read it is Grouped (#3435). The field is why the arm
                       can never be RankedTail here: the rows a cap removes are other databases, so "what
                       you lost ranks below what you got" would be false, and the type refuses to say it
                       rather than saying it with a caveat. */
                    order_semantics = reach.Order.ToString(),
                    is_complete = reach.IsComplete,
                    a_raised_limit_would_help = reach.ARaisedLimitWouldHelp,
                    rows_ahead_of_created = reach.RowsAhead,
                    created_rows_on_server = reach.WantedRows,
                    created_rows_reachable_here = reach.ReachableRows,
                    created_rows_reachable_at_max_limit = reach.ReachableAtMaxRows,
                    created_rows_withheld = reach.WithheldRows,
                    limit,
                    max_limit = McpHelpers.MaxTop,
                },

                /* THE INSTALL CENSUS: one row per extension somebody created anywhere in this response's
                   SCOPE, with the database count it is created in and the denominator beside it. Aggregated,
                   so it CANNOT be truncated away while its siblings survive - which is what happens to
                   plpgsql in the row list above on any host of ten databases or more. This is the answer to
                   "is X installed everywhere", and it is complete whatever the row limit did.

                   Scope, not server, because database_name narrows this too: one response describes one
                   population, and databases_total says which. Unfiltered, the scope IS the server. */
                install_census = census
                    .Where(row => row.ExtensionName is not null)
                    .Select(row => new
                    {
                        extension_name = row.ExtensionName,
                        databases_installed = row.DatabasesInstalled,
                        databases_outdated = row.DatabasesOutdated,
                        /* Created at all: installed plus outdated. Both mean the extension exists in that
                           database and only one of them means it is current, so they are reported apart and
                           summed here rather than being collapsed at the source. */
                        databases_created = row.DatabasesInstalled + row.DatabasesOutdated,
                        databases_reporting = row.DatabasesReporting,
                        databases_total = row.DatabasesTotal,
                    }),

                note = "Per DATABASE, not per cluster: installed_version describes the database the row "
                     + "names. 'available' means the files are present and CREATE EXTENSION would work — "
                     + "usually a one-line fix for an empty panel elsewhere. Rows are the PRODUCT of "
                     + "databases and extension names, so this row list outgrows any row limit on a "
                     + "multi-database host — read install_census for what is installed where, because it "
                     + "is aggregated per extension and no limit touches it."
                     + (truncated
                         ? " TRUNCATED at the row limit, so the state totals are WITHHELD: counting them "
                           + "over a capped result would describe this page rather than the server. And the "
                           + "cut is NOT a random slice — the ordering places non-relevant 'installed' rows "
                           + "behind every non-relevant 'available' one, so what a cap removes first is "
                           + "precisely what an install census needs. Do not read this page as one; use "
                           + "install_census, or pass database_name to complete one database at a time."
                         : string.Empty)
                     + " " + reach.Message,
                extensions,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_extensions", ex);
        }
    }

    [McpServerTool(Name = "get_pg_lock_stats"), Description("Gets PostgreSQL lock activity as SAMPLED by the collector: lock type, mode, whether it was granted, the relation involved, how many captures saw it and the worst wait observed. This is a SAMPLE, not an event log - the collector periodically photographs pg_locks, so a lock held briefly between two samples is invisible here and an absence is not proof nothing was locked. Ungranted rows are the ones that matter: a granted lock is normal operation, while a lock waiting to be granted is a session blocked behind another. For a blocking chain with the root blocker attributed, use get_pg_blocking instead - this tool answers which lock modes and relations are contended over time rather than who is blocking whom right now.")]
    public static async Task<string> GetPgLockStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            /* #3653 (the #3541 A3 class): limit + 1 as the fetch, the extra row as the observed truncation
               signal. Under the old `rows.Count >= limit`, a window holding exactly `limit` contended
               (type, mode, relation) groups read as truncated and its ungranted total was withheld. */
            var fetched = await DarlingPgLockStatsReader.GetPgLockStatsAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit + 1);

            if (fetched.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_lock_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No lock activity sampled on {resolved.ServerName} in the last {hours_back} "
                        + "hour(s). This is a SAMPLE of pg_locks rather than an event log, so this is the "
                        + "healthy state on a server without sustained contention — and it is not proof "
                        + "that nothing was ever locked.");
            }

            var truncated = fetched.Count > limit;
            var rows = truncated ? fetched.Take(limit).ToList() : fetched;
            var ungranted = rows.Count(r => !r.Granted);

            var locks = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                lock_type = r.LockType,
                mode = r.Mode,
                granted = r.Granted,
                relation_name = r.RelationName,
                captures = r.Captures,
                /* The denominator: how many captures happened at all, so `captures` can be read as a rate
                   of presence rather than an unanchored count. */
                total_captures = r.TotalCaptures,
                max_backends = r.MaxBackends,
                max_wait_ms = r.MaxWaitMs,
                last_seen = r.LastSeen,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                lock_count = rows.Count,
                truncated,
                /* Counted over the returned rows, and reported only when they are all of them. */
                ungranted_count = truncated ? (int?)null : ungranted,
                note = (truncated
                         ? "TRUNCATED at the row limit — the ungranted total is WITHHELD, because counting "
                           + "over a capped result would describe this page rather than the server. "
                         : string.Empty)
                     + "A SAMPLE of pg_locks, not an event log: a lock taken and released between two "
                     + "captures does not appear. Read `captures` against `total_captures` — that ratio is "
                     + "how much of the window the lock was present for. Ungranted rows are the contended "
                     + "ones; for who is blocking whom, use get_pg_blocking.",
                locks,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_lock_stats", ex);
        }
    }

    [McpServerTool(Name = "get_pg_write_stats"), Description("Gets PostgreSQL checkpoint and WAL write activity across the window: how many checkpoints were timed versus REQUESTED, how long they spent writing and syncing, how many buffers were written by checkpoints, by the background writer and by backends themselves, and the WAL record, full-page-image and byte totals. Requested checkpoints are the signal to look for - a timed checkpoint is the scheduled one, while a requested checkpoint means WAL filled max_wal_size before the interval elapsed, so a high requested share means checkpoints are being forced by write volume. buffers_backend counts writes a query had to do itself because no clean buffer was available, which is backpressure landing on user queries - PostgreSQL 17 removed that column from pg_stat_bgwriter, so it is null on 17 and later and the note says so. wal_fpi counts full-page images, which is why write volume spikes immediately after each checkpoint. A restart inside the window leaves checkpoints_requested, pct_checkpoints_requested, checkpoint_write_time_ms, checkpoint_sync_time_ms and buffers_written_checkpoint null, with postmaster_restarted true: PostgreSQL counts a shutdown checkpoint as requested and keeps the count across the restart, and that checkpoint's own write and sync work lands in the same counters, so across one those figures cannot be told from the workload's (postmaster_start_time says when the server last started, so a window after it can be chosen). Returns one row describing the whole window, not a series.")]
    public static async Task<string> GetPgWriteStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var row = await DarlingPgWriteStatsReader.GetPgWriteStatsAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd);

            if (row is null)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_write_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No checkpoint or WAL activity recorded for {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). These are differenced across snapshots, so a single "
                        + "collection has nothing to difference against and the window fills on the "
                        + "second one.");
            }

            var timed = row.CheckpointsTimed ?? 0;
            var requested = row.CheckpointsRequested ?? 0;

            /* Both registry facts in ONE read, because this payload has two kinds of structurally
               absent column and neither is distinguishable from an unmeasured one without asking.

               VERSION (#2653): 17 removed buffers_backend / buffers_backend_fsync from
               pg_stat_bgwriter with no successor there, so the collector writes NULL and the fact
               lives in pg_stat_io.

               FLAVOUR (#3156): Aurora does not implement pg_stat_wal at all - pg_stat_get_wal()
               raises 0A000 there - so every wal_* column is NULL on an Aurora target.

               Two reads would let one axis be answered from this server's row and the other from a
               stale copy, which is the hazard PostgresTargetFactsSql is a single statement for. Same
               discipline on both axes: a registry making no claim produces no claim here. */
            var (postgresMajor, engineKind) = await DarlingEngineCapability.PostgresTargetFactsAsync(
                postgres, resolved.ServerId);
            var backendCountersRemoved = postgresMajor >= BuffersBackendRemovedInMajor;
            var walAbsentOnAurora = MonitoredEngineKind.IsAurora(engineKind);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                window_start = row.WindowStartUtc,
                window_end = row.WindowEndUtc,
                checkpoints_timed = row.CheckpointsTimed,
                checkpoints_requested = row.CheckpointsRequested,
                /* The whole reason both numbers are here. Null rather than 0 when there were no
                   checkpoints at all — 0% requested would claim a healthy result from no evidence — and null when
                   the requested count itself is (a restart inside the window, #3955): a share computed from the
                   timed count alone would read as 0% requested, a clean result from no evidence again. */
                pct_checkpoints_requested = row.CheckpointsRequested is not null && timed + requested > 0
                    ? Math.Round((double)requested / (timed + requested) * 100, 1)
                    : (double?)null,
                checkpoint_write_time_ms = row.CheckpointWriteTimeMs,
                checkpoint_sync_time_ms = row.CheckpointSyncTimeMs,
                buffers_written_checkpoint = row.BuffersWrittenCheckpoint,
                buffers_clean = row.BuffersClean,
                /* Backpressure landing on user queries: a backend writing its own buffer is a query
                   paying for the write because no clean buffer was free. */
                buffers_backend = row.BuffersBackend,
                buffers_backend_fsync = row.BuffersBackendFsync,
                /* Named only when the registry actually carries a major that removed them: absent from the
                   payload otherwise, so a server whose version is unknown gets no claim rather than a
                   guessed one. */
                buffers_backend_availability = backendCountersRemoved
                    ? $"not_collected: removed from pg_stat_bgwriter in PostgreSQL 17 (this server runs {postgresMajor}); the fact now lives in pg_stat_io, read by get_pg_io_stats"
                    : null,
                buffers_alloc = row.BuffersAlloc,
                maxwritten_clean = row.MaxwrittenClean,
                wal_records = row.WalRecords,
                wal_fpi = row.WalFpi,
                wal_bytes = row.WalBytes,
                wal_buffers_full = row.WalBuffersFull,
                wal_write_time_ms = row.WalWriteTimeMs,
                wal_sync_time_ms = row.WalSyncTimeMs,
                /* The flavour sibling of buffers_backend_availability, named for the same reason: without
                   it a permanently absent column and an idle server both render as NULL. Absent from the
                   payload unless the registry actually says Aurora, so a target whose kind is unknown gets
                   no claim rather than a guessed one. */
                wal_availability = walAbsentOnAurora
                    ? "not_collected: Aurora does not implement pg_stat_wal, so the CLUSTER-WIDE WAL "
                      + "volume and timing are not available on this server - pg_stat_get_wal() raises "
                      + "0A000 there. Per-statement WAL volume is collected from pg_stat_statements and "
                      + "read by get_pg_top_queries, and the checkpointer and background-writer counters "
                      + "beside these are collected normally."
                    : null,
                counter_reset = row.ResetDuringWindow,
                /* #3955: a restart inside the window, found from consecutive rows' postmaster start times; it is
                   why checkpoints_requested, the checkpoint write and sync time and the checkpoint buffer counts are
                   null. postmaster_start_time is the window's last sample's. */
                postmaster_restarted = row.PostmasterRestartedDuringWindow,
                postmaster_start_time = row.PostmasterStartTimeUtc,
                note = "Requested checkpoints mean max_wal_size filled before the scheduled interval, so a "
                     + "high requested share means write volume is forcing them. "
                     + (backendCountersRemoved
                         ? "buffers_backend and buffers_backend_fsync are NULL because PostgreSQL 17 removed "
                           + "them from pg_stat_bgwriter, not because nothing was measured: the question they "
                           + "answered - whether backends are writing their own buffers - is answered on this "
                           + "server by pg_stat_io, which get_pg_io_stats reads. "
                         : "buffers_backend is a write a QUERY had to perform itself for want of a clean "
                           + "buffer. ")
                     + (walAbsentOnAurora
                         ? "The wal_* columns are NULL because Aurora does not implement pg_stat_wal, not "
                           + "because no WAL was written: Aurora replaced the WAL layer with its own "
                           + "distributed storage. The cluster-wide total is unavailable here, but "
                           + "per-statement WAL volume is collected and get_pg_top_queries reads it, so "
                           + "which statement is generating WAL is still answerable. The checkpointer and "
                           + "background-writer figures above are unaffected."
                         : "wal_fpi counts full-page images, which is why WAL volume spikes just after "
                           + "each checkpoint.")
                     + (row.ResetDuringWindow
                         ? " The counters were RESET inside this window, so these figures cover only the "
                           + "time since the reset."
                         : string.Empty)
                     + (row.PostmasterRestartedDuringWindow
                         ? " PostgreSQL RESTARTED inside this window"
                           + (row.PostmasterStartTimeUtc is { } started
                               ? " (its postmaster started at "
                                 + started.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + " UTC)"
                               : string.Empty)
                           + ". A shutdown checkpoint is counted as REQUESTED and the count survives the restart, and "
                           + "its own write and sync phases and the buffers it flushed land in the same counters, where "
                           + "nothing can separate them from the workload's checkpoints. So checkpoints_requested, "
                           + "pct_checkpoints_requested, checkpoint_write_time_ms, checkpoint_sync_time_ms and "
                           + "buffers_written_checkpoint are null rather than figures that would read "
                           + "the restart as write pressure. Set hours_back (or as_of) so the window starts after that "
                           + "restart to see them. The timed count (which a restart does not move) and the WAL figures "
                           + "are still stated."
                         : string.Empty),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_write_stats", ex);
        }
    }

    [McpServerTool(Name = "get_pg_server_config"), Description("Gets the PostgreSQL server's configuration from pg_settings - what each parameter is set to, whether it differs from the compiled-in default, where the value came from (configuration file, command line, ALTER SYSTEM, per-database or per-role), and whether changing it needs a restart or only a reload. Non-default settings are listed FIRST, because a server has several hundred parameters and only the ones somebody chose are an answer. Reports pending_restart loudly: that means postgresql.conf was edited and reloaded but the running server is still using the old value, so the file and the server disagree with no symptom until the next restart. Session-scoped rows are excluded - pg_settings is a per-connection view and its client-source rows describe the monitoring connection, not the server. LATEST IS A TIME: this is the newest stored snapshot, not a window and not the live server - captured_at is the instant it was taken. The collector runs hourly, so a value here is 'as of' that stamp: a setting changed since (ALTER SYSTEM, a reload, a parameter-group edit) is not reflected until the next collection, and on a server whose collector has stalled the stamp is the only thing that says how stale the answer is. Compare captured_at against get_collection_log before trusting a value in an incident. THE PAGE IS BOUNDED BY limit: settings_returned is how many rows you got, truncated says the population you asked for (the non-default settings, or every setting when include_defaults is true) held more, and the rows are the chosen ones first. COUNTS ARE OF THE SNAPSHOT, NOT OF THE PAGE: non_default_count is how many settings in the whole snapshot differ from their default, computed in the same statement as the rows before the cap, so it is the same number at any limit; non_default_returned is how many of those are on this page, and the gap between the two is what the cap left out. PER-DATABASE AND PER-ROLE OVERRIDES ARE A SEPARATE SECTION: the settings list is the SERVER's configuration, and database_overrides - present only when the cluster has any - carries the values one database or one role was given with ALTER DATABASE/ROLE SET, which are what sessions there actually run with rather than the server-wide value beside them.")]
    public static async Task<string> GetPgServerConfig(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Maximum settings to return. Default 100. This is what bounds the page - read truncated to know whether the population held more; non_default_count stays the whole snapshot's whatever this is set to.")] int limit = 100,
        [Description("When true, include settings still at their default. Default false - the non-default ones are the answer, and a setting marked pending_restart is kept in that view whatever its source, because it is the row that says the file and the server disagree.")] bool include_defaults = false)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;

        try
        {
            /* #3653 (the #3541 A3 residue on this tool): the caller's limit + 1 as the fetch, the extra row as
               the OBSERVED truncation signal, and the include_defaults filter inside the statement so the cap
               cuts the population the caller asked for. The first shape fetched exactly `limit` rows of the
               whole snapshot, filtered the defaults out in C#, and published `rows.Count >= limit` as
               truncated: a snapshot of exactly `limit` non-session rows read as truncated, and the default
               view read as truncated on nearly every call because a server has several hundred parameters. */
            var page = await DarlingPgServerConfigReader.GetCurrentConfigPageAsync(
                postgres, resolved.ServerId, limit + 1, include_defaults);

            if (page.Rows.Count == 0)
            {
                /* Empty on the default view can mean two things: no snapshot, or a snapshot in which nothing
                   is set and nothing is pending. SnapshotNonDefaultCount cannot tell them apart (it rides on
                   rows that did not come back), so the capability read decides as it always has. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_server_config")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No configuration snapshot has been collected for {resolved.ServerName} yet. "
                        + "This collector runs hourly, so a server registered in the last hour has not "
                        + "reached its first collection.");
            }

            var truncated = page.Rows.Count > limit;
            var rows = truncated ? page.Rows.Take(limit).ToList() : page.Rows;
            var pendingRestart = rows.Where(r => r.PendingRestart).Select(r => r.Name).ToList();

            /* #3691 (V138): the per-database and per-role overrides, from the same newest snapshot as the
               rows above (both statements anchor on MAX(collection_time) for this server, so they agree
               without an instant threaded between them). A second read on a tool path, not the alert path,
               behind the table's (server_id, collection_time) index on a snapshot of a few hundred rows.

               UNCAPPED, deliberately, where the settings list is paged: pg_db_role_setting holds one row per
               scope that has ever been given a setting, which on a real cluster is a handful and on the
               measured population is zero - and a truncated override list is worse than none, because the
               question it answers is "is my value overridden anywhere" and a cap turns a No into a maybe. */
            var overrides = await DarlingPgServerConfigReader.GetOverridesAsync(postgres, resolved.ServerId);

            var configPage = new
            {
                server = resolved.ServerName,
                status = "server_config",
                /* #3653 (from #3541 A10): the snapshot's own clock, read off the row statement — every row of
                   this call carries the same collection_time because the reader pins the newest one — so the
                   answer says when it was true. No age_seconds and no as_of: this read takes no window to
                   anchor against (the Stamped dialect, McpLatestSnapshotStampTests.DarlingOnlyStamped). */
                captured_at = rows[0].CollectionTimeUtc.ToString("o"),
                /* Three counts, each with its denominator in its name (#3653). settings_returned is the page.
                   non_default_count is the SNAPSHOT's: computed on the reader's statement above the LIMIT, so
                   it is the same figure at limit = 5 and at limit = 500 — the old `rows.Count(!IsDefault)`
                   counted the rows fetched and, at a limit below the number of chosen settings, reported the
                   limit as a fact about the server. non_default_returned is how many of the snapshot's
                   chosen settings are on THIS page; when it equals non_default_count every chosen setting is
                   here, whatever truncated says about the rest of the population. */
                settings_returned = rows.Count,
                non_default_count = page.SnapshotNonDefaultCount,
                non_default_returned = rows.Count(r => !r.IsDefault),
                truncated,
                pending_restart_count = pendingRestart.Count,
                pending_restart_settings = pendingRestart.Count > 0 ? pendingRestart : null,
                note = pendingRestart.Count > 0
                    ? "One or more settings are marked PENDING RESTART: the configuration file has been "
                      + "changed and reloaded, but the running server is still using the previous value. "
                      + "The file and the server disagree until the next restart, at which point the "
                      + "behaviour changes with no deployment to explain it."
                    : "Non-default settings first. 'source' says where the value came from; 'context' says "
                      + "what changing it would take - postmaster needs a restart, sighup a reload, user "
                      + "nothing. Session-scoped rows are excluded: pg_settings is a per-connection view "
                      + "and those describe the monitoring connection rather than the server.",
                settings = rows.Select(r => new
                {
                    name = r.Name,
                    setting = r.Setting,
                    unit = r.Unit,
                    /* The compiled-in default, so a reader can see what was moved away FROM without
                       needing a table of defaults that would rot at every major. */
                    default_value = r.BootValue,
                    is_default = r.IsDefault,
                    source = r.Source,
                    context = r.Context,
                    requires_restart_to_change = string.Equals(r.Context, "postmaster", StringComparison.Ordinal),
                    pending_restart = r.PendingRestart,
                    category = r.Category,
                    description = r.ShortDescription,
                }),
            };

            /* #3691 (V138): the overrides section is ATTACHED, never a property. McpHelpers.JsonOptions writes
               nulls, so `database_overrides = overrides.Count > 0 ? … : null` would put "database_overrides": null
               on every page of every cluster with no overrides — and on every pre-V138 snapshot, where the truth is
               that the collector was not reading the catalog yet. Absent means "nothing to say"; a caller that
               sees no key reads the description, not the value. The live pin holds the whole page string
               byte-identical with and without overrides planted (PgServerConfigOverrideLivePostgresTests).

               Why this is not folded into the settings list: an override is keyed by SCOPE plus name while a
               server setting is keyed by name, so two rows called work_mem on one page would need a column read
               to tell which one a session actually gets — and every count above (settings_returned,
               non_default_count, non_default_returned) is a count of the server-wide population, which is what
               those names have always promised. The overrides sit beside them, not among them. */
            if (overrides.Count == 0)
            {
                return JsonSerializer.Serialize(configPage, McpHelpers.JsonOptions);
            }

            var node = JsonSerializer.SerializeToNode(configPage, McpHelpers.JsonOptions)?.AsObject()
                ?? throw new InvalidOperationException("the server-config page did not serialize to a JSON object");
            node["database_overrides"] = JsonSerializer.SerializeToNode(overrides.Select(o => new
            {
                /* NULL means "not scoped to one": a database with no role is ALTER DATABASE ... SET, a role with no
                   database is ALTER ROLE ... SET (that role in every database), and both is ALTER ROLE ... IN
                   DATABASE ... SET. Neither-NULL cannot appear here — that is a server-wide row, and the read
                   excludes it. */
                database_name = o.DatabaseName,
                role_name = o.RoleName,
                name = o.Name,
                setting = o.Setting,
            }).ToList(), McpHelpers.JsonOptions);
            node["database_overrides_note"] = "database_overrides are values one DATABASE or one ROLE was given with ALTER DATABASE "
                + "/ ALTER ROLE ... SET, read from pg_db_role_setting. A session connecting to that "
                + "database, or as that role, runs with the override rather than with the server-wide "
                + "value listed above - so a setting that appears in both places has TWO answers and "
                + "which one applies depends on who is connecting. The stored text is what was SET, "
                + "not a resolved value: PostgreSQL resolves database, role and session scopes per "
                + "connection at connect time, and the catalog records only the instruction. No unit, "
                + "default or context is carried on these rows because the catalog does not hold them "
                + "- read those off the server-wide row for the same setting name.";
            return node.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_server_config", ex);
        }
    }

    [McpServerTool(Name = "get_pg_server_config_changes"), Description("Gets PostgreSQL configuration parameters whose value CHANGED during the window, newest first, with the old and new value side by side. This is the read that answers 'this got slow sometime last month, what changed' - and nothing else in the stack can reconstruct it after the fact, because a configuration history that was not recorded cannot be recovered from the server. A setting appearing for the first time is deliberately NOT reported as a change: the first snapshot after an upgrade, or after an extension is loaded, would otherwise manufacture hundreds of changes nobody made. Session-scoped rows are excluded, so a monitoring reconnect does not read as a configuration change. PER-DATABASE AND PER-ROLE OVERRIDES (ALTER DATABASE/ROLE SET) are reported too, interleaved with the server-wide rows by changed_at: such a row carries database_name and/or role_name (only the ones it is scoped to) and change_kind - changed, set (old_value null: the override appeared) or reset (new_value null: it was removed) - while a server-wide row carries neither, and overrides already present when the collector first read them are not reported as set.")]
    public static async Task<string> GetPgServerConfigChanges(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (one week).")] int hours_back = 168,
        [Description("Maximum changes to return. Default 100.")] int limit = 100,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;

        try
        {
            /* #3653 (the #3541 A3 class): limit + 1 as the fetch, the extra row as the observed truncation
               signal, so a week with exactly `limit` changes is not reported as holding more. */
            var fetched = await DarlingPgServerConfigReader.GetConfigChangesAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit + 1);

            if (fetched.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_server_config")
                    ?? McpHelpers.Status(
                        "no_changes",
                        $"No configuration parameter changed value on {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). That is a real finding rather than missing data - this "
                        + "read compares consecutive snapshots, so an unchanged server produces no rows.");
            }

            var truncated = fetched.Count > limit;
            var rows = truncated ? fetched.Take(limit).ToList() : fetched;

            /* #3937: the rows are typed `object` so one list can carry two shapes. A server-wide row is the SAME
               anonymous shape it always was, and System.Text.Json serializes an object-typed element by its
               runtime type, so its bytes do not move. An override row is built as a JsonObject because
               McpHelpers.JsonOptions writes nulls: a shared shape with database_name / role_name / change_kind
               would put `"database_name": null` on every server-wide row, and an override scoped to one role
               would say `"database_name": null` where the truth is "every database". So a scoped row names only
               the scope it has, plus change_kind, and omits unit / context / description, which the
               pg_db_role_setting catalog does not hold (read them off the server-wide row for the same name, as
               get_pg_server_config's database_overrides_note says). old_value and new_value stay on it even
               when NULL, because there NULL is the answer: set had nothing before, reset has nothing after. */
            var page = new
            {
                server = resolved.ServerName,
                hours_back,
                status = "config_changes",
                change_count = rows.Count,
                truncated,
                note = "changed_at is the time of the snapshot that FIRST reported the new value, so the "
                     + "change happened at some point in the hour before it - this collector runs hourly. "
                     + "A setting appearing for the first time is not reported here.",
                changes = rows.Select(r => r.DatabaseName is null && r.RoleName is null
                    ? (object)new
                    {
                        changed_at = r.ChangedAtUtc,
                        name = r.Name,
                        old_value = r.OldValue,
                        new_value = r.NewValue,
                        unit = r.Unit,
                        source = r.Source,
                        context = r.Context,
                        description = r.ShortDescription,
                    }
                    : ScopedChange(r)),
            };

            /* The override note is ATTACHED, like get_pg_server_config's database_overrides, so a page with no
               override row - every page on a cluster with none, and every page before V138 - is byte-identical
               to what this tool returned before #3937. */
            if (rows.All(r => r.DatabaseName is null && r.RoleName is null))
            {
                return JsonSerializer.Serialize(page, McpHelpers.JsonOptions);
            }

            var node = JsonSerializer.SerializeToNode(page, McpHelpers.JsonOptions)?.AsObject()
                ?? throw new InvalidOperationException("the config-changes page did not serialize to a JSON object");
            node["scoped_changes_note"] = "Rows carrying database_name and/or role_name are per-database or per-role "
                + "overrides (ALTER DATABASE/ROLE SET), interleaved with the server-wide rows by changed_at. "
                + "change_kind says what happened between two consecutive snapshots of the server: changed (the "
                + "override's value moved), set (it appeared - old_value is null) or reset (it was removed - "
                + "new_value is null). An override already present on the first snapshot after the collector "
                + "began reading overrides is not reported as set: nobody set it then, the collector started "
                + "seeing it. An override row has no unit, context or description; read those off the "
                + "server-wide setting of the same name.";
            return node.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_server_config_changes", ex);
        }
    }

    /// <summary>
    /// One override change as the payload renders it (#3937): the scope names it HAS and none it does not, then
    /// change_kind, then the value pair. Property order follows the server-wide row's where the two share names.
    /// </summary>
    private static JsonObject ScopedChange(DarlingPgServerConfigReader.PgConfigChangeRow r)
    {
        var o = new JsonObject
        {
            ["changed_at"] = JsonSerializer.SerializeToNode(r.ChangedAtUtc, McpHelpers.JsonOptions),
            ["name"] = r.Name,
        };
        if (r.DatabaseName is not null)
        {
            o["database_name"] = r.DatabaseName;
        }

        if (r.RoleName is not null)
        {
            o["role_name"] = r.RoleName;
        }

        o["change_kind"] = r.ChangeKind;
        o["old_value"] = r.OldValue;
        o["new_value"] = r.NewValue;
        o["source"] = r.Source;
        return o;
    }

}
