/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The logging-settings audit for a PostgreSQL target (#3607): which of the server's logging GUCs are
/// producing the lines they can, judged facet by facet from the stored <c>pg_settings</c> snapshot, with what
/// each unlocks, the recommended value AND its cost, and the remedy in the syntax the hosting flavour needs.
///
/// <para><b>The failure this fixes is the one plan-capture readiness fixed for auto_explain, over the rest
/// of the log.</b> A target with <c>log_lock_waits</c>, <c>log_temp_files</c>,
/// <c>log_autovacuum_min_duration</c>, <c>log_checkpoints</c>, <c>log_connections</c>,
/// <c>log_disconnections</c> and <c>log_min_duration_statement</c> all off looks IDENTICAL to a fully
/// instrumented one from every read this product has — the same counters, the same sampled views — and
/// the operator learns the difference at incident time, when the log they reach for holds nothing. This
/// answers "is this target telling us everything it could" before that moment, and answers it per setting,
/// because each one has a different cost and a different consumer and a single "logging: partial" would tell
/// nobody what to do.</para>
///
/// <para><b>It is a READ, not a collector, and the shape follows from what it judges.</b>
/// <c>PgPlanCaptureReadinessCollector</c> persists its facets because judging them means PROBING the target
/// — whether <c>auto_explain</c> is in <c>shared_preload_libraries</c>, whether an <c>auto_explain.*</c> GUC
/// even exists — and that probe is worth a history. Every setting here is a plain core GUC that
/// <c>PgServerConfigCollector</c> already stores hourly with its value, source, unit and context, so the
/// judgment is a pure function over rows the store holds, computed when asked. No new table, no schema rung,
/// nothing for a second collector to disagree with the first about. The snapshot's own
/// <c>collection_time</c> is the audit's <c>captured_at</c>.</para>
///
/// <para><b>Four verdicts, and <c>partial</c> is not a lesser <c>instrumented</c>.</b> <c>instrumented</c>
/// means the setting is producing every line it can; <c>off</c> means none; <c>unknown</c> means the setting
/// is not in the stored snapshot and NOTHING is inferred about it; <c>partial</c> means a THRESHOLD is
/// filtering — statements faster than N ms, temp files under N kB, autovacuum runs shorter than N ms — and
/// what falls below it is stated on the row. For two of the settings the threshold is the RECOMMENDED
/// posture: a <c>log_min_duration_statement</c> of 0 logs every statement the server runs, and #2565
/// measured the capture-everything shape of that mechanism at 31 percent of throughput. So the summary
/// names the <c>off</c> and <c>unknown</c> settings as the actionable ones, and a <c>partial</c> row's
/// <c>cost_note</c> says whether its threshold is the recommendation or a compromise.</para>
///
/// <para><b>Consumers are named honestly, "planned" and "shipped" alike.</b> The issue's own sequencing note
/// says this audit earns its keep once the log pipeline (#3601) and its first parser families (#3602 temp
/// files, #3603 autovacuum) consume the lines. The two parser families have landed and their facets say
/// <c>SHIPPED</c> with the read that consumes the lines; the rest still say <c>PLANNED</c> where that is the
/// truth, beside the read that exists today and what it cannot see (the #3601 pipeline's own three families
/// — lock waits, connections, checkpoints — ship in <c>get_pg_log_events</c> too, and re-wording those facets
/// is the pipeline's follow-up, pinned by <c>DarlingMcpPgLoggingAuditToolsTests</c> so the edit is deliberate).
/// The setting is still worth turning on before a consumer lands: the log it fills is the one somebody
/// opens at incident time, whichever tool reads it.</para>
///
/// <para><b>Plan capture's own settings are shown, not re-judged.</b> <c>shared_preload_libraries</c>,
/// <c>auto_explain.log_min_duration</c>, <c>log_line_prefix</c> and <c>lc_messages</c> appear in the same
/// snapshot and are listed for completeness with the readiness facet that owns each, because
/// <c>get_pg_plan_capture_readiness</c> already judges them with the trap each one carries (a loaded library
/// capturing nothing at -1, a placeholder GUC on a server that never loaded the module, a translated
/// message catalogue) and a second judgment here would be a second place for those to drift.
/// <c>lc_messages</c> matters to every row above it — the lines these settings produce are English text the
/// parsers match — which is why it is in the list at all.</para>
///
/// <para><b>Hosting flavour comes from the snapshot, not from the registry.</b> The store's engine token
/// (<c>MonitoredEngineKind</c>) separates Aurora from everything else, and "everything else" is both
/// self-hosted PostgreSQL and RDS for PostgreSQL — which need OPPOSITE remedies: <c>ALTER SYSTEM</c> plus a
/// reload on one, a parameter group on the other, where <c>ALTER SYSTEM</c> is refused. Any <c>rds.*</c> GUC
/// in the snapshot (<c>rds.extensions</c>, <c>rds.superuser_reserved_connections</c>, … — RDS and Aurora
/// both carry them) is evidence in the same rows this already reads, and the response says which way it
/// decided and on what.</para>
/// </summary>
public static class DarlingPgLoggingAudit
{
    /// <summary>The verdict vocabulary on the wire — the header says what each means.</summary>
    public const string Instrumented = "instrumented";
    public const string Partial = "partial";
    public const string Off = "off";
    public const string Unknown = "unknown";

    /// <summary>The tool that judges the plan-capture settings this audit only lists.</summary>
    public const string ReadinessTool = "get_pg_plan_capture_readiness";

    /// <param name="Setting">The GUC.</param>
    /// <param name="Value">What the snapshot holds, verbatim, or null when the setting is not in it.</param>
    /// <param name="Unit">The unit <c>pg_settings</c> reports for a numeric setting.</param>
    /// <param name="DefaultValue">The compiled-in default, from the same snapshot.</param>
    /// <param name="Source">Where the value came from.</param>
    /// <param name="ChangeNeeds">Reload or restart, from the setting's context.</param>
    /// <param name="Verdict">One of the four constants above.</param>
    /// <param name="Unlocks">What telemetry the setting produces, and what this product has INSTEAD today.</param>
    /// <param name="Consumer">The Darling family that reads the lines, marked planned where it does not ship.</param>
    /// <param name="Recommended">The value to set.</param>
    /// <param name="CostNote">What the recommended value costs, and when to deviate from it.</param>
    /// <param name="Remedy">The change, in the syntax this server's hosting flavour needs — or why none is needed.</param>
    /// <param name="ScopeNote">Set when the value came from a per-role or per-database override the monitoring
    /// connection resolved, so the server-wide value may differ.</param>
    /// <param name="PendingRestart">The file and the running server disagree about this setting, so the value
    /// judged here is the RUNNING one and changes at the next restart.</param>
    /// <param name="RestartNote">The consequence of <paramref name="PendingRestart"/>, spelled out on the row;
    /// null when the two agree.</param>
    public sealed record Facet(
        string Setting,
        string? Value,
        string? Unit,
        string? DefaultValue,
        string? Source,
        string? ChangeNeeds,
        string Verdict,
        string Unlocks,
        string Consumer,
        string Recommended,
        string CostNote,
        string Remedy,
        string? ScopeNote,
        bool PendingRestart,
        string? RestartNote);

    /// <summary>A plan-capture setting shown as observed, with the readiness facet that judges it.</summary>
    public sealed record ReadinessSetting(string Setting, string? Value, string? Source, string ReadinessFacet);

    /// <param name="CapturedAt">The snapshot's collection time — the one stamp every row shares.</param>
    /// <param name="Managed">True when the snapshot carries <c>rds.*</c> parameters.</param>
    /// <param name="HostingEvidence">What the flavour decision rested on.</param>
    /// <param name="Facets">One per judged setting, in the order an operator reaches for them.</param>
    /// <param name="JudgedByReadiness">The plan-capture settings, listed not judged.</param>
    public sealed record Result(
        DateTime CapturedAt,
        bool Managed,
        string HostingEvidence,
        IReadOnlyList<Facet> Facets,
        IReadOnlyList<ReadinessSetting> JudgedByReadiness);

    /// <summary>
    /// The settings this audit judges, in the order they are reported: what somebody reaches for FIRST when
    /// a server is slow — the statements — then the locks, the spills, the maintenance, the checkpoints, and
    /// the connection churn. There is no causal chain between them (unlike readiness, where the library gates
    /// the threshold), so the order is the reader's, and the note on the response says so.
    /// </summary>
    public static readonly IReadOnlyList<string> JudgedSettings = new[]
    {
        "log_min_duration_statement",
        "log_lock_waits",
        "log_temp_files",
        "log_autovacuum_min_duration",
        "log_checkpoints",
        "log_connections",
        "log_disconnections",
    };

    /// <summary>The plan-capture settings listed for completeness, with the readiness facet owning each.</summary>
    public static readonly IReadOnlyList<(string Setting, string ReadinessFacet)> ReadinessSettings = new[]
    {
        ("shared_preload_libraries", "library_loaded"),
        ("auto_explain.log_min_duration", "capture_threshold"),
        ("log_line_prefix", "plan_attribution"),
        ("lc_messages", "message_locale"),
    };

    /// <summary>
    /// Judges one server's newest snapshot. The rows are what
    /// <see cref="DarlingPgLoggingAuditReader.GetNewestSnapshotAsync"/> returned — every non-session setting
    /// at the newest <c>collection_time</c> — and MUST be non-empty; an empty snapshot is the tool's
    /// <c>empty</c>/<c>not_collected</c> path, not an audit of nothing.
    /// </summary>
    public static Result Audit(IReadOnlyList<DarlingPgLoggingAuditReader.PgLoggingSettingRow> snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (snapshot.Count == 0)
        {
            throw new ArgumentException("An empty snapshot cannot be audited; report it as not collected.", nameof(snapshot));
        }

        var byName = new Dictionary<string, DarlingPgLoggingAuditReader.PgLoggingSettingRow>(StringComparer.Ordinal);
        foreach (var row in snapshot)
        {
            /* First wins. The reader excludes session-scoped sources, so two rows for one name at one
               collection_time should not happen; if a store ever holds them, the audit must not throw over
               a duplicate the collector wrote. */
            byName.TryAdd(row.Name, row);
        }

        var capturedAt = snapshot.Max(r => r.CollectionTime);

        /* The hosting flavour, from evidence in the rows rather than from a registry token that cannot
           separate RDS from self-hosted. Counted so the response can say how much evidence there was. */
        var rdsParameters = byName.Keys.Count(n => n.StartsWith("rds.", StringComparison.Ordinal));
        var managed = rdsParameters > 0;
        var hostingEvidence = managed
            ? $"{rdsParameters} rds.* parameter(s) in the snapshot, which only RDS and Aurora carry - remedies are "
              + "worded for a parameter group, where ALTER SYSTEM is refused."
            : "no rds.* parameter in the snapshot, so this is not RDS or Aurora - remedies are worded as ALTER "
              + "SYSTEM plus a reload. If this server IS managed by a provider that hides its own parameters, "
              + "translate each remedy to that provider's parameter surface.";

        var deadlockTimeout = Describe(byName, "deadlock_timeout");

        var facets = new List<Facet>(JudgedSettings.Count)
        {
            MinDurationStatement(byName, managed),
            LockWaits(byName, managed, deadlockTimeout),
            TempFiles(byName, managed),
            AutovacuumMinDuration(byName, managed),
            Checkpoints(byName, managed),
            Connections(byName, managed),
            Disconnections(byName, managed),
        };

        var readiness = ReadinessSettings
            .Select(s => byName.TryGetValue(s.Setting, out var row)
                ? new ReadinessSetting(s.Setting, row.Setting, row.Source, s.ReadinessFacet)
                : new ReadinessSetting(s.Setting, null, null, s.ReadinessFacet))
            .ToList();

        return new Result(capturedAt, managed, hostingEvidence, facets, readiness);
    }

    /* ───────────────────────── the facets ───────────────────────── */

    private static Facet MinDurationStatement(
        IReadOnlyDictionary<string, DarlingPgLoggingAuditReader.PgLoggingSettingRow> byName, bool managed)
    {
        const string Setting = "log_min_duration_statement";
        var row = Find(byName, Setting);
        var threshold = Threshold(row);

        /* -1 off, 0 everything, N a threshold. The RECOMMENDED state is the threshold, and the verdict is
           still partial - the header says why: partial describes the lines, and this row's cost_note says
           the filter is the point. */
        var verdict = row is null ? Unknown
            : threshold is null ? Unknown
            : threshold < 0 ? Off
            : threshold == 0 ? Instrumented
            : Partial;

        var cost = verdict switch
        {
            Instrumented =>
                "0 logs EVERY statement the server runs, with its text. That is the capture-everything shape "
                + "#2565 measured for auto_explain at 31 percent of throughput and 772 MB of log in 20 seconds "
                + "(pgbench, 8 clients). The statement log at 0 emits one entry per statement exactly as "
                + "auto_explain at 0 does - smaller entries with no plan body, but no fewer of them. Move to a "
                + "millisecond threshold; the fast statements are the volume and nobody reads them.",
            Partial =>
                $"Statements faster than {threshold} ms write nothing, which is the intended trade: the slow "
                + "ones are the question and the fast ones are the volume. This is the recommended posture, "
                + "not a gap. To see a SAMPLE of the faster ones without the volume, log_min_duration_sample "
                + "with log_statement_sample_rate is the sampling form; it is a separate setting and not judged "
                + "here.",
            Off =>
                "Off costs nothing and records nothing: a 40-second statement cancelled by its client leaves "
                + "no trace anywhere but this log line. A threshold sized to the workload - well above the "
                + "normal statement time, so that only the outliers write - costs one line per outlier.",
            _ => UnknownNote(row),
        };

        return new Facet(
            Setting,
            row?.Setting, row?.Unit, row?.BootValue, row?.Source, ChangeNeeds(row),
            verdict,
            Unlocks: "One LOG line per EXECUTION that ran longer than the threshold, carrying the duration and "
                     + "the statement text. What this product has instead is pg_stat_statements through "
                     + "get_pg_top_queries: per-SHAPE aggregates that can say a shape averages 200 ms and "
                     + "never that one execution took 40 seconds at 03:07 - the log line is the only record "
                     + "of the individual slow execution.",
            Consumer: "PLANNED - no Darling family reads statement-duration lines yet; #3601's log pipeline is "
                      + "where they would land, and the statement text carries literals, so the same redaction "
                      + "pass plan capture applies before storage is a precondition of storing them at all. "
                      + "get_pg_top_queries is the aggregate the lines would sharpen. Until then the line is "
                      + "what an operator finds in the server log at incident time.",
            Recommended: "A millisecond threshold sized to the workload, never 0 - 1000 is a common starting "
                         + "point on an OLTP workload; lower it as the volume proves tolerable. "
                         + "auto_explain.log_min_duration is the separate threshold for PLANS and is judged by "
                         + ReadinessTool + ".",
            CostNote: cost,
            Remedy: Remedy(Setting, "1000", row, managed, verdict, alreadyRight: verdict == Partial),
            ScopeNote: ScopeNote(row),
            PendingRestart: row?.PendingRestart ?? false,
            RestartNote: RestartNote(row));
    }

    private static Facet LockWaits(
        IReadOnlyDictionary<string, DarlingPgLoggingAuditReader.PgLoggingSettingRow> byName, bool managed,
        string deadlockTimeout)
    {
        const string Setting = "log_lock_waits";
        var row = Find(byName, Setting);
        var on = Bool(row);

        var verdict = row is null || on is null ? Unknown : on.Value ? Instrumented : Off;

        return new Facet(
            Setting,
            row?.Setting, row?.Unit, row?.BootValue, row?.Source, ChangeNeeds(row),
            verdict,
            Unlocks: $"A LOG line whenever a session waits longer than deadlock_timeout ({deadlockTimeout}) for a "
                     + "lock, naming the waiting process, the lock it wanted and the statement that wanted it - "
                     + "the ENGINE-recorded record of lock waits. What this product has instead is get_pg_blocking "
                     + "from pg_blocking, which SAMPLES pg_locks and pg_stat_activity on a cadence: a wait that "
                     + "starts and ends between two samples is invisible to it and would be in this line.",
            Consumer: "PLANNED - #3601 names lock-wait reports among the families the log pipeline would "
                      + "classify. get_pg_blocking is the sampled read that exists today, and its own response "
                      + "says how many samples its 'no blocking' rests on.",
            Recommended: "on.",
            CostNote: verdict == Unknown ? UnknownNote(row)
                : "One line per wait longer than deadlock_timeout - negligible on a workload that is not "
                      + "already lock-bound, and on one that is, the volume is itself the finding. Do NOT lower "
                      + "deadlock_timeout to make this fire sooner: that is also how often the deadlock "
                      + "detector runs, and it is a lock-heavy operation in its own right.",
            Remedy: Remedy(Setting, "on", row, managed, verdict, alreadyRight: verdict == Instrumented),
            ScopeNote: ScopeNote(row),
            PendingRestart: row?.PendingRestart ?? false,
            RestartNote: RestartNote(row));
    }

    private static Facet TempFiles(
        IReadOnlyDictionary<string, DarlingPgLoggingAuditReader.PgLoggingSettingRow> byName, bool managed)
    {
        const string Setting = "log_temp_files";
        var row = Find(byName, Setting);
        var threshold = Threshold(row);

        var verdict = row is null ? Unknown
            : threshold is null ? Unknown
            : threshold < 0 ? Off
            : threshold == 0 ? Instrumented
            : Partial;

        var cost = verdict switch
        {
            Instrumented =>
                "0 logs EVERY temporary file at the moment it is deleted, including the small ones. On a "
                + "workload whose sorts sit just over work_mem that is a line per spill, constantly; if that is "
                + "this server, set a kilobyte threshold (10240 is 10 MB) and accept that spills under it are "
                + "unseen. On most workloads 0 is cheap and is the recommendation.",
            Partial =>
                $"Temporary files under {threshold} kB write nothing. That is a deliberate trade for a workload "
                + "that spills small files constantly; if this server does not, 0 sees everything at little "
                + "cost. Per-event attribution works from whatever crosses the line either way.",
            Off =>
                "Off records nothing: the counters say a database spilled 4 GB in an hour and the log says "
                + "nothing about which statement did it or when. 0 costs one line per temp file; on a workload "
                + "that spills constantly a kilobyte threshold bounds it.",
            _ => UnknownNote(row),
        };

        return new Facet(
            Setting,
            row?.Setting, row?.Unit, row?.BootValue, row?.Source, ChangeNeeds(row),
            verdict,
            Unlocks: "One LOG line per temporary file, with its size and the statement that wrote it - "
                     + "per-EVENT spill attribution. What this product has instead is the counter shadow of "
                     + "that: per-database temp_files / temp_bytes deltas (get_pg_database_stats, "
                     + "get_pg_database_trend) and per-shape temp_blks_* from pg_stat_statements "
                     + "(get_pg_top_queries). Between them you can know a database spilled and that a shape "
                     + "spills - never that THIS execution spilled 4 GB at 03:07, which is the question when a "
                     + "disk fills.",
            Consumer: "SHIPPED - #3602: get_pg_log_events with family temp_file stores one event per spilled "
                      + "file with its exact bytes and the fingerprint of the statement that spilled (the "
                      + "statement itself is never stored - plan capture's redaction pass runs first). "
                      + "get_pg_database_stats still carries the spill FINDING from the counters.",
            Recommended: "0 (every spill), or a kilobyte threshold on a workload that spills small files constantly.",
            CostNote: cost,
            Remedy: Remedy(Setting, "0", row, managed, verdict, alreadyRight: verdict is Instrumented or Partial),
            ScopeNote: ScopeNote(row),
            PendingRestart: row?.PendingRestart ?? false,
            RestartNote: RestartNote(row));
    }

    private static Facet AutovacuumMinDuration(
        IReadOnlyDictionary<string, DarlingPgLoggingAuditReader.PgLoggingSettingRow> byName, bool managed)
    {
        const string Setting = "log_autovacuum_min_duration";
        var row = Find(byName, Setting);
        var threshold = Threshold(row);

        var verdict = row is null ? Unknown
            : threshold is null ? Unknown
            : threshold < 0 ? Off
            : threshold == 0 ? Instrumented
            : Partial;

        /* PostgreSQL 15 moved the default from -1 to 600000 (ten minutes). A server sitting on that default
           at a positive threshold is the shape worth naming: it sees only the outlier runs. The judgment is
           PostgreSQL's OWN - source = 'default' is the server saying nobody set it - and not a text
           comparison against boot_val, for the reason DarlingPgServerConfigReader.CurrentConfigSql gives:
           an administrator who writes 600000 into postgresql.conf has made a choice that happens to equal
           the boot value, and calling that choice a default would attribute it to inaction. */
        var atDefault = row is not null && threshold > 0
            && string.Equals(row.Source, "default", StringComparison.Ordinal);

        var cost = verdict switch
        {
            Instrumented =>
                "0 logs every autovacuum and autoanalyze run. The line rate is the rate at which tables get "
                + "vacuumed, bounded by autovacuum_max_workers (three by default) each finishing one table before "
                + "starting the next - small on most servers, and the cheapest setting on this list for what it "
                + "returns. A database with thousands of tiny tables is the exception where a threshold earns "
                + "its place.",
            Partial =>
                $"Runs shorter than {threshold} ms write nothing."
                + (atDefault
                    ? " This is PostgreSQL's own default since 15 (ten minutes), and on most tables that is "
                      + "every run: a cost history that sees only the outliers cannot say what a NORMAL run "
                      + "costs, which is the baseline the outliers are judged against. 0 is cheap here."
                    : " Whether that is the right cut depends on what you want the history for: outliers "
                      + "only, or a baseline of what a normal run costs. 0 is cheap here."),
            Off =>
                "Off records nothing about what any run cost. 0 costs one line per run, and the run rate is "
                + "bounded by the worker count, so the volume is small on all but a server with thousands of "
                + "tiny tables.",
            _ => UnknownNote(row),
        };

        return new Facet(
            Setting,
            row?.Setting, row?.Unit, row?.BootValue, row?.Source, ChangeNeeds(row),
            verdict,
            Unlocks: "One LOG line per autovacuum or autoanalyze run that took longer than the threshold: "
                     + "pages and tuples removed, buffer hits and misses, read and write rates, WAL usage and "
                     + "elapsed time - what the run COST. What this product has instead is pg_stat_user_tables "
                     + "through get_pg_autovacuum_health: whether autovacuum ran and when, never what it cost, "
                     + "so a run that takes 40 minutes of I/O in the business peak reads as healthy there.",
            Consumer: "SHIPPED - #3603: get_pg_log_events with family autovacuum stores one event per completed "
                      + "run with its duration, pages, tuples, buffers and WAL lifted, and get_pg_autovacuum_health "
                      + "shows them per table as recent_runs beside the whether-it-ran catalog half.",
            Recommended: "0 (every run).",
            CostNote: cost,
            Remedy: Remedy(Setting, "0", row, managed, verdict, alreadyRight: verdict == Instrumented),
            ScopeNote: ScopeNote(row),
            PendingRestart: row?.PendingRestart ?? false,
            RestartNote: RestartNote(row));
    }

    private static Facet Checkpoints(
        IReadOnlyDictionary<string, DarlingPgLoggingAuditReader.PgLoggingSettingRow> byName, bool managed)
    {
        const string Setting = "log_checkpoints";
        var row = Find(byName, Setting);
        var on = Bool(row);

        var verdict = row is null || on is null ? Unknown : on.Value ? Instrumented : Off;

        return new Facet(
            Setting,
            row?.Setting, row?.Unit, row?.BootValue, row?.Source, ChangeNeeds(row),
            verdict,
            Unlocks: "A LOG line per checkpoint with what it did: buffers written, files synced, write, sync "
                     + "and total time, WAL distance - the CAUSE side of a checkpoint I/O storm, per "
                     + "checkpoint. What this product has instead is get_pg_write_stats over the checkpointer "
                     + "counters: timed versus requested and buffers written across a window, never the "
                     + "duration of one checkpoint.",
            Consumer: "PLANNED - #3601's log pipeline. get_pg_write_stats reads the checkpoint counters today.",
            Recommended: "on - PostgreSQL 15 made it the default.",
            CostNote: verdict == Unknown ? UnknownNote(row)
                : "One or two lines per checkpoint, and a checkpoint happens at most every "
                      + "checkpoint_timeout (five minutes by default) unless something requests one - "
                      + "negligible. A server reporting off has it set that way in a file or parameter group, "
                      + "or is on a major before 15 where off was the default.",
            Remedy: Remedy(Setting, "on", row, managed, verdict, alreadyRight: verdict == Instrumented),
            ScopeNote: ScopeNote(row),
            PendingRestart: row?.PendingRestart ?? false,
            RestartNote: RestartNote(row));
    }

    private static Facet Connections(
        IReadOnlyDictionary<string, DarlingPgLoggingAuditReader.PgLoggingSettingRow> byName, bool managed)
    {
        const string Setting = "log_connections";
        var row = Find(byName, Setting);

        /* A boolean through PostgreSQL 17 and a STRING LIST in 18 (receipt, authentication, authorization,
           setup_durations, all), where on/true/yes/1 still mean all and the empty string means off. Measured
           on 18.4: the value is stored verbatim as written - 'on', 'true', '1', 'all', 'receipt,authentication'
           - so this reads any non-empty, non-false value as producing lines and shows the value itself. */
        var verdict = row is null ? Unknown
            : ConnectionLogging(row.Setting) is bool on ? (on ? Instrumented : Off)
            : Unknown;

        return new Facet(
            Setting,
            row?.Setting, row?.Unit, row?.BootValue, row?.Source, ChangeNeeds(row),
            verdict,
            Unlocks: "A LOG line per connection as it is received, authenticated and authorized - the only "
                     + "record of connection CHURN and of who a FATAL authentication failure was. What this "
                     + "product has instead is pg_stat_database's numbackends, a gauge, and its sessions "
                     + "counter, a cumulative total: neither says who connected when, or that 400 connections "
                     + "arrived in the minute before the incident.",
            Consumer: "PLANNED - #3601 names connection churn among the log pipeline's families.",
            Recommended: "on (PostgreSQL 18: 'all', or a list such as receipt,authentication).",
            CostNote: verdict == Unknown ? UnknownNote(row)
                : "On a POOLED workload connections are rare and this costs nothing. On an unpooled one - a "
                      + "connection per request - it is a line per request, and that volume is itself the "
                      + "finding: the pool that is missing. On 18 the list form narrows the lines to the "
                      + "stages you want.",
            Remedy: Remedy(Setting, "on", row, managed, verdict, alreadyRight: verdict == Instrumented),
            ScopeNote: ScopeNote(row),
            PendingRestart: row?.PendingRestart ?? false,
            RestartNote: RestartNote(row));
    }

    private static Facet Disconnections(
        IReadOnlyDictionary<string, DarlingPgLoggingAuditReader.PgLoggingSettingRow> byName, bool managed)
    {
        const string Setting = "log_disconnections";
        var row = Find(byName, Setting);
        var on = Bool(row);

        var verdict = row is null || on is null ? Unknown : on.Value ? Instrumented : Off;

        return new Facet(
            Setting,
            row?.Setting, row?.Unit, row?.BootValue, row?.Source, ChangeNeeds(row),
            verdict,
            Unlocks: "A LOG line per session end WITH the session's duration - the half that turns connection "
                     + "lines into session lifetimes, so a churning application shows as thousands of "
                     + "two-second sessions rather than as a connection count that looks stable.",
            Consumer: "PLANNED - #3601, beside log_connections.",
            Recommended: "on, together with log_connections.",
            CostNote: verdict == Unknown ? UnknownNote(row)
                : "The same profile as log_connections: one line per session end, nothing on a pooled "
                      + "workload, a line per request on an unpooled one.",
            Remedy: Remedy(Setting, "on", row, managed, verdict, alreadyRight: verdict == Instrumented),
            ScopeNote: ScopeNote(row),
            PendingRestart: row?.PendingRestart ?? false,
            RestartNote: RestartNote(row));
    }

    /* ───────────────────────── shared pieces ───────────────────────── */

    private const string NotInSnapshot =
        "This setting is not in the stored snapshot, so nothing is claimed about it - not inferred from the "
        + "default, not inferred from the major version. get_pg_server_config shows what the snapshot holds.";

    /// <summary>
    /// The two ways a verdict is <c>unknown</c>, told apart on the row: the setting is not in the snapshot at
    /// all, or it is there with a value this audit cannot read as the boolean or integer PostgreSQL renders
    /// for it. The second is close to unreachable — <c>pg_settings</c> renders well-formed values — but a
    /// message that said "not in the snapshot" about a row that is plainly in it would be false, and the
    /// contract this type's header states ("unknown means the setting is not in the stored snapshot") is
    /// what the first sentence below keeps true by naming the exception.
    /// </summary>
    private static string UnknownNote(DarlingPgLoggingAuditReader.PgLoggingSettingRow? row) => row is null
        ? NotInSnapshot
        : $"This setting IS in the stored snapshot but its value '{row.Setting}' is not one this audit can read "
          + "as the boolean or integer PostgreSQL renders for it, so nothing is claimed about it. "
          + "get_pg_server_config shows the raw row; if this recurs, the collector's rendering has changed "
          + "and the audit's parse needs to learn it.";

    private static DarlingPgLoggingAuditReader.PgLoggingSettingRow? Find(
        IReadOnlyDictionary<string, DarlingPgLoggingAuditReader.PgLoggingSettingRow> byName, string name) =>
        byName.TryGetValue(name, out var row) ? row : null;

    /// <summary>A setting's value with its unit, for prose — <c>1000 ms</c> — or a plain statement that
    /// the snapshot does not have it.</summary>
    private static string Describe(
        IReadOnlyDictionary<string, DarlingPgLoggingAuditReader.PgLoggingSettingRow> byName, string name)
    {
        var row = Find(byName, name);
        if (row?.Setting is null)
        {
            return "not in the snapshot";
        }

        return string.IsNullOrEmpty(row.Unit) ? row.Setting : $"{row.Setting} {row.Unit}";
    }

    /// <summary>
    /// The integer a threshold GUC holds. <c>pg_settings.setting</c> renders integers in the setting's base
    /// unit with no suffix (<c>600000</c> with <c>unit = ms</c>), so this is a plain parse; anything else is
    /// null and the caller says <c>unknown</c> rather than guessing.
    /// </summary>
    private static long? Threshold(DarlingPgLoggingAuditReader.PgLoggingSettingRow? row)
    {
        /* A block body, not an expression with a property pattern: TsqlConventionGuardTests' member scan
           reads a `{ }` pattern as the member's body and stops short, which strands everything after it. */
        var text = row?.Setting;
        if (text is null)
        {
            return null;
        }

        return long.TryParse(text.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;
    }

    /// <summary>The spellings PostgreSQL accepts for a boolean GUC, as <c>pg_settings</c> renders them.</summary>
    private static bool? Bool(DarlingPgLoggingAuditReader.PgLoggingSettingRow? row) => BoolText(row?.Setting);

    private static bool? BoolText(string? text)
    {
        if (text is null)
        {
            return null;
        }

        switch (text.Trim().ToLowerInvariant())
        {
            case "on":
            case "true":
            case "yes":
            case "1":
                return true;
            case "off":
            case "false":
            case "no":
            case "0":
                return false;
            default:
                return null;
        }
    }

    /// <summary>
    /// <c>log_connections</c> across the 17/18 boundary: the boolean spellings first, then the 18 list —
    /// empty is off, anything else non-empty is a stage list and produces lines.
    /// </summary>
    internal static bool? ConnectionLogging(string? text)
    {
        if (text is null)
        {
            return null;
        }

        if (BoolText(text) is bool asBool)
        {
            return asBool;
        }

        return text.Trim().Length > 0;
    }

    /// <summary>Reload or restart, from the GUC's context — the fact <c>get_pg_server_config</c> already
    /// exposes as <c>requires_restart_to_change</c>, worded for a remedy.</summary>
    private static string? ChangeNeeds(DarlingPgLoggingAuditReader.PgLoggingSettingRow? row) => row?.Context switch
    {
        null => null,
        "postmaster" => "restart",
        "superuser-backend" or "backend" => "reload; applies to connections opened after it",
        _ => "reload",
    };

    /// <summary>
    /// A per-role or per-database override resolved on the MONITORING connection is not the server's value —
    /// the limit the readiness collector states for <c>lc_messages</c>, and it holds for every GUC here. A
    /// log line is written by the backend that raised it, under THAT backend's resolved value.
    /// </summary>
    private static string? ScopeNote(DarlingPgLoggingAuditReader.PgLoggingSettingRow? row) => row?.Source switch
    {
        "user" or "database" or "database user" =>
            $"source is '{row.Source}': this value came from a per-role or per-database override that the "
            + "monitoring connection resolved, so the server-wide value may differ, and every other backend "
            + "logs under its OWN resolved value. get_pg_server_config shows the setting's source; ALTER ROLE "
            + "/ ALTER DATABASE ... RESET removes the override.",
        _ => null,
    };

    /// <summary>
    /// <c>pending_restart</c> is the one row where the value judged is provably NOT the value the server will
    /// have: postgresql.conf (or ALTER SYSTEM's file) already holds something else and the running server
    /// has not restarted. get_pg_server_config reports it loudly for the same reason; here it matters twice
    /// over, because the remedy is written against the running value and a restart may deliver the change,
    /// or a different one, with no deployment to explain it. Which value the file holds is not in the
    /// snapshot - pg_settings does not carry it - so the note says the disagreement exists and where to look,
    /// and does not guess the direction.
    /// </summary>
    private static string? RestartNote(DarlingPgLoggingAuditReader.PgLoggingSettingRow? row)
    {
        /* A block body for the reason Threshold has one: a `{ }` property pattern reads as the member's
           body to TsqlConventionGuardTests' scan. */
        if (row is null || !row.PendingRestart)
        {
            return null;
        }

        return "pending_restart is TRUE: the configuration file already holds a different value for this setting "
               + "and the running server has not restarted, so the value judged here is the RUNNING one and it "
               + "changes at the next restart with no deployment to explain it. pg_settings does not carry the "
               + "file's value, so which way it changes is not knowable from here - read the file, or "
               + "get_pg_server_config's pending_restart_settings, before acting on this row's remedy.";
    }

    /// <summary>
    /// The change in the hosting flavour's own syntax, or the reason none is needed. <c>alreadyRight</c> is
    /// the caller's judgment that the current value is the recommended posture — which for a threshold
    /// setting can be a <c>partial</c> verdict — so the remedy does not tell somebody to change a value that
    /// is already right.
    /// </summary>
    private static string Remedy(
        string setting, string recommendedLiteral,
        DarlingPgLoggingAuditReader.PgLoggingSettingRow? row, bool managed, string verdict, bool alreadyRight)
    {
        if (verdict == Unknown)
        {
            return row is null
                ? "No remedy is offered for a setting the snapshot does not hold: check get_pg_server_config, "
                  + "and if the collector is running, the next hourly snapshot will carry it."
                : "No remedy is offered for a value this audit could not read: get_pg_server_config shows the "
                  + "raw row, and the remedy depends on what it actually says.";
        }

        if (alreadyRight)
        {
            return "No change needed - the current value is the recommended posture. cost_note says what "
                   + "it does and does not write.";
        }

        var restart = string.Equals(row?.Context, "postmaster", StringComparison.Ordinal);
        var perBackend = row?.Context is "superuser-backend" or "backend";

        if (managed)
        {
            return $"Set {setting} = {recommendedLiteral} in the DB parameter group and apply it - on Aurora the "
                   + "CLUSTER parameter group covers every instance and an instance-level group overrides it "
                   + "per instance. ALTER SYSTEM is refused on RDS and Aurora. "
                   + (restart
                       ? "This is a STATIC parameter and needs a reboot."
                       : "This is a dynamic parameter and applies WITHOUT a reboot"
                         + (perBackend ? ", to connections opened after it." : "."));
        }

        return $"ALTER SYSTEM SET {setting} = {recommendedLiteral}; SELECT pg_reload_conf(); - "
               + (restart
                   ? "then RESTART: this parameter's context is postmaster and a reload does not apply it."
                   : "a reload, not a restart"
                     + (perBackend
                         ? "; sessions already open keep their value and new connections take the new one."
                         : "."));
    }
}
