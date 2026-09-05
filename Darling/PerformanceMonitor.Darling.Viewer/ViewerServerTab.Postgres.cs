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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The seven PostgreSQL inner tabs (#2530) — which tab set a server gets, what each tab loads, and the
/// projections that turn stored rows into something a grid may show.
///
/// <para><b>Why the projections exist rather than binding the reader records directly.</b> Every one of
/// these reads carries at least one value a grid must not print raw. <c>-1</c> is the store's
/// not-applicable sentinel for a duration or a size (0 was rejected because it reads as "started this
/// instant" / "retains nothing"); <c>pg_stat_io</c>'s write counters are genuinely ABSENT on Aurora rather
/// than zero, because backends there do not write data files; recurrence is NULL when the blocker's own row
/// had already left <c>pg_stat_activity</c>, and "cannot tell" is a different claim from "seen once"; and
/// every timestamp is naive UTC that has to go through <see cref="ViewerTimeHelper.ForDisplay"/> like every
/// other timestamp the viewer renders. A DataGrid bound straight at the record would show <c>-1</c>, a
/// misleading <c>0</c>, and UTC.</para>
///
/// <para><b>Every panel says why it is empty.</b> <see cref="PanelNote"/> asks
/// <see cref="CollectorEngineCapability.NotCollectedMessage"/> first — the same sentence the MCP surface and
/// the web dashboard print, naming the server, the engine, the collector and the exact surface it would have
/// read, ending "and never will". That is what makes the two Aurora-only panels worth SHOWING on stock
/// PostgreSQL rather than hiding: the defect #2530 is about is unexplained emptiness, not emptiness, and a
/// tab set that changed shape between two PostgreSQL servers in one fleet would be its own confusion.</para>
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>
    /// A row limit for the per-tab PostgreSQL grids. Generous — these are already server-side ranked reads
    /// — but bounded, because a fleet-sized autovacuum backlog is tens of thousands of tables and a WPF
    /// DataGrid asked to realise all of them is a hung UI thread rather than a slow one.
    /// </summary>
    private const int PgGridRowLimit = 200;

    /// <summary>
    /// Picks the engine's tab set, once, in the constructor. Only a POSITIVE PostgreSQL claim switches:
    /// <see cref="DarlingServer.IsPostgres"/> is false for an absent kind, an unrecognised token and a
    /// server that has never connected, none of which is evidence for either engine. Falling back to the
    /// SQL Server tabs there is the pre-#2530 behaviour, unchanged — and it has to be, because the
    /// unclaimed population is every server that has not reconnected since the engine-kind rung landed.
    /// </summary>
    private void ApplyEngineTabSet()
    {
        var engine = _server.EngineDescription;
        if (engine is not null)
        {
            ServerEngineText.Text = engine;
            ServerEngineBadge.Visibility = Visibility.Visible;
        }

        if (!_server.IsPostgres)
        {
            return;
        }

        /* Collapse the SQL Server run and reveal the PostgreSQL one. Visibility rather than removal, so
           both sets keep their fixed indices: LoadInnerTabAsync dispatches on SelectedIndex and every
           drill-down in the viewer navigates by one of the index constants above. */
        for (var i = 0; i < InnerTabs.Items.Count; i++)
        {
            if (InnerTabs.Items[i] is TabItem item)
            {
                item.Visibility = ViewerPostgresTabs.Owns(i) ? Visibility.Visible : Visibility.Collapsed;
            }
        }

        /* A collapsed TabItem can still be the SELECTED one — WPF hides the header and shows the content
           anyway — so the selection has to move explicitly. Index 0 (the SQL Server Overview lanes) would
           otherwise be what a PostgreSQL server opens on. */
        InnerTabs.SelectedIndex = ViewerPostgresTabs.FirstInnerTabIndex;

        /* The per-server database filter drives the SQL Server database-scoped reads and nothing else. On a
           PostgreSQL server it would sit there offering to filter views that never consult it, which is a
           worse answer than not offering. */
        DatabaseFilterButton.Visibility = Visibility.Collapsed;

        /* IsPostgres is only true for a token the describer recognises, so `engine` has words here by
           construction; the coalesce is the compiler's, not a state this can reach. */
        PgEngineBanner.Text = $"{_server.DisplayName} runs {engine ?? "PostgreSQL"}.";

        /* Looked up by ID, never by position: the registry's order is the STRIP order and is free to change,
           while these four assignments are about which tab gets which framing. Indexing All[0..2] would
           have silently put the Vacuum note above the Activity grids the first time someone reordered. */
        PgOverviewNote.Text = ViewerPostgresTabs.NoteFor("overview");
        PgActivityNote.Text = ViewerPostgresTabs.NoteFor("activity");
        PgVacuumNote.Text = ViewerPostgresTabs.NoteFor("vacuum");
        PgStorageNote.Text = ViewerPostgresTabs.NoteFor("storage");
    }

    // ─────────────────────────────────────────────────────────────────────────────────────────────
    // Empty-state prose
    // ─────────────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// What a panel says when it has nothing to show. Three different answers, because they need three
    /// different responses from the person reading them: a collector this engine can never run (the shared
    /// capability sentence — nothing to do), a zero-row window for a collector where zero IS the answer
    /// (nothing wrong), and a zero-row window for a collector that should have data (something to chase).
    /// Returns empty for a panel with rows: the rows speak for themselves and a standing sentence above a
    /// full grid is noise.
    /// </summary>
    private string PanelNote(string collectorName, int rowCount, string healthyEmptyText)
    {
        var gap = CollectorEngineCapability.NotCollectedMessage(
            _server.ServerName, _server.EngineEdition, _server.EngineKind, collectorName);

        if (gap is not null)
        {
            return gap;
        }

        return rowCount > 0 ? string.Empty : healthyEmptyText;
    }

    /// <summary>True when a panel's collector cannot run here — used to skip the read entirely rather than
    /// spend a round trip proving a permanent gap is still permanent.</summary>
    private bool PgCollectorIsGatedOff(string collectorName) =>
        CollectorEngineCapability.NotCollectedMessage(
            _server.ServerName, _server.EngineEdition, _server.EngineKind, collectorName) is not null;

    // ─────────────────────────────────────────────────────────────────────────────────────────────
    // Tab loaders
    // ─────────────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Overview — every PostgreSQL collector for this server, from <c>collection_log</c> composed against
    /// the CATALOG. Catalog-driven so a collector that is gated off for this engine (and therefore writes
    /// no log row at all) is still a visible row carrying its own explanation, instead of the one line an
    /// operator most needs being the one line missing.
    /// </summary>
    private async Task LoadPgOverviewAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var collectors = ViewerPostgresTabs.PostgresCollectors();

        var facts = await _dataService.GetPostgresCollectorLogFactsAsync(
            _server.ServerId, startUtc, endUtc, collectors.Select(c => c.Name).ToList());

        PgCollectorHealthGrid.ItemsSource =
            ViewerDataService.BuildPostgresCollectorHealth(_server, collectors, facts);

        await LoadPgExtensionsAsync(startUtc, endUtc);
        await LoadPgServerConfigAsync();
        await LoadPgCpuUtilizationAsync(startUtc, endUtc);
    }

    /// <summary>
    /// The instance-level CPU gauge (#2719), beneath configuration because it is the same kind of fact one
    /// layer further in: extensions say what this server CAN do, settings say what it was told to do, and
    /// this says what it is doing right now. Read via AWS Performance Insights rather than a database
    /// connection — an empty grid here means the ingestor hasn't run yet, not that the collector is gated
    /// off for this engine, though <see cref="PgCollectorIsGatedOff"/> is still consulted first for
    /// consistency with every other panel on this tab.
    /// </summary>
    private async Task LoadPgCpuUtilizationAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_cpu_utilization"))
        {
            PgCpuGrid.ItemsSource = null;
            PgCpuNote.Text = PanelNote("pg_cpu_utilization", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgCpuUtilizationHistoryAsync(_server.ServerId, startUtc, endUtc);

        PgCpuGrid.ItemsSource = rows;
        PgCpuNote.Text = PanelNote("pg_cpu_utilization", rows.Count,
            "This collector samples AWS Performance Insights on a 5-minute cadence, so a server added "
            + "recently may have nothing here yet.");
    }

    /// <summary>
    /// The extension capability axis (#2545), under the collector grid because it answers the question that
    /// grid raises and cannot: a collector missing for want of an extension is a SETUP step rather than a
    /// permanent gap, and this names the step.
    ///
    /// <para>The note leads with the ACTIONABLE count — extensions this product can use that are available
    /// on the server and simply not installed — because that number is the entire reason to look. Zero of
    /// them is also worth saying: it means the gap is a platform limit rather than a missed install.</para>
    /// </summary>
    private async Task LoadPgExtensionsAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_extension_availability"))
        {
            PgExtensionsGrid.ItemsSource = null;
            PgExtensionsNote.Text = PanelNote("pg_extension_availability", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgExtensionAvailabilityAsync(_server.ServerId, startUtc, endUtc);

        PgExtensionsGrid.ItemsSource = rows;

        var actionable = rows.Count(r => r.IsMonitoringRelevant && string.Equals(r.State, "available", StringComparison.Ordinal));
        var outdated = rows.Count(r => r.IsMonitoringRelevant && string.Equals(r.State, "outdated", StringComparison.Ordinal));

        PgExtensionsNote.Text = PanelNote("pg_extension_availability", rows.Count,
            "The extension collector runs daily, so a server added in the last day has nothing here yet.")
            + (rows.Count == 0
                ? string.Empty
                : $"  {actionable} extension(s) this product can use are available on this server and NOT "
                  + "installed — each is one CREATE EXTENSION away."
                  + (outdated > 0
                      ? $"  {outdated} are installed but behind the version the server offers, which is worth "
                        + "fixing with ALTER EXTENSION … UPDATE: a stale extension can be missing columns a "
                        + "collector reads, and that surfaces as a confusing error rather than as a gap."
                      : string.Empty)
                  + "  \u201cInstalled\u201d is scoped to the database this server entry connects to — "
                  + "pg_extension is per-database while the server's offer is cluster-wide.");
    }

    /// <summary>
    /// The server's own configuration (#2658), under the extension axis because it is the same kind of fact
    /// one layer in: extensions say what this server CAN do, settings say what it was told to do.
    ///
    /// <para><b>Not scoped to the toolbar window, unlike every other panel on this tab.</b> A configuration
    /// is the state NOW rather than something that happened during an interval, and filtering it by the
    /// window would return nothing for a server whose HOURLY collector last ran just outside it — which on
    /// this screen reads as "this server has no configuration" rather than "widen the window".</para>
    ///
    /// <para>The note leads with pending_restart when there is one, because that is the only row here that
    /// reports a DISAGREEMENT rather than a value: the file has been changed and reloaded, the running
    /// server is still on the old value, and nothing else in the product would ever mention it.</para>
    /// </summary>
    private async Task LoadPgServerConfigAsync()
    {
        if (PgCollectorIsGatedOff("pg_server_config"))
        {
            PgServerConfigGrid.ItemsSource = null;
            PgServerConfigNote.Text = PanelNote("pg_server_config", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgServerConfigAsync(_server.ServerId);

        /* Non-default only in the grid: several hundred parameters sorted alphabetically is a dump, and the
           ones somebody chose are the answer. The full set stays one MCP call away for anyone who wants it. */
        var chosen = rows.Where(r => !r.IsDefault).ToList();
        PgServerConfigGrid.ItemsSource = chosen;

        var pendingRestart = rows.Where(r => r.PendingRestart).Select(r => r.Name).ToList();

        PgServerConfigNote.Text = PanelNote("pg_server_config", chosen.Count,
            "This collector runs HOURLY, so a server added in the last hour has nothing here yet.")
            + (chosen.Count == 0
                ? string.Empty
                : $"  {chosen.Count} setting(s) differ from the compiled-in default; the rest are omitted "
                  + "rather than truncated.")
            + (pendingRestart.Count > 0
                ? $"  {pendingRestart.Count} setting(s) are PENDING RESTART — "
                  + string.Join(", ", pendingRestart)
                  + " — meaning the configuration file has been changed and reloaded but the running "
                  + "server is still using the previous value. The file and the server disagree until the "
                  + "next restart, at which point behaviour changes with no deployment to explain it."
                : string.Empty);
    }

    /// <summary>
    /// Activity — blocking (denominator, chains, cycles) and query shapes (statements over per-database
    /// counters). All five reads fire together: the sub-tabs are two views of one load, so switching
    /// between them needs no second round trip.
    /// </summary>
    private async Task LoadPgActivityAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        using var readFanOut = ViewerReadFanOut.Of(5);

        var countsTask = _dataService.GetPgBlockingCaptureCountsAsync(_server.ServerId, startUtc, endUtc);
        var chainsTask = _dataService.GetPgBlockingChainsAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);
        var cyclesTask = _dataService.GetPgBlockingCyclesAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        /* Aurora-only, so on stock PostgreSQL the read is skipped and the panel carries the capability
           sentence instead. Skipped rather than run-and-discarded: the answer is decided by the collectors'
           own gate and cannot change between now and the query returning. */
        var statementsGatedOff = PgCollectorIsGatedOff("pg_statement_stats");
        var statementsTask = statementsGatedOff
            ? Task.FromResult(new List<DarlingPgStatementReader.PgStatementRow>())
            : _dataService.GetPgTopQueriesAsync(_server.ServerId, startUtc, endUtc);

        var databasesTask = _dataService.GetPgDatabaseStatsAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        await Task.WhenAll(countsTask, chainsTask, cyclesTask, statementsTask, databasesTask);

        /* Released here rather than at the closing brace: the six sub-tab loads at the end of this method
           run after these five have finished, so they do not contend with them. */
        readFanOut.Release();

        var counts = countsTask.Result;
        var chains = chainsTask.Result;
        var cycles = cyclesTask.Result;

        PgBlockingChainsGrid.ItemsSource = chains.Select(PgDisplay.Chain).ToList();
        PgBlockingCyclesGrid.ItemsSource = cycles.Select(PgDisplay.Cycle).ToList();
        PgBlockingCyclesExpander.Visibility = cycles.Count > 0 ? Visibility.Visible : Visibility.Collapsed;

        /* The denominator, always — not only when the grid is empty. "Three chains" means something
           different in a window of 60 captures than in a window of 4, and the edge table cannot tell an
           absent capture from a capture that found nothing: both are an absence of rows. This is the whole
           reason the read carries capture counts from collection_log. */
        PgBlockingNote.Text = PgCollectorIsGatedOff("pg_blocking")
            ? PanelNote("pg_blocking", 0, string.Empty)
            : counts.CapturesTotal == 0
                ? "No blocking capture ran for this server in this window, so this grid being empty says "
                  + "nothing about whether anything blocked. Check the Overview tab for the pg_blocking "
                  + "collector's status."
                : $"{counts.CapturesWithBlocking:N0} of {counts.CapturesTotal:N0} captures in this window "
                  + $"saw blocking{(chains.Count == 0 && cycles.Count == 0 ? " — and none of them produced a chain or a cycle in view" : "")}.";

        PgTopQueriesGrid.ItemsSource = statementsTask.Result.Select(PgDisplay.Statement).ToList();
        PgStatementsNote.Text = PanelNote("pg_statement_stats", statementsTask.Result.Count,
            "No statement accumulated execution time in this window.");

        PgDatabaseStatsGrid.ItemsSource = databasesTask.Result.Select(PgDisplay.Database).ToList();
        PgDatabasesNote.Text = PanelNote("pg_database_stats", databasesTask.Result.Count,
            "No database counter moved in this window.");

        await LoadPgLockStatsAsync(startUtc, endUtc);
        await LoadPgWaitSamplingAsync(startUtc, endUtc);
        await LoadPgKernelStatsAsync(startUtc, endUtc);
        await LoadPgPredicateStatsAsync(startUtc, endUtc);
        await LoadPgPlanCaptureAsync(startUtc, endUtc);
        await LoadPgDeadlocksAsync(startUtc, endUtc);
    }

    /// <summary>
    /// The Locks sub-tab (#2544) — lock state by mode and relation, beside Blocking rather than inside it.
    ///
    /// <para>The two answer different questions about the same event: Blocking has the blocked/blocker
    /// pairs, and this has the MODE, which is what decides the remedy. An ungranted
    /// <c>AccessExclusiveLock</c> is a DDL queue and everything arriving behind it will also queue;
    /// <c>RowExclusiveLock</c> contention is ordinary write traffic. Identical pair shape, opposite
    /// advice.</para>
    ///
    /// <para>The note leads with the QUEUE count rather than the row count, because a granted lock is not a
    /// finding — a healthy server holds thousands — and a panel that opened with "412 rows" would bury the
    /// three that matter.</para>
    /// </summary>
    private async Task LoadPgLockStatsAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_lock_stats"))
        {
            PgLockStatsGrid.ItemsSource = null;
            PgLockStatsNote.Text = PanelNote("pg_lock_stats", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgLockStatsAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        PgLockStatsGrid.ItemsSource = rows;

        var queued = rows.Where(r => !r.Granted).ToList();
        var totalCaptures = rows.Count == 0 ? 0 : rows[0].TotalCaptures;

        PgLockStatsNote.Text = rows.Count == 0
            ? PanelNote("pg_lock_stats", 0,
                "No lock sample was taken for this server in this window, so an empty grid says nothing "
                + "about whether anything contended. Check the pg_lock_stats collector on the Overview tab.")
            : queued.Count == 0
                ? $"No lock was waiting in any of {totalCaptures:N0} captures — every lock held in this "
                  + "window was granted, which is the healthy answer rather than a missing read."
                : $"{queued.Count:N0} lock queue(s) across {totalCaptures:N0} captures. These are SAMPLES, "
                  + "so the capture columns are the denominator: a queue seen once in 60 captures is a blip, "
                  + "and one seen in 55 is a standing problem. The Mode column decides the remedy — an "
                  + "ungranted AccessExclusiveLock is a DDL everything else is queued behind. A blank "
                  + "Relation with an OID is a lock in a different database, not a missing name.";
    }

    /// <summary>
    /// Wait events attributed to the query that waited (#2603).
    ///
    /// <para>An empty grid here has THREE causes and they need different actions, so the note names
    /// which one it is: the module is not loaded (an install step), the collector is gated off, or the
    /// server genuinely waited on nothing. Collapsing those into &quot;no data&quot; is how a missing
    /// extension reads as a healthy server.</para>
    /// </summary>
    private async Task LoadPgWaitSamplingAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_wait_sampling"))
        {
            PgWaitSamplingGrid.ItemsSource = null;
            PgWaitSamplingNote.Text = PanelNote("pg_wait_sampling", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgWaitSamplingAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        PgWaitSamplingGrid.ItemsSource = rows;

        var attributed = rows.Count(r => r.QueryId != 0);
        var reset = rows.Any(r => r.CounterReset);

        PgWaitSamplingNote.Text = rows.Count == 0
            ? PanelNote("pg_wait_sampling", 0,
                "No wait samples for this server in this window. The usual cause is that "
                + "pg_wait_sampling is not in shared_preload_libraries — check the Extensions panel, "
                + "which says whether it is installed, available or absent. If it IS loaded, an empty "
                + "grid means the server waited on nothing worth sampling, which is the healthy answer.")
            : $"{rows.Count:N0} wait event(s), {attributed:N0} attributed to a query. "
              + "Est. Wait is samples multiplied by the profile period — an estimate from a sampling "
              + "profiler, not a measured duration, so treat it as a ranking rather than a stopwatch. "
              + "A Query ID of 0 is a background process rather than an unknown query, and CPU/Running "
              + "is the backend on processor rather than waiting."
              + (reset
                  ? " One or more counters RESET inside this window (a restart, or "
                    + "pg_wait_sampling_reset_profile), so those rows cover only the time since the reset."
                  : string.Empty);
    }

    /// <summary>
    /// The kernel's CPU and disk per query (#2603).
    ///
    /// <para>The note has to say what zero read bytes means, because it is the reading most likely to
    /// be got wrong: these counters measure I/O that reached the DEVICE, so a cached read is genuinely
    /// zero and that is the healthy case, not a broken instrument.</para>
    /// </summary>
    private async Task LoadPgKernelStatsAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_kernel_stats"))
        {
            PgKernelStatsGrid.ItemsSource = null;
            PgKernelStatsNote.Text = PanelNote("pg_kernel_stats", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgKernelStatsAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        PgKernelStatsGrid.ItemsSource = rows;

        var reset = rows.Any(r => r.CounterReset);

        PgKernelStatsNote.Text = rows.Count == 0
            ? PanelNote("pg_kernel_stats", 0,
                "No kernel statistics for this server in this window. The usual cause is that "
                + "pg_stat_kcache is not installed - the Extensions panel says whether it is available "
                + "here. Where it IS installed, this separates a query that was WAITING from one that "
                + "was burning processor, which elapsed time alone cannot do.")
            : $"{rows.Count:N0} statement(s) by OS CPU. Read bytes are I/O that reached the DEVICE, so "
              + "zero with high CPU is a cached workload behaving well rather than a missing "
              + "measurement. Query ID joins the statement grid above."
              + (reset
                  ? " One or more counters RESET inside this window, so those rows cover only the "
                    + "time since the reset."
                  : string.Empty);
    }

    /// <summary>
    /// Predicates evaluated and how badly the planner estimated them (#2603).
    ///
    /// <para>The note leads with the SAMPLE RATE because it is the number most likely to mislead: the
    /// extension defaults to 1/max_connections, so a small count means the sampler fired rarely rather
    /// than the predicate being rare.</para>
    /// </summary>
    private async Task LoadPgPredicateStatsAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_predicate_stats"))
        {
            PgPredicateStatsGrid.ItemsSource = null;
            PgPredicateStatsNote.Text = PanelNote("pg_predicate_stats", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgPredicateStatsAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        PgPredicateStatsGrid.ItemsSource = rows;

        var sampled = rows.Count == 0 ? 1.0 : rows[0].SampleRate;
        var misestimated = rows.Count(r => r.WorstEstimateErrorRatio >= 10);

        PgPredicateStatsNote.Text = rows.Count == 0
            ? PanelNote("pg_predicate_stats", 0,
                "No predicate statistics for this server in this window. pg_qualstats is created PER "
                + "DATABASE, so it may be installed in one database and not another - the Extensions "
                + "panel says which. Note also that its sample_rate defaults to 1/max_connections, so a "
                + "lightly-used database can genuinely record nothing.")
            : $"{rows.Count:N0} predicate(s), sampled at {sampled:P2} of executions - these counts are a "
              + "SAMPLE, so a small number means the sampler fired rarely rather than the predicate "
              + "being rare. Filtered % with no supporting index is the index candidate. "
              + $"{misestimated:N0} predicate(s) show an estimate error of 10x or worse, which is a "
              + "different problem: the planner does not understand that column, and an index will not "
              + "fix a plan built on a wrong row count.";
    }

    /// <summary>
    /// Plans captured by auto_explain (#2566).
    ///
    /// <para>An empty grid here almost always means a MISSING GRANT rather than a quiet server, so the
    /// note says so: reading the log needs pg_read_server_files plus an explicit EXECUTE on pg_read_file,
    /// and on Aurora or RDS the route does not exist at all.</para>
    /// </summary>
    private async Task LoadPgPlanCaptureAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_plan_capture"))
        {
            PgCapturedPlansGrid.ItemsSource = null;
            PgCapturedPlansNote.Text = PanelNote("pg_plan_capture", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgPlanCaptureAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        PgCapturedPlansGrid.ItemsSource = rows;

        var orphans = rows.Count(r => r.QueryId == 0);

        PgCapturedPlansNote.Text = rows.Count == 0
            ? PanelNote("pg_plan_capture", 0,
                "No captured plans. The Plan Capture Readiness panel says which precondition is missing - "
                + "usually auto_explain is not loaded, or the monitoring login cannot read the server log. "
                + "That read needs pg_read_server_files AND an explicit GRANT EXECUTE on pg_read_file; the "
                + "role alone does not carry it. On Aurora and RDS there is no filesystem to read, so this "
                + "panel stays empty by design.")
            : $"{rows.Count:N0} plan shape(s), ranked by TOTAL time - a plan that takes 40ms constantly "
              + "costs more than one that took 900ms once. Captures counts how often the collector SAW "
              + "the plan, not how often it ran. Plan JSON is redacted at collection: query text is "
              + "dropped and literals are replaced, so nothing here carries customer values."
              + (orphans > 0
                  ? $" {orphans:N0} plan(s) have no query id, which means log_line_prefix lacks %Q - they "
                    + "cannot be joined to a statement until that is fixed."
                  : string.Empty);
    }

    /// <summary>
    /// Deadlocks reported in the window (#2661), last on the Activity tab's Blocking sub-tab because they
    /// are blocking at its limit: a chain the server had to break by cancelling somebody.
    ///
    /// <para><b>An empty grid is the healthy answer AND the shape of a log that cannot be read or cannot be
    /// parsed</b>, which is why the note names the other checks rather than leaving them. Deadlock reports
    /// need nothing ENABLED on the target, unlike plan capture, but they are not unsuppressable. Two things
    /// have to hold: the log must be readable, which the Vacuum tab's plan-capture readiness panel
    /// reports on because it reads the same file, AND it must carry DETAIL, which <c>log_error_verbosity = terse</c>
    /// strips along with the whole graph. pg_stat_database's cumulative deadlock counter is the independent
    /// test: if that moved and this is empty, the log is the problem — unreadable or too terse — rather
    /// than the server (#3030).</para>
    /// </summary>
    private async Task LoadPgDeadlocksAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_deadlocks"))
        {
            PgDeadlocksGrid.ItemsSource = null;
            PgDeadlocksNote.Text = PanelNote("pg_deadlocks", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgDeadlocksAsync(_server.ServerId, startUtc, endUtc);

        PgDeadlocksGrid.ItemsSource = rows;

        PgDeadlocksNote.Text = PanelNote("pg_deadlocks", rows.Count,
            "No deadlock was reported in this window. That is the healthy answer, and it is also what an "
            + "unreadable server log looks like — the Vacuum tab's plan-capture readiness panel reads the "
            + "same file and says which it is.")
            + (rows.Count == 0
                ? string.Empty
                : "  Sightings counts how often the collector saw the SAME report while it stayed inside "
                  + "the log tail it re-reads; a deadlock that genuinely recurred is its own row, because "
                  + "the process IDs differ.");
    }

    /// <summary>
    /// Vacuum — the sessions holding a transaction open, the xmin horizon, the autovacuum backlog and the
    /// freeze headroom, in that causal order. One load for all four: they are one story, and reading them a
    /// tab apart is how each of them ends up looking survivable.
    ///
    /// <para>Session states leads because it is the only panel that can name the SESSION behind a pinned
    /// horizon, and the only one that can say a long idle-in-transaction session pins nothing at all.</para>
    /// </summary>
    private async Task LoadPgVacuumAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        using var readFanOut = ViewerReadFanOut.Of(5);

        var sessionsTask = _dataService.GetPgSessionStatesAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);
        var xminTask = _dataService.GetPgXminHorizonAsync(_server.ServerId, startUtc, endUtc);
        var autovacuumTask = _dataService.GetPgAutovacuumAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);
        var wraparoundTask = _dataService.GetPgWraparoundAsync(_server.ServerId, startUtc, endUtc);
        var planCaptureTask = _dataService.GetPgPlanCaptureReadinessAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        await Task.WhenAll(sessionsTask, xminTask, autovacuumTask, wraparoundTask, planCaptureTask);

        PgSessionStatesGrid.ItemsSource = sessionsTask.Result.Select(PgDisplay.SessionState).ToList();

        /* The healthy-empty sentence has to carry BOTH halves. Zero rows here is a real all-clear — the
           collector stores nothing when every transaction is short — and it is an all-clear about a SAMPLE,
           so a transaction that opened and closed between two captures left no trace to find. Saying only
           the first half would overstate it; saying only the second would read as broken collection. */
        PgSessionStatesNote.Text = PgCollectorIsGatedOff("pg_session_states")
            ? PanelNote("pg_session_states", 0, string.Empty)
            : sessionsTask.Result.Count == 0
                ? "No session held a transaction open past the collector's floor in this window. Zero rows "
                  + "is the HEALTHY answer here rather than a missing read — the collector stores nothing "
                  + "when every transaction is short. It is also a SAMPLE taken once per collection cycle, "
                  + "not an event log: PostgreSQL records nothing about session state unless something "
                  + "asks, so a transaction that opened and closed between two captures is genuinely "
                  + "invisible here."
                : "Duration is NOT evidence that a session is starving vacuum — read the Pinned Horizon "
                  + "column, where \"" + PgDisplay.PinsNothingText + "\" means the session held neither a "
                  + "snapshot nor a transaction id in any sample and terminating it would not reclaim one "
                  + "dead row. This is a SAMPLE at the collection interval, so a transaction that opened "
                  + "and closed between two captures never appears, and a grey row is one PostgreSQL "
                  + "redacted because the monitoring login lacks pg_monitor.";

        PgXminHorizonGrid.ItemsSource = xminTask.Result.Select(PgDisplay.Xmin).ToList();
        PgXminNote.Text = PanelNote("pg_xmin_horizon", xminTask.Result.Count,
            "Nothing held the xmin horizon back in this window — no long-running transaction, replication "
            + "slot, standby feedback or prepared transaction pinned an old xmin. Zero rows is the healthy "
            + "answer here, not a missing read.");

        PgAutovacuumGrid.ItemsSource = autovacuumTask.Result.Select(PgDisplay.Autovacuum).ToList();
        PgAutovacuumNote.Text = PanelNote("pg_autovacuum_stats", autovacuumTask.Result.Count,
            "No table reported an autovacuum backlog in this window. This collector runs on the WRITER "
            + "only — pg_stat_user_tables reports zeros on a replica — so an empty grid on a reader is "
            + "expected rather than informative.");

        PgWraparoundGrid.ItemsSource = wraparoundTask.Result.Select(PgDisplay.Wraparound).ToList();
        PgWraparoundNote.Text = PanelNote("pg_wraparound_stats", wraparoundTask.Result.Count,
            "No per-database freeze headroom has been collected in this window.");

        PgPlanCaptureGrid.ItemsSource = planCaptureTask.Result
            .Select(r => new
            {
                r.Facet,
                /* Rendered as words, not a checkbox or a bare bool. The reader is being told whether a
                   PRECONDITION holds, and "False" beside a remedy sentence reads as a failure rather than as
                   a step not yet taken. */
                Satisfied = r.IsSatisfied ? "yes" : "no",
                Observed = r.Observed ?? "(not reported)",
                Detail = r.Detail ?? string.Empty,
            })
            .ToList();

        /* Three states, and they are genuinely different answers. Gated off is the engine sentence. Zero
           rows means the collector has not run yet on a server that only just started collecting - NOT that
           capture is impossible, which is the mistake worth heading off, because "no rows about readiness"
           and "not ready" look identical. And when rows exist the panel says whether every facet is
           satisfied, because the useful summary is the AND of them: one unsatisfied facet is enough to mean
           no plans. */
        PgPlanCaptureNote.Text = PgCollectorIsGatedOff("pg_plan_capture_readiness")
            ? PanelNote("pg_plan_capture_readiness", 0, string.Empty)
            : planCaptureTask.Result.Count == 0
                ? "Plan-capture readiness has not been collected for this server yet. This is an hourly "
                  + "collector, so a server added in the last hour has nothing here — it does NOT mean plan "
                  + "capture is unavailable."
                : planCaptureTask.Result.All(r => r.IsSatisfied)
                    ? "Every precondition for auto_explain capture is satisfied on this server, and a "
                      + "captured plan carries the query id that joins it back to pg_stat_statements. That "
                      + "means plans CAN be captured — not that this product is reading them, which is a "
                      + "separate thing: auto_explain writes to the server log, which a SQL connection "
                      + "cannot read."
                    : "At least one precondition is unmet, so either no execution plans are being captured "
                      + "by auto_explain here or the ones that are cannot be attributed. Read the rows in "
                      + "order — extension_available, library_loaded, capture_threshold, plan_text_setting, "
                      + "plan_attribution — each names the specific step and, on Aurora/RDS, whether it "
                      + "needs a parameter-group change and a reboot rather than a SET. plan_attribution is "
                      + "the one that is easy to miss: auto_explain puts no query id in the plan itself, so "
                      + "without %Q in log_line_prefix every captured plan is an orphan.";
    }

    /// <summary>Waits — Aurora's cumulative wait counters. Shown on stock PostgreSQL too, where the panel
    /// carries the capability sentence rather than a blank rectangle; see the type header.</summary>
    private async Task LoadPgWaitsAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        var rows = PgCollectorIsGatedOff("pg_wait_stats")
            ? new List<DarlingPgWaitReader.PgWaitRow>()
            : await _dataService.GetPgWaitStatsAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        PgWaitStatsGrid.ItemsSource = rows.Select(PgDisplay.Wait).ToList();
        PgWaitsNote.Text = PanelNote("pg_wait_stats", rows.Count,
            "No wait time was recorded for this server in this window.");
    }

    /// <summary>I/O — <c>pg_stat_io</c>, differenced over the window.</summary>
    private async Task LoadPgIoAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        var rows = await _dataService.GetPgIoAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        PgIoStatsGrid.ItemsSource = PgDisplay.IoRows(rows);
        PgIoNote.Text = PanelNote("pg_io_stats", rows.Count,
            "No backend / object / context combination did any I/O in this window. This view needs "
            + "PostgreSQL 16 or newer, where pg_stat_io exists; below that the collector does not run and "
            + "the Overview tab says so.");

        await LoadPgWriteStatsAsync(startUtc, endUtc);
        await LoadPgBufferUsageAsync(startUtc, endUtc);
    }

    /// <summary>
    /// The buffer-pool panel (#2544) — what the memory is actually holding, the third end of the same
    /// subject as the two grids above it.
    ///
    /// <para>Needs the <c>pg_buffercache</c> extension. When it is absent the collector records an
    /// ObjectMissing outcome and this panel says so AND says the remedy is one <c>CREATE EXTENSION</c> —
    /// which is checkable on the Overview tab's extension panel, so the two halves meet.</para>
    /// </summary>
    private async Task LoadPgBufferUsageAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_buffer_usage"))
        {
            PgBufferUsageGrid.ItemsSource = null;
            PgBufferUsageNote.Text = PanelNote("pg_buffer_usage", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgBufferUsageAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        PgBufferUsageGrid.ItemsSource = rows;

        var total = rows.Count == 0 ? 0L : rows[0].PoolBuffersTotal;
        var used = rows.Count == 0 ? 0L : rows[0].PoolBuffersUsed;

        PgBufferUsageNote.Text = rows.Count == 0
            ? "Nothing recorded. This panel needs the pg_buffercache extension — without it the collector "
              + "reports the object as missing rather than failing, and the Overview tab's extension panel "
              + "says whether it is available on this server and one CREATE EXTENSION away."
            : $"Latest snapshot: {used:N0} of {total:N0} buffers in use "
              + $"({(total == 0 ? 0 : 100.0 * used / total):N1}% of the pool). Residency is a LEVEL, so this "
              + "is the newest sample rather than a window average — averaging what was resident over a day "
              + "answers nothing. A blank Relation is another database's table or a shared catalog, not a "
              + "missing name: the pool is cluster-wide while pg_class is per-database. Avg Usage near 0 "
              + "means a relation is holding memory it is not earning.";
    }

    /// <summary>
    /// The write-side panel under the I/O grid (#2544) — checkpoints, background writer and WAL as the
    /// CHANGE across the window.
    ///
    /// <para>Three states, and they must not read alike. A gated-off collector says so; a null read means
    /// fewer than two samples, which is a real and temporary state on a freshly added server rather than a
    /// quiet one; and a row whose <c>ResetDuringWindow</c> is set has had at least one statistics family
    /// reset underneath it, so those metrics are blank rather than wrong.</para>
    /// </summary>
    private async Task LoadPgWriteStatsAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_write_stats"))
        {
            PgWriteStatsGrid.ItemsSource = null;
            PgWriteStatsNote.Text = PanelNote("pg_write_stats", 0, string.Empty);
            return;
        }

        var row = await _dataService.GetPgWriteStatsAsync(_server.ServerId, startUtc, endUtc);

        PgWriteStatsGrid.ItemsSource = row is null ? null : PgDisplay.WriteStatsRows(row);
        PgWriteStatsNote.Text = row is null
            ? "Write-side counters need TWO collections before a change exists between them, so a server "
              + "added in the last cycle has nothing here yet. This is not the same as a quiet server, "
              + "which would show zeroes."
            : row.ResetDuringWindow
                ? "At least one statistics family was RESET inside this window, so its counters went "
                  + "backwards. Those metrics are left blank rather than differenced across the reset — a "
                  + "difference taken across one reports an enormous number that looks like a catastrophe "
                  + "and means nothing. The families reset independently, so the untouched ones below are "
                  + "still accurate."
                : "Change across the window, not the counters' cumulative levels. Requested checkpoints "
                  + "climbing against timed ones is the max_wal_size-too-small signal; full-page images "
                  + "spiking right after each checkpoint points at checkpoint_timeout instead. A blank "
                  + "value is a metric this PostgreSQL version does not expose, which is not zero.";
    }

    /// <summary>Replication — slot WAL retention and the xmin each slot pins.</summary>
    private async Task LoadPgReplicationAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        var rows = await _dataService.GetPgSlotsAsync(_server.ServerId, startUtc, endUtc);

        PgReplicationSlotsGrid.ItemsSource = rows.Select(PgDisplay.Slot).ToList();
        PgReplicationNote.Text = PanelNote("pg_replication_slots", rows.Count,
            "This server has no replication slots, so nothing is retaining WAL or pinning an xmin on their "
            + "account. Zero rows is the healthy answer here, not a missing read.");

        await LoadPgReplicationStatsAsync(startUtc, endUtc);
    }

    /// <summary>
    /// Connected standbys (#2544), beneath the slots grid — the other half of "is replication healthy".
    ///
    /// <para><b>Zero rows on a REPLICA is correct, not a fault.</b> <c>pg_stat_replication</c> is the
    /// primary-side view, so a standby reports nothing unless it is cascading to a downstream of its own.
    /// The note says which of those it is looking at rather than reporting an absence of replication.</para>
    ///
    /// <para>The note leads with the WORST distance reached, not the current one: a replica that drifts
    /// hundreds of megabytes behind every afternoon and recovers by evening reads as perfectly healthy in
    /// every single sample, and it is the one most likely to be useless at the moment somebody needs to fail
    /// over to it.</para>
    /// </summary>
    private async Task LoadPgReplicationStatsAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_replication_stats"))
        {
            PgReplicationStatsGrid.ItemsSource = null;
            PgReplicationStatsNote.Text = PanelNote("pg_replication_stats", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgReplicationStatsAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        PgReplicationStatsGrid.ItemsSource = rows;

        var worst = rows.Count == 0 ? 0L : rows.Max(r => r.WorstReplayBytesBehind ?? 0L);
        var flapping = rows.Count(r => r.TotalSamples > 0 && r.Samples < r.TotalSamples);

        PgReplicationStatsNote.Text = rows.Count == 0
            ? "No standby was streaming from this server in this window. On a REPLICA that is the expected "
              + "answer — pg_stat_replication is the primary-side view and reports nothing unless this "
              + "server is cascading to a downstream of its own. On a primary it means nothing is "
              + "replicating from it, which is either correct or the finding."
            : $"{rows.Count:N0} standby connection(s). The worst any of them fell behind in this window was "
              + $"{worst:N0} bytes of unapplied WAL — that column, not the current one, is what catches a "
              + "replica that drifts far behind and recovers before anybody looks. Rank on BYTES rather than "
              + "the lag columns: measured against a stalled standby, the time lag read 2.8 seconds for a "
              + "33.7 MB backlog, because it times the round trip of the last replayed record rather than "
              + "sizing the backlog."
              + (flapping > 0
                  ? $"  {flapping:N0} standby(s) appeared in fewer samples than were taken, which means they "
                    + "have been DISCONNECTING — every other column shows that as healthy."
                  : string.Empty);
    }

    /// <summary>
    /// Storage - the per-table bloat estimate and per-index usage. Both reads fire together: they are one
    /// tab answering one question (where the space went, and whether it is earning its keep), so moving
    /// between the two grids needs no second round trip.
    /// </summary>
    private async Task LoadPgStorageAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        using var readFanOut = ViewerReadFanOut.Of(2);

        var bloatTask = _dataService.GetPgTableBloatAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);
        var indexTask = _dataService.GetPgIndexUsageAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        await Task.WhenAll(bloatTask, indexTask);

        /* Released here — the two sub-tab loads at the end of this method do not contend with these two. */
        readFanOut.Release();

        var bloat = bloatTask.Result;
        var indexes = indexTask.Result;

        var bloatRows = bloat.Select(PgDisplay.TableBloat).ToList();
        PgTableBloatGrid.ItemsSource = bloatRows;

        /* The suppression count is stated ON THE PANEL rather than left to the per-row Confidence column,
           because the usual cause is a single instance-wide permissions gap: every row is suppressed for
           the same reason, and saying it once above the grid is what gets it fixed. Counted from the
           PROJECTED rows so the sentence and the grid cannot disagree about which rows were suppressed. */
        var suppressed = bloatRows.Count(r => r.EstimateSuppressed);

        PgTableBloatNote.Text = PgCollectorIsGatedOff("pg_table_bloat_stats")
            ? PanelNote("pg_table_bloat_stats", 0, string.Empty)
            : bloatRows.Count == 0
                ? "No table of at least 1 MB was measured on this server in this window. Below that floor "
                  + "bloat is not an actionable amount of space, so zero rows here is the healthy answer "
                  + "rather than a missing read."
                : suppressed == 0
                    ? "Bloat figures are ESTIMATES computed from column-width statistics - the table itself "
                      + "is never read. Confirm one with pgstattuple before rewriting anything."
                    : $"Bloat figures are ESTIMATES computed from column-width statistics. {suppressed} of "
                      + $"{bloatRows.Count} row(s) have NO publishable estimate and show a dash rather than "
                      + "a number - see the Confidence column. If most of them say 'no column statistics', "
                      + "the monitoring login cannot SELECT these tables: pg_stats is filtered by SELECT "
                      + "privilege and pg_monitor does not grant it, so granting pg_read_all_data "
                      + "(PostgreSQL 14+) fixes the whole instance at once.";

        PgIndexUsageGrid.ItemsSource = indexes.Select(PgDisplay.IndexUsage).ToList();
        PgIndexUsageNote.Text = PgCollectorIsGatedOff("pg_index_usage_stats")
            ? PanelNote("pg_index_usage_stats", 0, string.Empty)
            : indexes.Count == 0
                ? "No index of at least 64 KB was recorded on this server in this window. Below that floor "
                  + "an index costs effectively nothing to keep, so zero rows here is the healthy answer "
                  + "rather than a missing read."
                : "Scans are cumulative since each database's statistics were last reset. An index with no "
                  + "scans is a CANDIDATE, never a conclusion: check the Can It Go? column, and widen the "
                  + "window past the slowest scheduled job you have before acting - a monthly report looks "
                  + "exactly like a dead index over seven days.";

        await LoadPgColumnStatsAsync(startUtc, endUtc);
        await LoadPgIndexBloatAsync(startUtc, endUtc);
    }

    /// <summary>
    /// Measured index bloat (#2561), under index usage — the two are halves of one question.
    ///
    /// <para>The note leads with RECLAIMABLE BYTES rather than a worst-density figure, because density
    /// alone ranks the wrong thing: a tiny index at 20% looks alarming and is worth kilobytes. It also has
    /// to say that a healthy index measures near 90 rather than 100, or the first person to read the density
    /// column concludes every index in the fleet is 10% bloated.</para>
    /// </summary>
    private async Task LoadPgIndexBloatAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_index_bloat"))
        {
            PgIndexBloatGrid.ItemsSource = null;
            PgIndexBloatNote.Text = PanelNote("pg_index_bloat", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgIndexBloatAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        PgIndexBloatGrid.ItemsSource = rows;

        var reclaimable = 0L;
        foreach (var r in rows)
        {
            reclaimable += r.EstimatedReclaimableBytes ?? 0L;
        }

        var skipped = rows.Count(r => r.SkippedReason is not null);

        PgIndexBloatNote.Text = rows.Count == 0
            ? "Nothing recorded. This panel needs the pgstattuple extension — without it the collector "
              + "reports the function as missing rather than failing, and the Overview tab's extension panel "
              + "says whether it is available on this server and one CREATE EXTENSION away. Only B-TREE "
              + "indexes are measured; pgstatindex raises on GIN, BRIN and hash."
            : $"MEASURED, not estimated from column statistics — every page of each index was read. About "
              + $"{reclaimable:N0} bytes look reclaimable across {rows.Count:N0} index(es), and that is what "
              + "the grid is ranked by: density alone ranks the wrong thing, since a tiny index at 20% is "
              + "worth kilobytes next to a large one at 70%. **Leaf density is the server's raw figure and "
              + "is not 100-minus-bloat** — a freshly built index measures around 90, so the reclaimable "
              + "estimate is computed against that floor rather than against a full page."
              + (skipped > 0
                  ? $"  {skipped:N0} index(es) were too large to read and are listed FIRST with their reason: "
                    + "their bloat is unknown rather than zero, and they are the likeliest big win."
                  : string.Empty);
    }

    /// <summary>
    /// The column-statistics panel (#2543) — the planner inputs that explain WHY a plan was chosen, and the
    /// same statistics the bloat estimate above is computed from.
    ///
    /// <para><b>Zero rows has two causes and the note must not collapse them.</b> <c>pg_stats</c> filters on
    /// <c>has_column_privilege</c>, so a monitoring login without SELECT on a table sees nothing for it —
    /// measured: a <c>pg_monitor</c>-only role gets zero rows where a superuser gets all of them. Row-level
    /// security empties it the same way. Neither is an absence of problems, and reporting "no statistics" as
    /// though the data were clean is the exact claim the miss vocabulary exists to prevent.</para>
    /// </summary>
    private async Task LoadPgColumnStatsAsync(DateTime startUtc, DateTime endUtc)
    {
        if (PgCollectorIsGatedOff("pg_column_stats"))
        {
            PgColumnStatsGrid.ItemsSource = null;
            PgColumnStatsNote.Text = PanelNote("pg_column_stats", 0, string.Empty);
            return;
        }

        var rows = await _dataService.GetPgColumnStatsAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        PgColumnStatsGrid.ItemsSource = rows;

        var skewed = rows.Count(r => r.TopValueFrequency >= 0.25);

        PgColumnStatsNote.Text = rows.Count == 0
            ? "No column statistics were collected. That is NOT the same as clean statistics, and it has two "
              + "causes worth telling apart: pg_stats is filtered by SELECT privilege, so a monitoring login "
              + "without it on a table sees nothing for that table (row-level security empties the view the "
              + "same way) — or the server genuinely has no table above the 1 MB floor this collects at."
            : $"Ranked by suspicion, not alphabetically. {skewed:N0} column(s) have a single value covering "
              + "a quarter or more of the table, which is the PostgreSQL analogue of parameter sniffing: a "
              + "plan that suits most values is catastrophic for that one. Low correlation on a wide column "
              + "is the other shape, and it is why an index scan was rejected on a column that obviously "
              + "has an index. Distinct is NEGATIVE when it is a ratio of row count — -1 means nearly every "
              + "row is unique, not minus one value. Most-common VALUES and histogram bounds are "
              + "deliberately not collected: they hold raw column data.";
    }
}
