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
    }

    /// <summary>
    /// Activity — blocking (denominator, chains, cycles) and query shapes (statements over per-database
    /// counters). All five reads fire together: the sub-tabs are two views of one load, so switching
    /// between them needs no second round trip.
    /// </summary>
    private async Task LoadPgActivityAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

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
                    ? "Every precondition for auto_explain capture is satisfied on this server. That means "
                      + "plans CAN be captured — not that this product is reading them, which is a separate "
                      + "thing: auto_explain writes to the server log, which a SQL connection cannot read."
                    : "At least one precondition is unmet, so no execution plans are being captured by "
                      + "auto_explain here. Read the rows in order — extension_available, library_loaded, "
                      + "capture_threshold — each names the specific step and, on Aurora/RDS, whether it "
                      + "needs a parameter-group change and a reboot rather than a SET.";
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
    }

    /// <summary>
    /// Storage - the per-table bloat estimate and per-index usage. Both reads fire together: they are one
    /// tab answering one question (where the space went, and whether it is earning its keep), so moving
    /// between the two grids needs no second round trip.
    /// </summary>
    private async Task LoadPgStorageAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        var bloatTask = _dataService.GetPgTableBloatAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);
        var indexTask = _dataService.GetPgIndexUsageAsync(_server.ServerId, startUtc, endUtc, PgGridRowLimit);

        await Task.WhenAll(bloatTask, indexTask);

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
    }
}
