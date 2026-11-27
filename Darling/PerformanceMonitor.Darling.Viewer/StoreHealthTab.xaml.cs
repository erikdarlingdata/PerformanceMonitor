/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Store Health (#8787): the health of the monitoring store's own tables and indexes, read only. The Viewer twin of the web's Store
/// health page. Every read goes through <see cref="ViewerDataService"/>'s store health reads, which call the same Storage readers the web
/// page and the MCP tools call. The tables, the indexes and the maintenance check are three separate reads started together: the tables
/// grid and the header render as soon as the tables read returns (the indexes read takes about as long again), and the index sections
/// say they are loading until theirs does. Load failures surface on the host status bar via <see cref="StatusChanged"/> and in the
/// section that failed.
/// <para>A refresh of a page that already shows rows repaints in place (#8819): the grids and the summary lines stay readable and the
/// strip beside Refresh says "Refreshing…". The "Loading…" text is for a section with nothing on screen. A refresh that comes right
/// after a successful one is skipped, so a trigger that queued while a load ran (the refresh timer, a tab pick) does not run the same
/// slow reads again the moment the first ends.</para>
///
/// <para>#8950: beside the tables read runs the autovacuum settings read, which fills the line under the header with the write budget,
/// the settings in force and the cancels the store log holds in the last day. A settings read past its limit leaves the line saying it was
/// left out, and the tables still show.</para>
///
/// <para>#8924: the tables and indexes reads are held for <see cref="DarlingStoreTableHealthCache.Lifetime"/>, so a refresh the timer or a
/// tab pick starts shows the held rows and sends the store only the maintenance check. The Refresh button asks for a new read of both.
/// The strip beside it says when the tab last refreshed and, after that, when the store answered the tables and indexes on screen, so
/// held rows are read against the time they were true.</para>
/// </summary>
public partial class StoreHealthTab : UserControl
{
    private ViewerDataService? _dataService;
    private readonly ScopedLoadGenerations _loads = new();
    private bool _measuring;

    /// <summary>Each read's last outcome: true once its content is on screen, false once it failed. A read that a newer one superseded
    /// leaves its outcome to that newer read.</summary>
    private bool _tablesLoaded;
    private bool _indexesLoaded;
    private bool _readinessLoaded;

    /// <summary>True while a refresh runs. A second trigger that arrives meanwhile returns at once: the load on its way answers it.</summary>
    private bool _refreshing;

    /// <summary>The stamp text ("15:14:07 UTC") of the last refresh that loaded all three reads; null before the first.</summary>
    private string? _lastStamp;

    /// <summary>When that refresh ended, on the monotonic clock; null before the first.</summary>
    private long? _lastLoadedAt;

    /// <summary>When the store answered the tables and the indexes on screen (UTC); null while that section shows no read.</summary>
    private DateTime? _tablesReadAtUtc;
    private DateTime? _indexesReadAtUtc;

    /// <summary>#9374: when the store measured the sizes of the rows on screen, while the tables still show the store's last measured sizes
    /// (the first open's rows, before the live read replaced them). The flag clears when the live tables read lands. A failed live read
    /// leaves the flag and the rows, so the strip keeps saying where the sizes are from. The indexes have no such rows: only the live read fills them.</summary>
    private DateTime? _lastSizesAtUtc;
    private bool _tablesFromLastSizes;

    /// <summary>#9364: whether the tables and the indexes on screen are the rows the Viewer saved after its last live read of this store (a first
    /// open in a new process), with the time the store answered each. Each flag clears when that part's live read lands. A failed live read
    /// leaves the rows and the flag, so the strip keeps saying where they are from.</summary>
    private bool _tablesFromSnapshot;
    private bool _indexesFromSnapshot;
    private DateTime _snapshotTablesAtUtc;
    private DateTime _snapshotIndexesAtUtc;

    /// <summary>How long after a successful refresh another one that nothing forced is skipped (#8819). The tables read takes seconds on a
    /// large store, and a trigger queued behind a running load starts the moment it ends. The Refresh button is never skipped.</summary>
    internal TimeSpan FreshFor { get; set; } = TimeSpan.FromSeconds(5);

    /* #8549, #8819: the first open's clock (see ViewerFirstOpen): the shell starts it when the tab becomes visible and the tab's first load
       runs on it, so a wait behind another tab's refresh counts. Every later load runs on its own clock. A slow one writes a SlowLoad line. */
    private readonly ViewerFirstOpen _firstOpen = new();

    /// <summary>Where a slow load's line goes; null is the Viewer log's "SlowLoad" source. A test swaps it to read the line.</summary>
    internal Action<string>? SlowLoadSink { get; set; }

    /// <summary>The slow-load threshold of every load of this tab. A test lowers it to read a line without waiting three seconds.</summary>
    internal int SlowLoadThresholdMs { get => _firstOpen.SlowThresholdMs; set => _firstOpen.SlowThresholdMs = value; }

    /// <summary>Test seams for the three reads. Null reads the store through the data service. The tables and indexes reads take whether
    /// the Refresh button asked (true: a new store read; false: the held rows when the data service has them).</summary>
    internal Func<bool, Task<StoreHealthRead<IReadOnlyList<DarlingStoreTableHealthReader.StoreTableHealthRow>>>>? TablesRead { get; set; }
    internal Func<bool, Task<StoreIndexHealth>>? IndexesRead { get; set; }
    internal Func<Task<DarlingStoreMaintenanceReadinessReader.StoreMaintenanceReadiness>>? ReadinessRead { get; set; }

    /// <summary>#9374: test seams for the first open's read of the store's last measured sizes and for whether the data service already holds
    /// a tables read. Null reads through the data service; with no data service, no last sizes are read.</summary>
    internal Func<Task<StoreLastSizesHealth?>>? LastSizesRead { get; set; }
    internal Func<bool>? HoldsTablesRead { get; set; }

    /// <summary>#9364: where the tab saves its rows after each live read and shows them from on a first open in a new process. The shell's
    /// <see cref="Initialize"/> sets the file for the connected store. Null (a test with no store) saves and shows nothing.</summary>
    internal StoreHealthSnapshotStore? Snapshots { get; set; }

    /// <summary>The saves queued so far, one after another. A test awaits it to read the file.</summary>
    internal Task SnapshotSaves { get; private set; } = Task.CompletedTask;

    /// <summary>Test seam for the autovacuum settings read (#8950), which runs beside the tables read and fills the line under the header.
    /// Null reads the store through the data service; with no data service either, the line stays hidden.</summary>
    internal Func<bool, Task<StoreHealthRead<DarlingStoreTableHealthReader.StoreAutovacuumSettings>>>? AutovacuumSettingsRead { get; set; }

    /// <summary>The shell calls this when it makes the tab visible, so a wait behind another tab's refresh counts in a slow first open's line.</summary>
    internal void BeginFirstOpen() => _firstOpen.Begin();

    /// <summary>The shell calls this when the fleet lane starts the tab's refresh.</summary>
    internal void FirstOpenRefreshStarted() => _firstOpen.RefreshStarted();

    /// <summary>Raised with a short status message on load outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    public StoreHealthTab()
    {
        InitializeComponent();
    }

    /// <summary>Wires the data service.</summary>
    public void Initialize(ViewerDataService dataService)
    {
        _dataService = dataService;
        Snapshots = StoreHealthSnapshotStore.ForStore(dataService.StoreConnectionIdentity);
    }

    /// <summary>Reruns the tables, indexes and maintenance reads together (the shell calls this when the tab becomes visible and on its timer).
    /// Skipped when a refresh is running, or when one succeeded less than <see cref="FreshFor"/> ago.</summary>
    public Task RefreshAsync() => RefreshAsync(force: false);

    /// <summary>The refresh. <paramref name="force"/> is the Refresh button: it skips the freshness window, but not a refresh already running,
    /// and it reads the tables and indexes from the store instead of showing the held rows.</summary>
    internal async Task RefreshAsync(bool force)
    {
        if (_dataService == null && (TablesRead is null || IndexesRead is null || ReadinessRead is null))
        {
            return;
        }

        if (_refreshing)
        {
            return;
        }

        if (!force && LastLoadIsRecent())
        {
            ShowStamp();
            return;
        }

        _refreshing = true;
        var timer = _firstOpen.Take() ?? new ViewerLoadTimer(_firstOpen.SlowThresholdMs);
        try
        {
            ShowRefreshing();
            /* #9374: the first open, with nothing on screen and no read held, shows the store's last measured tables sizes the moment they
               arrive and the live read replaces them. The live reads start beside the last sizes read, never after it, so the refresh costs
               the longer of the two. The last sizes never overwrite a live result that landed first. */
            var readiness = LoadReadinessAsync(timer);
            var firstOpen = !force && !_tablesLoaded && TablesDataGrid.Items.Count == 0 && !(HoldsTablesRead?.Invoke() ?? _dataService?.HoldsStoreTables ?? false);
            /* #9364: the rows the Viewer saved after its last live read (a local file, read at once) and the store's last measured sizes
               are both read, and the tables on screen are whichever was measured later (the store's last sizes win a tie). The file's indexes
               stay, since the last sizes have no index rows. Both are shown beside the live reads, which start now. */
            var snapshot = firstOpen ? ShowSnapshotAsync(timer) : Task.FromResult(false);
            var lastSizes = firstOpen ? ShowLastSizesAsync(timer) : Task.CompletedTask;
            await Task.WhenAll(LoadTablesAsync(timer, fresh: force), LoadIndexesAsync(timer, fresh: force), readiness, snapshot, lastSizes);
            if (!StampIfLoaded())
            {
                ShowStamp();
            }
        }
        finally
        {
            _refreshing = false;
            timer.FinishAndLog("Store Health", SlowLoadSink);
        }
    }

    /// <summary>True when the last refresh loaded all three reads less than <see cref="FreshFor"/> ago.</summary>
    private bool LastLoadIsRecent() =>
        _lastLoadedAt is { } at && _tablesLoaded && _indexesLoaded && _readinessLoaded && Stopwatch.GetElapsedTime(at) < FreshFor;

    /// <summary>The shell calls this the moment the tab is picked, before the fleet lane may have started its load (another tab's load can
    /// hold the lane for seconds), so the tab says it is refreshing at once. A tab with nothing on screen already says "Loading…".</summary>
    internal void ShowRefreshPending()
    {
        if (!_refreshing && !LastLoadIsRecent())
        {
            ShowRefreshing();
        }
    }

    /// <summary>The strip beside Refresh while a refresh runs: it keeps the last stamp, so the page is still read against the time it was true.</summary>
    private void ShowRefreshing() =>
        LastRefreshedIndicator.Text = ProvisionalText() is { } provisional ? provisional + " A live read is running…"
            : _lastStamp is null ? "Loading…" : $"Refreshing… (last refreshed {_lastStamp}{CatalogsReadText()})";

    /// <summary>The strip beside Refresh when no refresh runs: the last stamp, or nothing before the first.</summary>
    private void ShowStamp() =>
        LastRefreshedIndicator.Text = ProvisionalText() is { } provisional ? provisional + " The live read failed."
            : _lastStamp is null ? "" : "Last refreshed " + _lastStamp + CatalogsReadText();

    /// <summary>#9364: what the strip says while any rows on screen are not from a live read of this open: where they are from and how old
    /// they are. Null when every section on screen is live.</summary>
    private string? ProvisionalText()
    {
        var parts = new List<string>();
        if (OnLastSizes())
        {
            parts.Add(LastSizesText());
        }

        if (_tablesFromSnapshot || _indexesFromSnapshot)
        {
            var oldest = (_tablesFromSnapshot, _indexesFromSnapshot) switch
            {
                (true, true) => _snapshotTablesAtUtc < _snapshotIndexesAtUtc ? _snapshotTablesAtUtc : _snapshotIndexesAtUtc,
                (true, false) => _snapshotTablesAtUtc,
                _ => _snapshotIndexesAtUtc,
            };
            var which = (_tablesFromSnapshot, _indexesFromSnapshot) switch
            {
                (true, true) => "Tables and indexes are",
                (true, false) => "Tables are",
                _ => "Indexes are",
            };
            parts.Add($"{which} from the last measurement, {StoreHealthRows.SnapshotAge(oldest, DateTime.UtcNow)} ago.");
        }

        return parts.Count == 0 ? null : string.Join(" ", parts);
    }

    /// <summary>#9364: puts the rows the Viewer saved after its last live read of this store on screen with the strip saying how old they are,
    /// unless the live read of a part has already landed (live rows are never overwritten). Leaves the screen alone when there is no file or it
    /// cannot be read. The file's tables yield to the store's last sizes when those are on screen and were measured at the same time or later.
    /// Returns whether the tables are on screen from the file.</summary>
    private async Task<bool> ShowSnapshotAsync(ViewerLoadTimer timer)
    {
        if (Snapshots is not { } store)
        {
            return false;
        }

        var saved = await ViewerServerTab.Timed(timer, "store saved rows read", Task.Run(store.Load));
        if (saved is null)
        {
            return false;
        }

        var tablesShown = false;
        if (!_tablesLoaded && saved.Tables is { } tables && saved.TablesReadAtUtc is { } tablesAt && !(OnLastSizes() && _lastSizesAtUtc >= tablesAt))
        {
            ShowTables(tables);
            _snapshotTablesAtUtc = tablesAt;
            _tablesFromSnapshot = true;
            _tablesFromLastSizes = false;
            tablesShown = true;
        }

        if (!_indexesLoaded && saved.Indexes is { } indexes && saved.IndexesReadAtUtc is { } indexesAt)
        {
            ShowIndexes(new StoreIndexHealth(indexes, saved.Coverage ?? [], saved.CoverageLeftOutNote, indexesAt));
            _snapshotIndexesAtUtc = indexesAt;
            _indexesFromSnapshot = true;
        }

        if (tablesShown || _indexesFromSnapshot)
        {
            timer.NotePhase("store saved rows read", $"showed {(tablesShown ? CountText.Plural(saved.Tables!.Count, "table") : "no tables")} and {(_indexesFromSnapshot ? CountText.Plural(saved.Indexes!.Count, "index", "indexes") : "no indexes")} from the last measurement");
            ShowRefreshing();
        }

        return tablesShown;
    }

    /// <summary>#9364: saves a live read for the next process's first open, off the UI thread, one save after another. A failed save is a log
    /// line (the store's own), never a message on screen.</summary>
    private void QueueSnapshotSave(Action<StoreHealthSnapshotStore> save)
    {
        if (Snapshots is not { } store)
        {
            return;
        }

        var previous = SnapshotSaves;
        SnapshotSaves = Task.Run(async () =>
        {
            await previous;
            save(store);
        });
    }

    /// <summary>#9374: whether the tables still show the store's last measured sizes.</summary>
    private bool OnLastSizes() => _lastSizesAtUtc is not null && _tablesFromLastSizes;

    /// <summary>#9374: what the strip says while the rows on screen are the store's last measured sizes, with when they were measured.</summary>
    private string LastSizesText() =>
        "Sizes are from the store's last measurement, taken " + ViewerTimeHelper.RefreshedStamp(_lastSizesAtUtc!.Value, ViewerTimeHelper.CurrentDisplayMode) + ".";

    /// <summary>#9374: reads the store's last measured sizes and puts the tables rows on screen, with the strip saying so, unless the live
    /// tables read has already landed (its rows are never overwritten). Leaves the screen alone when the store has no sizes to give (the live
    /// read then runs as it always did). Tables the Viewer saved (#9364) stay when they were measured later than these sizes, so a tie goes
    /// to these. The caller starts the live reads before awaiting this one.</summary>
    private async Task ShowLastSizesAsync(ViewerLoadTimer timer)
    {
        var read = LastSizesRead?.Invoke() ?? _dataService?.GetStoreLastSizesHealthAsync();
        if (read is null)
        {
            return;
        }

        var last = await ViewerServerTab.Timed(timer, "store last sizes read", read);
        if (last is null)
        {
            return;
        }

        timer.NotePhase("store last sizes read", StoreHealthRows.LastSizesNote(last.Tables.Rows.Count, last.MeasuredAtUtc, DateTime.UtcNow));
        if (_tablesLoaded || (_tablesFromSnapshot && _snapshotTablesAtUtc > last.MeasuredAtUtc))
        {
            return;
        }

        ShowTables(last.Tables.Rows);
        _lastSizesAtUtc = last.MeasuredAtUtc;
        _tablesFromLastSizes = true;
        _tablesFromSnapshot = false;
        ShowRefreshing();
    }

    /// <summary>What follows the stamp: when the store answered the tables and indexes on screen, the older of the two. The rows are held
    /// between store reads, so this time can be minutes before the refresh stamp. A section whose read failed shows no rows, so the time
    /// is then the other section's alone, and the text is empty when neither shows a read.</summary>
    private string CatalogsReadText()
    {
        static string At(DateTime utc) => ViewerTimeHelper.RefreshedStamp(utc, ViewerTimeHelper.CurrentDisplayMode);

        return (_tablesReadAtUtc, _indexesReadAtUtc) switch
        {
            ({ } tables, { } indexes) => ", tables and indexes read " + At(tables < indexes ? tables : indexes),
            ({ } tables, null) => ", tables read " + At(tables),
            (null, { } indexes) => ", indexes read " + At(indexes),
            _ => string.Empty,
        };
    }

    /// <summary>Raises "store health: refreshed …" when all three reads last finished. Each read reports its own failure, so the stamp never
    /// writes over one. Returns whether it stamped.</summary>
    private bool StampIfLoaded()
    {
        if (!(_tablesLoaded && _indexesLoaded && _readinessLoaded))
        {
            return false;
        }

        _lastStamp = ViewerTimeHelper.RefreshedStamp();
        _lastLoadedAt = Stopwatch.GetTimestamp();
        ShowStamp();
        StatusChanged?.Invoke($"store health: refreshed {_lastStamp}");
        return true;
    }

    /// <summary>The autovacuum settings line, or why it is missing: a read past its limit is left out with the reader's note, and any other
    /// failure says so. Never throws, so the tables read beside it is never cut short by it. Empty text with no read to run.</summary>
    private async Task<(string Text, DateTime? ReadAtUtc)> AutovacuumSettingsOutcomeAsync(ViewerLoadTimer timer, bool fresh)
    {
        var read = AutovacuumSettingsRead?.Invoke(fresh) ?? _dataService?.GetStoreAutovacuumSettingsAsync(fresh);
        if (read is null)
        {
            return (string.Empty, null);
        }

        try
        {
            var settings = await ViewerServerTab.Timed(timer, "store autovacuum settings read", read);
            return (StoreHealthRows.AutovacuumSettingsText(settings.Rows), settings.ReadAtUtc);
        }
        catch (StoreTableHealthTimeoutException ex)
        {
            return ("The autovacuum settings were left out. " + ex.Message, null);
        }
        catch (Exception ex)
        {
            /* #9365: one warning line in the Viewer log naming the tab and the read, and plain words in the tab and the status bar, never the driver's text. */
            var text = ViewerRefreshFailure.Report("Store Health", timer, ex);
            StatusChanged?.Invoke(text);
            return (text, null);
        }
    }

    private async Task LoadTablesAsync(ViewerLoadTimer timer, bool fresh)
    {
        var gen = _loads.Claim(nameof(LoadTablesAsync));
        var settingsOutcome = AutovacuumSettingsOutcomeAsync(timer, fresh);
        /* #8819: a refresh over rows already on screen keeps them and the summary line; the loading text is for a grid with nothing in it. */
        if (TablesDataGrid.Items.Count == 0)
        {
            TablesMessage.Text = "Loading store tables…";
            TablesMessage.Visibility = Visibility.Visible;
            HeaderText.Text = "Loading store tables…";
        }

        try
        {
            var startedUtc = DateTime.UtcNow;
            var read = await ViewerServerTab.Timed(timer, "store tables read", TablesRead?.Invoke(fresh) ?? _dataService!.GetStoreTableHealthAsync(fresh));
            /* #9364: the slow-load line says how many relations the read sized and whether the store measured them for this open or the Viewer held them. */
            timer.NotePhase("store tables read", StoreHealthRows.ReadNote(read.Rows.Count, read.ReadAtUtc, startedUtc, DateTime.UtcNow));
            if (_loads.Superseded(nameof(LoadTablesAsync), gen)) return;

            ShowTables(read.Rows);
            _tablesReadAtUtc = read.ReadAtUtc;
            _tablesFromLastSizes = false;
            _tablesFromSnapshot = false;
            _tablesLoaded = true;
            QueueSnapshotSave(store => store.SaveTables(read.Rows, read.ReadAtUtc));
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadTablesAsync), gen)) return;

            _tablesLoaded = false;
            _tablesReadAtUtc = null;
            /* #9365: one warning line in the Viewer log naming the tab and the read, and plain words in the tab and the status bar, never the driver's text. */
            var text = ViewerRefreshFailure.Report("Store Health", timer, ex);
            if (!_tablesFromLastSizes && !_tablesFromSnapshot)
            {
                /* #9374: rows that are the store's last measured sizes stay when the live read fails, and the strip keeps saying so. */
                TablesDataGrid.ItemsSource = null;
                HeaderText.Text = "The store tables failed to load.";
                TablesMessage.Text = text;
                TablesMessage.Visibility = Visibility.Visible;
            }

            StatusChanged?.Invoke(text);
        }

        /* The settings are held and read beside the tables, so the strip's "tables read" time is the older of the two answers. */
        var (settingsText, settingsReadAtUtc) = await settingsOutcome;
        if (_loads.Superseded(nameof(LoadTablesAsync), gen)) return;

        AutovacuumSettingsTextBlock.Text = settingsText;
        AutovacuumSettingsTextBlock.Visibility = settingsText.Length == 0 ? Visibility.Collapsed : Visibility.Visible;
        if (_tablesReadAtUtc is { } tablesAt && settingsReadAtUtc is { } settingsAt && settingsAt < tablesAt)
        {
            _tablesReadAtUtc = settingsAt;
        }

        UpdateMeasureButton();
    }

    /// <summary>Puts <paramref name="tableRows"/> in the tables grid and the header, keeping the selected table.</summary>
    private void ShowTables(IReadOnlyList<DarlingStoreTableHealthReader.StoreTableHealthRow> tableRows)
    {
        var selected = (TablesDataGrid.SelectedItem as StoreTableRow)?.Table;
        var rows = StoreHealthRows.Tables(tableRows);
        TablesDataGrid.ItemsSource = rows;
        TablesDataGrid.SelectedItem = selected is null ? null : rows.Find(r => r.Table == selected);
        HeaderText.Text = StoreHealthRows.HeaderText(tableRows);
        TablesMessage.Text = "The store has no tables to show.";
        TablesMessage.Visibility = rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>Puts the index rows and the coverage rows of <paramref name="read"/> on screen.</summary>
    private void ShowIndexes(StoreIndexHealth read)
    {
        var indexRows = StoreHealthRows.Indexes(read.Indexes);
        IndexesDataGrid.ItemsSource = indexRows;
        IndexHeaderText.Text = StoreHealthRows.IndexHeaderText(read.Indexes);
        IndexesMessage.Text = "The store has no indexes to show.";
        IndexesMessage.Visibility = indexRows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

        var coverageRows = StoreHealthRows.CoverageRows(read.Coverage);
        CoverageDataGrid.ItemsSource = coverageRows;
        CoverageDataGrid.Visibility = coverageRows.Count == 0 ? Visibility.Collapsed : Visibility.Visible;
        CoverageMessage.Text = read.CoverageLeftOutNote is null
            ? "No partitioned tables in the store have indexes to check."
            : "The partition index coverage was left out. " + read.CoverageLeftOutNote;
        CoverageMessage.Visibility = coverageRows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    private async Task LoadIndexesAsync(ViewerLoadTimer timer, bool fresh)
    {
        var gen = _loads.Claim(nameof(LoadIndexesAsync));
        if (IndexesDataGrid.Items.Count == 0)
        {
            IndexesMessage.Text = "Loading store indexes…";
            IndexesMessage.Visibility = Visibility.Visible;
            IndexHeaderText.Text = "Loading store indexes…";
        }

        if (CoverageDataGrid.Items.Count == 0)
        {
            CoverageMessage.Text = "Loading partition index coverage…";
            CoverageMessage.Visibility = Visibility.Visible;
        }

        try
        {
            var startedUtc = DateTime.UtcNow;
            var read = await ViewerServerTab.Timed(timer, "store indexes read", IndexesRead?.Invoke(fresh) ?? _dataService!.GetStoreIndexHealthAsync(fresh));
            /* #9364: as the tables read. An answer with no read time adds no note. */
            if (read.ReadAtUtc is { } indexesAt)
            {
                timer.NotePhase("store indexes read", StoreHealthRows.ReadNote(read.Indexes.Count, indexesAt, startedUtc, DateTime.UtcNow));
            }
            if (_loads.Superseded(nameof(LoadIndexesAsync), gen)) return;

            ShowIndexes(read);
            _indexesReadAtUtc = read.ReadAtUtc;
            _indexesFromSnapshot = false;
            _indexesLoaded = true;
            QueueSnapshotSave(store => store.SaveIndexes(read));
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadIndexesAsync), gen)) return;

            _indexesLoaded = false;
            _indexesReadAtUtc = null;
            /* #9365: one warning line in the Viewer log naming the tab and the read, and plain words in the tab and the status bar, never the driver's text. */
            var text = ViewerRefreshFailure.Report("Store Health", timer, ex);
            if (!_indexesFromSnapshot)
            {
                /* #9364: rows that are the saved last measurement stay when the live read fails, and the strip keeps saying so. */
                IndexesDataGrid.ItemsSource = null;
                CoverageDataGrid.ItemsSource = null;
                CoverageDataGrid.Visibility = Visibility.Collapsed;
                IndexHeaderText.Text = "The store indexes failed to load.";
                IndexesMessage.Text = text;
                IndexesMessage.Visibility = Visibility.Visible;
                CoverageMessage.Text = "Partition index coverage failed to load.";
                CoverageMessage.Visibility = Visibility.Visible;
            }

            StatusChanged?.Invoke(text);
        }
    }

    private async Task LoadReadinessAsync(ViewerLoadTimer timer)
    {
        var gen = _loads.Claim(nameof(LoadReadinessAsync));
        /* A verdict already on screen stays until the new one lands. */
        if (!_readinessLoaded)
        {
            ReadinessTextBlock.Text = "Checking whether now is a safe time for maintenance…";
        }

        try
        {
            var readiness = await ViewerServerTab.Timed(timer, "store maintenance check", ReadinessRead?.Invoke() ?? _dataService!.GetStoreMaintenanceReadinessAsync());
            if (_loads.Superseded(nameof(LoadReadinessAsync), gen)) return;

            ShowReadiness(StoreHealthRows.ReadinessParts(readiness));
            _readinessLoaded = true;
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadReadinessAsync), gen)) return;

            _readinessLoaded = false;
            /* #9365: one warning line in the Viewer log naming the tab and the read, and plain words in the tab and the status bar, never the driver's text. */
            var text = ViewerRefreshFailure.Report("Store Health", timer, ex);
            ReadinessTextBlock.Text = text;
            StatusChanged?.Invoke(text);
        }
    }

    /// <summary>Fills the verdict block: the verdict lead in semibold, the sentences and reasons after it in normal weight.</summary>
    private void ShowReadiness((string Lead, string After) parts)
    {
        ReadinessTextBlock.Inlines.Clear();
        if (parts.Lead.Length > 0)
        {
            ReadinessTextBlock.Inlines.Add(new Run(parts.Lead) { FontWeight = FontWeights.SemiBold });
        }

        if (parts.After.Length > 0)
        {
            ReadinessTextBlock.Inlines.Add(new Run(parts.After));
        }
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e) => await RefreshAsync(force: true);

    private void TablesDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e) => UpdateMeasureButton();

    /// <summary>Measure is on for a plain table, and off while a measure runs (one table at a time).</summary>
    private void UpdateMeasureButton()
    {
        var target = (TablesDataGrid.SelectedItem as StoreTableRow)?.MeasureTarget;
        MeasureButton.IsEnabled = target is not null && !_measuring;
        if (!_measuring && TablesDataGrid.SelectedItem is StoreTableRow { MeasureTarget: null } other)
        {
            MeasureResultText.Text = $"{other.Table} is a {other.Kind}. Measure works on plain tables. {StoreHealthRows.MeasureHint}";
        }
    }

    private async void MeasureButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null || _measuring || (TablesDataGrid.SelectedItem as StoreTableRow)?.MeasureTarget is not { } target)
        {
            return;
        }

        _measuring = true;
        MeasureButton.IsEnabled = false;
        MeasureResultText.Text = $"Measuring {target}. This reads every page of the table.";
        var clock = Stopwatch.StartNew();
        try
        {
            var result = await _dataService.MeasureStoreTableAsync(target);
            MeasureResultText.Text = StoreHealthRows.MeasureText(target, result, clock.Elapsed);
        }
        catch (Exception ex)
        {
            MeasureResultText.Text = $"Measuring {target} failed: {ex.Message}";
            StatusChanged?.Invoke($"failed to measure {target}: {ex.Message}");
        }
        finally
        {
            _measuring = false;
            UpdateMeasureButton();
        }
    }
}
