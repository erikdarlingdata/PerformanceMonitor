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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Column selectivity (#8798), the <c>column_stats</c> run of one server. Reads the header and density vector of each candidate
/// statistic with DBCC SHOW_STATISTICS and keeps the current rows in <c>collect.column_stats</c>, with one run row per database in
/// <c>collect.column_stats_runs</c>.
///
/// <para><b>Which databases.</b> Only on a server where <c>schema_objects</c> is enabled, and only the databases both its scope and
/// <c>column_stats</c>' own scope cover (exclusions win), writable by the schema pass's rules
/// (<see cref="SchemaWorkPlanner.NotWritableReason"/>). Oldest full read first, a never-read database before any. Only where column_stats
/// itself is enabled: a named run of a collector that is off reads nothing. A database in scope that is not read is still stamped, with a
/// state row made for it when it has none. A database the run had before and no longer has loses its state row and, unless another server
/// still points at the same history, its rows: on a server one that is dropped or out of scope; on Azure SQL Database, whose list can be
/// one database or online ones only, one out of scope.</para>
///
/// <para><b>Incremental.</b> A statistic is read when it is new, its <c>STATS_DATE</c> differs from the stored one, or a name it was
/// stored under changed (a renamed schema, table, statistic or column keeps <c>STATS_DATE</c>), and the stored
/// rows of a statistic that is gone are deleted. A database with no state, a new recovery fork (a restore) or a new history key starts
/// from nothing: its rows are deleted first, so a baseline cut short continues on the next run with what is still missing. Run now
/// re-reads every candidate. Statistics are read in batches of <see cref="BatchSize"/>, largest tables first, and each batch is stored
/// before the next starts.</para>
///
/// <para><b>Permissions.</b> A database where the login lacks SELECT is skipped (<c>refused = no_select</c>) and named in the note, with one
/// run row until the answer changes (the newest row keeps the refusal for the status in get_object_definition). The
/// note names a precondition only when no database had the grant or a database in <c>column_stats</c>' own list lacked it: an empty list
/// means every database while the grant goes to a few, so a missing grant is normal. Trace flag 9485 makes SELECT insufficient for DBCC
/// SHOW_STATISTICS; while it is on, a database where nothing could be read (no statistic read this run, and nothing stored or a stored
/// statistic now refused) records <c>refused = tf9485</c>; a refusal among readable statistics is an error.</para>
///
/// <para><b>Budgets.</b> The schema pass's values: five minutes per run, checked before a database starts and between batches, and 120
/// seconds per database, checked between batches and cancelling a batch in flight. Every skip ends in the note, never in an exception.</para>
/// </summary>
public static class DarlingColumnStatsCapture
{
    /// <summary>Statistics per read batch; the rows of each batch are stored before the next starts.</summary>
    internal const int BatchSize = 200;

    /* A named run (run_collectors_now, the web run) dispatches a collector even when it is off; this one reads nothing then. */
    internal const string CollectorOffNote = "column_stats is off for this server";

    private const string SchemaObjectsOffNote =
        "column_stats reads only on a server where schema_objects is enabled, and it is not enabled here: enable schema_objects first. Nothing was read.";

    private const string GrantSentence =
        "DBCC SHOW_STATISTICS needs SELECT in the database: GRANT SELECT TO the monitoring login there (the column_stats block of the grant script)";

    /* A server message kept in a run row (the sample error, a failed read's note) is cut here, with McpHelpers.Truncate's marker. */
    internal const int MaxStoredMessageChars = 500;

    private const string NoSelectRefusal = "no_select";

    private const int LockTimeoutError = 1222;
    private const int HeaderShapeError = 213;

    /* SELECT denied on an object (229) or a column (230), and DBCC's own refusal (2557). */
    private static readonly HashSet<int> s_permissionErrors = [229, 230, 2557];

    private static readonly TimeSpan s_runBudget = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan s_databaseBudget = TimeSpan.FromSeconds(120);

    public static async Task<CollectorRunResult> RunAsync(DarlingCollectorRunner runner, ServerRuntime server, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(runner);
        ArgumentNullException.ThrowIfNull(server);

        if (!runner.CollectorEnabledFor(ColumnStatsCollector.Instance.Name, server.ServerId))
        {
            return new CollectorRunResult(0, 0, 0, CollectorContext.NoMeasurements, CollectorOffNote);
        }

        if (!runner.CollectorEnabledFor(SchemaObjectsCollector.Instance.Name, server.ServerId))
        {
            return new CollectorRunResult(0, 0, 0, CollectorContext.NoMeasurements, SchemaObjectsOffNote);
        }

        var run = new Run(runner, server, DarlingSnapshotScope.IsActive, ct);
        await using var store = await runner.Store.OpenConnectionAsync(StoreSessionNames.SchemaCapture, ct);
        run.Store = store;
        await run.LoadTraceFlagAsync();
        if (server.Target.IsAzureSqlDb)
        {
            await run.RunAzureAsync();
        }
        else
        {
            await run.RunServerAsync();
        }

        return await run.FinishAsync();
    }

    private sealed class Run
    {
        private readonly DarlingCollectorRunner _runner;
        private readonly ServerRuntime _server;
        private readonly bool _runNow;
        private readonly CancellationToken _ct;
        private readonly DateTime _runTime = DateTime.UtcNow;
        private readonly PassBudget _runBudget;
        private readonly TimeSpan _databaseBudget;
        private readonly IReadOnlyList<string> _schemaScope;
        private readonly IReadOnlyList<string> _ownScope;
        private readonly List<ColumnStatsCollector.RunRow> _rows = [];
        private readonly Stopwatch _sql = new();
        private readonly Stopwatch _storage = new();

        /* What the note says, collected by kind. */
        private readonly Dictionary<string, List<string>> _notWritable = new(StringComparer.Ordinal);
        private readonly List<string> _noSelect = [];
        private readonly List<string> _tf9485 = [];
        private readonly List<string> _failed = [];
        private readonly List<string> _unfinished = [];
        private int _granted;
        private int _repeatedRefusals;
        private int _locked;
        private int _errors;
        private string? _sampleError;
        private int _started;
        private int _toRead;
        private bool _runBudgetStopped;
        private bool _traceFlag9485;

        /* The STAT_HEADER shape: from the build until a batch proves the other one. Only a shape that worked is kept. */
        private bool? _headerShape;

        public Run(DarlingCollectorRunner runner, ServerRuntime server, bool runNow, CancellationToken ct)
        {
            _runner = runner;
            _server = server;
            _runNow = runNow;
            _ct = ct;
            var budgets = runner.SchemaBudgetsForTests;
            _runBudget = new PassBudget(budgets?.Pass ?? s_runBudget);
            _databaseBudget = budgets?.Database ?? s_databaseBudget;
            _schemaScope = runner.DatabaseScopeFor(SchemaObjectsCollector.Instance.Name, server.ServerId);
            _ownScope = runner.DatabaseScopeFor(ColumnStatsCollector.Instance.Name, server.ServerId);
        }

        public NpgsqlConnection Store { get; set; } = null!;

        public async Task LoadTraceFlagAsync()
        {
            var flags = await TimedStore(() => DarlingCurrentConfigReader.GetLatestTraceFlagsAsync(_runner.Store, _server.ServerId, _ct));
            _traceFlag9485 = flags.Rows.Any(f => f.TraceFlag == 9485 && f.Status && f.IsGlobal);
        }

        public async Task RunServerAsync()
        {
            await using var connection = await TimedSql(() => _runner.ColumnStatsSource.OpenAsync(_server, null, _ct));
            var probes = await TimedSql(() => connection.ReadProbesAsync(_ct));
            var work = new List<(SchemaDatabaseProbe Probe, ColumnStatsStateRow? State)>();
            var covered = Covered(probes);
            await ForgetGoneAsync(covered.Select(p => p.Name).ToList());
            foreach (var probe in covered)
            {
                var state = await TimedStore(() => ColumnStatsStore.LoadStateAsync(Store, _server.ServerId, probe.Name, _ct));
                if (NoteNotWritable(probe))
                {
                    /* Not read, but still in scope: a read-only, standby or offline database's statistics cannot change there, and a
                       secondary's rows are its group's, so the stored rows stay current and the stamp keeps them from ageing out. */
                    await StampNotReadAsync(state, probe);
                    continue;
                }

                work.Add((probe, state));
            }

            _toRead = work.Count;
            foreach (var (probe, state) in work.OrderBy(w => w.State?.LastFullReadAt ?? DateTime.MinValue).ThenBy(w => w.Probe.Name, StringComparer.Ordinal))
            {
                _ct.ThrowIfCancellationRequested();
                if (await StopForRunBudgetAsync(state))
                {
                    continue;
                }

                await ReadDatabaseAsync(connection, probe, state);
            }
        }

        public async Task RunAzureAsync()
        {
            var listed = _runner.AzureDatabaseListOverrideForTests is { } listOverride
                ? await TimedSql(() => listOverride(_server, _ct))
                : await TimedSql(() => _runner.GetAzureDatabaseListAsync(_server, _schemaScope, _ct));
            var work = new List<(string Database, ColumnStatsStateRow? State)>();
            foreach (var database in AzureSweepScope.WithoutMaster(listed).Order(StringComparer.Ordinal))
            {
                /* The scope check by name first, so a database the scopes leave out is never connected to. */
                if (Covered([NameOnly(database)]).Count > 0)
                {
                    work.Add((database, await TimedStore(() => ColumnStatsStore.LoadStateAsync(Store, _server.ServerId, database, _ct))));
                }
            }

            await ForgetOutOfScopeAzureAsync();
            _toRead = work.Count;
            foreach (var (database, state) in work.OrderBy(w => w.State?.LastFullReadAt ?? DateTime.MinValue))
            {
                _ct.ThrowIfCancellationRequested();
                if (await StopForRunBudgetAsync(state))
                {
                    continue;
                }

                try
                {
                    await using var connection = await TimedSql(() => _runner.ColumnStatsSource.OpenAsync(_server, database, _ct));
                    var probe = (await TimedSql(() => connection.ReadProbesAsync(_ct)))
                        .FirstOrDefault(p => string.Equals(p.Name, database, StringComparison.OrdinalIgnoreCase));
                    if (probe is null || Covered([probe]).Count == 0)
                    {
                        continue;
                    }

                    if (NoteNotWritable(probe))
                    {
                        await StampNotReadAsync(state, probe);
                        continue;
                    }

                    await ReadDatabaseAsync(connection, probe, state);
                }
                catch (Exception ex) when (ex is not OperationCanceledException && ex is not NpgsqlException)
                {
                    _failed.Add(database);
                    _runner.Logger?.LogWarning("[{Server}] [{Database}] The column_stats read could not reach the database: {Message}", _server.Config.DisplayName, database, ex.Message);
                }
            }
        }

        private static SchemaDatabaseProbe NameOnly(string database) =>
            new(database, int.MaxValue, "ONLINE", false, false, null, true, null, null, null, null);

        /* The probed databases both scopes cover, exclusions winning. */
        private List<SchemaDatabaseProbe> Covered(IReadOnlyList<SchemaDatabaseProbe> probes)
        {
            var schema = new HashSet<string>(DarlingSchemaCapture.InScope(probes, _schemaScope, _server.Config.ExcludedDatabases), StringComparer.Ordinal);
            var own = new HashSet<string>(DarlingSchemaCapture.InScope(probes, _ownScope, _server.Config.ExcludedDatabases), StringComparer.Ordinal);
            return probes.Where(p => schema.Contains(p.Name) && own.Contains(p.Name)).ToList();
        }

        private bool NoteNotWritable(SchemaDatabaseProbe probe)
        {
            var reason = SchemaWorkPlanner.NotWritableReason(probe);
            if (reason is null)
            {
                return false;
            }

            if (!_notWritable.TryGetValue(reason, out var names))
            {
                _notWritable[reason] = names = [];
            }

            names.Add(probe.Name);
            return true;
        }

        /* True when the run budget is spent and this database is left for the next run; the first database always starts. */
        private async Task<bool> StopForRunBudgetAsync(ColumnStatsStateRow? state)
        {
            if (_started > 0 && _runBudget.Spent)
            {
                _runBudgetStopped = true;
                await StampAsync(state);
                return true;
            }

            _started++;
            return false;
        }

        /* A database in scope keeps its stamp fresh even when it is not read, so its stored rows are not taken for a database that left scope. */
        private async Task StampAsync(ColumnStatsStateRow? state)
        {
            if (state is not null)
            {
                await TimedStore(() => ColumnStatsStore.SaveStateAsync(Store, state with { LastSeenInScopeAt = _runTime }, _ct));
            }
        }

        /* A database this server does not read (read-only, standby, offline, a secondary replica). With no state row yet (a secondary that
           was never primary) one is made under the probe's history key, never fully read, so the group's rows stay while this server
           still sees the database even after the server that read them is switched off or removed. */
        private Task StampNotReadAsync(ColumnStatsStateRow? state, SchemaDatabaseProbe probe) =>
            state is not null
                ? StampAsync(state)
                : TimedStore(() => ColumnStatsStore.SaveStateAsync(
                    Store,
                    new ColumnStatsStateRow(
                        _server.ServerId, probe.Name, DarlingSchemaCapture.KeyText(SchemaWorkPlanner.KeyFor(_server.ServerId, probe)), probe.RecoveryForkGuid, _runTime, null),
                    _ct));

        /* Azure SQL Database: the list can be the one-database fallback or online databases only, so a database missing from it is not
           known to be gone. Leaving a scope is configuration, though: a state row whose database either scope (or an exclusion) now leaves
           out is forgotten by name, as on a server. */
        private async Task ForgetOutOfScopeAzureAsync()
        {
            var states = await TimedStore(() => ColumnStatsStore.LoadServerStatesAsync(Store, _server.ServerId, _ct));
            foreach (var state in states.Where(s => Covered([NameOnly(s.Database)]).Count == 0))
            {
                await TimedStore(() => ColumnStatsStore.ForgetDatabaseAsync(Store, state, _ct));
            }
        }

        /* The run's own databases that are gone (dropped, renamed) or out of either scope: their state row goes, and their rows too when no
           other server's state row still points at the same history and database (a group's rows stay while another replica sees it). */
        private async Task ForgetGoneAsync(IReadOnlyCollection<string> covered)
        {
            var keep = new HashSet<string>(covered, StringComparer.Ordinal);
            var states = await TimedStore(() => ColumnStatsStore.LoadServerStatesAsync(Store, _server.ServerId, _ct));
            foreach (var state in states.Where(s => !keep.Contains(s.Database)))
            {
                await TimedStore(() => ColumnStatsStore.ForgetDatabaseAsync(Store, state, _ct));
            }
        }

        private async Task ReadDatabaseAsync(IColumnStatsConnection connection, SchemaDatabaseProbe probe, ColumnStatsStateRow? state)
        {
            var database = probe.Name;
            var key = SchemaWorkPlanner.KeyFor(_server.ServerId, probe);
            var keyText = DarlingSchemaCapture.KeyText(key);
            await StampAsync(state);

            var forkChanged = state?.RecoveryForkGuid is { } storedFork && probe.RecoveryForkGuid is { } fork && storedFork != fork;
            var keyChanged = state is not null && !string.Equals(state.HistoryKey, keyText, StringComparison.Ordinal);
            var fresh = state is null || forkChanged || keyChanged;
            var kind = _runNow ? "run_now" : fresh || state!.LastFullReadAt is null ? "baseline" : "incremental";
            var tally = new Tally();

            using var budgetSource = EnumeratedCollectorDriver.StartItemBudget(_databaseBudget, _ct);
            var token = budgetSource?.Token ?? _ct;
            var databaseBudget = new PassBudget(_databaseBudget);
            var advanceState = !fresh;
            var complete = false;
            string? refused = null;
            string? note = null;
            var skipped = SkipCounts.None;
            var candidateCount = 0;
            try
            {
                if (!await TimedSql(() => connection.HasSelectAsync(database, token)))
                {
                    _noSelect.Add(database);

                    /* One run row per refusal, not one per run: while the newest row of the history (any server's, the row the status in
                       get_object_definition reads) already says no_select the answer has not changed, and that row stays the newest. The
                       database still counts in the run's rows, so a server where every database is refused does not read to collection
                       health as a collector that produced and then stopped. */
                    var previous = await TimedStore(() => ColumnStatsStore.LoadNewestRefusedAsync(Store, keyText, database, _ct));
                    if (previous == NoSelectRefusal)
                    {
                        _repeatedRefusals++;
                    }
                    else
                    {
                        _rows.Add(Row(database, key, kind, 0, tally, SkipCounts.None, NoSelectRefusal, complete: false, "The login has no SELECT in this database; nothing was read."));
                    }

                    return;
                }

                _granted++;
                var (listed, skippedByRead, allIds) = await TimedSql(() => connection.ReadCandidatesAsync(database, token));
                var candidates = listed.OrderByDescending(c => c.TableRows).Take(ColumnStatsModel.MaxStatisticsPerDatabase).ToList();
                skipped = skippedByRead with { OverCap = skippedByRead.OverCap + listed.Count - candidates.Count };
                candidateCount = candidates.Count;

                if (fresh)
                {
                    /* A fresh start replaces the history's rows, whichever server wrote them: a new primary with no state row for the
                       database (it never stamped it, as a secondary that was not in scope or not yet run) starts here and removes what the old
                       primary stored. A new primary that stamped the database as a secondary has a state row under the group's key, is not
                       fresh, and reads against the group's rows instead (they describe the same database). A database that moved from this server's own key (it joined a group) also
                       loses the rows under that key; a group's rows are the group's and are left to its servers and the stamp age. */
                    /* The new key and fork are saved with the deletes, in one transaction under the history lock, not after the last
                       batch: a purge in between would otherwise find no stamp under the new key and delete the rows this run writes. */
                    var ownKey = DarlingSchemaCapture.KeyText(new SchemaHistoryKey(_server.ServerId, null, null));
                    var alsoClear = keyChanged && state!.HistoryKey == ownKey ? ownKey : null;
                    await TimedStore(() => ColumnStatsStore.StartFreshAsync(
                        Store, new ColumnStatsStateRow(_server.ServerId, database, keyText, probe.RecoveryForkGuid ?? state?.RecoveryForkGuid, _runTime, null), alsoClear, _ct));
                    advanceState = true;
                }

                var stored = await TimedStore(() => ColumnStatsStore.LoadStoredAsync(Store, keyText, database, _ct));
                var byId = candidates.ToDictionary(c => (c.ObjectId, c.StatsId));

                /* Over the cap the read list is not the whole database: a statistic is gone when it is missing from the list of every
                   candidate's ids the read returned beside it. Without that list (none came back) nothing is known to be gone. */
                var existing = skippedByRead.OverCap == 0
                    ? listed.Select(c => (c.ObjectId, c.StatsId)).ToHashSet()
                    : allIds.Count > 0 ? allIds.ToHashSet() : null;
                if (existing is not null)
                {
                    var gone = stored.Keys.Where(k => !existing.Contains(k)).ToList();
                    await TimedStore(() => ColumnStatsStore.DeleteStatisticsAsync(Store, keyText, database, gone, _ct));
                    tally.Deleted = gone.Count;
                }

                var toRead = candidates
                    .Where(c => _runNow || ColumnStatsModel.Decide(c, stored.GetValueOrDefault((c.ObjectId, c.StatsId))) != StatChange.Unchanged)
                    .ToList();

                complete = true;
                for (var offset = 0; offset < toRead.Count; offset += BatchSize)
                {
                    if (offset > 0 && (databaseBudget.Spent || _runBudget.Spent))
                    {
                        complete = false;
                        note = string.Create(CultureInfo.InvariantCulture, $"Budget spent after {offset} of {toRead.Count} statistics to read; the rest are read on the next run.");
                        _unfinished.Add(database);
                        break;
                    }

                    await ReadBatchAsync(connection, database, keyText, toRead.Skip(offset).Take(BatchSize).ToList(), byId, tally, token);
                }

                /* Trace flag 9485 explains a refusal only when nothing could be read: this run read nothing, and either nothing is stored
                   for the database or a statistic read before now fails. One denied column among readable statistics is an error. */
                if (_traceFlag9485 && tally.PermissionFailed.Count > 0 && tally.Read == 0
                    && (stored.Count == 0 || tally.PermissionFailed.Any(stored.ContainsKey)))
                {
                    refused = "tf9485";
                    _tf9485.Add(database);
                }
            }
            catch (OperationCanceledException) when (!_ct.IsCancellationRequested)
            {
                complete = false;
                note = "The read did not finish within " + ((int)_databaseBudget.TotalSeconds).ToString(CultureInfo.InvariantCulture) + " s; the rest is read on the next run.";
                _unfinished.Add(database);
                _runner.Logger?.LogWarning("[{Server}] [{Database}] The column_stats read did not finish within its budget", _server.Config.DisplayName, database);
            }
            catch (Exception ex) when (ex is not OperationCanceledException && ex is not NpgsqlException)
            {
                complete = false;
                note = "The read failed: " + McpHelpers.Truncate(ex.Message, MaxStoredMessageChars);
                _failed.Add(database);
                _runner.Logger?.LogWarning("[{Server}] [{Database}] The column_stats read failed: {Message}", _server.Config.DisplayName, database, ex.Message);
            }

            /* A fresh start that failed before its rows were deleted keeps the old fork and key, so the next run starts fresh again. */
            if (advanceState)
            {
                var saved = new ColumnStatsStateRow(
                    _server.ServerId,
                    database,
                    keyText,
                    probe.RecoveryForkGuid ?? state?.RecoveryForkGuid,
                    _runTime,
                    complete ? _runTime : fresh ? null : state?.LastFullReadAt);
                await TimedStore(() => ColumnStatsStore.SaveStateAsync(Store, saved, _ct));
            }

            _rows.Add(Row(database, key, kind, candidateCount, tally, skipped, refused, complete, note));
        }

        private async Task ReadBatchAsync(
            IColumnStatsConnection connection, string database, string keyText, List<StatCandidate> batch,
            Dictionary<(int, int), StatCandidate> byId, Tally tally, CancellationToken token)
        {
            var ids = batch.Select(c => (c.ObjectId, c.StatsId)).ToList();
            var shape = _headerShape ?? ShapeFromBuild(connection.ServerVersion);
            var (rows, errors) = await TimedSql(() => connection.ReadStatisticsAsync(database, ids, shape, token));
            if (errors.Any(e => e.Number == HeaderShapeError))
            {
                /* The header has the other column count on this server: once with the other shape, never more. */
                (rows, errors) = await TimedSql(() => connection.ReadStatisticsAsync(database, ids, !shape, token));
                if (!errors.Any(e => e.Number == HeaderShapeError))
                {
                    _headerShape = !shape;
                }
            }
            else
            {
                _headerShape = shape;
            }

            var write = new List<(StatCandidate Stat, StatHeader Header, IReadOnlyList<StatPrefix> Prefixes)>();
            foreach (var row in rows)
            {
                if (byId.TryGetValue((row.ObjectId, row.StatsId), out var stat))
                {
                    write.Add((stat, row.Header, ColumnStatsModel.MapDensityRows(stat, row.AllDensities)));
                }
            }

            await TimedStore(() => ColumnStatsStore.ReplaceStatisticsAsync(Store, _server.ServerId, keyText, database, write, DateTime.UtcNow, _ct));
            tally.Read += write.Count;

            foreach (var error in errors)
            {
                if (error.Number == LockTimeoutError)
                {
                    tally.Locked++;
                    continue;
                }

                tally.Errors++;
                if (s_permissionErrors.Contains(error.Number))
                {
                    tally.PermissionFailed.Add((error.ObjectId, error.StatsId));
                }

                var name = byId.TryGetValue((error.ObjectId, error.StatsId), out var stat)
                    ? stat.SchemaName + "." + stat.ObjectName + " (" + stat.StatsName + ")"
                    : "a statistic";
                tally.SampleError ??= string.Create(CultureInfo.InvariantCulture, $"{name}: error {error.Number}: {McpHelpers.Truncate(error.Message, MaxStoredMessageChars)}");
            }
        }

        /* Azure SQL Database and Managed Instance (EngineEdition 5 and 8) report major version 12 but have the column. The 213 retry stays as the net. */
        private bool ShapeFromBuild(Version version) =>
            _server.Target.IsAzureSqlDb
            || _server.Target.IsAzureManagedInstance
            || ColumnStatsQueries.HeaderHasPersistedSamplePercent(version.Major, Math.Max(version.Minor, 0), Math.Max(version.Build, 0));

        private ColumnStatsCollector.RunRow Row(
            string database, SchemaHistoryKey key, string kind, int checkedCount, Tally tally, SkipCounts skipped, string? refused, bool complete, string? note)
        {
            _locked += tally.Locked;
            _errors += tally.Errors;
            _sampleError ??= tally.SampleError;
            return new ColumnStatsCollector.RunRow(
                database, DarlingSchemaCapture.KeyText(key), kind, checkedCount, tally.Read, tally.Deleted,
                skipped.Filtered, skipped.MemoryOptimized, skipped.XmlSpatialColumnstore, skipped.NeverUpdated, tally.Locked, skipped.OverCap,
                tally.Errors, tally.SampleError, refused, complete, note);
        }

        public async Task<CollectorRunResult> FinishAsync()
        {
            var written = 0;
            if (_rows.Count > 0)
            {
                written = await TimedStore(() => _runner.WriteColumnStatsRunRowsAsync(Store, _rows, _server, _runTime, _ct));
            }

            /* Rows is every database the run accounted for: a repeated refusal's row is the stored one, not a new one. */
            return new CollectorRunResult(written + _repeatedRefusals, _sql.ElapsedMilliseconds, _storage.ElapsedMilliseconds, CollectorContext.NoMeasurements, BuildNote());
        }

        private string? BuildNote()
        {
            string? note = null;
            void Add(string? text) => note = EnumeratedCollectorDriver.MergeNotes(note, text);

            var ownList = new HashSet<string>(_ownScope.Select(s => s.Trim()), StringComparer.OrdinalIgnoreCase);
            var missingInOwnList = _noSelect.Where(ownList.Contains).ToList();
            var noGrantAnywhere = _noSelect.Count > 0 && _granted == 0;
            if (noGrantAnywhere)
            {
                Add($"precondition: no database in scope grants the monitoring login SELECT, so no statistics were read ({NoDatabaseAccessNote.NamedList(_noSelect)}). {GrantSentence}");
            }
            else if (missingInOwnList.Count > 0)
            {
                Add($"precondition: column_stats names {NoDatabaseAccessNote.NamedList(missingInOwnList)}, where the monitoring login has no SELECT. {GrantSentence}");
            }

            var notInOwnList = _noSelect.Except(missingInOwnList).ToList();
            if (!noGrantAnywhere && notInOwnList.Count > 0)
            {
                Add($"{CountText.Plural(notInOwnList.Count, "database")} without the SELECT grant, not read: {NoDatabaseAccessNote.NamedList(notInOwnList)}");
            }

            if (_tf9485.Count > 0)
            {
                Add($"precondition: DBCC SHOW_STATISTICS was refused in {NoDatabaseAccessNote.NamedList(_tf9485)} despite the SELECT grant");
            }

            if (_traceFlag9485 && (_tf9485.Count > 0 || noGrantAnywhere || missingInOwnList.Count > 0))
            {
                Add(ColumnSelectivity.TraceFlagSentence);
            }

            foreach (var (reason, names) in _notWritable)
            {
                Add(string.Create(CultureInfo.InvariantCulture, $"{CountText.Plural(names.Count, "database")} not read ({reason.TrimEnd('.').ToLowerInvariant()}): {NoDatabaseAccessNote.NamedList(names)}"));
            }

            if (_locked > 0)
            {
                Add(string.Create(CultureInfo.InvariantCulture, $"{CountText.Plural(_locked, "statistic")} locked (lock timeout), read on the next run"));
            }

            if (_errors > 0)
            {
                Add(string.Create(CultureInfo.InvariantCulture, $"{CountText.Plural(_errors, "statistic")} could not be read and are tried again on the next run, for example {_sampleError}"));
            }

            if (_failed.Count > 0)
            {
                Add($"{CountText.Plural(_failed.Count, "database")} failed, read on the next run: {NoDatabaseAccessNote.NamedList(_failed)}");
            }

            if (_unfinished.Count > 0)
            {
                Add($"{CountText.Plural(_unfinished.Count, "database")} not finished within the budget, continued on the next run: {NoDatabaseAccessNote.NamedList(_unfinished)}");
            }

            if (_runBudgetStopped)
            {
                Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"run budget ({_runBudget.BudgetSeconds} s) spent after {_started} of {CountText.Plural(_toRead, "database")}; the rest are read on the next run"));
            }

            return note;
        }

        private async Task<T> TimedSql<T>(Func<Task<T>> work)
        {
            _sql.Start();
            try
            {
                return await work();
            }
            finally
            {
                _sql.Stop();
            }
        }

        private async Task<T> TimedStore<T>(Func<Task<T>> work)
        {
            _storage.Start();
            try
            {
                return await work();
            }
            finally
            {
                _storage.Stop();
            }
        }

        private async Task TimedStore(Func<Task> work)
        {
            _storage.Start();
            try
            {
                await work();
            }
            finally
            {
                _storage.Stop();
            }
        }
    }

    /// <summary>What one database's read counted.</summary>
    private sealed class Tally
    {
        public int Read { get; set; }
        public int Deleted { get; set; }
        public int Locked { get; set; }
        public int Errors { get; set; }
        public List<(int ObjectId, int StatsId)> PermissionFailed { get; } = [];
        public string? SampleError { get; set; }
    }
}
