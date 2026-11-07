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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Schema tracking (#7643), the <c>schema_objects</c> pass of one server. Ensures the change-tracking session, reads the stored cursor,
/// the databases and the session's events, lets <see cref="SchemaWorkPlanner"/> decide what each database needs, reads the catalog of
/// the ones that need it, writes the versions, and logs one pass row per database read.
///
/// <para><b>Budgets.</b> A database's read gets 120 seconds and the pass five minutes, checked before a database starts, so one slow
/// database cannot hold the sweep. A database whose work did not finish (an error, either budget) is not stamped as seen in scope and its
/// state row is not advanced, so the planner reconciles it on the next pass. The server's event position still advances, because the
/// events it passed are not read again.</para>
///
/// <para><b>Azure SQL Database</b> has one session per database, so each database is opened, ensured, probed, planned and read on its
/// own connection. The server row (empty database name) holds the pass stamp and each database row holds that database's own session
/// position; the planner is called once per database with a server state made from the two.</para>
/// </summary>
public static class DarlingSchemaCapture
{
    private const string NoInstallIdNote = "The schema pass did nothing: this install has no id to name its change-tracking session with.";

    /* Every XE event is after this, so a server with no stored position reads the whole ring buffer. */
    private static readonly DateTime s_readEverything = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static readonly TimeSpan s_passBudget = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan s_databaseBudget = TimeSpan.FromSeconds(120);

    public static async Task<CollectorRunResult> RunAsync(DarlingCollectorRunner runner, ServerRuntime server, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(runner);
        ArgumentNullException.ThrowIfNull(server);

        runner.UnsettleSchemaSession(server.ServerId);
        var sessionName = SchemaChangesXeSession.TryNameFor(runner.InstallId);
        if (sessionName is null)
        {
            return new CollectorRunResult(0, 0, 0, CollectorContext.NoMeasurements, NoInstallIdNote);
        }

        var pass = new Pass(runner, server, sessionName, DarlingSnapshotScope.IsActive, cancellationToken);
        await using var store = await runner.Store.OpenConnectionAsync(cancellationToken);
        pass.Store = store;
        if (server.Target.IsAzureSqlDb)
        {
            await pass.RunAzureAsync();
        }
        else
        {
            await pass.RunServerAsync();
        }

        return await pass.FinishAsync();
    }

    /// <summary>
    /// Drops this install's session on a server whose <c>schema_objects</c> collector is off, once (#7643). Only a server the store shows
    /// ran the collector is touched, so a server that never did costs one store read and then nothing until its next pass. After a drop
    /// the store forgets the session (<see cref="SchemaCaptureStateStore.ClearSessionAsync"/>), so it is not dropped again. A failure
    /// leaves the state as it was and the next sweep tries again; it never throws.
    /// </summary>
    internal static async Task DropWhenDisabledAsync(
        DarlingCollectorRunner runner,
        ServerRuntime server,
        IReadOnlyList<MonitoredServer> others,
        Func<int, bool> schemaOn,
        Func<CancellationToken, Task<LongQueryTraceInstanceGuard>> instanceGuard,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        if (runner.SchemaSessionSettled(server.ServerId)
            || SchemaChangesXeSession.TryNameFor(runner.InstallId) is null
            || !CollectorTargetInfo.XeSessionSupported(server.Target))
        {
            return;
        }

        try
        {
            await using var store = await runner.Store.OpenConnectionAsync(cancellationToken);
            if (!await SchemaCaptureStateStore.HasSessionAsync(store, server.ServerId, cancellationToken))
            {
                runner.SettleSchemaSession(server.ServerId);
                return;
            }

            using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            budget.CancelAfter(DarlingRemovedServerSessions.Timeout);
            var removed = new RemovedLongQueryServer(server.Config, server, null);
            var dropped = await DarlingRemovedServerSessions.DropSchemaSessionAsync(
                removed, runner, others, id => id == server.ServerId || schemaOn(id), instanceGuard, logger, budget.Token);
            if (dropped)
            {
                await SchemaCaptureStateStore.ClearSessionAsync(store, server.ServerId, DateTime.UtcNow, cancellationToken);
                runner.SettleSchemaSession(server.ServerId);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("[{Server}] Could not drop the schema-changes XE session of the disabled collector; the next sweep tries again: {Message}",
                server.Config.DisplayName, ex.Message);
        }
    }

    /// <summary>The databases the pass covers: above the system range, not a vendor or tool database, in the collector's scope, not excluded.</summary>
    internal static List<string> InScope(
        IReadOnlyList<SchemaDatabaseProbe> probes, IReadOnlyList<string> scope, IReadOnlyList<string>? excluded)
    {
        var skipped = new HashSet<string>(IdentityCapacityCollector.SkippedDatabaseNames, StringComparer.OrdinalIgnoreCase);
        var allowed = scope.Count == 0 ? null : new HashSet<string>(scope.Select(s => s.Trim()), StringComparer.OrdinalIgnoreCase);
        var excludedNames = new HashSet<string>((excluded ?? []).Select(s => s.Trim()), StringComparer.OrdinalIgnoreCase);
        return probes
            .Where(p => p.DatabaseId > 4 && !skipped.Contains(p.Name) && (allowed is null || allowed.Contains(p.Name)) && !excludedNames.Contains(p.Name))
            .Select(p => p.Name)
            .ToList();
    }

    private static string KeyText(SchemaHistoryKey key) =>
        key.AgGroupId is { } group
            ? "ag:" + group.ToString("D", CultureInfo.InvariantCulture)
            : "server:" + (key.ServerId ?? 0).ToString(CultureInfo.InvariantCulture);

    private sealed class Pass
    {
        private readonly DarlingCollectorRunner _runner;
        private readonly ServerRuntime _server;
        private readonly string _sessionName;
        private readonly bool _runNow;
        private readonly CancellationToken _ct;
        private readonly DateTime _passTime = DateTime.UtcNow;
        private readonly PassBudget _passBudget;
        private readonly TimeSpan _databaseBudget;
        private readonly bool _azure;
        private readonly List<SchemaObjectsCollector.PassRow> _rows = [];
        private readonly Stopwatch _sql = new();
        private readonly Stopwatch _storage = new();

        /* What the run says on its row, collected by kind. */
        private readonly List<string> _notes = [];
        private readonly Dictionary<string, List<string>> _skipped = new(StringComparer.Ordinal);
        private readonly List<string> _noAccess = [];
        private readonly List<string> _overCap = [];
        private readonly List<string> _noViewDefinition = [];
        private readonly List<string> _failed = [];
        private string? _sessionFault;
        private bool _sessionFaultIsPermission;
        private readonly List<string> _faultDatabases = [];

        /* The databases stamped as seen in scope when the pass ends, and the ones whose work did not finish. */
        private readonly List<string> _stamp = [];
        private int _started;
        private int _planned;
        private SchemaCaptureStateRow? _serverRow;
        private Dictionary<string, SchemaCaptureStateRow> _dbStates = new();
        private SchemaCaptureStateRow? _newServerRow;
        private bool _budgetStopped;

        public Pass(DarlingCollectorRunner runner, ServerRuntime server, string sessionName, bool runNow, CancellationToken ct)
        {
            _runner = runner;
            _server = server;
            _sessionName = sessionName;
            _runNow = runNow;
            _ct = ct;
            _azure = server.Target.IsAzureSqlDb;
            var budgets = runner.SchemaBudgetsForTests;
            _passBudget = new PassBudget(budgets?.Pass ?? s_passBudget);
            _databaseBudget = budgets?.Database ?? s_databaseBudget;
        }

        public NpgsqlConnection Store { get; set; } = null!;

        public async Task RunServerAsync()
        {
            await using var connection = await TimedSql(() => _runner.SchemaSource.OpenAsync(_server, null, _ct));
            var session = await TimedSql(() => connection.EnsureSessionAsync(_sessionName, _ct));
            NoteSessionFault(session, null);

            await LoadStateAsync();
            var probes = await TimedSql(() => connection.ReadProbesAsync(_ct));
            var batch = session.Running ? await ReadEventsAsync(connection, _serverRow?.LastEventTime, null) : null;
            var inScope = InScope(probes, _runner.DatabaseScopeFor(SchemaObjectsCollector.Instance.Name, _server.ServerId), _server.Config.ExcludedDatabases);
            var plans = SchemaWorkPlanner.Plan(_server.ServerId, _serverRow, _dbStates, session, batch, probes, inScope, _runNow);
            _planned = plans.Count(p => p.Work != SchemaDatabaseWork.Skip);

            foreach (var plan in plans)
            {
                _ct.ThrowIfCancellationRequested();
                var probe = probes.First(p => string.Equals(p.Name, plan.DatabaseName, StringComparison.OrdinalIgnoreCase));
                if (plan.Work == SchemaDatabaseWork.Skip)
                {
                    NoteSkip(plan, probe);
                    continue;
                }

                if (_started > 0 && _passBudget.Spent)
                {
                    _budgetStopped = true;
                    continue;
                }

                _started++;
                using var budget = StartDatabaseBudget();
                await ExecutePlanAsync(plan, probe, connection, null, budget?.Token ?? _ct);
            }

            _newServerRow = AdvanceServerRow(session, batch);
        }

        public async Task RunAzureAsync()
        {
            await LoadStateAsync();
            var listed = _runner.AzureDatabaseListOverrideForTests is { } listOverride
                ? await TimedSql(() => listOverride(_server, _ct))
                : await TimedSql(() => _runner.GetAzureDatabaseListAsync(_server, _runner.DatabaseScopeFor(SchemaObjectsCollector.Instance.Name, _server.ServerId), _ct));
            var skipped = new HashSet<string>(IdentityCapacityCollector.SkippedDatabaseNames, StringComparer.OrdinalIgnoreCase);
            var databases = AzureSweepScope.WithoutMaster(listed).Where(d => !skipped.Contains(d)).Order(StringComparer.Ordinal).ToList();

            foreach (var database in databases)
            {
                _ct.ThrowIfCancellationRequested();
                if (_started > 0 && _passBudget.Spent)
                {
                    _budgetStopped = true;
                    continue;
                }

                _started++;
                using var budget = StartDatabaseBudget();
                var token = budget?.Token ?? _ct;
                try
                {
                    await RunAzureDatabaseAsync(database, token);
                }
                catch (OperationCanceledException) when (!_ct.IsCancellationRequested)
                {
                    _failed.Add(database + " (not finished within " + ((int)_databaseBudget.TotalSeconds).ToString(CultureInfo.InvariantCulture) + " s)");
                }
                catch (Exception ex) when (ex is not OperationCanceledException && ex is not NpgsqlException)
                {
                    _failed.Add(database + " (" + ex.Message + ")");
                    _runner.Logger?.LogWarning("[{Server}] [{Database}] The schema pass failed in the database: {Message}", _server.Config.DisplayName, database, ex.Message);
                }
            }

            _newServerRow = (_serverRow ?? new SchemaCaptureStateRow { ServerId = _server.ServerId }) with { LastSeenInScopeAt = _passTime };
        }

        private async Task RunAzureDatabaseAsync(string database, CancellationToken token)
        {
            await using var connection = await TimedSql(() => _runner.SchemaSource.OpenAsync(_server, database, token));
            var session = await TimedSql(() => connection.EnsureSessionAsync(_sessionName, token));
            NoteSessionFault(session, database);

            var probe = (await TimedSql(() => connection.ReadProbesAsync(token)))
                .FirstOrDefault(p => string.Equals(p.Name, database, StringComparison.OrdinalIgnoreCase));
            if (probe is null || probe.DatabaseId <= 4)
            {
                return;
            }

            _dbStates.TryGetValue(database, out var dbRow);
            var serverState = new SchemaCaptureStateRow
            {
                ServerId = _server.ServerId,
                SessionCreateTime = dbRow?.SessionCreateTime,
                LastEventSequence = dbRow?.LastEventSequence,
                LastEventTime = dbRow?.LastEventTime,
                TotalEventsProcessed = dbRow?.TotalEventsProcessed,
                LastSeenInScopeAt = _serverRow?.LastSeenInScopeAt,
            };
            var batch = session.Running ? await ReadEventsAsync(connection, dbRow?.LastEventTime, database, token) : null;
            var dbStates = dbRow is null ? new Dictionary<string, SchemaCaptureStateRow>() : new Dictionary<string, SchemaCaptureStateRow> { [database] = dbRow };
            var plan = SchemaWorkPlanner.Plan(_server.ServerId, serverState, dbStates, session, batch, [probe], [database], _runNow)[0];
            var advanced = AdvanceSession(serverState, session, batch);
            Func<SchemaCaptureStateRow, SchemaCaptureStateRow> withSession = row => row with
            {
                SessionCreateTime = advanced.SessionCreateTime,
                LastEventSequence = advanced.LastEventSequence,
                LastEventTime = advanced.LastEventTime,
                TotalEventsProcessed = advanced.TotalEventsProcessed,
            };

            if (plan.Work == SchemaDatabaseWork.Skip)
            {
                NoteSkip(plan, probe);
                if (dbRow is not null && !IsNotWritable(plan))
                {
                    await TimedStore(() => SchemaCaptureStateStore.SaveAsync(Store, withSession(dbRow) with { LastSeenInScopeAt = _passTime }, _ct));
                }

                return;
            }

            await ExecutePlanAsync(plan, probe, connection, withSession, token);
        }

        private async Task LoadStateAsync()
        {
            var rows = await TimedStore(() => SchemaCaptureStateStore.LoadAsync(Store, _server.ServerId, _ct));
            _serverRow = rows.FirstOrDefault(r => r.DatabaseName.Length == 0);
            _dbStates = rows.Where(r => r.DatabaseName.Length > 0).ToDictionary(r => r.DatabaseName, StringComparer.OrdinalIgnoreCase);
        }

        private async Task<SchemaEventBatch?> ReadEventsAsync(ISchemaCaptureConnection connection, DateTime? lastEventTime, string? database, CancellationToken? token = null)
        {
            var cutoff = lastEventTime is { } last ? last - TimeSpan.FromSeconds(1) : s_readEverything;
            try
            {
                return await TimedSql(() => connection.ReadEventsAsync(_sessionName, cutoff, token ?? _ct));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _notes.Add("The change-tracking session's events could not be read" + (database is null ? string.Empty : " in " + database) + ", so what it recorded is read on the next pass: " + ex.Message);
                return null;
            }
        }

        private void NoteSessionFault(SchemaSessionState session, string? database)
        {
            if (session.Running)
            {
                return;
            }

            _sessionFault ??= session.FaultMessage ?? "the schema-changes session is not running";
            _sessionFaultIsPermission |= session.FaultIsPermission;
            if (database is not null)
            {
                _faultDatabases.Add(database);
            }
        }

        private static bool IsNotWritable(SchemaDatabasePlan plan) =>
            plan.Work == SchemaDatabaseWork.Skip
            && plan.Reason is not null
            && !plan.Reason.StartsWith(SchemaWorkPlanner.NoChangeTrackingPrefix, StringComparison.Ordinal);

        private void NoteSkip(SchemaDatabasePlan plan, SchemaDatabaseProbe probe)
        {
            if (!IsNotWritable(plan))
            {
                /* A skip for no events, or while the session is down (the fault note says so): the database was seen and is stamped. */
                _stamp.Add(plan.DatabaseName);
                return;
            }

            if (!probe.HasAccess)
            {
                _noAccess.Add(plan.DatabaseName);
                return;
            }

            if (!_skipped.TryGetValue(plan.Reason!, out var names))
            {
                _skipped[plan.Reason!] = names = [];
            }

            names.Add(plan.DatabaseName);
        }

        /// <summary>Reads and writes one planned database. True when it finished; false leaves it unstamped and its state row as it was.</summary>
        private async Task<bool> ExecutePlanAsync(
            SchemaDatabasePlan plan, SchemaDatabaseProbe probe, ISchemaCaptureConnection connection,
            Func<SchemaCaptureStateRow, SchemaCaptureStateRow>? withSession, CancellationToken token)
        {
            var kind = plan.Work switch
            {
                SchemaDatabaseWork.Baseline => "baseline",
                SchemaDatabaseWork.Targeted => "targeted",
                _ => "reconcile",
            };

            /* A targeted read names at most MaxTargetedIds objects; more is read in full. A full read passes no ids, which is what lets the
               writer record drops among every object the database held. */
            IReadOnlySet<int>? ids = plan.Work == SchemaDatabaseWork.Targeted && plan.ObjectIds.Count <= SchemaCatalogQueries.MaxTargetedIds
                ? plan.ObjectIds
                : null;

            SchemaCatalogSnapshot snapshot;
            try
            {
                snapshot = await TimedSql(() => connection.ReadCatalogAsync(plan.DatabaseName, ids, token));
            }
            catch (OperationCanceledException) when (!_ct.IsCancellationRequested)
            {
                _runner.Logger?.LogWarning(
                    "[{Server}] [{Database}] The schema read did not finish within {Seconds} seconds; the database is reconciled on the next pass",
                    _server.Config.DisplayName, plan.DatabaseName, (int)_databaseBudget.TotalSeconds);
                Fail(plan, kind, "not finished within " + ((int)_databaseBudget.TotalSeconds).ToString(CultureInfo.InvariantCulture) + " s");
                return false;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _runner.Logger?.LogWarning(
                    "[{Server}] [{Database}] The schema read failed; the database is reconciled on the next pass: {Message}",
                    _server.Config.DisplayName, plan.DatabaseName, ex.Message);
                Fail(plan, kind, ex.Message);
                return false;
            }

            var complete = snapshot.HasViewDefinition && !snapshot.OverCap;
            var existing = _dbStates.GetValueOrDefault(plan.DatabaseName);
            var fullRead = ids is null;
            var state = (existing ?? new SchemaCaptureStateRow { ServerId = _server.ServerId, DatabaseName = plan.DatabaseName }) with
            {
                RecoveryForkGuid = probe.RecoveryForkGuid ?? existing?.RecoveryForkGuid,
                HistoryServerId = plan.Key.ServerId,
                HistoryAgGroupId = plan.Key.AgGroupId,
                WasPrimary = probe.IsPrimaryReplica,
                BaselineAt = existing?.BaselineAt ?? (plan.Work == SchemaDatabaseWork.Baseline ? _passTime : null),
                LastCompleteReadAt = complete && fullRead ? _passTime : existing?.LastCompleteReadAt,
                LastSeenInScopeAt = _passTime,
                LastReconcileAt = plan.Work == SchemaDatabaseWork.Reconcile ? _passTime : existing?.LastReconcileAt,
                LastReconcileReason = plan.Work == SchemaDatabaseWork.Reconcile ? plan.Reason : existing?.LastReconcileReason,
            };
            if (withSession is not null)
            {
                state = withSession(state);
            }

            var detectedBy = plan.Work switch
            {
                SchemaDatabaseWork.Baseline => "baseline",
                SchemaDatabaseWork.Targeted => "xe_event",
                _ => string.Equals(plan.Reason, "run_now", StringComparison.Ordinal) ? "run_now" : "reconcile",
            };
            var write = new SchemaDatabaseWrite(
                plan.Key, _server.ServerId, plan.DatabaseName, _passTime, detectedBy, plan.Work == SchemaDatabaseWork.Baseline, complete,
                snapshot.Objects, plan.Attribution, state)
            {
                TargetedIds = ids,
            };
            var counts = await TimedStore(() => SchemaVersionStore.ApplyAsync(Store, write, _ct));

            string? note = null;
            if (snapshot.OverCap)
            {
                _overCap.Add(plan.DatabaseName);
                note = string.Create(CultureInfo.InvariantCulture, $"More than {SchemaCatalogQueries.MaxObjectsPerRead} tables and views: only the first were read, so no drops are recorded.");
            }
            else if (!snapshot.HasViewDefinition)
            {
                _noViewDefinition.Add(plan.DatabaseName);
                note = "The login lacks VIEW DEFINITION in this database: what it cannot see is missing from the read, so no drops are recorded.";
            }

            _stamp.Add(plan.DatabaseName);
            _rows.Add(new SchemaObjectsCollector.PassRow(
                plan.DatabaseName, KeyText(plan.Key), kind, plan.Reason, plan.Attribution.Count, snapshot.Objects.Count,
                counts.VersionsWritten, counts.Dropped, complete, note));
            return true;
        }

        private void Fail(SchemaDatabasePlan plan, string kind, string message)
        {
            _failed.Add(plan.DatabaseName);
            _rows.Add(new SchemaObjectsCollector.PassRow(
                plan.DatabaseName, KeyText(plan.Key), kind, plan.Reason, plan.Attribution.Count, 0, 0, 0, false, "The read did not finish: " + message));
        }

        private SchemaCaptureStateRow AdvanceServerRow(SchemaSessionState session, SchemaEventBatch? batch)
        {
            var row = _serverRow ?? new SchemaCaptureStateRow { ServerId = _server.ServerId };
            return AdvanceSession(row, session, batch) with { LastSeenInScopeAt = _passTime };
        }

        /* The session position after this pass: a session that did not run, or events that were not read, leave it where it was. A new
           session's sequence starts again, so the old one is not carried over. */
        private static SchemaCaptureStateRow AdvanceSession(SchemaCaptureStateRow row, SchemaSessionState session, SchemaEventBatch? batch)
        {
            if (!session.Running || batch is null)
            {
                return row;
            }

            var same = SchemaWorkPlanner.IsSameSession(row, session);
            return row with
            {
                SessionCreateTime = session.CreateTimeUtc,
                LastEventSequence = batch.Events.Count > 0 ? batch.Events.Max(e => e.Sequence) : same ? row.LastEventSequence : null,
                LastEventTime = batch.Events.Count > 0 ? batch.Events.Max(e => e.TimeUtc) : same ? row.LastEventTime : null,
                TotalEventsProcessed = batch.TotalEventsProcessed,
            };
        }

        public async Task<CollectorRunResult> FinishAsync()
        {
            var written = 0;
            if (_newServerRow is not null)
            {
                await TimedStore(() => SchemaCaptureStateStore.SaveAsync(Store, _newServerRow, _ct));
                if (!_azure)
                {
                    await TimedStore(() => SchemaCaptureStateStore.StampSeenInScopeAsync(Store, _server.ServerId, _stamp, _passTime, _ct));
                }
            }

            if (_rows.Count > 0)
            {
                written = await TimedStore(() => _runner.WriteSchemaPassRowsAsync(Store, _rows, _server, _passTime, _ct));
            }

            return new CollectorRunResult(written, _sql.ElapsedMilliseconds, _storage.ElapsedMilliseconds, CollectorContext.NoMeasurements, BuildNote());
        }

        private string? BuildNote()
        {
            string? note = null;
            void Add(string? text) => note = EnumeratedCollectorDriver.MergeNotes(note, text);

            if (_sessionFault is not null)
            {
                var where = _azure && _faultDatabases.Count > 0 ? " in " + NoDatabaseAccessNote.NamedList(_faultDatabases) : string.Empty;
                var grant = _azure ? "ALTER ANY DATABASE EVENT SESSION" : "ALTER ANY EVENT SESSION";
                Add($"Change tracking is not running{where}: {_sessionFault} Grant {grant}" + (_sessionFaultIsPermission ? string.Empty : " or fix the fault")
                    + ". Until then each database is read in full only for its first baseline, and changes appear only at run_collectors_now.");
            }

            foreach (var (reason, names) in _skipped)
            {
                Add(string.Create(CultureInfo.InvariantCulture, $"{names.Count} database(s) not read ({reason.TrimEnd('.').ToLowerInvariant()}): {NoDatabaseAccessNote.NamedList(names)}"));
            }

            Add(NoDatabaseAccessNote.Sentence(_noAccess, _server.Target.IsAwsRds));
            if (_overCap.Count > 0)
            {
                Add(string.Create(CultureInfo.InvariantCulture, $"more than {SchemaCatalogQueries.MaxObjectsPerRead} tables and views in {NoDatabaseAccessNote.NamedList(_overCap)}: only the first were read and no drops are recorded there"));
            }

            if (_noViewDefinition.Count > 0)
            {
                Add($"the login lacks VIEW DEFINITION in {NoDatabaseAccessNote.NamedList(_noViewDefinition)}: no drops are recorded there");
            }

            if (_failed.Count > 0)
            {
                Add($"{_failed.Count} database(s) not finished, read in full next pass: {NoDatabaseAccessNote.NamedList(_failed)}");
            }

            if (_budgetStopped)
            {
                Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"pass budget ({_passBudget.BudgetSeconds} s) spent after {_started} of {_planned} database(s); the rest were not read and are read in full on the next pass"));
            }

            foreach (var text in _notes)
            {
                Add(text);
            }

            return note;
        }

        private CancellationTokenSource? StartDatabaseBudget() => EnumeratedCollectorDriver.StartItemBudget(_databaseBudget, _ct);

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
}
