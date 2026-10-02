/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/* #4961: the long-query session that versions before this one shared between installs. Each upgraded install drops it once per
   registration and database, then records the drop in collector_state, so it never touches that session again. The step lives
   here, apart from the reconcile, because the reconcile's own file must never name the legacy session (its source pins say so). */
public partial class RemoteCollectorService
{
    /// <summary>
    /// Replaces the look for a legacy long-query session that an older install created again: called with the server and the
    /// database (empty on server scope), true when the session exists there. Null in production, where the ensure's own batch
    /// looks.
    /// </summary>
    internal Func<ServerConnection, string, bool>? LegacyLongQuerySessionExistsForTests { get; set; }

    /* The keys known to be recorded (server id, state key), so a later cycle does not read the store again for them. */
    private readonly ConcurrentDictionary<(int ServerId, string StateKey), bool> _legacyLongQuerySessionRecorded = new();

    /* The servers whose every key is recorded this start: the legacy step has nothing left to look at. */
    private readonly ConcurrentDictionary<string, bool> _legacyLongQuerySessionDone = new();

    /* The servers that already logged the line about a legacy session an older install created again: once per start. */
    private readonly ConcurrentDictionary<string, bool> _legacyLongQuerySessionReported = new();

    /* The second look in each ensure's batch: does the legacy session exist? Its name is a parameter that is NULL when the
       look is not due, so the one statement text serves every cycle. */
    private const string LegacyLongQuerySessionServerProbeSql = @"

SELECT /* PerformanceMonitorLite */
    legacy_exists = 1
FROM sys.server_event_sessions AS ses
WHERE ses.name = @legacy_session_name;";

    private const string LegacyLongQuerySessionDatabaseProbeSql = @"

SELECT /* PerformanceMonitorLite */
    legacy_exists = 1
FROM sys.database_event_sessions AS des
WHERE des.name = @legacy_session_name;";

    /// <summary>
    /// One long-query reconcile's share of the legacy step: whether the step runs this pass, the one listing of every database
    /// that the step and the drop of this install's own session share, and the failure the step keeps until the rest of the
    /// reconcile has run. The pass throws that failure at its end, so each reconcile costs the retry cap one failed pass at most.
    /// </summary>
    private sealed class LongQueryReconcilePass
    {
        private readonly Func<Task<List<string>>> _listEvery;
        private Task<List<string>>? _listed;

        public LongQueryReconcilePass(bool legacyDue, Func<Task<List<string>>> listEvery)
        {
            LegacyDue = legacyDue;
            _listEvery = listEvery;
        }

        public bool LegacyDue { get; }

        public LongQueryTraceDropException? LegacyFailure { get; set; }

        /// <summary>Every database, listed once for the whole pass. A failed listing fails every caller with the same exception.</summary>
        public Task<List<string>> ListedAsync() => _listed ??= _listEvery();

        public void ThrowLegacyFailure()
        {
            if (LegacyFailure is { } failure)
            {
                throw failure;
            }
        }
    }

    /// <summary>True until this server's legacy drop is on record, for this start.</summary>
    private bool LegacyLongQuerySessionPending(ServerConnection server) =>
        !_legacyLongQuerySessionDone.ContainsKey(server.Id);

    /// <summary>
    /// Whether this pass runs the legacy step: it is still pending, and either the retry has not given up or the pass is due for
    /// another reason (the hourly attempt after the cap, a start, or a change to the trace's settings).
    /// </summary>
    private bool LegacyLongQuerySessionDue(ServerConnection server, LongQueryTraceDropRetry retry, bool passDue) =>
        LegacyLongQuerySessionPending(server) && (retry.NextAttemptUtc is null || passDue);

    /// <summary>
    /// The two failures of one pass as one: the databases of both, so the warning that gives up names every place the sessions may
    /// remain. A drop on the server's own scope has no database to name.
    /// </summary>
    private static LongQueryTraceDropException MergeLongQueryTraceDropFailures(LongQueryTraceDropException first, LongQueryTraceDropException second)
    {
        if (first.OnServer || second.OnServer)
        {
            return first.OnServer ? first : second;
        }

        var databases = first.Databases.Concat(second.Databases).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        return new LongQueryTraceDropException(databases, (Exception?)second.InnerException ?? second, second.CreateNote ?? first.CreateNote);
    }

    /// <summary>
    /// Drops the legacy session where it is not yet recorded as dropped, and records each drop right after it, so a later pass
    /// retries only the databases that failed. On Azure SQL Database that is every listed database except master and the databases
    /// monitored as their own servers, as the trace-off path lists them (<see cref="LongQueryTraceDatabases.Plan"/>); an exclusion
    /// and another registration's session do not spare a database, because nothing owns the legacy session any more. A database
    /// monitored as its own server runs its own legacy drop, under its own record. Server scope is the one database named with an
    /// empty string. A failure is kept on the pass, not thrown, so this install's own session is still created. A record that
    /// cannot be read skips the step for this pass: it is never taken for "not dropped".
    /// </summary>
    private async Task RunLegacyLongQuerySessionAsync(
        ServerConnection server, LongQueryReconcilePass pass, bool isAzureSqlDatabase, bool afterTheCap, CancellationToken cancellationToken)
    {
        if (!pass.LegacyDue)
        {
            return;
        }

        var serverId = GetServerId(server);
        IReadOnlyList<string> databases;
        if (isAzureSqlDatabase)
        {
            try
            {
                /* The trace-off path's own plan, over the same one listing and the same separately monitored databases it reads
                   (SeparatelyMonitoredDatabasesFor), with no exclusion and no other registration's session applied: Darling's
                   legacy drop asks the plan the same way. */
                var listed = await pass.ListedAsync();
                databases = LongQueryTraceDatabases.Plan(
                    enabled: false, listed, Array.Empty<string>(), SeparatelyMonitoredDatabasesFor(server), keptElsewhere: Array.Empty<string>()).Drop;
            }
            catch (LongQueryTraceDropException ex)
            {
                pass.LegacyFailure = ex;
                return;
            }
        }
        else
        {
            databases = new[] { string.Empty };
        }

        var unknown = databases
            .Where(database => !_legacyLongQuerySessionRecorded.ContainsKey((serverId, LegacyLongQuerySession.StateKey(database))))
            .ToList();
        if (unknown.Count > 0)
        {
            var stored = await ReadLegacyLongQuerySessionRecordsAsync(server, serverId, cancellationToken);
            if (stored is null)
            {
                return;
            }

            foreach (var key in stored)
            {
                _legacyLongQuerySessionRecorded[(serverId, key)] = true;
            }

            unknown = unknown.Where(database => !stored.Contains(LegacyLongQuerySession.StateKey(database))).ToList();
        }

        if (unknown.Count == 0)
        {
            _legacyLongQuerySessionDone[server.Id] = true;
            return;
        }

        try
        {
            if (isAzureSqlDatabase)
            {
                await LongQueryTraceDatabases.DropEachAsync(
                    unknown,
                    (database, token) => DropLegacyLongQuerySessionAsync(server, database, isAzureSqlDatabase: true, token),
                    (database, ex) =>
                    {
                        var line = $"[{server.DisplayName}] [{database}] Could not drop the long-query session that earlier versions shared between installs: {ex.Message}";
                        if (afterTheCap)
                        {
                            AppLogger.Debug("XeSession", line);
                        }
                        else
                        {
                            AppLogger.Warn("XeSession", line);
                        }
                    },
                    createNote: null,
                    cancellationToken);
            }
            else
            {
                try
                {
                    await DropLegacyLongQuerySessionAsync(server, string.Empty, isAzureSqlDatabase: false, cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    throw LongQueryTraceDropException.ForServer(ex);
                }
            }

            _legacyLongQuerySessionDone[server.Id] = true;
        }
        catch (LongQueryTraceDropException ex)
        {
            pass.LegacyFailure = ex;
        }
    }

    /// <summary>The guarded drop of the legacy session in one database (server scope when the name is empty), then its record.</summary>
    private async Task DropLegacyLongQuerySessionAsync(ServerConnection server, string database, bool isAzureSqlDatabase, CancellationToken cancellationToken)
    {
        var name = LongQueryCompletionsCollector.LegacyXeSessionName;
        if (LongQueryTraceDatabaseOverrideForTests is { } dropOverride)
        {
            await dropOverride(server, database, false, name, cancellationToken);
        }
        else
        {
            using var connection = isAzureSqlDatabase
                ? await OpenAzureDatabaseConnectionAsync(server, database, cancellationToken)
                : await CreateConnectionAsync(server, cancellationToken);
            using var command = new SqlCommand(LongQueryCompletionsCollector.BuildDropSessionSql(name, databaseScoped: isAzureSqlDatabase), connection);
            command.CommandTimeout = CommandTimeoutSeconds;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        /* Drop, then record: a crash between the two costs one more guarded drop at the next start, and recording first could
           lose the drop for good. A record that cannot be written fails the step like a drop does, so it takes the same cap. */
        await RecordLegacyLongQuerySessionDropAsync(server, database, cancellationToken);
    }

    private async Task RecordLegacyLongQuerySessionDropAsync(ServerConnection server, string database, CancellationToken cancellationToken)
    {
        var serverId = GetServerId(server);
        var stateKey = LegacyLongQuerySession.StateKey(database);
        var utcNow = LongQueryTraceUtcNowForTests?.Invoke() ?? DateTime.UtcNow;

        using (var writeLock = _duckDb.AcquireWriteLock())
        {
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText = @"
INSERT OR REPLACE INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)";
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = LegacyLongQuerySession.StateCollector });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = stateKey });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = utcNow.ToString("O", CultureInfo.InvariantCulture) });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = utcNow });
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        _legacyLongQuerySessionRecorded[(serverId, stateKey)] = true;
    }

    /// <summary>
    /// The drop records this registration holds, or null when the store could not be read. Null is not "none": the step skips the
    /// pass and reads again on the next one, so a failed read never becomes a second drop.
    /// </summary>
    private async Task<HashSet<string>?> ReadLegacyLongQuerySessionRecordsAsync(ServerConnection server, int serverId, CancellationToken cancellationToken)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText = @"
SELECT state_key
FROM collector_state
WHERE server_id = $1
AND   collector_name = $2
AND   starts_with(state_key, $3)";
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = LegacyLongQuerySession.StateCollector });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = LegacyLongQuerySession.StateKeyPrefix });

            var keys = new HashSet<string>(StringComparer.Ordinal);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                keys.Add(reader.GetString(0));
            }

            return keys;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            AppLogger.Debug("XeSession", $"[{server.DisplayName}] The record of the legacy long-query session's drop could not be read, so this cycle leaves the legacy session alone and the next one reads again: {ex.Message}");
            return null;
        }
    }

    /* The look for an older install's copy is due once the drop is on record and the line has not been logged this start. */
    private bool ShouldLookForLegacyLongQuerySession(ServerConnection server, string database) =>
        _legacyLongQuerySessionRecorded.ContainsKey((GetServerId(server), LegacyLongQuerySession.StateKey(database)))
        && !_legacyLongQuerySessionReported.ContainsKey(server.Id);

    /// <summary>The parameter of the probe in each ensure's batch: the legacy name when the look is due, otherwise NULL, which matches nothing.</summary>
    private SqlParameter LegacyLongQuerySessionProbeParameter(ServerConnection server, string database) =>
        new("@legacy_session_name", SqlDbType.NVarChar, 128)
        {
            Value = ShouldLookForLegacyLongQuerySession(server, database) ? LongQueryCompletionsCollector.LegacyXeSessionName : DBNull.Value,
        };

    /// <summary>
    /// An ensure's batch found the legacy session although this install dropped it: an older Lite or Darling created it again. It
    /// is left alone, and one Information line per start says so. Nothing is logged until the drop is on record.
    /// </summary>
    private void ReportLegacyLongQuerySessionFound(ServerConnection server, string database)
    {
        if (!ShouldLookForLegacyLongQuerySession(server, database) || !_legacyLongQuerySessionReported.TryAdd(server.Id, true))
        {
            return;
        }

        var where = database.Length == 0 ? string.Empty : $"[{database}] ";
        AppLogger.Info("XeSession", $"[{server.DisplayName}] {where}The long-query session {LongQueryCompletionsCollector.LegacyXeSessionName} exists. An older Performance Monitor Lite or Darling created it. This install dropped its own copy once and no longer touches it, so it stays until someone drops it by hand. The README says how, and Darling's --drop-xe-sessions command drops it.");
    }

    /// <summary>The same report for a test that replaces the ensure's batch (<see cref="LegacyLongQuerySessionExistsForTests"/>).</summary>
    private void LookForLegacyLongQuerySessionForTests(ServerConnection server, string database)
    {
        if (LegacyLongQuerySessionExistsForTests is { } exists && ShouldLookForLegacyLongQuerySession(server, database) && exists(server, database))
        {
            ReportLegacyLongQuerySessionFound(server, database);
        }
    }
}
