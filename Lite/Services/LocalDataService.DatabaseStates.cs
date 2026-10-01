/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Whether the last deviation sweep for a server SKIPPED its maintenance block (#2266). Transition-logged
    /// from this, so a sustained contention window reports once rather than once per sweep.
    ///
    /// <para><b>Why this needs recording at all.</b> The maintenance block below — the baseline seed, the #2189
    /// heal, the #2203 forget and the prune — is best-effort: it opens the write connection with a 5-second lock
    /// acquisition and, on <c>TimeoutException</c>, skips everything and lets the deviation read run anyway.
    /// Skipping is the right call and the block comment there argues it well, but it was completely SILENT, so a
    /// sustained window of write-lock contention meant baselines quietly stopped being seeded and healed with no
    /// evidence anywhere. That matters because #2189 exists precisely because an unhealed baseline inverts the
    /// alert permanently — the failure it prevents is invisible, so its absence has to be visible.</para>
    ///
    /// <para>Per server, because the lock is process-wide but the consequence is not: one server's skipped heal
    /// says nothing about another's. Never pruned — one bool per server ever swept.</para>
    /// </summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, bool> _lastMaintenanceSkipped = new();
    /* Effective state = STANDBY for a read-only log-shipping secondary (is_in_standby), else the raw
       state_desc. A standby secondary reports state_desc = ONLINE with is_in_standby = 1 and flips through
       RESTORING on every log restore; collapsing it to a single stable STANDBY token means it baselines as
       STANDBY and never churns. Composed identically in the seed, the deviation read, the editor and reset. */
    private const string EffectiveStateSql = "CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END";

    /* The two places a database_states snapshot can be read from. Every read in this file (the alert sweep, the
       override editor and the re-baseline button) takes its snapshots from one of these two and from nothing
       else. */
    private const string HotDatabaseStates = "database_states";
    private const string ArchivedDatabaseStates = "v_database_states";

    /// <summary>
    /// The table or view ONE database-state read takes its snapshots from, and what that source can support.
    /// </summary>
    /// <param name="Table">Named by every statement of the read: <see cref="HotDatabaseStates"/> or <see cref="ArchivedDatabaseStates"/>.</param>
    /// <param name="Newest">The newest collection time the source holds for the server, or null when it holds none.</param>
    /// <param name="HasTwoSnapshots">Whether the source holds at least two distinct collection times for the server, the least the deviation rule compares.</param>
    private sealed record DatabaseStatesSource(string Table, DateTime? Newest, bool HasTwoSnapshots)
    {
        /// <summary>
        /// The newest snapshot is older than the age at which archival moves rows out of the hot tables
        /// (<see cref="ArchiveService.HotDataDays"/> days). The server has not reported for longer than the hot
        /// table keeps rows, so the archive's last rows are not the state of the server now.
        /// </summary>
        public bool IsStale => Newest is { } newest && DateTime.UtcNow - newest > TimeSpan.FromDays(ArchiveService.HotDataDays);

        /// <summary>Two snapshots to compare, and a newest one that is not stale.</summary>
        public bool HasVerdict => HasTwoSnapshots && !IsStale;
    }

    /// <summary>
    /// Picks the source ONE database-state read uses: the hot <c>database_states</c> table when it holds at
    /// least two snapshots for the server, else <c>v_database_states</c> (hot plus Parquet archive) when that
    /// does, else the hot table. The alert sweep, the override editor and the re-baseline button all call this
    /// one method, so the editor lists the databases the alert is judging.
    ///
    /// <para>The deviation rule compares the two newest snapshots, and the maintenance statements treat the
    /// newest one as the full list of databases on the server. Both need two snapshots in one place. The hot
    /// table does not hold them just after the 512 MB archive-and-reset, which moves every hot row to Parquet
    /// and keeps the config tables, and not after a week without a collection, when archival has moved the old
    /// rows out. Read as it was, an empty hot table meant "no databases": the prune deleted every
    /// auto-baseline, and the read returned nothing, which the alert path takes as "every database
    /// recovered".</para>
    ///
    /// <para>With fewer than two in both, the hot table is returned, as before: a brand-new server has nothing
    /// to compare, and its first snapshot still seeds its baselines.</para>
    ///
    /// <para>The caller holds a lock on <paramref name="connection"/> and keeps it until its reads are done, so
    /// the archive-and-reset, which takes the write lock, cannot empty the chosen source in between. A failed
    /// read throws; the alert engine logs it and skips the check, which never resolves an active alert.</para>
    /// </summary>
    private static async Task<DatabaseStatesSource> ChooseDatabaseStatesSourceAsync(LockedConnection connection, int serverId)
    {
        var hot = await ProbeDatabaseStatesAsync(connection, HotDatabaseStates, serverId);
        if (hot.HasTwoSnapshots)
        {
            return hot;
        }

        var archived = await ProbeDatabaseStatesAsync(connection, ArchivedDatabaseStates, serverId);
        return archived.HasTwoSnapshots ? archived : hot;
    }

    /// <summary>
    /// The newest collection time in <paramref name="table"/> for the server, and whether an older one exists:
    /// the same pair the deviation read compares. A second collection time that exists is "two or more
    /// snapshots", and finding it does not need a count of every distinct time the table holds.
    /// </summary>
    private static async Task<DatabaseStatesSource> ProbeDatabaseStatesAsync(LockedConnection connection, string table, int serverId)
    {
        using var probe = connection.CreateCommand();
        probe.CommandText = $@"
WITH newest AS (
    SELECT MAX(collection_time) AS t FROM {table} WHERE server_id = $1
)
SELECT
    (SELECT t FROM newest),
    (SELECT MAX(collection_time) FROM {table} WHERE server_id = $1 AND collection_time < (SELECT t FROM newest))";
        probe.Parameters.Add(new DuckDBParameter { Value = serverId });
        using var reader = await probe.ExecuteReaderAsync();
        await reader.ReadAsync();
        DateTime? newest = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
        return new DatabaseStatesSource(table, newest, HasTwoSnapshots: !reader.IsDBNull(1));
    }

    /// <summary>
    /// The databases whose collected state deviates from their expected state, for the baseline-deviation
    /// database-state alert. Fires only when the deviation is present in the TWO most recent collections
    /// (so a restart's RECOVERY_PENDING / RECOVERING transients — and a standby secondary's per-restore
    /// RESTORING flicker — don't page unless the condition actually sticks). First AUTO-SEEDS a baseline
    /// (the effective state) for any database in the newest snapshot that has none — EXCEPT an integrity or
    /// transient effective state (<see cref="DatabaseStateTokens.NeverBaselinedSqlList"/>), which is left
    /// pending so onboarding a server mid-outage or mid-restore doesn't learn that state as expected. Then
    /// HEALS an auto-baseline that nonetheless records one of those states — written by an older build, or
    /// by re-baselining a database by hand while it was mid-something — to ONLINE once the database reaches
    /// ONLINE, so it stops deviating by being healthy (#2189); a user override, and an OFFLINE or STANDBY
    /// baseline, are never touched. Then FORGETS the recorded alerted-state of any database now back at its
    /// expected state (#2203), so a second episode can announce. Also tidies auto-baselines for databases
    /// that have dropped off the newest snapshot (user overrides are preserved).
    ///
    /// <para><b>Which snapshots.</b> One source for the whole sweep, chosen by
    /// <see cref="ChooseDatabaseStatesSourceAsync"/> under the write lock the sweep runs under: the hot table
    /// when it holds two or more snapshots for the server (the common case), else <c>v_database_states</c> (hot
    /// plus Parquet archive) when that does. Right after the 512 MB archive-and-reset the hot table holds none,
    /// and after a week without a collection archival has moved its rows out, so the sweep reads the archive
    /// rather than an empty table.</para>
    ///
    /// <para><b>No verdict.</b> Returns null, never an empty list, when the store cannot judge this pass, and the
    /// alert engine then fires nothing and resolves nothing. That is the case when the chosen source holds
    /// fewer than two snapshots to compare, and when its newest snapshot is older than
    /// <see cref="ArchiveService.HotDataDays"/> days, because rows that old are the archive's last, not the
    /// state of the server now. An empty list is a verdict: nothing deviates. A null return changes nothing in
    /// the store, with one exception: the baseline seed still runs on a source with a single fresh snapshot,
    /// so a brand-new server learns its baselines from its first snapshot (the seed only adds missing rows).
    /// On a stale source nothing runs.</para>
    /// </summary>
    public async Task<List<DatabaseStateInfo>?> GetDatabaseStateDeviationsAsync(int serverId)
    {
        /* #2208: the sweep INSERTs, UPDATEs and DELETEs, so it runs under the WRITE lock — which is what its own
           contract asks for ("operations that must not race with archival or compaction"). It used the READ
           lock, which was wrong twice over: the writes could interleave with archival, and holding a read lock
           across several statements starves writers, because a ReaderWriterLockSlim writer waits for every
           reader to drain and OpenWriteConnectionAsync gives up after 5 seconds. That is how this surfaced — an
           unrelated server-tags test timed out acquiring the write lock while this method held the read lock,
           on a static lock shared by the whole process.

           The source is chosen under that same write lock, and the deviation read runs under it too. The
           archive-and-reset takes the write lock to empty the hot table, so it cannot run between the choice and
           the reads; a source chosen under a lock released first could be emptied before the read, and an empty
           read means "every database recovered".

           BEST-EFFORT. If archival holds the write lock for the whole 5-second wait, the sweep is skipped for
           this cycle and the deviation read still runs, under the read lock, with the source chosen under that
           same read lock. A cycle without seeding is a cycle where a brand-new database has no baseline yet,
           which the no-baseline arm already handles. The alternative — letting the timeout escape — would either
           crash the sweep or, if swallowed into an empty result, read as "every database recovered" and clear
           the alert memory for all of them. Skipping maintenance is the only failure mode here that loses
           nothing. */
        var maintenanceSkipped = false;
        var judged = false;
        List<DatabaseStateInfo>? deviations = null;
        try
        {
            using var maintenance = await OpenWriteConnectionAsync();
            deviations = await SweepDatabaseStatesAsync(maintenance, serverId);
            judged = true;
        }
        catch (TimeoutException)
        {
            /* Archival or compaction holds the write lock. Skip this cycle's maintenance and read anyway —
               see the block comment at the top of the method for why skipping is the only lossless option. */
            maintenanceSkipped = true;
        }

        /* #2266: report the skip, on the TRANSITION. Warn rather than Error, because a single skipped cycle is
           the expected benign outcome of colliding with archival and the next sweep re-runs everything; it is a
           SUSTAINED run of them that means baselines have stopped being seeded and healed. One line when it
           starts and one when it recovers, rather than a line per sweep that would read as noise and be
           filtered — which is how the silence would effectively return. */
        if (maintenanceSkipped)
        {
            if (!_lastMaintenanceSkipped.TryGetValue(serverId, out var wasSkipped) || !wasSkipped)
            {
                _lastMaintenanceSkipped[serverId] = true;
                AppLogger.Warn(nameof(GetDatabaseStateDeviationsAsync),
                    $"server {serverId}: skipped this cycle's database-state maintenance — could not acquire the " +
                    "store write lock within 5s (archival or compaction holds it). Baselines are not being " +
                    "seeded or healed while this persists (#2189/#2203); deviations are still read. Expected " +
                    "occasionally — if it repeats, the write lock is contended.");
            }
        }
        else if (_lastMaintenanceSkipped.TryGetValue(serverId, out var hadSkipped) && hadSkipped)
        {
            _lastMaintenanceSkipped[serverId] = false;
            AppLogger.Info(nameof(GetDatabaseStateDeviationsAsync),
                $"server {serverId}: database-state maintenance is running again after one or more skipped " +
                "cycles (#2266).");
        }

        if (judged)
        {
            return deviations;
        }

        /* The write lock was busy, so the sweep above did not run. Read under the read lock instead, choosing the
           source under that same lock; the archive-and-reset takes the write lock and cannot run in between. */
        using var connection = await OpenConnectionAsync();
        var source = await ChooseDatabaseStatesSourceAsync(connection, serverId);
        return source.HasVerdict
            ? await ReadDatabaseStateDeviationsAsync(connection, serverId, source.Table)
            : null;
    }

    /// <summary>
    /// The sweep, under the write lock the caller holds: chooses the source, runs the baseline maintenance on
    /// it and reads the deviations from it, so nothing can empty the source between those steps. Null is "no
    /// verdict" (see <see cref="GetDatabaseStateDeviationsAsync"/>).
    /// </summary>
    private static async Task<List<DatabaseStateInfo>?> SweepDatabaseStatesAsync(LockedConnection maintenance, int serverId)
    {
        var source = await ChooseDatabaseStatesSourceAsync(maintenance, serverId);
        var src = source.Table;

        /* A newest snapshot older than the age at which archival moves rows out of the hot tables is not the
           state of the server now: it is what the archive kept when the server stopped reporting. Nothing is
           learned from it and nothing is changed because of it. */
        if (source.IsStale)
        {
            return null;
        }

        /* Seed missing baselines from the latest snapshot (insert-if-absent; effective state). An integrity
           or transient state is never learned: a critical first observation stays pending and alerts via the
           no-baseline arm below, and a transient one stays pending SILENTLY until it settles. */
        using (var seed = maintenance.CreateCommand())
        {
            seed.CommandText = $@"
INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, ds.database_name, {EffectiveStateSql}, false, now()::TIMESTAMP
FROM {src} ds
WHERE ds.server_id = $1
AND   ds.collection_time = (SELECT MAX(collection_time) FROM {src} WHERE server_id = $1)
AND   ds.state_desc IS NOT NULL
AND   {EffectiveStateSql} NOT IN ({DatabaseStateTokens.NeverBaselinedSqlList})
AND   NOT EXISTS (
    SELECT 1 FROM config_database_state_expected e
    WHERE e.server_id = $1 AND e.database_name = ds.database_name
)";
            seed.Parameters.Add(new DuckDBParameter { Value = serverId });
            await seed.ExecuteNonQueryAsync();
        }

        /* One snapshot is enough to seed a baseline, and seeding only adds missing rows. It is not enough for the
           heal, the forget and the prune, which treat the newest snapshot as the full list of databases, nor for
           the deviation read, which compares two. With fewer than two there is no verdict, and the rest leaves
           the store as it is. */
        if (!source.HasTwoSnapshots)
        {
            return null;
        }

        /* #2189: re-learn an ILLEGITIMATE inferred baseline as ONLINE once the database reaches ONLINE — the
           seed's own rule applied after the fact. An expectation recording a state the seed would refuse to
           learn is not a baseline, it is a snapshot of a database mid-something, and left alone it inverts
           the alert permanently. The widened seed above cannot fix that on its own because it only governs
           rows that do not exist yet; this heals the ones already written, by the old seed or by "reset to
           current" pressed during a restore or an outage (that path records whatever it sees, no filter).

           Two gates. is_user_override = false: an operator who declared an expected state meant it, and a
           database parked at expected OFFLINE must still alert when it comes back ONLINE. And the state list
           is NOT "anything that is not ONLINE" — OFFLINE and STANDBY are steady states worth learning, and
           leaving one is real news: a STANDBY secondary that turns up ONLINE has stopped being a secondary
           (somebody recovered it, log shipping is broken), and an auto-OFFLINE database brought up for an
           hour and re-parked would come back deviating forever. Both are this bug's own shape.

           The EFFECTIVE state is what is matched, never state_desc — a standby secondary reports
           state_desc = 'ONLINE' with is_in_standby set, so matching the raw column would re-baseline every
           log-shipping secondary to ONLINE and then alert it forever for being STANDBY. Uncorrelated
           IN (...), like the prune below. */
        using (var heal = maintenance.CreateCommand())
        {
            heal.CommandText = $@"
UPDATE config_database_state_expected
SET expected_state = 'ONLINE',
    updated_at = now()::TIMESTAMP
WHERE server_id = $1
AND   is_user_override = false
AND   expected_state IN ({DatabaseStateTokens.NeverBaselinedSqlList})
AND   database_name IN (
    SELECT ds.database_name
    FROM {src} ds
    WHERE ds.server_id = $1
    AND   ds.collection_time = (SELECT MAX(collection_time) FROM {src} WHERE server_id = $1)
    AND   {EffectiveStateSql} = 'ONLINE'
)";
            heal.Parameters.Add(new DuckDBParameter { Value = serverId });
            await heal.ExecuteNonQueryAsync();
        }

        /* #2203: forget the announced-state for any database the store now shows back AT its expected state, so
           this cycle judges against a healed memory. Darling learned why this must be store-derived rather than
           engine-derived (#2166): the engine also clears on the falling edge it witnesses, but that path is
           reachable only through an in-memory active set that empties on every restart, so a restart landing
           between an alert and the recovery left the memory sticky FOREVER and silently swallowed the next
           episode. Asking the store cannot have that gap. One sample at expected is enough where the deviation
           rule needs two: clearing can only cause an extra alert, never a missed one, and a flap cannot exploit
           it because a flap never survives the two-sample test to alert at all.

           Placed AFTER the #2189 heal, not before the seed, because the heal REWRITES expected_state: a database
           whose illegitimate RESTORING baseline was just healed to ONLINE is, from this statement's point of
           view, a database that has arrived back at its expected state, and its memory is about a deviation that
           no longer exists. Running first would leave that dead memory for a cycle. Ordering against the seed is
           immaterial either way — a row the seed just inserted has a NULL memory, which this skips. */
        using (var clearRecovered = maintenance.CreateCommand())
        {
            clearRecovered.CommandText = $@"
UPDATE config_database_state_expected AS e
SET last_alerted_state = NULL,
    last_alerted_at = NULL
WHERE e.server_id = $1
AND   e.last_alerted_state IS NOT NULL
AND   (e.expected_state = '{DatabaseStateTokens.Ignore}'
       OR EXISTS (
           SELECT 1 FROM {src} ds
           WHERE ds.server_id = $1
           AND   ds.collection_time = (SELECT MAX(collection_time) FROM {src} WHERE server_id = $1)
           AND   ds.database_name = e.database_name
           AND   {EffectiveStateSql} = e.expected_state
       ))";
            clearRecovered.Parameters.Add(new DuckDBParameter { Value = serverId });
            await clearRecovered.ExecuteNonQueryAsync();
        }

        /* Tidy auto-baselines for databases no longer in the newest snapshot (dropped/renamed). User
           overrides are kept — an operator's intent shouldn't vanish because a database is briefly gone.

           The EXISTS guard: with no snapshot at all for the server in the chosen source, "not in the newest
           snapshot" is true of every row, so without it this statement deleted every auto-baseline. An empty
           source is missing data, not a list of dropped databases. */
        using (var prune = maintenance.CreateCommand())
        {
            prune.CommandText = $@"
DELETE FROM config_database_state_expected
WHERE server_id = $1
AND   is_user_override = false
AND   EXISTS (SELECT 1 FROM {src} WHERE server_id = $1)
AND   database_name NOT IN (
    SELECT database_name FROM {src}
    WHERE server_id = $1
    AND   collection_time = (SELECT MAX(collection_time) FROM {src} WHERE server_id = $1)
)";
            prune.Parameters.Add(new DuckDBParameter { Value = serverId });
            await prune.ExecuteNonQueryAsync();
        }

        return await ReadDatabaseStateDeviationsAsync(maintenance, serverId, src);
    }

    /// <summary>
    /// The two-snapshot deviation read, against the source the caller chose, on a connection whose lock the
    /// caller still holds.
    /// </summary>
    private static async Task<List<DatabaseStateInfo>> ReadDatabaseStateDeviationsAsync(LockedConnection connection, int serverId, string src)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $@"
WITH newest AS (
    SELECT MAX(collection_time) AS t FROM {src} WHERE server_id = $1
),
prev AS (
    SELECT MAX(collection_time) AS t FROM {src}
    WHERE server_id = $1 AND collection_time < (SELECT t FROM newest)
),
latest AS (
    SELECT ds.database_name, {EffectiveStateSql} AS eff
    FROM {src} ds
    WHERE ds.server_id = $1 AND ds.collection_time = (SELECT t FROM newest)
),
previous AS (
    SELECT ds.database_name, {EffectiveStateSql} AS eff
    FROM {src} ds
    WHERE ds.server_id = $1 AND ds.collection_time = (SELECT t FROM prev)
)
SELECT l.database_name, l.eff, COALESCE(e.expected_state, ''), COALESCE(e.last_alerted_state, '')
FROM latest l
JOIN previous p
  ON p.database_name = l.database_name
LEFT JOIN config_database_state_expected e
  ON  e.server_id = $1
  AND e.database_name = l.database_name
WHERE (e.expected_state IS NULL
        AND l.eff IN ({DatabaseStateTokens.CriticalSqlList})
        AND p.eff IN ({DatabaseStateTokens.CriticalSqlList}))
   OR (e.expected_state IS NOT NULL AND e.expected_state <> '(ignore)'
        AND l.eff IS DISTINCT FROM e.expected_state
        AND p.eff IS DISTINCT FROM e.expected_state)
ORDER BY l.database_name";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<DatabaseStateInfo>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseStateInfo
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                StateDesc = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ExpectedState = reader.IsDBNull(2) ? "" : reader.GetString(2),
                /* #2203: what the engine last TOLD an operator about, so alreadyAnnounced can be true in
                   Lite the way it already is in Darling. Empty means never announced. */
                LastAlertedState = reader.IsDBNull(3) ? "" : reader.GetString(3)
            });
        }

        return items;
    }

    /// <summary>
    /// Every database in the newest snapshot, for the override editor: its current EFFECTIVE state joined to
    /// its expected state (and whether that expected state is a user override vs the auto-seeded baseline).
    /// Seeds/prunes first via the alert read so the editor and the alert always agree on what "expected" is,
    /// and reads the same source the alert sweep does (<see cref="ChooseDatabaseStatesSourceAsync"/>), so after
    /// the 512 MB archive-and-reset the editor still lists the databases and their overrides instead of an
    /// empty grid. Unlike the alert it has no age limit: an operator can still set an override for a server
    /// that has not reported for a while.
    /// </summary>
    public async Task<List<DatabaseStateExpectedRow>> GetDatabaseStateExpectationsAsync(int serverId)
    {
        /* Reuse the seed/prune side-effect so the editor shows a baseline for every current database. The
           verdict it returns is not needed here: null only means the alert has nothing to say this pass. */
        await GetDatabaseStateDeviationsAsync(serverId);

        using var connection = await OpenConnectionAsync();
        /* Chosen under the read lock this read holds, so the archive-and-reset cannot empty it in between. */
        var src = (await ChooseDatabaseStatesSourceAsync(connection, serverId)).Table;
        using var command = connection.CreateCommand();
        command.CommandText = $@"
WITH latest AS (
    SELECT ds.database_name, {EffectiveStateSql} AS eff
    FROM {src} ds
    WHERE ds.server_id = $1
    AND   ds.collection_time = (SELECT MAX(collection_time) FROM {src} WHERE server_id = $1)
)
SELECT
    l.database_name,
    l.eff,
    e.expected_state,
    e.is_user_override
FROM latest l
LEFT JOIN config_database_state_expected e
  ON  e.server_id = $1
  AND e.database_name = l.database_name
ORDER BY l.database_name";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<DatabaseStateExpectedRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseStateExpectedRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CurrentState = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ExpectedState = reader.IsDBNull(2) ? "" : reader.GetString(2),
                IsUserOverride = !reader.IsDBNull(3) && reader.GetBoolean(3)
            });
        }

        return items;
    }

    /// <summary>
    /// Sets a database's expected state (the override) — pass <see cref="DatabaseStateTokens.Ignore"/>
    /// to opt the database out of the alert. Upserts on (server_id, database_name) and marks the row a
    /// user override so a later auto-seed never clobbers it.
    /// </summary>
    public async Task SetDatabaseStateExpectedAsync(int serverId, string databaseName, string expectedState)
    {
        /* #2208: an upsert, so the WRITE lock. Unlike the deviation read's maintenance prologue this one does
           NOT swallow a timeout: it is a user action, and TimeoutException's message ("try again in a few
           moments") is written to be shown. Silently dropping an operator's declared expected state would be
           the worst outcome available here. */
        using var connection = await OpenWriteConnectionAsync();
        using var command = connection.CreateCommand();
        /* now()::TIMESTAMP, not a bare current_timestamp: DuckDB resolves a bare current_timestamp against
           the table's columns (and errors) inside a VALUES row and an ON CONFLICT DO UPDATE SET, so the
           override write uses the function form — which binds as an expression everywhere. INSERT ... SELECT
           (not VALUES) for the same reason, matching the auto-seed and reset shape. */
        command.CommandText = @"
INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, $2, $3, true, now()::TIMESTAMP
ON CONFLICT (server_id, database_name)
DO UPDATE SET expected_state = EXCLUDED.expected_state, is_user_override = true, updated_at = now()::TIMESTAMP";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = expectedState });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Re-baselines a database: sets its expected state to its current EFFECTIVE state and clears the
    /// user-override flag, so the alert stops firing for a state the operator has accepted as the new normal.
    /// The current state comes from the same source the alert sweep reads, so this also works for a server whose
    /// newest snapshots are only in the archive.
    /// </summary>
    public async Task ResetDatabaseStateExpectedToCurrentAsync(int serverId, string databaseName)
    {
        /* #2208: an upsert, so the WRITE lock, and the timeout surfaces for the same reason as the override
           write above — this is the operator pressing a button. */
        using var connection = await OpenWriteConnectionAsync();
        /* Chosen under the write lock this write holds, so the archive-and-reset cannot empty it in between. */
        var src = (await ChooseDatabaseStatesSourceAsync(connection, serverId)).Table;
        using var command = connection.CreateCommand();
        command.CommandText = $@"
INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, $2, {EffectiveStateSql}, false, now()::TIMESTAMP
FROM {src} ds
WHERE ds.server_id = $1
AND   ds.database_name = $2
AND   ds.collection_time = (SELECT MAX(collection_time) FROM {src} WHERE server_id = $1)
ON CONFLICT (server_id, database_name)
DO UPDATE SET expected_state = EXCLUDED.expected_state, is_user_override = false, updated_at = now()::TIMESTAMP";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        await command.ExecuteNonQueryAsync();
    }
}

/// <summary>
/// One row of the database-state override editor: a database's current effective state alongside its
/// expected state (auto-seeded baseline or user override).
/// </summary>
public sealed class DatabaseStateExpectedRow
{
    public string DatabaseName { get; set; } = "";
    public string CurrentState { get; set; } = "";
    public string ExpectedState { get; set; } = "";
    public bool IsUserOverride { get; set; }

    public bool IsIgnored => ExpectedState == PerformanceMonitor.Alerting.DatabaseStateTokens.Ignore;

    /// <summary>True when the current state differs from the expected state and the DB isn't ignored.</summary>
    public bool IsDeviating => !IsIgnored && !string.Equals(CurrentState, ExpectedState, System.StringComparison.Ordinal);
}
