/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #9439: one row per lock wait on the monitoring store that lasted at least <see cref="ThresholdMilliseconds"/>, with the waiting and the
/// blocking role and application, the relation, the lock mode, how long it waited and how it ended.
///
/// <para><b>The source is the store's own log.</b> A once-a-minute sample of <c>pg_locks</c> would miss most waits of 5 to 50 seconds. The
/// managed store runs <c>log_lock_waits</c>, which logs <c>process N still waiting for MODE on OBJECT after X ms</c> with a
/// <c>DETAIL:  Process holding the lock: M. Wait queue: ...</c> line when a wait passes <c>deadlock_timeout</c>, and
/// <c>process N acquired MODE on OBJECT after X ms</c> when it ends. The store log sweep (<see cref="StoreLogSweep"/>) already reads that
/// log, so each slab it reads is also handed to <see cref="Parse"/>.</para>
///
/// <para><b>Roles.</b> The log names process ids, and the application of any process that wrote a line (the managed prefix carries
/// <c>%a</c>). A role is read from a rolling map of <c>pg_stat_activity</c> that the sweep refreshes each time it runs
/// (<see cref="Pids"/>), kept about <see cref="PidMapRetention"/>. An id the map does not know reads <see cref="Unresolved"/>. A guess is
/// never made: an entry answers for a wait only when its session had started by the wait's start and was still alive when the map was
/// read, which is the only case where the same process id cannot belong to another session.</para>
///
/// <para><b>Outcome.</b> <c>granted</c> (an <c>acquired</c> line), <c>timed_out</c> (the same process's next ERROR is a lock or statement
/// timeout), <c>cancelled</c> (any other ERROR or FATAL), <c>deadlock</c> (a <c>detected deadlock</c> line or a deadlock ERROR) or
/// <c>waiting</c> (the log read ended with the wait still open, finished by a later sweep).</para>
///
/// <para><b>Time.</b> The log's stamps are in the store's <c>log_timezone</c>. The store converts them itself (see
/// <see cref="ConvertStampsSql"/>), so a DST change or any zone it names is handled by the engine and not guessed here.</para>
/// </summary>
public static class StoreLockWaits
{
    /// <summary>The store schema version that adds <c>collect.store_lock_waits</c>.</summary>
    public const int Version = 316;

    /// <summary>A wait shorter than this gets no row.</summary>
    public const int ThresholdMilliseconds = 5000;

    /// <summary>What a role or an application reads when the log or the map cannot say.</summary>
    public const string Unresolved = "unresolved";

    /// <summary>The wait ended with the lock granted.</summary>
    public const string Granted = "granted";

    /// <summary>The wait ended in a <c>lock_timeout</c> or a <c>statement_timeout</c>.</summary>
    public const string TimedOut = "timed_out";

    /// <summary>The wait ended in a cancel, a disconnect or any other error.</summary>
    public const string Cancelled = "cancelled";

    /// <summary>The wait ended in a deadlock.</summary>
    public const string Deadlock = "deadlock";

    /// <summary>The wait was still open when the log read ended.</summary>
    public const string Waiting = "waiting";

    /// <summary>
    /// How long the map of sessions keeps a reading after it was last seen. The log sweep runs hourly and resolves each wait in the
    /// hour it reads, so a reading has to outlive a whole sweep slab: 60 minutes of log plus 15 for a late or skipped tick.
    /// </summary>
    public static readonly TimeSpan PidMapRetention = TimeSpan.FromMinutes(75);

    /// <summary>How often the service reads the sessions into the map.</summary>
    public static readonly TimeSpan PidMapRefreshInterval = TimeSpan.FromMinutes(1);

    /// <summary>
    /// How long after a session was last seen it still answers for a wait that began then: two refresh intervals. A blocker usually
    /// ends within seconds of the wait it caused, so its last reading is often a little older than the wait. Its process id could
    /// only belong to another session if the operating system wrapped its process ids inside that time, and a later session on the
    /// same id that the map read has its own start time, which is what keeps the two apart.
    /// </summary>
    public static readonly TimeSpan PidMapResolveSlack = TimeSpan.FromMinutes(2);

    /// <summary>The time one read of the sessions may take, in seconds.</summary>
    public const int PidMapRefreshTimeoutSeconds = 15;

    /// <summary>The rung (V316): one empty plain table and two indexes. No data moves. An older build never reads it.</summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.store_lock_waits
(
    wait_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    wait_started_at timestamp NOT NULL,
    captured_at timestamp NOT NULL,
    waiter_pid integer NOT NULL,
    waiter_role text NOT NULL,
    waiter_application text NOT NULL,
    holder_pids integer[] NOT NULL,
    holder_roles text[] NOT NULL,
    holder_applications text[] NOT NULL,
    lock_mode text NOT NULL,
    lock_object text NOT NULL,
    relation_name text,
    duration_ms bigint NOT NULL,
    outcome text NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_store_lock_waits_time
    ON collect.store_lock_waits(wait_started_at);

CREATE INDEX IF NOT EXISTS idx_store_lock_waits_open
    ON collect.store_lock_waits(waiter_pid)
    WHERE outcome = 'waiting';";

    /// <summary>The store's retention for these rows, on the same cutoff as the log census. $1 cutoff (naive UTC).</summary>
    public const string RetentionDeleteSql = @"
DELETE FROM collect.store_lock_waits
WHERE wait_started_at < $1";

    /// <summary>
    /// The log stamps as naive UTC. $1 the stamps as <c>timestamp[]</c>, as the log wrote them (no zone). The store reads each as a
    /// time in its own <c>log_timezone</c>, the zone <c>%m</c> renders in, so the answer is exact across a DST change.
    /// </summary>
    public const string ConvertStampsSql = @"
SELECT ((t.s AT TIME ZONE pg_catalog.current_setting('log_timezone')) AT TIME ZONE 'UTC')
FROM unnest($1::timestamp[]) WITH ORDINALITY AS t(s, ord)
ORDER BY t.ord";

    /// <summary>
    /// The names of the relations a wait named by oid, found in this database only. $1 database oids, $2 relation oids, parallel
    /// <c>bigint[]</c>. A relation dropped since the wait, or in another database, has no row.
    /// </summary>
    public const string ResolveRelationsSql = @"
SELECT x.rel_oid, n.nspname || '.' || c.relname
FROM unnest($1::bigint[], $2::bigint[]) AS x(db_oid, rel_oid)
JOIN pg_catalog.pg_database AS d ON d.oid = x.db_oid::oid AND d.datname = pg_catalog.current_database()
JOIN pg_catalog.pg_class AS c ON c.oid = x.rel_oid::oid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace";

    /// <summary>
    /// One wait row. A <c>waiting</c> row is written only when the same process has none open on the same lock, because PostgreSQL 18 logs
    /// <c>still waiting</c> again on each wake of a wait that has not ended.
    /// </summary>
    public const string InsertSql = @"
INSERT INTO collect.store_lock_waits
    (wait_started_at, captured_at, waiter_pid, waiter_role, waiter_application, holder_pids, holder_roles, holder_applications,
     lock_mode, lock_object, relation_name, duration_ms, outcome)
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
WHERE $13 <> 'waiting'
OR    NOT EXISTS (SELECT 1 FROM collect.store_lock_waits AS w
                  WHERE w.waiter_pid = $3 AND w.lock_object = $10 AND w.outcome = 'waiting')";

    /// <summary>
    /// Closes the open waits of one process, for a wait whose opening line an earlier read saw. $1 pid, $2 outcome, $3 duration in
    /// ms when the log gave it (<c>acquired</c> and <c>detected deadlock</c> do) else null, $4 the end time as naive UTC, $5 the lock
    /// the wait was on, or null for any. Only a row that opened within <see cref="OpenWaitMaxAge"/> before the end can be closed: after a
    /// store crash an open row never gets its end line, and a process id reused much later must not close it with a duration of hours.
    /// Returns each closed row's id and duration so the caller can drop the ones under the threshold.
    /// </summary>
    public const string CloseSql = @"
UPDATE collect.store_lock_waits AS w
SET outcome = $2,
    duration_ms = COALESCE($3::bigint,
                           GREATEST(w.duration_ms, (EXTRACT(EPOCH FROM ($4::timestamp - w.wait_started_at)) * 1000)::bigint))
WHERE w.waiter_pid = $1
AND   w.outcome = 'waiting'
AND   w.wait_started_at <= $4::timestamp
AND   w.wait_started_at >= $4::timestamp - interval '1 day'
AND   ($5::text IS NULL OR w.lock_object = $5)
RETURNING w.wait_id, w.duration_ms";

    /// <summary>The longest an open wait can stay open and still be closed by a later end line.</summary>
    public static readonly TimeSpan OpenWaitMaxAge = TimeSpan.FromDays(1);

    /// <summary>The processes with an open wait that an end at or after $1 (naive UTC) could still close.</summary>
    public const string OpenPidsSql = @"
SELECT DISTINCT waiter_pid
FROM collect.store_lock_waits
WHERE outcome = 'waiting'
AND   wait_started_at >= $1::timestamp - interval '1 day'";

    /// <summary>Drops closed rows that proved shorter than the threshold. $1 ids.</summary>
    public const string DeleteShortSql = @"
DELETE FROM collect.store_lock_waits
WHERE wait_id = ANY($1::bigint[])";

    /// <summary>
    /// Every process of the store now, one row each, background processes included. An autovacuum worker holding SHARE UPDATE EXCLUSIVE
    /// while a migration's ALTER TABLE waits is the likeliest holder on this store, and it has no role, so its backend type stands in.
    /// </summary>
    public const string ActivitySql = @"
SELECT a.pid,
       (a.backend_start AT TIME ZONE 'UTC'),
       COALESCE(a.usename::text, a.backend_type),
       COALESCE(a.application_name, ''),
       (pg_catalog.clock_timestamp() AT TIME ZONE 'UTC')
FROM pg_catalog.pg_stat_activity AS a
WHERE a.pid IS NOT NULL
AND   a.backend_start IS NOT NULL";

    /* ===================== the sessions map ===================== */

    /// <summary>
    /// A rolling map of the store's processes: a process id to the role and application it had, with when it started and when it was
    /// last seen alive. <see cref="Shared"/> is the service's, refreshed once a minute by the store pass.
    /// </summary>
    public sealed class PidMap
    {
        private readonly Dictionary<int, List<Entry>> _byPid = [];
        private readonly object _gate = new();

        /// <summary>One reading of a session.</summary>
        /// <param name="BackendStartUtc">When the session started.</param>
        /// <param name="SeenAliveUtc">When the map read it, so it was alive then.</param>
        /// <param name="Role">Its role, or its backend type for a process with none.</param>
        /// <param name="Application">Its application name.</param>
        public readonly record struct Entry(DateTime BackendStartUtc, DateTime SeenAliveUtc, string Role, string Application);

        /// <summary>Takes one reading of every process, client or background, and drops readings older than <see cref="PidMapRetention"/>.</summary>
        public async Task RefreshAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
        {
            var read = new List<(int Pid, Entry Entry)>();
            await using (var command = new NpgsqlCommand(ActivitySql, connection) { CommandTimeout = PidMapRefreshTimeoutSeconds })
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    read.Add((
                        reader.GetInt32(0),
                        new Entry(
                            DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Unspecified),
                            DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Unspecified),
                            reader.GetString(2),
                            reader.GetString(3))));
                }
            }

            lock (_gate)
            {
                foreach (var (pid, entry) in read)
                {
                    if (!_byPid.TryGetValue(pid, out var list))
                    {
                        _byPid[pid] = list = [];
                    }

                    list.RemoveAll(e => e.BackendStartUtc == entry.BackendStartUtc);
                    list.Add(entry);
                }

                if (read.Count > 0)
                {
                    var cutoff = read[0].Entry.SeenAliveUtc - PidMapRetention;
                    foreach (var pid in new List<int>(_byPid.Keys))
                    {
                        _byPid[pid].RemoveAll(e => e.SeenAliveUtc < cutoff);
                        if (_byPid[pid].Count == 0)
                        {
                            _byPid.Remove(pid);
                        }
                    }
                }
            }
        }

        /// <summary>Adds a reading directly, for a test.</summary>
        internal void Add(int pid, Entry entry)
        {
            lock (_gate)
            {
                if (!_byPid.TryGetValue(pid, out var list))
                {
                    _byPid[pid] = list = [];
                }

                list.Add(entry);
            }
        }

        /// <summary>
        /// The reading that answers for <paramref name="pid"/> at <paramref name="atUtc"/>, or null. A reading answers only when the
        /// session had started by then and was last seen no more than <see cref="PidMapResolveSlack"/> before it. When two readings of
        /// the id both qualify, the later start wins. Otherwise the process id may have belonged to another
        /// session at <paramref name="atUtc"/>.
        /// </summary>
        public Entry? Resolve(int pid, DateTime atUtc)
        {
            lock (_gate)
            {
                if (_byPid.TryGetValue(pid, out var list))
                {
                    Entry? best = null;
                    foreach (var entry in list)
                    {
                        if (entry.BackendStartUtc <= atUtc
                            && atUtc <= entry.SeenAliveUtc + PidMapResolveSlack
                            && (best is null || entry.BackendStartUtc > best.Value.BackendStartUtc))
                        {
                            best = entry;
                        }
                    }

                    return best;
                }
            }

            return null;
        }
    }

    /// <summary>The service's map, refreshed by every store log sweep.</summary>
    public static PidMap Shared { get; } = new();

    /* ===================== the parser ===================== */

    /// <summary>A wait as the log tells it, before roles and times are resolved. Stamps are as the log wrote them (no zone).</summary>
    /// <param name="WaiterPid">The waiting process.</param>
    /// <param name="WaiterApplication">The application in the waiter's log prefix, or empty when the prefix has none (an empty <c>%a</c>, or the <c>[unknown]</c> PostgreSQL writes for a session with no application name).</param>
    /// <param name="LockMode">The mode named, such as <c>ShareLock</c>.</param>
    /// <param name="LockObject">What is locked, as PostgreSQL wrote it, such as <c>relation 16385 of database 16384</c>.</param>
    /// <param name="DatabaseOid">The database oid in <paramref name="LockObject"/>, or null.</param>
    /// <param name="RelationOid">The relation oid in <paramref name="LockObject"/>, or null.</param>
    /// <param name="ContextRelation">The relation a CONTEXT line names, or null.</param>
    /// <param name="HolderPids">The processes the DETAIL names as holding the lock.</param>
    /// <param name="OpenedStamp">The stamp of the line that opened the wait.</param>
    /// <param name="OpenedAfterMs">The wait's age on that line.</param>
    /// <param name="Outcome">How the log read left it.</param>
    /// <param name="ClosedStamp">The stamp of the line that ended it, or null while it is open.</param>
    /// <param name="ClosedAfterMs">The wait's whole length when the ending line gives it, else null.</param>
    public readonly record struct ParsedWait(
        int WaiterPid,
        string? WaiterApplication,
        string LockMode,
        string LockObject,
        long? DatabaseOid,
        long? RelationOid,
        string? ContextRelation,
        IReadOnlyList<int> HolderPids,
        DateTime OpenedStamp,
        double OpenedAfterMs,
        string Outcome,
        DateTime? ClosedStamp,
        double? ClosedAfterMs);

    /// <summary>The end of a wait whose opening line an earlier read saw.</summary>
    public readonly record struct ParsedClose(int Pid, string Outcome, DateTime Stamp, double? AfterMs);

    /// <summary>An end the store held no open wait for when its slab was written, with its end time as naive UTC. The sweep retries it after it has read on, because the wait it ends may be opened by a slab it reads later.</summary>
    internal readonly record struct UnmatchedClose(ParsedClose Close, DateTime EndUtc);

    /// <summary>What <see cref="Parse"/> found in one slab.</summary>
    /// <param name="Waits">Waits opened in the slab, ended or still open.</param>
    /// <param name="Closes">Ends of waits opened before the slab.</param>
    /// <param name="ApplicationByPid">The application every process that wrote an entry in the slab had, for a holder that wrote nothing else.</param>
    public sealed record ParseResult(
        IReadOnlyList<ParsedWait> Waits,
        IReadOnlyList<ParsedClose> Closes,
        IReadOnlyDictionary<int, string> ApplicationByPid)
    {
        /// <summary>No waits and no ends.</summary>
        public static ParseResult Empty { get; } = new([], [], new Dictionary<int, string>());

        /// <summary>Whether the slab held nothing to write.</summary>
        public bool IsEmpty => Waits.Count == 0 && Closes.Count == 0;
    }

    /// <summary>The managed prefix: <c>%m [%p] %a </c>. The stamp's zone name is read past and never used.</summary>
    private static readonly Regex s_prefix = new(
        @"^(?<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)(?: [^\s\[]+)? \[(?<pid>\d+)\] ?(?<app>.*)$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>Text every slab needs at least one of to hold a wait or an end: the three lock lines, or an error entry (<see cref="Handle"/>).</summary>
    private static readonly SearchValues<string> s_endOrWaitText = SearchValues.Create(
        [" still waiting for ", " acquired ", " detected deadlock ", "ERROR:", "FATAL:", "PANIC:"],
        StringComparison.Ordinal);

    private static readonly Regex s_waitingLine = new(
        @"^process (?<pid>\d+) still waiting for (?<mode>\w+) on (?<obj>.+?) after (?<ms>\d+(?:\.\d+)?) ms$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_acquiredLine = new(
        @"^process (?<pid>\d+) acquired (?<mode>\w+) on (?<obj>.+?) after (?<ms>\d+(?:\.\d+)?) ms$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_deadlockLine = new(
        @"^process (?<pid>\d+) detected deadlock while waiting for (?<mode>\w+) on (?<obj>.+?) after (?<ms>\d+(?:\.\d+)?) ms$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_holders = new(
        @"Process(?:es)? holding the lock: (?<pids>\d+(?:, \d+)*)\.",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_relationOf = new(
        @"relation (?<rel>\d+) of database (?<db>\d+)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_contextRelation = new(
        "in relation \"(?<name>[^\"]+)\"",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly string[] s_stampFormats =
        ["yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.ffffff", "yyyy-MM-dd HH:mm:ss.ff", "yyyy-MM-dd HH:mm:ss.f"];

    private sealed class OpenWait
    {
        public int Pid;
        public string? App;
        public string Mode = string.Empty;
        public string Object = string.Empty;
        public long? Db;
        public long? Rel;
        public string? ContextRelation;
        public List<int> Holders = [];
        public DateTime Stamp;
        public double AfterMs;
    }

    private sealed class Entry
    {
        public bool Has;
        public int Pid;
        public string? App;
        public DateTime Stamp;
        public string Severity = string.Empty;
        public string Message = string.Empty;
        public readonly List<string> Continuations = [];
    }

    /// <summary>
    /// Reads the lock waits out of one slab of the store's log. English only: the lines are matched by their English text, so a store
    /// whose <c>lc_messages</c> is not English (<paramref name="detailInEnglish"/> false, the classifier's own rule, #4251) yields
    /// nothing rather than a guess. A wait opened in the slab and ended in it is complete. One opened and not ended is
    /// <see cref="Waiting"/>. An end with no opening in the slab (<c>acquired</c>, a lock or statement timeout, a deadlock) is a
    /// <see cref="ParsedClose"/>, for the rows an earlier read left <see cref="Waiting"/>.
    /// </summary>
    public static ParseResult Parse(string? slab, bool detailInEnglish)
    {
        if (!detailInEnglish || string.IsNullOrEmpty(slab) || !slab.AsSpan().ContainsAny(s_endOrWaitText))
        {
            /* A wait or an end comes only from a lock line or an ERROR, FATAL or PANIC entry, and the text of each is in the slab if one is.
               A slab with none (most of a busy store's log is checkpoints and autovacuum) holds nothing to write, so it is not read line by line. */
            return ParseResult.Empty;
        }

        var waits = new List<ParsedWait>();
        var closes = new List<ParsedClose>();
        var apps = new Dictionary<int, string>();
        var open = new Dictionary<int, OpenWait>();
        var current = new Entry();

        void Flush()
        {
            if (!current.Has)
            {
                return;
            }

            var entry = current;
            current = new Entry();
            Handle(entry, open, waits, closes);
        }

        var start = 0;
        while (start <= slab.Length)
        {
            var newline = slab.IndexOf('\n', start);
            var end = newline < 0 ? slab.Length : newline;
            var line = slab.AsSpan(start, end - start);
            if (line.Length > 0 && line[^1] == '\r')
            {
                line = line[..^1];
            }

            start = newline < 0 ? slab.Length + 1 : newline + 1;
            if (line.Length == 0)
            {
                continue;
            }

            if (line[0] != '\t' && StoreLogClassifier.TryReadField(line, out var primary, out var name, out var labelStart, out var messageStart))
            {
                if (primary)
                {
                    Flush();
                    current.Has = true;
                    current.Severity = name;
                    current.Message = line[messageStart..].ToString();
                    var prefix = labelStart > 0 ? line[..labelStart].ToString() : string.Empty;
                    if (prefix.EndsWith(' '))
                    {
                        prefix = prefix[..^1];
                    }

                    var m = s_prefix.Match(prefix);
                    if (m.Success && int.TryParse(m.Groups["pid"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var pid))
                    {
                        current.Pid = pid;
                        current.App = NormalizeApplication(m.Groups["app"].Value);
                        current.Stamp = ParseStamp(m.Groups["ts"].Value);
                        apps[pid] = current.App;
                    }
                    else
                    {
                        current.Has = false;
                    }

                    continue;
                }
            }

            if (current.Has)
            {
                current.Continuations.Add(line.ToString());
            }
        }

        Flush();

        foreach (var w in open.Values)
        {
            waits.Add(new ParsedWait(w.Pid, w.App, w.Mode, w.Object, w.Db, w.Rel, w.ContextRelation, w.Holders, w.Stamp, w.AfterMs, Waiting, null, null));
        }

        return waits.Count == 0 && closes.Count == 0 ? ParseResult.Empty : new ParseResult(waits, closes, apps);
    }

    /// <summary>The application a prefix names. PostgreSQL writes <c>[unknown]</c> for a session with no application name and nothing at all for a process with none, so both read as empty.</summary>
    private static string NormalizeApplication(string app) =>
        app.Equals("[unknown]", StringComparison.Ordinal) ? string.Empty : app;

    private static DateTime ParseStamp(string text) =>
        DateTime.TryParseExact(text, s_stampFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var stamp)
            ? DateTime.SpecifyKind(stamp, DateTimeKind.Unspecified)
            : default;

    private static void Handle(Entry e, Dictionary<int, OpenWait> open, List<ParsedWait> waits, List<ParsedClose> closes)
    {
        if (e.Severity == "LOG")
        {
            var m = s_waitingLine.Match(e.Message);
            if (m.Success && int.Parse(m.Groups["pid"].Value, CultureInfo.InvariantCulture) == e.Pid)
            {
                if (open.ContainsKey(e.Pid))
                {
                    /* PostgreSQL 18 writes this line again each time a wait that has not ended wakes: still the same wait. */
                    return;
                }

                var wait = new OpenWait
                {
                    Pid = e.Pid,
                    App = e.App,
                    Mode = m.Groups["mode"].Value,
                    Object = m.Groups["obj"].Value,
                    Stamp = e.Stamp,
                    AfterMs = double.Parse(m.Groups["ms"].Value, CultureInfo.InvariantCulture),
                };
                var rel = s_relationOf.Match(wait.Object);
                if (rel.Success)
                {
                    wait.Rel = long.Parse(rel.Groups["rel"].Value, CultureInfo.InvariantCulture);
                    wait.Db = long.Parse(rel.Groups["db"].Value, CultureInfo.InvariantCulture);
                }

                foreach (var continuation in e.Continuations)
                {
                    var holders = s_holders.Match(continuation);
                    if (holders.Success)
                    {
                        foreach (var id in holders.Groups["pids"].Value.Split(", "))
                        {
                            wait.Holders.Add(int.Parse(id, CultureInfo.InvariantCulture));
                        }
                    }

                    var context = s_contextRelation.Match(continuation);
                    if (context.Success && wait.ContextRelation is null)
                    {
                        wait.ContextRelation = context.Groups["name"].Value;
                    }
                }

                open[e.Pid] = wait;
                return;
            }

            m = s_acquiredLine.Match(e.Message);
            if (m.Success && int.Parse(m.Groups["pid"].Value, CultureInfo.InvariantCulture) == e.Pid)
            {
                End(e, Granted, double.Parse(m.Groups["ms"].Value, CultureInfo.InvariantCulture), open, waits, closes);
                return;
            }

            m = s_deadlockLine.Match(e.Message);
            if (m.Success && int.Parse(m.Groups["pid"].Value, CultureInfo.InvariantCulture) == e.Pid)
            {
                End(e, Deadlock, double.Parse(m.Groups["ms"].Value, CultureInfo.InvariantCulture), open, waits, closes);
                return;
            }

            return;
        }

        if (e.Severity is "ERROR" or "FATAL" or "PANIC")
        {
            /* The next error of a process that is waiting ends the wait: a waiting backend runs nothing else. */
            var outcome =
                e.Message.StartsWith("canceling statement due to lock timeout", StringComparison.Ordinal)
                || e.Message.StartsWith("canceling statement due to statement timeout", StringComparison.Ordinal) ? TimedOut
                : e.Message.StartsWith("deadlock detected", StringComparison.Ordinal) ? Deadlock
                : Cancelled;

            if (open.ContainsKey(e.Pid))
            {
                End(e, outcome, null, open, waits, closes);
            }
            else
            {
                /* The opening line may be in an earlier read. WriteAsync checks the store for an open wait of this process first. */
                closes.Add(new ParsedClose(e.Pid, outcome, e.Stamp, null));
            }
        }
    }

    private static void End(Entry e, string outcome, double? afterMs, Dictionary<int, OpenWait> open, List<ParsedWait> waits, List<ParsedClose> closes)
    {
        if (open.Remove(e.Pid, out var w))
        {
            waits.Add(new ParsedWait(w.Pid, w.App, w.Mode, w.Object, w.Db, w.Rel, w.ContextRelation, w.Holders, w.Stamp, w.AfterMs, outcome, e.Stamp, afterMs));
        }
        else
        {
            closes.Add(new ParsedClose(e.Pid, outcome, e.Stamp, afterMs));
        }
    }

    /* ===================== the write ===================== */

    /// <summary>
    /// Writes what <paramref name="parsed"/> found, inside the slab's own transaction, so the rows and the marker that accounts for them
    /// land together. Roles come from <paramref name="pids"/>. Returns the rows written, for the caller's log. An end with no open wait
    /// in the store is added to <paramref name="unmatched"/> when given.
    /// </summary>
    internal static async Task<int> WriteAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ParseResult parsed,
        PidMap pids,
        DateTime captureTime,
        List<UnmatchedClose>? unmatched,
        CancellationToken cancellationToken)
    {
        if (parsed.IsEmpty)
        {
            return 0;
        }

        var stamps = new List<DateTime>();
        foreach (var w in parsed.Waits)
        {
            stamps.Add(w.OpenedStamp);
            stamps.Add(w.ClosedStamp ?? w.OpenedStamp);
        }

        foreach (var c in parsed.Closes)
        {
            stamps.Add(c.Stamp);
        }

        var utc = new List<DateTime>(stamps.Count);
        await using (var convert = new NpgsqlCommand(ConvertStampsSql, connection, transaction) { CommandTimeout = StoreLogSweep.SweepTimeoutSeconds })
        {
            convert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = stamps.ToArray() });
            await using var reader = await convert.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                utc.Add(DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Unspecified));
            }
        }

        if (utc.Count != stamps.Count)
        {
            return 0;
        }

        var relationNames = await ResolveRelationsAsync(connection, transaction, parsed.Waits, cancellationToken);

        var written = 0;
        var shortIds = new List<long>();
        /* The slab's ends come first: each closes a wait an earlier slab opened, so it is earlier in the log than anything the loop below
           handles. Handled the other way round, a later wait of the same process on the same lock would take over the older open row. */
        if (parsed.Closes.Count > 0)
        {
            var closeUtc = new List<DateTime>(parsed.Closes.Count);
            for (var i = 0; i < parsed.Closes.Count; i++)
            {
                closeUtc.Add(utc[(parsed.Waits.Count * 2) + i]);
            }

            var openPids = await ReadOpenPidsAsync(connection, transaction, closeUtc.Min(), cancellationToken);
            for (var i = 0; i < parsed.Closes.Count; i++)
            {
                var c = parsed.Closes[i];
                if (!openPids.Contains(c.Pid))
                {
                    unmatched?.Add(new UnmatchedClose(c, closeUtc[i]));
                    continue;
                }

                written += (await CloseAsync(connection, transaction, c.Pid, c.Outcome, c.AfterMs is { } ms ? (long)Math.Round(ms) : null, closeUtc[i], null, shortIds, cancellationToken)).Kept;
            }
        }

        var index = 0;
        foreach (var w in parsed.Waits)
        {
            var openedUtc = utc[index++];
            var closedUtc = utc[index++];
            var startedUtc = openedUtc.AddMilliseconds(-w.OpenedAfterMs);
            double durationMs = w.Outcome == Waiting
                ? w.OpenedAfterMs
                : w.ClosedAfterMs ?? Math.Max(w.OpenedAfterMs, (closedUtc - startedUtc).TotalMilliseconds);
            if (w.Outcome != Waiting)
            {
                /* PostgreSQL 18 writes "still waiting" again each time a long wait wakes, so a slab can open on a repeat of a wait an
                   earlier slab left open. That wait is complete here, and it ends the open row: closing it, and not inserting a second
                   row, keeps one row per wait. */
                var (closed, kept) = await CloseAsync(connection, transaction, w.WaiterPid, w.Outcome, (long)Math.Round(durationMs), closedUtc, w.LockObject, shortIds, cancellationToken);
                if (closed > 0)
                {
                    written += kept;
                    continue;
                }

                if (durationMs < ThresholdMilliseconds)
                {
                    continue;
                }
            }

            var waiter = pids.Resolve(w.WaiterPid, startedUtc);
            var waiterApplication = !string.IsNullOrEmpty(w.WaiterApplication) ? w.WaiterApplication : waiter?.Application ?? Unresolved;
            var holderPids = new List<int>();
            var holderRoles = new List<string>();
            var holderApps = new List<string>();
            foreach (var holder in w.HolderPids)
            {
                var found = pids.Resolve(holder, startedUtc);
                holderPids.Add(holder);
                holderRoles.Add(found?.Role ?? Unresolved);
                var seenApp = parsed.ApplicationByPid.TryGetValue(holder, out var seen) ? seen : null;
                holderApps.Add(
                    !string.IsNullOrEmpty(found?.Application) ? found!.Value.Application
                    : !string.IsNullOrEmpty(seenApp) ? seenApp
                    : found is not null || seenApp is not null ? string.Empty : Unresolved);
            }

            string? relation = w.ContextRelation;
            if (relation is null && w.RelationOid is { } oid && relationNames.TryGetValue(oid, out var name))
            {
                relation = name;
            }

            await using var insert = new NpgsqlCommand(InsertSql, connection, transaction) { CommandTimeout = StoreLogSweep.SweepTimeoutSeconds };
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = startedUtc });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = captureTime });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = w.WaiterPid });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = waiter?.Role ?? Unresolved });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = waiterApplication });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = holderPids.ToArray() });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = holderRoles.ToArray() });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = holderApps.ToArray() });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = w.LockMode });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = w.LockObject });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)relation ?? DBNull.Value });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (long)Math.Round(durationMs) });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = w.Outcome });
            written += await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        if (shortIds.Count > 0)
        {
            await using var delete = new NpgsqlCommand(DeleteShortSql, connection, transaction) { CommandTimeout = StoreLogSweep.SweepTimeoutSeconds };
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = shortIds.ToArray() });
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        return written;
    }

    /// <summary>The processes with an open wait that an end at or after <paramref name="earliestEndUtc"/> could close.</summary>
    private static async Task<HashSet<int>> ReadOpenPidsAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime earliestEndUtc, CancellationToken cancellationToken)
    {
        var openPids = new HashSet<int>();
        await using var command = new NpgsqlCommand(OpenPidsSql, connection, transaction) { CommandTimeout = StoreLogSweep.SweepTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = earliestEndUtc });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            openPids.Add(reader.GetInt32(0));
        }

        return openPids;
    }

    /// <summary>
    /// Closes the open rows of one process (see <see cref="CloseSql"/>). Returns how many it closed and how many of those it kept. A
    /// closed row shorter than the threshold is added to <paramref name="shortIds"/> for the caller to delete.
    /// </summary>
    private static async Task<(int Closed, int Kept)> CloseAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int pid, string outcome, long? durationMs, DateTime endUtc, string? lockObject,
        List<long> shortIds, CancellationToken cancellationToken)
    {
        var closed = 0;
        var kept = 0;
        await using var close = new NpgsqlCommand(CloseSql, connection, transaction) { CommandTimeout = StoreLogSweep.SweepTimeoutSeconds };
        close.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = pid });
        close.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = outcome });
        close.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = durationMs is { } ms ? ms : DBNull.Value });
        close.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = endUtc });
        close.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)lockObject ?? DBNull.Value });
        await using var reader = await close.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            closed++;
            if (reader.GetInt64(1) < ThresholdMilliseconds)
            {
                shortIds.Add(reader.GetInt64(0));
            }
            else
            {
                kept++;
            }
        }

        return (closed, kept);
    }

    /// <summary>
    /// Retries the ends <see cref="WriteAsync"/> found no open wait for, once the sweep has read every slab it will read, in its own
    /// transaction. A wait that opened in a later slab of an older file and ended in a newer file's first slab is open only now.
    /// Returns the rows closed and kept.
    /// </summary>
    internal static async Task<int> RetryClosesAsync(
        NpgsqlConnection connection, IReadOnlyList<UnmatchedClose> unmatched, CancellationToken cancellationToken)
    {
        if (unmatched.Count == 0)
        {
            return 0;
        }

        var ordered = unmatched.OrderBy(u => u.EndUtc).ToList();
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var openPids = await ReadOpenPidsAsync(connection, transaction, ordered[0].EndUtc, cancellationToken);
        var shortIds = new List<long>();
        var written = 0;
        foreach (var u in ordered)
        {
            if (!openPids.Contains(u.Close.Pid))
            {
                continue;
            }

            written += (await CloseAsync(connection, transaction, u.Close.Pid, u.Close.Outcome,
                u.Close.AfterMs is { } ms ? (long)Math.Round(ms) : null, u.EndUtc, null, shortIds, cancellationToken)).Kept;
        }

        if (shortIds.Count > 0)
        {
            await using var delete = new NpgsqlCommand(DeleteShortSql, connection, transaction) { CommandTimeout = StoreLogSweep.SweepTimeoutSeconds };
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = shortIds.ToArray() });
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return written;
    }

    private static async Task<Dictionary<long, string>> ResolveRelationsAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, IReadOnlyList<ParsedWait> waits, CancellationToken cancellationToken)
    {
        var names = new Dictionary<long, string>();
        var dbs = new List<long>();
        var rels = new List<long>();
        foreach (var w in waits)
        {
            if (w.RelationOid is { } rel && w.DatabaseOid is { } db && w.ContextRelation is null)
            {
                dbs.Add(db);
                rels.Add(rel);
            }
        }

        if (rels.Count == 0)
        {
            return names;
        }

        await using var command = new NpgsqlCommand(ResolveRelationsSql, connection, transaction) { CommandTimeout = StoreLogSweep.SweepTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = dbs.ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = rels.ToArray() });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            names[reader.GetInt64(0)] = reader.GetString(1);
        }

        return names;
    }
}
