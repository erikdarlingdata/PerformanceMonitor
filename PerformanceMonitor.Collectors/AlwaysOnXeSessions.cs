/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Which of the three always-on Extended Events sessions a statement, a name or a choice is about (#4961, #5640). Every
/// decision on a kind is a <c>switch</c> whose last arm throws: a fourth kind fails loudly instead of reading as a
/// deadlock session.
/// </summary>
public enum AlwaysOnXeSessionKind
{
    Deadlock,
    BlockedProcess,
    /// <summary>Client-side timeouts and cancels (<c>sqlserver.attention</c>), counted by an event_counter target and sampled by a ring buffer (#5640).</summary>
    Attention,
}

/// <summary>Which session of a kind an install reads in one Azure SQL Database: the name every install shares, or its own.</summary>
public enum AlwaysOnXeChoice
{
    Shared,
    Own,
}

/// <summary>What the catalog of one database says about one session (<c>sys.database_event_sessions</c>).</summary>
public enum AlwaysOnXeCatalog
{
    /// <summary>No row. The catalogs are scoped to what a login can see, so this is not proof the session is absent.</summary>
    Missing = 0,

    /// <summary>
    /// A row, but not with the definition a read needs: for a deadlock session, without its event; for a client-timeout session
    /// (#5640), with events, actions or targets other than the ones the shared builder makes. Never reported for blocked-process sessions.
    /// </summary>
    WrongEvent = 1,

    /// <summary>A row with the right event.</summary>
    Present = 2,
}

/// <summary>What one pass of the Azure ensure did, for the host's log line.</summary>
public enum AlwaysOnXeChange
{
    /// <summary>The session the install reads was there and running.</summary>
    None,

    /// <summary>The install created the session it reads.</summary>
    Created,

    /// <summary>
    /// The install created the client-timeout session without the actions Azure SQL Database was not proved to accept (#5640):
    /// the first create was refused with error 25744, so the session captures client timeouts without statement text.
    /// </summary>
    CreatedWithoutStatementText,

    /// <summary>The install started a stopped session by name.</summary>
    Started,

    /// <summary>The shared session was unusable, so the install now reads its own.</summary>
    FellBack,

    /// <summary>The shared session reads back usable again, so the install dropped its own and reads the shared one.</summary>
    SwitchedBack,
}

/// <summary>The answer of one pass of the Azure ensure: the session the install reads from now on, and what the pass did.</summary>
public readonly record struct AlwaysOnXeAzureResult(AlwaysOnXeChoice Choice, AlwaysOnXeChange Change);

/// <summary>
/// One open connection to one Azure SQL Database, as the Azure ensure needs it. Each host implements it over its own
/// connection, with the statements the builders in <see cref="AlwaysOnXeSessions"/> make, so the decisions are made once,
/// here, and a test replaces the server with a script.
/// </summary>
public interface IAlwaysOnXeDatabase
{
    /// <summary>What the database's catalog says about the named session of this kind.</summary>
    Task<AlwaysOnXeCatalog> ReadCatalogAsync(AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken);

    /// <summary>
    /// Whether the named session has a row in <c>sys.dm_xe_database_sessions</c>, the DMV the ring-buffer read joins. That DMV
    /// lists started sessions only, so a stopped session reads false here.
    /// </summary>
    Task<bool> IsStartedAsync(string sessionName, CancellationToken cancellationToken);

    /// <summary>Runs one statement the builders made: a create, a start or a drop.</summary>
    Task ExecuteAsync(string statement, CancellationToken cancellationToken);

    /// <summary>True for the engine's "already exists" (25631) and "already started" (25705), which say the session is there.</summary>
    bool IsAlreadyPresent(Exception exception);

    /// <summary>
    /// True when the registration's own connection to this database carries read-only intent (#4961). A session cannot be
    /// created or dropped over such a connection, because it reaches a read-only replica, so the ensure sends a create and a
    /// drop through <see cref="ExecuteWithoutReadOnlyIntentAsync"/> instead. False for a registration without the intent,
    /// whose every statement goes over its own connection, as before.
    /// </summary>
    bool ReadOnlyIntent => false;

    /// <summary>
    /// Runs one statement the builders made, a create's definition or a drop, over a connection with the read-only intent
    /// forced off, which reaches the primary. The definition replicates to the read-only replicas. Only called when
    /// <see cref="ReadOnlyIntent"/> is true; without the intent it is <see cref="ExecuteAsync"/>.
    /// </summary>
    Task ExecuteWithoutReadOnlyIntentAsync(string statement, CancellationToken cancellationToken) =>
        ExecuteAsync(statement, cancellationToken);

    /// <summary>
    /// The numbers of the errors a failed statement carries, read off the host's own exception because this project has no
    /// SqlClient (#4961). <see cref="AlwaysOnXeSessions.MarkAzureCapsFailure"/> leaves a read-only database's refusal unmarked
    /// by them. None by default, so a host that does not say marks every failure.
    /// </summary>
    IEnumerable<int> ErrorNumbers(Exception exception) => Array.Empty<int>();
}

/// <summary>
/// One database as a registration with read-only intent sees it (#4961): the reads, the starts and the stops go over the
/// registration's own connection, because run state is per replica, and a create or a drop goes over a connection without the
/// intent. That connection is opened on the first statement that needs it, so a database that needs no create or drop never
/// opens one, and it is kept for the rest of the pass and closed with this object.
/// </summary>
public sealed class AlwaysOnXeReadOnlyIntentDatabase : IAlwaysOnXeDatabase, IDisposable
{
    private readonly IAlwaysOnXeDatabase _own;
    private readonly Func<CancellationToken, Task<IAlwaysOnXeDatabase>> _openWithoutReadOnlyIntent;
    private IAlwaysOnXeDatabase? _withoutReadOnlyIntent;

    /// <param name="own">The registration's own connection.</param>
    /// <param name="openWithoutReadOnlyIntent">Opens a connection to the same database with the intent forced off.</param>
    public AlwaysOnXeReadOnlyIntentDatabase(IAlwaysOnXeDatabase own, Func<CancellationToken, Task<IAlwaysOnXeDatabase>> openWithoutReadOnlyIntent)
    {
        ArgumentNullException.ThrowIfNull(own);
        ArgumentNullException.ThrowIfNull(openWithoutReadOnlyIntent);
        _own = own;
        _openWithoutReadOnlyIntent = openWithoutReadOnlyIntent;
    }

    public bool ReadOnlyIntent => true;

    public Task<AlwaysOnXeCatalog> ReadCatalogAsync(AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken) =>
        _own.ReadCatalogAsync(kind, sessionName, cancellationToken);

    public Task<bool> IsStartedAsync(string sessionName, CancellationToken cancellationToken) =>
        _own.IsStartedAsync(sessionName, cancellationToken);

    public Task ExecuteAsync(string statement, CancellationToken cancellationToken) =>
        _own.ExecuteAsync(statement, cancellationToken);

    public bool IsAlreadyPresent(Exception exception) => _own.IsAlreadyPresent(exception);

    public IEnumerable<int> ErrorNumbers(Exception exception) => _own.ErrorNumbers(exception);

    public async Task ExecuteWithoutReadOnlyIntentAsync(string statement, CancellationToken cancellationToken)
    {
        _withoutReadOnlyIntent ??= await _openWithoutReadOnlyIntent(cancellationToken);
        await _withoutReadOnlyIntent.ExecuteAsync(statement, cancellationToken);
    }

    /// <summary>Closes the connection without the intent, if a statement opened one. The registration's own is the host's to close.</summary>
    public void Dispose()
    {
        (_withoutReadOnlyIntent as IDisposable)?.Dispose();
        _withoutReadOnlyIntent = null;
    }
}

/// <summary>
/// The deadlock and blocked-process sessions on Azure SQL Database (#4961): the names, the statements and the decisions that
/// keep two installs of the product from fighting over one session.
///
/// <para><b>Two names per capture.</b> Every install shares one name per capture, as before. An install that cannot use the
/// shared session in a database (the catalog holds one without the deadlock event, or it cannot be started) makes its own,
/// <c>PerformanceMonitor_{product}_{id}_{Deadlock|BlockedProcess}</c>, in that database, and reads that one. The own name is
/// made here from the same product and install id as the long-query session
/// (<see cref="LongQueryCompletionsCollector.XeSessionNameFor"/>), and the two capture suffixes are cut from the shared
/// names, so no second spelling of either exists.</para>
///
/// <para><b>The client-timeout capture has no shared session in this build (#9895).</b> Two installs at different builds that
/// monitor one server used to replace each other's <c>PerformanceMonitor_Attention</c> session, each dropping the other's
/// definition on connect. Every install now makes and reads only its own, <c>PerformanceMonitor_{product}_{id}_Attention</c>, on
/// a server and in an Azure SQL Database alike, the way the long-query completions session has been named since #1496. The shared
/// name (<see cref="AttentionXeSessionName"/>) is the one older builds make: no builder here creates, starts, reads, stops or drops
/// it, because an older install may still read it.</para>
///
/// <para><b>Nothing here drops a session it did not make.</b> The drop builder takes an own name, and refuses any other.
/// A shared deadlock or blocked-process session is not dropped, because another install may be reading it.</para>
/// </summary>
public static class AlwaysOnXeSessions
{
    private const string Prefix = "PerformanceMonitor_";

    /// <summary>
    /// The sentence a failed create or start of a session on Azure SQL Database carries. The documented caps are 100 started
    /// sessions in one database, 100 database-scoped sessions per elastic pool, and 128 MB of session memory per database and
    /// 512 MB per pool. The engine gives no error number for a refusal at a cap, so the failure cannot be told from other
    /// refusals, and the sentence goes on every one of them, except a read-only database's, which no cap explains
    /// (<see cref="MarkAzureCapsFailure"/>).
    /// </summary>
    public const string AzureCapsSentence =
        "Azure SQL Database allows at most 100 started event sessions in a database and 100 database-scoped sessions per elastic pool, "
        + "and caps session memory at 128 MB per database and 512 MB per pool, so a create or start can be refused near those limits.";

    private const string CapsKey = "PerformanceMonitor.AzureXeSessionCaps";

    /// <summary>
    /// How often a host that does not ensure on every cycle ensures these sessions again: once an hour, the same interval the
    /// long-query trace's create pass keeps (<see cref="LongQueryTraceDatabases.RetryInterval"/>).
    /// </summary>
    public static TimeSpan EnsureInterval => LongQueryTraceDatabases.RetryInterval;

    /// <summary>True when the ensure has not run yet (null), or last ran at least <see cref="EnsureInterval"/> before <paramref name="utcNow"/>.</summary>
    public static bool EnsureIsDue(DateTime? lastEnsuredUtc, DateTime utcNow)
    {
        if (lastEnsuredUtc is null)
        {
            return true;
        }

        return utcNow - lastEnsuredUtc.Value >= EnsureInterval;
    }

    /// <summary>The name every install shares for this capture.</summary>
    public static string SharedNameFor(AlwaysOnXeSessionKind kind) => kind switch
    {
        AlwaysOnXeSessionKind.Deadlock => DeadlocksCollector.XeSessionName,
        AlwaysOnXeSessionKind.BlockedProcess => BlockedProcessReportCollector.XeSessionName,
        AlwaysOnXeSessionKind.Attention => AttentionXeSessionName,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>
    /// The shared name of the client-timeout capture (#5640), the one every install and both products read before #9895. It is
    /// the attention collector's own constant. A build with #9895 never creates, reads, alters or drops it (an older install may
    /// still read it), so it stays only to cut the capture's suffix from, and for the verb an operator runs to drop sessions.
    /// </summary>
    public const string AttentionXeSessionName = AttentionCollector.XeSessionName;

    /// <summary>The capture's own suffix, cut from the shared name: <c>Deadlock</c>, <c>BlockedProcess</c> or <c>Attention</c>.</summary>
    private static string SuffixFor(AlwaysOnXeSessionKind kind) => SharedNameFor(kind).Substring(Prefix.Length);

    /// <summary>
    /// This install's own session of the capture: <c>PerformanceMonitor_{product}_{id}_{Deadlock|BlockedProcess|Attention}</c>. Throws
    /// for a product other than <see cref="LongQueryCompletionsCollector.LiteProduct"/> or
    /// <see cref="LongQueryCompletionsCollector.DarlingProduct"/>, and for an id that fails <see cref="InstallId.IsValid"/>, so
    /// a name it makes is safe to put in a statement (the id is hex only).
    /// </summary>
    public static string OwnNameFor(string product, string? installId, AlwaysOnXeSessionKind kind)
    {
        if (!IsProduct(product))
        {
            throw new ArgumentException($"The product must be {LongQueryCompletionsCollector.LiteProduct} or {LongQueryCompletionsCollector.DarlingProduct}.", nameof(product));
        }

        if (!InstallId.IsValid(installId))
        {
            throw new ArgumentException("The install id is missing or not eight lowercase hex digits.", nameof(installId));
        }

        return Prefix + product + "_" + installId + "_" + SuffixFor(kind);
    }

    /// <summary><see cref="OwnNameFor"/>, or null when the id does not pass <see cref="InstallId.IsValid"/>: a host with no id has no fallback to make.</summary>
    public static string? TryOwnNameFor(string product, string? installId, AlwaysOnXeSessionKind kind) =>
        InstallId.IsValid(installId) ? OwnNameFor(product, installId, kind) : null;

    /// <summary>True when <paramref name="name"/> is a name <see cref="OwnNameFor"/> makes for this capture.</summary>
    public static bool IsOwnName(string? name, AlwaysOnXeSessionKind kind)
    {
        var suffix = "_" + SuffixFor(kind);
        if (name is null
            || !name.StartsWith(Prefix, StringComparison.Ordinal)
            || !name.EndsWith(suffix, StringComparison.Ordinal)
            || name.Length <= Prefix.Length + suffix.Length)
        {
            return false;
        }

        var middle = name.Substring(Prefix.Length, name.Length - Prefix.Length - suffix.Length);
        foreach (var product in new[] { LongQueryCompletionsCollector.LiteProduct, LongQueryCompletionsCollector.DarlingProduct })
        {
            if (middle.Length == product.Length + 1 + InstallId.Length
                && middle.StartsWith(product + "_", StringComparison.Ordinal)
                && InstallId.IsValid(middle.Substring(product.Length + 1)))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsProduct(string product) =>
        string.Equals(product, LongQueryCompletionsCollector.LiteProduct, StringComparison.Ordinal)
        || string.Equals(product, LongQueryCompletionsCollector.DarlingProduct, StringComparison.Ordinal);

    /// <summary>
    /// The session a read of this capture names: <paramref name="chosen"/> when the host's ensure chose one for the database,
    /// and the shared name when it has not (no entry yet, and every server-scoped read). A chosen name that is neither the
    /// shared name nor an own name of this capture is refused, so a read can only ever name one of the two.
    /// </summary>
    public static string ReadNameFor(AlwaysOnXeSessionKind kind, string? chosen)
    {
        /* #9895: the client-timeout read names this install's own session only. With no name (an install with no id) there is no
           session to read, and the shared one is another install's, so the read refuses instead of falling back to it. */
        if (kind == AlwaysOnXeSessionKind.Attention)
        {
            if (string.IsNullOrEmpty(chosen))
            {
                throw new InvalidOperationException(NoOwnAttentionSessionMessage);
            }

            if (!IsOwnName(chosen, kind))
            {
                throw new ArgumentException("The client-timeout session name must be this install's own name.", nameof(chosen));
            }

            return chosen;
        }

        if (string.IsNullOrEmpty(chosen))
        {
            return SharedNameFor(kind);
        }

        if (!string.Equals(chosen, SharedNameFor(kind), StringComparison.Ordinal) && !IsOwnName(chosen, kind))
        {
            throw new ArgumentException("The session name is neither the shared name nor an own name of this capture.", nameof(chosen));
        }

        return chosen;
    }

    /// <summary>What a client-timeout read, create or start says when this install has no id to name its own session with (#9895).</summary>
    public const string NoOwnAttentionSessionMessage =
        "The client-timeout session is one per install, named from the install's id, and this install has no id for now, so there is no session to create or read.";

    private static void RequireName(AlwaysOnXeSessionKind kind, string sessionName)
    {
        /* #9895: no shared client-timeout session is made, started or read any more. */
        var sharedAllowed = kind != AlwaysOnXeSessionKind.Attention;
        if (!(sharedAllowed && string.Equals(sessionName, SharedNameFor(kind), StringComparison.Ordinal)) && !IsOwnName(sessionName, kind))
        {
            throw new ArgumentException("The session name is neither the shared name nor an own name of this capture.", nameof(sessionName));
        }
    }

    /// <summary>
    /// The create and the start of one database-scoped session, as one batch. The shared session starts with the database
    /// (<c>STARTUP_STATE = ON</c>, as it always has); an own session does not (<c>OFF</c>), so a database that restarts does
    /// not bring back a fallback the install may have dropped. Refuses a name that is not the shared name or an own name.
    /// </summary>
    public static string BuildAzureCreateSql(
        AlwaysOnXeSessionKind kind, string sessionName, bool withoutUnverifiedActions = false, int? ringBufferMaxEvents = RingBufferTarget.DefaultMaxEvents) =>
        BuildAzureCreateDefinitionSql(kind, sessionName, withoutUnverifiedActions, ringBufferMaxEvents) + "\n" + BuildAzureStartSql(kind, sessionName);

    /// <summary>
    /// The create alone, with no start: what a registration with read-only intent sends over a connection without the intent
    /// (#4961). The definition replicates to the read-only replicas, and the session is started over the registration's own
    /// connection, because run state is per replica. A start here would run the session on the primary only. The same
    /// statement <see cref="BuildAzureCreateSql"/> begins with, and it refuses the same names.
    /// </summary>
    public static string BuildAzureCreateDefinitionSql(
        AlwaysOnXeSessionKind kind, string sessionName, bool withoutUnverifiedActions = false, int? ringBufferMaxEvents = RingBufferTarget.DefaultMaxEvents)
    {
        RequireName(kind, sessionName);
        /* An own deadlock or blocked-process session is a fallback and does not start with the database (OFF). The client-timeout
           session is always own (#9895) and is the install's only one, so it starts with the database as a shared one did. */
        var startsWithDatabase = kind == AlwaysOnXeSessionKind.Attention
            || string.Equals(sessionName, SharedNameFor(kind), StringComparison.Ordinal);
        /* One arm per kind, so a new kind cannot fall into the deadlock arm (#5640). */
        var (eventClause, targets, retention) = kind switch
        {
            AlwaysOnXeSessionKind.Deadlock => (
                "ADD EVENT sqlserver.database_xml_deadlock_report\n",
                RingBufferTarget.Sql(ringBufferMaxEvents) + "\n",
                "    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,\n"),
            AlwaysOnXeSessionKind.BlockedProcess => (
                "ADD EVENT sqlserver.blocked_process_report\n",
                RingBufferTarget.Sql(ringBufferMaxEvents) + "\n",
                string.Empty),
            AlwaysOnXeSessionKind.Attention => (
                AttentionEventClause(azure: true, withoutUnverifiedActions),
                AttentionTargets,
                "    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,\n"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };

        return "\nCREATE EVENT SESSION [" + sessionName + "]\n"
            + "ON DATABASE\n"
            + eventClause
            + targets
            + "WITH\n"
            + "(\n"
            + "    MAX_DISPATCH_LATENCY = 5 SECONDS,\n"
            + retention
            + "    STARTUP_STATE = " + (startsWithDatabase ? "ON" : "OFF") + "\n"
            + ");\n";
    }

    /* The attention event (#5640): five actions, client_app_name, client_hostname, database_name, session_id and sql_text.
       No query_hash, query_plan_hash or plan_handle: all three read 0 on every attention (observed on SQL Server 2019), so
       a hash column would only mislead. The event's own data is duration and request_id. */
    private static readonly string[] AttentionActions =
    {
        "sqlserver.client_app_name",
        "sqlserver.client_hostname",
        "sqlserver.database_name",
        "sqlserver.session_id",
        "sqlserver.sql_text",
    };

    /// <summary>
    /// The attention actions Azure SQL Database REJECTS in a database-scoped session: a CLOSED list the Azure DDL is generated
    /// from, as <see cref="LongQueryCompletionsCollector"/> keeps for #3753. Empty today because no Azure run has proved a
    /// rejection: <c>client_app_name</c>, <c>database_name</c> and <c>session_id</c> are proved in the Azure long-query DDL, and
    /// <c>client_hostname</c> and <c>sql_text</c> are unverified there (the release's Azure SQL Database run checks them). The
    /// engine refuses the whole CREATE with error 25744 for one rejected action, so a rejected action found by that run is one
    /// line added here.
    /// </summary>
    internal static readonly string[] AttentionAzureRejectedActions = Array.Empty<string>();

    /// <summary>
    /// The attention actions no Azure SQL Database run has proved (#5640). <c>client_hostname</c> is proved: Microsoft's own
    /// Azure SQL Database profiler template, <c>Standard_Azure</c> in Azure Data Studio
    /// (src/sql/workbench/contrib/profiler/browser/profiler.contribution.ts), is an <c>ON DATABASE</c> session whose
    /// <c>sqlserver.attention</c> event carries <c>sqlserver.client_hostname</c>, <c>client_app_name</c> and <c>session_id</c>.
    /// <c>sql_text</c> is on no Azure SQL Database session in this repo or in that template, and MS Learn lists no per-action
    /// table (it points to <c>sys.dm_xe_objects</c>). So a create the engine refuses with error 25744 is retried once without
    /// these (<see cref="AlwaysOnXeChange.CreatedWithoutStatementText"/>), and the definition probe accepts a session without them.
    /// </summary>
    internal static readonly string[] AttentionAzureUnverifiedActions = { "sqlserver.sql_text" };

    /// <summary>The engine's refusal of an action a database-scoped session cannot hold: "The action is not available for Azure SQL Database".</summary>
    internal const int AzureActionNotAvailableErrorNumber = 25744;

    private static string AttentionEventClause(bool azure, bool withoutUnverifiedActions = false)
    {
        var actions = new List<string>();
        foreach (var action in AttentionActions)
        {
            if (azure && withoutUnverifiedActions && Array.IndexOf(AttentionAzureUnverifiedActions, action) >= 0)
            {
                continue;
            }

            if (!azure || Array.IndexOf(AttentionAzureRejectedActions, action) < 0)
            {
                actions.Add("        " + action);
            }
        }

        var actionBlock = "    ACTION\n"
            + "    (\n"
            + string.Join(",\n", actions) + "\n"
            + "    )\n";
        var floor = " AND [duration] >= " + CapturedCallMinDurationUs.ToString(CultureInfo.InvariantCulture);
        var abortPredicate = "    WHERE (([result] = " + AbortResultMapKey.ToString(CultureInfo.InvariantCulture) + floor + ")"
            + " OR ([result] = " + ErrorResultMapKey.ToString(CultureInfo.InvariantCulture) + floor + "))\n";

        /* #9507: a real application command timeout raises an attention event that times the cancel (milliseconds), so the
           attention duration says nothing about how long the command ran. The call that was still running when the client
           cancelled ends with an rpc_completed or sql_batch_completed carrying its elapsed duration, its object or statement
           and the same actions. SQL Server 2016 to 2022 end it with result Abort, 2025 with result Error, so the predicate keeps
           an Abort or an Error that ran at least 0.9 s (a command timeout is whole seconds in every driver). A cancel that
           arrives after the command finished raises no such event, and one a data layer issues a few milliseconds into a call
           (a reader closed with rows unread) is left out, so it takes no ring buffer slot from the attention samples (#9538).
           The two completed events keep their statement and batch_text fields OFF (the SET lines). rpc_completed's statement
           carries the call's parameter values, which sensitive-statement withholding does not strip, and the engine copies the
           field on every call of every monitored server before the predicate drops the event. The sql_text action carries the
           text without values, so the collector reads that. */
        return "ADD EVENT sqlserver.attention\n"
            + "(\n"
            + actionBlock
            + "),\n"
            + "ADD EVENT sqlserver.rpc_completed\n"
            + "(\n"
            + "    SET collect_statement = (0)\n"
            + actionBlock
            + abortPredicate
            + "),\n"
            + "ADD EVENT sqlserver.sql_batch_completed\n"
            + "(\n"
            + "    SET collect_batch_text = (0)\n"
            + actionBlock
            + abortPredicate
            + ")\n";
    }

    /// <summary>
    /// The key of <c>Abort</c> in the <c>sys.dm_xe_map_values</c> map that the <c>result</c> field of <c>rpc_completed</c> and of
    /// <c>sql_batch_completed</c> uses (map <c>rpc_return_result</c>: 0 OK, 1 Error, 2 Abort, 3 Skipped). The same on SQL Server
    /// 2016, 2017, 2019, 2022 and 2025 (#9507).
    /// </summary>
    internal const int AbortResultMapKey = 2;

    /// <summary>The key of <c>Error</c> in the same map. SQL Server 2025 ends a client-cancelled call with it (#9507).</summary>
    internal const int ErrorResultMapKey = 1;

    /// <summary>
    /// The shortest elapsed time, in microseconds, of a completed call the session keeps (#9507). A command timeout is whole seconds in
    /// every driver, so a real one runs about 1 s and can be recorded just under it, and the floor keeps ordinary fast failed batches out of
    /// the ring buffer. The value lives in <see cref="PerformanceMonitor.Common.AttentionWindowSql.CapturedCallMinDurationUs"/> so the read of the captured calls
    /// keeps the same floor (#9538).
    /// </summary>
    internal const long CapturedCallMinDurationUs = PerformanceMonitor.Common.AttentionWindowSql.CapturedCallMinDurationUs;

    /* event_counter is the exact count (it never drops an event); the ring buffer is a sample for the detail. 1024 KB is
       Microsoft's tip for a ring buffer and stays under the 4 MB cut-off of the XML a read shreds. */
    private const string AttentionTargets =
        "ADD TARGET package0.event_counter,\n"
        + "ADD TARGET package0.ring_buffer\n"
        + "(\n"
        + "    SET max_memory = 1024\n"
        + ")\n";

    /// <summary>
    /// The create and the start of the client-timeout session on a server, box, VM, managed instance, RDS or a read replica
    /// (#5640): <c>ON SERVER</c>, <c>MEMORY_PARTITION_MODE = NONE</c> (AWS RDS requires it), <c>STARTUP_STATE = ON</c>. Both
    /// products build it here, so there is one copy of this statement; the deadlock and blocked-process statements are still
    /// spelled out by each host. <paramref name="sessionName"/> is this install's own session (<see cref="OwnNameFor"/>, #9895):
    /// the shared name older builds make is refused, so a build never creates the session another install reads. Throws for any
    /// other name.
    /// </summary>
    public static string BuildAttentionServerCreateSql(string sessionName) =>
        "\nCREATE EVENT SESSION [" + RequireOwnAttention(sessionName) + "]\n"
        + "ON SERVER\n"
        + AttentionEventClause(azure: false)
        + AttentionTargets
        + "WITH\n"
        + "(\n"
        + "    MAX_DISPATCH_LATENCY = 5 SECONDS,\n"
        + "    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,\n"
        + "    MEMORY_PARTITION_MODE = NONE,\n"
        + "    STARTUP_STATE = ON\n"
        + ");\n"
        + "\n"
        + BuildAttentionServerStartSql(sessionName);

    /// <summary>The start of the server-scoped client-timeout session (#5640). Own name only, as <see cref="BuildAttentionServerCreateSql"/>.</summary>
    public static string BuildAttentionServerStartSql(string sessionName) =>
        "ALTER EVENT SESSION [" + RequireOwnAttention(sessionName) + "] ON SERVER STATE = START;";

    /// <summary>
    /// The drop of the server-scoped client-timeout session (#5640), for this install's own session with another definition.
    /// Own name only: a session under the shared name belongs to an older install, and is never dropped here (#9895).
    /// </summary>
    public static string BuildAttentionServerDropSql(string sessionName) =>
        "DROP EVENT SESSION [" + RequireOwnAttention(sessionName) + "] ON SERVER;";

    /// <summary>
    /// The drop of this install's own server-scoped client-timeout session when a server is removed (#9895): a no-op where the
    /// session is not there, so a server that never had one is not a failure. Own name only, as <see cref="BuildAttentionServerDropSql"/>.
    /// </summary>
    public static string BuildAttentionServerDropIfExistsSql(string sessionName)
    {
        var name = RequireOwnAttention(sessionName);
        return $@"
IF EXISTS
(
    SELECT
        1/0
    FROM sys.server_event_sessions
    WHERE name = N'{name}'
)
BEGIN
    DROP EVENT SESSION [{name}] ON SERVER;
END;";
    }

    private static string RequireOwnAttention(string sessionName)
    {
        if (!IsOwnName(sessionName, AlwaysOnXeSessionKind.Attention))
        {
            throw new ArgumentException("Only this install's own client-timeout session is created, started or dropped, never the shared one.", nameof(sessionName));
        }

        return sessionName;
    }

    /// <summary>The <see cref="BuildAttentionServerProbeSql"/> answer for a session that exists with another definition (#5640).</summary>
    public const int AttentionDefinitionMismatch = 2;

    /// <summary>
    /// The presence probe for the server-scoped client-timeout session (#5640): no row when the session does not exist, and
    /// <c>session_state</c> 0 (stopped), 1 (running) or <see cref="AttentionDefinitionMismatch"/> (2: it exists with events, actions or
    /// targets other than the ones <see cref="BuildAttentionServerCreateSql"/> makes, which is an older definition of our own
    /// session, since the name is ours). <c>@session_name</c> is this install's own name. <paramref name="appTag"/> is the host's own
    /// comment tag. The first read of an ensure is a dirty read, so it sees a session another caller is still creating, which can
    /// read as another definition (#9675): <paramref name="committedRead"/> asks for the read that waits for that create to commit,
    /// and the confirming read of <see cref="ConfirmAttentionMismatchAsync"/> uses it.
    /// </summary>
    public static string BuildAttentionServerProbeSql(string appTag, bool committedRead = false) => @"
SET TRANSACTION ISOLATION LEVEL " + (committedRead ? "READ COMMITTED" : "READ UNCOMMITTED") + @";

SELECT /* " + appTag + @" */
    session_state =
        CASE
            WHEN " + AttentionDefinitionMatchPredicate(azure: false, "ses") + @"
            THEN CASE WHEN dxs.name IS NOT NULL THEN 1 ELSE 0 END
            ELSE " + AttentionDefinitionMismatch.ToString(CultureInfo.InvariantCulture) + @"
        END
FROM sys.server_event_sessions AS ses
LEFT JOIN sys.dm_xe_sessions AS dxs
  ON dxs.name = ses.name
WHERE ses.name = @session_name;";

    /// <summary>How long a probe that read another definition waits before it reads again (#9675): the create of the session is a few milliseconds of catalog writes.</summary>
    public static readonly TimeSpan AttentionMismatchConfirmPause = TimeSpan.FromSeconds(2);

    /// <summary>
    /// #9675: the step that goes between the first probe of the server-scoped client-timeout session and the drop of a session that
    /// probe called another definition. Several entries on one instance run the ensure at the same moment, and the first probe is a
    /// dirty read: while one entry's CREATE EVENT SESSION is still writing the session's events, actions and targets, another
    /// entry's probe can read the half-written definition as another one, and its drop then removes the session the first entry is
    /// about to start. So an answer of <see cref="AttentionDefinitionMismatch"/> is read again, after <paramref name="pause"/>
    /// and with <paramref name="probeAgain"/> (the committed read), and that second answer is the one the ensure acts on. Any
    /// other first answer is returned as it is, with no second read.
    /// </summary>
    public static async Task<object?> ConfirmAttentionMismatchAsync(
        object? probe, Func<CancellationToken, Task<object?>> probeAgain, TimeSpan pause, CancellationToken cancellationToken)
    {
        if (!(probe is int mismatch && mismatch == AttentionDefinitionMismatch))
        {
            return probe;
        }

        if (pause > TimeSpan.Zero)
        {
            await Task.Delay(pause, cancellationToken).ConfigureAwait(false);
        }

        return await probeAgain(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// #9675: runs the drop of a session of another definition. When the drop fails because another entry on the same instance
    /// dropped it first (both read the same older definition, which is every upgrade with two entries on one instance), the
    /// session is read again: one that is gone, or that now has this version's definition, is what the drop was for, so the
    /// failure is not one. A session that is still another definition rethrows the drop's own failure.
    /// </summary>
    public static async Task DropAttentionMismatchAsync(
        Func<CancellationToken, Task> drop, Func<CancellationToken, Task<object?>> probeAgain, CancellationToken cancellationToken)
    {
        try
        {
            await drop(cancellationToken).ConfigureAwait(false);
        }
        catch (System.Data.Common.DbException)
        {
            var now = await probeAgain(cancellationToken).ConfigureAwait(false);
            if (now is int mismatch && mismatch == AttentionDefinitionMismatch)
            {
                throw;
            }
        }
    }

    /// <summary>
    /// The T-SQL condition that a session's definition is the client-timeout one (#5640): the <c>attention</c> event and the two completed events that carry a predicate (<c>rpc_completed</c>, <c>sql_batch_completed</c>, #9507) and no other event, every
    /// action the builder adds to them and none other, and the <c>event_counter</c> and <c>ring_buffer</c> targets and no other.
    /// On Azure SQL Database the actions Azure rejects are not required and the unverified ones are optional, because a create the
    /// engine refused is retried without them. Built from the same lists as the DDL, so the two cannot drift.
    /// </summary>
    private static string AttentionDefinitionMatchPredicate(bool azure, string sessionAlias)
    {
        var prefix = azure ? "sys.database_event_session" : "sys.server_event_session";
        var key = sessionAlias + ".event_session_id";
        var allowed = new List<string>();
        var required = new List<string>();
        foreach (var action in AttentionActions)
        {
            if (azure && Array.IndexOf(AttentionAzureRejectedActions, action) >= 0)
            {
                continue;
            }

            var bare = "N'" + action.Substring(action.IndexOf('.') + 1) + "'";
            allowed.Add(bare);
            if (!azure || Array.IndexOf(AttentionAzureUnverifiedActions, action) < 0)
            {
                required.Add(bare);
            }
        }

        var allowedList = string.Join(", ", allowed);
        var requiredList = string.Join(", ", required);

        /* #9538: the engine stores the predicate text with the floor as written, so a completed event whose predicate holds the floor
           fewer than twice (once per arm) is a session an older build made, with the floor on the Error arm only. It reads as another
           definition, so the host drops it and creates it again. */
        var floorText = CapturedCallMinDurationUs.ToString(CultureInfo.InvariantCulture);

        return @"
            (
                (
                    SELECT
                        COUNT_BIG(DISTINCT e.name)
                    FROM " + prefix + @"_events AS e
                    WHERE e.event_session_id = " + key + @"
                    AND   e.name IN (N'attention', N'rpc_completed', N'sql_batch_completed')
                ) = 3
                AND NOT EXISTS
                (
                    SELECT
                        1/0
                    FROM " + prefix + @"_events AS e
                    WHERE e.event_session_id = " + key + @"
                    AND   e.name NOT IN (N'attention', N'rpc_completed', N'sql_batch_completed')
                )
                AND NOT EXISTS
                (
                    SELECT
                        1/0
                    FROM " + prefix + @"_events AS e
                    WHERE e.event_session_id = " + key + @"
                    AND   e.name IN (N'rpc_completed', N'sql_batch_completed')
                    AND   (e.predicate IS NULL OR (LEN(e.predicate) - LEN(REPLACE(e.predicate, N'" + floorText + @"', N''))) < " + (2 * floorText.Length).ToString(CultureInfo.InvariantCulture) + @")
                )
                AND
                (
                    SELECT
                        COUNT_BIG(DISTINCT a.name)
                    FROM " + prefix + @"_actions AS a
                    WHERE a.event_session_id = " + key + @"
                    AND   a.name IN (" + requiredList + @")
                ) = " + required.Count.ToString(CultureInfo.InvariantCulture) + @"
                AND NOT EXISTS
                (
                    SELECT
                        1/0
                    FROM " + prefix + @"_actions AS a
                    WHERE a.event_session_id = " + key + @"
                    AND   a.name NOT IN (" + allowedList + @")
                )
                AND EXISTS
                (
                    SELECT
                        1/0
                    FROM " + prefix + @"_targets AS t
                    WHERE t.event_session_id = " + key + @"
                    AND   t.name = N'event_counter'
                )
                AND EXISTS
                (
                    SELECT
                        1/0
                    FROM " + prefix + @"_targets AS t
                    WHERE t.event_session_id = " + key + @"
                    AND   t.name = N'ring_buffer'
                )
                AND NOT EXISTS
                (
                    SELECT
                        1/0
                    FROM " + prefix + @"_targets AS t
                    WHERE t.event_session_id = " + key + @"
                    AND   t.name NOT IN (N'event_counter', N'ring_buffer')
                )
            )";
    }

    /// <summary>The start of one session by name. Refuses a name that is not the shared name or an own name.</summary>
    public static string BuildAzureStartSql(AlwaysOnXeSessionKind kind, string sessionName)
    {
        RequireName(kind, sessionName);
        return "ALTER EVENT SESSION [" + sessionName + "] ON DATABASE STATE = START;";
    }

    /// <summary>
    /// The stop of one session by name, for a session that runs on a read-only replica: it is stopped over a connection to
    /// the replica before it is dropped on the primary (#4961). Takes an own name only, as <see cref="BuildAzureDropSql"/>
    /// does. Throws for any other name.
    /// </summary>
    public static string BuildAzureStopSql(AlwaysOnXeSessionKind kind, string sessionName)
    {
        if (!IsOwnName(sessionName, kind))
        {
            throw new ArgumentException("Only an own session is stopped for a drop, never the shared one.", nameof(sessionName));
        }

        return "ALTER EVENT SESSION [" + sessionName + "] ON DATABASE STATE = STOP;";
    }

    /// <summary>
    /// The drop of one session. Takes an own name only: the shared session is not dropped by this builder, because another
    /// install may be reading it. Throws for any other name.
    /// </summary>
    public static string BuildAzureDropSql(AlwaysOnXeSessionKind kind, string sessionName)
    {
        if (!IsOwnName(sessionName, kind))
        {
            throw new ArgumentException("Only an own session is dropped, never the shared one.", nameof(sessionName));
        }

        return "DROP EVENT SESSION [" + sessionName + "] ON DATABASE;";
    }

    /// <summary>
    /// The catalog probe: one scalar, the <see cref="AlwaysOnXeCatalog"/> value as an integer, for the session named by
    /// <c>@session_name</c>. A deadlock session counts as present only with <c>database_xml_deadlock_report</c>.
    /// <paramref name="appTag"/> is the host's own comment tag.
    /// </summary>
    public static string BuildAzureCatalogProbeSql(AlwaysOnXeSessionKind kind, string appTag)
    {
        /* One arm per kind (#5640). A deadlock session counts as present only with its event; the others by name. */
        switch (kind)
        {
            case AlwaysOnXeSessionKind.Deadlock:
                return @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* " + appTag + @" */
    catalog_state =
        CASE
            WHEN EXISTS
            (
                SELECT
                    1/0
                FROM sys.database_event_session_events AS dese
                JOIN sys.database_event_sessions AS des
                  ON des.event_session_id = dese.event_session_id
                WHERE des.name = @session_name
                AND   dese.name = N'database_xml_deadlock_report'
            )
            THEN 2
            WHEN EXISTS
            (
                SELECT
                    1/0
                FROM sys.database_event_sessions AS des
                WHERE des.name = @session_name
            )
            THEN 1
            ELSE 0
        END;";
            case AlwaysOnXeSessionKind.Attention:
                /* #5640: present (2) only with the client-timeout definition; another definition under our name is 1. The Azure ensure
                   drops and creates again this install's own session (#9507, #9895), and leaves every shared session alone. */
                return @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* " + appTag + @" */
    catalog_state =
        CASE
            WHEN NOT EXISTS
            (
                SELECT
                    1/0
                FROM sys.database_event_sessions AS des
                WHERE des.name = @session_name
            )
            THEN 0
            WHEN EXISTS
            (
                SELECT
                    1/0
                FROM sys.database_event_sessions AS des
                WHERE des.name = @session_name
                AND   " + AttentionDefinitionMatchPredicate(azure: true, "des") + @"
            )
            THEN 2
            ELSE 1
        END;";
            case AlwaysOnXeSessionKind.BlockedProcess:
                return @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* " + appTag + @" */
    catalog_state =
        CASE
            WHEN EXISTS
            (
                SELECT
                    1/0
                FROM sys.database_event_sessions AS des
                WHERE des.name = @session_name
            )
            THEN 2
            ELSE 0
        END;";
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    /// <summary>
    /// The started probe: one scalar, 1 when the session named by <c>@session_name</c> has a row in
    /// <c>sys.dm_xe_database_sessions</c>, the DMV the ring-buffer read joins, and 0 when it does not.
    /// </summary>
    public static string BuildAzureStartedProbeSql(string appTag) => @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* " + appTag + @" */
    is_visible =
        CASE
            WHEN EXISTS
            (
                SELECT
                    1/0
                FROM sys.dm_xe_database_sessions AS xes
                WHERE xes.name = @session_name
            )
            THEN 1
            ELSE 0
        END;";

    /// <summary>
    /// The one message for a create that a read-only database refused (#4961): why it cannot work, and what to change. It
    /// words the reason as <see cref="LongQueryTraceDatabases.ReadOnlyDatabaseMessage"/> does, for a session that has no
    /// trace to turn off. A registration that points straight at such a database, as at an Azure geo-secondary, lands there
    /// without read-only intent.
    /// </summary>
    public static string ReadOnlyDatabaseMessage(AlwaysOnXeSessionKind kind)
    {
        /* A block body with the switch in a local, not one expression-bodied concatenation: the member scan closes an
           expression-bodied member that opens with a braced switch at the switch's own brace and strands the literals after it. */
        var sessionLabel = kind switch
        {
            AlwaysOnXeSessionKind.Deadlock => "deadlock",
            AlwaysOnXeSessionKind.BlockedProcess => "blocked process",
            AlwaysOnXeSessionKind.Attention => "client timeout",
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };

        return "The " + sessionLabel + " Extended Events session could not be created: "
            + "the database this registration reaches is read-only (error "
            + LongQueryTraceDatabases.ReadOnlyDatabaseErrorNumber.ToString(CultureInfo.InvariantCulture) + "), "
            + "as an Azure geo-secondary is, and a read-only database cannot hold a session. Register the primary database instead.";
    }

    /// <summary>
    /// Marks a failure of a create or start on Azure SQL Database, so <see cref="DescribeFailure"/> adds the caps sentence.
    /// A read-only database's refusal (error <see cref="LongQueryTraceDatabases.ReadOnlyDatabaseErrorNumber"/>) is never marked:
    /// that database refuses every create, whatever its caps, and the long-query trace words that refusal itself
    /// (<see cref="LongQueryTraceDatabases.ReadOnlyDatabaseMessage"/>). The error numbers are the host's to read off its own
    /// exception, because this project has no SqlClient; none means the exception carries no SQL error.
    /// </summary>
    /// <param name="exception">The failure of the create or start.</param>
    /// <param name="errorNumbers">The numbers of the errors the failure carries, or null when the host reads none.</param>
    public static void MarkAzureCapsFailure(Exception exception, IEnumerable<int>? errorNumbers = null)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (errorNumbers is not null && LongQueryTraceDatabases.IsReadOnlyDatabaseRefusal(errorNumbers))
        {
            return;
        }

        exception.Data[CapsKey] = true;
    }

    /// <summary>True when <see cref="MarkAzureCapsFailure"/> marked the exception.</summary>
    public static bool CarriesAzureCaps(Exception? exception) =>
        exception is not null && exception.Data.Contains(CapsKey);

    /// <summary>The exception's message, with <see cref="AzureCapsSentence"/> after it when it is a marked create or start failure.</summary>
    public static string DescribeFailure(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return CarriesAzureCaps(exception)
            ? string.Create(CultureInfo.InvariantCulture, $"{exception.Message} {AzureCapsSentence}")
            : exception.Message;
    }
}

/// <summary>
/// The in-memory choice of one host: which session of each capture it reads in each database of each registration. The
/// ensure sets it and the read takes its name from it. Nothing is stored: a restart starts every entry at shared, and the
/// first ensure sets it again.
/// </summary>
public sealed class AlwaysOnXeChoices
{
    private readonly ConcurrentDictionary<string, AlwaysOnXeChoice> _choices = new(StringComparer.Ordinal);

    private static string KeyOf(string serverKey, string database, AlwaysOnXeSessionKind kind) =>
        serverKey + "\u0001" + database.ToUpperInvariant() + "\u0001" + kind.ToString();

    /// <summary>The choice for one database; <see cref="AlwaysOnXeChoice.Shared"/> while the ensure has set none.</summary>
    public AlwaysOnXeChoice Get(string serverKey, string database, AlwaysOnXeSessionKind kind) =>
        _choices.TryGetValue(KeyOf(serverKey, database, kind), out var choice) ? choice : AlwaysOnXeChoice.Shared;

    public void Set(string serverKey, string database, AlwaysOnXeSessionKind kind, AlwaysOnXeChoice choice) =>
        _choices[KeyOf(serverKey, database, kind)] = choice;

    /* The databases where a create was refused as read-only (#4961), so the host logs the explanation once and the repeats
       quietly. In memory, like the choices. */
    private readonly ConcurrentDictionary<string, bool> _readOnlyRefusals = new(StringComparer.Ordinal);

    /// <summary>Records a create refused as read-only in one database. True the first time since the last success, so the host logs the explanation then.</summary>
    public bool MarkReadOnlyRefusal(string serverKey, string database, AlwaysOnXeSessionKind kind) =>
        _readOnlyRefusals.TryAdd(KeyOf(serverKey, database, kind), true);

    /// <summary>Forgets the refusal of one database, after a pass that did not hit it.</summary>
    public void ClearReadOnlyRefusal(string serverKey, string database, AlwaysOnXeSessionKind kind) =>
        _readOnlyRefusals.TryRemove(KeyOf(serverKey, database, kind), out _);

    /// <summary>
    /// The session a read of this capture names in this database: the install's own when the ensure chose it and the host has
    /// an own name, else the shared name.
    /// </summary>
    public string NameFor(string serverKey, string database, AlwaysOnXeSessionKind kind, string? ownName) =>
        kind == AlwaysOnXeSessionKind.Attention
            ? AlwaysOnXeSessions.ReadNameFor(kind, ownName)
            : Get(serverKey, database, kind) == AlwaysOnXeChoice.Own && ownName is not null
            ? AlwaysOnXeSessions.ReadNameFor(kind, ownName)
            : AlwaysOnXeSessions.SharedNameFor(kind);

    /// <summary>The databases of one registration where the install reads its own session of this capture.</summary>
    public IReadOnlyList<string> OwnDatabases(string serverKey, AlwaysOnXeSessionKind kind)
    {
        var prefix = serverKey + "\u0001";
        var suffix = "\u0001" + kind.ToString();
        var found = new List<string>();
        foreach (var entry in _choices)
        {
            if (entry.Value == AlwaysOnXeChoice.Own
                && entry.Key.StartsWith(prefix, StringComparison.Ordinal)
                && entry.Key.EndsWith(suffix, StringComparison.Ordinal))
            {
                found.Add(entry.Key.Substring(prefix.Length, entry.Key.Length - prefix.Length - suffix.Length));
            }
        }

        return found;
    }

    /// <summary>Forgets every entry of one registration (the registration is gone, or reconnected).</summary>
    public void Forget(string serverKey)
    {
        var prefix = serverKey + "\u0001";
        foreach (var key in _choices.Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
            {
                _choices.TryRemove(key, out _);
            }
        }

        foreach (var key in _readOnlyRefusals.Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
            {
                _readOnlyRefusals.TryRemove(key, out _);
            }
        }
    }
}

/// <summary>
/// One database's pass of the Azure ensure for the deadlock or blocked-process session (#4961), as one set of decisions for
/// both hosts.
///
/// <para>With the shared session as the choice: the catalog is read. A deadlock session without the deadlock event is
/// unusable, and is not dropped. A row that is not running gets a start by name, and the DMV is read again. No row gets the
/// create, and a create that collides after the catalog missed the session (the catalog is scoped to the login) is read
/// back the same way: started or not, and a start by name if not. The DMV lists started sessions only, so a stopped shared
/// session looks invisible until it is started, and the start also covers a create that raced another install's start. Only
/// a session still not running after that is unusable, and then the install creates and starts its own and reads that.</para>
///
/// <para>With its own session as the choice: the shared session is probed first, the same way. Usable, the install drops its
/// own (its own id, so it may) and reads the shared one again; gone, the install creates it under this login as before, then
/// switches back. A shared session that stays unusable keeps the fallback, and only the fallback is ensured.</para>
///
/// <para>A create or a start that fails is marked (<see cref="AlwaysOnXeSessions.MarkAzureCapsFailure"/>) and thrown as it
/// came, so the host classifies it as before and its message carries <see cref="AlwaysOnXeSessions.AzureCapsSentence"/>.</para>
/// </summary>
public static class AlwaysOnXeAzureEnsure
{
    /// <summary>
    /// The fault a pass throws when the shared session cannot be used and the install has no id to name a session of its own.
    /// A host whose id store says why it has no id right now (<paramref name="idFailure"/>: the id file cannot be read, or a
    /// new id cannot be saved) gets that cause and the retry in the message, in the long-query fault's words; a host with
    /// no id store has nothing to retry and gets the plain sentence.
    /// </summary>
    public static string NoOwnSessionMessage(AlwaysOnXeSessionKind kind, string? idFailure)
    {
        var shared = AlwaysOnXeSessions.SharedNameFor(kind);
        return idFailure is { } failure
            ? $"The shared {shared} session cannot be used in this database, and this install has no id for now to name a session of its own, because {failure}. The next cycle tries again."
            : $"The shared {shared} session cannot be used in this database, and this install has no id to name a session of its own.";
    }

    /// <summary>
    /// One pass of the ensure in one database. <paramref name="ownName"/> is null for a host with no install id, and
    /// <paramref name="idFailure"/> is why, when the host's id store knows (see <see cref="NoOwnSessionMessage"/>).
    /// The client-timeout kind has no shared session (#9895): it is ensured as this install's own session and never falls back to
    /// the shared one an older install may read, so it throws when the host has no own name.
    /// </summary>
    public static async Task<AlwaysOnXeAzureResult> RunAsync(
        IAlwaysOnXeDatabase database,
        AlwaysOnXeSessionKind kind,
        string? ownName,
        AlwaysOnXeChoice current,
        CancellationToken cancellationToken,
        string? idFailure = null,
        int ringBufferMaxEvents = RingBufferTarget.DefaultMaxEvents)
    {
        ArgumentNullException.ThrowIfNull(database);

        if (kind == AlwaysOnXeSessionKind.Attention)
        {
            if (ownName is null)
            {
                throw new InvalidOperationException(idFailure is { } failure
                    ? $"{AlwaysOnXeSessions.NoOwnAttentionSessionMessage} The id store says: {failure}. The next cycle tries again."
                    : AlwaysOnXeSessions.NoOwnAttentionSessionMessage);
            }

            var attentionChange = await EnsureOwnAsync(database, kind, ownName, ringBufferMaxEvents, cancellationToken);
            return new AlwaysOnXeAzureResult(AlwaysOnXeChoice.Own, attentionChange);
        }

        if (current == AlwaysOnXeChoice.Own && ownName is not null)
        {
            (bool Usable, AlwaysOnXeChange Change) shared;
            try
            {
                shared = await TrySharedAsync(database, kind, ringBufferMaxEvents, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* The fallback is working, so a shared session that cannot be created or started leaves it in place. */
                shared = (false, AlwaysOnXeChange.None);
            }

            if (shared.Usable)
            {
                await DropOwnAsync(database, kind, ownName, cancellationToken);
                return new AlwaysOnXeAzureResult(AlwaysOnXeChoice.Shared, AlwaysOnXeChange.SwitchedBack);
            }

            var ownChange = await EnsureOwnAsync(database, kind, ownName, ringBufferMaxEvents, cancellationToken);
            return new AlwaysOnXeAzureResult(AlwaysOnXeChoice.Own, ownChange);
        }

        var first = await TrySharedAsync(database, kind, ringBufferMaxEvents, cancellationToken);
        if (first.Usable)
        {
            return new AlwaysOnXeAzureResult(AlwaysOnXeChoice.Shared, first.Change);
        }

        if (ownName is null)
        {
            throw new InvalidOperationException(NoOwnSessionMessage(kind, idFailure));
        }

        await EnsureOwnAsync(database, kind, ownName, ringBufferMaxEvents, cancellationToken);
        return new AlwaysOnXeAzureResult(AlwaysOnXeChoice.Own, AlwaysOnXeChange.FellBack);
    }

    private static async Task<(bool Usable, AlwaysOnXeChange Change)> TrySharedAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, int ringBufferMaxEvents, CancellationToken cancellationToken)
    {
        var shared = AlwaysOnXeSessions.SharedNameFor(kind);
        var catalog = await database.ReadCatalogAsync(kind, shared, cancellationToken);

        switch (catalog)
        {
            case AlwaysOnXeCatalog.WrongEvent:
                /* Not dropped: another install may be reading it. This install reads its own instead. */
                return (false, AlwaysOnXeChange.None);

            case AlwaysOnXeCatalog.Present:
                if (await database.IsStartedAsync(shared, cancellationToken))
                {
                    return (true, AlwaysOnXeChange.None);
                }

                return await StartAndReadBackAsync(database, kind, shared, cancellationToken);

            default:
                return await CreateSharedAsync(database, kind, shared, ringBufferMaxEvents, cancellationToken);
        }
    }

    private static async Task<(bool Usable, AlwaysOnXeChange Change)> CreateSharedAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, string shared, int ringBufferMaxEvents, CancellationToken cancellationToken)
    {
        try
        {
            var withoutStatementText = await CreateAsync(database, kind, shared, ringBufferMaxEvents, cancellationToken);
            return (true, withoutStatementText ? AlwaysOnXeChange.CreatedWithoutStatementText : AlwaysOnXeChange.Created);
        }
        catch (Exception ex) when (database.IsAlreadyPresent(ex))
        {
            /* The catalog missed a session the engine has: another install's, or one this login cannot list. */
            if (await database.IsStartedAsync(shared, cancellationToken))
            {
                return (true, AlwaysOnXeChange.None);
            }

            return await StartAndReadBackAsync(database, kind, shared, cancellationToken);
        }
    }

    private static async Task<(bool Usable, AlwaysOnXeChange Change)> StartAndReadBackAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken)
    {
        await StartByNameAsync(database, kind, sessionName, cancellationToken);
        return await database.IsStartedAsync(sessionName, cancellationToken)
            ? (true, AlwaysOnXeChange.Started)
            : (false, AlwaysOnXeChange.None);
    }

    private static async Task<AlwaysOnXeChange> EnsureOwnAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, string ownName, int ringBufferMaxEvents, CancellationToken cancellationToken)
    {
        var catalog = await database.ReadCatalogAsync(kind, ownName, cancellationToken);
        if (catalog == AlwaysOnXeCatalog.WrongEvent)
        {
            /* #9507: the own name is this install's to drop. A session under it with another definition is one an older version of
               this install made (the attention session before it gained its completed events), and starting it would read it forever
               without the rows the current definition captures. Dropped and created again with the current definition. */
            await DropOwnSessionAsync(database, kind, ownName, cancellationToken);
            return await CreateOwnAsync(database, kind, ownName, ringBufferMaxEvents, cancellationToken);
        }

        if (catalog != AlwaysOnXeCatalog.Missing)
        {
            if (await database.IsStartedAsync(ownName, cancellationToken))
            {
                return AlwaysOnXeChange.None;
            }

            await StartByNameAsync(database, kind, ownName, cancellationToken);
            return AlwaysOnXeChange.Started;
        }

        return await CreateOwnAsync(database, kind, ownName, ringBufferMaxEvents, cancellationToken);
    }

    private static async Task<AlwaysOnXeChange> CreateOwnAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, string ownName, int ringBufferMaxEvents, CancellationToken cancellationToken)
    {
        try
        {
            var withoutStatementText = await CreateAsync(database, kind, ownName, ringBufferMaxEvents, cancellationToken);
            return withoutStatementText ? AlwaysOnXeChange.CreatedWithoutStatementText : AlwaysOnXeChange.Created;
        }
        catch (Exception ex) when (database.IsAlreadyPresent(ex))
        {
            await StartByNameAsync(database, kind, ownName, cancellationToken);
            return AlwaysOnXeChange.Started;
        }
    }

    private static async Task DropOwnAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, string ownName, CancellationToken cancellationToken)
    {
        /* An own session the catalog does not list is not there to drop. */
        if (await database.ReadCatalogAsync(kind, ownName, cancellationToken) == AlwaysOnXeCatalog.Missing)
        {
            return;
        }

        await DropOwnSessionAsync(database, kind, ownName, cancellationToken);
    }

    /// <summary>
    /// Drops this install's own session in one database, in the order the engine documents for a session that runs on a
    /// read-only replica (#4961): stopped over the registration's own connection, only when it runs there, then dropped over a
    /// connection without the read-only intent. A registration without the intent drops it over its own connection in one
    /// statement, as before. Throws for a name that is not an own name, as the drop builder does. Used by the switch back and
    /// by a removed server's drop, so the order exists once.
    /// </summary>
    public static async Task DropOwnSessionAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, string ownName, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(database);

        await DropByStatementsAsync(
            database, ownName, AlwaysOnXeSessions.BuildAzureStopSql(kind, ownName), AlwaysOnXeSessions.BuildAzureDropSql(kind, ownName), cancellationToken);
    }

    /// <summary>The drop's order for one session, with its statements already built (so a name the builders refuse never gets here).</summary>
    private static async Task DropByStatementsAsync(
        IAlwaysOnXeDatabase database, string sessionName, string stop, string drop, CancellationToken cancellationToken)
    {
        if (!database.ReadOnlyIntent)
        {
            await database.ExecuteAsync(drop, cancellationToken);
            return;
        }

        /* Hyperscale limit (#4961): with several high-availability replicas, a read-only connection lands on one replica the app
           cannot choose, so this stop reaches only that one, and a copy of the session that runs on another replica is not stopped.
           Microsoft Learn describes no way to address one HA replica; it says the read-intent workload is "distributed arbitrarily
           across all available HA replicas": https://learn.microsoft.com/azure/azure-sql/database/service-tier-hyperscale-replicas#connect-to-an-ha-replica */
        if (await database.IsStartedAsync(sessionName, cancellationToken))
        {
            await database.ExecuteAsync(stop, cancellationToken);
        }

        await database.ExecuteWithoutReadOnlyIntentAsync(drop, cancellationToken);
    }

    /// <summary>
    /// Creates one session and starts it. A registration without read-only intent sends the create and the start as one batch
    /// over its own connection, as before. One with the intent sends the definition over a connection without it, then starts
    /// the session over its own connection, by name: the definition replicates to the replica, and run state is per replica
    /// (#4961). A failure of either is marked for the caps sentence, unless it is the engine's "already there".
    /// </summary>
    private static async Task<bool> CreateAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, string sessionName, int ringBufferMaxEvents, CancellationToken cancellationToken)
    {
        try
        {
            try
            {
                await CreateOnceAsync(database, kind, sessionName, withoutUnverifiedActions: false, ringBufferMaxEvents, cancellationToken);
                return false;
            }
            catch (Exception refused) when (refused is not OperationCanceledException
                && kind is AlwaysOnXeSessionKind.Deadlock or AlwaysOnXeSessionKind.BlockedProcess
                && database.ErrorNumbers(refused).Any(RingBufferTarget.OptionRefusedErrorNumbers.Contains))
            {
                /* #8146: the engine refused max_events_limit (RingBufferTarget.OptionRefusedErrorNumbers); a session at the default limit
                   is better than none, and the loss note shows the session's actual limit. One retry without it; if that fails too, its
                   exception surfaces. Any other failure, "already there" included, surfaces as before #8146. */
                try
                {
                    await CreateOnceAsync(database, kind, sessionName, withoutUnverifiedActions: false, ringBufferMaxEvents: null, cancellationToken);
                }
                catch (Exception retry) when (retry is not OperationCanceledException && database.IsAlreadyPresent(retry))
                {
                    /* The create and the start can go as one batch, so the refused attempt may have left a session it never started.
                       The callers take "already there" as a session that is up, so the refusal surfaces instead and a session that did
                       not start is not hidden. */
                    ExceptionDispatchInfo.Capture(refused).Throw();
                }

                return false;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException
            && kind == AlwaysOnXeSessionKind.Attention
            && AlwaysOnXeSessions.AttentionAzureUnverifiedActions.Length > 0
            && database.ErrorNumbers(ex).Contains(AlwaysOnXeSessions.AzureActionNotAvailableErrorNumber))
        {
            /* #5640: an action the engine has not been proved to accept was refused (error 25744 refuses the whole create). The
               session is more use without statement text than not at all: retry once without the unverified actions. */
            await CreateOnceAsync(database, kind, sessionName, withoutUnverifiedActions: true, ringBufferMaxEvents, cancellationToken);
            return true;
        }
    }

    private static async Task CreateOnceAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, string sessionName, bool withoutUnverifiedActions, int? ringBufferMaxEvents, CancellationToken cancellationToken)
    {
        if (!database.ReadOnlyIntent)
        {
            await ExecuteDdlAsync(database, AlwaysOnXeSessions.BuildAzureCreateSql(kind, sessionName, withoutUnverifiedActions, ringBufferMaxEvents), cancellationToken);
            return;
        }

        await ExecuteDdlAsync(
            database, AlwaysOnXeSessions.BuildAzureCreateDefinitionSql(kind, sessionName, withoutUnverifiedActions, ringBufferMaxEvents), cancellationToken, withoutReadOnlyIntent: true);
        await StartByNameAsync(database, kind, sessionName, cancellationToken);
    }

    private static async Task StartByNameAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken)
    {
        try
        {
            await ExecuteDdlAsync(database, AlwaysOnXeSessions.BuildAzureStartSql(kind, sessionName), cancellationToken);
        }
        catch (Exception ex) when (database.IsAlreadyPresent(ex))
        {
            /* 25705: already started, which is what the start was for. */
        }
    }

    /// <summary>
    /// Runs a create or a start. A failure that is not the engine's "already there" is marked for the caps sentence, unless
    /// it is a read-only database's refusal (<see cref="AlwaysOnXeSessions.MarkAzureCapsFailure"/>).
    /// </summary>
    private static async Task ExecuteDdlAsync(
        IAlwaysOnXeDatabase database, string statement, CancellationToken cancellationToken, bool withoutReadOnlyIntent = false)
    {
        try
        {
            if (withoutReadOnlyIntent)
            {
                await database.ExecuteWithoutReadOnlyIntentAsync(statement, cancellationToken);
            }
            else
            {
                await database.ExecuteAsync(statement, cancellationToken);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException && !database.IsAlreadyPresent(ex))
        {
            AlwaysOnXeSessions.MarkAzureCapsFailure(ex, database.ErrorNumbers(ex));
            throw;
        }
    }
}
