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
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>Which of the two always-on Extended Events sessions a statement, a name or a choice is about (#4961).</summary>
public enum AlwaysOnXeSessionKind
{
    Deadlock,
    BlockedProcess,
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

    /// <summary>A row, but not with the event a deadlock read needs. Never reported for blocked-process sessions.</summary>
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
/// <para><b>Nothing here drops a session it did not make.</b> The only drop builder takes an own name, and refuses any other.
/// The shared session is never dropped, because another install may be reading it.</para>
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
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>The capture's own suffix, cut from the shared name: <c>Deadlock</c> or <c>BlockedProcess</c>.</summary>
    private static string SuffixFor(AlwaysOnXeSessionKind kind) => SharedNameFor(kind).Substring(Prefix.Length);

    /// <summary>
    /// This install's own session of the capture: <c>PerformanceMonitor_{product}_{id}_{Deadlock|BlockedProcess}</c>. Throws
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

    private static void RequireName(AlwaysOnXeSessionKind kind, string sessionName)
    {
        if (!string.Equals(sessionName, SharedNameFor(kind), StringComparison.Ordinal) && !IsOwnName(sessionName, kind))
        {
            throw new ArgumentException("The session name is neither the shared name nor an own name of this capture.", nameof(sessionName));
        }
    }

    /// <summary>
    /// The create and the start of one database-scoped session, as one batch. The shared session starts with the database
    /// (<c>STARTUP_STATE = ON</c>, as it always has); an own session does not (<c>OFF</c>), so a database that restarts does
    /// not bring back a fallback the install may have dropped. Refuses a name that is not the shared name or an own name.
    /// </summary>
    public static string BuildAzureCreateSql(AlwaysOnXeSessionKind kind, string sessionName) =>
        BuildAzureCreateDefinitionSql(kind, sessionName) + "\n" + BuildAzureStartSql(kind, sessionName);

    /// <summary>
    /// The create alone, with no start: what a registration with read-only intent sends over a connection without the intent
    /// (#4961). The definition replicates to the read-only replicas, and the session is started over the registration's own
    /// connection, because run state is per replica. A start here would run the session on the primary only. The same
    /// statement <see cref="BuildAzureCreateSql"/> begins with, and it refuses the same names.
    /// </summary>
    public static string BuildAzureCreateDefinitionSql(AlwaysOnXeSessionKind kind, string sessionName)
    {
        RequireName(kind, sessionName);
        var shared = string.Equals(sessionName, SharedNameFor(kind), StringComparison.Ordinal);
        var eventName = kind == AlwaysOnXeSessionKind.Deadlock
            ? "sqlserver.database_xml_deadlock_report"
            : "sqlserver.blocked_process_report";
        var retention = kind == AlwaysOnXeSessionKind.Deadlock
            ? "    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,\n"
            : string.Empty;

        return "\nCREATE EVENT SESSION [" + sessionName + "]\n"
            + "ON DATABASE\n"
            + "ADD EVENT " + eventName + "\n"
            + "ADD TARGET package0.ring_buffer\n"
            + "(\n"
            + "    SET max_memory = 4096\n"
            + ")\n"
            + "WITH\n"
            + "(\n"
            + "    MAX_DISPATCH_LATENCY = 5 SECONDS,\n"
            + retention
            + "    STARTUP_STATE = " + (shared ? "ON" : "OFF") + "\n"
            + ");\n";
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
    /// The drop of one session. Takes an own name only: the shared session is never dropped by an install, because another
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
        if (kind == AlwaysOnXeSessionKind.Deadlock)
        {
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
        }

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
    public static string ReadOnlyDatabaseMessage(AlwaysOnXeSessionKind kind) =>
        "The " + (kind == AlwaysOnXeSessionKind.Deadlock ? "deadlock" : "blocked process") + " Extended Events session could not be created: "
        + "the database this registration reaches is read-only (error "
        + LongQueryTraceDatabases.ReadOnlyDatabaseErrorNumber.ToString(CultureInfo.InvariantCulture) + "), "
        + "as an Azure geo-secondary is, and a read-only database cannot hold a session. Register the primary database instead.";

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
        Get(serverKey, database, kind) == AlwaysOnXeChoice.Own && ownName is not null
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
    /// <summary>One pass of the ensure in one database. <paramref name="ownName"/> is null for a host with no install id.</summary>
    public static async Task<AlwaysOnXeAzureResult> RunAsync(
        IAlwaysOnXeDatabase database,
        AlwaysOnXeSessionKind kind,
        string? ownName,
        AlwaysOnXeChoice current,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(database);

        if (current == AlwaysOnXeChoice.Own && ownName is not null)
        {
            (bool Usable, AlwaysOnXeChange Change) shared;
            try
            {
                shared = await TrySharedAsync(database, kind, cancellationToken);
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

            var ownChange = await EnsureOwnAsync(database, kind, ownName, cancellationToken);
            return new AlwaysOnXeAzureResult(AlwaysOnXeChoice.Own, ownChange);
        }

        var first = await TrySharedAsync(database, kind, cancellationToken);
        if (first.Usable)
        {
            return new AlwaysOnXeAzureResult(AlwaysOnXeChoice.Shared, first.Change);
        }

        if (ownName is null)
        {
            throw new InvalidOperationException(
                $"The shared {AlwaysOnXeSessions.SharedNameFor(kind)} session cannot be used in this database, and this install has no id to name a session of its own.");
        }

        await EnsureOwnAsync(database, kind, ownName, cancellationToken);
        return new AlwaysOnXeAzureResult(AlwaysOnXeChoice.Own, AlwaysOnXeChange.FellBack);
    }

    private static async Task<(bool Usable, AlwaysOnXeChange Change)> TrySharedAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, CancellationToken cancellationToken)
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
                try
                {
                    await CreateAsync(database, kind, shared, cancellationToken);
                    return (true, AlwaysOnXeChange.Created);
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
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, string ownName, CancellationToken cancellationToken)
    {
        var catalog = await database.ReadCatalogAsync(kind, ownName, cancellationToken);
        if (catalog != AlwaysOnXeCatalog.Missing)
        {
            if (await database.IsStartedAsync(ownName, cancellationToken))
            {
                return AlwaysOnXeChange.None;
            }

            await StartByNameAsync(database, kind, ownName, cancellationToken);
            return AlwaysOnXeChange.Started;
        }

        try
        {
            await CreateAsync(database, kind, ownName, cancellationToken);
            return AlwaysOnXeChange.Created;
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

        var drop = AlwaysOnXeSessions.BuildAzureDropSql(kind, ownName);
        if (!database.ReadOnlyIntent)
        {
            await database.ExecuteAsync(drop, cancellationToken);
            return;
        }

        if (await database.IsStartedAsync(ownName, cancellationToken))
        {
            await database.ExecuteAsync(AlwaysOnXeSessions.BuildAzureStopSql(kind, ownName), cancellationToken);
        }

        await database.ExecuteWithoutReadOnlyIntentAsync(drop, cancellationToken);
    }

    /// <summary>
    /// Creates one session and starts it. A registration without read-only intent sends the create and the start as one batch
    /// over its own connection, as before. One with the intent sends the definition over a connection without it, then starts
    /// the session over its own connection, by name: the definition replicates to the replica, and run state is per replica
    /// (#4961). A failure of either is marked for the caps sentence, unless it is the engine's "already there".
    /// </summary>
    private static async Task CreateAsync(
        IAlwaysOnXeDatabase database, AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken)
    {
        if (!database.ReadOnlyIntent)
        {
            await ExecuteDdlAsync(database, AlwaysOnXeSessions.BuildAzureCreateSql(kind, sessionName), cancellationToken);
            return;
        }

        await ExecuteDdlAsync(
            database, AlwaysOnXeSessions.BuildAzureCreateDefinitionSql(kind, sessionName), cancellationToken, withoutReadOnlyIntent: true);
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
