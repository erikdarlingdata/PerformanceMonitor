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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;

namespace PerformanceMonitor.Darling.Service;

/// <summary>Where an Extended Events session lives: on the server (on-premises, Managed Instance, AWS RDS) or inside one
/// database (Azure SQL Database, which has no server-scoped sessions).</summary>
public enum XeSessionScope
{
    /// <summary><c>DROP EVENT SESSION [name] ON SERVER</c>.</summary>
    Server,

    /// <summary><c>DROP EVENT SESSION [name] ON DATABASE</c>, run on a connection to that database.</summary>
    Database,
}

/// <summary>One Darling session found on a target. <see cref="Database"/> is set for <see cref="XeSessionScope.Database"/>
/// and only then.</summary>
public sealed record ExistingXeSession(string Name, XeSessionScope Scope, string? Database = null);

/// <summary>
/// One planned DROP. It is built from a session and nothing else: <see cref="Statement"/> is derived on demand through
/// <see cref="DarlingXeSessionCleanup.DropStatement"/>, which accepts only Darling's own session names, so no code path can
/// hand the executor a statement that did not come from the allow-list.
/// </summary>
public sealed record XeSessionDrop(ExistingXeSession Session, string? InstallId = null)
{
    /// <summary>The single statement that drops <see cref="Session"/>, bracket-quoted.</summary>
    public string Statement => DarlingXeSessionCleanup.DropStatement(Session.Name, Session.Scope, InstallId);
}

/// <summary>What a search of one target found: the Darling sessions that exist, and a sentence for every monitored place
/// that could not be searched (an Azure SQL Database database that refused the connection, say), each of which fails the
/// run. <see cref="Notes"/> holds the sentences for a place the registration excludes, which the search opened only for the
/// long-query session and could not search: it says where a session may be left and does not fail the run. It also holds the
/// sentence for this install's long-query session that the search left out of <see cref="Sessions"/> because another
/// registration of this install keeps it on the same instance.</summary>
public sealed record XeSessionSearch(IReadOnlyList<ExistingXeSession> Sessions, IReadOnlyList<string> Problems)
{
    /// <summary>A sentence for each excluded database that could not be searched for the long-query session, and one for this
    /// install's long-query session left in place on an instance because another registration of this install keeps it. The
    /// verb prints each on stderr and does not change its exit code for it: before the search reached excluded databases, the
    /// verb never opened one, so an excluded database the login cannot open is not a reason to stop a script that removes the
    /// server next. A session left in such a database needs a manual drop.</summary>
    public IReadOnlyList<string> Notes { get; init; } = Array.Empty<string>();

    /// <summary>The sessions of other installs the search found (<see cref="DarlingXeSessionCleanup.ComposeFindOthersSql"/>).
    /// The verb lists them and never drops them.</summary>
    public IReadOnlyList<ExistingXeSession> Others { get; init; } = Array.Empty<ExistingXeSession>();
}

/// <summary>What the verb reads from the Darling store before it connects to the server: this install's id, every schedule
/// override (so another registration's long-query setting is its effective one: its own override, else the install's
/// default), and each registration's last-known instance name
/// (<see cref="PerformanceMonitor.Collectors.ServerEpoch.LastKnownName"/>).</summary>
/// <param name="InstallId">This install's id, or null when the store has none yet or could not be read.</param>
/// <param name="ScheduleOverrides">Every row of the store's collector schedules, or null when they could not be read. With
/// none read, every other registration counts as having its long-query trace on.</param>
/// <param name="InstanceNames">The last-known <c>@@SERVERNAME</c> of each registration, by registration id. A registration
/// with no name matches no instance, so a session is never left in place for it.</param>
/// <param name="Note">Why the id is not known, when it is not.</param>
internal sealed record XeCleanupStoreFacts(
    string? InstallId,
    IReadOnlyList<ScheduleOverride>? ScheduleOverrides = null,
    IReadOnlyDictionary<int, string>? InstanceNames = null,
    string? Note = null)
{
    /// <summary>A store that holds no install id and nothing else the verb reads.</summary>
    public static XeCleanupStoreFacts NoId { get; } = new(InstallId: null);
}

/// <summary>
/// The seam between the cleanup verb's decisions and the SQL Server it runs against, so the whole verb is testable
/// without one (#4732). The real implementation is <see cref="SqlServerXeSessionCleanupTarget"/>.
/// </summary>
public interface IXeSessionCleanupTarget
{
    /// <summary>Which of Darling's sessions exist on the target. Throws when the target itself cannot be searched; a
    /// monitored place inside it that cannot be searched comes back as a <see cref="XeSessionSearch.Problems"/> entry instead,
    /// and an excluded database that cannot be searched for the long-query session as a <see cref="XeSessionSearch.Notes"/>
    /// entry.</summary>
    Task<XeSessionSearch> FindSessionsAsync(CancellationToken cancellationToken);

    /// <summary>Runs one planned drop. Throws when the server refuses it.</summary>
    Task DropAsync(XeSessionDrop drop, CancellationToken cancellationToken);
}

/// <summary>
/// The <c>--drop-xe-sessions</c> verb's plan and executor (#4732, #4961): drops the Extended Events sessions Darling can
/// leave on a server, and lists the sessions of other installs for the operator to drop.
///
/// <para><b>What removal drops, and what is left for this verb.</b> Removing a server drops this install's own sessions on it
/// (<see cref="DarlingRemovedServerSessions"/>): one attempt within 15 seconds, except a session another registration of this
/// install keeps. This verb is for what removal could not drop, such as a server that was unreachable at removal or a drop
/// that timed out. It also drops the old shared long-query session, which the service tries to drop only once, and the shared
/// deadlock and blocked-process sessions. Those names are shared with Lite and with any other Darling service that monitors
/// the same server, so the service never drops them (dropping them under a monitor that is still running blinds it until its
/// next connect or cycle). And it lists the sessions of other installs, each with a guarded DROP the operator runs when that
/// install no longer monitors the server. The named form finds a server only while it is still registered, so after a
/// removal the <c>--print-sql</c> script is the way. The deadlock and blocked-process sessions cost a 4 MB ring buffer each;
/// the long-query completion session has a 4 MB ring buffer too and also tests every completed statement and batch against
/// its duration filter, which is why it is opt-in.</para>
///
/// <para><b>Only Darling's own names, never one from input.</b> <see cref="SessionNames"/> is the deadlock and
/// blocked-process sessions the ensure lifecycle in <see cref="DarlingXeSessions"/> creates, and the old shared long-query
/// completion session that older versions made; <see cref="InstallSessionNames"/> adds this install's own, named from its id.
/// All of them are taken from the same constants and builders the collectors use. <see cref="DropStatement"/> refuses any
/// other name, and <see cref="PlanDrops"/> rewrites a matching name to the constant before it builds a statement, so what
/// reaches a server is always Darling's own spelling, bracket-quoted. The old shared long-query session is in the list
/// because the service drops it only once and then leaves it (<see cref="DarlingLegacyLongQuerySession"/>), and removing a
/// server does not drop it.</para>
///
/// <para><b>Plan and executor are separate.</b> <see cref="PlanDrops"/> and <see cref="GuardedDropScript"/> are pure and
/// pin as text. <see cref="RunAsync"/> is the thin executor over <see cref="IXeSessionCleanupTarget"/>, so tests drive the
/// find-print-drop path with a fake target.</para>
/// </summary>
public static class DarlingXeSessionCleanup
{
    /// <summary>The sessions this verb may drop, in the order it reports and drops them. The find queries, the drop
    /// statements, the <c>--print-sql</c> script and the help text are all built from this one list.</summary>
    public static IReadOnlyList<string> SessionNames { get; } = new[]
    {
        DeadlocksCollector.XeSessionName,
        BlockedProcessReportCollector.XeSessionName,
        /* The legacy long-query name, on purpose (#4961): this verb finds and drops the session that older versions made and
           every install shared. An install now makes its own session, named from its id, and this list does not name it yet. */
        LongQueryCompletionsCollector.LegacyXeSessionName,
    };

    /// <summary>
    /// This install's own sessions (#4961), made by the builders the collectors use, never by hand: its long-query session and,
    /// on Azure SQL Database, the deadlock and blocked-process fallbacks it makes when a shared session is unusable. Empty when
    /// <paramref name="installId"/> is not an install id, because an install with no id has made none of them.
    /// </summary>
    public static IReadOnlyList<string> InstallSessionNames(string? installId)
    {
        if (!InstallId.IsValid(installId))
        {
            return Array.Empty<string>();
        }

        return new[]
        {
            LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, installId),
            AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, installId, AlwaysOnXeSessionKind.Deadlock),
            AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, installId, AlwaysOnXeSessionKind.BlockedProcess),
        };
    }

    /// <summary>The sessions the verb may drop for an install: <see cref="SessionNames"/> (the two shared names and the old
    /// shared long-query name), then <see cref="InstallSessionNames"/>. The one allow-list: <see cref="DropStatement"/>, the
    /// plan and the find text all read it.</summary>
    public static IReadOnlyList<string> NamesToDrop(string? installId) => SessionNames.Concat(InstallSessionNames(installId)).ToList();

    /* The per-install shapes, cut from the shared names so no second spelling exists: PerformanceMonitor_{product}_{id}_{suffix}
       becomes a LIKE pattern with the underscores escaped. The shared names and the old shared long-query name carry no middle
       segment, so none of them matches. */
    private const string SharedNamePrefix = "PerformanceMonitor_";

    private static string PerInstallPattern(string sharedName) =>
        "PerformanceMonitor[_]%[_]" + sharedName.Substring(SharedNamePrefix.Length);

    /// <summary>
    /// True when <paramref name="name"/> is the exact shape of another install's session (<c>PerformanceMonitor_{Lite|Darling}_{id}_</c>
    /// and a long-query, deadlock or blocked-process suffix) and is not one of this install's own. False for every name when
    /// <paramref name="installId"/> is not an id: with no id the verb cannot tell this install's sessions from another's, so it
    /// lists none. The exact shape, not the LIKE match, decides, so a name is never printed into a statement unless it is made of
    /// the product, a hex id and a fixed suffix.
    /// </summary>
    public static bool IsOtherInstallSessionName(string? name, string? installId)
    {
        if (name is null || !InstallId.IsValid(installId))
        {
            return false;
        }

        var shaped = LongQueryCompletionsCollector.IsInstallSessionName(name)
            || AlwaysOnXeSessions.IsOwnName(name, AlwaysOnXeSessionKind.Deadlock)
            || AlwaysOnXeSessions.IsOwnName(name, AlwaysOnXeSessionKind.BlockedProcess);
        return shaped && !InstallSessionNames(installId).Contains(name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The query that lists the sessions of every install, this one's included, in the catalog of <paramref name="scope"/>: the
    /// three per-install shapes as LIKE patterns. The caller drops this install's own names from the result and lists the rest
    /// (<see cref="IsOtherInstallSessionName"/>). The patterns are fixed text, never input.
    /// </summary>
    internal static string ComposeFindOthersSql(XeSessionScope scope)
    {
        var (catalogView, alias) = scope switch
        {
            XeSessionScope.Server => ("sys.server_event_sessions", "ses"),
            XeSessionScope.Database => ("sys.database_event_sessions", "des"),
            _ => throw new ArgumentOutOfRangeException(nameof(scope)),
        };

        var patterns = new[]
        {
            LongQueryCompletionsCollector.LegacyXeSessionName,
            DeadlocksCollector.XeSessionName,
            BlockedProcessReportCollector.XeSessionName,
        }.Select(PerInstallPattern).ToList();

        var conditions = string.Join(Environment.NewLine + "OR ", patterns.Select(pattern => $"{alias}.name LIKE N'{pattern}'"));
        return $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    {alias}.name
FROM {catalogView} AS {alias}
WHERE {conditions};";
    }

    /// <summary>The line above the sessions of other installs the verb lists. It says the verb never drops them.</summary>
    public const string OtherInstallsHeading =
        "Sessions of other installs on this server. This verb lists them and never drops them. "
        + "Run a statement below only when that install no longer monitors this server:";

    /// <summary>What the verb says when the store holds no install id: it leaves every session named for an install alone.</summary>
    public const string NoInstallIdNote =
        "The store has no install id yet, so this run leaves alone every session named for an install. "
        + "The service makes the id when it starts.";

    /// <summary>The guarded DROP of one session as lines of text: an <c>IF EXISTS</c> against the catalog of its scope around the
    /// DROP. The caller has already limited the name to one it may print (<see cref="DropStatement"/>'s list, or
    /// <see cref="IsOtherInstallSessionName"/>).</summary>
    internal static IReadOnlyList<string> GuardedDropLines(string name, XeSessionScope scope)
    {
        var (catalogView, alias) = scope switch
        {
            XeSessionScope.Server => ("sys.server_event_sessions", "ses"),
            XeSessionScope.Database => ("sys.database_event_sessions", "des"),
            _ => throw new ArgumentOutOfRangeException(nameof(scope)),
        };

        return new[]
        {
            "IF EXISTS",
            "(",
            "    SELECT",
            "        1/0",
            $"    FROM {catalogView} AS {alias}",
            $"    WHERE {alias}.name = N'{name.Replace("'", "''", StringComparison.Ordinal)}'",
            ")",
            "BEGIN",
            "    " + ComposeDropStatement(name, scope),
            "END;",
        };
    }

    /// <summary>The names as one phrase for console and help text: <c>A, B and C</c>.</summary>
    public static string SessionNamesPhrase() => JoinAsPhrase(SessionNames);

    /// <summary><c>A, B and C</c>; a single item stands alone.</summary>
    private static string JoinAsPhrase(IReadOnlyList<string> items) =>
        items.Count < 2
            ? string.Join(string.Empty, items)
            : string.Join(", ", items.Take(items.Count - 1)) + " and " + items[^1];

    /// <summary>
    /// The one warning both modes print, without its prefix. It says only what the code does: Lite ensures a missing
    /// session on every collection cycle, and <see cref="DarlingXeSessions.EnsureAllAsync"/> creates a missing session
    /// (server-scoped, or in each database on Azure SQL Database) when a Darling service next connects to the server.
    /// While its long-query trace is on, a Darling service also creates a missing long-query completion session within an
    /// hour (<c>DarlingWorker.ReconcileLongQueryTraceAsync</c>). Both apps create the long-query completion session only for
    /// a server whose collector is turned on. The deprecated Full
    /// Dashboard installer names the same deadlock and blocked-process sessions, and the collection procedures it installs
    /// (<c>install/22_collect_blocked_processes.sql</c>, <c>install/24_collect_deadlock_xml.sql</c>) create a missing one at
    /// the top of every run; it has no long-query completion session (#4732).
    /// </summary>
    public const string SharedNamesWarning =
        "a Lite app, a deprecated Full Dashboard install or another Darling service that still monitors this server uses "
        + "the same session names and creates a missing session again (Lite on its next collection cycle, the Dashboard on "
        + "its next collection run, Darling on its next connect to the server or, for the long query completions session, "
        + "within an hour; the long query completions session only where that collector is turned on, and never by the "
        + "Dashboard), "
        + "so run this only once nothing else monitors it.";

    /// <summary>What each session captures, in the words the note after a drop uses. A name with no entry reads as itself, so
    /// a session added to <see cref="SessionNames"/> is never left out of the note.</summary>
    private static string? CapturePhrase(string name) =>
        name == DeadlocksCollector.XeSessionName || AlwaysOnXeSessions.IsOwnName(name, AlwaysOnXeSessionKind.Deadlock) ? "deadlocks"
        : name == BlockedProcessReportCollector.XeSessionName || AlwaysOnXeSessions.IsOwnName(name, AlwaysOnXeSessionKind.BlockedProcess) ? "blocked processes"
        : name == LongQueryCompletionsCollector.LegacyXeSessionName || LongQueryCompletionsCollector.IsInstallSessionName(name) ? "long query completions"
        : null;

    /// <summary>
    /// The one line a run that dropped at least one session prints after its results, or null when nothing was dropped (a
    /// dry run, a search that found nothing, or every drop refused) (#4732). The named form reaches only a server this service
    /// still monitors, and a session dropped there is created again only when the service next connects to that server
    /// (<see cref="DarlingXeSessions.EnsureAllAsync"/>, and the long-query reconcile that resets on connect), so until then the
    /// service captures none of what those sessions capture. What stops is read from the sessions that were dropped, in the
    /// order of <see cref="SessionNames"/>, so the note never claims a capture whose session is still there.
    /// </summary>
    internal static string? CaptureStopsNote(IEnumerable<ExistingXeSession> dropped)
    {
        ArgumentNullException.ThrowIfNull(dropped);

        var order = new[] { "deadlocks", "blocked processes", "long query completions" };
        var captures = dropped
            .Select(d => CapturePhrase(d.Name))
            .OfType<string>()
            .Distinct(StringComparer.Ordinal)
            .OrderBy(phrase => Array.IndexOf(order, phrase))
            .ToList();
        return captures.Count == 0
            ? null
            : "NOTE: capture of " + JoinAsPhrase(captures) + " on that server stops until this service reconnects to it, so remove the server next.";
    }

    /// <summary>Server-scoped sessions of Darling's names. Composed from <see cref="SessionNames"/> by
    /// <see cref="ComposeFindSql"/>, the one place find text is composed, so no input reaches it and the search can never
    /// look for fewer names than the plan accepts. A cleanup target composes its own find text through the same method, so a
    /// target given a copy of <see cref="SessionNames"/> sends exactly this text, byte for byte (#4732). It reads
    /// <see cref="SessionNames"/> during static initialization, so that property stays declared above it.</summary>
    internal static readonly string FindServerSessionsSql = ComposeFindSql(XeSessionScope.Server, SessionNames);

    /// <summary>Database-scoped sessions of Darling's names, read inside one Azure SQL Database database. Composed as
    /// <see cref="FindServerSessionsSql"/> is.</summary>
    internal static readonly string FindDatabaseSessionsSql = ComposeFindSql(XeSessionScope.Database, SessionNames);

    /// <summary>
    /// The query that lists which of <paramref name="names"/> exist as Extended Events sessions in the catalog of
    /// <paramref name="scope"/> (<c>sys.server_event_sessions</c>, or <c>sys.database_event_sessions</c> inside one Azure SQL
    /// Database database). The names are the caller's constants, never input; quotes in them are doubled all the same, a
    /// guard against a future rename like <see cref="BracketQuote"/>'s. Both find constants above and every cleanup target
    /// compose through here, so the text a test runs against a server is the text the product sends (#4732).
    /// </summary>
    internal static string ComposeFindSql(XeSessionScope scope, IReadOnlyList<string> names)
    {
        ArgumentNullException.ThrowIfNull(names);

        var (catalogView, alias) = scope switch
        {
            XeSessionScope.Server => ("sys.server_event_sessions", "ses"),
            XeSessionScope.Database => ("sys.database_event_sessions", "des"),
            _ => throw new ArgumentOutOfRangeException(nameof(scope)),
        };

        /* N'a', N'b', N'c': the names as Unicode string literals for the IN list. */
        var literals = string.Join(", ", names.Select(n => "N'" + n.Replace("'", "''", StringComparison.Ordinal) + "'"));
        return $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    {alias}.name
FROM {catalogView} AS {alias}
WHERE {alias}.name IN ({literals});";
    }

    /// <summary>Brackets an identifier, doubling any closing bracket. Darling's names contain none, so this is a guard
    /// against a future rename, not a path input takes.</summary>
    internal static string BracketQuote(string identifier) => "[" + identifier.Replace("]", "]]", StringComparison.Ordinal) + "]";

    /// <summary>
    /// <c>DROP EVENT SESSION [name] ON SERVER;</c> or <c>... ON DATABASE;</c> for one of <see cref="SessionNames"/>.
    /// Throws <see cref="ArgumentException"/> for any other name, which is the whole of the "never a name from input" rule.
    /// </summary>
    public static string DropStatement(string sessionName, XeSessionScope scope, string? installId = null)
    {
        var canonical = Canonical(sessionName, installId)
            ?? throw new ArgumentException("Only Darling's own session names can be dropped by this verb.", nameof(sessionName));
        if (!string.Equals(canonical, sessionName, StringComparison.Ordinal))
        {
            throw new ArgumentException("The session name is not spelled exactly as Darling creates it.", nameof(sessionName));
        }

        return ComposeDropStatement(canonical, scope);
    }

    /// <summary>
    /// The STOP of one session in a database, for a session that runs on a read-only replica (#4961). Run state is per replica,
    /// so a registration with read-only intent stops the session over its own connection, which reaches that replica, and only
    /// then drops it over a connection without the intent. It takes exactly the names <see cref="DropStatement"/> takes, spelled
    /// exactly, and throws <see cref="ArgumentException"/> for any other. The text is guarded on
    /// <c>sys.dm_xe_database_sessions</c>, which lists the sessions that run there, so a replica where the session does not run
    /// is a clean no-op.
    /// </summary>
    public static string StopStatement(string sessionName, string? installId = null)
    {
        var canonical = Canonical(sessionName, installId)
            ?? throw new ArgumentException("Only Darling's own session names can be stopped by this verb.", nameof(sessionName));
        if (!string.Equals(canonical, sessionName, StringComparison.Ordinal))
        {
            throw new ArgumentException("The session name is not spelled exactly as Darling creates it.", nameof(sessionName));
        }

        return ComposeStopStatement(canonical);
    }

    /// <summary>
    /// The STOP for a name the caller has already limited to a list it owns, as <see cref="ComposeDropStatement"/> is for the
    /// drop. A long-query name (this install's own, or the old shared one) and this install's own deadlock or blocked-process
    /// fallback are stopped by the builders the collectors use
    /// (<see cref="LongQueryCompletionsCollector.BuildStopSessionSql"/>, <see cref="AlwaysOnXeSessions.BuildAzureStopSql"/>). The
    /// two shared names are ones no builder takes, so they get the long-query builder's guard around one STOP.
    /// </summary>
    internal static string ComposeStopStatement(string sessionName)
    {
        if (string.Equals(sessionName, LongQueryCompletionsCollector.LegacyXeSessionName, StringComparison.Ordinal)
            || LongQueryCompletionsCollector.IsInstallSessionName(sessionName))
        {
            return LongQueryCompletionsCollector.BuildStopSessionSql(sessionName);
        }

        foreach (var kind in new[] { AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeSessionKind.BlockedProcess })
        {
            if (AlwaysOnXeSessions.IsOwnName(sessionName, kind))
            {
                return AlwaysOnXeSessions.BuildAzureStopSql(kind, sessionName);
            }
        }

        return $@"
IF EXISTS
(
    SELECT
        1/0
    FROM sys.dm_xe_database_sessions
    WHERE name = N'{sessionName.Replace("'", "''", StringComparison.Ordinal)}'
)
BEGIN
    ALTER EVENT SESSION {BracketQuote(sessionName)} ON DATABASE STATE = STOP;
END;";
    }

    /// <summary>
    /// <c>DROP EVENT SESSION [name] ON SERVER;</c> or <c>... ON DATABASE;</c>, with no check on the name: the caller has
    /// already limited it to a list it owns. <see cref="DropStatement"/> limits it to <see cref="SessionNames"/> and the plan
    /// builds its statements through that; a cleanup target limits it to the names it was given. Both compose here, so the
    /// statement a test drops with is the statement the product sends (#4732).
    /// </summary>
    internal static string ComposeDropStatement(string sessionName, XeSessionScope scope) => scope switch
    {
        XeSessionScope.Server => $"DROP EVENT SESSION {BracketQuote(sessionName)} ON SERVER;",
        XeSessionScope.Database => $"DROP EVENT SESSION {BracketQuote(sessionName)} ON DATABASE;",
        _ => throw new ArgumentOutOfRangeException(nameof(scope)),
    };

    /// <summary>The constant spelling of <paramref name="name"/> when it is one of <see cref="NamesToDrop"/>, else null.</summary>
    private static string? Canonical(string? name, string? installId) =>
        NamesToDrop(installId).FirstOrDefault(n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// The drops for the sessions that were found: pure, so "which statements for these existing sessions" pins without a
    /// server. Anything that is not one of <see cref="SessionNames"/> is ignored (a server can hold any number of other
    /// sessions); a name that differs from the constant only by case is planned under the constant. The order is fixed:
    /// server scope before database scope, databases by name, then the order of <see cref="SessionNames"/> (deadlock,
    /// blocked-process, long-query completions). Duplicates collapse.
    /// </summary>
    public static IReadOnlyList<XeSessionDrop> PlanDrops(IEnumerable<ExistingXeSession> existing, string? installId = null)
    {
        ArgumentNullException.ThrowIfNull(existing);

        var planned = new List<ExistingXeSession>();
        foreach (var session in existing)
        {
            var canonical = Canonical(session.Name, installId);
            if (canonical is null)
            {
                continue;
            }

            if (session.Scope == XeSessionScope.Database && string.IsNullOrWhiteSpace(session.Database))
            {
                throw new ArgumentException("A database-scoped session needs the database it was found in.", nameof(existing));
            }

            var database = session.Scope == XeSessionScope.Database ? session.Database : null;
            var candidate = new ExistingXeSession(canonical, session.Scope, database);
            if (!planned.Any(p => p.Scope == candidate.Scope
                    && p.Name == candidate.Name
                    && string.Equals(p.Database, candidate.Database, StringComparison.OrdinalIgnoreCase)))
            {
                planned.Add(candidate);
            }
        }

        return planned
            .OrderBy(p => p.Scope)
            .ThenBy(p => p.Database, StringComparer.OrdinalIgnoreCase)
            .ThenBy(p => SessionIndex(p.Name, installId))
            .Select(p => new XeSessionDrop(p, installId))
            .ToList();
    }

    private static int SessionIndex(string canonicalName, string? installId)
    {
        var names = NamesToDrop(installId);
        for (var i = 0; i < names.Count; i++)
        {
            if (names[i] == canonicalName)
            {
                return i;
            }
        }

        return names.Count;
    }

    /// <summary>How one found or dropped session reads on the console.</summary>
    public static string Describe(ExistingXeSession session) =>
        session.Scope == XeSessionScope.Server
            ? $"{session.Name} (server scope)"
            : $"{session.Name} (database scope, in database {session.Database})";

    /// <summary>
    /// The <c>--print-sql</c> script: no connection, no input. Every DROP sits behind an <c>IF EXISTS</c> against the
    /// catalog view of its scope, so running it twice, or on a server that never had the sessions, changes nothing. Part 1
    /// is the server scope (run it once on the instance); part 2 is the database scope (run it in each monitored database on
    /// Azure SQL Database, where <c>sys.server_event_sessions</c> does not exist and a session cannot live in master).
    /// </summary>
    public static string GuardedDropScript()
    {
        var lines = new List<string>
        {
            "-- PerformanceMonitor Darling: drop the Extended Events sessions Darling creates on a monitored server.",
            "-- WARNING: " + SharedNamesWarning,
            "--",
            "-- Part 1 of 2 - run once on the SQL Server instance (on-premises, Managed Instance, AWS RDS).",
        };

        foreach (var name in SessionNames)
        {
            lines.AddRange(GuardedDropLines(DropStatementName(name, XeSessionScope.Server), XeSessionScope.Server));
            lines.Add(string.Empty);
        }

        lines.AddRange(PerInstallListing(XeSessionScope.Server));
        lines.Add(string.Empty);

        lines.Add("-- Part 2 of 2 - run inside EACH monitored database on Azure SQL Database (a database-scoped session lives in its own database, never in master).");

        foreach (var name in SessionNames)
        {
            lines.AddRange(GuardedDropLines(DropStatementName(name, XeSessionScope.Database), XeSessionScope.Database));
            lines.Add(string.Empty);
        }

        lines.AddRange(PerInstallListing(XeSessionScope.Database));
        lines.Add(string.Empty);

        return string.Join(Environment.NewLine, lines);
    }

    /// <summary>A name the allow-list accepts, returned as it was given: the script prints a name only after
    /// <see cref="DropStatement"/> has accepted it.</summary>
    private static string DropStatementName(string name, XeSessionScope scope)
    {
        _ = DropStatement(name, scope);
        return name;
    }

    /// <summary>
    /// The script's part for the sessions named for an install: a query that lists each one in the catalog of
    /// <paramref name="scope"/> with its DROP statement as a column. The script connects to nothing, so it cannot know this
    /// install's id; the operator reads it from the store and runs the statements that carry it. A session with another id
    /// belongs to another install.
    /// </summary>
    private static IEnumerable<string> PerInstallListing(XeSessionScope scope)
    {
        var (catalogView, alias, on) = scope == XeSessionScope.Server
            ? ("sys.server_event_sessions", "ses", "SERVER")
            : ("sys.database_event_sessions", "des", "DATABASE");

        yield return "-- Sessions named for an install (" + (scope == XeSessionScope.Server ? "server scope" : "database scope") + "): the query lists each one with its DROP statement.";
        yield return "-- This install's sessions carry this install's id (the install_id column of config.config_install_id in the Darling store): run those statements.";
        yield return "-- A session with another id belongs to another install: run its statement only when that install no longer monitors this server.";
        yield return "SELECT";
        yield return $"    {alias}.name,";
        yield return $"    N'DROP EVENT SESSION ' + QUOTENAME({alias}.name) + N' ON {on};' AS drop_statement";
        yield return $"FROM {catalogView} AS {alias}";
        var patterns = new[]
        {
            LongQueryCompletionsCollector.LegacyXeSessionName,
            DeadlocksCollector.XeSessionName,
            BlockedProcessReportCollector.XeSessionName,
        }.Select(PerInstallPattern).ToList();
        for (var i = 0; i < patterns.Count; i++)
        {
            yield return (i == 0 ? "WHERE " : "OR ") + $"{alias}.name LIKE N'{patterns[i]}'";
        }

        yield return $"ORDER BY {alias}.name;";
    }

    /// <summary>
    /// The connected half of the verb, over an <see cref="IXeSessionCleanupTarget"/>: find the sessions, print the shared-names
    /// warning (before anything is dropped, so the operator reads it first), print each session, drop each unless
    /// <paramref name="dryRun"/>, and print what the drops stop (<see cref="CaptureStopsNote"/>, only when one was dropped). Returns
    /// <see cref="DarlingCliCommands.DropXeSessionsExitCode.Success"/> when everything found was dropped (or listed, in a dry
    /// run) or nothing was there, and <see cref="DarlingCliCommands.DropXeSessionsExitCode.TargetUnavailable"/> when the target
    /// could not be searched, a monitored part of it could not be searched, or a drop was refused. A refused drop does not
    /// stop the ones after it. An excluded database that could not be searched for the long-query session is printed on
    /// stderr as a note (<see cref="XeSessionSearch.Notes"/>) and does not change the exit code.
    /// </summary>
    internal static async Task<int> RunAsync(
        string serverLabel,
        bool dryRun,
        IXeSessionCleanupTarget target,
        TextWriter output,
        TextWriter error,
        CancellationToken cancellationToken,
        string? installId = null)
    {
        XeSessionSearch search;
        try
        {
            search = await target.FindSessionsAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not search '{serverLabel}' for Extended Events sessions: {ex.Message}");
            error.WriteLine(PrintSqlHint);
            return DarlingCliCommands.DropXeSessionsExitCode.TargetUnavailable;
        }

        var exitCode = DarlingCliCommands.DropXeSessionsExitCode.Success;
        foreach (var problem in search.Problems)
        {
            error.WriteLine(problem);
            exitCode = DarlingCliCommands.DropXeSessionsExitCode.TargetUnavailable;
        }

        /* A note is not a failure: it names an excluded database the search opened only for the long-query session and could not
           open, which the verb never searched before that session was added to the search. Exit code untouched. */
        foreach (var note in search.Notes)
        {
            error.WriteLine(note);
        }

        if (!InstallId.IsValid(installId))
        {
            output.WriteLine(NoInstallIdNote);
        }

        var drops = PlanDrops(search.Sessions, installId);
        if (drops.Count == 0)
        {
            /* "In the places searched": a database that could not be searched (a Problems entry, exit 2, or a note) may hold a session. */
            output.WriteLine($"No Darling Extended Events sessions ({string.Join(", ", NamesToDrop(installId))}) were found on '{serverLabel}'; nothing to drop in the places searched.");
        }

        /* Before the first drop: the warning says other installs recreate a shared session, which the operator needs to know
           before the sessions are gone, not after. */
        output.WriteLine("WARNING: " + SharedNamesWarning);

        var dropped = new List<ExistingXeSession>();
        foreach (var drop in drops)
        {
            cancellationToken.ThrowIfCancellationRequested();
            output.WriteLine($"  Found {Describe(drop.Session)}");

            if (dryRun)
            {
                output.WriteLine($"  [WOULD DROP] {drop.Statement}");
                continue;
            }

            try
            {
                await target.DropAsync(drop, cancellationToken);
                dropped.Add(drop.Session);
                output.WriteLine($"  [DROPPED] {Describe(drop.Session)}");
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                error.WriteLine($"  [FAILED] {Describe(drop.Session)}: {ex.Message}");
                exitCode = DarlingCliCommands.DropXeSessionsExitCode.TargetUnavailable;
            }
        }

        if (dryRun && drops.Count > 0)
        {
            output.WriteLine("Dry run: nothing was dropped.");
        }

        /* Another install's sessions are text for the operator: listed with a guarded statement, never handed to the target. */
        var others = search.Others
            .Where(other => IsOtherInstallSessionName(other.Name, installId)
                && (other.Scope == XeSessionScope.Server || !string.IsNullOrWhiteSpace(other.Database)))
            .DistinctBy(other => (other.Scope, other.Name, Database: other.Database?.ToUpperInvariant()))
            .OrderBy(other => other.Scope)
            .ThenBy(other => other.Database, StringComparer.OrdinalIgnoreCase)
            .ThenBy(other => other.Name, StringComparer.Ordinal)
            .ToList();
        if (others.Count > 0)
        {
            output.WriteLine();
            output.WriteLine(OtherInstallsHeading);
            foreach (var other in others)
            {
                output.WriteLine($"  {Describe(other)}");
                foreach (var line in GuardedDropLines(other.Name, other.Scope))
                {
                    output.WriteLine("    " + line);
                }
            }
        }

        if (CaptureStopsNote(dropped) is { } captureStops)
        {
            output.WriteLine();
            output.WriteLine(captureStops);
        }

        return exitCode;
    }

    /// <summary>What to do when the server cannot be reached from where the verb runs.</summary>
    internal const string PrintSqlHint =
        "If the server cannot be reached from this machine, run --drop-xe-sessions --print-sql to print the DROP statements and run them where it can.";
}

/// <summary>
/// <see cref="IXeSessionCleanupTarget"/> over a connected SQL Server target: the ServerRuntime the shared connector produced
/// (#4732). On Azure SQL Database it visits two sets, and skips master, which cannot host a session. For the deadlock and
/// blocked-process sessions it visits the same databases <see cref="DarlingXeSessions.EnsureAllAsync"/> would, by the same rule
/// (a registration that names a database is that database alone; one that names none enumerates master through the provider's
/// own plan, honoring the server's excluded databases). For the long-query completion session it visits every online database,
/// the exclusions not applied, as the trace's off-side reconcile does, except a database monitored as its own server and a
/// database where another registration of the logical server keeps the session (<see cref="PlanAzureSearch"/>). A database the
/// search cannot open is a problem when the registration monitors it and a note when only the long-query search reaches it
/// (<see cref="AzureSearchPlan.Unsearched"/>), because the verb never opened an excluded database before that search did.
/// </summary>
internal sealed class SqlServerXeSessionCleanupTarget : IXeSessionCleanupTarget
{
    private const int CommandTimeoutSeconds = 60;

    private readonly ServerRuntime _server;

    private readonly string[] _sessionNames;

    private readonly IReadOnlyList<MonitoredServer> _registry;

    private readonly string? _installId;

    private readonly IReadOnlyList<ScheduleOverride>? _scheduleOverrides;

    private readonly IReadOnlyDictionary<int, string>? _instanceNames;

    /// <param name="server">The connected server.</param>
    /// <param name="sessionNames">The names to search for and to drop, copied here. Every product caller leaves this null, which
    /// is <see cref="DarlingXeSessionCleanup.SessionNames"/>. The live test passes a test-only name so the real find and drop
    /// run against a real server without ever naming one of Darling's own sessions. Either way the target composes its find
    /// and drop text through the same methods the plan's constants and statements are built with, so the live test sends the
    /// text the product sends, and a test pins that a copy of Darling's names sends exactly the plan's text (#4732).</param>
    /// <param name="registry">The servers this service monitors, from the same list the verb resolved the target in. On Azure SQL
    /// Database it says which databases belong to another registration of the same logical server.</param>
    /// <param name="facts">What the verb read from the store: this install's id, which adds this install's own session names to the
    /// names a product target (no <paramref name="sessionNames"/>) searches for and may drop, and makes it list the sessions of other
    /// installs. A target given names of its own searches for those alone.</param>
    internal SqlServerXeSessionCleanupTarget(
        ServerRuntime server,
        IReadOnlyList<string>? sessionNames = null,
        IReadOnlyList<MonitoredServer>? registry = null,
        XeCleanupStoreFacts? facts = null)
    {
        _server = server ?? throw new ArgumentNullException(nameof(server));
        _registry = registry ?? Array.Empty<MonitoredServer>();
        _installId = sessionNames is null && InstallId.IsValid(facts?.InstallId) ? facts!.InstallId : null;
        _scheduleOverrides = facts?.ScheduleOverrides;
        _instanceNames = facts?.InstanceNames;
        _sessionNames = (sessionNames ?? DarlingXeSessionCleanup.NamesToDrop(_installId)).ToArray();
        if (_sessionNames.Length == 0 || _sessionNames.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException("A cleanup target needs at least one session name, and none of them blank.", nameof(sessionNames));
        }

        FindServerSql = DarlingXeSessionCleanup.ComposeFindSql(XeSessionScope.Server, _sessionNames);
        FindDatabaseSql = DarlingXeSessionCleanup.ComposeFindSql(XeSessionScope.Database, _sessionNames);
        if (_installId is not null)
        {
            FindOthersServerSql = DarlingXeSessionCleanup.ComposeFindOthersSql(XeSessionScope.Server);
            FindOthersDatabaseSql = DarlingXeSessionCleanup.ComposeFindOthersSql(XeSessionScope.Database);
        }
    }

    /// <summary>The query that lists the sessions of every install on a server that has server-scoped sessions. Null when the
    /// target has no install id: it then lists none.</summary>
    internal string? FindOthersServerSql { get; }

    /// <summary>The same query in each database on Azure SQL Database. Null when the target has no install id.</summary>
    internal string? FindOthersDatabaseSql { get; }

    /// <summary>Whether a session of this name is a long-query session: the old shared one or an install's own. Both are searched
    /// for in the same databases, which are not the deadlock and blocked-process sessions'.</summary>
    internal static bool IsLongQuerySession(string sessionName) =>
        string.Equals(sessionName, LongQueryCompletionsCollector.LegacyXeSessionName, StringComparison.OrdinalIgnoreCase)
        || LongQueryCompletionsCollector.IsInstallSessionName(sessionName);

    /// <summary>The query <see cref="FindSessionsAsync"/> runs on a server that has server-scoped sessions.</summary>
    internal string FindServerSql { get; }

    /// <summary>The query <see cref="FindSessionsAsync"/> runs in each database on Azure SQL Database.</summary>
    internal string FindDatabaseSql { get; }

    /// <summary>The DROP for one found session, and the only text <see cref="DropAsync"/> sends. It is refused unless the name
    /// is one of the names this target was given, spelled exactly; otherwise it is the statement
    /// <see cref="DarlingXeSessionCleanup.ComposeDropStatement"/> builds, which is also what the plan's
    /// <see cref="XeSessionDrop.Statement"/> is for Darling's own names.</summary>
    internal string StatementFor(XeSessionDrop drop)
    {
        ArgumentNullException.ThrowIfNull(drop);

        var session = drop.Session;
        if (!_sessionNames.Contains(session.Name, StringComparer.Ordinal))
        {
            throw new ArgumentException("Only the session names this target was given can be dropped by it.", nameof(drop));
        }

        return DarlingXeSessionCleanup.ComposeDropStatement(session.Name, session.Scope);
    }

    /// <summary>
    /// Where a search of an Azure SQL Database server looks, and which sessions it reports in each database.
    /// </summary>
    /// <param name="AlwaysOnDatabases">The databases searched for every session but the long-query one: the registration's
    /// monitored databases.</param>
    /// <param name="LongQueryDatabases">The databases searched for the long-query session.</param>
    internal sealed record AzureSearchPlan(IReadOnlyList<string> AlwaysOnDatabases, IReadOnlyList<string> LongQueryDatabases)
    {
        /// <summary>Every database the search opens, once, in order.</summary>
        public IReadOnlyList<string> Visited { get; } =
            AlwaysOnDatabases.Concat(LongQueryDatabases).Distinct(StringComparer.OrdinalIgnoreCase).ToList();

        /// <summary>Whether a session of this name, found in this database, belongs in the result.</summary>
        public bool Reports(string database, string sessionName) =>
            (IsLongQuerySession(sessionName)
                ? LongQueryDatabases
                : AlwaysOnDatabases).Contains(database, StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// The sentence for a database the search could not open, and whether it fails the run. A monitored database is
        /// searched for every session, so one that cannot be opened is a problem (<see cref="XeSessionSearch.Problems"/>, exit
        /// 2). A database in <see cref="LongQueryDatabases"/> and not in <see cref="AlwaysOnDatabases"/> is one the
        /// registration excludes, opened only because a long-query session created before the exclusion stays there; the verb
        /// never opened it before that search, so a login with no user in it, or a paused serverless database, is a note
        /// (<see cref="XeSessionSearch.Notes"/>) that does not change the exit code.
        /// </summary>
        public (string Text, bool IsProblem) Unsearched(string database, string reason) =>
            AlwaysOnDatabases.Contains(database, StringComparer.OrdinalIgnoreCase)
                ? ($"Could not search database {database} for Extended Events sessions: {reason}", true)
                : ($"Database {database} is excluded from monitoring and could not be searched for the long-query completion session ({reason}), so a {LongQueryCompletionsCollector.LegacyXeSessionName} session left there needs a manual drop.", false);
    }

    /// <summary>
    /// The databases the search visits. <paramref name="monitored"/> is the registration's monitored databases;
    /// <paramref name="every"/> is every online database, with no exclusions.
    /// </summary>
    internal AzureSearchPlan PlanAzureSearch(IReadOnlyList<string> monitored, IReadOnlyList<string> every)
    {
        var alwaysOn = monitored.Where(database => !string.Equals(database, "master", StringComparison.OrdinalIgnoreCase)).ToList();
        if (!_sessionNames.Any(IsLongQuerySession))
        {
            return new AzureSearchPlan(alwaysOn, Array.Empty<string>());
        }

        /* The long-query session follows the worker's rule for a trace that is off (LongQueryTraceDatabases.Plan), the one
           the off-side reconcile applies: every listed database, the registration's exclusions not applied, because a session
           created before a database was excluded stays there. Never master, never a database monitored as its own server, and
           never a database where another registration of the logical server keeps the session (#4961). Another registration
           keeps it when its effective long-query setting is on, its own override or else the install's default, from the
           schedule rows the verb read (TraceOn). Its database scope is not read, so it counts as keeping the session in every
           database it does not exclude, which leaves more, never fewer. */
        var host = _server.Config.Host;
        var selfId = _server.ServerId.ToString(CultureInfo.InvariantCulture);
        var registrations = DarlingWorker.LongQueryTraceRegistrations(
            host, _registry, TraceOn, databaseScope: _ => Array.Empty<string>());
        var separatelyMonitored = AzureMasterScope.SeparatelyMonitoredDatabases(
            isAzureSqlDb: true, selfId, host, _server.Config.Database, DarlingWorker.LiveAlertTargets(_registry));
        var keptElsewhere = LongQueryTraceDatabases.KeptElsewhere(
            selfId, host, every, registrations, DarlingWorker.LongQueryTraceServerSeparatelyMonitored(host, _registry));

        var off = LongQueryTraceDatabases.Plan(enabled: false, every, Array.Empty<string>(), separatelyMonitored, keptElsewhere);
        return new AzureSearchPlan(alwaysOn, off.Drop);
    }

    /// <summary>
    /// Whether another registration's long-query trace is on, by its effective setting: its own override, else the install's
    /// default (<see cref="StoreConfigProvider.ResolveSchedule"/>), the same rule the service applies. With no schedule rows
    /// read the verb has nothing to go by, so every other registration counts as having it on.
    /// </summary>
    private bool TraceOn(int registrationId) =>
        _scheduleOverrides is null
        || StoreConfigProvider.ResolveSchedule(LongQueryCompletionsCollector.Instance.Name, registrationId, _scheduleOverrides).Enabled;

    /// <summary>
    /// The identity row of one registration, in the form the service's guard reads it (<see cref="ServerEpoch.LastKnownNameAsync"/>):
    /// the verb read each registration's last-known instance name from the store already, so it hands that name back as the
    /// row. No name is no row.
    /// </summary>
    private Task<Dictionary<string, string>> IdentityRowAsync(int registrationId, string carrier)
    {
        var state = new Dictionary<string, string>();
        if (_instanceNames is not null && _instanceNames.TryGetValue(registrationId, out var name))
        {
            state[ServerEpoch.IdentityStateKey] = ServerEpoch.Serialize(new ServerEpoch.Stamp(null, name));
        }

        return Task.FromResult(state);
    }

    /// <summary>
    /// The result of the server-scope search (every engine but Azure SQL Database) for the names the catalog gave: the sessions of
    /// this install and the shared ones, and the sessions of other installs. Split from the connection so a test drives it
    /// without a server.
    ///
    /// <para>#4961: the long-query session of this install is the instance's own, so it is left out of the sessions to drop,
    /// with a note that says why, when <see cref="LongQueryTraceInstanceGuard.Kept"/> finds another registration of this
    /// install on the same instance with its long-query trace on. That is the guard the service builds
    /// (<see cref="DarlingWorker.LongQueryTraceInstanceGuardFor"/>), fed from the facts the verb read. A name that is not
    /// known matches nothing, so the session is dropped, as the service drops it when its trace turns off. The old shared
    /// long-query session and the shared sessions belong to no registration of this install, so they are never left.</para>
    /// </summary>
    internal async Task<XeSessionSearch> ServerScopeSearchAsync(IReadOnlyList<string> foundNames, IReadOnlyList<string> otherNames)
    {
        var found = foundNames.Select(name => new ExistingXeSession(name, XeSessionScope.Server)).ToList();
        var others = otherNames.Select(name => new ExistingXeSession(name, XeSessionScope.Server)).ToList();
        var notes = new List<string>();

        if (_installId is not null)
        {
            var ownLongQuery = LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, _installId);
            var own = found.Find(session => string.Equals(session.Name, ownLongQuery, StringComparison.OrdinalIgnoreCase));

            /* Asked only when the session is there to drop, so a server without it reads no registry and no state. */
            if (own is not null && (await DarlingWorker.LongQueryTraceInstanceGuardFor(_server.ServerId, _registry, TraceOn, IdentityRowAsync)).Kept)
            {
                found.Remove(own);
                notes.Add($"Left in place: {DarlingXeSessionCleanup.Describe(own)}. Another registration of this install keeps it on the same instance.");
            }
        }

        return new XeSessionSearch(found, new List<string>()) { Others = others, Notes = notes };
    }

    public async Task<XeSessionSearch> FindSessionsAsync(CancellationToken cancellationToken)
    {
        if (!_server.Target.IsAzureSqlDb)
        {
            using var connection = new SqlConnection(_server.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            var foundNames = await ReadNamesAsync(connection, FindServerSql, cancellationToken);
            var otherNames = FindOthersServerSql is null
                ? new List<string>()
                : await ReadNamesAsync(connection, FindOthersServerSql, cancellationToken);
            return await ServerScopeSearchAsync(foundNames, otherNames);
        }

        var monitored = await ListAzureDatabasesAsync(applyExclusions: true, cancellationToken);

        /* The second listing only where exclusions could make it differ, and only for a search that includes the long-query
           session. */
        var every = _server.Config.ExcludedDatabases.Count > 0
            && _sessionNames.Any(IsLongQuerySession)
                ? await ListAzureDatabasesAsync(applyExclusions: false, cancellationToken)
                : monitored;
        return await SearchAzureDatabasesAsync(
            PlanAzureSearch(monitored, every), NamesInDatabaseAsync, cancellationToken, FindOthersDatabaseSql is null ? null : OthersInDatabaseAsync);
    }

    /// <summary>
    /// The Azure SQL Database search over the plan's databases, with the read of one database passed in so a test can make one
    /// refuse the connection without a server. A database that cannot be searched is filed by <see cref="AzureSearchPlan.Unsearched"/>:
    /// a monitored one as a problem, an excluded one as a note.
    /// </summary>
    internal static async Task<XeSessionSearch> SearchAzureDatabasesAsync(
        AzureSearchPlan plan,
        Func<string, CancellationToken, Task<IReadOnlyList<string>>> namesInDatabase,
        CancellationToken cancellationToken,
        Func<string, CancellationToken, Task<IReadOnlyList<string>>>? othersInDatabase = null)
    {
        var found = new List<ExistingXeSession>();
        var problems = new List<string>();
        var notes = new List<string>();
        var others = new List<ExistingXeSession>();

        foreach (var database in plan.Visited)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                foreach (var name in await namesInDatabase(database, cancellationToken))
                {
                    if (plan.Reports(database, name))
                    {
                        found.Add(new ExistingXeSession(name, XeSessionScope.Database, database));
                    }
                }

                /* The sessions of other installs are listed from every database the search opened. */
                if (othersInDatabase is not null)
                {
                    foreach (var name in await othersInDatabase(database, cancellationToken))
                    {
                        others.Add(new ExistingXeSession(name, XeSessionScope.Database, database));
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var (text, isProblem) = plan.Unsearched(database, ex.Message);
                (isProblem ? problems : notes).Add(text);
            }
        }

        return new XeSessionSearch(found, problems) { Notes = notes, Others = others };
    }

    private async Task<IReadOnlyList<string>> NamesInDatabaseAsync(string database, CancellationToken cancellationToken)
    {
        using var connection = new SqlConnection(SqlServerTargetProvider.Instance.WithDatabase(_server.ConnectionString, database));
        await connection.OpenAsync(cancellationToken);
        return await ReadNamesAsync(connection, FindDatabaseSql, cancellationToken);
    }

    private async Task<IReadOnlyList<string>> OthersInDatabaseAsync(string database, CancellationToken cancellationToken)
    {
        using var connection = new SqlConnection(SqlServerTargetProvider.Instance.WithDatabase(_server.ConnectionString, database));
        await connection.OpenAsync(cancellationToken);
        return await ReadNamesAsync(connection, FindOthersDatabaseSql!, cancellationToken);
    }

    /// <summary>A test's stand-in for the connection a drop opens: called with the connection string it would open, it answers
    /// the database the drop's statements run through. Null in production, which opens a SQL connection.</summary>
    internal Func<string, CancellationToken, Task<IAlwaysOnXeDatabase>>? OpenDatabaseForTests { get; set; }

    /// <summary>The STOP that goes before the DROP of one found session over a registration with read-only intent (#4961). It is
    /// refused unless the name is one of the names this target was given, spelled exactly, as <see cref="StatementFor"/> is, and
    /// it is for a database-scoped session only: the stop exists for the read-only replicas of Azure SQL Database. Otherwise it is
    /// the statement <see cref="DarlingXeSessionCleanup.ComposeStopStatement"/> builds, which is also what
    /// <see cref="DarlingXeSessionCleanup.StopStatement"/> is for Darling's own names.</summary>
    internal string StopStatementFor(XeSessionDrop drop)
    {
        ArgumentNullException.ThrowIfNull(drop);

        var session = drop.Session;
        if (session.Scope != XeSessionScope.Database)
        {
            throw new ArgumentException("Only a database-scoped session is stopped before it is dropped.", nameof(drop));
        }

        if (!_sessionNames.Contains(session.Name, StringComparer.Ordinal))
        {
            throw new ArgumentException("Only the session names this target was given can be stopped by it.", nameof(drop));
        }

        return DarlingXeSessionCleanup.ComposeStopStatement(session.Name);
    }

    /// <summary>
    /// Drops one found session. On Azure SQL Database a session cannot be dropped over a read-only connection, so for a
    /// registration with read-only intent the drop goes in the order the engine documents for a session that runs on a read-only
    /// replica (#4961): the session is stopped over the registration's own connection, only when it runs there, and then dropped
    /// over a connection with the intent forced off, which reaches the primary. Every other target, and every server-scoped one, sends
    /// the one DROP over its own connection.
    /// </summary>
    public async Task DropAsync(XeSessionDrop drop, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(drop);

        /* Built before any connection opens, so a name this target may not drop is refused without touching the server. */
        var statement = StatementFor(drop);
        var connectionString = drop.Session.Scope == XeSessionScope.Database
            ? SqlServerTargetProvider.Instance.WithDatabase(_server.ConnectionString, drop.Session.Database!)
            : _server.ConnectionString;
        var readOnlyIntent = drop.Session.Scope == XeSessionScope.Database && DarlingXeSessions.HasReadOnlyIntent(connectionString);
        var stop = readOnlyIntent ? StopStatementFor(drop) : null;

        var own = await OpenDatabaseAsync(connectionString, cancellationToken);
        IAlwaysOnXeDatabase database = own;
        try
        {
            if (!readOnlyIntent)
            {
                await own.ExecuteAsync(statement, cancellationToken);
                return;
            }

            /* The services' own wrapper: reads and the stop go over the own connection, the drop over one without the intent. */
            database = DarlingAlwaysOnXeSessions.WithReadOnlyIntent(connectionString, own, OpenDatabaseForTests);
            if (await database.IsStartedAsync(drop.Session.Name, cancellationToken))
            {
                await database.ExecuteAsync(stop!, cancellationToken);
            }

            await database.ExecuteWithoutReadOnlyIntentAsync(statement, cancellationToken);
        }
        finally
        {
            (database as IDisposable)?.Dispose();
            if (!ReferenceEquals(database, own))
            {
                (own as IDisposable)?.Dispose();
            }
        }
    }

    private async Task<IAlwaysOnXeDatabase> OpenDatabaseAsync(string connectionString, CancellationToken cancellationToken)
    {
        if (OpenDatabaseForTests is { } open)
        {
            return await open(connectionString, cancellationToken);
        }

        var connection = new SqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken);
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }

        return new DarlingAlwaysOnXeSessions.Database(connection, ownsConnection: true);
    }

    private static async Task<List<string>> ReadNamesAsync(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        var names = new List<string>();
        using var command = new SqlCommand(sql, connection) { CommandTimeout = CommandTimeoutSeconds };
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    /// <summary>
    /// The databases the search lists on Azure SQL Database, by the collector runner's rule (see its
    /// <c>GetAzureDatabaseListAsync</c>): a registration that names a database sweeps that database alone (#2220), and one
    /// that names none enumerates master through the provider's own plan. With <paramref name="applyExclusions"/> the list is
    /// the one <see cref="DarlingXeSessions.EnsureAllAsync"/> visits, minus the registration's excluded databases; without it,
    /// every online database, as the long-query trace's off-side reconcile lists them. Unlike the runner there is no fallback
    /// when master cannot be read (a registration that names no database has nothing to fall back to), so the failure
    /// propagates and the verb reports the server as unavailable.
    /// </summary>
    private async Task<IReadOnlyList<string>> ListAzureDatabasesAsync(bool applyExclusions, CancellationToken cancellationToken)
    {
        var own = AzureSweepScope.OwnDatabaseOrEmpty(new SqlConnectionStringBuilder(_server.ConnectionString).InitialCatalog);
        if (own.Count > 0)
        {
            return own;
        }

        var (masterConnectionString, query) = DarlingCollectorRunner.AzureDatabaseListPlan(_server, databaseScope: null, applyExclusions);

        var databases = new List<string>();
        using var connection = new SqlConnection(masterConnectionString);
        await connection.OpenAsync(cancellationToken);
        using var command = DarlingCollectorRunner.CreateCollectorCommand(query, connection, CommandTimeoutSeconds);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            databases.Add(reader.GetString(0));
        }

        return databases;
    }
}
