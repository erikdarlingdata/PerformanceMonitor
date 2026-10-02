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
public sealed record XeSessionDrop(ExistingXeSession Session)
{
    /// <summary>The single statement that drops <see cref="Session"/>, bracket-quoted.</summary>
    public string Statement => DarlingXeSessionCleanup.DropStatement(Session.Name, Session.Scope);
}

/// <summary>What a search of one target found: the Darling sessions that exist, and a sentence for every monitored place
/// that could not be searched (an Azure SQL Database database that refused the connection, say), each of which fails the
/// run. <see cref="Notes"/> holds the sentences for a place the registration excludes, which the search opened only for the
/// long-query session and could not search: it says where a session may be left and does not fail the run.</summary>
public sealed record XeSessionSearch(IReadOnlyList<ExistingXeSession> Sessions, IReadOnlyList<string> Problems)
{
    /// <summary>A sentence for each excluded database that could not be searched for the long-query session. The verb prints
    /// each on stderr and does not change its exit code for it: before the search reached excluded databases, the verb never
    /// opened one, so an excluded database the login cannot open is not a reason to stop a script that removes the server
    /// next. A session left in such a database needs a manual drop.</summary>
    public IReadOnlyList<string> Notes { get; init; } = Array.Empty<string>();
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
/// The <c>--drop-xe-sessions</c> verb's plan and executor (#4732): removes the Extended Events sessions Darling leaves on a
/// server after the server stops being monitored.
///
/// <para><b>Why an explicit verb and not part of removing a server.</b> The service never drops these sessions when a
/// server is removed, for two reasons that do not go away: the names are shared with Lite and with any other Darling
/// service that monitors the same server (dropping them under a monitor that is still running blinds it until its next
/// connect or cycle), and a server that is unreachable from the service cannot be cleaned at all. The deadlock and
/// blocked-process sessions cost a 4 MB ring buffer each; the long-query completion session has a 4 MB ring buffer too
/// and also tests every completed statement and batch against its duration filter, which is why it is opt-in. An
/// operator who wants them gone runs this deliberately.</para>
///
/// <para><b>Only Darling's own names, never one from input.</b> <see cref="SessionNames"/> is the deadlock and
/// blocked-process sessions the ensure lifecycle in <see cref="DarlingXeSessions"/> creates, and the opt-in long-query
/// completion session its reconcile creates while that collector is on, all taken from the same constants the collectors
/// read. <see cref="DropStatement"/> refuses any other name, and <see cref="PlanDrops"/> rewrites a matching name to the
/// constant before it builds a statement, so what reaches a server is always Darling's own spelling, bracket-quoted.
/// The long-query completion session is in the list because the service drops it itself only for a server it still
/// monitors, when that collector is turned off (<see cref="DarlingXeSessions.ReconcileLongQueryCompletionsAsync"/>): a
/// server removed while the collector was on keeps the session, and this verb is the only thing that can drop it.</para>
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
        LongQueryCompletionsCollector.XeSessionName,
    };

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
    private static string CapturePhrase(string canonicalName) =>
        canonicalName == DeadlocksCollector.XeSessionName ? "deadlocks"
        : canonicalName == BlockedProcessReportCollector.XeSessionName ? "blocked processes"
        : canonicalName == LongQueryCompletionsCollector.XeSessionName ? "long query completions"
        : canonicalName;

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

        var droppedNames = dropped.Select(d => d.Name).ToHashSet(StringComparer.Ordinal);
        var captures = SessionNames.Where(droppedNames.Contains).Select(CapturePhrase).ToList();
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
    public static string DropStatement(string sessionName, XeSessionScope scope)
    {
        var canonical = Canonical(sessionName)
            ?? throw new ArgumentException("Only Darling's own session names can be dropped by this verb.", nameof(sessionName));
        if (!string.Equals(canonical, sessionName, StringComparison.Ordinal))
        {
            throw new ArgumentException("The session name is not spelled exactly as Darling creates it.", nameof(sessionName));
        }

        return ComposeDropStatement(canonical, scope);
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

    /// <summary>The constant spelling of <paramref name="name"/> when it is one of <see cref="SessionNames"/>, else null.</summary>
    private static string? Canonical(string? name) =>
        SessionNames.FirstOrDefault(n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// The drops for the sessions that were found: pure, so "which statements for these existing sessions" pins without a
    /// server. Anything that is not one of <see cref="SessionNames"/> is ignored (a server can hold any number of other
    /// sessions); a name that differs from the constant only by case is planned under the constant. The order is fixed:
    /// server scope before database scope, databases by name, then the order of <see cref="SessionNames"/> (deadlock,
    /// blocked-process, long-query completions). Duplicates collapse.
    /// </summary>
    public static IReadOnlyList<XeSessionDrop> PlanDrops(IEnumerable<ExistingXeSession> existing)
    {
        ArgumentNullException.ThrowIfNull(existing);

        var planned = new List<ExistingXeSession>();
        foreach (var session in existing)
        {
            var canonical = Canonical(session.Name);
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
            .ThenBy(p => SessionIndex(p.Name))
            .Select(p => new XeSessionDrop(p))
            .ToList();
    }

    private static int SessionIndex(string canonicalName)
    {
        for (var i = 0; i < SessionNames.Count; i++)
        {
            if (SessionNames[i] == canonicalName)
            {
                return i;
            }
        }

        return SessionNames.Count;
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
            lines.Add("IF EXISTS");
            lines.Add("(");
            lines.Add("    SELECT");
            lines.Add("        1/0");
            lines.Add("    FROM sys.server_event_sessions AS ses");
            lines.Add($"    WHERE ses.name = N'{name}'");
            lines.Add(")");
            lines.Add("BEGIN");
            lines.Add("    " + DropStatement(name, XeSessionScope.Server));
            lines.Add("END;");
            lines.Add(string.Empty);
        }

        lines.Add("-- Part 2 of 2 - run inside EACH monitored database on Azure SQL Database (a database-scoped session lives in its own database, never in master).");

        foreach (var name in SessionNames)
        {
            lines.Add("IF EXISTS");
            lines.Add("(");
            lines.Add("    SELECT");
            lines.Add("        1/0");
            lines.Add("    FROM sys.database_event_sessions AS des");
            lines.Add($"    WHERE des.name = N'{name}'");
            lines.Add(")");
            lines.Add("BEGIN");
            lines.Add("    " + DropStatement(name, XeSessionScope.Database));
            lines.Add("END;");
            lines.Add(string.Empty);
        }

        return string.Join(Environment.NewLine, lines);
    }

    /// <summary>
    /// The connected half of the verb, over an <see cref="IXeSessionCleanupTarget"/>: find the sessions, print each, drop each
    /// unless <paramref name="dryRun"/>, print what the drops stop (<see cref="CaptureStopsNote"/>, only when one was
    /// dropped) and the shared-names warning. Returns
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
        CancellationToken cancellationToken)
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

        var drops = PlanDrops(search.Sessions);
        if (drops.Count == 0)
        {
            /* "In the places searched": a database that could not be searched (a Problems entry, exit 2, or a note) may hold a session. */
            output.WriteLine($"No Darling Extended Events sessions ({string.Join(", ", SessionNames)}) were found on '{serverLabel}'; nothing to drop in the places searched.");
        }

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

        output.WriteLine();
        if (CaptureStopsNote(dropped) is { } captureStops)
        {
            output.WriteLine(captureStops);
        }

        output.WriteLine("WARNING: " + SharedNamesWarning);
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

    /// <param name="server">The connected server.</param>
    /// <param name="sessionNames">The names to search for and to drop, copied here. Every product caller leaves this null, which
    /// is <see cref="DarlingXeSessionCleanup.SessionNames"/>. The live test passes a test-only name so the real find and drop
    /// run against a real server without ever naming one of Darling's own sessions. Either way the target composes its find
    /// and drop text through the same methods the plan's constants and statements are built with, so the live test sends the
    /// text the product sends, and a test pins that a copy of Darling's names sends exactly the plan's text (#4732).</param>
    /// <param name="registry">The servers this service monitors, from the same list the verb resolved the target in. On Azure SQL
    /// Database it says which databases belong to another registration of the same logical server.</param>
    public SqlServerXeSessionCleanupTarget(
        ServerRuntime server, IReadOnlyList<string>? sessionNames = null, IReadOnlyList<MonitoredServer>? registry = null)
    {
        _server = server ?? throw new ArgumentNullException(nameof(server));
        _registry = registry ?? Array.Empty<MonitoredServer>();
        _sessionNames = (sessionNames ?? DarlingXeSessionCleanup.SessionNames).ToArray();
        if (_sessionNames.Length == 0 || _sessionNames.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException("A cleanup target needs at least one session name, and none of them blank.", nameof(sessionNames));
        }

        FindServerSql = DarlingXeSessionCleanup.ComposeFindSql(XeSessionScope.Server, _sessionNames);
        FindDatabaseSql = DarlingXeSessionCleanup.ComposeFindSql(XeSessionScope.Database, _sessionNames);
    }

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
            (string.Equals(sessionName, LongQueryCompletionsCollector.XeSessionName, StringComparison.OrdinalIgnoreCase)
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
                : ($"Database {database} is excluded from monitoring and could not be searched for the long-query completion session ({reason}), so a {LongQueryCompletionsCollector.XeSessionName} session left there needs a manual drop.", false);
    }

    /// <summary>
    /// The databases the search visits. <paramref name="monitored"/> is the registration's monitored databases;
    /// <paramref name="every"/> is every online database, with no exclusions.
    /// </summary>
    internal AzureSearchPlan PlanAzureSearch(IReadOnlyList<string> monitored, IReadOnlyList<string> every)
    {
        var alwaysOn = monitored.Where(database => !string.Equals(database, "master", StringComparison.OrdinalIgnoreCase)).ToList();
        if (!_sessionNames.Contains(LongQueryCompletionsCollector.XeSessionName, StringComparer.OrdinalIgnoreCase))
        {
            return new AzureSearchPlan(alwaysOn, Array.Empty<string>());
        }

        /* The long-query session follows the worker's rule for a trace that is off (LongQueryTraceDatabases.Plan), the one
           the off-side reconcile applies: every listed database, the registration's exclusions not applied, because a session
           created before a database was excluded stays there. Never master, never a database monitored as its own server, and
           never a database where another registration of the logical server keeps the session. The verb cannot read another
           registration's long-query schedule, so it counts every other registration as keeping it. */
        var host = _server.Config.Host;
        var selfId = _server.ServerId.ToString(CultureInfo.InvariantCulture);
        var registrations = DarlingWorker.LongQueryTraceRegistrations(
            host, _registry, traceOn: _ => true, databaseScope: _ => Array.Empty<string>());
        var separatelyMonitored = AzureMasterScope.SeparatelyMonitoredDatabases(
            isAzureSqlDb: true, selfId, host, _server.Config.Database, DarlingWorker.LiveAlertTargets(_registry));
        var keptElsewhere = LongQueryTraceDatabases.KeptElsewhere(
            selfId, host, every, registrations, DarlingWorker.LongQueryTraceServerSeparatelyMonitored(host, _registry));

        var off = LongQueryTraceDatabases.Plan(enabled: false, every, Array.Empty<string>(), separatelyMonitored, keptElsewhere);
        return new AzureSearchPlan(alwaysOn, off.Drop);
    }

    public async Task<XeSessionSearch> FindSessionsAsync(CancellationToken cancellationToken)
    {
        var found = new List<ExistingXeSession>();
        var problems = new List<string>();

        if (!_server.Target.IsAzureSqlDb)
        {
            using var connection = new SqlConnection(_server.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            foreach (var name in await ReadNamesAsync(connection, FindServerSql, cancellationToken))
            {
                found.Add(new ExistingXeSession(name, XeSessionScope.Server));
            }

            return new XeSessionSearch(found, problems);
        }

        var monitored = await ListAzureDatabasesAsync(applyExclusions: true, cancellationToken);

        /* The second listing only where exclusions could make it differ, and only for a search that includes the long-query
           session. */
        var every = _server.Config.ExcludedDatabases.Count > 0
            && _sessionNames.Contains(LongQueryCompletionsCollector.XeSessionName, StringComparer.OrdinalIgnoreCase)
                ? await ListAzureDatabasesAsync(applyExclusions: false, cancellationToken)
                : monitored;
        return await SearchAzureDatabasesAsync(PlanAzureSearch(monitored, every), NamesInDatabaseAsync, cancellationToken);
    }

    /// <summary>
    /// The Azure SQL Database search over the plan's databases, with the read of one database passed in so a test can make one
    /// refuse the connection without a server. A database that cannot be searched is filed by <see cref="AzureSearchPlan.Unsearched"/>:
    /// a monitored one as a problem, an excluded one as a note.
    /// </summary>
    internal static async Task<XeSessionSearch> SearchAzureDatabasesAsync(
        AzureSearchPlan plan,
        Func<string, CancellationToken, Task<IReadOnlyList<string>>> namesInDatabase,
        CancellationToken cancellationToken)
    {
        var found = new List<ExistingXeSession>();
        var problems = new List<string>();
        var notes = new List<string>();

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
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var (text, isProblem) = plan.Unsearched(database, ex.Message);
                (isProblem ? problems : notes).Add(text);
            }
        }

        return new XeSessionSearch(found, problems) { Notes = notes };
    }

    private async Task<IReadOnlyList<string>> NamesInDatabaseAsync(string database, CancellationToken cancellationToken)
    {
        using var connection = new SqlConnection(SqlServerTargetProvider.Instance.WithDatabase(_server.ConnectionString, database));
        await connection.OpenAsync(cancellationToken);
        return await ReadNamesAsync(connection, FindDatabaseSql, cancellationToken);
    }

    public async Task DropAsync(XeSessionDrop drop, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(drop);

        /* Built before any connection opens, so a name this target may not drop is refused without touching the server. */
        var statement = StatementFor(drop);
        var connectionString = drop.Session.Scope == XeSessionScope.Database
            ? SqlServerTargetProvider.Instance.WithDatabase(_server.ConnectionString, drop.Session.Database!)
            : _server.ConnectionString;

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        using var command = new SqlCommand(statement, connection) { CommandTimeout = CommandTimeoutSeconds };
        await command.ExecuteNonQueryAsync(cancellationToken);
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
