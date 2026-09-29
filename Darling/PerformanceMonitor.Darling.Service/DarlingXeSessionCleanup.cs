/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
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
/// <see cref="DarlingXeSessionCleanup.DropStatement"/>, which accepts only Darling's own two names, so no code path can
/// hand the executor a statement that did not come from the allow-list.
/// </summary>
public sealed record XeSessionDrop(ExistingXeSession Session)
{
    /// <summary>The single statement that drops <see cref="Session"/>, bracket-quoted.</summary>
    public string Statement => DarlingXeSessionCleanup.DropStatement(Session.Name, Session.Scope);
}

/// <summary>What a search of one target found: the Darling sessions that exist, and a sentence for every place that could
/// not be searched (an Azure SQL Database database that refused the connection, say).</summary>
public sealed record XeSessionSearch(IReadOnlyList<ExistingXeSession> Sessions, IReadOnlyList<string> Problems);

/// <summary>
/// The seam between the cleanup verb's decisions and the SQL Server it runs against, so the whole verb is testable
/// without one (#4732). The real implementation is <see cref="SqlServerXeSessionCleanupTarget"/>.
/// </summary>
public interface IXeSessionCleanupTarget
{
    /// <summary>Which of Darling's sessions exist on the target. Throws when the target itself cannot be searched; a
    /// place inside it that cannot be searched comes back as a <see cref="XeSessionSearch.Problems"/> entry instead.</summary>
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
/// connect or cycle), and a server that is unreachable from the service cannot be cleaned at all. The cost of leaving them
/// is small and bounded (a 4 MB ring buffer each), so an operator who wants them gone runs this deliberately.</para>
///
/// <para><b>Only two names, never one from input.</b> <see cref="SessionNames"/> is the deadlock and blocked-process
/// sessions the ensure lifecycle in <see cref="DarlingXeSessions"/> creates, taken from the same constants the collectors
/// read. <see cref="DropStatement"/> refuses any other name, and <see cref="PlanDrops"/> rewrites a matching name to the
/// constant before it builds a statement, so what reaches a server is always Darling's own spelling, bracket-quoted. The
/// opt-in long-query completion session is not in the list: the service drops it itself when its collector is disabled
/// while the server is still monitored.</para>
///
/// <para><b>Plan and executor are separate.</b> <see cref="PlanDrops"/> and <see cref="GuardedDropScript"/> are pure and
/// pin as text. <see cref="RunAsync"/> is the thin executor over <see cref="IXeSessionCleanupTarget"/>, so tests drive the
/// find-print-drop path with a fake target.</para>
/// </summary>
public static class DarlingXeSessionCleanup
{
    /// <summary>The two sessions this verb may drop, in the order it reports and drops them.</summary>
    public static IReadOnlyList<string> SessionNames { get; } = new[]
    {
        DeadlocksCollector.XeSessionName,
        BlockedProcessReportCollector.XeSessionName,
    };

    /// <summary>
    /// The one warning both modes print, without its prefix. It says only what the code does: Lite ensures a missing
    /// session on every collection cycle, and <see cref="DarlingXeSessions.EnsureAllAsync"/> creates a missing session
    /// (server-scoped, or in each database on Azure SQL Database) when a Darling service next connects to the server.
    /// </summary>
    public const string SharedNamesWarning =
        "a Lite app or another Darling service that still monitors this server uses the same session names and "
        + "creates a missing session again (Lite on its next collection cycle, Darling on its next connect to the server), "
        + "so run this only once nothing else monitors it.";

    /// <summary>Server-scoped sessions of Darling's two names. Composed from the constants, so no input reaches it.</summary>
    internal static readonly string FindServerSessionsSql = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    ses.name
FROM sys.server_event_sessions AS ses
WHERE ses.name IN (N'{DeadlocksCollector.XeSessionName}', N'{BlockedProcessReportCollector.XeSessionName}');";

    /// <summary>Database-scoped sessions of Darling's two names, read inside one Azure SQL Database database.</summary>
    internal static readonly string FindDatabaseSessionsSql = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    des.name
FROM sys.database_event_sessions AS des
WHERE des.name IN (N'{DeadlocksCollector.XeSessionName}', N'{BlockedProcessReportCollector.XeSessionName}');";

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

        return scope switch
        {
            XeSessionScope.Server => $"DROP EVENT SESSION {BracketQuote(canonical)} ON SERVER;",
            XeSessionScope.Database => $"DROP EVENT SESSION {BracketQuote(canonical)} ON DATABASE;",
            _ => throw new ArgumentOutOfRangeException(nameof(scope)),
        };
    }

    /// <summary>The constant spelling of <paramref name="name"/> when it is one of Darling's two names, else null.</summary>
    private static string? Canonical(string? name) =>
        SessionNames.FirstOrDefault(n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// The drops for the sessions that were found: pure, so "which statements for these existing sessions" pins without a
    /// server. Anything that is not one of Darling's two names is ignored (a server can hold any number of other
    /// sessions); a name that differs from the constant only by case is planned under the constant. The order is fixed:
    /// server scope before database scope, databases by name, then deadlock before blocked-process. Duplicates collapse.
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
    /// unless <paramref name="dryRun"/>, print the shared-names warning. Returns
    /// <see cref="DarlingCliCommands.DropXeSessionsExitCode.Success"/> when everything found was dropped (or listed, in a dry
    /// run) or nothing was there, and <see cref="DarlingCliCommands.DropXeSessionsExitCode.TargetUnavailable"/> when the target
    /// could not be searched, part of it could not be searched, or a drop was refused. A refused drop does not stop the ones
    /// after it.
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

        var drops = PlanDrops(search.Sessions);
        if (drops.Count == 0)
        {
            output.WriteLine($"No Darling Extended Events sessions ({string.Join(", ", SessionNames)}) were found on '{serverLabel}'; nothing to drop.");
        }

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
        output.WriteLine("WARNING: " + SharedNamesWarning);
        return exitCode;
    }

    /// <summary>What to do when the server cannot be reached from where the verb runs.</summary>
    internal const string PrintSqlHint =
        "If the server cannot be reached from this machine, run --drop-xe-sessions --print-sql to print the DROP statements and run them where it can.";
}

/// <summary>
/// <see cref="IXeSessionCleanupTarget"/> over a connected SQL Server target: the ServerRuntime the shared connector produced
/// (#4732). On Azure SQL Database it visits the same databases <see cref="DarlingXeSessions.EnsureAllAsync"/> would, by the
/// same rule (a registration that names a database is that database alone; one that names none enumerates master through the
/// provider's own plan, honoring the server's excluded databases), and skips master, which cannot host a session.
/// </summary>
internal sealed class SqlServerXeSessionCleanupTarget : IXeSessionCleanupTarget
{
    private const int CommandTimeoutSeconds = 60;

    private readonly ServerRuntime _server;

    public SqlServerXeSessionCleanupTarget(ServerRuntime server)
    {
        _server = server ?? throw new ArgumentNullException(nameof(server));
    }

    public async Task<XeSessionSearch> FindSessionsAsync(CancellationToken cancellationToken)
    {
        var found = new List<ExistingXeSession>();
        var problems = new List<string>();

        if (!_server.Target.IsAzureSqlDb)
        {
            using var connection = new SqlConnection(_server.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            foreach (var name in await ReadNamesAsync(connection, DarlingXeSessionCleanup.FindServerSessionsSql, cancellationToken))
            {
                found.Add(new ExistingXeSession(name, XeSessionScope.Server));
            }

            return new XeSessionSearch(found, problems);
        }

        foreach (var database in await ListAzureDatabasesAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (string.Equals(database, "master", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            try
            {
                using var connection = new SqlConnection(SqlServerTargetProvider.Instance.WithDatabase(_server.ConnectionString, database));
                await connection.OpenAsync(cancellationToken);
                foreach (var name in await ReadNamesAsync(connection, DarlingXeSessionCleanup.FindDatabaseSessionsSql, cancellationToken))
                {
                    found.Add(new ExistingXeSession(name, XeSessionScope.Database, database));
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                problems.Add($"Could not search database {database} for Extended Events sessions: {ex.Message}");
            }
        }

        return new XeSessionSearch(found, problems);
    }

    public async Task DropAsync(XeSessionDrop drop, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(drop);

        var connectionString = drop.Session.Scope == XeSessionScope.Database
            ? SqlServerTargetProvider.Instance.WithDatabase(_server.ConnectionString, drop.Session.Database!)
            : _server.ConnectionString;

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        using var command = new SqlCommand(drop.Statement, connection) { CommandTimeout = CommandTimeoutSeconds };
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
    /// The databases <see cref="DarlingXeSessions.EnsureAllAsync"/> visits on Azure SQL Database, by the collector runner's
    /// rule (see its <c>GetAzureDatabaseListAsync</c>): a registration that names a database sweeps that database alone
    /// (#2220), and one that names none enumerates master through the provider's own plan. Unlike the runner there is no
    /// fallback when master cannot be read (a registration that names no database has nothing to fall back to), so the
    /// failure propagates and the verb reports the server as unavailable.
    /// </summary>
    private async Task<IReadOnlyList<string>> ListAzureDatabasesAsync(CancellationToken cancellationToken)
    {
        var own = AzureSweepScope.OwnDatabaseOrEmpty(new SqlConnectionStringBuilder(_server.ConnectionString).InitialCatalog);
        if (own.Count > 0)
        {
            return own;
        }

        var (masterConnectionString, query) = SqlServerTargetProvider.Instance.BuildDatabaseListPlan(
            _server.ConnectionString, _server.Config.ExcludedDatabases, databaseScope: null);

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
