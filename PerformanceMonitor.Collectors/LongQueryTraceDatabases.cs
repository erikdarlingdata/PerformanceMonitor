/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Where the opt-in long-query trace keeps its Extended Events session on Azure SQL Database, where each database
/// holds its own session. Lite and Darling both plan every create and drop here, so the two apps touch the same
/// databases.
/// <list type="bullet">
/// <item>Trace ON: create the session in each monitored database, and drop it from each listed database outside
/// that set. The trace costs overhead, so it must not run in a database the user left out.</item>
/// <item>Trace OFF: drop it from every listed database. That list is the full one, with no exclusions and no
/// database scope, so a session created before a database was left out is removed too.</item>
/// <item>Both: never touch <c>master</c>, which cannot hold a database-scoped session, or a database monitored as
/// its own server (<c>AzureMasterScope.SeparatelyMonitoredDatabases</c>). That database's own registration creates
/// and drops its session, so the logical server's registration leaves it alone.</item>
/// </list>
/// The always-on deadlock and blocked-process sessions do not use this. They follow the inventory: they are
/// created in every inventoried database whatever the database scope, because they are cheap and the alerts read
/// them.
/// </summary>
public static class LongQueryTraceDatabases
{
    /// <summary>
    /// Failed cleanup passes in a row, for one registration, before it stops retrying. Without a cap, a database
    /// that refuses the drop forever would repeat a warning every cycle forever.
    /// </summary>
    public const int DropAttemptCap = 5;

    /* A character no database name can contain, so a joined list cannot read two ways. */
    private const char KeySeparator = '\u001F';

    /// <summary>
    /// The databases one reconcile creates the session in and drops it from.
    /// </summary>
    /// <param name="enabled">Whether the trace is on for this registration.</param>
    /// <param name="listedDatabases">Every online database the registration can see, with no exclusions and no
    /// database scope. For a registration that names a database, that database alone.</param>
    /// <param name="monitoredDatabases">The databases the registration monitors: the listed ones minus its
    /// exclusions and, in Darling, narrowed by the trace's database scope. Read only while the trace is on.</param>
    /// <param name="separatelyMonitoredDatabases">The databases monitored as their own servers.</param>
    public static LongQueryTracePlan Plan(
        bool enabled,
        IEnumerable<string> listedDatabases,
        IEnumerable<string> monitoredDatabases,
        IEnumerable<string> separatelyMonitoredDatabases)
    {
        var ownedElsewhere = new HashSet<string>(separatelyMonitoredDatabases, StringComparer.OrdinalIgnoreCase);
        bool CanTouch(string database) =>
            !string.Equals(database, "master", StringComparison.OrdinalIgnoreCase)
            && !ownedElsewhere.Contains(database);

        if (!enabled)
        {
            return new LongQueryTracePlan(Array.Empty<string>(), DistinctInOrder(listedDatabases.Where(CanTouch)));
        }

        var create = DistinctInOrder(monitoredDatabases.Where(CanTouch));
        var keep = new HashSet<string>(monitoredDatabases, StringComparer.OrdinalIgnoreCase);
        var drop = DistinctInOrder(listedDatabases.Where(database => CanTouch(database) && !keep.Contains(database)));
        return new LongQueryTracePlan(create, drop);
    }

    /// <summary>
    /// The settings a plan depends on, apart from the database lists. When this changes, the last reconcile no
    /// longer says where the session belongs, so the reconcile runs again. While the trace is on that is the
    /// database scope, the exclusions and the separately monitored databases. While it is off the plan ignores
    /// the scope and the exclusions, so only the separately monitored databases count.
    /// </summary>
    public static string StateKey(
        bool enabled,
        IEnumerable<string>? databaseScope,
        IEnumerable<string>? excludedDatabases,
        IEnumerable<string> separatelyMonitoredDatabases)
    {
        var owned = JoinNames(separatelyMonitoredDatabases);
        return enabled
            ? string.Join(KeySeparator, "on", JoinNames(databaseScope), JoinNames(excludedDatabases), owned)
            : string.Join(KeySeparator, "off", owned);
    }

    /// <summary>
    /// Drops the session in each database, trying every one before it gives up, so one database that refuses
    /// cannot keep the session in the others. When any drop failed, throws one
    /// <see cref="LongQueryTraceDropException"/> that names each database where it failed.
    /// </summary>
    /// <param name="databases">The databases to drop the session from (a plan's <see cref="LongQueryTracePlan.Drop"/>).</param>
    /// <param name="dropOne">Drops the session in one database.</param>
    /// <param name="onFailure">Called once for each database whose drop failed, before the next one is tried. The
    /// apps log a warning that names the database here.</param>
    /// <param name="createNote">Trace ON only: the create side's note about databases it could not create the
    /// session in, carried on the exception so a failed drop does not lose it.</param>
    /// <param name="cancellationToken">Cancels the remaining drops.</param>
    public static async Task DropEachAsync(
        IReadOnlyList<string> databases,
        Func<string, CancellationToken, Task> dropOne,
        Action<string, Exception> onFailure,
        string? createNote,
        CancellationToken cancellationToken)
    {
        var failed = new List<string>();
        Exception? firstFailure = null;

        foreach (var database in databases)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await dropOne(database, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed.Add(database);
                firstFailure ??= ex;
                onFailure(database, ex);
            }
        }

        if (firstFailure is not null)
        {
            throw new LongQueryTraceDropException(failed, firstFailure, createNote);
        }
    }

    /// <summary>
    /// The one warning a registration logs when it stops retrying (<see cref="DropAttemptCap"/>). It names each
    /// database where the session may remain. When the last pass could not list the databases there are no
    /// names to give, and the warning says so.
    /// </summary>
    public static string GiveUpWarning(IReadOnlyList<string> databases) => databases.Count == 0
        ? $"Stopped retrying the long-query trace cleanup after {DropAttemptCap} failed attempts in a row. The databases could not be listed, so the session may remain in any of them. The next attempt comes after a restart or a change to the trace's settings."
        : $"Stopped retrying the long-query trace cleanup after {DropAttemptCap} failed attempts in a row. The session may remain in: {string.Join(", ", databases)}. The next attempt comes after a restart or a change to the trace's settings.";

    private static string JoinNames(IEnumerable<string>? names) =>
        names is null
            ? string.Empty
            : string.Join(KeySeparator, names
                .Select(name => name.Trim())
                .Where(name => name.Length > 0)
                .Select(name => name.ToUpperInvariant())
                .Distinct(StringComparer.Ordinal)
                .OrderBy(name => name, StringComparer.Ordinal));

    private static List<string> DistinctInOrder(IEnumerable<string> names)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<string>();
        foreach (var name in names)
        {
            if (seen.Add(name))
            {
                result.Add(name);
            }
        }

        return result;
    }
}

/// <summary>What one long-query trace reconcile does on Azure SQL Database (<see cref="LongQueryTraceDatabases.Plan"/>).</summary>
/// <param name="Create">The databases to create the session in. Empty while the trace is off.</param>
/// <param name="Drop">The databases to drop the session from.</param>
public sealed record LongQueryTracePlan(IReadOnlyList<string> Create, IReadOnlyList<string> Drop);

/// <summary>
/// Counts one registration's failed cleanup passes in a row, for <see cref="LongQueryTraceDatabases.DropAttemptCap"/>.
/// The count belongs to one state key (<see cref="LongQueryTraceDatabases.StateKey"/>): a pass that succeeds starts
/// it again, and so does any change to the key, such as turning the trace on or off.
/// </summary>
public sealed class LongQueryTraceDropRetry
{
    private readonly object _gate = new();
    private string? _stateKey;
    private int _failures;

    /// <summary>Failed passes in a row under the current state key.</summary>
    public int ConsecutiveFailures
    {
        get
        {
            lock (_gate)
            {
                return _failures;
            }
        }
    }

    /// <summary>
    /// Records a failed pass. True when it is the last one the cap allows: the caller then marks the reconcile done
    /// and logs <see cref="LongQueryTraceDatabases.GiveUpWarning"/> once.
    /// </summary>
    public bool RecordFailure(string stateKey)
    {
        lock (_gate)
        {
            if (!string.Equals(_stateKey, stateKey, StringComparison.Ordinal))
            {
                _stateKey = stateKey;
                _failures = 0;
            }

            _failures++;
            if (_failures < LongQueryTraceDatabases.DropAttemptCap)
            {
                return false;
            }

            _stateKey = null;
            _failures = 0;
            return true;
        }
    }

    /// <summary>Starts the count again: after a pass that succeeds, and in Darling after every reconnect.</summary>
    public void Reset()
    {
        lock (_gate)
        {
            _stateKey = null;
            _failures = 0;
        }
    }
}

/// <summary>
/// A long-query trace reconcile that could not finish its drops on Azure SQL Database: the database list could not
/// be read, or the drop failed in some databases (the others were still dropped). The caller does not mark the
/// reconcile done, so the next cycle tries again, up to <see cref="LongQueryTraceDatabases.DropAttemptCap"/> passes
/// in a row.
/// </summary>
public sealed class LongQueryTraceDropException : Exception
{
    /// <summary>Creates the exception for the given databases. An empty list means the databases could not be listed.</summary>
    public LongQueryTraceDropException(IReadOnlyList<string> databases, Exception firstFailure, string? createNote = null)
        : base(Describe(databases, firstFailure), firstFailure)
    {
        Databases = databases;
        CreateNote = createNote;
    }

    /// <summary>The databases where the drop failed, so the session may remain there. Empty when the list could not be read.</summary>
    public IReadOnlyList<string> Databases { get; }

    /// <summary>Trace ON only: the create side's note about databases it could not create the session in.</summary>
    public string? CreateNote { get; }

    private static string Describe(IReadOnlyList<string> databases, Exception firstFailure) => databases.Count == 0
        ? $"Could not list the databases to drop the long-query trace session from: {firstFailure.Message}"
        : $"Could not drop the long-query trace session in {databases.Count} database(s) ({string.Join(", ", databases)}): {firstFailure.Message}";
}
