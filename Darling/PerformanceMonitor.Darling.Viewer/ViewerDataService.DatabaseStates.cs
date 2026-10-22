/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's reads/writes for the database-state alert's per-(server, database) expected state
/// (<c>config.database_state_expected</c>) — the override editor's backing store. The SQL is
/// <see cref="DatabaseStateExpectedStore"/>'s, shared with the MCP tools (#6397); it mirrors the service's
/// <c>DarlingAlertReadAdapter</c> seed/deviation SQL so the editor and the alert agree on what "expected"
/// means: opening the editor seeds a first-observation baseline for any current database that has none
/// (skipped on a read-only seat, which can't write — the service seeds during its sweeps anyway). Writes
/// route through <see cref="ExecuteWriteAsync"/> so a read-only <c>viewer</c> seat degrades to
/// <see cref="ViewerReadOnlyException"/> rather than a raw 42501.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Every database in the latest snapshot for the override editor: current state, expected state, and
    /// whether the expected state is a user override. Seeds missing baselines first (unless read-only).
    /// </summary>
    public async Task<List<DatabaseStateExpectedRow>> GetDatabaseStateExpectationsAsync(int serverId, CancellationToken cancellationToken = default)
    {
        if (!IsReadOnly)
        {
            await using var seed = DatabaseStateExpectedStore.CreateSeedCommand(_dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);
            await seed.ExecuteNonQueryAsync(cancellationToken);

            await using var heal = DatabaseStateExpectedStore.CreateHealCommand(_dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);
            await heal.ExecuteNonQueryAsync(cancellationToken);
        }

        var rows = new List<DatabaseStateExpectedRow>();
        foreach (var r in await DatabaseStateExpectedStore.ReadAsync(_dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken))
        {
            rows.Add(new DatabaseStateExpectedRow
            {
                DatabaseName = r.DatabaseName,
                CurrentState = r.CurrentState,
                ExpectedState = r.ExpectedState ?? "",
                IsUserOverride = r.IsUserOverride
            });
        }

        return rows;
    }

    /// <summary>Sets a database's expected state (the override); pass <see cref="DatabaseStateTokens.Ignore"/>
    /// to opt the database out of the alert.</summary>
    public async Task SetDatabaseStateExpectedAsync(int serverId, string databaseName, string expectedState, CancellationToken cancellationToken = default)
    {
        await using var command = DatabaseStateExpectedStore.CreateSetCommand(_dataSource, serverId, databaseName, expectedState, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Re-baselines a database: sets its expected state to its current state and clears the override flag.</summary>
    public async Task ResetDatabaseStateExpectedToCurrentAsync(int serverId, string databaseName, CancellationToken cancellationToken = default)
    {
        await using var command = DatabaseStateExpectedStore.CreateResetToCurrentCommand(_dataSource, serverId, databaseName, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);
        await ExecuteWriteAsync(command, cancellationToken);
    }
}

/// <summary>
/// One row of the database-state override editor: a database's current collected state alongside its
/// expected state (auto-seeded baseline or user override).
/// </summary>
public sealed class DatabaseStateExpectedRow
{
    public string DatabaseName { get; set; } = "";
    public string CurrentState { get; set; } = "";
    public string ExpectedState { get; set; } = "";
    public bool IsUserOverride { get; set; }

    public bool IsIgnored => ExpectedState == DatabaseStateTokens.Ignore;

    public bool IsDeviating => !IsIgnored && !string.Equals(CurrentState, ExpectedState, System.StringComparison.Ordinal);
}
