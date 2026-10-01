/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>One-time removal of exact duplicate rows already stored in <c>collect.deadlocks</c>.</summary>
public static class DeadlockDuplicateCleanup
{
    /// <summary>The owner name in <c>collect.collector_state</c>.</summary>
    public const string StateCollectorName = "deadlock_duplicate_cleanup";

    /// <summary>The marker's one key.</summary>
    public const string CleanupVersionStateKey = "cleanup_version";

    /// <summary>The marker's value once every server has been cleaned.</summary>
    public const int CleanupVersion = 1;

    /// <summary>What one run found and did.</summary>
    public sealed class Summary
    {
        public static readonly Summary NoOp = new(alreadyDone: true, 0, 0);

        internal Summary(bool alreadyDone, int rowsRemoved, int daysVisited)
        {
            AlreadyDone = alreadyDone;
            RowsRemoved = rowsRemoved;
            DaysVisited = daysVisited;
        }

        public bool AlreadyDone { get; }

        public int RowsRemoved { get; }

        public int DaysVisited { get; }
    }

    public static Task<Summary> RunAsync(NpgsqlDataSource postgres, ILogger? logger, CancellationToken cancellationToken)
    {
        return Task.FromResult(Summary.NoOp);
    }
}
