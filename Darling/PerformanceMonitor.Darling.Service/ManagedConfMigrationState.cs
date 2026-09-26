/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The pure gate <see cref="Classify"/> reads once per start, before <see cref="DarlingManagedPostgres.EnsureConfAppended"/>
/// runs (#4336): what state this data directory's conf is in, so the caller can
/// decide whether the legacy v1-v15 appenders may run at all, whether Step A's write step runs, and which
/// migration action (if any) belongs after this start.
/// </summary>
internal static class ManagedConfMigrationState
{
    /// <summary>
    /// <list type="bullet">
    /// <item><see cref="Legacy"/> — a v-marker is present in <c>postgresql.conf</c>, OR the managed
    /// <c>include</c> is missing. Only this state may run the legacy appenders
    /// (<see cref="DarlingManagedPostgres.EnsureConfAppended"/>); migration happens post-start.</item>
    /// <item><see cref="PendingVerify"/> — the conf carries the managed include (no v-marker), and
    /// <see cref="ManagedConfMigrationSteps.PendingFileName"/> exists: a crash landed between
    /// <see cref="ManagedConfMigrationSteps.WriteTwoSteps"/> and the stamp. Resume, never re-append.</item>
    /// <item><see cref="Verified"/> — the conf carries the include, no v-marker, no pending file, and
    /// <see cref="ManagedConfMigrationSteps.IsVerified"/> is true. Step A is done; Step B (#4336) is
    /// what runs from here on.</item>
    /// <item><see cref="MigratedUnstamped"/> — the conf carries the include, no v-marker, no pending file,
    /// but the stamp is missing or stale (a hand edit of <c>darling-managed.conf</c>, or a crash inside Step
    /// B). Re-verify against the file on disk; never re-append.</item>
    /// </list>
    /// </summary>
    internal enum Kind
    {
        Legacy,
        PendingVerify,
        Verified,
        MigratedUnstamped,
    }

    /// <summary>
    /// Classifies <paramref name="dataDir"/>'s <c>postgresql.conf</c>: a
    /// v-marker present, or the managed include missing, is <see cref="Kind.Legacy"/> — checked FIRST and
    /// unconditionally, because a re-appended v-marker on an otherwise-migrated conf must never be read as
    /// migrated. Only once that is excluded does the pending file, then the stamp, decide the rest.
    /// </summary>
    internal static Kind Classify(string dataDir)
    {
        var postgresqlConfPath = Path.Combine(dataDir, "postgresql.conf");
        var postgresqlConfText = File.Exists(postgresqlConfPath) ? File.ReadAllText(postgresqlConfPath) : string.Empty;

        if (HasAnyMarker(postgresqlConfText) || !ManagedConfFile.HasManagedInclude(postgresqlConfText))
        {
            return Kind.Legacy;
        }

        var pendingPath = Path.Combine(dataDir, ManagedConfMigrationSteps.PendingFileName);
        if (File.Exists(pendingPath))
        {
            return Kind.PendingVerify;
        }

        return ManagedConfMigrationSteps.IsVerified(dataDir) ? Kind.Verified : Kind.MigratedUnstamped;
    }

    private static bool HasAnyMarker(string postgresqlConfText)
    {
        foreach (var marker in DarlingManagedPostgres.AllManagedConfMarkers)
        {
            if (postgresqlConfText.Contains(marker, System.StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
