/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Hears what the migration ladder does as it runs (#9415), so the service can keep a row per rung it applied in the store's
/// start history. The runner calls it in-process and ignores nothing it throws: an implementation only keeps what it is told in
/// memory and never fails. The rows cannot be written by the runner itself, because the table that holds them is created by a rung.
/// </summary>
public interface IMigrationRungObserver
{
    /// <summary>The ladder is about to apply the rungs above <paramref name="storeVersion"/>, the highest version the store has stamped (0 for a store with none).</summary>
    void LadderStarted(int storeVersion);

    /// <summary>One rung finished or failed. <paramref name="outcome"/> is <see cref="MigrationRungOutcome.Applied"/> or <see cref="MigrationRungOutcome.Failed"/>.</summary>
    void RungFinished(int version, string name, DateTime startedUtc, long elapsedMilliseconds, string outcome, string? errorText);

    /// <summary>
    /// What an applied rung created, changed or dropped (#9438), called right after <see cref="RungFinished"/> for that rung and only for a ladder that
    /// began on a store that already had a version: a ladder from an empty store lists nothing, because every object would read as created. The list is
    /// empty for a rung that changed no object the catalog diff covers (<see cref="MigrationCatalogSnapshot"/>). It is not called when the diff could not be taken.
    /// </summary>
    void RungObjects(int version, IReadOnlyList<MigrationObjectChange> changes);
}

/// <summary>The outcome words a rung row carries.</summary>
public static class MigrationRungOutcome
{
    /// <summary>The rung ran and its stamp committed.</summary>
    public const string Applied = "applied";

    /// <summary>The rung threw, or the start was cancelled inside it, and nothing of it was stamped.</summary>
    public const string Failed = "failed";
}
