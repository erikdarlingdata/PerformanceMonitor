/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The longest single checkpoint sync a <see cref="CheckpointSyncSampler"/> window saw.</summary>
/// <param name="SampledUtc">When the sample that saw it was taken, UTC.</param>
/// <param name="SyncMs">Milliseconds of sync time that finished since the sample before it.</param>
internal readonly record struct CheckpointSyncMax(DateTime SampledUtc, long SyncMs);

/// <summary>
/// Compile-only placeholder: the members exist so the tests that pin the behavior build and fail on their
/// assertions. The next commit replaces this body.
/// </summary>
internal sealed class CheckpointSyncSampler
{
    /// <summary>Records one sample of the cumulative sync time. Placeholder: does nothing yet.</summary>
    public void Observe(DateTime sampledUtc, long syncTimeMs)
    {
    }

    /// <summary>Returns the window maximum and clears it. Placeholder: always null yet.</summary>
    public CheckpointSyncMax? TakeWindowMax() => null;
}
