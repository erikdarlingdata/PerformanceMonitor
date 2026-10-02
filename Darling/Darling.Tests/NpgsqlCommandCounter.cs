/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Darling.Tests;

/// <summary>
/// Counts the commands Npgsql runs against ONE database whose text contains every one of a set of fragments.
/// Npgsql opens an <see cref="Activity"/> per command on its <c>Npgsql</c> <see cref="ActivitySource"/> once a
/// listener samples it, tagged with the database name and the command text; the count is taken when the
/// activity stops, which is when the command's reader closes. The tags are matched by VALUE rather than by
/// name (the same way <c>SharedBaselineCacheTests.CaptureAsync</c> finds the command text), because the
/// OpenTelemetry attribute names Npgsql uses have changed between major versions.
///
/// <para>Scoping by the scratch database's name keeps a class running in parallel against another database
/// invisible to the count. The text carries the statement's <c>$n</c> placeholders, never the parameter values,
/// so a count cannot tell one bound value from another: keep the scenario to one value, or count deltas. A
/// caller trusts a count only after a control command it ran itself was counted once, so a renamed tag reads as
/// a loud failure rather than a silent 0.</para>
/// </summary>
internal sealed class NpgsqlCommandCounter : IDisposable
{
    private readonly string _databaseName;
    private readonly string[] _fragments;
    private readonly ActivityListener _listener;
    private long _count;

    public NpgsqlCommandCounter(string databaseName, params string[] fragments)
    {
        _databaseName = databaseName;
        _fragments = fragments;
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Npgsql",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = OnStopped,
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public long Count => Interlocked.Read(ref _count);

    private void OnStopped(Activity activity)
    {
        var onThisDatabase = false;
        var carriesTheText = false;
        foreach (var tag in activity.TagObjects)
        {
            if (tag.Value is not string value)
                continue;
            if (string.Equals(value, _databaseName, StringComparison.Ordinal))
                onThisDatabase = true;
            else if (_fragments.All(fragment => value.Contains(fragment, StringComparison.Ordinal)))
                carriesTheText = true;
        }
        if (onThisDatabase && carriesTheText)
            Interlocked.Increment(ref _count);
    }

    public void Dispose() => _listener.Dispose();
}
