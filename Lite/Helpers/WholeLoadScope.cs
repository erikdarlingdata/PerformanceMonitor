/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Gives each whole-tab load a cancellation token and cancels the token of the load before it (#9058). A load that a newer
/// one has superseded drops its paint (<c>ScopedLoadGenerations</c>), but the reads it started still ran to the end and held
/// their store read locks the whole time, so a server switch or a collection refresh during a slow load stacked a second set
/// of long reads on the first. The token reaches those reads, which stop and release their connection and lock.
/// <para>Not thread-safe, deliberately: <see cref="Begin"/> and <see cref="Lease.Dispose"/> run on the dispatcher, like the
/// load claims. A lease that a newer one cancelled is no longer the current source when it is disposed, and a lease that is
/// still the current one is cleared on dispose, so <see cref="Begin"/> never cancels a disposed source.</para>
/// </summary>
internal sealed class WholeLoadScope
{
    private CancellationTokenSource? _current;

    /// <summary>Cancels the load before this one, if it is still running, and returns the lease for this load.</summary>
    internal Lease Begin()
    {
        var previous = _current;
        var next = new CancellationTokenSource();
        _current = next;
        previous?.Cancel();
        return new Lease(this, next);
    }

    internal sealed class Lease : IDisposable
    {
        private readonly WholeLoadScope _owner;
        private readonly CancellationTokenSource _source;

        internal Lease(WholeLoadScope owner, CancellationTokenSource source)
        {
            _owner = owner;
            _source = source;
        }

        /// <summary>Fires when a newer load begins.</summary>
        internal CancellationToken Token => _source.Token;

        public void Dispose()
        {
            if (ReferenceEquals(_owner._current, _source))
            {
                _owner._current = null;
            }

            _source.Dispose();
        }
    }
}
