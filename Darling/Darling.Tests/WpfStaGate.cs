/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;

/// <summary>
/// #5559: one gate for every test that builds WPF elements on its own STA thread. WPF keeps process-wide state that is
/// not thread-safe: <c>DependencyPropertyDescriptor.FromProperty</c> hands every caller the same descriptor out of a static
/// cache, and that descriptor's <c>AddValueChanged</c> writes an unsynchronised per-descriptor <c>Dictionary</c>. The app has
/// one UI thread, so the product never races. xUnit runs test classes in parallel and about fifty of them start their own
/// STA thread, so two of them reaching <c>AccessibleNames.ActivateCell</c> at once corrupted that dictionary
/// ("Operations that change non-concurrent collections must have exclusive access") in CI, a different class each time.
/// <para>
/// Every STA helper calls <c>using var staGate = WpfStaGate.Enter();</c> on the CALLING thread, before it creates the
/// thread, and keeps it until it has joined the thread. Only one STA test body then runs at a time. Taking the gate on the
/// calling thread, not inside the new thread, keeps a timed <c>Join</c> honest: the wait for the gate is not charged to it.
/// <c>WpfStaGateCensusTests</c> fails when a test file starts an STA thread without calling <see cref="Enter"/>.
/// </para>
/// This file lives in Darling.Tests and is compiled into Lite.Tests by a link (one gate, two suites). It is in the global
/// namespace so the Lite classes, which sit in a different namespace, need no extra <c>using</c>.
/// </summary>
internal static class WpfStaGate
{
    /* A SemaphoreSlim, not a Monitor: the holder may be released from a different thread than took it if a helper is async. */
    private static readonly SemaphoreSlim s_gate = new(1, 1);

    /// <summary>Waits for the gate and returns the token that releases it. Not re-entrant: do not call it from inside a body that already holds it.</summary>
    internal static IDisposable Enter()
    {
        s_gate.Wait();
        return new Releaser();
    }

    private sealed class Releaser : IDisposable
    {
        private int _released;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
            {
                s_gate.Release();
            }
        }
    }
}
