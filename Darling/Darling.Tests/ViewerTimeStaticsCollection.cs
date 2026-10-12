/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;

namespace Darling.Tests;

/// <summary>
/// The definition behind the pre-existing <c>[Collection("viewer-time-statics")]</c> attribute, which until
/// now had none — so the collection grouped its members but never constrained them.
///
/// <para><see cref="PerformanceMonitor.Darling.Viewer.ViewerTimeHelper"/> keeps the display mode and the
/// active server's UTC offset in PROCESS-WIDE mutable statics, and three test classes must set them to
/// exercise the production code that reads them (<c>ForDisplay</c>, <c>DrillWindowUtc</c>,
/// <c>LastCollectedDisplay</c>). Each already saves and restores the previous value in a <c>finally</c>,
/// which makes them safe against each other inside the collection — but does nothing about the rest of the
/// assembly, because xUnit runs SEPARATE collections in PARALLEL. Any viewer test that renders a timestamp
/// (every <c>*Local</c> / <c>*Display</c> row property routes through <c>ForDisplay</c>) could therefore
/// observe a mutated mode mid-assertion and fail for reasons having nothing to do with what it tests.</para>
///
/// <para>That was a live intermittent failure, not a theoretical one: repeated full-suite runs failed
/// roughly one time in six, landing on a different victim each time
/// (<c>ViewerSystemEventsTests.DefaultTraceEventRow_EventTimeLocal_SharesTheSystemHealthUtcFrame</c>,
/// <c>ViewerWave3DisplayTests.TimeLocal_TreatsTheStoredValueAsUtc</c>) — the shape that reads as "flaky
/// test" and gets re-run rather than fixed. The second of those now sets the mode and the row's clock it
/// asserts under, so its class joined this collection (#4766).</para>
///
/// <para><see cref="CollectionDefinitionAttribute.DisableParallelization"/> makes this collection run on its
/// own, so no other collection can be mid-assertion while the statics are swapped. Chasing the readers
/// instead would be endless — the mutators are three classes, the potential readers are every display test
/// in the viewer suite — so the constraint belongs on the small set that causes the problem.</para>
/// </summary>
[CollectionDefinition("viewer-time-statics", DisableParallelization = true)]
public sealed class ViewerTimeStaticsCollection
{
}
