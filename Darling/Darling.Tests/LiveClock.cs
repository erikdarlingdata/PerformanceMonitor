/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace Darling.Tests;

/// <summary>
/// The one clock a live test seeds against (#5608). A live test that seeds rows "N hours before now" puts them in
/// different Timescale chunks depending on the hour the test runs: the store's raw chunks are one UTC day, so between
/// 00:00 and 04:00 UTC a 4-hour window straddles a chunk boundary, and between 00:00 and 24:00 a window of a day or
/// more always does. A test that counts blocks, reads a plan or looks at which chunks hold rows then answers
/// differently around midnight, and fails only on a pull request that happens to run then.
///
/// <para><see cref="AnchoredEnd"/> is a fixed point inside one chunk (22:00 UTC on the previous UTC day), so a window
/// of up to four hours ending there never leaves its chunk and the answer is the same at any hour. A test that
/// wants a window of a day or more still straddles a boundary, but at the same place every run.</para>
///
/// <para>The environment variable <c>DARLING_TEST_CLOCK_UTC</c> (an ISO-8601 UTC time, read only here) replaces the
/// real clock, so a run at 12:00 can be proved as if it were 00:30 without touching the machine's clock.
/// The product still reads its own clock (<c>DateTime.UtcNow</c>, <c>now()</c>), so an override BEHIND the real clock
/// makes a class that asks the product "is this recent" or "does the table cover now" answer for a different instant
/// than the one it seeded (#6374: <c>CollectionHealthRecoveryLiveTests</c> reads STALE, the interval-table gate sends the
/// read to raw). That is the override disagreeing with the clock, not an hour dependence: prove the hour with an override
/// at or after the real clock.</para>
///
/// <para>An override on the UTC day after the real clock's puts <see cref="AnchoredEnd"/> after the real clock until 22:00 UTC.
/// A test that asks the product about the anchored window (a tool's <c>as_of</c>, a write the store must see as late) anchors at
/// <see cref="AnchoredEndNotAfterNow"/>, which both clocks have passed.</para>
/// </summary>
internal static class LiveClock
{
    /// <summary>Name of the test-only override; it is read nowhere in product code.</summary>
    public const string OverrideVariable = "DARLING_TEST_CLOCK_UTC";

    /// <summary>"Now" for a live test: the override when set, else the UTC clock, cut to the second, unspecified kind (a store timestamp).</summary>
    public static DateTime Now()
    {
        var text = Environment.GetEnvironmentVariable(OverrideVariable);
        var now = string.IsNullOrWhiteSpace(text)
            ? DateTime.UtcNow
            : DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        return DateTime.SpecifyKind(new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
    }

    /// <summary>
    /// A fixed point inside one raw chunk: 22:00 UTC on the previous UTC day. Four hours before it is 18:00 the same day,
    /// so a window of up to four hours never crosses a 24, 12 or 6 hour chunk boundary, whatever hour the test runs.
    /// </summary>
    public static DateTime AnchoredEnd() => Now().Date.AddDays(-1).AddHours(22);

    /// <summary>
    /// <see cref="AnchoredEnd"/> for a window the product is asked about: 22:00 UTC on the UTC day before the earlier of
    /// <see cref="Now"/> and the real clock. The product reads its own clock: a tool refuses an <c>as_of</c> after it, and the
    /// store marks a row late only when the row is older than the store's own hour. A test clock ahead of the real clock (the
    /// pinned 00:30 pass, run before 22:00 UTC) puts <see cref="AnchoredEnd"/> after the real clock, and this stays behind both.
    /// The two are the same instant unless the test clock is on a later UTC day than the real clock. Four hours before
    /// 22:00 is 18:00 the same day, so a window of up to four hours stays inside one raw chunk at any hour.
    /// </summary>
    public static DateTime AnchoredEndNotAfterNow()
    {
        var testNow = Now();
        var realNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        return (testNow < realNow ? testNow : realNow).Date.AddDays(-1).AddHours(22);
    }
}
