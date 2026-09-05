/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using Npgsql;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// How many pooled store connections this viewer seat actually has, as a RUNTIME fact rather than a
/// compile-time one (#3016).
///
/// <para><b>Why this exists at all.</b> The read-deadline family prices contention against the number of
/// permits a fan-out can hold (<see cref="ViewerCommandDeadlines.FanOutReadSeconds(int)"/>) and the fan-out
/// census bounds a declared width by it (<see cref="ViewerReadFanOut.MaxConcurrentReads"/>). Both read
/// <see cref="ViewerSettings.ManagedMaxPoolSize"/> when #3007 landed, and that constant is applied to ONE of
/// the two connection strings the viewer can be handed: the managed derivation sets
/// <c>MaxPoolSize = 10</c> (<c>ViewerSettings.DeriveManagedConnectionString</c>), while a bring-your-own
/// <c>postgres.connectionString</c> is used verbatim and its pool is whatever the operator wrote — Npgsql's
/// own default being <see cref="NpgsqlDefaultMaxPoolSize"/>. So on a BYO store the constant was not the pool
/// size, it was a guess about it, and the deadline sized for ten lanes was handed to a fan-out that could
/// hold a hundred permits.</para>
///
/// <para><b>Process-wide, because the pool is.</b> A viewer seat opens exactly one
/// <c>NpgsqlDataSource</c> (<c>MainWindow.OnLoaded</c> constructs the single
/// <see cref="ViewerDataService"/>), so one published value describes every read in the process. The
/// publish happens in <see cref="ViewerDataService"/>'s constructor rather than at the call site, so the
/// thing that CREATES the pool is the thing that reports its size and a second construction cannot leave
/// the two disagreeing.</para>
///
/// <para><b>Both consumers take a minimum against a measured width, which is what makes this safe to
/// publish repeatedly.</b> A larger pool than the managed ten cannot raise either consumer past ten (the
/// concurrent band was only ever measured at ten wide), so a re-publish can only matter when the operator
/// configured a pool SMALLER than ten — the case the constant got wrong in the expensive direction. Reads
/// and writes go through <see cref="Volatile"/> because the fleet timer's fan-out and the UI thread both
/// read it.</para>
///
/// <para><b>What it does not model.</b> <c>Pooling=false</c> on a BYO string removes the permit ceiling
/// entirely; this reports Npgsql's nominal maximum for that string and the concurrency bound then comes
/// from <see cref="ViewerReadFanOut.MaxConcurrentReads"/> alone, which is the correct answer either way
/// because that bound is the smaller of the two.</para>
/// </summary>
public static class ViewerStorePool
{
    /// <summary>
    /// Npgsql's own <c>Max Pool Size</c> default — what a bring-your-own connection string that names no
    /// pool size actually gets. Measured off <c>NpgsqlConnectionStringBuilder</c> at the pinned Npgsql
    /// version rather than quoted from its documentation, and pinned by
    /// <c>ViewerCommandTimeoutTests.TheEffectivePoolSize_ReadsTheStringNotTheManagedConstant</c> so a
    /// version bump that moves it fails a build instead of silently re-guessing.
    /// </summary>
    public const int NpgsqlDefaultMaxPoolSize = 100;

    /// <summary>
    /// The managed seat's ceiling, which is also what an unpublished process reports: a code path that
    /// never published leaves the deadline family exactly where #3007 left it rather than inventing a
    /// wider pool it cannot prove exists.
    /// </summary>
    private static int s_maxPoolSize = ViewerSettings.ManagedMaxPoolSize;

    /// <summary>The effective pool size for this seat's store connection.</summary>
    public static int MaxPoolSize => Volatile.Read(ref s_maxPoolSize);

    /// <summary>
    /// Records the pool size the given connection string will actually get. Called from
    /// <see cref="ViewerDataService"/>'s constructor with the string the data source is built on — the
    /// EFFECTIVE string, after the connect-timeout preference is applied, so the value published and the
    /// value Npgsql enforces come from the same text.
    /// </summary>
    internal static void Publish(string? connectionString) =>
        Volatile.Write(ref s_maxPoolSize, MaxPoolSizeOf(connectionString));

    /// <summary>
    /// The pool size a connection string resolves to: the configured value when it names one, Npgsql's
    /// <see cref="NpgsqlDefaultMaxPoolSize"/> when it does not, and
    /// <see cref="ViewerSettings.ManagedMaxPoolSize"/> when there is no usable string to read.
    ///
    /// <para><b>Npgsql's builder, not the base <c>DbConnectionStringBuilder</c></b> — the opposite choice
    /// from <c>ViewerDataService.StoreHostIsLoopback</c>, and for the opposite reason. That one needs to
    /// know whether a key was PRESENT, so it cannot use a builder that answers with defaults. This one
    /// needs the EFFECTIVE value, which is exactly what a defaulting builder returns, and it also gets the
    /// keyword aliases (<c>Maximum Pool Size</c>) and the range validation for free.</para>
    ///
    /// <para>An unusable string falls back rather than throwing: this feeds a deadline computation, and a
    /// string Npgsql cannot parse fails at <c>NpgsqlDataSource.Create</c> with a message about the string —
    /// which is the error the operator needs, not a second one from a timeout calculation. This runs one
    /// statement ahead of that <c>Create</c>, inside a constructor whose call site does not catch, so the
    /// fallback has to be unconditional or it would replace that error rather than defer to it.</para>
    ///
    /// <para><b>Caught broadly, matching the three other sites that wrap this constructor</b>
    /// (<c>ViewerConfigDiagnostics</c>, <c>ViewerCertificateAnchor</c>, <c>StoreConnectionSelfTest</c>),
    /// whose own comments say why: Npgsql's parse failures are not one exception type. Measured at 10.0.3 —
    /// a bad value for a recognised keyword raises <c>ArgumentException</c>, but <c>"====="</c> raises
    /// <c>KeyNotFoundException</c>, which does not derive from it. Both are pinned.</para>
    /// </summary>
    public static int MaxPoolSizeOf(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return ViewerSettings.ManagedMaxPoolSize;
        }

        try
        {
            var configured = new NpgsqlConnectionStringBuilder(connectionString).MaxPoolSize;

            return configured < 1 ? ViewerSettings.ManagedMaxPoolSize : configured;
        }
        catch (Exception)
        {
            return ViewerSettings.ManagedMaxPoolSize;
        }
    }
}
