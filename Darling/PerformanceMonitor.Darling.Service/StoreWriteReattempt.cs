/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// What one store write did (#3099). One value rather than a row count plus an out-parameter, so a caller
/// cannot take the rows and drop the fact that they arrived on the second try — which is the whole signal.
/// </summary>
internal readonly record struct StoreWriteOutcome(int RowsWritten, bool Reattempted);

/// <summary>
/// The once-only re-attempt policy for a collector's store write (#3099), as a pure function of two
/// attempt delegates so it can be exercised without a store: the host supplies the COPY, this supplies the
/// decision about whether a failure gets a second one.
///
/// <para><b>Once, not a loop.</b> A batch is one cycle's sample of a source that keeps producing; the next
/// scheduled cycle is the second retry, and it costs nothing to wait for. What the immediate re-attempt
/// buys is the sample that would otherwise be unrecoverable, on the collectors whose
/// <c>WatermarkColumn</c> is the base <c>null</c> — a watermarked collector re-reads the same range next
/// cycle and never needed this.</para>
///
/// <para><b>Both attempts run on the caller's token, and a cancelled token suppresses the re-attempt
/// entirely.</b> The token here is the service's stopping token rather than a deadline, so honouring it
/// costs nothing in the population this exists for — a store-contention fault on a running service has no
/// cancellation in flight. What it prevents is a re-attempt that outlives an orderly stop while holding a
/// sweep permit and a store connection, against a bundled store the same process is shutting down: the one
/// case where a second attempt is guaranteed to be useless. The suppression is in the filter rather than a
/// throw inside the arm, so a write that fails during a stop still reports its own transport fault instead
/// of being relabelled a cancellation.</para>
/// </summary>
internal static class StoreWriteReattempt
{
    /// <summary>
    /// Runs <paramref name="write"/>; on a transport fault, calls <paramref name="onReattempt"/> with the
    /// first attempt's exception and then runs <paramref name="rewrite"/> once.
    ///
    /// <para>A failure on the SECOND attempt propagates. That is deliberate: the caller's fault arms record
    /// it as an ERROR exactly as they record a single failure today, so a store that is genuinely refusing
    /// writes stays loud. The first attempt's exception is not chained onto it — it has already gone to
    /// <paramref name="onReattempt"/>, which is where the host logs it, and burying it as an inner
    /// exception would put two different faults in one message column.</para>
    /// </summary>
    internal static async Task<StoreWriteOutcome> RunAsync(
        Func<CancellationToken, Task<int>> write,
        Func<CancellationToken, Task<int>> rewrite,
        Action<Exception> onReattempt,
        CancellationToken cancellationToken)
    {
        try
        {
            return new StoreWriteOutcome(await write(cancellationToken), Reattempted: false);
        }
        /* Ahead of the filter rather than relying on it to answer false. A cancellation must never be
           reclassified as a transport fault, and an arm whose correctness rests on a predicate NOT matching
           is one predicate edit away from re-attempting through a shutdown. */
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception firstAttempt) when (
            !cancellationToken.IsCancellationRequested && PostgresTransportFault.IsTransportFault(firstAttempt))
        {
            onReattempt(firstAttempt);
            return new StoreWriteOutcome(await rewrite(cancellationToken), Reattempted: true);
        }
    }
}
