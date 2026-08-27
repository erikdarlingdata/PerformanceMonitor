using System;
using System.Threading;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Classifies whether an analysis-pass failure is the residue of an abandonment we asked for, so the
/// pass's many catch sites can tell <i>unfinished because we called it off</i> from <i>unfinished
/// because something broke</i> (#2443). Lite's counterpart to Darling's
/// <c>AnalysisShutdown.IsExpectedAbandon</c>, and deliberately narrower than it.
///
/// <para>Both halves of the predicate are load-bearing, and the TYPE half especially so. Since #2419
/// the pass token fires on an ordinary TIMEOUT as well as at shutdown, so it is signalled during
/// perfectly normal running — a filter that asked only "has the token fired?" would relabel any
/// genuine fault landing after the budget elapsed as an abandonment and swallow the one line of
/// evidence it left. That is the same defect #2419's first review round caught, one layer further
/// in.</para>
///
/// <para>The shape list is one entry long because that is what was MEASURED, not what was assumed.
/// Darling's classifier also has to name a disposed data source and the 57P0x trio, because its
/// store is a separate postmaster that can go away underneath an in-flight read; Lite's is an
/// embedded file in this process. Against DuckDB.NET 1.5.5 a pre-cancelled token throws
/// <see cref="OperationCanceledException"/> without touching the database, and a token fired
/// mid-query reaches <c>duckdb_interrupt</c> through <c>DuckDBCommand.Cancel()</c> and surfaces as
/// <see cref="OperationCanceledException"/> too — not as a <c>DuckDBException</c> anyone would have
/// to pattern-match. A 2,790 ms query cancelled at 2,000 ms aborted at 2,004 ms, and both the
/// interrupted connection and a fresh one were fully usable afterwards. So naming the one shape
/// loses nothing the cancellation actually produces, and widening it would cost the fault
/// evidence.</para>
/// </summary>
public static class AnalysisAbandon
{
    /// <summary>
    /// True when this failure should be ABANDONED quietly rather than treated as a fault: the pass's
    /// token has fired AND the exception is the shape an abandonment produces. Catch sites on the
    /// pass use this in a <c>when</c> filter so the residue PROPAGATES — unwinding to the one line
    /// <c>AnalysisService</c> logs for it — instead of being swallowed per collector.
    /// </summary>
    public static bool IsExpected(Exception ex, CancellationToken passToken) =>
        passToken.IsCancellationRequested && ex is OperationCanceledException;
}
