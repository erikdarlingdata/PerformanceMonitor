/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// What one AWS-API ingest cycle actually did: how many rows it stored, and whether it reached the source at
/// all (#3017).
///
/// <para><b>The two are one value without this type, and that is the defect.</b> The three ingestors
/// (<see cref="RdsPlanIngestor"/>, <see cref="RdsDeadlockIngestor"/>, <see cref="RdsCpuIngestor"/>) returned a
/// bare <c>int</c>, and <c>DarlingCollectorRunner</c> stamped a zero with a positive claim about the source's
/// contents — "no new deadlocks in the RDS log window", "no new Performance Insights CPU samples this cycle".
/// A zero, though, spans two states the runner could not tell apart: the AWS API answered and the log
/// genuinely held nothing new, or <see cref="RdsEndpoint.TryParse"/> returned null for the host, NO AWS CALL
/// WAS MADE AT ALL, and the ingestor returned 0. The second one renders a sentence about the contents of a log
/// nobody opened.</para>
///
/// <para><b>Why this rather than a second out-parameter or a sentinel row count.</b> A caller cannot take the
/// count and drop the signal: <see cref="Rows"/> is only reachable through a value that also carries
/// <see cref="SourceReached"/>. A negative-row sentinel would have been the same collapse with extra steps —
/// every arithmetic consumer of the count would have to know the convention, and one that did not would file
/// a phantom -1 into <c>collection_log</c>.</para>
///
/// <para><b>This is not the #2633 case and does not overlap it.</b> #2633 stopped a DENIAL arriving as a
/// zero-row success, by rethrowing: a refusal now reaches the runner as an exception and is classified
/// PERMISSIONS. What is left is the one door that is not a failure at all — a target this transport simply
/// does not apply to, answering honestly and quietly. #2633's own closing sentence is the rule both halves
/// serve: a cycle that could not look must not claim it looked.</para>
/// </summary>
/// <param name="Rows">Rows stored. Always 0 when <paramref name="SourceReached"/> is false.</param>
/// <param name="SourceReached">True when this cycle actually asked AWS for the source — so a
/// <paramref name="Rows"/> of 0 is a real statement about what the source held. False when the target's host
/// is not an RDS or Aurora endpoint, so no AWS call was made and nothing at all is known about the
/// source.</param>
public readonly record struct RdsIngestOutcome(int Rows, bool SourceReached)
{
    /// <summary>
    /// The source was never asked — this target's host is not an RDS or Aurora endpoint, so this transport
    /// does not apply to it. Zero rows, and zero rows carrying no claim about the source.
    /// </summary>
    public static RdsIngestOutcome NotReached => new(0, false);

    /// <summary>
    /// The source was read and held <paramref name="rows"/> rows worth storing — including zero, which here
    /// is a genuine all-clear bounded by the read's own window rather than an absence of information.
    /// </summary>
    public static RdsIngestOutcome Read(int rows) => new(rows, true);
}
