using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// The trivial, mechanical projection from the analysis engine's (internal) reconstruction into the shared
/// public <see cref="BlockingChainModel"/> for the viewer. This is the only per-app block-chain code — it
/// must bridge an internal analysis type to a Common type, and no shared assembly can see both (Ui sees
/// Common but not Analysis; Analysis does not reference Common by design). Kept byte-identical to Dashboard's copy.
/// </summary>
internal static class BlockingChainViewerProjection
{
    public const string EmptyStateDetail =
        "No blocked-process reports were collected in the selected time range. " +
        "Blocking chains require the blocked-process-report Extended Event; Azure SQL DB may not emit it " +
        "(the blocked-process threshold is set via sp_configure, which Azure SQL DB does not support).";

    /// <summary>
    /// Builds the model for the ONE chain that contains the clicked session (matched by SessionKey — the
    /// clicked session may be the apex, a mid-level blocker, or a leaf victim). The chain is rooted at its
    /// apex with the full descendant hierarchy. Returns null when the session isn't in any reconstructed
    /// chain (its wait may not have met the blocked-process threshold in the selected range).
    /// </summary>
    public static BlockingChainModel? BuildModelForSession(
        BlockingReconstruction reconstruction,
        int? monitorLoop, int spid, int ecid)
    {
        var chain = BlockingChainReconstructor.FindChainForSession(
            reconstruction, monitorLoop, spid, ecid);
        if (chain == null)
            return null;

        return BlockingChainTreeBuilder.Build(
            new[] { ToInput(chain) },
            reconstruction.CycleDetected,
            reconstruction.DepthCapped,
            reconstruction.TraversalTruncated);
    }

    private static BlockingChainInput ToInput(ReconstructedChain c) => new()
    {
        ApexSpid = c.ApexSpid,
        ApexEcid = c.ApexEcid,
        ApexTranStarted = c.ApexTranStarted,
        ApexSleeping = c.ApexSleeping,
        Magnitude = c.Magnitude,
        Edges = c.Levels.Select(static l => new BlockingEdgeInput
        {
            Level = l.Level,
            BlockingSpid = l.BlockingSpid,
            BlockingEcid = l.BlockingEcid,
            BlockingTranStarted = l.BlockingTranStarted,
            BlockedSpid = l.BlockedSpid,
            BlockedEcid = l.BlockedEcid,
            BlockedTranStarted = l.BlockedTranStarted,
            LockMode = l.LockMode,
            WaitTimeMs = l.WaitTimeMs,
            DatabaseName = l.DatabaseName,
            BlockingSqlText = l.BlockingSqlText,
            BlockedSqlText = l.BlockedSqlText,
            BlockedLoginName = l.BlockedLoginName,
            BlockedHostName = l.BlockedHostName,
            BlockedClientApp = l.BlockedClientApp,
            BlockingLoginName = l.BlockingLoginName,
            BlockingHostName = l.BlockingHostName,
            BlockingClientApp = l.BlockingClientApp,
            ContentiousObject = l.ContentiousObject
        }).ToList()
    };
}
