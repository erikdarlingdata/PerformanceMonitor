/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The trivial, mechanical projection from the analysis engine's (internal) reconstruction into the shared
/// public <see cref="BlockingChainModel"/> for the viewer. This is the only per-app block-chain code — it
/// must bridge an internal analysis type to a Common type, and no shared assembly can see both (Ui sees
/// Common but not Analysis; Analysis does not reference Common by design). Copied byte-identical from
/// Lite's/Dashboard's copy (W1e copy-parity — the viewer owns its own copy; Lite/Dashboard are untouched).
/// </summary>
internal static class BlockingChainViewerProjection
{
    public const string EmptyStateDetail =
        "No blocking was captured for this session in the selected time range — from either the " +
        "blocked-process-report Extended Event or the always-on DMV blocking snapshot. Its wait may not " +
        "have overlapped a collection in this window.";

    /// <summary>
    /// Builds the model for the ONE chain that contains the clicked session (matched by SessionKey — the
    /// clicked session may be the apex, a mid-level blocker, or a leaf victim). The chain is rooted at its
    /// apex with the full descendant hierarchy. Returns null when the session isn't in any reconstructed
    /// chain (its wait may not have met the blocked-process threshold in the selected range).
    /// </summary>
    public static BlockingChainModel? BuildModelForSession(
        BlockingReconstruction reconstruction,
        int? monitorLoop, int spid, int ecid,
        Func<DateTime, DateTime>? tranStampForDisplay = null)
    {
        var chain = BlockingChainReconstructor.FindChainForSession(
            reconstruction, monitorLoop, spid, ecid);
        if (chain == null)
            return null;

        return BlockingChainTreeBuilder.Build(
            new[] { ToInput(chain, tranStampForDisplay) },
            reconstruction.CycleDetected,
            reconstruction.DepthCapped,
            reconstruction.TraversalTruncated);
    }

    /// <summary>The window's transaction-start stamps are the monitored server's own wall clock, and the window prints
    /// them as they are, so it was the one place that ignored the Server / Local / UTC display mode the grids honour (#6292).
    /// <paramref name="display"/> is the app's server-clock-to-display conversion; null leaves the stamps as stored.</summary>
    private static DateTime? Shown(DateTime? stamp, Func<DateTime, DateTime>? display) =>
        stamp.HasValue && display != null ? display(stamp.Value) : stamp;

    private static BlockingChainInput ToInput(ReconstructedChain c, Func<DateTime, DateTime>? display) => new()
    {
        ApexSpid = c.ApexSpid,
        ApexEcid = c.ApexEcid,
        ApexTranStarted = Shown(c.ApexTranStarted, display),
        ApexSleeping = c.ApexSleeping,
        Magnitude = c.Magnitude,
        Edges = c.Levels.Select(l => new BlockingEdgeInput
        {
            Level = l.Level,
            BlockingSpid = l.BlockingSpid,
            BlockingEcid = l.BlockingEcid,
            BlockingTranStarted = Shown(l.BlockingTranStarted, display),
            BlockedSpid = l.BlockedSpid,
            BlockedEcid = l.BlockedEcid,
            BlockedTranStarted = Shown(l.BlockedTranStarted, display),
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
