/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>#8184: which probe a request to the health port is.</summary>
internal enum HealthProbe
{
    /// <summary><c>/livez</c>: the process is up and answering.</summary>
    Live,

    /// <summary><c>/readyz</c>: the process is collecting (or, on a front door, can serve).</summary>
    Ready,
}

/// <summary>#8184: a health answer, the status code and the one word that is its whole body.</summary>
internal readonly record struct HealthAnswer(int StatusCode, string Word);

/// <summary>
/// #8184: the pure mapping from the startup verdict to a probe answer. The word always comes from
/// <see cref="DarlingWebEndpoints.DescribePing"/>, so <c>/livez</c>, <c>/readyz</c> and <c>/api/ping</c> share one vocabulary.
/// <c>/livez</c> is 200 in every phase (a stopped process is still alive; restarting it is the operator's call); <c>/readyz</c> is 200 only
/// while collecting.
/// </summary>
internal static class DarlingHealthEndpoints
{
    internal static HealthAnswer Answer(HealthProbe probe, CollectorRuntimeState.Snapshot? snapshot)
    {
        var word = DarlingWebEndpoints.DescribePing(snapshot).Status;
        var ready = snapshot?.Phase == CollectorRuntimeState.CollectorPhase.Collecting;
        return new HealthAnswer(probe == HealthProbe.Live || ready ? 200 : 503, word);
    }

    /// <summary>A front door runs no collector, so it answers what its <c>/api/ping</c> answers: a refused <c>darling.json</c> is 503
    /// <c>stopped</c>, every store tried and unusable is 503 <c>degraded</c>, otherwise 200 <c>ok</c>. <c>/livez</c> is still 200.</summary>
    internal static HealthAnswer AnswerFrontDoor(HealthProbe probe, CollectorRuntimeState.Snapshot? collector, IReadOnlyList<FrontDoorStoreState> stores)
    {
        string word;
        bool ready;
        if (collector is { Phase: CollectorRuntimeState.CollectorPhase.Stopped })
        {
            word = DarlingWebEndpoints.DescribePing(collector).Status;
            ready = false;
        }
        else if (DarlingFrontDoorWebProxy.AllStoresUnusable(stores))
        {
            word = "degraded";
            ready = false;
        }
        else
        {
            word = "ok";
            ready = true;
        }

        return new HealthAnswer(probe == HealthProbe.Live || ready ? 200 : 503, word);
    }
}
