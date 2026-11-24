/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>
/// #9431: the front door's answer to "which build is serving, and are its stores up". It carries the build line (the same words the web
/// footer shows), the commit, when the process started, and for each store whether the front door can use it right now. Nothing else:
/// no address, no token and no error text, because it is read with no sign-in.
///
/// <para>Two places serve it. <c>GET /version</c> on the web listener is answered ahead of the sign-in gate (after the Host guard and the
/// network test), so it works on defaults with no <c>health</c> block. The health port's <c>/readyz</c> adds the same object after its
/// word when the health port is on. A probe must read the body: before this build the web listener answered 200 with the sign-in page
/// for any path, so a 200 alone does not show that the endpoint exists. The body is JSON with a <c>build</c> key.</para>
///
/// <para>A store is <c>up</c> when its last refresh succeeded. A store that is stale, never answered or refused the token is <c>down</c>.
/// It is the directory's last result, so a request here sends nothing to any store.</para>
/// </summary>
internal static class DarlingFrontDoorVersion
{
    /// <summary>The web listener's path.</summary>
    internal const string RoutePath = "/version";

    /// <summary>When this process started, read once.</summary>
    internal static readonly DateTimeOffset ProcessStartUtc = ReadProcessStart();

    private static DateTimeOffset ReadProcessStart()
    {
        using var process = Process.GetCurrentProcess();
        return new DateTimeOffset(process.StartTime.ToUniversalTime(), TimeSpan.Zero);
    }

    /// <summary>The word for one store: <c>up</c> or <c>down</c>.</summary>
    internal static string UpstreamWord(FrontDoorStoreHealth health) => health == FrontDoorStoreHealth.Ok ? "up" : "down";

    /// <summary>The object both places serve.</summary>
    internal static JsonObject Describe(IReadOnlyList<FrontDoorStoreState> stores, DateTimeOffset startedUtc)
    {
        var upstreams = new JsonArray();
        foreach (var state in stores)
        {
            upstreams.Add(new JsonObject { ["name"] = state.Store.Name, ["state"] = UpstreamWord(state.Health) });
        }

        return new JsonObject
        {
            ["build"] = DarlingCliCommands.BuildText(),
            ["commit"] = DarlingCliCommands.CommitText(),
            ["started_utc"] = startedUtc.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
            ["upstreams"] = upstreams,
        };
    }

    /// <summary>Whether this request is <c>GET /version</c>. Any other method or path goes on down the pipeline.</summary>
    internal static bool IsVersionRequest(HttpRequest request)
        => HttpMethods.IsGet(request.Method) && request.Path.Equals(RoutePath, StringComparison.OrdinalIgnoreCase);

    internal static Task WriteAsync(HttpContext context, IReadOnlyList<FrontDoorStoreState> stores, DateTimeOffset startedUtc)
    {
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "application/json; charset=utf-8";
        context.Response.Headers.CacheControl = "no-store";
        return context.Response.WriteAsync(Describe(stores, startedUtc).ToJsonString(), context.RequestAborted);
    }
}
