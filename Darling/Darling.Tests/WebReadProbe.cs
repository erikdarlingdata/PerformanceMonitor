/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Darling.Service;

namespace Darling.Tests;

/// <summary>
/// #5244 PR6 (W6): calls a read the way the web page does, through the web read dispatch, with the chosen databases as REPEATED
/// <c>database_name</c> keys. The configuration and events live tests use it to pin that the dispatch hands the whole list to the
/// reader (on the base the key was ignored, or only its first value was read).
/// </summary>
internal static class WebReadProbe
{
    public static async Task<JsonElement> ReadAsync(NpgsqlDataSource postgres, string serverName, string read, string hours, params string[] databases)
    {
        var query = new List<KeyValuePair<string, string?>> { new("server_name", serverName), new("hours_back", hours) };
        query.AddRange(databases.Select(d => new KeyValuePair<string, string?>("database_name", d)));
        var context = new DefaultHttpContext();
        context.Request.QueryString = QueryString.Create(query);
        return JsonDocument.Parse(await DarlingWebEndpoints.BuildReadDispatch()[read](context, postgres, null!)).RootElement;
    }
}
