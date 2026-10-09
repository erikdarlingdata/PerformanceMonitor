/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one Content-Type test every embedded HTTP surface applies to a request that carries a body: Darling's
/// web dashboard write routes, Darling's MCP host and Lite's MCP host. Authored once here, in the library both
/// apps already reference, so the two MCP hosts cannot drift apart on what counts as JSON.
///
/// <para>PURE (a string in, a bool out, no HTTP types) so it unit-tests without a server and this library keeps no
/// ASP.NET Core dependency. A request with a body must carry <c>application/json</c> (a charset parameter is fine);
/// <c>text/plain</c>, <c>application/x-www-form-urlencoded</c>, <c>multipart/form-data</c> or no Content-Type at all
/// is answered 415 before a handler runs.</para>
/// </summary>
public static class JsonContentType
{
    /// <summary>
    /// Whether <paramref name="contentType"/> is <c>application/json</c>: a trailing <c>; charset=...</c> parameter
    /// is ignored and the match is case-insensitive. A null, empty or any other media type (including
    /// <c>application/problem+json</c>) is not JSON here.
    /// </summary>
    public static bool IsJson(string? contentType)
    {
        if (string.IsNullOrEmpty(contentType))
        {
            return false;
        }

        var semicolon = contentType.IndexOf(';', StringComparison.Ordinal);
        var mediaType = semicolon >= 0 ? contentType.AsSpan(0, semicolon) : contentType.AsSpan();
        return mediaType.Trim().Equals("application/json", StringComparison.OrdinalIgnoreCase);
    }
}
