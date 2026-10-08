/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one server-scope predicate the composer's fact reads and the hourly-edges count guard share (#5525), so the guard proves the
/// rollup over exactly the rows the read takes. See the compiler's <c>ComposeCompiler.ServerScope</c> for the full semantics; this is
/// its text.
/// </summary>
public static class ServerScopeSql
{
    /// <summary>
    /// <c>&lt;prefix&gt;server_id = ANY(ARRAY(SELECT reg.server_id FROM collect.servers AS reg WHERE reg.server_name = ANY(names)))</c>,
    /// or, when <paramref name="unregisteredParam"/> is given, that predicate in parentheses OR'd with
    /// <c>&lt;prefix&gt;server_name = ANY(unregistered)</c> (the names the registry does not hold, matched on the row's stored name).
    /// Only parameter names reach the text; no value does.
    /// </summary>
    public static string Predicate(string prefix, string namesParam, string? unregisteredParam)
    {
        var byId = $"{prefix}server_id = ANY(ARRAY(SELECT reg.server_id FROM {PgSchemaGenerator.CollectSchema}.servers AS reg WHERE reg.server_name = ANY({namesParam})))";
        return unregisteredParam is null ? byId : $"({byId} OR {prefix}server_name = ANY({unregisteredParam}))";
    }
}
