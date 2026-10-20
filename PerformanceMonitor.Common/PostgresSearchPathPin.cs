/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// #6143: the ONE rule for whether a PostgreSQL session has the search_path this product sends at sign-in
/// (<c>-c search_path=pg_catalog,pg_temp</c>). The service's monitoring connections and the kill connection both ask it, so the two cannot
/// drift. Only the whitespace PostgreSQL itself skips between schema names (<see cref="IsPgListSpace"/>) is ignored; anything else must
/// match, so other Unicode spaces, which are part of a schema name to PostgreSQL, are not accepted.
/// </summary>
public static class PostgresSearchPathPin
{
    /// <summary>The statement that reads the search_path a PostgreSQL session actually has.</summary>
    public const string ReadSql = "SELECT pg_catalog.current_setting('search_path')";

    /// <summary>True when <paramref name="observedSearchPath"/> is <c>pg_catalog, pg_temp</c>, ignoring only the whitespace PostgreSQL skips.</summary>
    public static bool IsPinned(string? observedSearchPath) =>
        string.Equals(string.Concat((observedSearchPath ?? string.Empty).Where(c => !IsPgListSpace(c))), "pg_catalog,pg_temp", StringComparison.Ordinal);

    /// <summary>The characters PostgreSQL's identifier-list parser skips (scanner_isspace).</summary>
    public static bool IsPgListSpace(char c) => c is ' ' or '\t' or '\n' or '\r' or '\f';
}
