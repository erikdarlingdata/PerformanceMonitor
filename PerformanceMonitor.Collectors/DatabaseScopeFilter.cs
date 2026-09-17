/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Builds the parameterized per-collector database-scope fragment (#3477) — the ALLOW-LIST twin of
/// <see cref="DatabaseExclusionFilter"/>, and deliberately its structural mirror: each name is a
/// parameter of the same <see cref="CollectorParameterType.NVarChar128"/> type, compared by the ENGINE
/// with plain equality (<c>IN</c>), so a scoped-in name matches exactly the way an excluded name does
/// on that engine — SQL Server folds case under its default collations, PostgreSQL compares
/// <c>datname</c> byte for byte. The two instruments must agree on what a name IS, or a database could
/// be simultaneously scoped in by one spelling and not excluded by the other; mirroring the exclusion
/// filter's parameter shape makes that agreement the engine's job on both sides rather than a client
/// comparer's opinion on one.
///
/// <para><b>An allow-list is the whole point.</b> A non-empty scope means ONLY the named databases;
/// a database created tomorrow is NOT in the list and stays out of collection until an operator names
/// it — the issue's dev-instances-create-daily argument, and the property a deny-list structurally
/// cannot express. An empty scope produces no clause at all, so the unscoped query is byte-identical
/// to what shipped before the scope existed.</para>
/// </summary>
public static class DatabaseScopeFilter
{
    /// <param name="scopeDatabaseNames">The resolved allow-list; null/empty = unscoped (no clause).</param>
    /// <param name="columnExpression">SQL column to filter, e.g. "d.name".</param>
    public static (string Clause, List<CollectorParameter> Parameters) Build(
        IReadOnlyList<string>? scopeDatabaseNames, string columnExpression)
    {
        if (scopeDatabaseNames is null || scopeDatabaseNames.Count == 0)
        {
            return (string.Empty, new List<CollectorParameter>());
        }

        var paramNames = new List<string>(scopeDatabaseNames.Count);
        var parameters = new List<CollectorParameter>(scopeDatabaseNames.Count);
        for (int i = 0; i < scopeDatabaseNames.Count; i++)
        {
            /* @scope_db_N beside the exclusion's @excl_db_N — distinct prefixes so the two fragments
               can ride one statement without a parameter-name collision. */
            string p = $"@scope_db_{i}";
            paramNames.Add(p);
            parameters.Add(new CollectorParameter(p, scopeDatabaseNames[i], CollectorParameterType.NVarChar128));
        }

        return ($"AND {columnExpression} IN ({string.Join(", ", paramNames)})", parameters);
    }

    /// <summary>
    /// The composed enumeration predicate every fan-out enumeration splices: the scope's allow-list
    /// AND the server's exclusion list, in one fragment. This is the ONE seam of #3477's semantics —
    /// <c>IN (scope) AND NOT IN (excluded)</c> is the intersection "scoped-in minus excluded", which
    /// is how <c>excludedDatabases</c> keeps winning as the coarse instrument: a database named in
    /// both lists stays OUT, because composing the coarse veto under the fine scope would quietly
    /// turn a per-collector knob into an override of a server-level decision.
    /// </summary>
    public static (string Clause, List<CollectorParameter> Parameters) BuildEnumerationPredicate(
        CollectorContext context, string columnExpression)
    {
        var (scopeClause, parameters) = Build(context.DatabaseScope, columnExpression);
        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, columnExpression);

        parameters.AddRange(exclusionParameters);

        var clause = (scopeClause, exclusionClause) switch
        {
            ("", "") => string.Empty,
            ("", _) => exclusionClause,
            (_, "") => scopeClause,
            _ => scopeClause + " " + exclusionClause,
        };

        return (clause, parameters);
    }
}
