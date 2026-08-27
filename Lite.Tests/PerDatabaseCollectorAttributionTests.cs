/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the one invariant that ties a per-database collector's rows back to the database they describe:
/// if a definition ever returns true from <see cref="ICollectorDefinition.RunsPerDatabase"/>, its payload
/// must carry a <c>database_name</c> column.
///
/// <para><b>Why this is a build-time assertion and not a review habit.</b> A per-database collector without
/// that column still compiles, still collects, and still looks correct in a single-database test rig. It
/// fails only on a cluster that has the same schema in two databases — the ordinary multi-tenant shape —
/// where the rows collide on (server_id, schema, table, column) and nothing distinguishes them. Two
/// collectors shipped in that state (#2599): <c>pg_column_stats</c>, which declared
/// <c>RunsPerDatabase =&gt; true</c> and no <c>database_name</c>, and <c>pg_extension_availability</c>,
/// which read the per-database <c>pg_extension</c> catalog from a single connection.</para>
///
/// <para>The targets below are representative rather than exhaustive on purpose: the property takes a
/// target because a few SQL Server definitions only go per-database on Azure SQL DB, so a single target
/// would silently skip them. Any target for which the answer is true triggers the requirement.</para>
/// </summary>
public class PerDatabaseCollectorAttributionTests
{
    private static readonly CollectorTargetInfo[] s_targets =
    {
        new() { Engine = CollectorTargetEngine.SqlServer, SqlMajorVersion = 16 },
        new() { Engine = CollectorTargetEngine.SqlServer, SqlMajorVersion = 12, IsAzureSqlDb = true },
        new() { Engine = CollectorTargetEngine.SqlServer, SqlMajorVersion = 15, IsAzureManagedInstance = true },
        new() { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, PostgresVersionNum = 170000 },
        new() { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 14, PostgresVersionNum = 140000, IsAurora = true },
    };

    [Fact]
    public void EveryPerDatabaseCollectorDeclaresDatabaseName()
    {
        var offenders = new List<string>();

        foreach (var schema in CollectorCatalog.All)
        {
            /* RunsPerDatabase is declared on the generic ICollectorDefinition<TRow>, and the catalog is
               typed as the row-agnostic ICollectorSchemaInfo, so there is no non-generic surface to call
               it through. Reflection over the concrete type is the only route that stays generic across
               every row shape; a definition that somehow lacks the method is not per-database. */
            var method = schema.GetType().GetMethod(
                "RunsPerDatabase",
                BindingFlags.Public | BindingFlags.Instance,
                binder: null,
                types: new[] { typeof(CollectorTargetInfo) },
                modifiers: null);

            if (method is null)
            {
                continue;
            }

            var runsPerDatabase = s_targets.Any(target => (bool)method.Invoke(schema, new object[] { target })!);

            if (!runsPerDatabase)
            {
                continue;
            }

            var hasDatabaseName = schema.PayloadColumns
                .Any(column => column.Name == "database_name");

            if (!hasDatabaseName)
            {
                offenders.Add(schema.Name);
            }
        }

        Assert.True(
            offenders.Count == 0,
            "These collectors run per database but do not declare a database_name payload column, so their " +
            "rows cannot be attributed to the database they came from: " + string.Join(", ", offenders));
    }
}
