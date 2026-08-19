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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// No collector table may share a name with an object in <c>pg_catalog</c> or <c>information_schema</c>.
///
/// <para><b>Why this test exists.</b> A collector table was named <c>pg_replication_slots</c>, which is also
/// the name of a system view. <c>pg_catalog</c> is searched IMPLICITLY AND FIRST — ahead of every entry in
/// <c>search_path</c>, and there is no way to demote it short of naming schemas explicitly everywhere — so an
/// unqualified reference to that name resolves to the system view no matter what the store holds. That
/// produced two failures with very different characters:</para>
/// <list type="bullet">
/// <item>LOUD: the generated fresh-store schema emits an unqualified <c>CREATE INDEX ... ON pg_replication_slots</c>,
/// which is 42809 "cannot create index on relation" against a view. The migration aborted, so the store never
/// came up and 531 tests failed behind the fixture.</item>
/// <item>QUIET, and worse: a reader's unqualified <c>FROM pg_replication_slots</c> against the STORE returns
/// the monitoring store's own slot list — normally empty. The tool would have reported "no replication slots"
/// forever and the retention alert would never have fired. A silently muted outage predictor is worse than
/// no predictor at all.</item>
/// </list>
///
/// <para>Schema-qualifying every reference would fix the loud half and leave the quiet half one forgotten
/// qualifier away, which is why the rule is about the NAME. Asserted against a real catalog rather than a
/// hardcoded list of reserved names: the catalog is authoritative, differs between majors, and grows.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CollectorTableCatalogShadowTests
{
    private readonly LivePostgresStoreFixture _fixture;

    public CollectorTableCatalogShadowTests(LivePostgresStoreFixture fixture) => _fixture = fixture;

    /// <summary>
    /// Every distinct collector table name, checked against the live store's own catalog. Also covers
    /// functions, since a set-returning function of the same name would shadow it for a <c>FROM</c> too.
    /// </summary>
    [Fact]
    public async Task NoCollectorTableShadowsASystemCatalogObject()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the catalog-shadow check.");
        Assert.True(_fixture.Established, "The live-postgres fixture did not establish the store.");

        var tables = CollectorCatalog.All.Select(d => d.TargetTable).Distinct().ToArray();

        await using var connection = new NpgsqlConnection(_fixture.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        var shadowed = new List<string>();
        await using (var command = new NpgsqlCommand(@"
SELECT c.relname, n.nspname, c.relkind::text
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('pg_catalog', 'information_schema')
AND   c.relname = ANY($1)
UNION ALL
SELECT p.proname, n.nspname, 'function'
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname IN ('pg_catalog', 'information_schema')
AND   p.proname = ANY($1)
ORDER BY 1", connection))
        {
            command.Parameters.AddWithValue(tables);
            await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            {
                shadowed.Add($"{reader.GetString(0)} shadows {reader.GetString(1)}.{reader.GetString(0)} [{reader.GetString(2)}]");
            }
        }

        Assert.True(
            shadowed.Count == 0,
            "Collector table name(s) collide with a system catalog object. pg_catalog is searched before "
            + "search_path, so an unqualified reference resolves to the SYSTEM object — CREATE INDEX fails "
            + "42809 and, far worse, a reader silently returns the system view's contents instead of collected "
            + "history. RENAME the table (a `_stats` suffix is the house pattern: query_store -> "
            + "query_store_stats, pg_replication_slots -> pg_replication_slot_stats); do not try to fix it by "
            + "schema-qualifying every reference.\n" + string.Join("\n", shadowed));
    }

    /// <summary>
    /// The check has teeth: a name that IS a catalog object must be detected. Without this, the test above
    /// would pass just as happily against a query that silently matched nothing.
    /// </summary>
    [Fact]
    public async Task TheShadowCheckDetectsAKnownCatalogName()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the catalog-shadow check.");
        Assert.True(_fixture.Established, "The live-postgres fixture did not establish the store.");

        await using var connection = new NpgsqlConnection(_fixture.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        await using var command = new NpgsqlCommand(@"
SELECT count(*)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'pg_catalog'
AND   c.relname = ANY($1)", connection);
        command.Parameters.AddWithValue(new[] { "pg_replication_slots", "pg_class" });

        var found = (long)(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
        Assert.Equal(2, found);
    }
}
