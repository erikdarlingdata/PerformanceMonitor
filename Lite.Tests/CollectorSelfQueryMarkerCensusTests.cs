/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Every SQL Server collector's own statements carry the self-query marker (#1007 click-through, F14).
///
/// <para><b>Why a census.</b> Top Queries, Query Store and the procedure grids leave out statements whose
/// text contains <c>PerformanceMonitorLite</c>, so the collectors do not fill the grids with Lite watching
/// itself. The plan-correction collector shipped without the marker, and on a quiet server the top rows of
/// Top Queries by Duration and Query Store by Duration were its own <c>sys.dm_db_tuning_recommendations</c>
/// statement, one row per database. A collector added later without the marker would do it again, so this
/// builds every query every SQL Server collector can send, over the target shapes that change the SQL, and
/// fails on any that lacks the marker.</para>
/// </summary>
[Trait("Stage", "Guard")]
public class CollectorSelfQueryMarkerCensusTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static readonly CollectorTargetInfo[] s_targets =
    {
        new() { SqlMajorVersion = 13 },
        new() { SqlMajorVersion = 15 },
        new() { SqlMajorVersion = 17 },
        new() { SqlMajorVersion = 16, IsAzureSqlDb = true },
        new() { SqlMajorVersion = 16, IsAzureManagedInstance = true },
        new() { SqlMajorVersion = 15, IsAwsRds = true },
    };

    private static CollectorContext Context(CollectorTargetInfo target, bool collectedBefore, bool planAndTextBySeparateFetch = false) => new()
    {
        CapturePlanXml = planAndTextBySeparateFetch,
        FetchQueryTextSeparately = planAndTextBySeparateFetch,
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = DateTime.UtcNow,
        Deltas = s_deltas,
        Target = target,
        HasCollectedBefore = collectedBefore,
        CurrentDatabaseName = "SomeDatabase",
        LongQuerySessionName = LongQueryCompletionsCollector.XeSessionNameFor("Darling", "0a1b2c3d"),
    };

    private static IEnumerable<(string Where, CollectorQuery Query)> QueriesOf(ICollectorSchemaInfo schema, List<string> unbuildable)
    {
        var type = schema.GetType();

        foreach (var target in s_targets)
        {
            if (!CollectorCatalog.EngineMatches(schema, target))
            {
                continue;
            }

            foreach (var collectedBefore in new[] { false, true })
            {
                foreach (var name in new[] { "BuildQuery", "BuildSupplementalQuery", "BuildEnumerationQuery", "BuildEnumerationProbe", "BuildPerItemQuery" })
                {
                    var method = type.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                        .FirstOrDefault(m => m.Name == name);

                    if (method is null)
                    {
                        continue;
                    }

                    var args = method.GetParameters().Length == 2
                        ? new object?[] { "SomeDatabase", Context(target, collectedBefore) }
                        : new object?[] { Context(target, collectedBefore) };

                    object? result;

                    try
                    {
                        result = method.Invoke(schema, args);
                    }
                    catch (TargetInvocationException ex) when (ex.InnerException is NotSupportedException)
                    {
                        /* The collector says this entry point is not how it runs on this target (it enumerates
                           databases and BuildEnumerationQuery drives it, or it has no per-item query). The
                           per-collector floor below proves every collector still built at least one query. */
                        continue;
                    }
                    catch (TargetInvocationException ex)
                    {
                        unbuildable.Add($"{schema.Name}.{name} (v{target.SqlMajorVersion}): {ex.InnerException?.GetType().Name}: {ex.InnerException?.Message}");
                        continue;
                    }

                    if (result is CollectorQuery q)
                    {
                        yield return ($"{schema.Name}.{name} (v{target.SqlMajorVersion} azure={target.IsAzureSqlDb} mi={target.IsAzureManagedInstance} rds={target.IsAwsRds} before={collectedBefore})", q);
                    }
                }
            }
        }
    }

    [Fact]
    public void EverySqlServerCollectorQueryCarriesTheSelfQueryMarker()
    {
        var missing = new SortedSet<string>(StringComparer.Ordinal);
        var unbuildable = new List<string>();
        var checkedQueries = 0;
        var silent = new List<string>();

        foreach (var schema in CollectorCatalog.All)
        {
            if (schema.TargetEngine != CollectorTargetEngine.SqlServer)
            {
                continue;
            }

            var built = 0;

            foreach (var (where, query) in QueriesOf(schema, unbuildable))
            {
                checkedQueries++;
                built++;

                if (!query.Text.Contains(QueryStoreCollector.SelfQueryMarker, StringComparison.Ordinal))
                {
                    missing.Add(where.Split(' ')[0]);
                }
            }

            if (built == 0)
            {
                silent.Add(schema.Name);
            }
        }

        Assert.True(checkedQueries > 40, $"only {checkedQueries} queries were built; the census is not reaching the collectors");
        Assert.True(silent.Count == 0, "collectors that built no query at all: " + string.Join(", ", silent));
        Assert.True(unbuildable.Count == 0, "queries that could not be built:\n" + string.Join("\n", unbuildable.Take(20)));
        Assert.True(missing.Count == 0, "collector queries without the " + QueryStoreCollector.SelfQueryMarker + " marker:\n" + string.Join("\n", missing));
    }
    /// <summary>
    /// A query-stats row for a statement run through <c>sp_executesql</c> carries the INNER batch's text (it has its own sql_handle), so
    /// the marker has to sit in that inner literal: one in the outer batch does not reach it. The Query Store filter is per statement
    /// too. This pulls out every inner <c>sp_executesql</c> batch of every collector query and checks each on its own.
    /// </summary>
    [Fact]
    public void EveryInnerSpExecuteSqlBatchCarriesTheSelfQueryMarkerOnItsOwn()
    {
        var missing = new SortedSet<string>(StringComparer.Ordinal);
        var unbuildable = new List<string>();
        var inner = 0;

        foreach (var schema in CollectorCatalog.All)
        {
            if (schema.TargetEngine != CollectorTargetEngine.SqlServer)
            {
                continue;
            }

            foreach (var (where, query) in QueriesOf(schema, unbuildable))
            {
                foreach (var batch in InnerBatches(query.Text))
                {
                    inner++;

                    if (!batch.Contains(QueryStoreCollector.SelfQueryMarker, StringComparison.Ordinal))
                    {
                        var head = Regex.Replace(batch, @"\s+", " ").Trim();
                        missing.Add(where.Split(' ')[0] + ": " + head[..Math.Min(70, head.Length)]);
                    }
                }
            }
        }

        Assert.True(inner > 30, $"only {inner} inner sp_executesql batches were found; the extraction is not reaching them");
        Assert.True(unbuildable.Count == 0, "queries that could not be built:\n" + string.Join("\n", unbuildable.Take(20)));
        Assert.True(missing.Count == 0, "inner sp_executesql batches without the " + QueryStoreCollector.SelfQueryMarker + " marker:\n" + string.Join("\n", missing));
    }

    [Fact]
    public void TheInnerBatchExtraction_FindsLiteralVariableAndNestedBatches_AndSkipsComments()
    {
        const string Sql = @"
/* it's a comment with an apostrophe, sp_executesql N'not a batch' */
-- another one's comment
EXEC sys.sp_executesql N'SELECT 1 /* a */ WHERE x = N''y'';', N'@o int OUTPUT', @o = @p OUTPUT;
DECLARE @b nvarchar(max) = N'SELECT 2'
    + N' FROM t;';
EXECUTE sys.sp_executesql @b, N'@o bit OUTPUT', @o = @q OUTPUT;
SET @w = N'EXECUTE ' + QUOTENAME(@d) + N'.sys.sp_executesql N''SELECT 3 FROM u;''';
";
        var batches = InnerBatches(Sql).ToList();

        Assert.Contains(batches, b => b.StartsWith("SELECT 1", StringComparison.Ordinal) && b.Contains("N'y'", StringComparison.Ordinal));
        Assert.Contains(batches, b => b.Contains("SELECT 2", StringComparison.Ordinal) && b.Contains("FROM t", StringComparison.Ordinal));
        Assert.Contains(batches, b => b.Contains("SELECT 3", StringComparison.Ordinal));
        Assert.DoesNotContain(batches, b => b.Contains("not a batch", StringComparison.Ordinal));
    }

    /// <summary>
    /// The Query Store database probe builds its per-database statement into <c>@sql</c> and runs it through <c>EXECUTE @exec_sp @sql</c>.
    /// That inner batch is what the monitored server caches and Query Store records, one row per database, so the marker has to sit in it
    /// (V8 of the release walk: it topped Top Queries by Duration and Query Store by Duration without it).
    /// </summary>
    [Fact]
    public void TheQueryStoreProbeInnerBatchCarriesTheSelfQueryMarker()
    {
        var probe = QueryStoreCollector.Instance.BuildEnumerationQuery(Context(new CollectorTargetInfo { SqlMajorVersion = 17 }, collectedBefore: true))!;
        var batches = InnerBatches(probe.Text).ToList();

        Assert.Contains(batches, b => b.Contains("database_query_store_options", StringComparison.Ordinal));
        Assert.All(batches, b => Assert.Contains(QueryStoreCollector.SelfQueryMarker, b, StringComparison.Ordinal));
    }

    /// <summary>
    /// Query Store records and ranks each statement of a batch on its own, so every top-level statement of every Query Store
    /// query needs the marker, not only the batch's first one. The plan and text fetch by id, the backfill and the probe's result
    /// reads were the unmarked ones (V8): their <c>SELECT ... INTO #plan_fetch</c> and the ranked <c>SELECT</c> after it ranked first
    /// by duration on a quiet server.
    /// </summary>
    [Fact]
    public void EveryTopLevelSelectOfEveryQueryStoreQueryCarriesTheSelfQueryMarker()
    {
        var collector = QueryStoreCollector.Instance;
        var targets = new[]
        {
            new CollectorTargetInfo { SqlMajorVersion = 13 },
            new CollectorTargetInfo { SqlMajorVersion = 17 },
            new CollectorTargetInfo { SqlMajorVersion = 16, IsAzureSqlDb = true },
        };
        var queries = new List<(string Name, string Text)>();

        /* Darling's shape (plan XML and query text fetched by id) and Lite's, each over the targets that change the SQL. */
        foreach (var (target, separateFetch) in targets.SelectMany(t => new[] { (t, false), (t, true) }))
        {
            var context = Context(target, collectedBefore: true, planAndTextBySeparateFetch: separateFetch);
            var floor = DateTime.UtcNow.AddDays(-2);
            var ceiling = DateTime.UtcNow.AddDays(-1);

            /* The single-statement payload is the Azure SQL DB shape; every other target enumerates databases and nests it per item. */
            if (target.IsAzureSqlDb)
            {
                queries.Add(("BuildQuery", collector.BuildQuery(context).Text));
                queries.Add(("BuildBackfillQuery", collector.BuildBackfillQuery(context, floor, ceiling).Text));
            }

            var enumeration = collector.BuildEnumerationQuery(context);

            if (enumeration is not null)
            {
                queries.Add(("BuildEnumerationQuery", enumeration.Text));
            }

            queries.Add(("BuildPerItemQuery", collector.BuildPerItemQuery("SomeDatabase", context).Text));
            queries.Add(("BuildBackfillPerItemQuery", collector.BuildBackfillPerItemQuery("SomeDatabase", context, floor, ceiling).Text));

            if (separateFetch)
            {
                queries.Add(("BuildPlanFetchByIdsQuery", collector.BuildPlanFetchByIdsQuery("SomeDatabase", context, new long[] { 1, 2, 3 }, 1024 * 1024).Text));
                queries.Add(("BuildTextFetchByIdsQuery", collector.BuildTextFetchByIdsQuery("SomeDatabase", context, new long[] { 1, 2, 3 }, 1024 * 1024).Text));
            }
        }

        var missing = new SortedSet<string>(StringComparer.Ordinal);
        var checkedSelects = 0;

        foreach (var (name, text) in queries)
        {
            /* A top-level statement starts in column 0; the sub-selects and CTE bodies are indented. */
            foreach (Match select in Regex.Matches(text, @"(?m)^SELECT\b(?<rest>[^\r\n]*)"))
            {
                checkedSelects++;

                if (!select.Groups["rest"].Value.Contains(QueryStoreCollector.SelfQueryMarker, StringComparison.Ordinal))
                {
                    missing.Add(name + ": " + text.Substring(select.Index, Math.Min(60, text.Length - select.Index)).Replace("\r", "", StringComparison.Ordinal).Replace("\n", " ", StringComparison.Ordinal));
                }
            }
        }

        Assert.True(checkedSelects > 20, $"only {checkedSelects} top-level SELECT statements were found; the scan is not reaching them");
        Assert.True(missing.Count == 0, "top-level Query Store statements without the " + QueryStoreCollector.SelfQueryMarker + " marker:\n" + string.Join("\n", missing));
    }

    /// <summary>
    /// The statements that are not collectors but still run against the monitored server on a schedule or per alert: the connect-time
    /// detection probe and start-time read, the failed-jobs read and the job-step lookup (shared with Darling through
    /// <c>PerformanceMonitor.Alerting</c>). Each is a single statement, so the marker sits in its own text.
    /// </summary>
    [Fact]
    public void TheConnectProbesAndTheAlertJobReadsCarryTheSelfQueryMarker()
    {
        var statements = new Dictionary<string, string>
        {
            ["ServerManager.DetectionQueryText"] = PerformanceMonitorLite.Services.ServerManager.DetectionQueryText,
            ["ServerManager.ServerStartTimeQueryText"] = PerformanceMonitorLite.Services.ServerManager.ServerStartTimeQueryText,
            ["FailedJobsQuery.Sql"] = PerformanceMonitor.Alerting.FailedJobsQuery.Sql,
            ["AgentJobStepQuery.BuildSql"] = PerformanceMonitor.Alerting.AgentJobStepQuery.BuildSql(2),
        };

        foreach (var (name, text) in statements)
        {
            Assert.True(text.Contains(QueryStoreCollector.SelfQueryMarker, StringComparison.Ordinal), name + " has no " + QueryStoreCollector.SelfQueryMarker + " marker");
        }
    }

    private sealed record SqlLiteral(int Start, int End, string Content);

    /// <summary>The string literals of a T-SQL text, un-doubled, with comments skipped (an apostrophe in a comment is not a literal).</summary>
    private static List<SqlLiteral> Literals(string sql)
    {
        var list = new List<SqlLiteral>();
        var i = 0;

        while (i < sql.Length)
        {
            if (sql[i] == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                var end = sql.IndexOf("*/", i + 2, StringComparison.Ordinal);
                i = end < 0 ? sql.Length : end + 2;
            }
            else if (sql[i] == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                var end = sql.IndexOf('\n', i);
                i = end < 0 ? sql.Length : end + 1;
            }
            else if (sql[i] == '\'')
            {
                var content = new StringBuilder();
                var j = i + 1;

                while (j < sql.Length)
                {
                    if (sql[j] == '\'')
                    {
                        if (j + 1 < sql.Length && sql[j + 1] == '\'')
                        {
                            content.Append('\'');
                            j += 2;
                            continue;
                        }

                        j++;
                        break;
                    }

                    content.Append(sql[j]);
                    j++;
                }

                list.Add(new SqlLiteral(i, j, content.ToString()));
                i = j;
            }
            else
            {
                i++;
            }
        }

        return list;
    }

    /// <summary>
    /// Every inner batch an <c>sp_executesql</c> in <paramref name="sql"/> runs: the N-literal right after it, the literal(s) a variable
    /// argument was built from (up to the statement's end), and, recursively, the batches inside a literal that itself calls
    /// <c>sp_executesql</c> (the per-database quote-doubled wrappers).
    /// </summary>
    private static IEnumerable<string> InnerBatches(string sql)
    {
        var literals = Literals(sql);

        for (var k = 0; k < literals.Count; k++)
        {
            var lit = literals[k];
            var before = sql[Math.Max(0, lit.Start - 80)..lit.Start];

            if (Regex.IsMatch(before, @"sp_executesql\s+N$", RegexOptions.IgnoreCase))
            {
                yield return lit.Content;
            }

            if (lit.Content.Contains("sp_executesql", StringComparison.OrdinalIgnoreCase))
            {
                foreach (var nested in InnerBatches(lit.Content))
                {
                    yield return nested;
                }
            }
        }

        foreach (Match call in Regex.Matches(sql, @"(?:sp_executesql|EXECUTE\s+@\w+)\s+(@\w+)", RegexOptions.IgnoreCase))
        {
            var variable = Regex.Escape(call.Groups[1].Value);
            var start = -1;

            for (var k = 0; k < literals.Count && literals[k].Start < call.Index; k++)
            {
                if (Regex.IsMatch(sql[Math.Max(0, literals[k].Start - 80)..literals[k].Start], variable + @"\b[^;=]*=\s*(?:CAST\(\s*)?N$", RegexOptions.IgnoreCase))
                {
                    start = k;
                }
            }

            if (start < 0)
            {
                yield return "(variable " + call.Groups[1].Value + " is not built from a literal in this query)";
                continue;
            }

            var text = new StringBuilder(literals[start].Content);

            for (var k = start + 1; k < literals.Count && literals[k].Start < call.Index; k++)
            {
                var gap = sql[literals[k - 1].End..literals[k].Start];

                if (gap.Contains(';', StringComparison.Ordinal))
                {
                    break;
                }

                text.Append(literals[k].Content);
            }

            yield return text.ToString();
        }
    }
}
