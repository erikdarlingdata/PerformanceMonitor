/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins plan capture, and above all the redaction — this is the one collector that reads text a customer
/// wrote, so a regression here publishes their data rather than merely reporting a wrong number.
/// </summary>
public class PgPlanCaptureCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
                PostgresVersionNum = 170000,
            },
        };

    private static string Sql => PgPlanCaptureCollector.Instance.BuildQuery(MakeContext()).Text;

    private static JsonObject Redact(string json)
    {
        var root = (JsonObject)JsonNode.Parse(json)!;
        root.Remove("Query Text");

        typeof(PgPlanCaptureCollector)
            .GetMethod("Redact", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, new object[] { (JsonObject)root["Plan"]! });

        return root;
    }

    [Fact]
    public void Identity_IsTheTableAndEngineTheStoreExpects()
    {
        Assert.Equal("pg_plan_capture", PgPlanCaptureCollector.Instance.Name);
        Assert.Equal("pg_plan_capture", PgPlanCaptureCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgPlanCaptureCollector.Instance.TargetEngine);
    }

    /// <summary>
    /// Query text never becomes a column. <c>auto_explain</c> emits it verbatim — literals and all — and
    /// <c>log_parameter_max_length = 0</c> does not suppress it, because that setting covers bind parameters
    /// only (measured on #2565). The statement identity is <c>query_id</c>.
    /// </summary>
    [Fact]
    public void ThereIsNoQueryTextColumn()
    {
        var names = PgPlanCaptureCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.DoesNotContain("query_text", names);
        Assert.DoesNotContain("statement", names);
        Assert.Contains("query_id", names);
    }

    /// <summary>
    /// The header is dropped before anything else touches the tree, and the value that was in it must not
    /// survive anywhere else either — it also appears inside <c>Filter</c>.
    /// </summary>
    [Fact]
    public void QueryTextAndItsLiterals_AreBothRemoved()
    {
        var root = Redact("""
            {
              "Query Text": "SELECT 1 FROM accounts WHERE email = 'someone@example.com';",
              "Plan": { "Node Type": "Seq Scan", "Relation Name": "accounts",
                        "Filter": "(email = 'someone@example.com'::text)" }
            }
            """);

        var json = root.ToJsonString();

        Assert.Null(root["Query Text"]);
        Assert.DoesNotContain("someone@example.com", json, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT 1 FROM accounts", json, StringComparison.Ordinal);
    }

    /// <summary>
    /// Redaction must not mangle IDENTITY. A blanket numeric strip would rewrite a relation genuinely named
    /// <c>transactionitems1</c> into <c>transactionitems?</c>, destroying the name to hide a value that was
    /// never there — so bare numbers are stripped only inside condition fields.
    /// </summary>
    [Fact]
    public void RedactionStripsValues_WithoutDestroyingNames()
    {
        var root = Redact("""
            {
              "Plan": { "Node Type": "Seq Scan", "Relation Name": "transactionitems1", "Alias": "t1",
                        "Plan Rows": 100, "Filter": "((id > 100) AND (v = 'secret'::text))" }
            }
            """);

        var plan = (JsonObject)root["Plan"]!;

        Assert.Equal("transactionitems1", plan["Relation Name"]!.GetValue<string>());
        Assert.Equal("t1", plan["Alias"]!.GetValue<string>());

        var filter = plan["Filter"]!.GetValue<string>();
        Assert.DoesNotContain("100", filter, StringComparison.Ordinal);
        Assert.DoesNotContain("secret", filter, StringComparison.Ordinal);

        /* Numeric JSON values are estimates, not customer data, and stay untouched. */
        Assert.Equal(100, plan["Plan Rows"]!.GetValue<int>());
    }

    /// <summary>
    /// Nested plans and string arrays carry values too — <c>Output</c> and the various key lists are arrays,
    /// and a child node's Filter is where most real literals live.
    /// </summary>
    [Fact]
    public void RedactionReachesNestedPlansAndArrays()
    {
        var root = Redact("""
            {
              "Plan": { "Node Type": "Limit", "Output": ["id", "'inline-literal'"],
                        "Plans": [ { "Node Type": "Seq Scan",
                                     "Filter": "(name = 'nested-secret'::text)" } ] }
            }
            """);

        var json = root.ToJsonString();

        Assert.DoesNotContain("nested-secret", json, StringComparison.Ordinal);
        Assert.DoesNotContain("inline-literal", json, StringComparison.Ordinal);
    }

    /// <summary>
    /// The log read is BOUNDED. #2565 measured 772 MB of log in twenty seconds at capture-everything, and a
    /// collector that read the file whole would become the server's largest reader.
    /// </summary>
    [Fact]
    public void TheLogReadIsBounded()
    {
        Assert.Contains("greatest(n.size -", Sql, StringComparison.Ordinal);
        Assert.Contains("pg_catalog.pg_read_file", Sql, StringComparison.Ordinal);

        /* The current file is discovered, not configured: log_filename is a strftime pattern. */
        Assert.Contains("pg_catalog.pg_ls_logdir()", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Server-wide, and it claims no database attribution: the log line prefix is not guaranteed to carry
    /// <c>%d</c>, so a database column would be a claim the source cannot support (#2599).
    /// </summary>
    [Fact]
    public void ItClaimsNoDatabaseAttribution()
    {
        Assert.False(PgPlanCaptureCollector.Instance.RunsPerDatabase(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17 }));

        Assert.DoesNotContain("database_name",
            PgPlanCaptureCollector.Instance.PayloadColumns.Select(c => c.Name));
    }
}
