/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
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

    /// <summary>
    /// Through the PUBLIC parser entry point rather than reflection into a private method. The redaction
    /// is shared with the RDS log-API transport (#2538), so testing it through the seam both callers use
    /// is the only way this guard covers both of them.
    /// </summary>
    private static JsonObject Redact(string json)
    {
        var parsed = PgPlanLogParser.FromBlock(1, 1.0, json);
        Assert.NotNull(parsed);
        return (JsonObject)JsonNode.Parse(parsed!.Value.PlanJson)!;
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

/// <summary>
/// The parser is shared by both transports — <c>pg_read_file</c> against a self-hosted log, and the RDS
/// <c>DownloadDBLogFilePortion</c> API for managed PostgreSQL, which has no filesystem (#2538). These pin
/// the entry point the API transport uses, which no collector test reaches.
///
/// <para>The redaction living in one place is the point. Every other divergence in this codebase has cost a
/// wrong number; this one would cost a customer's data.</para>
/// </summary>
public class PgPlanLogParserTests
{
    /* Built with explicit \t escapes rather than a raw string literal. auto_explain indents the JSON
       block with real TABS and the parser keys on them, but inside a raw literal \t is two characters
       rather than one - so a raw-literal fixture silently stops looking like a log and the extraction
       finds nothing. Caught by this test failing 1 != 2 after passing in a scratch harness that used
       escaped strings. */
    private const string RawLog =
        "2026-08-25 14:50:05.299 UTC [58] -3560200806914842915 LOG:  duration: 0.006 ms  plan:\n"
        + "\t{\n"
        + "\t  \"Query Text\": \"SELECT 1 FROM accounts WHERE email = 'someone@example.com';\",\n"
        + "\t  \"Plan\": {\n"
        + "\t    \"Node Type\": \"Seq Scan\",\n"
        + "\t    \"Relation Name\": \"accounts1\",\n"
        + "\t    \"Filter\": \"((email = 'someone@example.com'::text) AND (id > 100))\"\n"
        + "\t  }\n"
        + "\t}\n"
        + "2026-08-25 14:50:05.300 UTC [48] 0 LOG:  received fast shutdown request\n"
        + "2026-08-25 14:50:11.111 UTC [81] 510393640047350727 LOG:  duration: 11.877 ms  plan:\n"
        + "\t{\n"
        + "\t  \"Query Text\": \"SELECT 2;\",\n"
        + "\t  \"Plan\": { \"Node Type\": \"Result\" }\n"
        + "\t}\n";

    [Fact]
    public void ExtractPullsEveryPlanAndIgnoresOrdinaryLogLines()
    {
        var plans = PgPlanLogParser.Extract(RawLog);

        Assert.Equal(2, plans.Count);
        Assert.Equal(-3560200806914842915, plans[0].QueryId);
        Assert.Equal(510393640047350727, plans[1].QueryId);
        Assert.Equal(0.006, plans[0].DurationMs, 3);

        /* The shutdown line between them is not a plan and must not become one. */
        Assert.DoesNotContain(plans, p => p.TopNodeType is null);
    }

    [Fact]
    public void ExtractRedactsEveryPlanItReturns()
    {
        var plans = PgPlanLogParser.Extract(RawLog);

        foreach (var plan in plans)
        {
            Assert.DoesNotContain("Query Text", plan.PlanJson, StringComparison.Ordinal);
            Assert.DoesNotContain("someone@example.com", plan.PlanJson, StringComparison.Ordinal);
            Assert.DoesNotMatch(new Regex(@"'(?!\?')[^']+'"), plan.PlanJson);
        }

        /* And the identity survives: a relation named with a trailing digit is not a redacted number. */
        Assert.Contains("accounts1", plans[0].PlanJson, StringComparison.Ordinal);
    }

    /// <summary>
    /// The hash is of the REDACTED plan, so the same shape recurs to the same hash whatever values it ran
    /// with — which is the whole basis of dedup. Hashing raw text would defeat it exactly where it matters.
    /// </summary>
    [Fact]
    public void TheSameShapeWithDifferentValues_HashesTheSame()
    {
        var a = PgPlanLogParser.FromBlock(1, 1.0,
            """{"Plan":{"Node Type":"Seq Scan","Relation Name":"t","Filter":"(id = 1)"}}""");
        var b = PgPlanLogParser.FromBlock(1, 9.0,
            """{"Plan":{"Node Type":"Seq Scan","Relation Name":"t","Filter":"(id = 99999)"}}""");

        Assert.NotNull(a);
        Assert.NotNull(b);
        Assert.Equal(a!.Value.PlanHash, b!.Value.PlanHash);

        /* A genuinely different SHAPE must not collide with it. */
        var c = PgPlanLogParser.FromBlock(1, 1.0,
            """{"Plan":{"Node Type":"Index Scan","Relation Name":"t","Filter":"(id = 1)"}}""");
        Assert.NotEqual(a.Value.PlanHash, c!.Value.PlanHash);
    }

    /// <summary>
    /// Both transports read a BOUNDED window, so a block cut in half at the edge is ordinary rather than
    /// exceptional. It is skipped, never stored half-parsed, and never throws.
    /// </summary>
    [Fact]
    public void ATruncatedBlockIsSkippedRatherThanThrowing()
    {
        var truncated = "2026-08-25 14:50:05.299 UTC [58] 123 LOG:  duration: 0.006 ms  plan:\n\t{\n\t  \"Plan\": {\n";

        var plans = PgPlanLogParser.Extract(truncated);

        Assert.Empty(plans);
        Assert.Null(PgPlanLogParser.FromBlock(1, 1.0, "{not json"));
        Assert.Null(PgPlanLogParser.FromBlock(1, 1.0, null));
    }
}
