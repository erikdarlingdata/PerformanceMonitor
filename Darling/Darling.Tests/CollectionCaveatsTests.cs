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
using System.Text.Json;
using System.Text.Json.Nodes;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3691: a pass whose fact families silently failed must say so. Every family read in all three collectors
/// degrades to "no facts" and LOGS (#2826); until now the pass learned nothing from it, so a pass in which
/// every family failed scored an empty fact list and both <c>analyze_server</c> tools rendered the
/// <c>empty</c> all-clear. This pins the shared machinery the fix rests on — the record, the family label,
/// the family count, the sentence, the block, and above all the rule that a CLEAN pass's payload is the
/// same object through the same serializer call, so its bytes cannot have moved.
/// </summary>
public sealed class CollectionCaveatsTests
{
    [Theory]
    [InlineData("CollectWriteFactsAsync", "write")]
    [InlineData("CollectPlanRegressionFactsAsync", "plan_regression")]
    [InlineData("CollectDatabaseSizeFactAsync", "database_size")]
    [InlineData("CollectObservedCoverageAsync", "observed_coverage")]
    [InlineData("CollectCpuUtilizationFactsAsync", "cpu_utilization")]
    [InlineData("CollectTraceFlagFactsAsync", "trace_flag")]
    [InlineData("SomethingElse", "something_else")]
    [InlineData("", "unknown")]
    public void TheFamilyLabel_IsDerivedFromTheCollectMethodName(string method, string expected) =>
        Assert.Equal(expected, CollectionFailure.FamilyOf(method));

    [Theory]
    [InlineData("/build/pm/Darling/PerformanceMonitor.Darling.Analysis/PgTargetFactCollector.Vacuum.cs", "vacuum")]
    [InlineData(@"D:\a\pm\Darling\PerformanceMonitor.Darling.Analysis\PgTargetFactCollector.Write.cs", "write")]
    [InlineData("/_/Darling/PerformanceMonitor.Darling.Analysis/PgTargetFactCollector.Sessions.cs", "sessions")]
    [InlineData("PgTargetFactCollector.Coverage.cs", "coverage")]
    [InlineData("Whatever.cs", "whatever")]
    [InlineData("", "unknown")]
    public void TheFamilyLabel_ForAPartialFile_IsTheDottedSegment_OnEitherSeparator(string path, string expected) =>
        Assert.Equal(expected, CollectionFailure.FamilyOfFile(path));

    /// <summary>
    /// The PostgreSQL-target collector labels by FILE because several of its families read through helper
    /// methods; every partial file must therefore map to a family the census knows, and vice versa.
    /// </summary>
    [Fact]
    public void EveryPgTargetPartialFile_LabelsAFamilyTheCollectSurfaceDeclares()
    {
        var declared = typeof(PgTargetFactCollector)
            .GetMethods(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.DeclaredOnly)
            .Where(m => m.Name.StartsWith("Collect", StringComparison.Ordinal) && m.Name.EndsWith("Async", StringComparison.Ordinal))
            .Select(m => CollectionFailure.FamilyOf(m.Name))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        var files = System.IO.Directory.GetFiles(RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Analysis"), "PgTargetFactCollector.*.cs")
            .Select(CollectionFailure.FamilyOfFile)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        /* The six files whose name is not the method's family word, spelled out so the mapping is a visible decision
           (the caveat labels by FILE, so a plan-family failure reads "plans", the source's own word). */
        var methodToFile = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["observed_coverage"] = "coverage",
            ["server_metadata"] = "metadata",
            ["session"] = "sessions",
            ["wait"] = "waits",
            ["query"] = "queries",
            ["plan"] = "plans",   /* #3691 v3 plumbing: CollectPlanFactsAsync in PgTargetFactCollector.Plans.cs, the Queries shape */
        };
        var expectedFiles = declared.Select(d => methodToFile.TryGetValue(d, out var f) ? f : d).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(expectedFiles, files);
    }

    [Fact]
    public void TheFamilyCount_IsDerivedFromTheCollectorType_AndMatchesTheEmissionLists()
    {
        /* 20 PostgreSQL-target families (the census in PgTargetFactCollectorTests names them — sixteen through the
           wave-3 blocking stub, nineteen since the #3691 v3 plumbing added the plan, kernel and memory stubs, twenty
           since lane 38 added the object-growth family with no stub in between; a stub is a family read that happens
           to return nothing, and the denominator counts reads, not facts); 32 SQL Server reads (thirty-one families
           plus the coverage witness, the class doc's count). */
        Assert.Equal(20, CollectionCaveats.CountFamilies(typeof(PgTargetFactCollector)));
        Assert.Equal(32, CollectionCaveats.CountFamilies(typeof(PgFactCollector)));
    }

    [Fact]
    public void TheClassifier_IsTheReportersThreeArms_PlusCancelled()
    {
        Assert.Equal(CollectionFailureOutcome.Timeout, PgFactCollector.ClassifyOutcome(new PostgresException("cancelled", "ERROR", "ERROR", "57014")));
        Assert.Equal(CollectionFailureOutcome.Timeout, PgFactCollector.ClassifyOutcome(new TimeoutException("timed out")));
        Assert.Equal(CollectionFailureOutcome.Timeout, PgFactCollector.ClassifyOutcome(new NpgsqlException("Exception while reading from stream", new TimeoutException())));
        Assert.Equal(CollectionFailureOutcome.MissingSchema, PgFactCollector.ClassifyOutcome(new PostgresException("no such table", "ERROR", "ERROR", "42P01")));
        Assert.Equal(CollectionFailureOutcome.MissingSchema, PgFactCollector.ClassifyOutcome(new PostgresException("no such column", "ERROR", "ERROR", "42703")));
        Assert.Equal(CollectionFailureOutcome.Cancelled, PgFactCollector.ClassifyOutcome(new OperationCanceledException()));
        Assert.Equal(CollectionFailureOutcome.Error, PgFactCollector.ClassifyOutcome(new NpgsqlException("Exception while reading from stream")));
        Assert.Equal(CollectionFailureOutcome.Error, PgFactCollector.ClassifyOutcome(new InvalidOperationException("boom")));
    }

    [Fact]
    public void RecordingAFailure_LandsOnTheContext_WithFamilyReadOutcomeAndMessage()
    {
        var context = new AnalysisContext { ServerId = 1, ServerName = "s" };
        Assert.Empty(context.CollectionFailures);
        Assert.Equal(0, context.CollectionFamilyCount);

        var ex = new PostgresException("relation \"x\" does not exist", "ERROR", "ERROR", "42P01");
        context.RecordCollectionFailure("write", "CollectWriteFactsAsync", CollectionFailureOutcome.MissingSchema, ex);

        var only = Assert.Single(context.CollectionFailures);
        Assert.Equal(
            new CollectionFailure(
                "write", "CollectWriteFactsAsync", CollectionFailureOutcome.MissingSchema,
                CollectionFailure.Describe(ex, CollectionFailureOutcome.MissingSchema)),
            only);
    }

    [Fact]
    public void ACleanPass_HasNoSentence_NoBlock_AndTheVerySamePayloadObject()
    {
        var context = new AnalysisContext();
        context.CollectionFamilyCount = 16;
        var state = CollectionCaveatState.From(context);

        Assert.False(state.Any);
        Assert.Null(state.Describe());
        Assert.Null(CollectionCaveats.ToPayload(context.CollectionFailures, 16));

        /* The byte-identity rule, by construction: the caller gets its own object back and serializes it
           exactly as it did before this change existed. */
        var payload = new { status = "empty", coverage = new { partial = false } };
        Assert.Same(payload, state.Attach(payload, McpHelpers.JsonOptions));
        Assert.Same(payload, CollectionCaveats.Attach(payload, [], 16, McpHelpers.JsonOptions));
        Assert.DoesNotContain("collection_caveats", JsonSerializer.Serialize(state.Attach(payload, McpHelpers.JsonOptions), McpHelpers.JsonOptions), StringComparison.Ordinal);

        Assert.Null(CollectionCaveats.Compose(null, null));
        Assert.Equal("cov", CollectionCaveats.Compose("cov", null));
    }

    [Fact]
    public void AFailingPass_AppendsTheBlockLast_AndLeavesEveryByteBeforeItUnchanged()
    {
        var context = new AnalysisContext { CollectionFamilyCount = 16 };
        var missingSchemaEx = new PostgresException("relation \"pg_autovacuum_stats\" does not exist", "ERROR", "ERROR", "42P01");
        var timeoutEx = new PostgresException("canceling statement due to user request", "ERROR", "ERROR", "57014");
        var errorEx = new InvalidOperationException("boom");
        context.RecordCollectionFailure("vacuum", "ReadAutovacuumBacklogAsync", CollectionFailureOutcome.MissingSchema, missingSchemaEx);
        context.RecordCollectionFailure("vacuum", "ReadXminHoldAsync", CollectionFailureOutcome.Timeout, timeoutEx);
        context.RecordCollectionFailure("write", "CollectWriteFactsAsync", CollectionFailureOutcome.Error, errorEx);
        var state = CollectionCaveatState.From(context);

        var payload = new { status = "empty", message = "No significant findings.", coverage = new { partial = false, observed_fraction = 1.0 } };
        var before = JsonSerializer.Serialize(payload, McpHelpers.JsonOptions);
        var after = JsonSerializer.Serialize(state.Attach(payload, McpHelpers.JsonOptions), McpHelpers.JsonOptions);

        /* Prefix-identical: everything the payload said before is said in the same bytes; the block is the
           last property. */
        Assert.StartsWith(before[..^1] + ",\"collection_caveats\":{", after, StringComparison.Ordinal);
        Assert.EndsWith("}}", after, StringComparison.Ordinal);

        var block = JsonNode.Parse(after)!["collection_caveats"]!.AsObject();
        /* DISTINCT families: vacuum failed twice and is one missing family. */
        Assert.Equal(2, block["families_failed"]!.GetValue<int>());
        Assert.Equal(16, block["families_total"]!.GetValue<int>());
        var entries = block["entries"]!.AsArray();
        Assert.Equal(3, entries.Count);
        Assert.Equal("vacuum", entries[0]!["family"]!.GetValue<string>());
        Assert.Equal("ReadAutovacuumBacklogAsync", entries[0]!["read"]!.GetValue<string>());
        Assert.Equal("missing_schema", entries[0]!["outcome"]!.GetValue<string>());
        Assert.Equal("timeout", entries[1]!["outcome"]!.GetValue<string>());
        Assert.Equal("write", entries[2]!["family"]!.GetValue<string>());
        Assert.Equal("error", entries[2]!["outcome"]!.GetValue<string>());
        Assert.Equal(CollectionFailure.Describe(errorEx, CollectionFailureOutcome.Error), entries[2]!["message"]!.GetValue<string>());

        var sentence = state.Describe();
        Assert.NotNull(sentence);
        Assert.StartsWith("2 of 16 fact families could not be read (vacuum (missing_schema/timeout), write (error)) — the absence of findings is not evidence", sentence, StringComparison.Ordinal);
        Assert.Contains("get_collection_health", sentence, StringComparison.Ordinal);

        Assert.Equal("cov " + sentence, CollectionCaveats.Compose("cov", sentence));
        Assert.Equal(sentence, CollectionCaveats.Compose(null, sentence));
    }

    [Fact]
    public void AnUnstampedTotal_IsSaidToBeUnknown_NotZero()
    {
        var context = new AnalysisContext();
        context.RecordCollectionFailure("write", "CollectWriteFactsAsync", CollectionFailureOutcome.Error, new InvalidOperationException("boom"));
        var sentence = CollectionCaveatState.From(context).Describe();
        Assert.StartsWith("1 of an unknown number of fact families could not be read", sentence, StringComparison.Ordinal);
    }
}
