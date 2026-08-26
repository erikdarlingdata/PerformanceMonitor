/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2612: the args model for the one PostgreSQL command that makes the product ACT on a monitored server.
///
/// <para>
/// Every other collector reaches its target through a parameterized query. This one composes DDL — the
/// <c>CREATE INDEX</c> text <c>hypopg_create_index</c> parses — from names a caller supplied, and that
/// function takes a whole statement as a string, so those names CANNOT be passed as parameters. They are
/// the single place in this feature where caller text reaches SQL text, which is why the acceptance rule is
/// "an unqualified, unquoted identifier or nothing" rather than an escaping routine. Refusing is auditable;
/// escaping is the thing that gets one case wrong three years later.
/// </para>
/// </summary>
public sealed class HypotheticalIndexRequestTests
{
    private const string Valid =
        """{"queryid":"-4149349750109407183","schemaName":"public","tableName":"t13","columns":["amount"]}""";

    [Fact]
    public void ACompleteRequestParses_AndComposesTheDdlItWillRun()
    {
        Assert.True(HypotheticalIndexRequest.TryParse(Valid, out var request));
        Assert.True(request.TryGetQueryId(out var queryId));
        Assert.Equal(-4149349750109407183L, queryId);
        Assert.Equal("CREATE INDEX ON public.t13 (amount)", request.BuildCreateIndexStatement());
    }

    /// <summary>
    /// <c>queryid</c> is a STRING both directions. It is a signed 64-bit value, and a JSON number would be
    /// rounded by any double-decoding parser into an id that resolves to no stored statement — the failure
    /// would be "no statement text captured", which is a sentence about a different problem entirely.
    /// </summary>
    [Fact]
    public void AQueryIdThatWouldNotSurviveADoubleRoundTrip_IsPreservedExactly()
    {
        Assert.True(HypotheticalIndexRequest.TryParse(Valid, out var request));
        Assert.True(request.TryGetQueryId(out var queryId));
        Assert.NotEqual((long)(double)queryId, queryId);
    }

    [Theory]
    /* No statement to re-plan. */
    [InlineData("""{"schemaName":"public","tableName":"t13","columns":["amount"]}""")]
    [InlineData("""{"queryid":"not-a-number","schemaName":"public","tableName":"t13","columns":["amount"]}""")]
    /* No candidate: this is not a tool for running EXPLAIN on somebody's query. */
    [InlineData("""{"queryid":"1","schemaName":"public","tableName":"t13","columns":[]}""")]
    [InlineData("""{"queryid":"1","schemaName":"public","columns":["amount"]}""")]
    /* Anything that is not a plain identifier — the injection surface, refused rather than escaped. */
    [InlineData("""{"queryid":"1","schemaName":"pub lic","tableName":"t13","columns":["amount"]}""")]
    [InlineData("""{"queryid":"1","schemaName":"public","tableName":"t13\");DROP TABLE t13;--","columns":["amount"]}""")]
    [InlineData("""{"queryid":"1","schemaName":"public","tableName":"t13","columns":["amount)) ,(1"]}""")]
    [InlineData("""{"queryid":"1","schemaName":"public","tableName":"t13","columns":["\"Quoted\""]}""")]
    /* Bounded: the column list is spliced into a statement, so an unbounded list is an unbounded statement. */
    [InlineData("""{"queryid":"1","schemaName":"public","tableName":"t13","columns":["a","b","c","d","e","f","g","h","i"]}""")]
    /* Not JSON at all. */
    [InlineData("")]
    [InlineData("not json")]
    public void AnythingUnusable_IsRefusedRatherThanRepaired(string argsJson)
        => Assert.False(HypotheticalIndexRequest.TryParse(argsJson, out _));

    [Theory]
    [InlineData("amount", true)]
    [InlineData("_private", true)]
    [InlineData("Mixed_Case_9", true)]
    [InlineData("", false)]
    [InlineData(" ", false)]
    [InlineData("9leading", false)]
    [InlineData("has space", false)]
    [InlineData("has-dash", false)]
    [InlineData("semi;colon", false)]
    [InlineData("quote\"mark", false)]
    public void IdentifierAcceptance_IsNarrowOnPurpose(string candidate, bool accepted)
        => Assert.Equal(accepted, HypotheticalIndexRequest.IsSafeIdentifier(candidate));

    /// <summary>
    /// A 64-character identifier is refused: PostgreSQL truncates at <c>NAMEDATALEN - 1</c> = 63, and an
    /// identifier that the server silently shortens is one this product would be reporting about under a
    /// name the server does not use.
    /// </summary>
    [Fact]
    public void AnIdentifierPastPostgresLimit_IsRefused()
    {
        Assert.True(HypotheticalIndexRequest.IsSafeIdentifier(new string('a', 63)));
        Assert.False(HypotheticalIndexRequest.IsSafeIdentifier(new string('a', 64)));
    }

    /// <summary>
    /// Composing DDL from an incomplete request throws rather than producing something. A future caller
    /// that skips <see cref="HypotheticalIndexRequest.TryParse"/> must not be able to reach a statement.
    /// </summary>
    [Fact]
    public void AnIncompleteRequestCannotComposeDdl()
        => Assert.Throws<InvalidOperationException>(
            () => new HypotheticalIndexRequest("1", "public", null, new[] { "amount" }, null).BuildCreateIndexStatement());

    [Fact]
    public void MultipleColumnsComposeInOrder()
    {
        var request = new HypotheticalIndexRequest("1", "public", "t13", new[] { "status", "amount" }, null);

        Assert.Equal("CREATE INDEX ON public.t13 (status, amount)", request.BuildCreateIndexStatement());
    }

    /// <summary>
    /// The source pin on the property that makes the experiment safe, and which two earlier designs did not
    /// have: the statement text is a BOUND parameter for its whole journey. It is staged into a
    /// transaction-local GUC and read back inside a <c>DO</c> block, so the SQL the bind step sees never
    /// contains a <c>$n</c> placeholder — which is what broke the obvious implementation, and what binding a
    /// NULL to silence that error silently corrupted.
    /// </summary>
    [Fact]
    public void TheExperimentNeverInterpolatesStatementTextIntoSql()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(),
            "Darling", "PerformanceMonitor.Darling.Service", "Targets", "HypotheticalIndexExperiment.cs"));

        Assert.Contains("SELECT set_config('pm.stmt', $1, true)", source, StringComparison.Ordinal);
        Assert.Contains("current_setting('pm.stmt')", source, StringComparison.Ordinal);

        /* The shape that fails: EXPLAIN with the statement pasted straight into the command text. */
        Assert.DoesNotContain("EXPLAIN (GENERIC_PLAN, FORMAT JSON) {statementText}", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// And the pin on the leak the verification run caught: <c>hypopg_reset()</c>, not just a rollback.
    ///
    /// <para>Hypothetical indexes are SESSION-local, not transaction-local — they live in the extension's
    /// own memory, so no transaction ever owned them. After two experiments and two rollbacks,
    /// <c>hypopg_list_indexes</c> still returned 2. On a pooled connection that means the next caller's
    /// plans are computed against phantom indexes, which is wrong in a way nothing anywhere would
    /// report.</para>
    /// </summary>
    [Fact]
    public void TheExperimentResetsHypopg_BecauseRollbackDoesNotRemoveIt()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(),
            "Darling", "PerformanceMonitor.Darling.Service", "Targets", "HypotheticalIndexExperiment.cs"));

        var finallyIndex = source.LastIndexOf("finally", StringComparison.Ordinal);
        Assert.True(finallyIndex >= 0, "The cleanup block is gone — this pin needs re-anchoring.");

        var cleanup = source[finallyIndex..];
        Assert.Contains("RollbackAsync", cleanup, StringComparison.Ordinal);
        Assert.Contains("hypopg_reset()", cleanup, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>EXPLAIN (GENERIC_PLAN)</c> arrived in PostgreSQL 16, and below it the experiment REFUSES rather
    /// than inventing parameter values — a plan built on a value nobody supplied is a plan for a query
    /// nobody ran, and it would be reported with the same confidence as a real one.
    /// </summary>
    [Fact]
    public void TheGenericPlanFloorIsPostgres16()
        => Assert.Equal(16, HypotheticalIndexExperiment.MinimumPostgresMajorForGenericPlan);

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
