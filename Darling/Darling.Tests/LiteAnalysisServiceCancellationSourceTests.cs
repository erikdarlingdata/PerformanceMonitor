/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A source-census twin of Lite's on-demand analysis cancellation, checked without a live DuckDB or a project
/// reference to Lite: a text scan of <c>Lite/Analysis/AnalysisService.cs</c> confirming each of the four
/// on-demand methods — the shape Darling's <c>DarlingAnalysisService</c> twins already carry — declares a
/// <c>CancellationToken</c> parameter, and that the ONE catch inside each method's own body excludes
/// <see cref="OperationCanceledException"/> (the same #4203 rule Darling's <c>CollectConfigAuditFactsAsync</c>
/// states) so a cancelled read reaches the caller as a cancellation, not an empty result logged as a fault.
/// The wiring itself — that DuckDB actually observes the token — is proven by Lite.Tests' Windows-only pins;
/// this pin only proves the shape is present.
/// </summary>
public sealed class LiteAnalysisServiceCancellationSourceTests
{
    private const string AnalysisServicePath = "Lite/Analysis/AnalysisService.cs";

    // Each method's own body: sliced from its signature anchor to the next entry's signature anchor (or, for
    // the last one, to a fixed downstream anchor that is not itself one of the four). None of the four
    // signatures below nests a brace or a paren that would confuse a plain IndexOf, so a slice-and-scan is
    // enough — no balanced-brace walk needed.
    private const string CollectAndScoreFactsAsyncSignature =
        "public async Task<(List<Fact> Facts, WindowCoverage? Coverage, CollectionCaveatState Caveats)> CollectAndScoreFactsAsync(";
    private const string CollectConfigAuditFactsAsyncSignature =
        "public async Task<List<Fact>> CollectConfigAuditFactsAsync(";
    private const string ComparePeriodsAsyncSignature =
        "public async Task<(List<Fact> BaselineFacts, List<Fact> ComparisonFacts, WindowCoverage? BaselineCoverage, WindowCoverage? ComparisonCoverage, IReadOnlyDictionary<string, BaselineBucket> Dispersion)> ComparePeriodsAsync(";
    private const string LookUpDispersionAsyncSignature =
        "private async Task<IReadOnlyDictionary<string, BaselineBucket>> LookUpDispersionAsync(";
    // The next declaration after LookUpDispersionAsync in file order (#4203's four end here); anchors the
    // slice's end without depending on the four staying textually last in the file.
    private const string NextDeclarationAfterTheFourSignature =
        "public async Task<List<AnalysisFinding>> GetLatestFindingsAsync(int serverId)";

    [Theory]
    [InlineData("CollectAndScoreFactsAsync", CollectAndScoreFactsAsyncSignature, CollectConfigAuditFactsAsyncSignature)]
    [InlineData("CollectConfigAuditFactsAsync", CollectConfigAuditFactsAsyncSignature, ComparePeriodsAsyncSignature)]
    [InlineData("ComparePeriodsAsync", ComparePeriodsAsyncSignature, LookUpDispersionAsyncSignature)]
    [InlineData("LookUpDispersionAsync", LookUpDispersionAsyncSignature, NextDeclarationAfterTheFourSignature)]
    public void EachOnDemandMethod_DeclaresACancellationTokenParameterAndExcludesCancellationFromItsFaultCatch(
        string methodName, string signatureAnchor, string nextAnchor)
    {
        var text = ReadRepoFile("Lite", "Analysis", "AnalysisService.cs");

        var start = text.IndexOf(signatureAnchor, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{methodName}'s signature anchor was not found in {AnalysisServicePath}.");

        var end = text.IndexOf(nextAnchor, start + signatureAnchor.Length, StringComparison.Ordinal);
        Assert.True(end > start, $"{methodName}'s next-declaration anchor was not found after it in {AnalysisServicePath}.");

        var body = text[start..end];

        // The parameter list: from the anchor to the open brace that starts the body. Small span, no nested
        // parens in any of the four signatures, so a plain search suffices.
        var bodyOpenBrace = body.IndexOf('{', 0);
        Assert.True(bodyOpenBrace > 0, $"{methodName}'s body open-brace was not found.");
        var signature = body[..bodyOpenBrace];
        Assert.Contains("CancellationToken", signature, StringComparison.Ordinal);

        // #4203: the fault-swallowing catch inside this method's own body must not swallow a cancellation.
        var catchIndex = body.IndexOf("catch (Exception ex)", StringComparison.Ordinal);
        Assert.True(catchIndex >= 0, $"{methodName} has no `catch (Exception ex)` fault handler in {AnalysisServicePath}.");

        var catchLineEnd = body.IndexOf('\n', catchIndex);
        Assert.True(catchLineEnd > catchIndex, $"{methodName}'s catch line was not terminated.");
        var catchLine = body[catchIndex..catchLineEnd];

        Assert.Contains("when", catchLine, StringComparison.Ordinal);
        Assert.Contains("OperationCanceledException", catchLine, StringComparison.Ordinal);
    }
}
