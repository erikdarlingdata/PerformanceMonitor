/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
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


    private static readonly string[] TrackedMethodNames =
    {
        "CollectAndScoreFactsAsync",
        "CollectConfigAuditFactsAsync",
        "ComparePeriodsAsync",
        "LookUpDispersionAsync",
    };

    // Every production call of the four on-demand methods anywhere under Lite/ must pass a CancellationToken
    // argument. A missed caller (#4203's own gap, at ComparePeriodsAsync's internal call inside
    // AnalysisService.cs) runs its read to completion after the caller has already given up. This scans
    // every .cs file under Lite/ (not just AnalysisService.cs) so a caller in Lite/Mcp or elsewhere is
    // covered the same as an internal caller. Deliberately unsophisticated: a call site is found by name
    // followed by '(' that is not itself the method's own declaration (no "async Task" immediately before
    // it on the same textual run), and the call's argument text — from the open paren to its matching
    // close paren — must contain "ancellationToken" (matches both "cancellationToken" and
    // "context.CancellationToken").
    [Fact]
    public void EveryProductionCallOfTheFourOnDemandMethods_PassesACancellationTokenArgument()
    {
        var liteRoot = Path.Combine(RepoFile.Root, "Lite");
        var files = Directory.GetFiles(liteRoot, "*.cs", SearchOption.AllDirectories);

        var missed = new List<string>();

        foreach (var file in files)
        {
            var text = File.ReadAllText(file);

            foreach (var methodName in TrackedMethodNames)
            {
                var searchStart = 0;

                while (true)
                {
                    var idx = text.IndexOf(methodName + "(", searchStart, StringComparison.Ordinal);
                    if (idx < 0)
                    {
                        break;
                    }

                    searchStart = idx + methodName.Length;

                    // Skip the method's own declaration: a declaration has "Task<...> " or "Task " right
                    // before the name, a call site does not (it has '.', a space after '=', 'await ', etc.
                    // right before it in every one of this file's own call sites, none of which is "> ").
                    var beforeStart = Math.Max(0, idx - 40);
                    var before = text[beforeStart..idx];
                    if (Regex.IsMatch(before, @">\s*$"))
                    {
                        continue;
                    }

                    // The collector's own interface method of the same name takes the whole AnalysisContext
                    // (which already carries the token inside it, at construction) rather than a separate
                    // CancellationToken argument — a different method, not a missed caller of the four
                    // AnalysisService methods this pin tracks.
                    if (before.EndsWith("_collector.", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    var openParen = idx + methodName.Length;
                    if (text[openParen] != '(')
                    {
                        continue;
                    }

                    var depth = 0;
                    var closeParen = -1;
                    for (var i = openParen; i < text.Length; i++)
                    {
                        if (text[i] == '(')
                        {
                            depth++;
                        }
                        else if (text[i] == ')')
                        {
                            depth--;
                            if (depth == 0)
                            {
                                closeParen = i;
                                break;
                            }
                        }
                    }

                    Assert.True(closeParen > openParen, $"{methodName}'s call in {file} at offset {idx} has no matching close paren.");

                    var argsText = text[openParen..(closeParen + 1)];

                    if (!argsText.Contains("ancellationToken", StringComparison.Ordinal))
                    {
                        var relative = Path.GetRelativePath(RepoFile.Root, file);
                        missed.Add($"{relative}: call of {methodName} at offset {idx} has no CancellationToken argument.");
                    }
                }
            }
        }

        Assert.True(missed.Count == 0, "Missed CancellationToken argument(s):\n" + string.Join("\n", missed));
    }
}
