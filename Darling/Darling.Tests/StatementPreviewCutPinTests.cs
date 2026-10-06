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
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5320: a statement preview is filtered, THEN cut. A URI's <c>user:secret@</c> is named by the at-sign that closes
/// it, so a preview cut BEFORE the at-sign is judged clean and returns part of the secret. These pins walk every cut
/// length over the shipping helper <see cref="McpHelpers.TruncateStatement"/> and <see cref="McpHelpers.StatementPreview"/>.
/// </summary>
public sealed class StatementPreviewCutPinTests
{
    /// <summary>Text in the corpus's named statements that must never leave in any preview of them.</summary>
    private static readonly string[] CorpusSecrets = { "S3cret", "secret-x", "sig=x", "0200AB", "'phrase'" };

    private static IEnumerable<(string Statement, string[] Needles)> Cases()
    {
        foreach (string statement in SensitiveStatementCorpus.Named)
        {
            foreach (string variant in SensitiveStatementCorpus.Variants(statement))
            {
                string[] needles = CorpusSecrets.Where(n => variant.Contains(n, StringComparison.Ordinal)).ToArray();
                if (needles.Length > 0) yield return (variant, needles);
            }
        }

        foreach (string scheme in new[] { "https", "ftp", "postgresql" })
        {
            yield return (
                "EXEC x @u = N'" + scheme + "://svc:" + StatementScrubCanary.UriSecret + "@host.example/path'",
                new[] { StatementScrubCanary.UriSecret, StatementScrubCanary.UriSecretPartial });
        }

        yield return (StatementScrubCanary.UriStatement(400), new[] { StatementScrubCanary.UriSecret, StatementScrubCanary.UriSecretPartial });
    }

    [Fact]
    public void ThePremise_APreviewCutBeforeTheFilterHoldsPartOfAUriSecret()
    {
        string statement = StatementScrubCanary.UriStatement(400);
        Assert.True(SensitiveStatements.Names(statement));

        string cutFirst = McpHelpers.Truncate(statement, 400)!;
        Assert.Contains(StatementScrubCanary.UriSecretPartial, cutFirst, StringComparison.Ordinal);
        Assert.False(SensitiveStatements.Names(cutFirst), "the cut text is judged clean, so the host's sweep would pass it");
    }

    [Fact]
    public void TruncateStatement_AtEveryCutLength_HoldsNoSecretOfTheStatement()
    {
        int checkedCuts = 0;
        foreach (var (statement, needles) in Cases())
        {
            Assert.True(SensitiveStatements.Names(statement), "every case is a named statement: " + statement.Trim());
            for (int cut = 0; cut <= statement.Length + 1; cut++)
            {
                string? preview = McpHelpers.TruncateStatement(statement, cut);
                foreach (string needle in needles)
                {
                    Assert.False(
                        preview != null && preview.Contains(needle, StringComparison.Ordinal),
                        $"TruncateStatement(cut {cut}) of '{statement.Trim()}' holds '{needle}'");
                }

                string? bare = McpHelpers.StatementPreview(statement, cut);
                foreach (string needle in needles)
                {
                    Assert.False(
                        bare != null && bare.Contains(needle, StringComparison.Ordinal),
                        $"StatementPreview(cut {cut}) of '{statement.Trim()}' holds '{needle}'");
                }

                checkedCuts++;
            }
        }

        Assert.True(checkedCuts > 1000, "the walk covered the corpus: " + checkedCuts);
    }

    [Fact]
    public void ACleanStatement_IsCutLikeTruncate_AndKeepsItsMarker()
    {
        string plain = StatementScrubCanary.PlainStatement;
        Assert.Equal(McpHelpers.Truncate(plain, 10), McpHelpers.TruncateStatement(plain, 10));
        Assert.Equal(plain, McpHelpers.TruncateStatement(plain, plain.Length));
        Assert.Equal(plain[..10], McpHelpers.StatementPreview(plain, 10));
        Assert.Null(McpHelpers.TruncateStatement(null, 10));
        Assert.Equal("", McpHelpers.TruncateStatement("", 10));
    }
}
