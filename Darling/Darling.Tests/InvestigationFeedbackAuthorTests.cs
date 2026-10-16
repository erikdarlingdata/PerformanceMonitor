/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The author a sign-in is stored as (#5913): a name V191 accepts is kept as typed; any other name keeps its distinguishing
/// part, so two sign-ins that differ only in characters V191 refuses never share one author (and so never overwrite each
/// other's feedback), and every result is an author V191 accepts.
/// </summary>
public sealed class InvestigationFeedbackAuthorTests
{
    [Theory]
    [InlineData("erik")]
    [InlineData("Erik.Darling-2")]
    [InlineData("svc:report_reader")]
    public void AName_V191Accepts_IsKeptAsTyped(string identity)
    {
        Assert.Equal(identity, InvestigationFeedbackRules.AuthorFromIdentity(identity));
        Assert.Equal(identity, InvestigationFeedbackRules.AuthorFromIdentity("  " + identity + " "));
    }

    [Fact]
    public void ABlankName_HasNoAuthor()
    {
        Assert.Null(InvestigationFeedbackRules.AuthorFromIdentity(null));
        Assert.Null(InvestigationFeedbackRules.AuthorFromIdentity("   "));
    }

    [Fact]
    public void SignInsThatDifferOnlyInRefusedCharacters_StayDistinctAuthors()
    {
        var names = new[]
        {
            "DOMAIN\\erik", "DOMAIN/erik", "DOMAIN erik", "DOMAIN_erik", "erik@example.com", "erik#example.com", "erik+1@example.com",
        };
        var authors = names.Select(n => InvestigationFeedbackRules.AuthorFromIdentity(n)!).ToList();
        Assert.Equal(names.Length, authors.Select(a => a.ToLowerInvariant()).Distinct().Count());
        Assert.All(authors, a => Assert.True(InvestigationFeedbackRules.IsValidAuthor(a), a));

        /* The readable part survives, and a name V191 accepts is not given a suffix. */
        Assert.StartsWith("DOMAIN_erik-", authors[0], StringComparison.Ordinal);
        Assert.Equal("DOMAIN_erik", authors[3]);
    }

    [Fact]
    public void TheSameSignIn_InAnyLetterCase_IsOneAuthorToTheUniqueIndex()
    {
        var a = InvestigationFeedbackRules.AuthorFromIdentity("DOMAIN\\Erik")!;
        var b = InvestigationFeedbackRules.AuthorFromIdentity("domain\\erik")!;
        Assert.Equal(a.ToLowerInvariant(), b.ToLowerInvariant());
        Assert.Equal(a, InvestigationFeedbackRules.AuthorFromIdentity("  DOMAIN\\Erik  "));
    }

    [Fact]
    public void LongNames_StayDistinct_PastTheSixtyFourthCharacter_AndFit()
    {
        var prefix = new string('a', 70);
        var one = InvestigationFeedbackRules.AuthorFromIdentity(prefix + "1")!;
        var two = InvestigationFeedbackRules.AuthorFromIdentity(prefix + "2")!;
        Assert.NotEqual(one, two);
        Assert.Equal(InvestigationFeedbackRules.MaxAuthorLength, one.Length);
        Assert.True(InvestigationFeedbackRules.IsValidAuthor(one));
        Assert.True(InvestigationFeedbackRules.IsValidAuthor(two));
    }
}
