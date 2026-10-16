/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common.QueryTags;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared query tag matcher (#5979, part of #5971): pattern semantics, the 4,000-character text cut, the
/// tag-comment rule, the precedence of hand assignments over rules, server scoping and the one-tag-per-key
/// rule. Pure unit tests; no store.
/// </summary>
public sealed class QueryTagResolverTests
{
    private const int Server = 1;
    private const long Billing = 10;
    private const long Reports = 20;
    private const long Hand = 30;

    private static readonly IReadOnlyDictionary<string, long> Tags =
        new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase) { ["billing"] = Billing, ["reports"] = Reports };

    private static QueryTagSighting Hash(string key = "0xABC", string? app = null, string? text = null,
        string? db = null, QueryTagObject? obj = null, string? login = null, string? host = null) =>
        new(QueryTagKeyKind.Hash, key, db, obj, app, login, host, text);

    private static QueryTagRule Rule(long id, QueryTagRuleField field, string? pattern, long? tag = Billing,
        int priority = 100, bool enabled = true, int? server = null) =>
        new(id, field, pattern, priority, enabled, tag, server);

    private static QueryTagResolution Resolve(IReadOnlyList<QueryTagSighting> sightings,
        IReadOnlyList<QueryTagRule>? rules = null, IReadOnlyList<QueryTagHandAssignment>? hand = null, int server = Server) =>
        QueryTagResolver.Resolve(server, sightings, hand ?? Array.Empty<QueryTagHandAssignment>(),
            rules ?? Array.Empty<QueryTagRule>(), Tags);

    // ---------- pattern semantics ----------

    [Fact]
    public void Pattern_IgnoresCase()
    {
        var r = Resolve([Hash(app: "SSMS - Query")], [Rule(1, QueryTagRuleField.Application, "ssms - QUERY")]);

        Assert.Equal(Billing, r.TagId);
    }

    [Theory]
    [InlineData("*Reporting", "Nightly Reporting", true)]       // star at the start
    [InlineData("Nightly*", "Nightly Reporting", true)]         // star at the end
    [InlineData("Night*port*", "Nightly Reporting", true)]      // star in the middle
    [InlineData("*", "anything", true)]
    [InlineData("Reporting*", "Nightly Reporting", false)]      // the start is anchored
    [InlineData("*Nightly", "Nightly Reporting", false)]        // the end is anchored
    [InlineData("a*a", "a", false)]                              // pieces do not overlap
    public void Star_MatchesAnyRunOfCharacters(string pattern, string value, bool expected)
    {
        Assert.Equal(expected, QueryTagPattern.MatchesWhole(pattern, value));
    }

    [Theory]
    [InlineData("a.c", "abc")]       // . is not any-character
    [InlineData("a(b", "ab")]         // an unbalanced ( would throw in a regex
    [InlineData("a[b", "ab")]         // an unbalanced [ would throw in a regex
    [InlineData("a[bc]d", "abd")]    // a character class is not one
    [InlineData("(a|b)", "a")]       // alternation is not alternation
    [InlineData("a+", "aa")]         // a quantifier is not one
    [InlineData("^a$", "a")]         // anchors are not anchors
    public void RegexCharacters_AreLiteral(string pattern, string notMatched)
    {
        Assert.False(QueryTagPattern.MatchesWhole(pattern, notMatched));
    }

    [Theory]
    [InlineData("a.c")]
    [InlineData("a(b")]
    [InlineData("a[b")]
    [InlineData("a[bc]d")]
    public void RegexCharacters_MatchThemselves(string literal)
    {
        Assert.True(QueryTagPattern.MatchesWhole(literal, literal));
    }

    [Fact]
    public void Text_MatchesInsideTheSample_ButApplicationDoesNot()
    {
        var sightings = new[] { Hash(app: "Some App", text: "SELECT * FROM dbo.Invoices WHERE 1 = 1") };

        Assert.Equal(Billing, Resolve(sightings, [Rule(1, QueryTagRuleField.Text, "dbo.Invoices")]).TagId);
        Assert.Null(Resolve(sightings, [Rule(1, QueryTagRuleField.Application, "App")]).TagId);
    }

    // ---------- the 4,000-character cut ----------

    [Fact]
    public void Text_SeesCharacter4000_ButNotCharacter4001()
    {
        var rule = Rule(2, QueryTagRuleField.Text, "Z");

        // The marker is character 4,000 (the last one the matcher reads): it fires.
        string lastAt4000 = new string('x', QueryTagLimits.TextSampleCap - 1) + "Z";
        // The marker is character 4,001: it does not fire.
        string firstAt4001 = new string('x', QueryTagLimits.TextSampleCap) + "Z";
        // A marker that straddles the cut is cut in half and does not fire.
        string straddles = new string('x', QueryTagLimits.TextSampleCap - 1) + "ZZ";

        Assert.True(Resolve([Hash(text: lastAt4000)], [rule]).HasTag);
        Assert.False(Resolve([Hash(text: firstAt4001)], [rule]).HasTag);
        Assert.False(Resolve([Hash(text: straddles)], [Rule(3, QueryTagRuleField.Text, "ZZ")]).HasTag);
    }

    // ---------- the tag comment rule ----------

    [Theory]
    [InlineData("/* tag:billing */ SELECT 1")]
    [InlineData("SELECT 1 /*tag:Billing*/")]
    [InlineData("-- tag:BILLING\nSELECT 1")]
    public void TagComment_TagsTheKeyWithTheNamedTag(string text)
    {
        var r = Resolve([Hash(text: text)], [Rule(7, QueryTagRuleField.TextTag, null, tag: null)]);

        Assert.Equal(Billing, r.TagId);
        Assert.Equal(QueryTagSource.Rule, r.Source);
        Assert.Equal(7, r.RuleId);
        Assert.Equal(QueryTagRuleField.TextTag, r.MatchedField);
    }

    [Fact]
    public void TagComment_NamingAnUnknownTag_DoesNotMatch_AndTheNextRuleIsTried()
    {
        var sightings = new[] { Hash(text: "/* tag:nosuch */ SELECT 1 FROM dbo.Invoices") };
        var rules = new[]
        {
            Rule(1, QueryTagRuleField.TextTag, null, tag: null, priority: 1),
            Rule(2, QueryTagRuleField.Text, "Invoices", tag: Reports, priority: 2),
        };

        var r = Resolve(sightings, rules);

        Assert.Equal(Reports, r.TagId);
        Assert.Equal(2, r.RuleId);
    }

    [Fact]
    public void TagComment_AfterTheUnknownOne_IsStillRead()
    {
        var r = Resolve([Hash(text: "/* tag:nosuch */ /* tag:reports */ SELECT 1")],
            [Rule(1, QueryTagRuleField.TextTag, null, tag: null)]);

        Assert.Equal(Reports, r.TagId);
    }

    [Fact]
    public void TagWord_OutsideAComment_IsNotATagComment()
    {
        var r = Resolve([Hash(text: "SELECT 'tag:billing'")], [Rule(1, QueryTagRuleField.TextTag, null, tag: null)]);

        Assert.False(r.HasTag);
    }

    // ---------- precedence ----------

    [Fact]
    public void HandHash_BeatsHandObject_BeatsRule()
    {
        var obj = new QueryTagObject("Sales", "dbo", "usp_Invoice");
        var sightings = new[] { Hash("0xABC", app: "App", obj: obj) };
        var rules = new[] { Rule(1, QueryTagRuleField.Application, "App", tag: Reports) };
        var handObject = new QueryTagHandAssignment(Reports, QueryTagKeyKind.Object, ObjectRef: obj);
        var handHash = new QueryTagHandAssignment(Hand, QueryTagKeyKind.Hash, Key: "0xabc");

        var viaRule = Resolve(sightings, rules);
        var viaObject = Resolve(sightings, rules, [handObject]);
        var viaHash = Resolve(sightings, rules, [handObject, handHash]);

        Assert.Equal(QueryTagSource.Rule, viaRule.Source);
        Assert.Equal(QueryTagSource.HandObject, viaObject.Source);
        Assert.Equal(Reports, viaObject.TagId);
        Assert.Equal(QueryTagSource.HandHash, viaHash.Source);
        Assert.Equal(Hand, viaHash.TagId);
    }

    [Fact]
    public void HandObject_TagsAHashWhoseSightingNamesThatObject()
    {
        var obj = new QueryTagObject("Sales", "dbo", "usp_Invoice");
        var sightings = new[] { Hash("0x1", app: "A"), Hash("0x1", app: "B", obj: new QueryTagObject("sales", "DBO", "USP_INVOICE")) };

        var r = Resolve(sightings, hand: [new QueryTagHandAssignment(Hand, QueryTagKeyKind.Object, ObjectRef: obj)]);

        Assert.Equal(Hand, r.TagId);
        Assert.Equal(QueryTagSource.HandObject, r.Source);
    }

    [Fact]
    public void HandHash_ForThisServer_BeatsOneForEveryServer_AndDoesNotApplyToAnotherServer()
    {
        var sightings = new[] { Hash("0xABC") };
        var everyServer = new QueryTagHandAssignment(Reports, QueryTagKeyKind.Hash, Key: "0xABC");
        var thisServer = new QueryTagHandAssignment(Hand, QueryTagKeyKind.Hash, Key: "0xABC", ServerId: Server);

        Assert.Equal(Hand, Resolve(sightings, hand: [everyServer, thisServer]).TagId);
        Assert.Equal(Reports, Resolve(sightings, hand: [everyServer, thisServer], server: 2).TagId);
        Assert.False(Resolve(sightings, hand: [thisServer], server: 2).HasTag);
    }

    [Fact]
    public void HandHash_BeatsAMatchingRule()
    {
        var r = Resolve([Hash("0xABC", app: "App")], [Rule(1, QueryTagRuleField.Application, "App", tag: Reports)],
            [new QueryTagHandAssignment(Hand, QueryTagKeyKind.Hash, Key: "0xABC")]);

        Assert.Equal(Hand, r.TagId);
    }

    // ---------- rule order, scope and state ----------

    [Fact]
    public void TwoMatchingRules_GiveOneTag_TheLowerPriorityNumber()
    {
        var rules = new[]
        {
            Rule(1, QueryTagRuleField.Application, "App", tag: Reports, priority: 50),
            Rule(2, QueryTagRuleField.Application, "App", tag: Billing, priority: 10),
        };

        var r = Resolve([Hash(app: "App")], rules);

        Assert.Equal(Billing, r.TagId);
        Assert.Equal(2, r.RuleId);
    }

    [Fact]
    public void TwoMatchingRulesOfEqualPriority_TheLowerIdWins()
    {
        var rules = new[]
        {
            Rule(9, QueryTagRuleField.Application, "App", tag: Reports),
            Rule(4, QueryTagRuleField.Application, "App", tag: Billing),
        };

        var r = Resolve([Hash(app: "App")], rules);

        Assert.Equal(Billing, r.TagId);
        Assert.Equal(4, r.RuleId);
    }

    [Fact]
    public void DisabledRule_NeverMatches()
    {
        var r = Resolve([Hash(app: "App")], [Rule(1, QueryTagRuleField.Application, "App", enabled: false)]);

        Assert.False(r.HasTag);
        Assert.Equal(QueryTagSource.None, r.Source);
    }

    [Fact]
    public void ServerScopedRule_DoesNotFireForAnotherServer()
    {
        var rules = new[] { Rule(1, QueryTagRuleField.Application, "App", server: 5) };

        Assert.False(Resolve([Hash(app: "App")], rules, server: 6).HasTag);
        Assert.True(Resolve([Hash(app: "App")], rules, server: 5).HasTag);
    }

    [Fact]
    public void HashSeenFromTwoApplications_TakesTheTagOfTheFirstRuleThatMatchesEither()
    {
        var sightings = new[] { Hash("0x1", app: "Alpha"), Hash("0x1", app: "Beta") };
        var rules = new[]
        {
            Rule(1, QueryTagRuleField.Application, "Beta", tag: Reports, priority: 1),
            Rule(2, QueryTagRuleField.Application, "Alpha", tag: Billing, priority: 2),
        };

        var r = Resolve(sightings, rules);

        Assert.Equal(Reports, r.TagId);
        Assert.Equal("Beta", r.MatchedValue);
    }

    [Fact]
    public void Resolution_SaysWhy()
    {
        var r = Resolve([Hash(host: "BILLING-01")], [Rule(33, QueryTagRuleField.Host, "billing*")]);

        Assert.Equal(QueryTagSource.Rule, r.Source);
        Assert.Equal(33, r.RuleId);
        Assert.Equal(QueryTagRuleField.Host, r.MatchedField);
        Assert.Equal("BILLING-01", r.MatchedValue);
    }

    // ---------- which fields reach which key kind ----------

    [Fact]
    public void CallerKey_IsNotMatchedByAnObjectRule_ButIsByCallerFields()
    {
        var caller = new QueryTagSighting(QueryTagKeyKind.Caller, "App|sa|HOST|Sales", Database: "Sales",
            ObjectRef: new QueryTagObject("Sales", "dbo", "p"), Application: "App", Login: "sa", Host: "HOST");

        Assert.False(Resolve([caller], [Rule(1, QueryTagRuleField.Object, "*")]).HasTag);
        Assert.True(Resolve([caller], [Rule(1, QueryTagRuleField.Application, "App")]).HasTag);
        Assert.True(Resolve([caller], [Rule(1, QueryTagRuleField.Login, "sa")]).HasTag);
        Assert.True(Resolve([caller], [Rule(1, QueryTagRuleField.Host, "host")]).HasTag);
        Assert.True(Resolve([caller], [Rule(1, QueryTagRuleField.Database, "sales")]).HasTag);
    }

    [Fact]
    public void ObjectKey_IsMatchedOnlyByObjectAndDatabaseRules()
    {
        var obj = new QueryTagObject("Sales", "dbo", "usp_Invoice");
        var key = new QueryTagSighting(QueryTagKeyKind.Object, obj.Qualified, Database: "Sales", ObjectRef: obj,
            Application: "App", TextSample: "EXEC usp_Invoice");

        Assert.True(Resolve([key], [Rule(1, QueryTagRuleField.Object, "sales.dbo.usp_*")]).HasTag);
        Assert.True(Resolve([key], [Rule(1, QueryTagRuleField.Database, "Sales")]).HasTag);
        Assert.False(Resolve([key], [Rule(1, QueryTagRuleField.Application, "App")]).HasTag);
        Assert.False(Resolve([key], [Rule(1, QueryTagRuleField.Text, "usp_Invoice")]).HasTag);
    }

    // ---------- validation ----------

    [Fact]
    public void Validation_RefusesEachInvalidRuleWithItsOwnCode()
    {
        Assert.Equal(QueryTagRuleError.EmptyPattern, QueryTagRuleValidator.Validate(Rule(1, QueryTagRuleField.Text, "")));
        Assert.Equal(QueryTagRuleError.EmptyPattern, QueryTagRuleValidator.Validate(Rule(1, QueryTagRuleField.Text, null)));
        Assert.Equal(QueryTagRuleError.PatternTooLong,
            QueryTagRuleValidator.Validate(Rule(1, QueryTagRuleField.Text, new string('a', QueryTagLimits.MaxPatternLength + 1))));
        Assert.Equal(QueryTagRuleError.TextTagNamesTag,
            QueryTagRuleValidator.Validate(Rule(1, QueryTagRuleField.TextTag, null, tag: Billing)));
        Assert.Equal(QueryTagRuleError.TagMissing, QueryTagRuleValidator.Validate(Rule(1, QueryTagRuleField.Text, "x", tag: null)));

        Assert.Equal(QueryTagRuleError.None,
            QueryTagRuleValidator.Validate(Rule(1, QueryTagRuleField.Text, new string('a', QueryTagLimits.MaxPatternLength))));
        Assert.Equal(QueryTagRuleError.None, QueryTagRuleValidator.Validate(Rule(1, QueryTagRuleField.TextTag, null, tag: null)));
    }
}
