/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common.QueryTags;

/// <summary>
/// What a rule matches on. <see cref="Application"/>, <see cref="Login"/>, <see cref="Host"/>,
/// <see cref="Database"/> and <see cref="Object"/> match the whole value; <see cref="Text"/> matches anywhere
/// in the first <see cref="QueryTagLimits.TextSampleCap"/> characters of the query text; <see cref="TextTag"/>
/// reads a <c>/* tag:NAME */</c> or <c>-- tag:NAME</c> comment in that text and uses NAME as the tag.
/// </summary>
public enum QueryTagRuleField
{
    Application = 0,
    Login = 1,
    Host = 2,
    Database = 3,
#pragma warning disable CA1720 // the stored value name is "object"
    Object = 4,
#pragma warning restore CA1720
    Text = 5,
    TextTag = 6,
}

/// <summary>
/// A tag rule. A pattern ignores case and treats <c>*</c> as the only wildcard; it is never a regular
/// expression. A <see cref="QueryTagRuleField.TextTag"/> rule carries no <paramref name="TagId"/>; every other
/// rule names the tag it applies.
/// </summary>
/// <param name="Id">The rule id, the tie-break between rules of equal priority (lower wins).</param>
/// <param name="Field">What the rule matches on.</param>
/// <param name="Pattern">The pattern. Unused by a <see cref="QueryTagRuleField.TextTag"/> rule.</param>
/// <param name="Priority">Lower runs first.</param>
/// <param name="Enabled">A disabled rule never matches.</param>
/// <param name="TagId">The tag the rule applies, or null for a <see cref="QueryTagRuleField.TextTag"/> rule.</param>
/// <param name="ServerId">The one server the rule applies to, or null for every server.</param>
public sealed record QueryTagRule(
    long Id,
    QueryTagRuleField Field,
    string? Pattern,
    int Priority,
    bool Enabled,
    long? TagId,
    int? ServerId = null);

/// <summary>
/// A hand assignment: a tag a user put on one query_hash (<see cref="QueryTagKeyKind.Hash"/>, by
/// <paramref name="Key"/>) or on one object (<see cref="QueryTagKeyKind.Object"/>, by
/// <paramref name="ObjectRef"/>). A caller key cannot be assigned by hand.
/// </summary>
/// <param name="TagId">The tag assigned.</param>
/// <param name="KeyKind">Hash or Object.</param>
/// <param name="Key">The query_hash text, for a hash assignment.</param>
/// <param name="ObjectRef">The object, for an object assignment.</param>
/// <param name="ServerId">The one server it applies to, or null for every server.</param>
public sealed record QueryTagHandAssignment(
    long TagId,
    QueryTagKeyKind KeyKind,
    string? Key = null,
    QueryTagObject? ObjectRef = null,
    int? ServerId = null);

/// <summary>Why a rule is refused. Each reason has its own code.</summary>
public enum QueryTagRuleError
{
    None = 0,
    EmptyPattern = 1,
    PatternTooLong = 2,
    TextTagNamesTag = 3,
    TagMissing = 4,
}

/// <summary>Validates a <see cref="QueryTagRule"/> before a store saves it.</summary>
public static class QueryTagRuleValidator
{
    public static QueryTagRuleError Validate(QueryTagRule rule)
    {
        ArgumentNullException.ThrowIfNull(rule);

        if (rule.Field == QueryTagRuleField.TextTag)
        {
            // A tag-comment rule reads its tag from the text, so it must not name one. Its pattern is unused.
            return rule.TagId is not null ? QueryTagRuleError.TextTagNamesTag : QueryTagRuleError.None;
        }

        if (string.IsNullOrWhiteSpace(rule.Pattern))
        {
            return QueryTagRuleError.EmptyPattern;
        }

        if (rule.Pattern.Length > QueryTagLimits.MaxPatternLength)
        {
            return QueryTagRuleError.PatternTooLong;
        }

        return rule.TagId is null ? QueryTagRuleError.TagMissing : QueryTagRuleError.None;
    }
}
