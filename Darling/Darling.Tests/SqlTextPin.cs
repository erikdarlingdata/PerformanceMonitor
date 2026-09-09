/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3217: a pin that searches a reader's SQL for a clause is asserting that the statement still SCOPES,
/// FILTERS, BOUNDS or ORDERS the way its doc comment says. Spelled as a substring of the statement's
/// current text, it cannot tell that behaviour being removed from that text being rewritten to mean exactly
/// the same thing — and #3212 demonstrated the cost by qualifying <c>database_name</c> to
/// <c>ios.database_name</c> after adding a table alias, reddening six assertions in four files the commit
/// never touched.
///
/// <para><b>What this normalises, and what it deliberately does not.</b> Alias qualifiers are stripped and
/// runs of whitespace collapse to one space, on BOTH the needle and the statement, so the two are compared
/// in the same frame. Case is left alone: <c>'Unused'</c> and <c>'unused'</c> are different values, and a
/// pin on a classification literal is asserting the value the tool emits. Schema qualifiers are left alone
/// too — <c>collect.</c> and <c>config.</c> select between two different relations, so treating
/// <c>collect.x</c> and <c>x</c> as the same token would let a read drop the schema and stay green, which
/// is the opposite of the direction #3217 asks for.</para>
///
/// <para><b>A needle whose aliases carry the claim does not come through here.</b> The distinction is the
/// one Erik drew on the issue: a pin greping one statement's spelling to assert a BEHAVIOUR is the defect,
/// while a pin comparing spellings to assert they AGREE is sound, because agreement of text is the
/// property. <c>IndexUsageTruncationTests.TheCountAndTheRowsShareTheirFilter</c> is the clearest case and
/// stays on plain <see cref="Assert.Contains(string, string?, StringComparison)"/>; so does
/// <c>DarlingMcpPvsToolsTests</c>'s join condition, whose content is a relationship BETWEEN two aliases —
/// normalising it would turn <c>ON t.database_name = p.database_name</c> into
/// <c>ON database_name = database_name</c> and stop it noticing a join rewritten against one side twice.</para>
///
/// <para><b>Alias-insensitivity covers REFERENCES, not declarations.</b> An <c>AS ios</c> is text like
/// any other, so a needle carrying one matches only a statement that declares it. A clause is the right
/// size for a needle here; a <c>FROM</c> line with its alias is not.</para>
///
/// <para><b>Nor is it a substitute for executing the claim.</b> Where the doc comment states a behaviour
/// that a real store can be asked about, the behavioural assertion belongs in the live suite and this is
/// the always-runs structural guard beneath it — <c>IndexUsageTruncationTests</c> carries both, and says
/// which is which.</para>
/// </summary>
internal static class SqlTextPin
{
    /// <summary>
    /// A qualifier on an identifier: <c>ios.</c> in <c>ios.database_name</c>. The lookbehind keeps it off
    /// anything already inside a dotted chain or a number (<c>100.0</c>, <c>a.b.c</c>), and the lookahead
    /// requires a real identifier after the dot rather than <c>.5</c> or <c>.*</c>.
    /// </summary>
    private static readonly Regex AliasQualifier =
        new(@"(?<![A-Za-z0-9_.""])([A-Za-z_][A-Za-z0-9_]*)\.(?=[A-Za-z_])", RegexOptions.CultureInvariant);

    private static readonly Regex WhitespaceRun = new(@"\s+", RegexOptions.CultureInvariant);

    /// <summary>
    /// The prefixes that are NOT aliases. Taken from the generator rather than typed, so a renamed schema
    /// moves this with it; the three PostgreSQL-owned ones cannot be read off anything and are named.
    /// </summary>
    private static readonly HashSet<string> SchemaNames = new(StringComparer.OrdinalIgnoreCase)
    {
        PgSchemaGenerator.CollectSchema,
        PgSchemaGenerator.ConfigSchema,
        "public",
        "pg_catalog",
        "information_schema",
    };

    /// <summary>Alias qualifiers dropped, whitespace runs collapsed, ends trimmed.</summary>
    internal static string Normalise(string sql)
    {
        var unqualified = AliasQualifier.Replace(
            sql,
            match => SchemaNames.Contains(match.Groups[1].Value) ? match.Value : "");

        return WhitespaceRun.Replace(unqualified, " ").Trim();
    }

    /// <summary>
    /// <paramref name="sql"/> still expresses <paramref name="clause"/>, in any spelling that differs only
    /// by alias qualification or whitespace.
    ///
    /// <para><paramref name="because"/> is what the failure says, and it is not decoration: the whole cost
    /// #3217 reports is that a red text pin taught the wrong lesson — six failures about a reporter's
    /// question, from a commit that changed no behaviour, read as "the codebase forbids qualifying a
    /// column". A failure here has already ruled that out, so it can say what is actually gone.</para>
    /// </summary>
    internal static void AssertExpresses(string clause, string sql, string because)
    {
        var normalisedClause = Normalise(clause);

        Assert.True(
            Normalise(sql).Contains(normalisedClause, StringComparison.Ordinal),
            $"the statement no longer expresses `{normalisedClause}`, so {because}. Alias qualification and "
            + "whitespace are already normalised away on both sides, so this is not a spelling break — the "
            + "clause is absent.");
    }
}
