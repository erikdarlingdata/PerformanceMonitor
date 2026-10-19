/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The password key tables' grant and revoke lines are written twice (V165, #5366): in the service's managed-role batch
/// (<c>DarlingManagedRoles.cs</c>, step 10a) and in the hand-run <c>provision-roles.sql</c> (step 3g). This static test (no
/// database) pins that the two say the same thing, and that each comes after the last schema-wide grant on the
/// <c>config</c> schema in its own file, so no blanket grant can run after it.
/// </summary>
public sealed class PasswordKeyGrantParityTests
{
    private static readonly string[] Placeholders = { "admin", "viewer", "mcp", "config" };

    private static string Managed => RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedRoles.cs");

    private static string Script => RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql");

    /// <summary>The statement starting at <paramref name="start"/> up to its semicolon, placeholders spelled as the script spells them, whitespace folded.</summary>
    private static string Normalise(string text, int start)
    {
        var end = text.IndexOf(';', start);
        var statement = text[start..end];
        foreach (var name in Placeholders)
        {
            statement = statement.Replace("{" + name + "}", name, StringComparison.Ordinal);
        }

        return Regex.Replace(statement, @"\s+", " ").Trim();
    }

    private static (int Revoke, string RevokeText, string GrantText) Lines(string text)
    {
        var revoke = text.IndexOf("REVOKE ALL ON {config}.password_key", StringComparison.Ordinal);
        if (revoke < 0)
        {
            revoke = text.IndexOf("REVOKE ALL ON config.password_key", StringComparison.Ordinal);
        }

        Assert.True(revoke >= 0, "the key tables' REVOKE is missing");
        var grant = Regex.Match(text[revoke..], @"GRANT SELECT ON (\{config\}|config)\.password_key,");
        Assert.True(grant.Success, "the key tables' SELECT grant is missing");
        return (revoke, Normalise(text, revoke), Normalise(text, revoke + grant.Index));
    }

    [Fact]
    public void TheManagedBatchAndTheScript_RevokeAndGrantTheSameThing_OnTheKeyTables()
    {
        var managed = Lines(Managed);
        var script = Lines(Script);

        Assert.Equal(script.RevokeText, managed.RevokeText);
        Assert.Equal(script.GrantText, managed.GrantText);
        Assert.Equal(
            "REVOKE ALL ON config.password_key, config.password_key_service, config.legacy_secret_pin, config.legacy_secret_pin_marker FROM PUBLIC, admin, viewer, mcp CASCADE",
            managed.RevokeText);
        Assert.Equal("GRANT SELECT ON config.password_key, config.password_key_service TO admin", managed.GrantText);

        /* The candidate table (V167) has its own REVOKE, the same text in both files. */
        const string CandidateRevoke = "REVOKE ALL ON config.legacy_secret_pin_candidate FROM PUBLIC, admin, viewer, mcp CASCADE";
        foreach (var text in new[] { Managed, Script })
        {
            var at = text.IndexOf("REVOKE ALL ON {config}.legacy_secret_pin_candidate", StringComparison.Ordinal);
            if (at < 0)
            {
                at = text.IndexOf("REVOKE ALL ON config.legacy_secret_pin_candidate", StringComparison.Ordinal);
            }

            Assert.True(at >= 0, "the candidate table's REVOKE is missing");
            Assert.Equal(CandidateRevoke, Normalise(text, at));
        }

        /* Nothing else is granted on the key tables in either file. */
        foreach (var text in new[] { Managed, Script })
        {
            Assert.Single(Regex.Matches(text, @"GRANT [^;]*password_key"));
            Assert.DoesNotContain("legacy_secret_pin TO", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheScriptsGuard_NamesTheFourV165Tables_AndTheCandidateTableHasItsOwnBranch()
    {
        var script = Script.Replace("\r\n", "\n", StringComparison.Ordinal);
        var start = script.IndexOf("-- 3g.", StringComparison.Ordinal);
        var block = script[start..script.IndexOf("-- 4. Default privileges", start, StringComparison.Ordinal)];

        /* The guard: the four V165 tables only, so a store at V165 or V166 still gets their revoke. */
        var guardStart = block.IndexOf("IF pg_catalog.to_regclass('config.password_key')", StringComparison.Ordinal);
        Assert.True(guardStart >= 0, "the guard is missing");
        var guard = block[guardStart..block.IndexOf("THEN", guardStart, StringComparison.Ordinal)];
        Assert.Equal(4, Regex.Matches(guard, "to_regclass\\(").Count);
        Assert.DoesNotContain("legacy_secret_pin_candidate", guard, StringComparison.Ordinal);

        /* The candidate table is revoked in its own branch, inside the guarded block and after the four-table revoke. */
        var branch = Regex.Match(block,
            @"IF pg_catalog\.to_regclass\('config\.legacy_secret_pin_candidate'\) IS NOT NULL THEN\s+REVOKE ALL ON config\.legacy_secret_pin_candidate FROM PUBLIC, admin, viewer, mcp CASCADE;\s+END IF;");
        Assert.True(branch.Success, "the candidate table's own branch is missing");
        Assert.True(branch.Index > block.IndexOf("REVOKE ALL ON config.password_key,", StringComparison.Ordinal), "the branch follows the four-table revoke");
        Assert.True(branch.Index > guardStart, "the branch sits inside the guarded block");

        /* The four-table revoke names no candidate table. */
        var four = block[block.IndexOf("REVOKE ALL ON config.password_key,", StringComparison.Ordinal)..];
        four = four[..four.IndexOf(';', StringComparison.Ordinal)];
        Assert.DoesNotContain("legacy_secret_pin_candidate", four, StringComparison.Ordinal);

        /* The rung that creates the candidate table is V167, and both texts say so. */
        Assert.Contains("V167", block, StringComparison.Ordinal);
        Assert.Contains("V167 adds the legacy pin candidate table", Managed, StringComparison.Ordinal);
    }

    [Fact]
    public void EachBlock_ComesAfterTheLastSchemaWideConfigGrant_AndNoOtherGrantFollowsItInItsSection()
    {
        foreach (var (text, sectionEnd) in new[] { (Managed, "-- 11. Role memberships"), (Script, "-- 4. Default privileges") })
        {
            var revoke = Lines(text).Revoke;
            var schemaWide = Regex.Matches(text, @"(GRANT|REVOKE)\b[^;]*\bIN SCHEMA (\{config\}|config\b)[^;]*;")
                .Where(m => !m.Value.Contains("DEFAULT PRIVILEGES", StringComparison.Ordinal))
                .Select(m => m.Index)
                .ToList();
            Assert.NotEmpty(schemaWide);
            Assert.True(schemaWide.Max() < revoke, "a schema-wide grant on config comes after the key tables' REVOKE");

            var rest = text[revoke..text.IndexOf(sectionEnd, revoke, StringComparison.Ordinal)];
            Assert.Single(Regex.Matches(rest, @"(?m)^\s*GRANT\b[^;]*;"));
        }
    }
}
