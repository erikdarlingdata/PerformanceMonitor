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
/// <c>Darling/tools/provision-roles.sql</c> and psql's error handling (#4746). psql carries on after a failed
/// statement unless <c>ON_ERROR_STOP</c> is set, so the role-collision guard's <c>RAISE</c> used to fail only its own
/// <c>DO</c> block: the <c>ALTER ROLE ... PASSWORD</c> lines that follow it then reset the password of the existing
/// role the guard had just refused, and every grant after them ran as well. The fix is <c>\set ON_ERROR_STOP</c>
/// around the guard ALONE: a global <c>-v ON_ERROR_STOP=1</c> would also stop an owner who is not a superuser at the
/// <c>temp_file_limit</c> line, before it had made a single grant. The script saves the caller's own value before the
/// guard and puts it back after, so a caller who ran psql with <c>-v ON_ERROR_STOP=1</c> (or set it in psqlrc) keeps it.
/// </summary>
public sealed class ProvisionRolesCollisionGuardTests
{
    private const string GuardRaise = "RAISE EXCEPTION 'Role \"";

    private static string Script() => RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql");

    /// <summary>The stop is switched on before the first guard block and put back after the last one, and both
    /// happen before the first <c>ALTER ROLE ... PASSWORD</c> - so a refused role is never re-keyed - and before
    /// the <c>temp_file_limit</c> line, so a failure there still lets the run finish its grants.</summary>
    [Fact]
    public void OnErrorStop_IsOnForTheCollisionGuardAlone_AndPutBackBeforeTheFirstPasswordReset()
    {
        var sql = Script();

        var on = Regex.Match(sql, @"(?m)^\\set ON_ERROR_STOP on[ \t]*\r?$");
        var off = Regex.Match(sql, @"(?m)^\\set ON_ERROR_STOP :darling_saved_stop[ \t]*\r?$");
        Assert.True(on.Success, "provision-roles.sql no longer runs '\\set ON_ERROR_STOP on' before the role-collision guard.");
        Assert.True(off.Success, "provision-roles.sql never puts the caller's ON_ERROR_STOP back after the role-collision guard.");
        Assert.Equal(2, Regex.Matches(sql, @"(?m)^\\set ON_ERROR_STOP\b").Count);

        var firstRaise = sql.IndexOf(GuardRaise, StringComparison.Ordinal);
        var lastRaise = sql.LastIndexOf(GuardRaise, StringComparison.Ordinal);
        Assert.True(firstRaise >= 0 && lastRaise > firstRaise, "the role-collision guard's RAISE EXCEPTION lines moved or were renamed - update this pin with them.");
        var guardStart = sql.LastIndexOf("DO $$", firstRaise, StringComparison.Ordinal);
        var guardEnd = sql.IndexOf("END $$;", lastRaise, StringComparison.Ordinal);
        Assert.True(guardStart >= 0 && guardEnd > guardStart, "could not find the DO block that holds the role-collision guard.");

        Assert.True(on.Index < guardStart, "ON_ERROR_STOP must be on BEFORE the first guard DO block, or that block's RAISE fails only itself.");
        Assert.True(off.Index > guardEnd, "the caller's ON_ERROR_STOP must be put back AFTER the last guard DO block.");

        var firstPasswordReset = Regex.Match(sql, @"(?m)^ALTER ROLE \w+\s+LOGIN NOSUPERUSER PASSWORD");
        Assert.True(firstPasswordReset.Success, "the ALTER ROLE ... PASSWORD lines are gone or renamed - update this pin with them.");
        Assert.True(on.Index < firstPasswordReset.Index && off.Index < firstPasswordReset.Index,
            "the stop must be on and put back before the first ALTER ROLE ... PASSWORD, or the guard no longer stops that reset.");

        /* Only the guard stops the run: step 0's SET (superuser-only once pg_stat_statements is loaded) sits before
           the stop, and the temp_file_limit line (superuser-only) after it, so an owner who is not a superuser
           still gets every grant it can make. */
        var stepZero = sql.IndexOf("SET pg_stat_statements.track_utility = off;", StringComparison.Ordinal);
        Assert.True(stepZero >= 0 && stepZero < on.Index, "step 0's SET must stay outside the stop.");
        var tempFileLimit = sql.IndexOf("ALTER ROLE viewer SET temp_file_limit", StringComparison.Ordinal);
        Assert.True(tempFileLimit > off.Index, "the temp_file_limit lines must stay outside the stop.");
    }

    /// <summary>The caller's own <c>ON_ERROR_STOP</c> (psql <c>-v ON_ERROR_STOP=1</c>, or psqlrc) survives the guard: the
    /// script saves it before it switches the stop on, and puts it back right after the last guard block. It never sets
    /// the variable to off, which would override a caller who asked psql to stop at the first error. From psql 10 an unset
    /// <c>ON_ERROR_STOP</c> reads as off, so the saved value is always on or off; in psql 9.6 it would stay the literal
    /// text and read as on, which is why the script and the README ask for psql 10 or later.</summary>
    [Fact]
    public void TheCallersOnErrorStop_IsSavedBeforeTheGuard_AndPutBackRightAfterIt_NeverForcedOff()
    {
        var sql = Script();

        var save = Regex.Match(sql, @"(?m)^\\set darling_saved_stop :ON_ERROR_STOP[ \t]*\r?$");
        var on = Regex.Match(sql, @"(?m)^\\set ON_ERROR_STOP on[ \t]*\r?$");
        var restore = Regex.Match(sql, @"(?m)^\\set ON_ERROR_STOP :darling_saved_stop[ \t]*\r?$");
        Assert.True(save.Success, "provision-roles.sql no longer saves the caller's ON_ERROR_STOP ('\\set darling_saved_stop :ON_ERROR_STOP') before the role-collision guard.");
        Assert.True(on.Success && restore.Success, "provision-roles.sql no longer switches the stop on for the guard and puts the caller's value back.");
        Assert.Single(Regex.Matches(sql, @"(?m)^\\set darling_saved_stop\b"));
        Assert.True(save.Index < on.Index, "the caller's value must be saved BEFORE the stop is switched on, or the script saves its own 'on'.");

        var lastRaise = sql.LastIndexOf(GuardRaise, StringComparison.Ordinal);
        var guardEnd = lastRaise < 0 ? -1 : sql.IndexOf("END $$;", lastRaise, StringComparison.Ordinal);
        Assert.True(guardEnd > lastRaise && lastRaise >= 0, "could not find the end of the DO block that holds the role-collision guard.");
        var afterGuard = guardEnd + "END $$;".Length;
        Assert.True(restore.Index >= afterGuard, "the caller's ON_ERROR_STOP must be put back AFTER the last guard DO block.");
        Assert.True(
            string.IsNullOrWhiteSpace(sql[afterGuard..restore.Index]),
            "the caller's ON_ERROR_STOP must be put back right after the guard: nothing may run between the guard's END and the restore.");

        Assert.DoesNotMatch(@"(?mi)^\\set\s+ON_ERROR_STOP\s+(off|0|false|no|f|n)\b", sql);
        Assert.DoesNotContain("\\unset ON_ERROR_STOP", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The comment above the <c>temp_file_limit</c> lines used to say a failure there stops the script. It
    /// does not: psql carries on, so the roles, the grants and the secret-column carve still land.</summary>
    [Fact]
    public void TheTempFileLimitComment_DoesNotClaimAFailureStopsTheScript()
    {
        var sql = Script();

        Assert.DoesNotContain("rest of the script does not run", sql, StringComparison.Ordinal);
        Assert.Contains("psql carries on", sql, StringComparison.Ordinal);
    }

    /// <summary>The live script test feeds the file to Npgsql, which cannot run psql's meta-commands, so it drops the
    /// lines that start with a backslash - the three <c>\set</c> lines - and nothing else: comments that mention a
    /// backslash command further along a line, and lines that only start with white space, stay.</summary>
    [Fact]
    public void TheLiveScriptTest_DropsOnlyTheLinesThatStartWithABackslash()
    {
        var sql = Script();
        var stripped = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(sql);

        var before = sql.Split('\n');
        var after = stripped.Split('\n');
        Assert.Equal(3, before.Length - after.Length);
        Assert.Equal(before.Where(line => !line.StartsWith('\\')), after);
        Assert.DoesNotContain(after, line => line.StartsWith('\\'));

        Assert.Equal(
            "SELECT 1;\r\n-- see \\password\r\n  \\set kept\r\nSELECT 2;",
            ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands("SELECT 1;\r\n\\set ON_ERROR_STOP on\r\n-- see \\password\r\n  \\set kept\r\nSELECT 2;"));
    }

    /// <summary>Both places that show the psql command line run it with <c>-X</c>, so the operator's own psqlrc stays
    /// out of the run: an <c>\set AUTOCOMMIT off</c> in it leaves every role, grant and setting uncommitted while psql
    /// still exits 0.</summary>
    [Fact]
    public void TheShownPsqlCommandLines_SkipThePsqlrc()
    {
        var readmeLine = Regex.Match(RepoFile.ReadRepoFile("Darling", "README.md"), @"(?m)^psql .*provision-roles\.sql.*$");
        Assert.True(readmeLine.Success, "the README no longer shows the psql command line that runs provision-roles.sql.");
        Assert.Matches(@"^psql -X ", readmeLine.Value);

        var headerLine = Regex.Match(Script(), @"(?m)^--\s+psql .*provision-roles\.sql.*$");
        Assert.True(headerLine.Success, "provision-roles.sql no longer shows the psql command line that runs it in its header.");
        Assert.Matches(@"^--\s+psql -X ", headerLine.Value);
    }

    /// <summary>The guard tests the <c>darling-managed</c> marker and nothing else, so the README says the role is
    /// missing the marker, not that Darling did not create it (a role made some other way can carry the marker).</summary>
    [Fact]
    public void TheReadme_SaysTheGuardChecksTheMarkerAlone()
    {
        var readme = RepoFile.ReadRepoFile("Darling", "README.md");

        Assert.Contains("already exists without Darling's `darling-managed` marker", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("Darling did not create it", readme, StringComparison.Ordinal);
    }

    /// <summary>The script and the README both say psql 10 or later, and the script no longer says a bare psql reads an
    /// unset <c>ON_ERROR_STOP</c> as off: that is true only from psql 10.</summary>
    [Fact]
    public void TheScriptAndTheReadme_SayPsql10OrLater()
    {
        var sql = Script();
        var readme = RepoFile.ReadRepoFile("Darling", "README.md");

        Assert.Contains("Needs psql 10 or later", sql, StringComparison.Ordinal);
        Assert.Contains("this script needs psql 10 or later", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("psql reads an unset ON_ERROR_STOP as off", sql, StringComparison.Ordinal);
        Assert.Contains("Use **psql 10 or later**", readme, StringComparison.Ordinal);
    }

    /// <summary>The README's psql command line must not set ON_ERROR_STOP for the whole run: that would stop an owner
    /// who is not a superuser at the <c>temp_file_limit</c> line, before any grant.</summary>
    [Fact]
    public void TheReadmePsqlCommandLine_SetsNoGlobalOnErrorStop()
    {
        var readme = RepoFile.ReadRepoFile("Darling", "README.md");

        var commandLine = Regex.Match(readme, @"(?m)^psql .*provision-roles\.sql.*$");
        Assert.True(commandLine.Success, "the README no longer shows the psql command line that runs provision-roles.sql.");
        Assert.DoesNotContain("ON_ERROR_STOP", commandLine.Value, StringComparison.Ordinal);
    }
}
