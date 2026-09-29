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
using PerformanceMonitor.Darling.Service;
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

        var firstPasswordReset = Regex.Match(sql, @"(?m)^ALTER ROLE \w+\s+LOGIN PASSWORD");
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

    /// <summary>The DO block that holds the role-collision guard: from the <c>DO $$</c> before its first role
    /// <c>RAISE</c> to the <c>END $$;</c> after its last one.</summary>
    private static string GuardBlock(string sql)
    {
        var firstRaise = sql.IndexOf(GuardRaise, StringComparison.Ordinal);
        var lastRaise = sql.LastIndexOf(GuardRaise, StringComparison.Ordinal);
        Assert.True(firstRaise >= 0 && lastRaise >= firstRaise, "the role-collision guard's RAISE EXCEPTION lines moved or were renamed - update this pin with them.");
        var start = sql.LastIndexOf("DO $$", firstRaise, StringComparison.Ordinal);
        var end = sql.IndexOf("END $$;", lastRaise, StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "could not find the DO block that holds the role-collision guard.");
        return sql[start..(end + "END $$;".Length)];
    }

    /// <summary>The script's lines that are not <c>--</c> comments, so a placeholder or a keyword quoted in prose is
    /// never counted as SQL.</summary>
    private static string[] CodeLines(string sql) =>
        sql.Split('\n').Where(line => !line.TrimStart().StartsWith("--", StringComparison.Ordinal)).ToArray();

    /// <summary>The guard creates each role WITHOUT a password. When it stops the run, the server writes the failed
    /// statement, its whole text, to the log (<c>log_min_error_statement</c> defaults to error), so a guard that carried
    /// the three new passwords put all of them in the log, and the documented remedy re-runs with the same ones. The
    /// three <c>ALTER ROLE</c> lines after the guard set the passwords, so each placeholder is in exactly one
    /// statement, after the guard.</summary>
    [Fact]
    public void TheGuard_CreatesTheRolesWithoutAPassword_SoAGuardThatStopsTheRunLogsNone()
    {
        var sql = Script();
        var guard = GuardBlock(sql);

        foreach (var role in new[] { "admin", "viewer", "mcp" })
        {
            Assert.Contains($"CREATE ROLE {role} LOGIN NOSUPERUSER;", guard, StringComparison.Ordinal);
            Assert.Contains($"COMMENT ON ROLE {role} IS 'darling-managed';", guard, StringComparison.Ordinal);
        }

        Assert.DoesNotMatch(@"(?i)\bPASSWORD\s+'", guard);
        Assert.DoesNotContain("CHANGE_ME_", guard, StringComparison.Ordinal);

        var code = CodeLines(sql);
        var guardEnd = sql.IndexOf(guard, StringComparison.Ordinal) + guard.Length;
        foreach (var (role, placeholder) in new[] { ("admin", "CHANGE_ME_ADMIN_PASSWORD"), ("viewer", "CHANGE_ME_VIEWER_PASSWORD"), ("mcp", "CHANGE_ME_MCP_PASSWORD") })
        {
            Assert.Single(code, line => line.Contains(placeholder, StringComparison.Ordinal));
            var statement = Regex.Match(sql, $@"(?m)^ALTER ROLE {role}\s+LOGIN PASSWORD '{placeholder}';");
            Assert.True(statement.Success && statement.Index > guardEnd, $"the {role} password must be set by an ALTER ROLE that comes after the guard.");
        }
    }

    /// <summary>The guard's FIRST statement stops the run while <c>pg_stat_statements.track_utility</c> is on. Step 0
    /// turns it off for the session, but the SET is superuser-only once the module is loaded, so for an owner without
    /// a superuser or a SET grant it fails, and the module would then record every password statement verbatim, where
    /// any <c>pg_read_all_stats</c> member reads it. The guard runs with the stop on, so this ends the run before any
    /// role is created or any password is sent. The message names the three ways out.</summary>
    [Fact]
    public void TheGuard_StopsFirst_WhileUtilityTrackingIsOn_AndNamesTheThreeWaysOut()
    {
        var sql = Script();
        var guard = GuardBlock(sql);

        var statements = guard[(guard.IndexOf("BEGIN", StringComparison.Ordinal) + "BEGIN".Length)..]
            .Split('\n').Select(line => line.Trim()).Where(line => line.Length > 0 && !line.StartsWith("--", StringComparison.Ordinal)).ToArray();
        Assert.Equal("IF current_setting('pg_stat_statements.track_utility', true) = 'on' THEN", statements[0]);
        Assert.StartsWith("RAISE EXCEPTION 'pg_stat_statements.track_utility is on", statements[1], StringComparison.Ordinal);
        Assert.Equal("END IF;", statements[2]);

        var message = statements[1];
        Assert.Contains("GRANT SET ON PARAMETER pg_stat_statements.track_utility TO <owner>", message, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL 15", message, StringComparison.Ordinal);
        Assert.Contains("postgresql.conf", message, StringComparison.Ordinal);
        Assert.Contains("superuser", message, StringComparison.Ordinal);

        Assert.True(guard.IndexOf("current_setting('pg_stat_statements.track_utility'", StringComparison.Ordinal) < guard.IndexOf("CREATE ROLE", StringComparison.Ordinal),
            "the utility-tracking check must come before the guard creates any role.");
    }

    /// <summary>The password lines name <c>LOGIN</c> and the password only. <c>NOSUPERUSER</c> in an <c>ALTER ROLE</c>
    /// needs a superuser on every PostgreSQL version, so with it every one of the three lines failed for an owner
    /// without one: a re-run never rotated a password, and psql still exited 0. <c>NOSUPERUSER</c> is re-asserted
    /// in a block that runs only when the role running the script is a superuser.</summary>
    [Fact]
    public void ThePasswordLines_NameNoSuperuserAttribute_AndNosuperuserIsReassertedOnlyForASuperuser()
    {
        var sql = Script();

        var passwordLines = Regex.Matches(sql, @"(?m)^ALTER ROLE (?<role>\w+)\s+(?<rest>.*\bPASSWORD\b.*)$");
        Assert.Equal(3, passwordLines.Count);
        Assert.All(passwordLines.Cast<Match>(), line =>
            Assert.Matches(@"^LOGIN PASSWORD '[^']+';\s*$", line.Groups["rest"].Value));
        Assert.DoesNotMatch(@"(?m)^ALTER ROLE \w+\s+.*NOSUPERUSER", sql);

        var reassert = sql.IndexOf("ALTER ROLE %I NOSUPERUSER", StringComparison.Ordinal);
        Assert.True(reassert >= 0, "the superuser re-assertion of NOSUPERUSER is gone.");
        var branch = sql.LastIndexOf("IF as_superuser THEN", reassert, StringComparison.Ordinal);
        Assert.True(branch >= 0 && reassert - branch < 200, "NOSUPERUSER must be re-asserted only under 'IF as_superuser THEN'.");
        Assert.Single(Regex.Matches(sql, @"ALTER ROLE %I NOSUPERUSER"));
        Assert.Contains("as_superuser boolean := (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)", sql, StringComparison.Ordinal);
    }

    /// <summary>The marker is a public string, so it does not show who made a role. From PostgreSQL 16 a role with
    /// CREATEROLE holds ADMIN OPTION on every role it creates: one that pre-creates <c>viewer</c> with the marker
    /// passed the guard, kept the option after the operator's run re-keyed and granted the role, and could reset its
    /// password. The guard now also refuses a marked role while a role that is not a superuser, not the role running
    /// the script and not the database owner holds ADMIN OPTION on it. A plain membership does not count.</summary>
    [Fact]
    public void TheGuard_RefusesAMarkedRole_ThatAnotherNonSuperuserHoldsAdminOptionOn()
    {
        var guard = GuardBlock(Script());

        Assert.Contains("FROM pg_auth_members AS membership", guard, StringComparison.Ordinal);
        Assert.Contains("granted.rolname IN ('admin', 'viewer', 'mcp')", guard, StringComparison.Ordinal);
        Assert.Contains("AND membership.admin_option", guard, StringComparison.Ordinal);
        Assert.Contains("AND NOT holder.rolsuper", guard, StringComparison.Ordinal);
        Assert.Contains("AND holder.rolname <> current_user", guard, StringComparison.Ordinal);
        Assert.Contains("AND holder.oid <> (SELECT datdba FROM pg_database WHERE datname = current_database())", guard, StringComparison.Ordinal);
        Assert.Contains("holds ADMIN OPTION on it", guard, StringComparison.Ordinal);

        /* It runs after the three marker branches, inside the block, so the refusal rolls back a role this run created. */
        Assert.True(guard.IndexOf("membership.admin_option", StringComparison.Ordinal) > guard.LastIndexOf("IS DISTINCT FROM 'darling-managed'", StringComparison.Ordinal),
            "the ADMIN OPTION check must come after the marker checks.");
    }

    /// <summary>An adopted role (it already carries the marker) keeps CREATEROLE, CREATEDB, REPLICATION, BYPASSRLS and
    /// its memberships in other roles, all of which managed provisioning takes back. The script now mirrors that: for
    /// a superuser the same attribute list and the same membership revoke, and for any other owner what its own
    /// privileges allow, with a WARNING for what it cannot remove.</summary>
    [Fact]
    public void TheScript_TakesBackWhatManagedProvisioningTakesBack()
    {
        var sql = Script();
        var managed = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp, 15);

        /* The twin's own statements: the attribute list, the membership revoke, its grantor clause and its CASCADE. */
        const string Attributes = "NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS";
        Assert.Contains($"LOGIN {Attributes}", managed, StringComparison.Ordinal);
        Assert.Contains("format('REVOKE %I FROM %I', membership.granted, membership.member)", managed, StringComparison.Ordinal);
        Assert.Contains("format(' GRANTED BY %I', membership.grantor)", managed, StringComparison.Ordinal);
        Assert.Contains("|| ' CASCADE'", managed, StringComparison.Ordinal);

        Assert.Contains($"EXECUTE format('ALTER ROLE %I {Attributes}', item.rolname);", sql, StringComparison.Ordinal);
        Assert.Contains("format('REVOKE %I FROM %I', item.granted, item.member)", sql, StringComparison.Ordinal);
        Assert.Contains("format(' GRANTED BY %I', item.grantor)", sql, StringComparison.Ordinal);
        Assert.Contains("|| ' CASCADE'", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE member.rolname IN ('admin', 'viewer', 'mcp')", sql, StringComparison.Ordinal);

        /* Not a superuser: each attribute the role has is switched off on its own, and a refusal is a WARNING, not an error. */
        foreach (var attribute in new[] { "SUPERUSER", "CREATEROLE", "CREATEDB", "REPLICATION", "BYPASSRLS" })
        {
            Assert.Contains($"THEN '{attribute}' END", sql, StringComparison.Ordinal);
        }

        Assert.Contains("EXCEPTION WHEN insufficient_privilege THEN", sql, StringComparison.Ordinal);
        Assert.Contains("RAISE WARNING 'Role \"%\" still has the % attribute", sql, StringComparison.Ordinal);
        Assert.Contains("RAISE WARNING 'Role \"%\" is still a member of \"%\"", sql, StringComparison.Ordinal);

        /* After the passwords, so the run has re-keyed the roles before it takes anything back. */
        Assert.True(sql.IndexOf("ALTER ROLE %I NOSUPERUSER", StringComparison.Ordinal) > sql.IndexOf("ALTER ROLE mcp    LOGIN PASSWORD", StringComparison.Ordinal));
    }

    /// <summary>The README says what a non-superuser owner gets, and the script's step 0 and guard match it: the run
    /// stops at the guard while utility tracking is on and the owner cannot switch it off, and on PostgreSQL 16 and
    /// later an owner needs ADMIN OPTION on each role, which it holds only for roles it created itself.</summary>
    [Fact]
    public void TheReadme_DescribesTheNonSuperuserOwnersLimits()
    {
        var readme = RepoFile.ReadRepoFile("Darling", "README.md");
        var sql = Script();

        Assert.Contains("GRANT SET ON PARAMETER pg_stat_statements.track_utility", readme, StringComparison.Ordinal);
        Assert.Contains("`ADMIN OPTION` on each of the three roles", readme, StringComparison.Ordinal);
        Assert.Contains("only for a role it created itself", readme, StringComparison.Ordinal);
        Assert.Contains("stops at the guard", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("and still gets the roles and every grant it can make", readme, StringComparison.Ordinal);

        Assert.Contains("GRANT SET ON PARAMETER", sql, StringComparison.Ordinal);
        Assert.Contains("AN OWNER WHO IS NOT A SUPERUSER", sql, StringComparison.Ordinal);
    }
}
