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
[Trait("Stage", "Guard")]
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
        /* Two pairs: the guard's, and the one around the attribute and membership blocks after the passwords. */
        Assert.Equal(4, Regex.Matches(sql, @"(?m)^\\set ON_ERROR_STOP\b").Count);

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
        /* The four unchanged-placeholder checks run inside the stop on purpose: each one has to end the run. Nothing else may. */
        var between = Regex.Replace(
            sql[afterGuard..restore.Index],
            @"(?s)DO \$darling_placeholder_check\$\s*BEGIN\s*IF 'CHANGE_ME_\w+' = 'CHANGE_ME_' \|\| '\w+' THEN.*?END \$darling_placeholder_check\$;", string.Empty);
        Assert.True(
            string.IsNullOrWhiteSpace(string.Join("\n", between.Split('\n').Where(line => !line.TrimStart().StartsWith("--", StringComparison.Ordinal)))),
            "the caller's ON_ERROR_STOP must be put back right after the guard and the placeholder checks: nothing else may run between the guard's END and the restore.");

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
    /// lines that start with a backslash - the five <c>\set</c> lines - and nothing else: comments that mention a
    /// backslash command further along a line, and lines that only start with white space, stay.</summary>
    [Fact]
    public void TheLiveScriptTest_DropsOnlyTheLinesThatStartWithABackslash()
    {
        var sql = Script();
        var stripped = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(sql);

        var before = sql.Split('\n');
        var after = stripped.Split('\n');
        Assert.Equal(5, before.Length - after.Length);
        Assert.Equal(before.Where(line => !line.StartsWith('\\')), after);
        Assert.DoesNotContain(after, line => line.StartsWith('\\'));

        Assert.Equal(
            "SELECT 1;\r\n-- see \\password\r\n  \\set kept\r\nSELECT 2;",
            ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands("SELECT 1;\r\n\\set ON_ERROR_STOP on\r\n-- see \\password\r\n  \\set kept\r\nSELECT 2;"));
    }

    /// <summary>Both places that show the psql command line run it with <c>-X</c>, so the operator's own psqlrc stays
    /// out of the run: an <c>\set AUTOCOMMIT off</c> in it leaves steps 3 to 5 uncommitted while psql
    /// still exits 0.</summary>
    [Fact]
    public void TheShownPsqlCommandLines_SkipThePsqlrc()
    {
        var readmeLine = Regex.Match(RepoFile.ReadRepoFile("docs", "darling", "security.md"), @"(?m)^psql .*provision-roles\.sql.*$"); // #6416: moved from Darling/README.md
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
        var readme = RepoFile.ReadRepoFile("docs", "darling", "security.md"); // #6416: moved from Darling/README.md

        Assert.Contains("already exists without Darling's `darling-managed` marker", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("Darling did not create it", readme, StringComparison.Ordinal);
    }

    /// <summary>The script and the README both say psql 10 or later, and the script no longer says a bare psql reads an
    /// unset <c>ON_ERROR_STOP</c> as off: that is true only from psql 10.</summary>
    [Fact]
    public void TheScriptAndTheReadme_SayPsql10OrLater()
    {
        var sql = Script();
        var readme = RepoFile.ReadRepoFile("docs", "darling", "security.md"); // #6416: moved from Darling/README.md

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
        var readme = RepoFile.ReadRepoFile("docs", "darling", "security.md"); // #6416: moved from Darling/README.md

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
    /// three <c>ALTER ROLE</c> lines after the guard set the passwords, so each placeholder is in one
    /// ALTER ROLE statement and in one check statement of its own, both after the guard.</summary>
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
            /* Two lines hold each placeholder, both after the guard: its unchanged-placeholder check, then its ALTER ROLE. */
            Assert.Equal(2, code.Count(line => line.Contains(placeholder, StringComparison.Ordinal)));
            Assert.True(sql.IndexOf($"IF '{placeholder}' = ", StringComparison.Ordinal) > guardEnd, $"the {role} placeholder check must come after the guard.");
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
        Assert.Equal(4, passwordLines.Count);
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
        Assert.Contains("granted.rolname IN ('admin', 'viewer', 'web', 'mcp')", guard, StringComparison.Ordinal);
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
        Assert.Contains("WHERE member.rolname IN ('admin', 'viewer', 'web', 'mcp')", sql, StringComparison.Ordinal);

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
        var readme = RepoFile.ReadRepoFile("docs", "darling", "security.md"); // #6416: moved from Darling/README.md
        var sql = Script();

        Assert.Contains("GRANT SET ON PARAMETER pg_stat_statements.track_utility", readme, StringComparison.Ordinal);
        Assert.Contains("`ADMIN OPTION` on each of the four roles", readme, StringComparison.Ordinal);
        Assert.Contains("only for a role it created itself", readme, StringComparison.Ordinal);
        Assert.Contains("stops at the guard", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("and still gets the roles and every grant it can make", readme, StringComparison.Ordinal);

        Assert.Contains("GRANT SET ON PARAMETER", sql, StringComparison.Ordinal);
        Assert.Contains("AN OWNER WHO IS NOT A SUPERUSER", sql, StringComparison.Ordinal);
    }

    private const string NonSuperuserBranch = "IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN";

    /// <summary>The part of the guard that only a role that is not a superuser reaches, from its opening <c>IF</c> to
    /// the end of the guard.</summary>
    private static string NonSuperuserChecks(string sql)
    {
        var guard = GuardBlock(sql);
        Assert.Single(Regex.Matches(guard, Regex.Escape(NonSuperuserBranch)));
        return guard[guard.IndexOf(NonSuperuserBranch, StringComparison.Ordinal)..];
    }

    /// <summary>The <c>RAISE EXCEPTION</c> line of <paramref name="checks"/> whose text starts with
    /// <paramref name="start"/>.</summary>
    private static string RaiseLine(string checks, string start)
    {
        var line = checks.Split('\n').Select(l => l.Trim()).SingleOrDefault(l => l.StartsWith($"RAISE EXCEPTION '{start}", StringComparison.Ordinal));
        Assert.True(line is not null, $"the guard has no RAISE EXCEPTION that starts '{start}'.");
        return line!;
    }

    /// <summary>Every stop the guard adds for a role that is not a superuser says what happened, that nothing was
    /// changed, and why it stopped before the password lines, and it never holds a password.</summary>
    private static void AssertStopExplainsItself(string line)
    {
        Assert.Contains("The script created and changed nothing.", line, StringComparison.Ordinal);
        Assert.Contains("a failed ALTER ROLE line does not write a password to the server log", line, StringComparison.Ordinal);
        Assert.DoesNotContain("CHANGE_ME_", line, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"(?i)\bPASSWORD\s+'", line);
        Assert.Contains("as a superuser", line, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>A role that is not a superuser and has no <c>CREATEROLE</c> cannot change another role at all, so each
    /// <c>ALTER ROLE ... LOGIN PASSWORD</c> line would fail, and the server would write the statement, password included,
    /// to its log. The guard stops such a run first. The branch is one that a superuser never enters, it comes after the
    /// marker and ADMIN OPTION checks, and it looks at the running role's own attribute, as PostgreSQL does.</summary>
    [Fact]
    public void TheGuard_StopsARunnerWithoutCreaterole_BeforeAnyPasswordIsSent()
    {
        var sql = Script();
        var guard = GuardBlock(sql);
        var checks = NonSuperuserChecks(sql);

        Assert.True(guard.IndexOf(NonSuperuserBranch, StringComparison.Ordinal) > guard.IndexOf("membership.admin_option", StringComparison.Ordinal),
            "the checks for a role that is not a superuser must come after the check for another login that administers a role.");
        Assert.True(guard.IndexOf(NonSuperuserBranch, StringComparison.Ordinal) > guard.LastIndexOf("IS DISTINCT FROM 'darling-managed'", StringComparison.Ordinal),
            "the checks for a role that is not a superuser must come after the marker checks, so an unrelated role is never offered a GRANT.");

        Assert.StartsWith("IF NOT (SELECT rolcreaterole FROM pg_roles WHERE rolname = current_user) THEN", checks[(NonSuperuserBranch.Length)..].TrimStart(), StringComparison.Ordinal);

        var line = RaiseLine(checks, "The role running this script, %, is not a superuser and does not have CREATEROLE");
        AssertStopExplainsItself(line);
        Assert.Contains("Run this script again as a superuser, or as a role that has CREATEROLE.", line, StringComparison.Ordinal);
        Assert.True(sql.IndexOf(NonSuperuserBranch, StringComparison.Ordinal) < Regex.Match(sql, @"(?m)^ALTER ROLE \w+\s+LOGIN PASSWORD").Index,
            "the stop must come before the first ALTER ROLE ... PASSWORD.");
    }

    /// <summary>Only a superuser can change a superuser role, on every version, so a marked role that has
    /// <c>SUPERUSER</c> stops the run for a role that is not a superuser. The message names the roles.</summary>
    [Fact]
    public void TheGuard_StopsARunnerThatIsNotASuperuser_WhenOneOfTheRolesIsASuperuser()
    {
        var checks = NonSuperuserChecks(Script());

        Assert.Matches(@"(?s)SELECT string_agg\(quote_ident\(rolname\), ', ' ORDER BY rolname\) INTO unchangeable\s+FROM pg_roles\s+WHERE rolname IN \('admin', 'viewer', 'web', 'mcp'\) AND rolsuper;", checks);

        var line = RaiseLine(checks, "These roles are superusers: %");
        AssertStopExplainsItself(line);
        Assert.Contains("is not a superuser, so it cannot change them", line, StringComparison.Ordinal);
        Assert.True(checks.IndexOf("AND rolsuper;", StringComparison.Ordinal) < checks.IndexOf("server_version_num", StringComparison.Ordinal),
            "the superuser check applies to every version, so it comes before the version branch.");
    }

    /// <summary>From PostgreSQL 16 a role needs ADMIN OPTION on a role to change its password. A role that a superuser
    /// made carries none for the owner, so the owner's <c>ALTER ROLE</c> lines failed and logged the passwords. The guard
    /// asks <c>pg_has_role</c>, which calls the function <c>ALTER ROLE</c> uses, and names the remedy: the GRANT
    /// statements for a superuser to run, or a superuser's own run. A role this run created passes, because its creator
    /// holds the option.</summary>
    [Fact]
    public void TheGuard_FromPostgres16_StopsARunnerThatLacksAdminOption_AndNamesTheGrant()
    {
        var checks = NonSuperuserChecks(Script());

        var branch = checks[checks.IndexOf("IF current_setting('server_version_num')::int >= 160000 THEN", StringComparison.Ordinal)..];
        Assert.StartsWith("IF current_setting('server_version_num')::int >= 160000 THEN", branch, StringComparison.Ordinal);
        Assert.Contains("WHERE rolname IN ('admin', 'viewer', 'web', 'mcp') AND NOT pg_has_role(current_user, oid, 'MEMBER WITH ADMIN OPTION');", branch, StringComparison.Ordinal);
        Assert.Contains("string_agg(format('GRANT %I TO %I WITH ADMIN OPTION;', rolname, current_user), ' ' ORDER BY rolname)", branch, StringComparison.Ordinal);

        var line = RaiseLine(checks, "The role running this script, %, does not hold ADMIN OPTION on these roles: %");
        AssertStopExplainsItself(line);
        Assert.Contains("Run this script as a superuser. Or, as a superuser, run these statements and then run this script again: %", line, StringComparison.Ordinal);
        Assert.Contains("From PostgreSQL 16", line, StringComparison.Ordinal);
        var newer = checks.IndexOf("IF current_setting('server_version_num')::int >= 160000 THEN", StringComparison.Ordinal);
        var older = checks.IndexOf("ELSE", newer, StringComparison.Ordinal);
        var stop = checks.IndexOf(line, StringComparison.Ordinal);
        Assert.True(newer >= 0 && stop > newer && stop < older, "the ADMIN OPTION stop belongs to the PostgreSQL 16 and later branch.");
    }

    /// <summary>Before PostgreSQL 16 a role with <c>CREATEROLE</c> cannot change a role that has <c>REPLICATION</c> (the
    /// PostgreSQL 15 ALTER ROLE page: only for non-superuser and non-replication roles), so the guard stops on it. From 16
    /// the same runner can, so the check belongs to the older branch alone.</summary>
    [Fact]
    public void TheGuard_Before16_StopsARunnerThatIsNotASuperuser_WhenOneOfTheRolesHasReplication()
    {
        var checks = NonSuperuserChecks(Script());

        var older = checks[checks.IndexOf("ELSE", StringComparison.Ordinal)..];
        Assert.Matches(@"(?s)^ELSE\s+SELECT string_agg\(quote_ident\(rolname\), ', ' ORDER BY rolname\) INTO unchangeable\s+FROM pg_roles\s+WHERE rolname IN \('admin', 'viewer', 'web', 'mcp'\) AND rolreplication;", older);
        Assert.DoesNotContain("rolreplication", checks[..checks.IndexOf("ELSE", StringComparison.Ordinal)], StringComparison.Ordinal);

        var line = RaiseLine(checks, "These roles have the REPLICATION attribute: %");
        AssertStopExplainsItself(line);
        Assert.Contains("Before PostgreSQL 16 only a superuser can change a role that has it", line, StringComparison.Ordinal);
    }

    /// <summary>The ADMIN OPTION refusal (a marked role that another login administers) names the step that destroys
    /// nothing: run the script as that login, which the check leaves out. It also says that only a superuser can revoke
    /// the option from a login that created the role, since PostgreSQL records that grant as made by the bootstrap
    /// superuser.</summary>
    [Fact]
    public void TheAdminOptionRefusal_NamesRunningTheScriptAsThatLogin_BeforeRevokingOrDropping()
    {
        var sql = Script();
        var line = Regex.Match(GuardBlock(sql), @"(?m)^\s*RAISE EXCEPTION 'Role ""%"" carries the .*$").Value;
        Assert.NotEmpty(line);

        Assert.Contains("To go on, run this script as that login, which this check leaves out", line, StringComparison.Ordinal);
        Assert.Contains("Or, as a superuser, revoke that ADMIN OPTION (a login that created the role cannot revoke it itself), or drop the role.", line, StringComparison.Ordinal);
        Assert.True(line.IndexOf("run this script as that login", StringComparison.Ordinal) < line.IndexOf("revoke that ADMIN OPTION", StringComparison.Ordinal));

        var readme = RepoFile.ReadRepoFile("docs", "darling", "security.md"); // #6416: moved from Darling/README.md
        Assert.Contains("To continue, run the script as that login, which the check leaves out.", readme, StringComparison.Ordinal);
        Assert.Contains("A login that created the role cannot revoke the option itself", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("To continue, revoke that option or drop the role", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("Revoke that option or drop the role, then", sql, StringComparison.Ordinal);
    }

    /// <summary>"Owner can switch off REPLICATION or BYPASSRLS itself" is true from PostgreSQL 16 only: before 16 only a
    /// superuser can change either. The script's WARNING, its header and the README all carry that qualifier.</summary>
    [Fact]
    public void TheReplicationAndBypassrlsClaims_CarryThePostgres16Qualifier_InTheWarningTheHeaderAndTheReadme()
    {
        var sql = Script();
        var readme = RepoFile.ReadRepoFile("docs", "darling", "security.md"); // #6416: moved from Darling/README.md

        Assert.Contains("switching it off needs a superuser, or, from PostgreSQL 16, an owner that holds % itself.", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("needs a superuser, or an owner that holds % itself", sql, StringComparison.Ordinal);
        Assert.Contains("nor is REPLICATION, BYPASSRLS or CREATEDB unless the owner", sql, StringComparison.Ordinal);
        Assert.Contains("(PostgreSQL 16 and later; before 16 only a superuser can change REPLICATION or", sql, StringComparison.Ordinal);

        Assert.Contains("From PostgreSQL 16, it can switch off `REPLICATION`, `BYPASSRLS` and `CREATEDB` only if it holds that attribute itself", readme, StringComparison.Ordinal);
        Assert.Contains("Before PostgreSQL 16, only a superuser can switch off `REPLICATION` or `BYPASSRLS`.", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("The owner cannot switch off `SUPERUSER`. It can switch off `REPLICATION`", readme, StringComparison.Ordinal);
    }

    /// <summary>A password literal reaches the server log on a successful run too, when <c>log_statement</c> is
    /// <c>ddl</c> or <c>all</c> or <c>log_min_duration_statement</c> is 0 or more (the sampling settings do the same). Step 0
    /// turns them off for the session, in a block that catches the refusal for a role that may not (and the error for a
    /// setting an older server does not have), so a run by an owner that is not a superuser prints nothing. It is
    /// session-level (<c>is_local</c> false), and it comes before the guard and every password line. The README names both
    /// settings, and the script's header names the second one.</summary>
    [Fact]
    public void StepZero_TurnsOffStatementLoggingForTheSession_QuietlyForARoleThatMayNot()
    {
        var sql = Script();

        var utilityOff = sql.IndexOf("\nSET pg_stat_statements.track_utility = off;", StringComparison.Ordinal);
        Assert.True(utilityOff >= 0);
        var block = sql[sql.IndexOf("DO $$", utilityOff, StringComparison.Ordinal)..];
        block = block[..(block.IndexOf("END $$;", StringComparison.Ordinal) + "END $$;".Length)];

        foreach (var (name, value) in new[]
        {
            ("log_statement", "none"), ("log_min_duration_statement", "-1"), ("log_min_duration_sample", "-1"), ("log_transaction_sample_rate", "0"),
            ("log_min_error_statement", "panic"), ("debug_print_parse", "off"), ("debug_print_rewritten", "off"), ("debug_print_plan", "off"),
            ("pgaudit.log_statement", "off"),
        })
        {
            Assert.Contains($"('{name}', '{value}')", block, StringComparison.Ordinal);
        }

        Assert.Contains("PERFORM set_config(log_setting.name, log_setting.value, false);", block, StringComparison.Ordinal);
        Assert.Contains("EXCEPTION WHEN insufficient_privilege OR undefined_object THEN", block, StringComparison.Ordinal);
        Assert.DoesNotContain("RAISE", block, StringComparison.Ordinal);
        Assert.DoesNotContain("password", block, StringComparison.OrdinalIgnoreCase);

        var guardStart = sql.IndexOf(GuardBlock(sql), StringComparison.Ordinal);
        Assert.True(sql.IndexOf(block, StringComparison.Ordinal) < guardStart, "step 0's logging block must come before the guard.");
        Assert.True(sql.IndexOf(block, StringComparison.Ordinal) < Regex.Match(sql, @"(?m)^ALTER ROLE \w+\s+LOGIN PASSWORD").Index);

        var header = sql[..sql.IndexOf("-- 0. Keep this script's own role DDL", StringComparison.Ordinal)];
        Assert.Contains("log_min_duration_statement set to 0 or more", header, StringComparison.Ordinal);
        Assert.Contains("log_statement set to ddl, mod or all", header, StringComparison.Ordinal);

        var readme = RepoFile.ReadRepoFile("docs", "darling", "security.md"); // #6416: moved from Darling/README.md
        Assert.Contains("Two settings put the passwords in the server log even when the run succeeds: `log_statement` at `ddl`, `mod` or `all`, and `log_min_duration_statement` at `0` or more.", readme, StringComparison.Ordinal);
        Assert.Contains("Step 0 turns those settings off for the session.", readme, StringComparison.Ordinal);
        Assert.Contains("GRANT SET ON PARAMETER <setting> TO <owner>", readme, StringComparison.Ordinal);
        Assert.Contains("A run that succeeds also writes all four passwords to the log when `log_statement` is `ddl`, `mod` or `all`, or when `log_min_duration_statement` is `0` or more.", readme, StringComparison.Ordinal);
        Assert.Contains("`log_min_error_statement` to `panic`", readme, StringComparison.Ordinal);
    }

    /// <summary>The README describes the new stops in the list of what an owner that is not a superuser can do.</summary>
    [Fact]
    public void TheReadme_SaysTheGuardStopsAnOwnerThatCannotChangeTheRoles_AndNamesTheGrant()
    {
        var readme = RepoFile.ReadRepoFile("docs", "darling", "security.md"); // #6416: moved from Darling/README.md

        Assert.Contains("The guard checks this before it sends a password.", readme, StringComparison.Ordinal);
        Assert.Contains("If the owner cannot change one of the four roles, the run stops at the guard with exit code 3. The message names the roles and the `GRANT` statements to run.", readme, StringComparison.Ordinal);
        Assert.Contains("The same stop happens when one of the roles is a superuser. Before PostgreSQL 16, it also happens when one of the roles has `REPLICATION`.", readme, StringComparison.Ordinal);
        Assert.Contains("A superuser is never stopped by these checks.", readme, StringComparison.Ordinal);
    }

    /// <summary>Step 0 also switches off <c>pg_stat_monitor.pgsm_track_utility</c> (any role may), as it does for
    /// <c>pg_stat_statements</c>, and the guard stops the run when it is still on. It sits after the
    /// <c>pg_stat_statements</c> check and before the guard creates a role. The README says so.</summary>
    [Fact]
    public void TheGuard_StopsWhilePgStatMonitorTrackingIsOn_AndStepZeroSwitchesItOff()
    {
        var sql = Script();
        var guard = GuardBlock(sql);

        var setOff = sql.IndexOf("\nSET pg_stat_monitor.pgsm_track_utility = off;", StringComparison.Ordinal);
        Assert.True(setOff > sql.IndexOf("\nSET pg_stat_statements.track_utility = off;", StringComparison.Ordinal), "step 0 must switch pg_stat_monitor's tracking off after pg_stat_statements'.");
        Assert.True(setOff < sql.IndexOf(guard, StringComparison.Ordinal), "step 0's SET must come before the guard.");

        var check = guard.IndexOf("current_setting('pg_stat_monitor.pgsm_track_utility', true)", StringComparison.Ordinal);
        Assert.True(check > guard.IndexOf("current_setting('pg_stat_statements.track_utility'", StringComparison.Ordinal));
        Assert.True(check < guard.IndexOf("CREATE ROLE", StringComparison.Ordinal), "the pg_stat_monitor check must come before the guard creates any role.");
        Assert.Contains("RAISE EXCEPTION 'pg_stat_monitor.pgsm_track_utility is on for this session.", guard, StringComparison.Ordinal);

        var readme = RepoFile.ReadRepoFile("docs", "darling", "security.md"); // #6416: moved from Darling/README.md
        Assert.Contains("`pg_stat_monitor.pgsm_track_utility = off`", readme, StringComparison.Ordinal);
    }

    /// <summary>When step 0 could not turn the statement logging off (the role may not change the setting), the script
    /// says so with a WARNING before the guard and before any password is sent.</summary>
    [Fact]
    public void AfterStepZero_AWarningNamesStatementLoggingThatIsStillOn_BeforeTheGuard()
    {
        var sql = Script();

        var warning = sql.IndexOf("RAISE WARNING 'The server log records the statements of this run", StringComparison.Ordinal);
        Assert.True(warning > sql.IndexOf("PERFORM set_config(log_setting.name", StringComparison.Ordinal));
        Assert.True(warning < sql.IndexOf(GuardBlock(sql), StringComparison.Ordinal));
        Assert.True(warning < Regex.Match(sql, @"(?m)^ALTER ROLE \w+\s+LOGIN PASSWORD").Index);

        var block = sql[sql.LastIndexOf("DO $$", warning, StringComparison.Ordinal)..warning];
        Assert.Contains("THEN setting IN ('ddl', 'mod', 'all')", block, StringComparison.Ordinal);
        /* The numeric cast sits in a CASE arm, so it is only evaluated for the row it belongs to. */
        Assert.Contains("WHEN 'log_min_duration_statement' THEN setting::bigint >= 0", block, StringComparison.Ordinal);
        /* The sampling settings and pgaudit's role class write the same lines, so each is named too, with its cast in its arm. */
        Assert.Contains("WHEN 'log_transaction_sample_rate' THEN setting::numeric > 0", block, StringComparison.Ordinal);
        Assert.Contains("WHEN 'log_min_duration_sample' THEN setting::bigint >= 0", block, StringComparison.Ordinal);
        Assert.Contains("sample.setting::numeric FROM pg_settings AS sample WHERE sample.name = 'log_statement_sample_rate') > 0", block, StringComparison.Ordinal);
        Assert.Contains("WHEN 'pgaudit.log' THEN (SELECT statement.setting FROM pg_settings AS statement WHERE statement.name = 'pgaudit.log_statement') = 'on'", block, StringComparison.Ordinal);
        Assert.Contains("&& ARRAY['role', 'all']", block, StringComparison.Ordinal);
        Assert.Contains("NOT ('-role' = ANY (", block, StringComparison.Ordinal);
    }

    /// <summary>A role that is not a superuser stops at the guard, before any password, when one of the three roles is
    /// a member of a role that would outrank the carve (the owner, a superuser or BYPASSRLS role, a predefined role
    /// that reads or writes everything or the server's files) and the owner cannot revoke the membership. The
    /// revoke block after the passwords can only warn about it.</summary>
    [Fact]
    public void TheGuard_StopsARunner_ThatCannotRevokeAMembershipThatVoidsTheCarve_BeforeAnyPasswordIsSent()
    {
        var sql = Script();
        var checks = NonSuperuserChecks(sql);

        var membership = checks.IndexOf("FROM pg_auth_members AS membership", StringComparison.Ordinal);
        Assert.True(membership > checks.IndexOf("rolreplication", StringComparison.Ordinal), "the membership check follows the checks for the roles themselves.");
        var query = checks[membership..];
        Assert.Contains("WHERE member.rolname IN ('admin', 'viewer', 'web', 'mcp')", query, StringComparison.Ordinal);
        Assert.Contains("voiding.rolsuper", query, StringComparison.Ordinal);
        Assert.Contains("voiding.rolbypassrls", query, StringComparison.Ordinal);
        Assert.Contains("rolname = current_user", query, StringComparison.Ordinal);
        foreach (var predefined in new[] { "pg_read_all_data", "pg_write_all_data", "pg_read_server_files", "pg_write_server_files", "pg_execute_server_program" })
        {
            Assert.Contains($"'{predefined}'", query, StringComparison.Ordinal);
        }

        Assert.Contains("pg_has_role(granted.oid, voiding.oid, 'MEMBER')", query, StringComparison.Ordinal);
        Assert.Contains("THEN pg_has_role(current_user, granted.oid, 'MEMBER WITH ADMIN OPTION')", query, StringComparison.Ordinal);

        /* The 16 test sits under the version CASE, so a server below 16 never evaluates the option string. */
        Assert.True(query.IndexOf("server_version_num", StringComparison.Ordinal) < query.IndexOf("'MEMBER WITH ADMIN OPTION'", StringComparison.Ordinal));

        /* From 16 a revoke removes only the row recorded with the grantor it names, so the runner must be able to act as it. */
        Assert.Contains("AND pg_has_role(current_user, membership.grantor, 'USAGE')", query, StringComparison.Ordinal);
        Assert.True(query.IndexOf("'USAGE'", StringComparison.Ordinal) < query.IndexOf("ELSE NOT granted.rolsuper", StringComparison.Ordinal), "the grantor term belongs to the 16 arm.");

        var line = RaiseLine(checks, "These roles are members of roles whose privileges outrank the grants below");
        Assert.Contains("The script created and changed nothing.", line, StringComparison.Ordinal);
        Assert.Contains("before a password is sent", line, StringComparison.Ordinal);
        Assert.Contains("as a superuser", line, StringComparison.Ordinal);
        Assert.DoesNotContain("CHANGE_ME_", line, StringComparison.Ordinal);
        Assert.True(sql.IndexOf(line, StringComparison.Ordinal) < Regex.Match(sql, @"(?m)^ALTER ROLE \w+\s+LOGIN PASSWORD").Index);

        var readme = RepoFile.ReadRepoFile("docs", "darling", "security.md"); // #6416: moved from Darling/README.md
        Assert.Contains("when one of the four roles is a member of a role that would outrank the grants", readme, StringComparison.Ordinal);
    }

    /// <summary>The attribute take-backs and the membership revokes are two <c>DO</c> blocks under a stop that only a
    /// privilege refusal skips. In one block, an error other than a refusal rolled back every take-back
    /// (<c>NOSUPERUSER</c> included) while psql carried on and exited 0.</summary>
    [Fact]
    public void TheAttributesAndTheMemberships_AreTwoBlocks_UnderAStopThatOnlyAPrivilegeRefusalSkips()
    {
        var sql = Script();

        var firstPassword = Regex.Match(sql, @"(?m)^ALTER ROLE \w+\s+LOGIN PASSWORD").Index;
        var stops = Regex.Matches(sql, @"(?m)^\\set ON_ERROR_STOP (on|:darling_saved_stop)[ \t]*\r?$").Cast<Match>().ToArray();
        Assert.Equal(4, stops.Length);
        Assert.Contains(" on", stops[2].Value, StringComparison.Ordinal);
        Assert.Contains(":darling_saved_stop", stops[3].Value, StringComparison.Ordinal);
        Assert.True(stops[2].Index > firstPassword, "the second stop pair comes after the passwords.");

        var region = sql[stops[2].Index..stops[3].Index];
        Assert.Equal(2, Regex.Matches(region, @"(?m)^DO \$\$").Count);
        var split = region.IndexOf("END $$;", StringComparison.Ordinal);
        var attributes = region[..split];
        var memberships = region[(split + 1)..];
        Assert.Contains("ALTER ROLE %I NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS", attributes, StringComparison.Ordinal);
        Assert.DoesNotContain("REVOKE", attributes, StringComparison.Ordinal);
        Assert.Contains("REVOKE %I FROM %I", memberships, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER ROLE", memberships, StringComparison.Ordinal);

        /* Only the privilege refusal is caught, in both blocks; no catch-all arm. */
        Assert.DoesNotContain("WHEN OTHERS", region, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(region, @"EXCEPTION WHEN insufficient_privilege THEN").Count);
    }

    /// <summary>From PostgreSQL 16 a revoke names the recorded grantor, for a runner that is not a superuser too, and the
    /// block that revokes reads the memberships again afterwards: a voiding one still there stops the run with an error
    /// before any grant, because the revoke itself only warns when it finds no row for the grantor it names. Before 16 the
    /// non-superuser statement is unchanged.</summary>
    [Fact]
    public void TheMembershipRevoke_NamesTheGrantorFrom16_AndStopsTheRun_WhenAVoidingMembershipIsLeft()
    {
        var sql = Script();
        var stops = Regex.Matches(sql, @"(?m)^\\set ON_ERROR_STOP (on|:darling_saved_stop)[ \t]*\r?$").Cast<Match>().ToArray();
        var region = sql[stops[2].Index..stops[3].Index];
        var memberships = region[(region.IndexOf("END $$;", StringComparison.Ordinal) + 1)..];

        Assert.Contains("CASE WHEN (as_superuser OR current_setting('server_version_num')::int >= 160000) AND item.grantor IS NOT NULL", memberships, StringComparison.Ordinal);
        Assert.Contains("THEN format(' GRANTED BY %I', item.grantor) ELSE '' END", memberships, StringComparison.Ordinal);

        var loopEnd = memberships.IndexOf("END LOOP;", StringComparison.Ordinal);
        var after = memberships[loopEnd..];
        Assert.Contains("FROM pg_auth_members AS membership", after, StringComparison.Ordinal);
        Assert.Contains("WHERE member.rolname IN ('admin', 'viewer', 'web', 'mcp')", after, StringComparison.Ordinal);
        Assert.Contains("pg_has_role(granted.oid, voiding.oid, 'MEMBER')", after, StringComparison.Ordinal);
        Assert.DoesNotContain("MEMBER WITH ADMIN OPTION", after, StringComparison.Ordinal);
        Assert.DoesNotContain("'USAGE'", after, StringComparison.Ordinal);
        var raise = after.IndexOf("RAISE EXCEPTION 'These roles are still members of roles whose privileges outrank the grants below: %.", StringComparison.Ordinal);
        Assert.True(raise > after.IndexOf("IF remaining IS NOT NULL THEN", StringComparison.Ordinal), "the check raises an exception, not a warning.");
        Assert.DoesNotContain("CHANGE_ME_", after, StringComparison.Ordinal);
    }

    /// <summary>An error other than a privilege refusal in the attribute or membership blocks (a <c>lock_not_available</c>
    /// from <c>lock_timeout</c>, a <c>query_canceled</c> from <c>statement_timeout</c>) is not caught: with the stop on, psql
    /// ends the run there, before step 1a and every grant. Nothing before the block is in a transaction, so the run leaves
    /// the passwords set and the take-backs unfinished, and running the script again (which does each of them every run)
    /// finishes them.</summary>
    [Fact]
    public void AnErrorThatIsNotAPrivilegeRefusal_InTheTakeBackBlocks_EndsTheRunBeforeStep1a_AndAReRunRedoesThem()
    {
        var sql = Script();
        var stops = Regex.Matches(sql, @"(?m)^\\set ON_ERROR_STOP (on|:darling_saved_stop)[ \t]*\r?$").Cast<Match>().ToArray();
        var region = sql[stops[2].Index..stops[3].Index];

        Assert.True(stops[3].Index < sql.IndexOf("-- 1a. statement_timeout backstop", StringComparison.Ordinal), "the stop covers the blocks and ends before step 1a.");
        Assert.DoesNotContain("WHEN OTHERS", region, StringComparison.Ordinal);
        Assert.DoesNotContain("lock_not_available", region, StringComparison.Ordinal);
        Assert.DoesNotContain("query_canceled", region, StringComparison.Ordinal);
        Assert.DoesNotContain("BEGIN;", region, StringComparison.Ordinal);

        /* The only arm in each block names the privilege refusal, so any other SQLSTATE leaves the block. */
        var arms = Regex.Matches(region, @"EXCEPTION WHEN [^\r\n]* THEN");
        Assert.Equal(2, arms.Count);
        foreach (Match arm in arms)
        {
            Assert.Equal("EXCEPTION WHEN insufficient_privilege THEN", arm.Value);
        }
    }
}
