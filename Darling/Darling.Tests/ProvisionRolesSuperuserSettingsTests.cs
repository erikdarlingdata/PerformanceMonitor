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
/// <c>Darling/tools/provision-roles.sql</c> run by an owner who is not a superuser (#8779). The nine superuser-only
/// <c>ALTER ROLE ... SET</c> statements (temp_file_limit, log_min_duration_statement, log_parameter_max_length on
/// viewer, web and mcp) and step 0's two tracking switches used to fail with psql carrying on, which a run under
/// <c>-v ON_ERROR_STOP=1</c> cannot do. Each now catches <c>insufficient_privilege</c> and nothing else, and one
/// WARNING names what a superuser still has to apply.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ProvisionRolesSuperuserSettingsTests
{
    private static readonly string[] Roles = { "viewer", "web", "mcp" };
    private static readonly string[] Parameters = { "temp_file_limit", "log_min_duration_statement", "log_parameter_max_length" };

    private static string Script() => RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql").Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The text of every <c>BEGIN ... EXCEPTION WHEN x THEN ... END;</c> block, as (body, condition list).</summary>
    private static Match[] Handlers(string sql) =>
        Regex.Matches(sql, @"(?s)BEGIN\s*\n(?<body>(?:(?!\bBEGIN\b).)*?)\n\s*EXCEPTION WHEN (?<conditions>[^\n]*?) THEN(?<handler>.*?)\n\s*END(?: \$\$)?;").ToArray();

    [Fact]
    public void EachSuperuserOnlySetting_IsInsideAnInsufficientPrivilegeHandler()
    {
        var sql = Script();
        var handlers = Handlers(sql);

        foreach (var parameter in Parameters)
        {
            foreach (var role in Roles)
            {
                var statement = new Regex($@"ALTER ROLE {role}\s+SET {parameter} = ");
                Assert.Single(statement.Matches(sql));
                var holder = handlers.Where(handler => statement.IsMatch(handler.Groups["body"].Value)).ToArray();
                Assert.True(holder.Length == 1, $"ALTER ROLE {role} SET {parameter} is not inside exactly one BEGIN ... EXCEPTION block.");
                Assert.Equal("insufficient_privilege", holder[0].Groups["conditions"].Value.Trim());
            }
        }
    }

    [Fact]
    public void NoSuperuserOnlySetting_RunsBare()
    {
        var sql = Script();
        var outsideDo = Regex.Replace(sql, @"(?s)DO \$\$.*?END \$\$;", string.Empty);

        Assert.DoesNotMatch(@"(?m)^ALTER ROLE \w+\s+SET (temp_file_limit|log_min_duration_statement|log_parameter_max_length)", outsideDo);
    }

    [Fact]
    public void StepZeroTrackingSwitches_AreCaught()
    {
        var sql = Script();

        Assert.DoesNotContain("SET pg_stat_statements.track_utility = off;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SET pg_stat_monitor.pgsm_track_utility = off;", sql, StringComparison.Ordinal);

        var handlers = Handlers(sql);
        foreach (var parameter in new[] { "pg_stat_statements.track_utility", "pg_stat_monitor.pgsm_track_utility" })
        {
            var holder = handlers.Where(handler => handler.Groups["body"].Value.Contains($"set_config('{parameter}', 'off', false)", StringComparison.Ordinal)).ToArray();
            Assert.True(holder.Length == 1, $"{parameter} is not set through set_config inside exactly one BEGIN ... EXCEPTION block.");
            Assert.Equal("insufficient_privilege", holder[0].Groups["conditions"].Value.Trim());
        }
    }

    [Fact]
    public void OneWarningForTheSettings()
    {
        var sql = Script();

        var warnings = Regex.Matches(sql, @"RAISE WARNING[^;]*superuser-only settings[^;]*;");
        Assert.Single(warnings);
        Assert.Single(Regex.Matches(sql, @"superuser-only settings below"));
    }

    [Fact]
    public void HeaderNoLongerSaysTheyFailAndCarryOn()
    {
        var sql = Script();
        var bullet = sql.IndexOf("-- AN OWNER WHO IS NOT A SUPERUSER", StringComparison.Ordinal);
        Assert.True(bullet >= 0, "the header bullet moved or was renamed.");
        var end = sql.IndexOf("-- ====", bullet, StringComparison.Ordinal);
        var header = sql[bullet..end];

        Assert.DoesNotMatch(@"fail, and\s+--?\s*psql carries on", header);
        Assert.Contains("insufficient_privilege", header, StringComparison.Ordinal);
        Assert.Contains("store.provisionRoles", sql[..bullet], StringComparison.Ordinal);
    }
}
