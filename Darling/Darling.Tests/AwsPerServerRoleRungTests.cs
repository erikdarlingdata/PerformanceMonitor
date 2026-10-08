/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V169: the per-server AWS role on <c>config.config_monitored_servers</c> (#5452). The static pins: the rung's place in
/// the ladder and its shape, the viewer's version probe, the column ACL, and the self-managed script's copy of the edit
/// function, its wrapper and the trigger rule. The store behaviour is in <see cref="AwsPerServerRoleLiveTests"/>.
///
/// <para>This file carries the "I am the top rung" claims, which move to the next rung's tests when one lands above it.</para>
/// </summary>
public sealed class AwsPerServerRoleRungTests
{
    internal const int RungVersion = 169;
    private const int PreviousVersion = 168;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument: V170's sentinel (#5495) follows it.</summary>
    internal const int ProbeOrdinal = 144;

    private const string RoleColumn = "aws_role_arn";

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("aws-per-server-role", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
        /* No longer the top rung: the PostgreSQL I/O hourly rollup rung (V170) landed above it. */
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Equal(PreviousVersion, RungVersion - 1);
        Assert.Contains(PreviousVersion, versions);
    }

    [Fact]
    public void TheRungIsIdempotent_AndMovesNoData()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        Assert.Equal(3, CountOf(rung, "ADD COLUMN IF NOT EXISTS"));
        Assert.Equal(3, CountOf(rung, "ALTER TABLE config.config_monitored_servers ADD COLUMN"));
        Assert.DoesNotContain("ALTER TABLE config_monitored_servers", rung, StringComparison.Ordinal);
        Assert.Contains("aws_role_arn text;", rung, StringComparison.Ordinal);
        Assert.Contains("aws_external_id text;", rung, StringComparison.Ordinal);
        Assert.Contains("GENERATED ALWAYS AS (aws_external_id IS NOT NULL) STORED", rung, StringComparison.Ordinal);

        /* The check is added once, behind a catalog guard (the V62 pattern), so a re-run does not fail on it. */
        Assert.Equal(1, CountOf(rung, "ADD CONSTRAINT config_monitored_servers_aws_role_check"));
        Assert.Contains("WHERE conname = 'config_monitored_servers_aws_role_check'", rung, StringComparison.Ordinal);
        Assert.Contains("IF NOT EXISTS (SELECT 1 FROM pg_constraint", rung, StringComparison.Ordinal);

        foreach (var shape in new[] { "UPDATE ", "DELETE ", "INSERT ", "CREATE INDEX", "DROP " })
        {
            Assert.DoesNotContain(shape, rung, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheCheckIsTheCoarseBackstop_WithNoRepeatCountOverTwoHundredFiftyFive()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        /* No term of the check can be NULL, so an external ID with no role is false rather than unknown. */
        Assert.Contains("(aws_external_id IS NULL OR aws_role_arn IS NOT NULL)", rung, StringComparison.Ordinal);
        Assert.Contains("aws_role_arn ~ '^arn:aws(-[a-z]+)*:iam::[0-9]{12}:role/[A-Za-z0-9_+=,.@/-]+$'", rung, StringComparison.Ordinal);
        Assert.Contains("char_length(aws_role_arn) <= " + AwsRoleSettings.RoleArnMaxLength, rung, StringComparison.Ordinal);
        Assert.Contains("aws_external_id ~ '^[A-Za-z0-9_+=,.@:/-]+$'", rung, StringComparison.Ordinal);
        Assert.Contains(
            $"char_length(aws_external_id) BETWEEN {AwsRoleSettings.ExternalIdMinLength} AND {AwsRoleSettings.ExternalIdMaxLength}",
            rung, StringComparison.Ordinal);

        /* PostgreSQL's regular expressions refuse a repeat count above 255 (2201B at the first use), so the lengths are
           char_length terms and the patterns carry no large count. */
        Assert.DoesNotMatch("[{][0-9]+,[0-9]*[}]", rung.Replace("{12}", string.Empty, StringComparison.Ordinal));
    }

    /* ---- the probe (three sites, top arm) ------------------------------------------------------------- */

    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains($"column_name = '{RoleColumn}'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);
        Assert.Contains("table_name = 'config_monitored_servers'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 2})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* A newer rung's sentinel (V170's) is the last argument. */
        Assert.Equal(ProbeOrdinal + 1, arity - 1);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);
        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source the arm sits ABOVE V168's, and returns this build's version. */
        var v169 = viewer.IndexOf("if (hasAwsRole)", StringComparison.Ordinal);
        var v168 = viewer.IndexOf("if (hasPlanRegressionDaily)", StringComparison.Ordinal);
        Assert.True(v169 >= 0, "the viewer has no V169 sentinel arm, so a fully-migrated store would map to 168");
        Assert.True(v168 >= 0, "the V168 arm is gone, so this pin is comparing against nothing");
        Assert.True(v169 < v168, "the V169 arm sits below V168's, so a current store maps one rung low");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[v169..v168], StringComparison.Ordinal);
    }

    /* ---- the column ACL -------------------------------------------------------------------------------- */

    [Fact]
    public void TheColumnAcl_ShowsTheRoleAndTheFlag_AndHidesTheExternalId()
    {
        var table = DarlingManagedRoles.ViewerRestrictedConfigTables.Single(t => t.Table == "config_monitored_servers");

        Assert.Contains("aws_role_arn", table.NonSecretColumns);
        Assert.Contains("aws_external_id_set", table.NonSecretColumns);
        Assert.Contains("aws_external_id", table.SecretColumns);
        Assert.DoesNotContain("aws_external_id", table.NonSecretColumns);

        /* The self-managed script grants the same two columns to both read-only roles, and never the ID. */
        var script = System.Text.RegularExpressions.Regex.Replace(
            RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql"), @"(?m)^\s*--.*$", string.Empty);
        var grants = System.Text.RegularExpressions.Regex.Matches(
            script, @"GRANT SELECT \([^;]*?\)\s+ON config\.config_monitored_servers TO (viewer|mcp);",
            System.Text.RegularExpressions.RegexOptions.Singleline);
        Assert.Equal(2, grants.Count);
        foreach (System.Text.RegularExpressions.Match grant in grants)
        {
            var uncommented = grant.Value;
            Assert.Contains("aws_role_arn", uncommented, StringComparison.Ordinal);
            Assert.Contains("aws_external_id_set", uncommented, StringComparison.Ordinal);
            Assert.DoesNotMatch(@"(?<![A-Za-z0-9_])aws_external_id(?![A-Za-z0-9_])", uncommented);
        }
    }

    /* ---- the edit function, its wrapper and the trigger rule -------------------------------------------- */

    [Fact]
    public void TheEditFunctionTakesSeventeenArguments_AndTheWrapperKeepsTheOldFifteen()
    {
        Assert.Equal(17, DarlingManagedRoles.EditMonitoredServerSignature.Split(',').Length);
        Assert.Equal(15, DarlingManagedRoles.EditMonitoredServerLegacySignature.Split(',').Length);
        Assert.Equal(DarlingManagedRoles.EditMonitoredServerLegacySignature + ", text, text", DarlingManagedRoles.EditMonitoredServerSignature);

        var function = DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config");
        Assert.Contains("p_aws_role_arn text,", function, StringComparison.Ordinal);
        Assert.Contains("p_aws_external_id text)", function, StringComparison.Ordinal);
        Assert.Contains("'external_id_needed'", function, StringComparison.Ordinal);
        Assert.Contains("'external_id_needs_role'", function, StringComparison.Ordinal);
        Assert.Contains(
            $"REVOKE ALL ON FUNCTION config.edit_monitored_server({DarlingManagedRoles.EditMonitoredServerSignature}) FROM PUBLIC;",
            function, StringComparison.Ordinal);

        var wrapper = DarlingManagedRoles.BuildEditMonitoredServerLegacyWrapperSql("config");
        Assert.Contains("SECURITY DEFINER", wrapper, StringComparison.Ordinal);
        Assert.Contains("SET search_path = pg_catalog, pg_temp", wrapper, StringComparison.Ordinal);
        Assert.Contains("array_remove(array_remove(COALESCE(p_columns, ARRAY[]::text[]), 'aws_role_arn'), 'aws_external_id')", wrapper, StringComparison.Ordinal);
        Assert.Contains("NULL::text, NULL::text) AS e;", wrapper, StringComparison.Ordinal);
        Assert.Contains(
            $"REVOKE ALL ON FUNCTION config.edit_monitored_server({DarlingManagedRoles.EditMonitoredServerLegacySignature}) FROM PUBLIC;",
            wrapper, StringComparison.Ordinal);

        /* The managed batch creates the long function before the wrapper that calls it, and grants both. */
        var batch = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);
        var functionAt = batch.IndexOf(function, StringComparison.Ordinal);
        var wrapperAt = batch.IndexOf(wrapper, StringComparison.Ordinal);
        Assert.True(functionAt >= 0 && wrapperAt > functionAt, "the wrapper must be created after the function it calls");
        Assert.Contains(
            $"GRANT EXECUTE ON FUNCTION config.edit_monitored_server({DarlingManagedRoles.EditMonitoredServerSignature}) TO viewer, mcp;",
            batch, StringComparison.Ordinal);
        Assert.Contains(
            $"GRANT EXECUTE ON FUNCTION config.edit_monitored_server({DarlingManagedRoles.EditMonitoredServerLegacySignature}) TO viewer, mcp;",
            batch, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSelfManagedScript_CarriesTheWrapper_BothGrants_AndTheTriggerRule()
    {
        var script = System.Text.RegularExpressions.Regex.Replace(
            RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql"), @"(?m)^\s*--.*$", string.Empty);

        Assert.Contains(
            NormalizeSql(DarlingManagedRoles.BuildEditMonitoredServerLegacyWrapperSql("config")), NormalizeSql(script), StringComparison.Ordinal);
        Assert.Contains(
            $"GRANT EXECUTE ON FUNCTION config.edit_monitored_server({DarlingManagedRoles.EditMonitoredServerSignature}) TO viewer, mcp;",
            script, StringComparison.Ordinal);
        Assert.Contains(
            $"GRANT EXECUTE ON FUNCTION config.edit_monitored_server({DarlingManagedRoles.EditMonitoredServerLegacySignature}) TO viewer, mcp;",
            script, StringComparison.Ordinal);

        var rules = DarlingManagedRoles.BuildServerPasswordRulesSql("config");
        Assert.Contains("USING ERRCODE = 'PW004'", rules, StringComparison.Ordinal);
        Assert.Contains(AwsRoleSettings.RoleChangeNeedsExternalIdMessage, rules, StringComparison.Ordinal);
        Assert.Contains(NormalizeSql(rules), NormalizeSql(script), StringComparison.Ordinal);
    }

    [Fact]
    public void TheMcpLogin_UpdatesEveryColumnButTheRoleAndExternalId_InTheManagedBatchAndTheScript()
    {
        var columns = DarlingManagedRoles.McpMonitoredServerUpdateColumns.Split(',', StringSplitOptions.TrimEntries);
        Assert.DoesNotContain("aws_role_arn", columns);
        Assert.DoesNotContain("aws_external_id", columns);
        Assert.DoesNotContain("aws_external_id_set", columns);
        Assert.Equal(columns.Length, columns.Distinct(StringComparer.Ordinal).Count());

        var batch = NormalizeSql(DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp));
        var script = NormalizeSql(RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql"));

        Assert.Contains($"GRANT UPDATE ({DarlingManagedRoles.McpMonitoredServerUpdateColumns}) ON config.config_monitored_servers TO mcp;", script, StringComparison.Ordinal);
        Assert.Contains("REVOKE UPDATE ON config.config_monitored_servers FROM mcp;", script, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT, DELETE ON config.config_monitored_servers TO mcp;", script, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT INSERT, UPDATE, DELETE ON config.config_monitored_servers", script, StringComparison.Ordinal);

        Assert.Contains($"GRANT UPDATE ({DarlingManagedRoles.McpMonitoredServerUpdateColumns}) ON", batch, StringComparison.Ordinal);
        Assert.Contains("REVOKE UPDATE ON", batch, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT, DELETE ON config.config_monitored_servers TO", batch, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT INSERT, UPDATE, DELETE ON config.config_monitored_servers", batch, StringComparison.Ordinal);
    }

    [Fact]
    public void TheEditFunction_AnswersInvalidValue_ForAnEmptyRequiredField_AndForAValueTheTableRefuses()
    {
        foreach (var body in new[]
        {
            NormalizeSql(DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config")),
            NormalizeSql(RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql")),
        })
        {
            Assert.Contains("'name' = ANY (p_columns) AND p_name IS NULL", body, StringComparison.Ordinal);
            Assert.Contains("OR v_host IS NULL OR v_port IS NULL OR v_auth IS NULL OR v_encrypt_mode IS NULL", body, StringComparison.Ordinal);
            Assert.Contains("'invalid_value'::text", body, StringComparison.Ordinal);
            Assert.Contains("EXCEPTION WHEN integrity_constraint_violation THEN", body, StringComparison.Ordinal);
        }
    }

    private static string NormalizeSql(string sql) =>
        System.Text.RegularExpressions.Regex.Replace(
            System.Text.RegularExpressions.Regex.Replace(sql, @"(?m)^\s*--.*$", ""), @"\s+", " ").Trim();

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        for (var at = haystack.IndexOf(needle, StringComparison.Ordinal);
             at >= 0;
             at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
