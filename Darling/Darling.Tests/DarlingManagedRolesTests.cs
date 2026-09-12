/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The least-privilege role provisioning DDL (V8 security hardening, #1262). Ungated: the
/// generated statements' shape — DO-guarded idempotent CREATE ROLE, the ALTER ROLE password
/// re-assert, the collect/config grants, the config-only writes, the ALTER DEFAULT PRIVILEGES that
/// auto-grant new tables, the public hardening + the admin/viewer CONNECT re-grant — plus the
/// alphanumeric-password safety guard. The live round-trip (roles actually created, admin writes /
/// viewer denied, default-privileges proof) is in the gated <see cref="DarlingSecuritySplitLiveTests"/>.
/// </summary>
public sealed class DarlingManagedRolesTests
{
    [Fact]
    public void BuildProvisioningSql_CreatesRolesIdempotently_LoginNoSuperuser()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        /* DO-guarded CREATE ROLE (no IF NOT EXISTS on CREATE ROLE) + a password re-assert every start. */
        Assert.Contains("FROM pg_roles WHERE rolname = 'admin'", sql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_roles WHERE rolname = 'viewer'", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE ROLE admin LOGIN NOSUPERUSER PASSWORD 'AdminPassword01';", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE ROLE viewer LOGIN NOSUPERUSER PASSWORD 'ViewerPassword02';", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE admin  LOGIN NOSUPERUSER PASSWORD 'AdminPassword01';", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE viewer LOGIN NOSUPERUSER PASSWORD 'ViewerPassword02';", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_StampsMarker_AndFailsLoudOnUnmarkedCollision()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        Assert.Equal("darling-managed", DarlingManagedRoles.RoleMarker);

        /* Fresh roles are STAMPED with the marker comment. */
        Assert.Contains("COMMENT ON ROLE admin IS 'darling-managed';", sql, StringComparison.Ordinal);
        Assert.Contains("COMMENT ON ROLE viewer IS 'darling-managed';", sql, StringComparison.Ordinal);

        /* An existing same-named role is trusted only if it carries the marker (read via
           shobj_description); otherwise provisioning RAISEs rather than repurposing it. */
        Assert.Contains("shobj_description((SELECT oid FROM pg_roles WHERE rolname = 'admin'), 'pg_authid') IS DISTINCT FROM 'darling-managed'", sql, StringComparison.Ordinal);
        Assert.Contains("shobj_description((SELECT oid FROM pg_roles WHERE rolname = 'viewer'), 'pg_authid') IS DISTINCT FROM 'darling-managed'", sql, StringComparison.Ordinal);
        Assert.Contains("RAISE EXCEPTION 'Role \"admin\" already exists and was not created by Darling", sql, StringComparison.Ordinal);
        Assert.Contains("RAISE EXCEPTION 'Role \"viewer\" already exists and was not created by Darling", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_GrantsReadBothSchemas_WritesConfigForAdminOnly()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        Assert.Contains("GRANT USAGE ON SCHEMA collect, config TO admin, viewer;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA collect TO admin, viewer;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA config  TO admin, viewer;", sql, StringComparison.Ordinal);

        /* config writes are admin-only — viewer never gets INSERT/UPDATE/DELETE. */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin, viewer", sql, StringComparison.Ordinal);

        /* No BLANKET (schema-wide) collect write to anyone but the owner. mcp gets a single-table INSERT on
           collect.analysis_findings (section 6, asserted separately), never a schema-wide collect write. */
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA collect", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_GrantsViewerTheNarrowCustomViewsWrite_WithoutWideningTheSchemaGrant()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        /* #1563: the ONE viewer write — an EXPLICIT single-table grant on config.custom_views (the web
           dashboard's user-authored view store; editing is loopback-gated server-side). */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.custom_views TO viewer;", sql, StringComparison.Ordinal);

        /* It must NOT widen the schema-wide config write to viewer (that grant stays admin-only, pinned here). */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin, viewer", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO viewer", sql, StringComparison.Ordinal);

        /* NO ALTER DEFAULT PRIVILEGES grants viewer a write — ADP has no per-table form, so an ADP write would
           broaden viewer to ALL of config. The default-privileges write grant names admin only. */
        Assert.DoesNotContain("GRANT INSERT, UPDATE, DELETE ON TABLES TO admin, viewer", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT INSERT, UPDATE, DELETE ON TABLES TO viewer", sql, StringComparison.Ordinal);

        /* custom_views carries no secret columns, so it is NOT part of the viewer secret-column carve. */
        Assert.DoesNotContain("REVOKE SELECT ON config.custom_views", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_DefaultPrivileges_AutoGrantNewTables()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        /* New collector tables (created bare into collect via search_path) auto-inherit SELECT. */
        Assert.Contains("ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA collect", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON TABLES TO admin, viewer;", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA config", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON TABLES TO admin;", sql, StringComparison.Ordinal);

        /* Fail-closed: a future serial/identity config column needs sequence USAGE for admin's INSERT. */
        Assert.Contains("GRANT USAGE, SELECT ON SEQUENCES TO admin;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_CarvesViewerSecretColumns_FailClosed()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        /* The blanket config SELECT to viewer is still present (covers the non-secret config tables), but
           the three credential-bearing tables are then carved to column-level for viewer. */
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA config  TO admin, viewer;", sql, StringComparison.Ordinal);

        foreach (var acl in DarlingManagedRoles.ViewerRestrictedConfigTables)
        {
            /* viewer's table-wide SELECT is dropped, then re-granted on ONLY the non-secret columns. */
            Assert.Contains($"REVOKE SELECT ON config.{acl.Table} FROM viewer;", sql, StringComparison.Ordinal);
            Assert.Contains($"GRANT SELECT ({string.Join(", ", acl.NonSecretColumns)}) ON config.{acl.Table} TO viewer;",
                sql, StringComparison.Ordinal);

            /* The carve is viewer-only — admin is never column-restricted (it writes these tables). */
            Assert.DoesNotContain($"REVOKE SELECT ON config.{acl.Table} FROM admin", sql, StringComparison.Ordinal);
        }

        /* No secret column name appears anywhere in a GRANT ... TO viewer (belt over the per-table check). */
        var viewerGrantLines = sql.Split('\n')
            .Where(line => line.Contains("TO viewer", StringComparison.Ordinal) && line.Contains("GRANT SELECT (", StringComparison.Ordinal));
        foreach (var secret in DarlingManagedRoles.ViewerRestrictedConfigTables.SelectMany(a => a.SecretColumns))
        {
            foreach (var line in viewerGrantLines)
            {
                Assert.DoesNotContain(secret, line, StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public void ViewerRestrictedConfigTables_SecretAndNonSecretColumns_AreDisjointAndNonEmpty()
    {
        var expectedSecrets = new Dictionary<string, string[]>(StringComparer.Ordinal)
        {
            /* V113 (#2138 phase 1): the remediation credential's blob is the same kind of thing as the
               monitoring one beside it — a DPAPI secret — and it authenticates a WRITE, so if anything on
               this table is secret it is. */
            ["config_monitored_servers"] = new[] { "encrypted_password", "remediation_encrypted_password" },
            ["config_command"] = new[] { "args_json" },
            /* generic_url is a bearer secret like the sibling webhook URLs, and generic_headers holds the
               Authorization token itself (#1506 / V26). pagerduty_routing_key is the Events API v2 integration
               key — a bearer secret like the webhook URLs (V40). */
            ["config_notification"] = new[]
            {
                "smtp_encrypted_password", "smtp_username", "teams_url", "slack_url",
                "generic_url", "generic_headers", "pagerduty_routing_key",
            },
        };

        /* Exactly the three known secret-bearing tables, with the expected secret columns. */
        Assert.Equal(
            expectedSecrets.Keys.OrderBy(k => k, StringComparer.Ordinal),
            DarlingManagedRoles.ViewerRestrictedConfigTables.Select(a => a.Table).OrderBy(k => k, StringComparer.Ordinal));

        foreach (var acl in DarlingManagedRoles.ViewerRestrictedConfigTables)
        {
            Assert.NotEmpty(acl.NonSecretColumns);
            Assert.NotEmpty(acl.SecretColumns);

            /* A column is never in both sets — a granted column can't also be a denied one. */
            Assert.Empty(acl.NonSecretColumns.Intersect(acl.SecretColumns, StringComparer.Ordinal));

            Assert.Equal(
                expectedSecrets[acl.Table].OrderBy(c => c, StringComparer.Ordinal),
                acl.SecretColumns.OrderBy(c => c, StringComparer.Ordinal));
        }
    }

    [Fact]
    public void BuildViewerColumnAclSql_UsesTheGivenSchemaAndRole()
    {
        /* The same generator the live test applies to its throwaway roles — pin that it honors the passed
           schema + role names (so the test and the real provisioning can never drift). */
        var sql = DarlingManagedRoles.BuildViewerColumnAclSql("config", "sec_viewer_test");

        Assert.Contains("REVOKE SELECT ON config.config_monitored_servers FROM sec_viewer_test;", sql, StringComparison.Ordinal);
        Assert.Contains("ON config.config_monitored_servers TO sec_viewer_test;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("encrypted_password", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("args_json", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("teams_url", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_HardensPublic_ButKeepsAdminViewerConnect()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        Assert.Contains("REVOKE CREATE ON SCHEMA public FROM PUBLIC;", sql, StringComparison.Ordinal);
        Assert.Contains("REVOKE ALL ON DATABASE darling FROM PUBLIC;", sql, StringComparison.Ordinal);

        /* REVOKE ALL drops PUBLIC's implicit CONNECT, so admin/viewer must be re-granted it, or the
           Viewer could no longer connect — the correctness fix over the design's literal DDL. */
        Assert.Contains("GRANT CONNECT ON DATABASE darling TO admin, viewer;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_McpRole_CreatedAndGrantedReadPlusTwoInserts_NoAdp()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        /* The mcp role is created + stamped + re-asserted, guarded exactly like admin/viewer. */
        Assert.Contains("FROM pg_roles WHERE rolname = 'mcp'", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE ROLE mcp LOGIN NOSUPERUSER PASSWORD 'McpPassword03';", sql, StringComparison.Ordinal);
        Assert.Contains("COMMENT ON ROLE mcp IS 'darling-managed';", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE mcp    LOGIN NOSUPERUSER PASSWORD 'McpPassword03';", sql, StringComparison.Ordinal);
        Assert.Contains("RAISE EXCEPTION 'Role \"mcp\" already exists and was not created by Darling", sql, StringComparison.Ordinal);

        /* viewer's read surface, granted as SEPARATE 'TO mcp' statements — NOT appended to the pinned
           'TO admin, viewer' grant lines (which stay verbatim). */
        Assert.Contains("GRANT USAGE ON SCHEMA collect, config TO mcp;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA collect TO mcp;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA config  TO mcp;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT CONNECT ON DATABASE darling TO mcp;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("TO admin, viewer, mcp", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA collect TO admin, viewer;", sql, StringComparison.Ordinal);

        /* Exactly the two narrow ANALYSIS writes (Round 4 #1: INSERT-only). The mcp role's ONE other write —
           INSERT/UPDATE/DELETE on config.custom_views (#1599, the custom-view tools) — is a single-table grant
           pinned separately by BuildProvisioningSql_McpRole_GrantsCustomViewsWrite_LikeViewer; it does not widen
           either schema-wide claim below. */
        Assert.Contains("GRANT INSERT ON collect.analysis_findings TO mcp;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT ON config.analysis_muted TO mcp;", sql, StringComparison.Ordinal);

        /* No DELETE on analysis_muted (no MCP unmute tool) and no schema-wide write for mcp. */
        Assert.DoesNotContain("DELETE ON config.analysis_muted TO mcp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO mcp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA collect TO mcp", sql, StringComparison.Ordinal);

        /* NO ALTER DEFAULT PRIVILEGES names mcp — ADP has no per-table form, so an ADP INSERT would broaden
           mcp to ALL of collect. The narrow grants are explicit single-table statements. */
        Assert.DoesNotContain("ON TABLES TO mcp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ON SEQUENCES TO mcp", sql, StringComparison.Ordinal);
        foreach (var adpLine in sql.Split('\n').Where(l => l.Contains("ALTER DEFAULT PRIVILEGES", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("mcp", adpLine, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BuildProvisioningSql_McpRole_GrantsCustomViewsWrite_LikeViewer()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        /* #1599: the mcp role gets the SAME single-table write on config.custom_views the viewer role has, so the
           MCP custom-view tools can create/update/delete views. An EXPLICIT single-table statement (its own
           'TO mcp' line, separate from viewer's), mirroring the mcp analysis-write grants. */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.custom_views TO mcp;", sql, StringComparison.Ordinal);

        /* Both write identities are pinned side by side and neither is folded into the other's line. */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.custom_views TO viewer;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ON config.custom_views TO viewer, mcp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ON config.custom_views TO mcp, viewer", sql, StringComparison.Ordinal);

        /* The write stays NARROW: it must NOT widen mcp to a schema-wide config write, and NO ALTER DEFAULT
           PRIVILEGES names mcp (that would broaden it to all of config). */
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO mcp", sql, StringComparison.Ordinal);
        foreach (var adpLine in sql.Split('\n').Where(l => l.Contains("ALTER DEFAULT PRIVILEGES", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("mcp", adpLine, StringComparison.Ordinal);
        }

        /* custom_views carries no secret columns, so it is NOT part of the mcp secret-column carve. */
        Assert.DoesNotContain("REVOKE SELECT ON config.custom_views", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_McpRole_GrantsAlertTuningWrites_NarrowlyWithBeaconColumns()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        /* The MCP alert-tuning write tools: full CRUD on the mute rules, and UPDATE-only on the SINGLETON
           alert-settings row (never INSERT/DELETE — the row is a fixed singleton the service seeds). */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.config_mute_rules TO mcp;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT UPDATE ON config.config_alert_settings TO mcp;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON config.config_alert_settings", sql, StringComparison.Ordinal);

        /* The beacon caveat: a config_alert_settings write fires the SECURITY INVOKER bump trigger, which UPDATEs
           config_service — so mcp needs a COLUMN-level UPDATE on JUST the two beacon columns (never paused /
           capture_plans / mcp_enabled / mcp_port, and never a table-wide config_service UPDATE). */
        Assert.Contains("GRANT UPDATE (config_version, updated_at) ON config.config_service TO mcp;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT UPDATE ON config.config_service TO mcp", sql, StringComparison.Ordinal);

        /* Still NARROW: no schema-wide config write for mcp, and NO ALTER DEFAULT PRIVILEGES names mcp (either
           would broaden it to all of config). */
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO mcp", sql, StringComparison.Ordinal);
        foreach (var adpLine in sql.Split('\n').Where(l => l.Contains("ALTER DEFAULT PRIVILEGES", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("mcp", adpLine, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BuildProvisioningSql_McpRole_GrantsMonitoredServersWrite_Narrowly()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        /* The MCP server-onboarding write tools (add_servers / remove_server): full CRUD on the single
           config_monitored_servers table — an EXPLICIT single-table statement, its own 'TO mcp' line. */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.config_monitored_servers TO mcp;", sql, StringComparison.Ordinal);

        /* Still NARROW: no schema-wide config write for mcp, and NO ALTER DEFAULT PRIVILEGES names mcp (either
           would broaden it to all of config). No NEW config_service grant — section 8's beacon column-grant
           already covers the monitored-servers bump trigger. */
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO mcp", sql, StringComparison.Ordinal);
        foreach (var adpLine in sql.Split('\n').Where(l => l.Contains("ALTER DEFAULT PRIVILEGES", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("mcp", adpLine, StringComparison.Ordinal);
        }

        /* The credential column stays SELECT-carved from mcp (section 6) — mcp WRITEs a password blob but never
           READs one back — so config_monitored_servers still appears in the mcp REVOKE/GRANT-column carve. */
        Assert.Contains("REVOKE SELECT ON config.config_monitored_servers FROM mcp;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_McpRole_CarvesSecretColumns_LikeViewer()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        /* The mcp role gets the SAME fail-closed secret-column carve as viewer (reusing
           BuildViewerColumnAclSql(config, "mcp")), so a network-reachable token can never read the
           credential columns. */
        foreach (var acl in DarlingManagedRoles.ViewerRestrictedConfigTables)
        {
            Assert.Contains($"REVOKE SELECT ON config.{acl.Table} FROM mcp;", sql, StringComparison.Ordinal);
            Assert.Contains($"GRANT SELECT ({string.Join(", ", acl.NonSecretColumns)}) ON config.{acl.Table} TO mcp;",
                sql, StringComparison.Ordinal);
        }

        /* No secret column name appears in any GRANT SELECT (...) TO mcp line (belt over the per-table check). */
        var mcpGrantLines = sql.Split('\n')
            .Where(line => line.Contains("TO mcp", StringComparison.Ordinal) && line.Contains("GRANT SELECT (", StringComparison.Ordinal));
        foreach (var secret in DarlingManagedRoles.ViewerRestrictedConfigTables.SelectMany(a => a.SecretColumns))
        {
            foreach (var line in mcpGrantLines)
            {
                Assert.DoesNotContain(secret, line, StringComparison.Ordinal);
            }
        }
    }

    [Theory]
    [InlineData("with space")]
    [InlineData("semi;colon")]
    [InlineData("quote'here")]
    [InlineData("dash-dash")]
    [InlineData("")]
    public void BuildProvisioningSql_RejectsNonAlphanumericPassword(string badPassword)
    {
        /* Passwords are interpolated into DDL literals; the alnum guard fails closed if the
           generator's alphabet is ever widened without switching to quote_literal. Guards all THREE
           role passwords (admin/viewer/mcp). */
        Assert.Throws<ArgumentException>(() => DarlingManagedRoles.BuildProvisioningSql(badPassword, "ViewerPassword02", "McpPassword03"));
        Assert.Throws<ArgumentException>(() => DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", badPassword, "McpPassword03"));
        Assert.Throws<ArgumentException>(() => DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", badPassword));
    }

    [Fact]
    public void BuildProvisioningSql_AcceptsGeneratedPasswords()
    {
        /* The real generated passwords (32-char alnum) pass the guard and inject cleanly. */
        var admin = DarlingManagedPostgres.GeneratePassword();
        var viewer = DarlingManagedPostgres.GeneratePassword();
        var mcp = DarlingManagedPostgres.GeneratePassword();

        var sql = DarlingManagedRoles.BuildProvisioningSql(admin, viewer, mcp);

        Assert.Contains($"PASSWORD '{admin}'", sql, StringComparison.Ordinal);
        Assert.Contains($"PASSWORD '{viewer}'", sql, StringComparison.Ordinal);
        Assert.Contains($"PASSWORD '{mcp}'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_CreatesResolveDefinerFunction_ScopedAndGrantedToViewerAndMcp()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");

        /* #3334: the SECURITY DEFINER resolve-on-delete write function, so viewer/mcp can record the recovery
           row config_alert_log otherwise refuses them WITHOUT a blanket INSERT grant that would let them forge
           history. Definer-safety is the whole point, so each safety property is pinned individually. */
        Assert.Contains("CREATE OR REPLACE FUNCTION config.record_custom_alert_resolution(", sql, StringComparison.Ordinal);
        Assert.Contains("SECURITY DEFINER", sql, StringComparison.Ordinal);

        /* The pinned search_path (config first, then pg_catalog) is what makes injection impossible — no caller
           path can redirect the unqualified config_alert_log or now(). */
        Assert.Contains("SET search_path = config, pg_catalog", sql, StringComparison.Ordinal);

        /* A fresh function is EXECUTE-able by PUBLIC by default, so the REVOKE is mandatory and must precede the
           narrow grant; EXECUTE is the ONLY privilege the least-privilege roles get. */
        Assert.Contains("REVOKE ALL ON FUNCTION config.record_custom_alert_resolution(integer, text, text, text) FROM PUBLIC;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT EXECUTE ON FUNCTION config.record_custom_alert_resolution(integer, text, text, text) TO viewer, mcp;", sql, StringComparison.Ordinal);
        var revokeAt = sql.IndexOf("REVOKE ALL ON FUNCTION config.record_custom_alert_resolution", StringComparison.Ordinal);
        var grantAt = sql.IndexOf("GRANT EXECUTE ON FUNCTION config.record_custom_alert_resolution", StringComparison.Ordinal);
        Assert.True(revokeAt >= 0 && grantAt > revokeAt, "the PUBLIC revoke must precede the EXECUTE grant.");

        /* The body writes ONE config_alert_log row, shape-LOCKED to a no-channel resolution (alert_sent false,
           notification_type 'none', zeroed values, unmuted) — so a grantee can page nothing and forge no fire. */
        Assert.Contains("INSERT INTO config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("false, 'none', NULL, false,", sql, StringComparison.Ordinal);

        /* NOT the rejected blanket grant: viewer/mcp never get a direct write on the history table. */
        Assert.DoesNotContain("ON config.config_alert_log TO viewer", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ON config.config_alert_log TO mcp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ON config_alert_log TO viewer", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ON config_alert_log TO mcp", sql, StringComparison.Ordinal);

        /* The shared builder emits the same CREATE + REVOKE the gated live proof test creates the function from. */
        Assert.Contains(DarlingManagedRoles.BuildCustomAlertResolveFunctionSql("config"), sql, StringComparison.Ordinal);
    }
}
