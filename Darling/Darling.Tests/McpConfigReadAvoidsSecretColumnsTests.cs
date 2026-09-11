/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The MCP host must not ask the store for columns its own role is denied.
///
/// <para><b>Observed on the dogfood box, 2026-08-16.</b> Startup logged
/// <c>42501: permission denied for table config_notification</c> followed by "MCP could not read the
/// monitored-server registry — live plan fetch will use darling.json". Two separate defects met:</para>
///
/// <para>1. <see cref="DarlingManagedRoles.ViewerRestrictedConfigTables"/> deliberately REVOKEs table-wide
/// SELECT on <c>config_notification</c> from BOTH <c>viewer</c> and <c>mcp</c> and re-grants only the
/// non-secret columns — the SMTP password and username and the Teams/Slack/generic/PagerDuty bearer URLs
/// stay unreadable. That carve is correct and is not what changed. The MCP host was simply asking for the
/// whole row, and a column-level denial answers for the TABLE.</para>
///
/// <para>2. Worse, and the reason one password cost the registry: every section of
/// <c>LoadViewAsync</c> shares ONE try/catch, so the failed notification read discarded the four reads that
/// had already succeeded. MCP therefore lost the monitored-server registry and fell back to
/// <c>darling.json</c> for live plan fetches — a silent capability loss whose cause named a table MCP does
/// not use.</para>
///
/// <para>The fix is for the host to stop re-reading as <c>mcp</c> what the process already loaded
/// privileged. The first cut (#2293) merely skipped the notification row — and the failure moved to the
/// next denied column — so #2298 removed the host's own config-view read entirely: the plan-fetch resolver
/// serves the worker-published registry state. These pins hold that agreement so it cannot drift back.</para>
/// </summary>
public sealed class McpConfigReadAvoidsSecretColumnsTests
{
    /// <summary>
    /// THE FIX, as revised by #2298: the MCP host performs NO config-view read of its own at all.
    ///
    /// <para>The first cut (#2293) skipped the notification row — and the failure simply moved to the next
    /// denied column, because <c>ReadMonitoredServersAsync</c> selects <c>encrypted_password</c>, which the
    /// section-6 secret ACL SELECT-carves from <c>mcp</c>. Skipping rows one 42501 at a time was chasing the
    /// carve. The durable agreement with the boundary is that the host does not re-read as <c>mcp</c> what
    /// the process already loaded privileged: the plan-fetch resolver reads the worker-published
    /// <c>MonitoredServerRegistryState</c> instead.</para>
    ///
    /// <para>Pinned textually because reproducing it needs a live store provisioned with the least-privilege
    /// roles and a connection as <c>mcp</c> — and the failure is invisible to every other test, because a
    /// host that reads secrets it never uses works perfectly as the owner.</para>
    /// </summary>
    [Fact]
    public void TheMcpHostReadsNoConfigViewOfItsOwn()
    {
        var source = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs"));

        Assert.DoesNotContain("LoadViewAsync", source, StringComparison.Ordinal);
        Assert.Contains("_registryState.Read()", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The MCP host has no use for what it was asking for. If this ever fails, MCP grew an alert-delivery
    /// path and the skip above needs revisiting rather than silently starving it.
    /// </summary>
    [Fact]
    public void TheMcpSurfaceUsesNeitherSmtpNorWebhooks()
    {
        var mcpDir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp");
        var offenders = Directory.GetFiles(mcpDir, "*.cs", SearchOption.AllDirectories)
            .Where(f =>
            {
                /* The comment explaining the skip names both types, so only real USES count. Walked rather
                   than filtered by line prefix: a block comment's continuation lines in this codebase carry
                   no asterisk, so a prefix filter reads the second line of an explanatory block as code and
                   reports the prose as a use. Both needles are property accesses, so blanking literal text
                   along with the comments costs nothing here. */
                var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(f));
                return code.Contains(".Smtp", StringComparison.Ordinal) || code.Contains(".Webhooks", StringComparison.Ordinal);
            })
            .Select(Path.GetFileName)
            .ToArray();

        Assert.Empty(offenders);
    }

    /// <summary>
    /// The reason the skip is necessary, asserted rather than remembered: the notification SELECT really does
    /// name columns the carve revokes. If someone narrows that SELECT to non-secret columns only, this fails
    /// and tells them the skip has become unnecessary — which is the useful direction for a guard to fail in.
    /// </summary>
    [Fact]
    public void TheNotificationReadStillNamesCarvedSecretColumns()
    {
        var provider = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs"));

        var start = provider.IndexOf("FROM config_notification", StringComparison.Ordinal);
        Assert.True(start > 0, "the notification read moved — re-point this guard");
        var selectStart = provider.LastIndexOf("SELECT", start, StringComparison.Ordinal);
        var readSql = provider[selectStart..start];

        var carved = DarlingManagedRoles.ViewerRestrictedConfigTables
            .Single(t => string.Equals(t.Table, "config_notification", StringComparison.Ordinal))
            .SecretColumns;

        var namedSecrets = carved.Where(c => readSql.Contains(c, StringComparison.Ordinal)).ToArray();
        Assert.NotEmpty(namedSecrets);
    }

    /// <summary>
    /// #3314 put a <c>config_notification</c> read back on the MCP surface — the delivery cooldown, the one
    /// alert-engine knob stored on that table — so the carve now has to hold for a read that DOES ship,
    /// rather than by the whole read having been removed.
    ///
    /// <para>The mirror image of <see cref="TheNotificationReadStillNamesCarvedSecretColumns"/>, and it
    /// fails in the costly direction: that one asserts the PRIVILEGED read still names secrets (so the skip
    /// stays justified), this one asserts the MCP read names NONE. Column-level denial answers for the whole
    /// TABLE, so a single carved column added to this SELECT does not degrade the read — it 42501s the entire
    /// call, and that is the #2293 failure, where skipping one denied row simply moved the error to the next.
    /// Every non-secret column is derived from the ACL rather than listed, so a column reclassified as secret
    /// makes THIS fail on the day of the reclassification instead of on the next deployment.</para>
    ///
    /// <para>The read is also asserted to stay narrow: the value the tool needs is one column, and
    /// <c>SELECT *</c> — or a convenience widening to "the non-secret columns" — is denied outright by the
    /// carve for the star and is a pointless secret-adjacent read for the rest.</para>
    /// </summary>
    [Fact]
    public void TheMcpDeliveryCooldownReadNamesNoCarvedSecretColumn()
    {
        var sql = PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.DeliveryCooldownSelectSql;
        var acl = DarlingManagedRoles.ViewerRestrictedConfigTables
            .Single(t => string.Equals(t.Table, "config_notification", StringComparison.Ordinal));

        Assert.Contains("FROM config_notification", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("*", sql, StringComparison.Ordinal);

        var named = acl.SecretColumns.Where(c => sql.Contains(c, StringComparison.Ordinal)).ToArray();
        Assert.Empty(named);

        /* And it really does name the one non-secret column it needs -- without this the assertions above
           are satisfied by a SELECT that reads nothing from the table at all. */
        var selected = sql[(sql.IndexOf("SELECT", StringComparison.Ordinal) + 6)..
                            sql.IndexOf("FROM config_notification", StringComparison.Ordinal)]
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .ToArray();

        Assert.Equal(new[] { "email_cooldown_minutes" }, selected);
        Assert.Contains("email_cooldown_minutes", acl.NonSecretColumns);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    private static string ReadSource(string relative) => File.ReadAllText(Path.Combine(RepoRoot(), relative));
}
