/* Copyright (c) 2026 Erik Darling, Darling Data LLC
 * Licensed under the MIT License. See LICENSE in the project root. */
using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

// #1776 own-store: each test creates a scratch database, and the temporary role is rolled back.
public sealed class WebAccessExportLiveTests
{
    [Fact]
    public async Task StandaloneProvisioningRemovesBlanketExportTableGrants()
    {
        var connection = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connection), "Set DARLING_TEST_PG for export provisioning tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connection!, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var postgres = await source.OpenConnectionAsync(ct);
        await PgMigrations.MigrateAsync(postgres, null, ct);
        var script = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql"), ct);
        var blocks = Regex.Matches(script, @"DO \$web_groups\$[\s\S]*?END \$web_groups\$;");
        Assert.NotEmpty(blocks);
        await using var transaction = await postgres.BeginTransactionAsync(ct);
        var role = "export_probe_" + Guid.NewGuid().ToString("N");
        async Task Execute(string sql)
        {
            await using var command = new NpgsqlCommand(sql, postgres, transaction);
            await command.ExecuteNonQueryAsync(ct);
        }
        await Execute("CREATE ROLE " + role + " NOLOGIN");
        foreach (Match block in blocks)
        {
            await Execute("GRANT SELECT ON ALL TABLES IN SCHEMA config TO " + role);
            await Execute(block.Value);
            await using var check = new NpgsqlCommand("""
                SELECT has_table_privilege($1,'config.web_access_exports','SELECT')
                    OR has_table_privilege($1,'config.web_access_export_keys','SELECT')
                """, postgres, transaction);
            check.Parameters.AddWithValue(role);
            Assert.False((bool)(await check.ExecuteScalarAsync(ct))!);
        }
        await transaction.RollbackAsync(ct);
    }

    [Fact]
    public async Task ViewerCanRecoverTheSameTokenAfterReconnectingAndRotationReplacesIt()
    {
        var connection = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connection), "Set DARLING_TEST_PG for token recovery tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connection!, ct);
        var settings = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        await using (var migration = await source.OpenConnectionAsync(ct))
            await PgMigrations.MigrateAsync(migration, null, ct);
        using var key = PasswordPrivateKey.Generate();
        var sealer = new PerformanceMonitor.Darling.Viewer.ViewerPasswordSealer(key.PublicKey);
        await using var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(settings.ConnectionString);
        await viewer.DetectReadOnlyAsync(ct);
        var token = await viewer.CreateWebAccessGroupAsync("Operations", WebAccessGroupScope.AllServers,
            Array.Empty<int>(), sealer, "Operations team", "https://dashboard.example", ct);
        var store = new WebAccessGroupStore(source);
        Assert.True(await store.ActivateAsync(token.GroupId, 1, token.Generation, null, ct));
        var group = (await viewer.GetWebAccessGroupAsync(token.GroupId, ct))!;
        Assert.Equal(("Operations team", "https://dashboard.example"), await viewer.GetWebAccessHandoverAsync(group, ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => viewer.UpdateWebAccessGroupWithHandoverAsync(
            group, group.Scope, Array.Empty<int>(), false, "Invalid address", "bad-address", ct));
        await using (var constraint = source.CreateCommand("""
            ALTER TABLE config.web_access_exports ADD CHECK (dashboard_address <> 'https://reject.example')
            """))
            await constraint.ExecuteNonQueryAsync(ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => viewer.CreateWebAccessGroupAsync(
            "RejectedCreate", WebAccessGroupScope.AllServers, Array.Empty<int>(), sealer,
            "Rejected handover", "https://reject.example", ct));
        Assert.Single(await store.ListAsync(ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => viewer.UpdateWebAccessGroupWithHandoverAsync(
            group, group.Scope, Array.Empty<int>(), false, "Rejected address", "https://reject.example", ct));
        var afterFailure = (await viewer.GetWebAccessGroupAsync(group.GroupId, ct))!;
        Assert.True(afterFailure.Enabled);
        Assert.Equal(group.Revision, afterFailure.Revision);
        Assert.Equal(("Operations team", "https://dashboard.example"), await viewer.GetWebAccessHandoverAsync(group, ct));
        await viewer.UpdateWebAccessGroupWithHandoverAsync(group, group.Scope, Array.Empty<int>(), true,
            "Operations team", "https://dashboard.example", ct);
        group = (await viewer.GetWebAccessGroupAsync(group.GroupId, ct))!;
        Assert.Equal(token.Plain, await viewer.ExportWebAccessTokenAsync(group, ct));
        await using var reconnected = new PerformanceMonitor.Darling.Viewer.ViewerDataService(settings.ConnectionString);
        await reconnected.DetectReadOnlyAsync(ct);
        Assert.Equal(token.Plain, await reconnected.ExportWebAccessTokenAsync(group, ct));
        Assert.Equal(("Operations team", "https://dashboard.example"), await reconnected.GetWebAccessHandoverAsync(group, ct));
        var replacement = await reconnected.RotateWebAccessGroupTokenAsync(group, sealer, ct);
        var stale = await Assert.ThrowsAsync<PostgresException>(() => viewer.ExportWebAccessTokenAsync(group, ct));
        Assert.Equal("WG409", stale.SqlState);
        var pending = (await reconnected.GetWebAccessGroupAsync(group.GroupId, ct))!;
        Assert.True(await store.ActivateAsync(group.GroupId, pending.Revision, replacement.Generation, null, ct));
        var active = (await reconnected.GetWebAccessGroupAsync(group.GroupId, ct))!;
        Assert.Equal(replacement.Plain, await viewer.ExportWebAccessTokenAsync(active, ct));
    }

    [Fact]
    public async Task RecoveryIsPrivateAndExportIsGenerationBoundAndAudited()
    {
        var connection = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connection), "Set DARLING_TEST_PG for isolated export storage tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connection!, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var migration = await source.OpenConnectionAsync(ct))
            await PgMigrations.MigrateAsync(migration, null, ct);
        var id = Guid.NewGuid();
        var generation = Guid.NewGuid();
        var store = new WebAccessGroupStore(source);
        await store.CreateAsync(id,"ExportTest",WebAccessGroupScope.AllServers,Array.Empty<int>(),generation,"sealed:v1:test",true,"test",ct);
        async Task<string?> Export(string action, Guid version, string? envelope = null)
        {
            await using var command = source.CreateCommand("SELECT config.web_access_export($1,$2,$3,$4,NULL,NULL)");
            command.Parameters.AddWithValue(action);
            command.Parameters.AddWithValue(id);
            command.Parameters.AddWithValue(version);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Text,(object?)envelope ?? DBNull.Value);
            return await command.ExecuteScalarAsync(ct) as string;
        }
        await Export("store",generation,"key.nonce.cipher.tag");
        var pending = await Assert.ThrowsAsync<PostgresException>(() => Export("export",generation));
        Assert.Equal("WG400",pending.SqlState);
        Assert.True(await store.ActivateAsync(id,1,generation,null,ct));
        Assert.Equal("key.nonce.cipher.tag",await Export("export",generation));
        Assert.Equal("key.nonce.cipher.tag",await Export("export",generation));
        await using (var audit = source.CreateCommand("SELECT count(*) FROM config.web_access_group_events WHERE event='exported'"))
            Assert.Equal(2L,await audit.ExecuteScalarAsync(ct));
        await using (var acl = source.CreateCommand("""
            SELECT count(*) FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a
            WHERE c.oid IN ('config.web_access_exports'::regclass,'config.web_access_export_keys'::regclass)
            AND a.grantee<>c.relowner
            """))
            Assert.Equal(0L,await acl.ExecuteScalarAsync(ct));
        await using (var functionAcl = source.CreateCommand("""
            SELECT count(*) FROM pg_proc p CROSS JOIN LATERAL aclexplode(p.proacl) a
            LEFT JOIN pg_roles r ON r.oid=a.grantee
            WHERE p.oid='config.web_access_export(text,uuid,uuid,text,text,text)'::regprocedure
            AND a.grantee<>p.proowner AND COALESCE(r.rolname,'PUBLIC')<>'admin'
            """))
            Assert.Equal(0L,await functionAcl.ExecuteScalarAsync(ct));
        var stale = await Assert.ThrowsAsync<PostgresException>(() => Export("export",Guid.NewGuid()));
        Assert.Equal("WG409",stale.SqlState);
        await store.UpdateAsync(id,2,WebAccessGroupScope.AllServers,Array.Empty<int>(),false,"test",ct);
        var disabled = await Assert.ThrowsAsync<PostgresException>(() => Export("export",generation));
        Assert.Equal("WG400",disabled.SqlState);
    }
}
