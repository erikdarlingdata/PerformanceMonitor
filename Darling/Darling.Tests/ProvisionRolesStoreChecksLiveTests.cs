/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8779: the store checks of <c>--prepare-provision-roles</c> against a real migrated store owned by the non-superuser
/// <c>prov_owner</c> (the setup of <see cref="ProvisionRolesOwnerRunLiveTests"/>). Gated on <c>DARLING_TEST_PGRUNTIME</c>.
/// </summary>
[Collection("darling-config-env")]
public sealed class ProvisionRolesStoreChecksLiveTests
{
    [Fact]
    public async Task ReadStoredSchemaVersion_IsCompiledVersion_AfterMigration()
    {
        await ProvisionRolesOwnerRunLiveTests.RunInOwnStoreAsync(createRole: true, async (_, _, _, ownerLogin, ct) =>
        {
            await using var connection = await ComposeStoreRolesLiveTests.OpenAsync(ownerLogin, ct);
            Assert.Equal(StorageVersion.SchemaVersion, await ProvisionRolesStoreChecks.ReadStoredSchemaVersionAsync(connection, ct));
        });
    }

    [Fact]
    public async Task ReadStoredSchemaVersion_IsNull_OnAnEmptyDatabase()
    {
        await ProvisionRolesOwnerRunLiveTests.RunInOwnStoreAsync(createRole: true, async (_, _, superuser, ownerLogin, ct) =>
        {
            await ComposeStoreRolesLiveTests.ExecAsync(superuser, "CREATE DATABASE prov_empty OWNER prov_owner", ct);
            var empty = new NpgsqlConnectionStringBuilder(ownerLogin) { Database = "prov_empty" }.ConnectionString;
            await using var connection = await ComposeStoreRolesLiveTests.OpenAsync(empty, ct);
            Assert.Null(await ProvisionRolesStoreChecks.ReadStoredSchemaVersionAsync(connection, ct));
        });
    }

    [Fact]
    public async Task ReadForeignOwnedObjects_IsEmpty_ForTheOwner()
    {
        await ProvisionRolesOwnerRunLiveTests.RunInOwnStoreAsync(createRole: true, async (_, _, _, ownerLogin, ct) =>
        {
            await using var connection = await ComposeStoreRolesLiveTests.OpenAsync(ownerLogin, ct);
            Assert.Empty(await ProvisionRolesStoreChecks.ReadForeignOwnedObjectsAsync(connection, ct));
        });
    }

    [Fact]
    public async Task ReadForeignOwnedObjects_NamesAFunctionASuperuserReowned()
    {
        await ProvisionRolesOwnerRunLiveTests.RunInOwnStoreAsync(createRole: true, async (_, _, superuser, ownerLogin, ct) =>
        {
            var signature = await ComposeStoreRolesLiveTests.OwnerScalarAsync<string>(superuser,
                "SELECT p.oid::regprocedure::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'config' AND p.prokind = 'f' ORDER BY p.oid LIMIT 1", ct);
            var superName = await ComposeStoreRolesLiveTests.OwnerScalarAsync<string>(superuser, "SELECT current_user::text", ct);
            await ComposeStoreRolesLiveTests.ExecAsync(superuser, $"ALTER FUNCTION {signature} OWNER TO \"{superName}\"", ct);
            await using var connection = await ComposeStoreRolesLiveTests.OpenAsync(ownerLogin, ct);

            var foreign = await ProvisionRolesStoreChecks.ReadForeignOwnedObjectsAsync(connection, ct);

            var item = Assert.Single(foreign);
            Assert.Equal("function", item.Kind);
            Assert.Equal("config", item.Schema);
            Assert.Equal(superName, item.Owner);
            Assert.Equal(signature.StartsWith("config.", StringComparison.Ordinal) ? signature["config.".Length..] : signature, item.Name);
            var text = ProvisionRolesStoreChecks.DescribeOwnership("prov_owner", foreign)!;
            Assert.Contains("OWNER TO \"prov_owner\";", text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ReadForeignOwnedObjects_SkipsLinkedSequences_AndTheListedFixRuns()
    {
        await ProvisionRolesOwnerRunLiveTests.RunInOwnStoreAsync(createRole: true, async (_, _, superuser, ownerLogin, ct) =>
        {
            await ComposeStoreRolesLiveTests.ExecAsync(superuser, "CREATE TABLE config.prov_identity_8779 (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, note text)", ct);
            await using var connection = await ComposeStoreRolesLiveTests.OpenAsync(ownerLogin, ct);

            var foreign = await ProvisionRolesStoreChecks.ReadForeignOwnedObjectsAsync(connection, ct);

            var item = Assert.Single(foreign);
            Assert.Equal("table", item.Kind);
            Assert.Equal("prov_identity_8779", item.Name);
            var text = ProvisionRolesStoreChecks.DescribeOwnership("prov_owner", foreign)!;
            var alters = text.Split((char)10, StringSplitOptions.RemoveEmptyEntries).Where(line => line.StartsWith("ALTER ", StringComparison.Ordinal)).ToList();
            Assert.Single(alters);
            foreach (var alter in alters)
            {
                await ComposeStoreRolesLiveTests.ExecAsync(superuser, alter, ct);
            }

            Assert.Empty(await ProvisionRolesStoreChecks.ReadForeignOwnedObjectsAsync(connection, ct));
        });
    }
}
