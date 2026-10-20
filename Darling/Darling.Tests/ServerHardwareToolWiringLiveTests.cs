// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6179: get_server_properties and get_memory_stats name the server by its registry display name, and build their payload
/// through the engine-scoped builders. This drives the tools against a store, one Azure SQL Database (engine edition 5) and
/// one box server, and checks what comes back: the host's hardware figures are withheld on the Azure database only, the
/// memory note rides on it only, and the <c>server</c> field is the display name, not the storage key.
/// </summary>
[Collection("live-postgres")]
public sealed class ServerHardwareToolWiringLiveTests
{
    [Fact]
    public async Task ServerPropertiesAndMemoryStats_ScopeTheHostByEngine_AndNameTheServerByDisplayName_AgainstDevPostgres()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the hardware tool wiring live test.");
        var ct = TestContext.Current.CancellationToken;

        var suffix = Guid.NewGuid().ToString("N");
        var azure = (Key: "hwwire_az_" + suffix, Display: "Azure display " + suffix, Edition: 5, EditionText: "SQL Azure");
        var box = (Key: "hwwire_box_" + suffix, Display: "Box display " + suffix, Edition: 3, EditionText: "Enterprise Edition (64-bit)");
        var servers = new[] { azure, box };

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var when = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-2));
            await using (var seed = new NpgsqlConnection(cs))
            {
                await seed.OpenAsync(ct);
                await PgMigrations.MigrateAsync(seed, ct);
                foreach (var s in servers)
                {
                    var id = ServerIdHelper.GetDeterministicHashCode(s.Key);
                    await DarlingMcpTestData.ExecAsync(seed, ct,
                        @"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, sql_engine_edition, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 12, $4, $5, $5)", id, s.Key, s.Display, s.Edition, when);
                    await DarlingMcpTestData.ExecAsync(seed, ct,
                        @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
    cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, is_hadr_enabled, is_clustered, service_objective, vcore_count)
VALUES ($1,$2,$3,$4,$5,'12.0.2000.8','RTM',$6,2,64,933888,0,32,false,false,$7,$8)",
                        CollectionIdGenerator.Next(), when, id, s.Key, s.EditionText, s.Edition,
                        s.Edition == 5 ? "GP_S_Gen5_1" : null, s.Edition == 5 ? (int?)1 : null);
                    await DarlingMcpTestData.ExecAsync(seed, ct,
                        @"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, plan_cache_mb, system_memory_state, sql_memory_model)
VALUES ($1,$2,$3,$4,65536,16384,49152,49152,40000,5000,'Available physical memory is high','CONVENTIONAL')",
                        CollectionIdGenerator.Next(), when, id, s.Key);
                }
            }

            string[] hostKeys = ["hyperthread_ratio", "socket_count", "cores_per_socket", "physical_memory_mb"];

            /* Azure SQL Database: the host's four figures are withheld, the vCores and a note come back, and the server is named by its display name. */
            using (var props = JsonDocument.Parse(await DarlingMcpDataTools.GetServerProperties(postgres, azure.Key, ct)))
            {
                var root = props.RootElement;
                Assert.Equal(azure.Display, root.GetProperty("server").GetString());
                foreach (var key in hostKeys)
                    Assert.Equal(JsonValueKind.Null, root.GetProperty(key).ValueKind);
                Assert.Equal(1, root.GetProperty("vcore_count").GetInt32());
                Assert.True(root.TryGetProperty("hardware_note", out _));
            }

            using (var mem = JsonDocument.Parse(await DarlingMcpDataTools.GetMemoryStats(postgres, azure.Key, ct)))
            {
                var root = mem.RootElement;
                Assert.Equal(azure.Display, root.GetProperty("server").GetString());
                Assert.Equal(5, root.GetProperty("engine_edition").GetInt32());
                Assert.Equal(JsonValueKind.Null, root.GetProperty("system_memory_state").ValueKind);
                Assert.True(root.TryGetProperty("memory_note", out _));
            }

            /* A box server keeps today's figures and gets no note, so the scoping is the engine's doing and not a blanket rule. */
            using (var props = JsonDocument.Parse(await DarlingMcpDataTools.GetServerProperties(postgres, box.Key, ct)))
            {
                var root = props.RootElement;
                Assert.Equal(box.Display, root.GetProperty("server").GetString());
                Assert.Equal(64, root.GetProperty("hyperthread_ratio").GetInt32());
                Assert.Equal(933888L, root.GetProperty("physical_memory_mb").GetInt64());
                Assert.False(root.TryGetProperty("hardware_note", out _));
            }

            using (var mem = JsonDocument.Parse(await DarlingMcpDataTools.GetMemoryStats(postgres, box.Key, ct)))
            {
                var root = mem.RootElement;
                Assert.Equal(box.Display, root.GetProperty("server").GetString());
                Assert.Equal("Available physical memory is high", root.GetProperty("system_memory_state").GetString());
                Assert.False(root.TryGetProperty("memory_note", out _));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var s in servers)
                {
                    var id = ServerIdHelper.GetDeterministicHashCode(s.Key);
                    foreach (var table in new[] { "memory_stats", "server_properties", "servers" })
                        await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, $"DELETE FROM collect.{table} WHERE server_id = $1", id);
                }
            });
        }
    }
}
