/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

public sealed class ViewerWebTokenTests
{
    [Fact]
    public void OnlyTheCurrentEnabledActiveGenerationCanBeShown()
    {
        var token = new ViewerWebToken(Guid.NewGuid(), Guid.NewGuid(), "never-log-this-token");
        var group = new WebAccessGroup(token.GroupId, "Finance", WebAccessGroupScope.Tags, true,
            token.Generation, 2, "active", null, new[]{1});
        Assert.True(token.CanReveal(group));
        Assert.False(token.CanReveal(null));
        Assert.False(token.CanReveal(group with { Enabled = false }));
        Assert.False(token.CanReveal(group with { ActivationState = "pending" }));
        Assert.False(token.CanReveal(group with { ActivationState = "refused" }));
        Assert.False(token.CanReveal(group with { GroupId = Guid.NewGuid() }));
        Assert.False(token.CanReveal(group with { TokenGeneration = Guid.NewGuid() }));
        Assert.DoesNotContain(token.Plain, token.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void FinalRefreshMustStillMatchBeforeTheTokenFieldIsPopulated()
    {
        StaTestThread.Run(async () =>
        {
            await using var data = new ViewerDataService("Host=127.0.0.1;Port=1;Username=unused;Database=unused");
            var window = new WebAccessGroupsWindow(data);
            var token = new ViewerWebToken(Guid.NewGuid(), Guid.NewGuid(), "one-time-test-value");
            var active = new WebAccessGroup(token.GroupId, "Finance", WebAccessGroupScope.Tags, true,
                token.Generation, 2, "active", null, new[]{1});
            var box = (TextBox)window.FindName("TokenBox");
            var panel = (StackPanel)window.FindName("TokenPanel");
            foreach (var changed in new WebAccessGroup?[] { null, active with { Enabled=false },
                active with { ActivationState="pending" }, active with { ActivationState="refused" },
                active with { TokenGeneration=Guid.NewGuid() }, active with { GroupId=Guid.NewGuid() } })
            {
                Assert.True(window.TryShowToken(token, active));
                Assert.Equal(token.Plain, box.Text);
                Assert.False(window.TryShowToken(token, changed));
                Assert.Equal("", box.Text);
                Assert.Equal(Visibility.Collapsed, panel.Visibility);
            }
            window.Close();
        });
    }
    [Fact]
    public void WindowStartsWithNoAllServersGrantAndClearsTheOneTimeValueOnClose()
    {
        StaTestThread.Run(async () =>
        {
            await using var data = new ViewerDataService("Host=127.0.0.1;Port=1;Username=unused;Database=unused");
            var window = new WebAccessGroupsWindow(data);
            Assert.True(((RadioButton)window.FindName("TagsRadio")).IsChecked);
            Assert.False(((RadioButton)window.FindName("AllRadio")).IsChecked);
            var box = (TextBox)window.FindName("TokenBox");
            var panel = (StackPanel)window.FindName("TokenPanel");
            Assert.Equal(Visibility.Collapsed, panel.Visibility);
            box.Text = "only-in-memory";
            panel.Visibility = Visibility.Visible;
            window.Close();
            Assert.Equal("", box.Text);
            Assert.Equal(Visibility.Collapsed, panel.Visibility);
        });
    }
}

[Collection("pg-cluster-roles")]
public sealed class ViewerWebAccessGroupsLiveTests
{
    [Fact]
    public async Task AdminCanMaintainSealedGroupsButOtherSeatsCannotManageThem()
    {
        var baseConnection = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnection), "Set DARLING_TEST_PG for Viewer group tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnection!, ct);
        var settings = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var owner = NpgsqlDataSource.Create(settings.ConnectionString);
        var rolesCreated = false;
        try
        {
            rolesCreated = true;
            await Exec(DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                target: ProvisioningTarget.ComposeStore(settings.Username!, scratch.DatabaseName),
                webVerifier: ProvisioningTestSecrets.Web));
            using var key = PasswordPrivateKey.Generate();
            var sealer = new ViewerPasswordSealer(key.PublicKey);
            await using var admin = Seat("admin", ProvisioningTestSecrets.AdminPassword);
            Assert.False(await admin.DetectReadOnlyAsync(ct));
            Assert.True(await admin.CanManageWebAccessGroupsAsync(ct));
            var token = await admin.CreateWebAccessGroupAsync("Finance", WebAccessGroupScope.AllServers, Array.Empty<int>(), sealer, ct);
            var store = new WebAccessGroupStore(owner);
            var group = (await admin.GetWebAccessGroupAsync(token.GroupId, ct))!;
            Assert.False(token.CanReveal(group));
            var credential = Assert.Single(await store.ReadCredentialsAsync(ct));
            Assert.NotEqual(token.Plain, credential.SealedToken);
            Assert.Equal(token.Plain, PasswordSeal.Open(credential.SealedToken, key,
                PasswordBinding.ForWebAccessToken(token.GroupId, token.Generation)));
            Assert.True(await store.ActivateAsync(group.GroupId, group.Revision, group.TokenGeneration, null, ct));
            group = (await admin.GetWebAccessGroupAsync(token.GroupId, ct))!;
            Assert.True(token.CanReveal(group));
            Assert.Equal(43, token.Plain.Length);
            var other = await admin.CreateWebAccessGroupAsync("Operations", WebAccessGroupScope.AllServers, Array.Empty<int>(), sealer, ct);
            Assert.NotEqual(token.Plain, other.Plain);
            await admin.UpdateWebAccessGroupAsync(group, WebAccessGroupScope.Tags, Array.Empty<int>(), true, ct);
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                admin.UpdateWebAccessGroupAsync(group, WebAccessGroupScope.AllServers, Array.Empty<int>(), true, ct));
            group = (await admin.GetWebAccessGroupAsync(group.GroupId, ct))!;
            Assert.Equal(WebAccessGroupScope.Tags, group.Scope);
            Assert.Empty(group.TagIds);
            var rotated = await admin.RotateWebAccessGroupTokenAsync(group, sealer, ct);
            group = (await admin.GetWebAccessGroupAsync(group.GroupId, ct))!;
            Assert.False(token.CanReveal(group));
            Assert.False(rotated.CanReveal(group));
            Assert.NotEqual(token.Generation, rotated.Generation);
            Assert.True(await store.ActivateAsync(group.GroupId, group.Revision, group.TokenGeneration, null, ct));
            group = (await admin.GetWebAccessGroupAsync(group.GroupId, ct))!;
            Assert.True(rotated.CanReveal(group));

            foreach (var (role, password) in new[]{
                ("viewer",ProvisioningTestSecrets.ViewerPassword), ("web",ProvisioningTestSecrets.WebPassword),
                ("mcp",ProvisioningTestSecrets.McpPassword)})
            {
                await using var seat = Seat(role,password);
                // Probe is checked even without DetectReadOnlyAsync; generic write permission is insufficient.
                Assert.False(await seat.CanManageWebAccessGroupsAsync(ct));
                await Assert.ThrowsAsync<InvalidOperationException>(() => seat.GetWebAccessGroupsAsync(ct));
                await Assert.ThrowsAsync<InvalidOperationException>(() => seat.CreateWebAccessGroupAsync("No",WebAccessGroupScope.AllServers,Array.Empty<int>(),sealer,ct));
                await Assert.ThrowsAsync<InvalidOperationException>(() => seat.UpdateWebAccessGroupAsync(group,WebAccessGroupScope.AllServers,Array.Empty<int>(),true,ct));
                await Assert.ThrowsAsync<InvalidOperationException>(() => seat.RotateWebAccessGroupTokenAsync(group,sealer,ct));
                await Assert.ThrowsAsync<InvalidOperationException>(() => seat.DeleteWebAccessGroupAsync(group,ct));
            }
            await admin.UpdateWebAccessGroupAsync(group,group.Scope,Array.Empty<int>(),false,ct);
            group = (await admin.GetWebAccessGroupAsync(group.GroupId,ct))!;
            Assert.False(rotated.CanReveal(group));
            await admin.DeleteWebAccessGroupAsync(group,ct);
            Assert.Null(await admin.GetWebAccessGroupAsync(group.GroupId,ct));
            Assert.Single(await admin.GetWebAccessGroupsAsync(ct));
        }
        finally
        {
            if (rolesCreated)
            {
                NpgsqlConnection.ClearAllPools();
                await Exec(LiveCleanupBatch.DropOwnedByExistingProductRolesSql + " DROP ROLE IF EXISTS admin,viewer,web,mcp", CancellationToken.None);
            }
        }
        ViewerDataService Seat(string role,string password) => new(new NpgsqlConnectionStringBuilder(settings.ConnectionString) { Username=role, Password=password }.ConnectionString);
        async Task Exec(string sql,CancellationToken token = default)
        { await using var cmd = owner.CreateCommand(sql); await cmd.ExecuteNonQueryAsync(token); }
    }
}
