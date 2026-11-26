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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using Edit = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/* #9693: the parts of --edit-server and edit_server that only a real store shows: a write that changes no row, a name several
   registrations answer to, and the start-up drift line with the configuration path in use. Each fact mints its own scratch
   database through the rig the other facts of this class use. */
public sealed partial class ServerEditLiveTests
{
    /// <summary>The row vanished between the read and the write: the enable write changes no row, the answer is
    /// <c>not_found</c> and never "is now enabled", nothing else in the registry moves, and the verb exits 1.</summary>
    [Fact]
    public async Task AnEnableWriteForARowRemovedSinceItWasRead_ChangesNoRow_AnswersNotFound_AndTheVerbExitsOne()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            await SeedServerAsync(rig.Owner, 5301, "gone-01", "gone-01.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 5302, "beta-01", "beta-01.example.test", "integrated", null, null, ct);

            /* The row as the enabled step reads it (disabled), then the server is removed before the write. */
            var read = (await DarlingMcpCollectionStateTools.ReadServersAsync(rig.Owner, ct)).Single(s => s.ServerId == 5301);
            Assert.False(read.Enabled);
            await ExecAsync(rig.Owner, "DELETE FROM config_monitored_servers WHERE server_id = 5301", ct);
            var staysBefore = await RowSignatureAsync(rig.Owner, 5302, ct);

            var answer = Parse(await DarlingMcpCollectionStateTools.ApplyEnabledToAsync(rig.Owner, "enable_server", read, enable: true, null, ct));

            Assert.Equal("refused", answer["status"]!.GetValue<string>());
            Assert.Equal("not_found", answer["reason"]!.GetValue<string>());
            Assert.Null(answer["what_changed"]);
            Assert.Equal(0L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers WHERE server_id = 5301", ct));
            Assert.Equal(staysBefore, await RowSignatureAsync(rig.Owner, 5302, ct));
            /* The reload beacon is not asserted: its trigger runs once for each statement on the table, one that matches no row included. */

            /* The same write as the verb's enabled step: one line that says nothing was enabled, and exit code 1. */
            Assert.True(DarlingCliCommands.TryParseEditServerArgs(["gone-01", "--set", "enabled=true"], out var request, out var argError), argError);
            var (lines, exitCode) = await DarlingCliCommands.RunEditServerAsync(
                request!,
                _ => throw new InvalidOperationException("no setting was asked for"),
                (_, enable) => DarlingMcpCollectionStateTools.ApplyEnabledToAsync(rig.Owner, "enable_server", read, enable, null, ct),
                _ => throw new InvalidOperationException("no password is needed"),
                new DarlingCliCommands.EditServerTarget(5301, read.StorageName, read.Name, "gone-01.example.test", 0, "Windows", null, ""));

            Assert.Equal(1, exitCode);
            var line = Assert.Single(lines);
            Assert.StartsWith("  enabled was not changed: ", line, StringComparison.Ordinal);
            Assert.Equal(answer["message"]!.GetValue<string>(), line["  enabled was not changed: ".Length..]);

            /* By id, the way the verb calls it, the server that is gone is not found either, and the one that stays is enabled. */
            var byId = Parse(await DarlingMcpCollectionStateTools.SetServerEnabledByIdAsync(rig.Owner, 5301, true, null, ct));
            Assert.Equal("not_found", byId["reason"]!.GetValue<string>());
            var stays = Parse(await DarlingMcpCollectionStateTools.SetServerEnabledByIdAsync(rig.Owner, 5302, true, null, ct));
            Assert.Equal("changed", stays["status"]!.GetValue<string>());
            Assert.True(await ScalarAsync<bool>(rig.Owner, "SELECT is_enabled FROM config_monitored_servers WHERE server_id = 5302", ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>edit_server end to end for a name two registrations answer to (one by its display name, one by its storage
    /// name): the answer is <c>ambiguous</c> with each candidate's id, nothing is written, and the <c>id:</c> form of one
    /// candidate edits that one and no other.</summary>
    [Fact]
    public async Task AsTheMcpRole_ANameTwoRegistrationsAnswerTo_IsAmbiguousWithEachId_AndTheIdFormEditsOne()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            /* "alpha-01" is the first server's display name and the second server's host, which is its storage name. */
            await SeedServerAsync(rig.Owner, 5311, "alpha-01", "gamma-01.example.test", "integrated", null, null, ct);
            await SeedServerAsync(rig.Owner, 5312, "zeta-01", "alpha-01", "integrated", null, null, ct);
            var firstBefore = await RowSignatureAsync(rig.Owner, 5311, ct);
            var secondBefore = await RowSignatureAsync(rig.Owner, 5312, ct);
            var versionBefore = await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct);

            var refused = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "alpha-01", "{\"monthly_cost_usd\":120}", Reachable, TestKeyRings.Healthy, null, ct));

            Assert.Equal("ambiguous", refused["status"]!.GetValue<string>());
            var candidates = refused["candidates"]!.AsArray().Select(c => c!["server_id"]!.GetValue<int>()).OrderBy(id => id).ToArray();
            Assert.Equal([5311, 5312], candidates);
            Assert.Equal(firstBefore, await RowSignatureAsync(rig.Owner, 5311, ct));
            Assert.Equal(secondBefore, await RowSignatureAsync(rig.Owner, 5312, ct));
            Assert.Equal(versionBefore, await ScalarAsync<long>(rig.Owner, "SELECT config_version FROM config_service", ct));

            var edited = Parse(await Edit.EditServerByNameAsync(rig.Mcp, "id:5312", "{\"monthly_cost_usd\":120}", Reachable, TestKeyRings.Healthy, null, ct));

            Assert.Equal("updated", edited["status"]!.GetValue<string>());
            Assert.Equal(120L, await ScalarAsync<long>(rig.Owner, "SELECT monthly_cost_usd::bigint FROM config_monitored_servers WHERE server_id = 5312", ct));
            Assert.Equal(firstBefore, await RowSignatureAsync(rig.Owner, 5311, ct));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }

    /// <summary>The start-up drift line the service logs for a registered server whose darling.json entry differs names the
    /// configuration file the service is using: the printed command ends with it as one quoted word, or, on a host where no
    /// command can be printed, the by-hand sentence ends with it in quotes.</summary>
    [Fact]
    public async Task TheStartupDriftLine_NamesTheConfigurationFileTheServiceIsUsing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        var ok = false;
        try
        {
            /* darling.json as it is parsed, with one Windows-authentication server. */
            static DarlingConfig File(bool trust) => DarlingConfig.Parse(System.Text.Json.JsonSerializer.Serialize(new
            {
                servers = new[] { new { name = "epsilon-01", host = "epsilon-01.example.test", auth = "integrated", trustServerCertificate = trust } },
            }));
            await new StoreConfigProvider(rig.Owner).SeedIfEmptyAsync(File(trust: false), ct);
            Assert.Equal(1L, await ScalarAsync<long>(rig.Owner, "SELECT count(*) FROM config_monitored_servers", ct));

            /* The file's entry is edited after the first start, and the service starts again. */
            var logger = new CapturingTestLogger();
            await new StoreConfigProvider(rig.Owner, logger).SeedIfEmptyAsync(File(trust: true), ct);

            var path = StoreConfigProvider.ConfigPathInUse();
            Assert.NotNull(path);
            var word = StoreConfigProvider.CommandWord(path, OperatingSystem.IsWindows());
            var expectedEnd = word is not null && OperatorVerbForm.PasteableEntryPointHere() is not null
                ? " --config " + word
                : $"({StoreConfigProvider.DriftValue(path)}).";
            Assert.Contains(logger.Lines, l =>
                l.Contains("\"epsilon-01\"", StringComparison.Ordinal)
                && l.Contains("trustServerCertificate", StringComparison.Ordinal)
                && l.EndsWith(expectedEnd, StringComparison.Ordinal));
            ok = true;
        }
        finally
        {
            await DropRoleAsync(rig, ok);
        }
    }
}
