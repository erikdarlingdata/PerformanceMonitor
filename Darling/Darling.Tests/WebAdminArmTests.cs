using System;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>#6600: which store login a web Admin write and its access check use on the managed store. No server is needed: a data source is
/// built without connecting, so the matrix is read from the source each seat gets.</summary>
public sealed class WebAdminArmTests
{
    private const string ArmConnection = "Host=127.0.0.1;Port=1;Username=admin;Password=x;Database=darling;Pooling=false";

    private static NpgsqlDataSource Web() => NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=viewer;Password=x;Database=darling");

    private static HttpContext ContextFor(DarlingWebSeat? seat)
    {
        var context = new DefaultHttpContext();
        if (seat is not null)
        {
            context.Items[DarlingWebSeat.HttpContextItemKey] = seat;
        }

        return context;
    }

    [Theory]
    [InlineData("shared token admin", true, true)]
    [InlineData("OIDC admin", true, true)]
    [InlineData("OIDC viewer", false, false)]
    [InlineData("reads only", false, false)]
    public void SourceFor_GivesTheArm_OnlyToASignedInAdminSeat(string label, bool canEdit, bool getsArm)
    {
        using var web = Web();
        using var arm = new WebAdminArm(() => ArmConnection);
        var seat = label switch
        {
            "shared token admin" => DarlingWebSeat.SharedToken,
            "OIDC admin" => new DarlingWebSeat("alice", canEdit, "owner-key"),
            "OIDC viewer" => new DarlingWebSeat("bob", canEdit, "owner-key"),
            _ => DarlingWebSeat.ReadsOnly,
        };

        var source = WebAdminArm.SourceFor(arm, ContextFor(seat), web);

        Assert.Equal(getsArm, !ReferenceEquals(source, web));
        Assert.Equal(getsArm, ReferenceEquals(source, arm.Source));
        if (getsArm)
        {
            Assert.Contains("PerformanceMonitorDarling-WebAdmin", source.ConnectionString, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void SourceFor_GivesTheWebSource_WhenNoSeatIsStored_OrTheArmIsNullOrUnavailable()
    {
        using var web = Web();
        using var arm = new WebAdminArm(() => ArmConnection);
        using var unavailable = new WebAdminArm(() => null);

        Assert.Same(web, WebAdminArm.SourceFor(arm, ContextFor(null), web));
        Assert.Same(web, WebAdminArm.SourceFor(null, ContextFor(DarlingWebSeat.SharedToken), web));
        Assert.Same(web, WebAdminArm.SourceFor(unavailable, ContextFor(DarlingWebSeat.SharedToken), web));
    }

    [Fact]
    public void TheArmBuildsItsSourceOnFirstUse_RetriesWhileTheCredentialIsMissing_AndKeepsTheSourceOnceBuilt()
    {
        var available = false;
        using var arm = new WebAdminArm(() => available ? ArmConnection : null);

        Assert.Null(arm.Source);
        Assert.Null(arm.Source);
        available = true;
        var built = arm.Source;
        Assert.NotNull(built);
        available = false;
        Assert.Same(built, arm.Source);

        arm.Dispose();
        Assert.Null(arm.Source);
    }

    [Fact]
    public void TheSwitchDefaultsToOff_AMissingKeyKeepsItOff_AndTheArmExistsOnlyOnTheManagedStoreWithTheSwitchOn()
    {
        // An upgrade from 3.11 has no key: it must keep 3.11's web scope (no admin pool) until the operator sets it.
        Assert.False(new WebConfig().AllowAdminWrites);
        Assert.False(System.Text.Json.JsonSerializer.Deserialize<WebConfig>("{}")!.AllowAdminWrites);
        Assert.False(DarlingConfig.Parse("{\"web\": {\"enabled\": true}}").Web.AllowAdminWrites);
        Assert.False(System.Text.Json.JsonSerializer.Deserialize<WebConfig>("{\"allowAdminWrites\": false}")!.AllowAdminWrites);
        Assert.True(System.Text.Json.JsonSerializer.Deserialize<WebConfig>("{\"allowAdminWrites\": true}")!.AllowAdminWrites);
        Assert.True(DarlingConfig.Parse("{\"web\": {\"allowAdminWrites\": true}}").Web.AllowAdminWrites);

        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        using var managedOn = WebAdminArm.ForHost(new PostgresConfig { Managed = true }, true, NullLogger.Instance);
        Assert.NotNull(managedOn);
        Assert.Null(WebAdminArm.ForHost(new PostgresConfig { Managed = true }, false, NullLogger.Instance));
        Assert.Null(WebAdminArm.ForHost(new PostgresConfig { Managed = false }, true, NullLogger.Instance));
    }

    [Fact]
    public void TheFilesWrittenForANewInstall_SetTheSwitchOnExplicitly()
    {
        var sample = DarlingConfig.Parse(System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "darling.sample.json")));
        Assert.True(sample.Web.AllowAdminWrites);
    }

    private static DarlingWebOidcClient OidcClient(string? roleClaim) =>
        new(new DarlingWebOidcClient.ResolvedOptions(
            "https://idp.example.test", "client-id", null, "openid", null, roleClaim,
            roleClaim is null ? Array.Empty<string>() : new[] { "admins" }, Array.Empty<string>()));

    [Theory]
    [InlineData(true, null, true, true, "Admin writes")]
    [InlineData(true, null, false, true, "monitored servers")]
    [InlineData(false, null, false, true, "depends on the web login's grants")]
    [InlineData(true, "groups", true, false, null)]
    [InlineData(false, "groups", false, false, null)]
    public void TheOidcWarning_FiresForOidcWithNoRoleMapping_OnEveryStoreKind_AndNotWhenARoleMappingExists(
        bool managedStore, string? roleClaim, bool armBuilt, bool warns, string? effect)
    {
        using var oidc = OidcClient(roleClaim);
        using var arm = armBuilt ? new WebAdminArm(() => null) : null;
        var log = new ListLogger();
        Assert.Equal(warns, WebAdminArm.WarnWhenOidcHasNoRoleMapping(oidc, managedStore, arm, log));
        if (!warns)
        {
            Assert.Empty(log.Lines);
            return;
        }

        var line = Assert.Single(log.Lines);
        Assert.Contains("no role mapping", line, StringComparison.Ordinal);
        Assert.Contains(effect!, line, StringComparison.Ordinal);
        Assert.Contains("web.network.oidc.roleClaim", line, StringComparison.Ordinal);
    }

    [Fact]
    public void TheOidcWarning_StaysQuietWithNoOidcSignIn()
    {
        var log = new ListLogger();
        Assert.False(WebAdminArm.WarnWhenOidcHasNoRoleMapping(null, true, new WebAdminArm(() => null), log));
        Assert.Empty(log.Lines);
    }

    [Fact]
    public void ForHost_LogsTheOnLineWhenTheArmIsBuilt_AndTheOffLineInsteadWhenItIsOff()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        var on = new ListLogger();
        using (WebAdminArm.ForHost(new PostgresConfig { Managed = true }, true, on))
        {
        }

        var line = Assert.Single(on.Lines);
        Assert.Contains("Admin writes are on (web.allowAdminWrites)", line, StringComparison.Ordinal);
        // #8609: false turns off the Admin writes alone, so the line names the writes a sign-in that can edit keeps and never says list-only.
        Assert.DoesNotContain("list-only", line, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("monitored servers", line, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("mute rules", line, StringComparison.OrdinalIgnoreCase);

        var off = new ListLogger();
        Assert.Null(WebAdminArm.ForHost(new PostgresConfig { Managed = true }, false, off));
        Assert.DoesNotContain(off.Lines, l => l.Contains("are on", StringComparison.Ordinal));
        Assert.Contains("are off", Assert.Single(off.Lines), StringComparison.Ordinal);

        var byo = new ListLogger();
        Assert.Null(WebAdminArm.ForHost(new PostgresConfig { Managed = false }, true, byo));
        Assert.Empty(byo.Lines);
    }

    /// <summary>#8609: with the switch off on the managed store a sign-in that can edit still adds and edits monitored servers and writes mute
    /// rules, dismissals and custom alert rules, through the web login's own grants. The start lines are what an operator reads to learn
    /// what the dashboard can change, so none of them says list-only, each names those writes, and each names an Admin write the switch
    /// does turn off. With OIDC and no role mapping every signed-in user can edit, and the warning says each one can add a server.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("groups")]
    public void WithTheSwitchOff_TheStartLinesNameTheWritesAnEditSignInKeeps_AndNeverSayListOnly(string? roleClaim)
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        var log = new ListLogger();
        var arm = WebAdminArm.ForHost(new PostgresConfig { Managed = true }, false, log);
        Assert.Null(arm);
        using var oidc = OidcClient(roleClaim);
        var warned = WebAdminArm.WarnWhenOidcHasNoRoleMapping(oidc, managedStore: true, arm, log);

        Assert.Equal(roleClaim is null, warned);
        Assert.Equal(warned ? 2 : 1, log.Lines.Count);
        Assert.All(log.Lines, line =>
        {
            Assert.DoesNotContain("list-only", line, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("lists only", line, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("web.allowAdminWrites", line, StringComparison.Ordinal);
            Assert.Contains("threshold overrides", line, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("monitored servers", line, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("mute rules", line, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("dismissals", line, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("custom alert", line, StringComparison.OrdinalIgnoreCase);
        });
        Assert.Contains("are off", log.Lines[0], StringComparison.Ordinal);

        if (warned)
        {
            Assert.Contains("every user who signs in", log.Lines[1], StringComparison.Ordinal);
            Assert.Contains("add", log.Lines[1], StringComparison.OrdinalIgnoreCase);
        }
    }

    private sealed class ListLogger : Microsoft.Extensions.Logging.ILogger
    {
        public System.Collections.Generic.List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter) => Lines.Add(formatter(state, exception));
    }

    [Fact]
    public void TheManagedRefusal_ClaimsNoSwitch_WhenTheSwitchIsOn_AndSaysTurnedOffOnlyWhenItIsOff()
    {
        // Switch on: the refusal is about privilege (no admin credential yet, or the store refused the web login), so nothing was turned off.
        var privilege = DarlingWebStoreWrites.Refused("press a proposal", managedStore: true, adminWritesOff: false);
        Assert.DoesNotContain("turned off", privilege, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("web.allowAdminWrites", privilege, StringComparison.Ordinal);
        Assert.Equal("This dashboard's store login has no write access, so it cannot press a proposal. Use the Viewer app to make the change.", privilege);

        // Switch off: the switch is the cause, so the sentence says so and names it.
        var off = DarlingWebStoreWrites.Refused("press a proposal", managedStore: true, adminWritesOff: true);
        Assert.StartsWith("Changes from this page are turned off", off, StringComparison.Ordinal);
        Assert.Contains("web.allowAdminWrites", off, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRefusalSentence_NamesTheSwitch_OnlyOnTheManagedStoreWithTheSwitchOff()
    {
        Assert.Contains("web.allowAdminWrites", DarlingWebStoreWrites.Refused("record feedback", true, adminWritesOff: true), StringComparison.Ordinal);
        Assert.DoesNotContain("web.allowAdminWrites", DarlingWebStoreWrites.Refused("record feedback", true), StringComparison.Ordinal);
        Assert.DoesNotContain("web.allowAdminWrites", DarlingWebStoreWrites.Refused("record feedback", false, adminWritesOff: true), StringComparison.Ordinal);
        Assert.True(DarlingWebStoreWrites.AdminWritesOff(true, null));
        Assert.False(DarlingWebStoreWrites.AdminWritesOff(false, null));
    }
}
