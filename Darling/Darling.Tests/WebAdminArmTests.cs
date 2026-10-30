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
    [InlineData(true, null, false, true, "list-only")]
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
        Assert.Contains("Set it to false to keep the dashboard list-only", line, StringComparison.Ordinal);

        var off = new ListLogger();
        Assert.Null(WebAdminArm.ForHost(new PostgresConfig { Managed = true }, false, off));
        Assert.DoesNotContain(off.Lines, l => l.Contains("are on", StringComparison.Ordinal));
        Assert.Contains("are off", Assert.Single(off.Lines), StringComparison.Ordinal);

        var byo = new ListLogger();
        Assert.Null(WebAdminArm.ForHost(new PostgresConfig { Managed = false }, true, byo));
        Assert.Empty(byo.Lines);
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
    public void TheRefusalSentence_NamesTheSwitch_OnlyOnTheManagedStoreWithTheSwitchOff()
    {
        Assert.Contains("web.allowAdminWrites", DarlingWebStoreWrites.Refused("record feedback", true, adminWritesOff: true), StringComparison.Ordinal);
        Assert.DoesNotContain("web.allowAdminWrites", DarlingWebStoreWrites.Refused("record feedback", true), StringComparison.Ordinal);
        Assert.DoesNotContain("web.allowAdminWrites", DarlingWebStoreWrites.Refused("record feedback", false, adminWritesOff: true), StringComparison.Ordinal);
        Assert.True(DarlingWebStoreWrites.AdminWritesOff(true, null));
        Assert.False(DarlingWebStoreWrites.AdminWritesOff(false, null));
    }
}
