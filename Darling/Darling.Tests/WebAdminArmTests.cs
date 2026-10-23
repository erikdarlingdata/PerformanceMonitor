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
    public void TheSwitchDefaultsToOn_AndTheArmExistsOnlyOnTheManagedStoreWithTheSwitchOn()
    {
        Assert.True(new WebConfig().AllowAdminWrites);
        Assert.True(System.Text.Json.JsonSerializer.Deserialize<WebConfig>("{}")!.AllowAdminWrites);
        Assert.False(System.Text.Json.JsonSerializer.Deserialize<WebConfig>("{\"allowAdminWrites\": false}")!.AllowAdminWrites);

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
    public void TheRefusalSentence_NamesTheSwitch_OnlyOnTheManagedStoreWithTheSwitchOff()
    {
        Assert.Contains("web.allowAdminWrites", DarlingWebStoreWrites.Refused("record feedback", true, adminWritesOff: true), StringComparison.Ordinal);
        Assert.DoesNotContain("web.allowAdminWrites", DarlingWebStoreWrites.Refused("record feedback", true), StringComparison.Ordinal);
        Assert.DoesNotContain("web.allowAdminWrites", DarlingWebStoreWrites.Refused("record feedback", false, adminWritesOff: true), StringComparison.Ordinal);
        Assert.True(DarlingWebStoreWrites.AdminWritesOff(true, null));
        Assert.False(DarlingWebStoreWrites.AdminWritesOff(false, null));
    }
}
