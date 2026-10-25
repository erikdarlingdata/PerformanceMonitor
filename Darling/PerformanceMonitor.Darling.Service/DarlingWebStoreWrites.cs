using System;
using System.Runtime.Versioning;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The one sentence builder for a web write the store refuses (#6445). The web host signs in to the store as the read-only <c>viewer</c> role
/// unless <c>postgres.webConnectionString</c> names another login, and the service already knows which kind of store it runs on. On the managed
/// store that setting is a validation error, so naming it (or "the admin login") sends the user to an option the store refuses. The sentence
/// therefore depends on <see cref="PostgresConfig.Managed"/>: a managed store says the dashboard lists only and the Viewer app makes the change,
/// a store the operator brought keeps the advice to give the web an admin store login. On the managed store with <c>web.allowAdminWrites</c>
/// off (#6600) the sentence also names that switch.
/// </summary>
internal static class DarlingWebStoreWrites
{
    /// <summary>Whether this config describes the managed store. A host with no config (a test that maps the routes directly) is not managed.</summary>
    internal static bool IsManaged(PostgresConfig? postgres) => postgres?.Managed == true;

    /// <summary>The 403 sentence for a write the web's store login cannot make. <paramref name="cannot"/> finishes "this dashboard cannot ..."
    /// (for example <c>record feedback</c>). <paramref name="instead"/> is the imperative that says where to do it, with no final period.
    /// <paramref name="bringYourOwnExtra"/> is appended on a store you bring yourself only. <paramref name="adminWritesOff"/> is true on the
    /// managed store when the dashboard has no admin pool (<c>web.allowAdminWrites</c> is off), and then the sentence names the switch.</summary>
    internal static string Refused(string cannot, bool managedStore, string instead = "Use the Viewer app to make the change",
        string? bringYourOwnExtra = null, bool adminWritesOff = false) =>
        managedStore
            ? $"This dashboard lists only on this store, so it cannot {cannot}. {instead}."
              + (adminWritesOff ? " To let a signed-in admin make it here, set web.allowAdminWrites to true in darling.json and restart the service." : "")
            : $"This dashboard's store login cannot {cannot}. {instead}, or give the web an admin store login "
              + "(set postgres.webConnectionString to an admin login)." + bringYourOwnExtra;

    /// <summary>Whether the managed store's dashboard has no admin pool, so its Admin writes are list-only until <c>web.allowAdminWrites</c> is on.</summary>
    internal static bool AdminWritesOff(bool managedStore, WebAdminArm? arm) => managedStore && arm is null;
}

/// <summary>
/// The web dashboard's second, small connection pool for its Admin writes on the managed store (#6600). The dashboard reads as the
/// <c>viewer</c> role there, which holds no write grant. This arm signs in as the managed <c>admin</c> role (the Viewer's login, which every
/// managed start already provisions with a stored credential file) and is used only for a request whose seat is a signed-in admin
/// (<see cref="DarlingWebSeat.IsSignedInAdmin"/>), for the write and for the access check that tells the page whether to show its controls.
/// Reads stay on the dashboard's own pool. It is built only on the managed store with <c>web.allowAdminWrites</c> on; a store the operator
/// brings never gets one. The data source is built on first use: while the credential file is missing the arm answers "not available" and
/// tries again on the next request, and once built it is kept. It never falls back to the owner connection.
/// </summary>
public sealed class WebAdminArm : IDisposable
{
    private readonly Func<string?> _connectionString;
    private readonly object _gate = new();
    private NpgsqlDataSource? _source;
    private bool _disposed;

    /// <param name="connectionString">The admin login's connection string, or null while its credential is not stored yet.</param>
    internal WebAdminArm(Func<string?> connectionString) => _connectionString = connectionString;

    /// <summary>The arm for the host's start: null unless the store is managed and <paramref name="allowAdminWrites"/> is on.</summary>
    [SupportedOSPlatform("windows")]
    internal static WebAdminArm? ForHost(PostgresConfig postgres, bool allowAdminWrites, ILogger logger)
    {
        if (!DarlingWebStoreWrites.IsManaged(postgres))
        {
            return null;
        }

        if (!allowAdminWrites)
        {
            logger.LogInformation("Web dashboard Admin writes are off (web.allowAdminWrites is false): it lists only on the managed store");
            return null;
        }

        logger.LogInformation("Web dashboard Admin writes are on (web.allowAdminWrites): a signed-in admin's Admin writes use the managed admin login. Set it to false to keep the dashboard list-only.");
        return new WebAdminArm(() => DarlingManagedPostgres.TryBuildWebAdminConnectionStringFromStoredCredential(postgres));
    }

    /// <summary>The start-up warning for OIDC sign-in with no role mapping (<c>web.network.oidc.roleClaim</c> unset): every user who signs in at the
    /// identity provider then holds the edit seat. What that reaches depends on the store: on the managed store it is the Admin writes while
    /// <paramref name="adminArm"/> exists, and nothing while it does not; on a store you bring it is whatever the web login's own grants allow
    /// (<c>postgres.webConnectionString</c>), which the service cannot see from here. Logs nothing when there is no OIDC sign-in or a role mapping
    /// exists (viewer-only included). Returns whether it warned.</summary>
    internal static bool WarnWhenOidcHasNoRoleMapping(DarlingWebOidcClient? oidc, bool managedStore, WebAdminArm? adminArm, ILogger logger)
    {
        if (oidc is null || oidc.Options.RoleClaim is not null)
        {
            return false;
        }

        var effect = !managedStore
            ? "On this store you brought, what they can change depends on the web login's grants (postgres.webConnectionString): with a login that can write, every one of them can change thresholds, routes, webhook URLs and the agent switch."
            : adminArm is not null
                ? "web.allowAdminWrites is on, so every user who signs in can make the dashboard's Admin writes."
                : "web.allowAdminWrites is off, so the dashboard stays list-only for them.";
        logger.LogWarning("web.network.oidc has no role mapping, so every user who signs in has the edit seat. {Effect} Set web.network.oidc.roleClaim with adminRoles and viewerRoles to limit who can edit.", effect);
        return true;
    }

    /// <summary>The admin pool, built on first use, or null while its credential is not stored yet (or after disposal).</summary>
    internal NpgsqlDataSource? Source
    {
        get
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    return null;
                }

                if (_source is null && _connectionString() is { } connectionString)
                {
                    _source = NpgsqlDataSource.Create(DarlingStoreConnection.PinSessionTimeZoneUtc(
                        DarlingStoreConnection.WithApplicationName(connectionString, DarlingManagedPostgres.WebAdminApplicationName)));
                }

                return _source;
            }
        }
    }

    /// <summary>The source for one request's Admin write and its access check: the arm's pool when the seat is a signed-in admin and the arm is
    /// available, otherwise <paramref name="web"/>. A null arm (a store you bring, or the switch off) always answers <paramref name="web"/>.</summary>
    internal static NpgsqlDataSource SourceFor(WebAdminArm? arm, HttpContext context, NpgsqlDataSource web) =>
        arm is not null && DarlingWebSeat.IsSignedInAdmin(context) && arm.Source is { } admin ? admin : web;

    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
            _source?.Dispose();
            _source = null;
        }
    }
}
