/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The store login the web dashboard and the MCP server connect with on a store the service does not manage
/// (#3914). On a managed store each host derives its least-privilege login from its DPAPI credential file and
/// never comes here. Everywhere else both used to connect with <c>postgres.connectionString</c>, the collection
/// login, which is the store OWNER, so none of the <c>viewer</c>/<c>mcp</c> roles' protections applied to a web
/// session or an MCP token-holder: not the secret-column carve, not the narrow write grants, not the
/// <c>statement_timeout</c> backstop. Now, in order:
/// <list type="number">
/// <item><description><c>postgres.webConnectionString</c> / <c>postgres.mcpConnectionString</c> when set: the
/// operator's own login, normally the role <c>tools/provision-roles.sql</c> creates. Same forms as
/// <c>postgres.connectionString</c> (a literal, or an <c>env:</c>/<c>file:</c> reference), resolved here when the
/// host starts rather than when the config is parsed, so an unreadable reference keeps that one surface down with
/// the reason logged instead of stopping collection.</description></item>
/// <item><description>On the Linux compose distribution's own store, the <c>viewer</c>/<c>mcp</c> role the service
/// provisioned there itself (<see cref="DarlingManagedRoles.EnsureComposeStoreProvisionedAsync"/>). The worker
/// publishes that verdict once the batch has committed, so a host never starts on a login the store does not
/// accept yet; a host in a container waits for it.</description></item>
/// <item><description>On that store, when this start did NOT provision the roles (refused, failed, or a collector
/// that stood down before it got there), the role's credential from an earlier start, when the service holds a
/// trusted one (<see cref="DarlingManagedRoles.ReadEarlierComposeCredential"/>): the role still has that password,
/// so one bad start does not hand a surface the owner for the whole process. A login the store refuses keeps the
/// surface down with the store's error; it never becomes the owner. A warning says which start's credential it
/// is (<see cref="EarlierCredentialWarning"/>).</description></item>
/// <item><description>Otherwise the owner, with a startup warning that names what the owner login gives up
/// (<see cref="OwnerFallbackWarning"/>). That is the ruling's fallback, and it keeps a dashboard that works today
/// working: a compose store with no provisioning this start and no trusted credential from an earlier one lands
/// here too, with the reasons, rather than taking the only Linux UI down.</description></item>
/// </list>
/// </summary>
internal static class DarlingStoreLogins
{
    /// <summary>The two network surfaces with a store login of their own.</summary>
    internal enum Surface
    {
        /// <summary>The web dashboard: the <c>viewer</c> role's surface.</summary>
        Web,

        /// <summary>The MCP server: the <c>mcp</c> role's surface.</summary>
        Mcp,
    }

    /// <summary>Where a surface's store login came from.</summary>
    internal enum LoginSource
    {
        /// <summary><c>postgres.webConnectionString</c> / <c>postgres.mcpConnectionString</c>.</summary>
        Configured,

        /// <summary>The role the service provisioned on the compose store this start.</summary>
        ComposeStoreRole,

        /// <summary>The same role, with the credential an earlier start gave it: this start did not provision the
        /// roles.</summary>
        ComposeStoreRoleFromEarlierStart,

        /// <summary><c>postgres.connectionString</c>: the owner.</summary>
        Owner,
    }

    /// <summary>How often, and how many times, a host polls for the worker's compose-store verdict before it gives
    /// up this start attempt — the managed hosts' credential wait (60 x 5 s), for the same first-boot window.</summary>
    internal static readonly TimeSpan VerdictPollInterval = TimeSpan.FromSeconds(5);

    internal const int VerdictPollAttempts = 60;

    /// <summary>The pool bound the managed role logins carry (<c>DarlingManagedPostgres.BuildRoleConnectionString</c>,
    /// #1559): every pooled connection is a backend process on the store.</summary>
    internal const int RoleLoginMaxPoolSize = 24;

    private static ComposeStoreVerdict? s_composeStoreVerdict;

    private static readonly ConcurrentDictionary<Surface, string> s_lastWarning = new();

    /// <summary>
    /// The worker's answer to "did this process provision the compose store's roles?", published once per process
    /// (#3914). A class, not a record, so no generated <c>ToString</c> can print a connection string with its
    /// password into a log line.
    /// </summary>
    internal sealed class ComposeStoreVerdict
    {
        private ComposeStoreVerdict(
            string? viewerConnectionString, string? mcpConnectionString, string? notProvisionedReason, int appliedSeconds,
            string? credentialDirectory)
        {
            ViewerConnectionString = viewerConnectionString;
            McpConnectionString = mcpConnectionString;
            NotProvisionedReason = notProvisionedReason;
            AppliedComposeStatementTimeoutSeconds = appliedSeconds;
            CredentialDirectory = credentialDirectory;
        }

        /// <summary>The roles are the service's and accept the logins below.</summary>
        internal bool Provisioned => ViewerConnectionString is not null;

        internal string? ViewerConnectionString { get; }

        internal string? McpConnectionString { get; }

        /// <summary>Why this start did not provision the roles; null when it did.</summary>
        internal string? NotProvisionedReason { get; }

        /// <summary>Where the credential files from earlier starts are, for a surface this start did not provision:
        /// the directory the worker provisions from, or the shipped one for a collector that stood down first.
        /// Null when provisioned.</summary>
        internal string? CredentialDirectory { get; }

        /// <summary>What provisioning wrote onto the roles, for the worker's #2918 reload baseline.</summary>
        internal int AppliedComposeStatementTimeoutSeconds { get; }

        internal string? ConnectionStringFor(Surface surface) =>
            surface == Surface.Web ? ViewerConnectionString : McpConnectionString;

        internal static ComposeStoreVerdict ProvisionedWith(string viewerConnectionString, string mcpConnectionString, int appliedSeconds) =>
            new(viewerConnectionString, mcpConnectionString, null, appliedSeconds, null);

        internal static ComposeStoreVerdict NotProvisioned(string reason, string? credentialDirectory = null) =>
            new(null, null, reason, DarlingManagedRoles.ComposeStatementTimeoutUnknown,
                credentialDirectory ?? DarlingManagedRoles.ComposeStoreCredentialDirectory);
    }

    /// <summary>One surface's resolved login. A class so its <c>ToString</c> cannot leak the password.</summary>
    internal sealed class StoreLogin
    {
        internal StoreLogin(string connectionString, LoginSource source, string? warning)
        {
            ConnectionString = connectionString;
            Source = source;
            Warning = warning;
        }

        internal string ConnectionString { get; }

        internal LoginSource Source { get; }

        /// <summary>What the startup warning says about this login, or null when there is nothing to say.</summary>
        internal string? Warning { get; }
    }

    internal static void PublishComposeStoreVerdict(ComposeStoreVerdict verdict) =>
        Volatile.Write(ref s_composeStoreVerdict, verdict ?? throw new ArgumentNullException(nameof(verdict)));

    /// <summary>
    /// Settles the verdict as not provisioned unless the worker already published one. Called from every
    /// collection-blocking stand-down (<c>CollectorRuntimeState.PublishStopped</c>), all of which come before
    /// provisioning, so a host waiting in a container learns that no verdict is coming instead of waiting for one,
    /// and connects with its role's credential from an earlier start when the service holds one.
    /// </summary>
    internal static void SettleComposeStoreVerdict(string reason) =>
        Interlocked.CompareExchange(ref s_composeStoreVerdict, ComposeStoreVerdict.NotProvisioned(reason), null);

    internal static ComposeStoreVerdict? ReadComposeStoreVerdict() => Volatile.Read(ref s_composeStoreVerdict);

    /// <summary>Test seam: the verdict is process state, so a test that asserts on it starts from none. Only the
    /// serialized <c>darling-config-env</c> test collection calls it.</summary>
    internal static void ResetComposeStoreVerdictForTests() => Volatile.Write(ref s_composeStoreVerdict, null);

    /// <summary>
    /// The worker's half (#3914): provisions the compose store's roles, builds the viewer and mcp logins on
    /// success, and publishes the verdict either way. Non-throwing apart from cancellation — the same posture as
    /// managed provisioning, whose failure never kills collection — so a refused or failed provisioning leaves each
    /// surface on its role's credential from an earlier start, or on the owner when there is none, with the
    /// reason, and collection carries on.
    /// </summary>
    internal static async Task<ComposeStoreVerdict> ProvisionComposeStoreAsync(
        NpgsqlDataSource postgres, string ownerConnectionString, ILogger logger, CancellationToken cancellationToken,
        string? credentialDirectory = null)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(logger);

        credentialDirectory ??= DarlingManagedRoles.ComposeStoreCredentialDirectory;
        ComposeStoreVerdict verdict;
        try
        {
            var result = await DarlingManagedRoles.EnsureComposeStoreProvisionedAsync(
                postgres, credentialDirectory, logger, cancellationToken);

            if (result.Provisioned)
            {
                verdict = ComposeStoreVerdict.ProvisionedWith(
                    BuildComposeStoreRoleConnectionString(ownerConnectionString, DarlingManagedPostgres.ViewerRoleName, result.ViewerPassword!),
                    BuildComposeStoreRoleConnectionString(ownerConnectionString, DarlingManagedPostgres.McpRoleName, result.McpPassword!),
                    result.AppliedComposeStatementTimeoutSeconds);
                logger.LogInformation(
                    "Compose store: the service provisioned its own least-privilege roles, so the web dashboard connects as viewer and the MCP server as mcp, not as the owner (#3914)");
            }
            else
            {
                verdict = ComposeStoreVerdict.NotProvisioned(result.RefusalReason!, credentialDirectory);
                logger.LogWarning(
                    "Compose store roles were not provisioned this start (the web dashboard and the MCP server each log which login they use instead): {Reason}",
                    result.RefusalReason);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(
                "Provisioning the compose store's least-privilege roles failed this start (the web dashboard and the MCP server each log which login they use instead): {Message}",
                ex.Message);
            verdict = ComposeStoreVerdict.NotProvisioned(
                $"Provisioning the store's least-privilege roles failed: {ex.Message.Split('\n')[0].TrimEnd('\r')}",
                credentialDirectory);
        }

        PublishComposeStoreVerdict(verdict);
        return verdict;
    }

    /// <summary>
    /// A role's login on the compose store, derived from the owner's connection string (#3914). It inherits only
    /// WHERE the store is and how to trust it — host, port, database, TLS mode and negotiation, root certificate,
    /// revocation checking, channel binding, the authentication methods it accepts, connect timeout — and nothing
    /// that is the owner's own: not its password or passfile, not a client certificate, and not <c>Options</c>,
    /// where a <c>-c statement_timeout=0</c> would switch off the very backstop the role carries. Every setting it
    /// does inherit can only make the role's connection stricter than the defaults, never looser, so an owner that
    /// requires channel binding or SCRAM does not hand its roles a connection that would settle for less. The search
    /// path and pool bound are the managed role logins'.
    /// </summary>
    internal static string BuildComposeStoreRoleConnectionString(string ownerConnectionString, string role, string password)
    {
        var owner = new NpgsqlConnectionStringBuilder(ownerConnectionString);
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = owner.Host,
            Port = owner.Port,
            Database = owner.Database,
            SslMode = owner.SslMode,
            SslNegotiation = owner.SslNegotiation,
            RootCertificate = owner.RootCertificate,
            CheckCertificateRevocation = owner.CheckCertificateRevocation,
            ChannelBinding = owner.ChannelBinding,
            RequireAuth = owner.RequireAuth,
            Timeout = owner.Timeout,
            Username = role,
            Password = password,
            SearchPath = DarlingManagedPostgres.SearchPath,
            MaxPoolSize = RoleLoginMaxPoolSize,
        };
        return builder.ConnectionString;
    }

    /// <summary>
    /// The user-authored custom-alert evaluator's viewer login on the compose store (#3970), or null when this
    /// deployment must not get one. The worker calls this beside the managed DPAPI gate
    /// (<see cref="DarlingManagedPostgres.TryBuildViewerConnectionStringFromStoredCredential"/>), after this
    /// start's <see cref="ComposeStoreVerdict"/> is already published (<see cref="ProvisionComposeStoreAsync"/>
    /// runs earlier in the same start). Follows the same two compose branches <see cref="ResolveUnmanagedAsync"/>
    /// gives a host — this start's provisioned role, or, when this start did not provision the roles, a trusted
    /// credential an earlier start left, accepted before use exactly as a host accepts one — but stops there: the
    /// evaluator has no configured setting of its own, and per the ruling it must NEVER fall back to the owner
    /// login the way a host's <see cref="Resolve"/> does: a rule's compose metric would then run with owner
    /// rights, which the viewer-role pool exists to prevent. A bring-your-own store, and a process that never
    /// reaches compose provisioning at all (managed mode, or not running in a container), publish no verdict
    /// here (<see cref="ReadComposeStoreVerdict"/> is null), so both return null — the ruling's "a bring-your-own
    /// store stays out": its viewer role comes from <c>tools/provision-roles.sql</c>, which does not create
    /// <c>config.record_custom_alert_resolution</c>.
    /// </summary>
    internal static async Task<string?> ResolveComposeCustomAlertViewerAsync(
        string ownerConnectionString, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(logger);

        var verdict = ReadComposeStoreVerdict();
        if (verdict is null)
        {
            return null;
        }

        if (verdict.ConnectionStringFor(Surface.Web) is { } roleLogin)
        {
            return roleLogin;
        }

        if (verdict.CredentialDirectory is not { } directory)
        {
            return null;
        }

        var earlier = DarlingManagedRoles.ReadEarlierComposeCredential(directory, DarlingManagedPostgres.ViewerRoleName, logger);
        if (earlier.Password is not { } password)
        {
            return null;
        }

        var candidate = BuildComposeStoreRoleConnectionString(ownerConnectionString, DarlingManagedPostgres.ViewerRoleName, password);
        return await AcceptsLoginAsync(
            candidate, "Custom alert evaluator", DarlingManagedPostgres.ViewerRoleName, logger, cancellationToken)
            ? candidate
            : null;
    }

    /// <summary>
    /// The non-managed branch of both hosts' start (#3914): the connection string the host's store pool is
    /// created from, or null when the host must not start this attempt — a configured reference that cannot
    /// resolve, or that resolves to nothing (fail-closed: the operator asked for a login, so the owner is not a
    /// silent substitute); a container host still waiting for the worker's compose-store verdict; or a role's
    /// credential from an earlier start that the store refuses (the role was dropped or re-keyed by hand), which
    /// keeps the surface down with the store's error rather than let it become the owner.
    /// </summary>
    internal static async Task<string?> ResolveUnmanagedAsync(
        Surface surface, PostgresConfig postgres, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(logger);

        var (what, setting, role) = Describe(surface);
        var raw = surface == Surface.Web ? postgres.WebConnectionString : postgres.McpConnectionString;
        string? configured = null;
        if (!string.IsNullOrWhiteSpace(raw))
        {
            try
            {
                configured = DarlingSecretSource.Resolve(raw, setting);
            }
            catch (InvalidOperationException ex)
            {
                logger.LogError("{What} not started this attempt: {Message}", what, ex.Message);
                return null;
            }

            /* The setting is there, so a blank result is no reason to treat it as unset: that would skip the verdict
               wait and land the surface on the owner, the one login the operator set it to avoid. Whatever branch
               produced it, the surface stays down. */
            if (string.IsNullOrWhiteSpace(configured))
            {
                logger.LogError(
                    "{What} not started this attempt: {Setting} is set but resolves to a blank connection string, and the owner login is never used in its place",
                    what, setting);
                return null;
            }
        }

        var inContainer = DarlingHostBinding.IsRunningInContainer;
        var verdict = ReadComposeStoreVerdict();
        if (configured is null && inContainer && verdict is null)
        {
            verdict = await WaitForComposeStoreVerdictAsync(what, logger, cancellationToken);
            if (verdict is null)
            {
                return null;
            }
        }

        EarlierComposeCredential? earlier = null;
        if (configured is null && inContainer && verdict is { Provisioned: false, CredentialDirectory: { } directory })
        {
            earlier = DarlingManagedRoles.ReadEarlierComposeCredential(directory, role, logger);
        }

        var login = Resolve(surface, postgres.ConnectionString, configured, inContainer, verdict, earlier);
        if (login.Warning is { } warning)
        {
            WarnOnce(surface, warning, logger);
        }

        if (login.Source == LoginSource.ComposeStoreRoleFromEarlierStart
            && !await AcceptsLoginAsync(login.ConnectionString, what, role, logger, cancellationToken))
        {
            return null;
        }

        return login.ConnectionString;
    }

    /// <summary>
    /// The decision, pure (#3914): a configured login wins; then, in a container, the compose store's role — with
    /// the login this start provisioned, or, when this start did not provision the roles, with the trusted
    /// credential an earlier start left (<paramref name="earlier"/>); then the owner.
    /// <paramref name="configuredConnectionString"/> is already resolved.
    /// </summary>
    internal static StoreLogin Resolve(
        Surface surface, string ownerConnectionString, string? configuredConnectionString, bool inContainer,
        ComposeStoreVerdict? verdict, EarlierComposeCredential? earlier = null)
    {
        if (!string.IsNullOrWhiteSpace(configuredConnectionString))
        {
            return new StoreLogin(
                configuredConnectionString, LoginSource.Configured,
                ConfiguredLoginWarning(surface, configuredConnectionString, ownerConnectionString, inContainer ? verdict : null));
        }

        if (inContainer && verdict is not null)
        {
            if (verdict.ConnectionStringFor(surface) is { } roleLogin)
            {
                return new StoreLogin(roleLogin, LoginSource.ComposeStoreRole, null);
            }

            if (earlier?.Password is { } password)
            {
                return new StoreLogin(
                    BuildComposeStoreRoleConnectionString(ownerConnectionString, Describe(surface).Role, password),
                    LoginSource.ComposeStoreRoleFromEarlierStart,
                    EarlierCredentialWarning(surface, verdict.NotProvisionedReason));
            }
        }

        var reason = inContainer ? verdict?.NotProvisionedReason : null;
        if (reason is not null && earlier?.MissingReason is { } missing)
        {
            reason += $" No {Describe(surface).Role} credential from an earlier start can stand in: {missing}.";
        }

        return new StoreLogin(ownerConnectionString, LoginSource.Owner, OwnerFallbackWarning(surface, reason));
    }

    /// <summary>
    /// The startup warning for a surface on its role's credential from an earlier start (#3914): this start did not
    /// provision the store's roles — <paramref name="reason"/> says why — so the surface keeps the login an earlier
    /// start gave its role, and a login the store refuses keeps it down rather than handing it the owner. Worded so
    /// it cannot be mistaken for the other two cases: the roles provisioned this start (no warning), and the owner
    /// (<see cref="OwnerFallbackWarning"/>).
    /// </summary>
    internal static string EarlierCredentialWarning(Surface surface, string? reason)
    {
        var (what, _, role) = Describe(surface);
        var warning =
            $"{what} connects to the store as the {role} role with the credential an earlier start provisioned, not as the owner: "
            + "the store's least-privilege roles were not provisioned this start.";
        if (reason is not null)
        {
            warning += " " + reason;
        }

        return warning
            + $" If the store refuses that login (the {role} role was dropped, or its password changed by hand), {char.ToLowerInvariant(what[0])}{what[1..]} "
            + "stays down with the store's error until a start provisions the roles again; it never falls back to the owner login.";
    }

    /// <summary>
    /// Whether the store accepts a role's credential from an earlier start (#3914). The only login this class
    /// checks before a host uses it, because it is the only one that can be stale: the password was set by an
    /// earlier start, and anyone with the owner's rights could have dropped or re-keyed the role since. A refusal,
    /// or any other failure to connect, keeps the surface down this attempt with the store's error, and the
    /// supervisor's retry tries again — never the owner in its place.
    /// </summary>
    private static async Task<bool> AcceptsLoginAsync(
        string connectionString, string what, string role, ILogger logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = new NpgsqlConnection(
                DarlingStoreConnection.PinSessionTimeZoneUtc(
                    new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString));
            await connection.OpenAsync(cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(
                "{What} not started this attempt: the store did not accept the {Role} login an earlier start provisioned ({Message}). It stays down rather than connect as the owner, until a start provisions the roles again.",
                what, role, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// The startup warning for a surface on the owner login (#3914, the ruling's text): which surface, why, what
    /// the owner login gives up — the secret-column carve, the narrow write grants and the statement_timeout
    /// backstop — and how to get them back. <paramref name="reason"/> is the compose store's refusal or failure,
    /// appended when there is one.
    /// </summary>
    internal static string OwnerFallbackWarning(Surface surface, string? reason)
    {
        var (what, setting, role) = Describe(surface);
        var warning =
            $"{what} connects to the store as the owner login (postgres.connectionString) because {setting} is not set. "
            + $"That gives up what the least-privilege {role} role holds it to: the secret-column carve (the SMTP password, "
            + "the webhook URLs, the PagerDuty routing key and the monitored servers' stored passwords are readable to it), "
            + "the narrow write grants (every table in the store is writable to it) and the statement_timeout backstop "
            + $"(nothing stops a runaway query at the store). Set {setting} to the {role} role tools/provision-roles.sql creates.";

        return reason is null ? warning : warning + " " + reason;
    }

    /// <summary>
    /// What a configured login needs said about it (#3914), or null: that it is the owner after all, which gives up
    /// the same three things as leaving it unset; or that it names a role the service provisions on this compose
    /// store and re-keys from its own credential file every start, so a password set by hand stops working.
    /// </summary>
    internal static string? ConfiguredLoginWarning(
        Surface surface, string configuredConnectionString, string ownerConnectionString, ComposeStoreVerdict? verdict)
    {
        string? configuredUser;
        string? ownerUser;
        try
        {
            configuredUser = new NpgsqlConnectionStringBuilder(configuredConnectionString).Username;
            ownerUser = new NpgsqlConnectionStringBuilder(ownerConnectionString).Username;
        }
        catch (ArgumentException)
        {
            /* A malformed string fails at the first connect with Npgsql's own message, which names the problem
               better than a guess here could. */
            return null;
        }

        if (string.IsNullOrEmpty(configuredUser))
        {
            return null;
        }

        var (what, setting, role) = Describe(surface);
        if (string.Equals(configuredUser, ownerUser, StringComparison.Ordinal))
        {
            return $"{setting} logs in as '{configuredUser}', the owner login postgres.connectionString uses, so {char.ToLowerInvariant(what[0])}{what[1..]} still gives up "
                + $"what the least-privilege {role} role holds it to: the secret-column carve, the narrow write grants and the "
                + $"statement_timeout backstop. Point {setting} at the {role} role tools/provision-roles.sql creates.";
        }

        if (verdict?.Provisioned == true
            && (string.Equals(configuredUser, DarlingManagedPostgres.AdminRoleName, StringComparison.Ordinal)
                || string.Equals(configuredUser, DarlingManagedPostgres.ViewerRoleName, StringComparison.Ordinal)
                || string.Equals(configuredUser, DarlingManagedPostgres.McpRoleName, StringComparison.Ordinal)))
        {
            return $"{setting} names the '{configuredUser}' role, which the service provisions on this compose store and re-keys "
                + $"from its own credential file on every start, so a password set by hand stops working. Leave {setting} unset "
                + $"and {char.ToLowerInvariant(what[0])}{what[1..]} connects as the {role} role the service provisions.";
        }

        return null;
    }

    private static (string What, string Setting, string Role) Describe(Surface surface) => surface == Surface.Web
        ? ("The web dashboard", "postgres.webConnectionString", DarlingManagedPostgres.ViewerRoleName)
        : ("The MCP server", "postgres.mcpConnectionString", DarlingManagedPostgres.McpRoleName);

    /// <summary>The warning is stated once per distinct text per surface, not on every supervisor restart.</summary>
    private static void WarnOnce(Surface surface, string warning, ILogger logger)
    {
        if (s_lastWarning.TryGetValue(surface, out var last) && string.Equals(last, warning, StringComparison.Ordinal))
        {
            return;
        }

        s_lastWarning[surface] = warning;
        logger.LogWarning("{Warning}", warning);
    }

    private static async Task<ComposeStoreVerdict?> WaitForComposeStoreVerdictAsync(
        string what, ILogger logger, CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < VerdictPollAttempts; attempt++)
        {
            if (ReadComposeStoreVerdict() is { } verdict)
            {
                return verdict;
            }

            if (attempt == 0)
            {
                logger.LogInformation(
                    "{What} waits for the collector to say whether it provisioned the store's least-privilege roles (the service runs in a container on a store it does not manage)",
                    what);
            }

            try
            {
                await Task.Delay(VerdictPollInterval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        logger.LogWarning(
            "{What} not started this attempt: the collector has not reached role provisioning after {Minutes} minutes (a long migration does this); retrying",
            what, (int)(VerdictPollInterval.TotalMinutes * VerdictPollAttempts));
        return null;
    }
}
