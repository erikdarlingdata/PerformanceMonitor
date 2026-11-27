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
/// login, which is the store OWNER, so none of the <c>web</c>/<c>mcp</c> roles' protections applied to a web
/// session or an MCP token-holder: not the secret-column carve, not the narrow write grants, not the
/// <c>statement_timeout</c> backstop. Now, in order:
/// <list type="number">
/// <item><description><c>postgres.webConnectionString</c> / <c>postgres.mcpConnectionString</c> when set: the
/// operator's own login, normally the role <c>tools/provision-roles.sql</c> creates. Same forms as
/// <c>postgres.connectionString</c> (a literal, or an <c>env:</c>/<c>file:</c> reference), resolved here when the
/// host starts rather than when the config is parsed, so an unreadable reference keeps that one surface down with
/// the reason logged instead of stopping collection.</description></item>
/// <item><description>On the Linux compose distribution's own store, the <c>web</c>/<c>mcp</c> role the service
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
    /// <summary>The network surfaces that resolve a store login.</summary>
    internal enum Surface
    {
        /// <summary>The web dashboard: the <c>web</c> role's surface (its own login, not the read-only <c>viewer</c> one).</summary>
        Web,

        /// <summary>The MCP server: the <c>mcp</c> role's surface.</summary>
        Mcp,

        /// <summary>#9013: the metrics port. It reads the web dashboard's setting (<c>postgres.webConnectionString</c>), but it only
        /// reads, so on the compose store it takes the read-only <c>viewer</c> login before the <c>web</c> one; its log lines name the
        /// metrics port.</summary>
        Metrics,
    }

    /// <summary>#9013: whether a surface resolves the web dashboard's login: the same setting, and the web role as the metrics port's
    /// fallback after the viewer one.</summary>
    private static bool UsesWebLogin(Surface surface) => surface is Surface.Web or Surface.Metrics;

    /* #9013: how a line about a failed attempt opens. The web dashboard and the MCP server do not start; the metrics port is
       already listening, and only its refresh cannot log in. */
    private static string NotThisAttempt(Surface surface, string what) =>
        surface == Surface.Metrics ? "The metrics port cannot read the store this attempt" : $"{what} not started this attempt";

    /* #9013: what a refused login does to the surface, for the warnings that say so. */
    private static string StaysDown(Surface surface, string what) =>
        surface == Surface.Metrics ? "the metrics port's refreshes fail" : $"{char.ToLowerInvariant(what[0])}{what[1..]} stays down";

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

        /// <summary>The web host on the <c>viewer</c> credential an earlier start gave that role, because this start did
        /// not provision the roles and no <c>web</c> credential exists yet: reads work, every write is refused. #9013: the metrics
        /// port, which only reads, takes it first, whether or not a <c>web</c> credential exists.</summary>
        ComposeStoreViewerFromEarlierStart,

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
            string? viewerConnectionString, string? mcpConnectionString, string? webConnectionString, string? notProvisionedReason,
            int appliedSeconds, string? credentialDirectory)
        {
            ViewerConnectionString = viewerConnectionString;
            WebConnectionString = webConnectionString;
            McpConnectionString = mcpConnectionString;
            NotProvisionedReason = notProvisionedReason;
            AppliedComposeStatementTimeoutSeconds = appliedSeconds;
            CredentialDirectory = credentialDirectory;
        }

        /// <summary>The roles are the service's and accept the logins below.</summary>
        internal bool Provisioned => ViewerConnectionString is not null;

        internal string? ViewerConnectionString { get; }

        internal string? McpConnectionString { get; }

        /// <summary>The <c>web</c> role's login: the web dashboard's own, the one that holds its writes.</summary>
        internal string? WebConnectionString { get; }

        /// <summary>Why this start did not provision the roles; null when it did.</summary>
        internal string? NotProvisionedReason { get; }

        /// <summary>Where the credential files from earlier starts are, for a surface this start did not provision:
        /// the directory the worker provisions from, or the shipped one for a collector that stood down first.
        /// Null when provisioned.</summary>
        internal string? CredentialDirectory { get; }

        /// <summary>What provisioning wrote onto the roles, for the worker's #2918 reload baseline.</summary>
        internal int AppliedComposeStatementTimeoutSeconds { get; }

        /* #9013: the metrics port only reads, so the login this start provisioned for it is the read-only viewer one. */
        internal string? ConnectionStringFor(Surface surface) => surface switch
        {
            Surface.Metrics => ViewerConnectionString,
            Surface.Web => WebConnectionString,
            _ => McpConnectionString,
        };

        internal static ComposeStoreVerdict ProvisionedWith(
            string viewerConnectionString, string mcpConnectionString, int appliedSeconds, string webConnectionString) =>
            new(viewerConnectionString, mcpConnectionString, webConnectionString, null, appliedSeconds, null);

        internal static ComposeStoreVerdict NotProvisioned(string reason, string? credentialDirectory = null) =>
            new(null, null, null, reason, DarlingManagedRoles.ComposeStatementTimeoutUnknown,
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

    /* Which login the MCP host's store pool was last built from (-1: none yet). The verdict cannot say whether this
       process is on the compose distribution's own store: a refusal on a bring-your-own store in a container and a
       stand-down both publish "not provisioned" too. A role login (this start's or an earlier start's) can: only the
       service's own provisioning ever hands one out. */
    private static int s_mcpLoginSource = -1;

    /// <summary>Whether the MCP host's store login is a compose-store role, this start's or an earlier start's. False
    /// before the host resolved a login, and for a configured login or the owner.</summary>
    internal static bool McpLoginIsComposeStoreRole() =>
        (LoginSource)Volatile.Read(ref s_mcpLoginSource) is LoginSource.ComposeStoreRole or LoginSource.ComposeStoreRoleFromEarlierStart;

    internal static void RecordMcpLoginSource(LoginSource source) => Volatile.Write(ref s_mcpLoginSource, (int)source);

    /// <summary>Test seam: the source is process state.</summary>
    internal static void ResetMcpLoginSourceForTests() => Volatile.Write(ref s_mcpLoginSource, -1);

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
                    result.AppliedComposeStatementTimeoutSeconds,
                    BuildComposeStoreRoleConnectionString(ownerConnectionString, DarlingManagedPostgres.WebRoleName, result.WebPassword!));
                logger.LogInformation(
                    "Compose store: the service provisioned its own least-privilege roles, so the web dashboard connects as web and the MCP server as mcp, not as the owner (#3914)");
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
            logger.LogError(ex, 
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
            /* #4442 scope 2: the same ApplicationName the managed-mode role connection strings carry
               (DarlingManagedPostgres.BuildRoleConnectionString), so a compose-provisioned role's pool names
               itself in the store's pg_stat_activity exactly like the managed one does. Role names are fixed
               to viewer/mcp on this path (DarlingManagedPostgres.ViewerRoleName / McpRoleName are the only
               callers), so the role name alone picks the surface. */
            ApplicationName = role == DarlingManagedPostgres.ViewerRoleName || role == DarlingManagedPostgres.WebRoleName
                ? DarlingManagedPostgres.WebApplicationName
                : role == DarlingManagedPostgres.McpRoleName
                    ? DarlingManagedPostgres.McpApplicationName
                    : null,
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
    /// store stays out" of this method: it takes <c>postgres.webConnectionString</c> instead
    /// (<see cref="ResolveConfiguredCustomAlertLogin"/>, #9700).
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

        /* The evaluator runs as the web login: it writes the resolution row through record_custom_alert_resolution, which only the
           web and mcp logins can execute (viewer is read-only). */
        var earlier = DarlingManagedRoles.ReadEarlierComposeCredential(directory, DarlingManagedPostgres.WebRoleName, logger);
        if (earlier.Password is not { } password)
        {
            return null;
        }

        var candidate = BuildComposeStoreRoleConnectionString(ownerConnectionString, DarlingManagedPostgres.WebRoleName, password);
        return await AcceptsLoginAsync(
            candidate, "Custom alert evaluator not started this attempt", "It stays down", DarlingManagedPostgres.WebRoleName, logger, cancellationToken)
            ? candidate
            : null;
    }

    /// <summary>
    /// The custom-alert evaluator's login on a bring-your-own store (#9700): the web login the operator named in
    /// <c>postgres.webConnectionString</c>, resolved the way the web host resolves it. Null when the setting is unset
    /// (the evaluator stays down and says which setting starts it) and when it cannot resolve or resolves blank (logged,
    /// and never the owner login in its place). The web login is the one that may execute
    /// <c>config.record_custom_alert_resolution</c>, which <c>tools/provision-roles.sql</c> creates for it.
    /// </summary>
    internal static string? ResolveConfiguredCustomAlertLogin(PostgresConfig postgres, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(logger);

        if (string.IsNullOrWhiteSpace(postgres.WebConnectionString))
        {
            return null;
        }

        string resolved;
        try
        {
            resolved = DarlingSecretSource.Resolve(postgres.WebConnectionString, "postgres.webConnectionString");
        }
        catch (InvalidOperationException ex)
        {
            logger.LogError("Custom alert evaluator not started: {Message}", ex.Message);
            return null;
        }

        if (string.IsNullOrWhiteSpace(resolved))
        {
            logger.LogError(
                "Custom alert evaluator not started: postgres.webConnectionString is set but resolves to a blank connection string, and the owner login is never used in its place");
            return null;
        }

        /* A string Npgsql cannot use would throw from the data source the worker creates from it, and the host logs that
           exception's message, which can quote part of the string (a password with an unquoted ';' makes the text after
           it the "keyword"). So prove it first, as every other site does, log the formatter's text and keep the
           evaluator down while the service runs on. */
        if (DarlingStoreConnection.TryDescribeParseFailure(resolved) is { } unusable)
        {
            logger.LogError(
                "Custom alert evaluator not started: postgres.webConnectionString is unusable: {Detail}", unusable);
            return null;
        }

        /* On a managed or compose store the evaluator gets the web role by construction. Here it gets whatever the
           setting names, and the web host's own warning does not show when web.enabled is off, so say it here. The
           login is still used, as the web host uses it. */
        if (ConfiguredLoginWarning(
                Surface.Web, resolved, postgres.ConnectionString, null, whatOverride: "The custom alert evaluator") is { } warning)
        {
            logger.LogWarning("{Warning}", warning);
        }

        return resolved;
    }

    /// <summary>
    /// <see cref="ResolveConfiguredCustomAlertLogin"/>, then one sign-in with what it resolved (#9760), as the compose
    /// store's evaluator login is accepted before use: the evaluator opens a connection only when a rule is due, so a
    /// login the store refuses (<c>tools/provision-roles.sql</c> has not run, or the password is wrong) would otherwise
    /// show up only as a rule that never fires. A refusal, or any other failure to connect, keeps the evaluator down
    /// for this start with a warning that names the setting, the login and the store's error text, never the string.
    /// </summary>
    internal static async Task<string?> ResolveConfiguredCustomAlertLoginAsync(
        PostgresConfig postgres, ILogger logger, CancellationToken cancellationToken)
    {
        var resolved = ResolveConfiguredCustomAlertLogin(postgres, logger);
        if (resolved is null)
        {
            return null;
        }

        var (accepted, failure) = await TryOpenLoginAsync(resolved, cancellationToken);
        if (failure is not null)
        {
            logger.LogWarning(
                "Custom alert evaluator not started: the store did not accept the {Login} login that postgres.webConnectionString names ({Message}). It stays down until the next start. Run tools/provision-roles.sql to create the web login, or fix the login and password in postgres.webConnectionString.",
                CustomAlertLoginName(resolved), failure.Message);
        }

        return accepted ? resolved : null;
    }

    /// <summary>
    /// The startup line that says the custom-alert evaluator is running and as which login (#9760): the login the
    /// connection string names, and, when an operator named it, the setting it came from. Never the string.
    /// </summary>
    internal static void LogCustomAlertEvaluatorReady(
        ILogger logger, string loginConnectionString, int sweepSeconds, bool fromWebConnectionString)
    {
        ArgumentNullException.ThrowIfNull(logger);

        logger.LogInformation(
            "Custom alert evaluator ready (#3285): evaluating user rules every {Seconds}s as the {Login} login{Source}.",
            sweepSeconds, CustomAlertLoginName(loginConnectionString),
            fromWebConnectionString ? " from postgres.webConnectionString" : string.Empty);
    }

    /// <summary>The user a connection string logs in as, for a log line. The string is already proven parseable.</summary>
    private static string CustomAlertLoginName(string connectionString) =>
        new NpgsqlConnectionStringBuilder(connectionString).Username is { Length: > 0 } user ? user : "(no Username given)";

    /// <summary>
    /// The non-managed branch of both hosts' start (#3914): the connection string the host's store pool is
    /// created from, or null when the host must not start this attempt — a configured reference that cannot
    /// resolve, or that resolves to nothing (fail-closed: the operator asked for a login, so the owner is not a
    /// silent substitute); a container host still waiting for the worker's compose-store verdict; or a role's
    /// credential from an earlier start that the store refuses (the role was dropped or re-keyed by hand), which
    /// keeps the surface down with the store's error rather than let it become the owner.
    /// <paramref name="exposed"/> (#9894): the host's bind result for the surface, true when it listens on the network. A container
    /// surface that is exposed and then resolves to the owner login logs the Critical line
    /// (<see cref="ReportContainerExposureOnOwnerLogin"/>).
    /// </summary>
    internal static async Task<string?> ResolveUnmanagedAsync(
        Surface surface, PostgresConfig postgres, ILogger logger, CancellationToken cancellationToken, bool exposed = false)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(logger);

        var (what, setting, role) = Describe(surface);
        var notThisAttempt = NotThisAttempt(surface, what);
        var raw = UsesWebLogin(surface) ? postgres.WebConnectionString : postgres.McpConnectionString;
        string? configured = null;
        if (!string.IsNullOrWhiteSpace(raw))
        {
            try
            {
                configured = DarlingSecretSource.Resolve(raw, setting);
            }
            catch (InvalidOperationException ex)
            {
                logger.LogError("{What}: {Message}", notThisAttempt, ex.Message);
                return null;
            }

            /* The setting is there, so a blank result is no reason to treat it as unset: that would skip the verdict
               wait and land the surface on the owner, the one login the operator set it to avoid. Whatever branch
               produced it, the surface stays down. */
            if (string.IsNullOrWhiteSpace(configured))
            {
                logger.LogError(
                    "{What}: {Setting} is set but resolves to a blank connection string, and the owner login is never used in its place",
                    notThisAttempt, setting);
                return null;
            }
        }

        var inContainer = DarlingHostBinding.IsRunningInContainer;
        var verdict = ReadComposeStoreVerdict();
        if (configured is null && inContainer && verdict is null)
        {
            verdict = await WaitForComposeStoreVerdictAsync(what, notThisAttempt, logger, cancellationToken);
            if (verdict is null)
            {
                return null;
            }
        }

        EarlierComposeCredential? earlier = null;
        EarlierComposeCredential? earlierViewer = null;
        if (configured is null && inContainer && verdict is { Provisioned: false, CredentialDirectory: { } directory })
        {
            /* #8141: a credentials directory another user owns stopped collection; its role credentials are behind it,
               so the surface stays down like one whose role login is refused, never the owner. */
            if (DarlingManagedRoles.ComposeCredentialDirectoryOwnershipRefusal(directory) is { } refusal)
            {
                logger.LogWarning("{What}: {Reason}", notThisAttempt, refusal.Sentence);
                return null;
            }

            /* #9013: the metrics port reads the viewer credential first and the web one only when there is no viewer one; the web
               dashboard the other way round. */
            if (surface == Surface.Metrics)
            {
                earlierViewer = DarlingManagedRoles.ReadEarlierComposeCredential(directory, DarlingManagedPostgres.ViewerRoleName, logger);
                if (earlierViewer.Password is null)
                {
                    earlier = DarlingManagedRoles.ReadEarlierComposeCredential(directory, ComposeRole(surface), logger);
                }
            }
            else
            {
                earlier = DarlingManagedRoles.ReadEarlierComposeCredential(directory, ComposeRole(surface), logger);
                if (UsesWebLogin(surface) && earlier.Password is null)
                {
                    earlierViewer = DarlingManagedRoles.ReadEarlierComposeCredential(directory, DarlingManagedPostgres.ViewerRoleName, logger);
                }
            }
        }

        var login = Resolve(surface, postgres.ConnectionString, configured, inContainer, verdict, earlier, earlierViewer);
        if (surface == Surface.Mcp)
        {
            RecordMcpLoginSource(login.Source);
        }

        LogLoginWarnings(surface, login, verdict, logger);
        ReportContainerExposureOnOwnerLogin(surface, login, postgres.ConnectionString, exposed, inContainer, logger);

        if (login.Source is LoginSource.ComposeStoreRoleFromEarlierStart or LoginSource.ComposeStoreViewerFromEarlierStart
            && !await AcceptsLoginAsync(
                login.ConnectionString, notThisAttempt, surface == Surface.Metrics ? "Its refreshes fail" : "It stays down",
                login.Source == LoginSource.ComposeStoreViewerFromEarlierStart ? DarlingManagedPostgres.ViewerRoleName : ComposeRole(surface),
                logger, cancellationToken))
        {
            return null;
        }

        return login.ConnectionString;
    }

    /// <summary>
    /// The decision, pure (#3914): a configured login wins; then, in a container, the compose store's role — with
    /// the login this start provisioned, or, when this start did not provision the roles, with the trusted
    /// credential an earlier start left (<paramref name="earlier"/>); then the owner.
    /// <paramref name="configuredConnectionString"/> is already resolved. #9013: the metrics port only reads, so its compose-store
    /// order is the viewer login (this start's, then an earlier start's), then the web one an earlier start left, then the owner.
    /// </summary>
    internal static StoreLogin Resolve(
        Surface surface, string ownerConnectionString, string? configuredConnectionString, bool inContainer,
        ComposeStoreVerdict? verdict, EarlierComposeCredential? earlier = null, EarlierComposeCredential? earlierViewer = null)
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

            var viewerLogin = earlierViewer?.Password is { } viewerPassword
                ? new StoreLogin(
                    BuildComposeStoreRoleConnectionString(ownerConnectionString, DarlingManagedPostgres.ViewerRoleName, viewerPassword),
                    LoginSource.ComposeStoreViewerFromEarlierStart,
                    EarlierViewerWarning(verdict.NotProvisionedReason, surface))
                : null;

            /* #9013: the metrics port only reads, so the viewer credential an earlier start left comes before the web one. */
            if (surface == Surface.Metrics && viewerLogin is not null)
            {
                return viewerLogin;
            }

            if (earlier?.Password is { } password)
            {
                return new StoreLogin(
                    BuildComposeStoreRoleConnectionString(ownerConnectionString, ComposeRole(surface), password),
                    LoginSource.ComposeStoreRoleFromEarlierStart,
                    EarlierCredentialWarning(surface, verdict.NotProvisionedReason));
            }

            /* The web host with no web credential yet: the viewer login an earlier start gave that role comes before
               the owner, the way the managed path falls back. Reads work; the store refuses every write, which the web
               answers as a refused change. The owner stays the last resort, where dev used it. */
            if (UsesWebLogin(surface) && viewerLogin is not null)
            {
                return viewerLogin;
            }
        }

        var reason = inContainer ? verdict?.NotProvisionedReason : null;
        if (reason is not null && earlier?.MissingReason is { } missing)
        {
            reason += $" No {ComposeRole(surface)} credential from an earlier start can stand in: {missing}.";
            if (UsesWebLogin(surface) && earlierViewer?.MissingReason is { } viewerMissing)
            {
                reason += $" No viewer credential from an earlier start can stand in either: {viewerMissing}.";
            }
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
        var (what, _, _) = Describe(surface);
        var role = ComposeRole(surface);
        var warning =
            $"{what} connects to the store as the {role} role with the credential an earlier start provisioned, not as the owner: "
            + "the store's least-privilege roles were not provisioned this start.";
        if (reason is not null)
        {
            warning += " " + reason;
        }

        return warning
            + $" If the store refuses that login (the {role} role was dropped, or its password changed by hand), {StaysDown(surface, what)} "
            + "with the store's error until a start provisions the roles again; it never falls back to the owner login.";
    }

    /// <summary>The startup warning for the web host on the read-only viewer login an earlier start provisioned: the store's
    /// roles were not provisioned this start (<paramref name="reason"/> says why) and no web credential exists, so the
    /// pages read but every change is refused until the web role exists. #9013: the metrics port only reads and takes this login
    /// before a web one, so its text says nothing about refused changes or a missing web credential.</summary>
    internal static string EarlierViewerWarning(string? reason, Surface surface = Surface.Web)
    {
        if (surface == Surface.Metrics)
        {
            var metricsWarning =
                "The metrics port connects to the store as the read-only viewer role with the credential an earlier start provisioned, not as the owner: "
                + "the store's least-privilege roles were not provisioned this start.";
            if (reason is not null)
            {
                metricsWarning += " " + reason;
            }

            return metricsWarning
                + " If the store refuses the viewer login, the metrics port's refreshes fail with the store's error; it never falls back to the owner login.";
        }

        var warning =
            "The web dashboard connects to the store as the read-only viewer role with the credential an earlier start provisioned, not as the owner: "
            + "the store's least-privilege roles were not provisioned this start and no web credential exists yet.";
        if (reason is not null)
        {
            warning += " " + reason;
        }

        return warning
            + " Pages read, and every change (mute rules, dismissals, custom alerts, server edits) is refused until a start provisions the web role. "
            + "If the store refuses the viewer login, the web dashboard stays down with the store's error; it never falls back to the owner login.";
    }

    /// <summary>
    /// Whether the store accepts a role's credential from an earlier start (#3914). The only login this class
    /// checks before a host uses it, because it is the only one that can be stale: the password was set by an
    /// earlier start, and anyone with the owner's rights could have dropped or re-keyed the role since. A refusal,
    /// or any other failure to connect, keeps the surface down this attempt with the store's error, and the
    /// supervisor's retry tries again — never the owner in its place.
    /// </summary>
    private static async Task<bool> AcceptsLoginAsync(
        string connectionString, string notThisAttempt, string staysDown, string role, ILogger logger, CancellationToken cancellationToken)
    {
        var (accepted, failure) = await TryOpenLoginAsync(connectionString, cancellationToken);
        if (failure is not null)
        {
            logger.LogError(failure,
                "{What}: the store did not accept the {Role} login an earlier start provisioned ({Message}). {StaysDown} rather than connect as the owner, until a start provisions the roles again.",
                notThisAttempt, role, failure.Message, staysDown);
        }

        return accepted;
    }

    /// <summary>
    /// Opens one unpooled connection with a login: accepted, refused or failed (the failure is the store's or the
    /// driver's own exception), or cancelled (neither accepted nor a failure, so the callers stay quiet while the
    /// service stops).
    /// </summary>
    private static async Task<(bool Accepted, Exception? Failure)> TryOpenLoginAsync(
        string connectionString, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = new NpgsqlConnection(
                DarlingStoreConnection.PinSessionTimeZoneUtc(
                    new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString));
            await connection.OpenAsync(cancellationToken);
            return (true, null);
        }
        catch (OperationCanceledException)
        {
            return (false, null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (false, ex);
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
    /// The user names a configured connection string and the owner's connection string log in as, read the one way every
    /// comparison of the two reads them (Npgsql's own parse). False when either string is malformed: that fails at the
    /// first connect with Npgsql's message, which names the problem better than a guess here could.
    /// </summary>
    private static bool TryReadLoginUsers(
        string configuredConnectionString, string ownerConnectionString, out string? configuredUser, out string? ownerUser)
    {
        try
        {
            configuredUser = new NpgsqlConnectionStringBuilder(configuredConnectionString).Username;
            ownerUser = new NpgsqlConnectionStringBuilder(ownerConnectionString).Username;
            return true;
        }
        catch (ArgumentException)
        {
            configuredUser = null;
            ownerUser = null;
            return false;
        }
    }

    /// <summary>What the user names alone say about a surface's own login on a store the service does not manage (#9894).</summary>
    internal enum LoginNameCheck
    {
        /// <summary>No login of its own: the dedicated connection string is unset, blank, cannot resolve, is malformed, names no user, or names the owner's user.</summary>
        NotDedicated,

        /// <summary>The dedicated string names a user, and the owner's string names a different one.</summary>
        Dedicated,

        /// <summary>The dedicated string names a user, but the owner's string names none (peer or socket authentication, or a default user), so
        /// the names cannot prove the two differ. Never "dedicated" on names alone: the host's probe of the login decides.</summary>
        OwnerUserUnknown,
    }

    /// <summary>
    /// #9894: what the user names say about whether a surface of a store the service does not manage has its own login: its dedicated
    /// connection string (<c>postgres.webConnectionString</c> / <c>postgres.mcpConnectionString</c>) is set, resolves, and names a user, and
    /// the owner's (<c>postgres.connectionString</c>) names a different one. The user names are compared the way
    /// <see cref="ConfiguredLoginWarning"/> compares them (ordinal). A reference that cannot resolve or a malformed string counts as no
    /// dedicated login: the host start logs the resolve failure itself, and the surface stays on loopback. An owner string that names no
    /// user is <see cref="LoginNameCheck.OwnerUserUnknown"/>, never a pass.
    /// </summary>
    internal static LoginNameCheck CheckLoginNames(Surface surface, PostgresConfig postgres)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var (_, setting, _) = Describe(surface);
        var raw = UsesWebLogin(surface) ? postgres.WebConnectionString : postgres.McpConnectionString;
        if (string.IsNullOrWhiteSpace(raw))
        {
            return LoginNameCheck.NotDedicated;
        }

        string configured;
        try
        {
            configured = DarlingSecretSource.Resolve(raw, setting);
        }
        catch (InvalidOperationException)
        {
            return LoginNameCheck.NotDedicated;
        }

        if (string.IsNullOrWhiteSpace(configured)
            || !TryReadLoginUsers(configured, postgres.ConnectionString, out var configuredUser, out var ownerUser)
            || string.IsNullOrEmpty(configuredUser))
        {
            return LoginNameCheck.NotDedicated;
        }

        if (string.IsNullOrEmpty(ownerUser))
        {
            return LoginNameCheck.OwnerUserUnknown;
        }

        return string.Equals(configuredUser, ownerUser, StringComparison.Ordinal) ? LoginNameCheck.NotDedicated : LoginNameCheck.Dedicated;
    }

    /// <summary>
    /// #9894: whether the names alone prove a surface has its own login (<see cref="LoginNameCheck.Dedicated"/>). The name-level rule every
    /// caller that does not connect (the CLI, a report) applies. The host start applies the probe on top of it
    /// (<see cref="DarlingHostBinding.ResolveBindWithLoginProbeAsync"/>).
    /// </summary>
    internal static bool HasDedicatedLogin(Surface surface, PostgresConfig postgres) =>
        CheckLoginNames(surface, postgres) == LoginNameCheck.Dedicated;

    /// <summary>What the probe of a surface's login found (#9894).</summary>
    internal enum LoginProbeVerdict
    {
        /// <summary>Neither a superuser nor a member of the role that owns the store's <c>config</c> schema: the login a network-facing surface may use.</summary>
        Unprivileged,

        /// <summary>A superuser, or a member of the store owner role (under whatever name), so exposure is refused.</summary>
        Privileged,

        /// <summary>The probe could not connect, or the query failed or found no <c>config</c> schema: exposure is refused until a start can run it.</summary>
        Failed,
    }

    /// <summary>The probe's answer. <see cref="ErrorClass"/> is set for <see cref="LoginProbeVerdict.Failed"/> only, and holds a type name and
    /// SQLSTATE, never the connection string or a password.</summary>
    internal readonly record struct LoginProbeResult(LoginProbeVerdict Verdict, string? ErrorClass = null);

    /// <summary>The seconds the probe waits to connect, and for its one command. Short, so a start never waits on it for long.</summary>
    private const int LoginProbeSeconds = 10;

    /// <summary>The query the probe runs, with the store's schema as <c>$1</c>: whether the connected login is a superuser, and whether it is a
    /// member of the role that owns that schema (the migrations create the store's schemas as the owner login).</summary>
    private const string LoginProbeSql =
        "SELECT r.rolsuper, pg_has_role(current_user, n.nspowner, 'MEMBER') "
        + "FROM pg_roles AS r CROSS JOIN pg_namespace AS n WHERE r.rolname = current_user AND n.nspname = $1";

    /// <summary>PURE: the probe's two answers as a verdict. Either one true is <see cref="LoginProbeVerdict.Privileged"/>.</summary>
    internal static LoginProbeResult ClassifyLoginProbe(bool isSuperuser, bool isOwnerMember) =>
        new(isSuperuser || isOwnerMember ? LoginProbeVerdict.Privileged : LoginProbeVerdict.Unprivileged);

    /// <summary>PURE: the error class a failed probe reports: the exception type, plus the SQLSTATE when the store refused it.</summary>
    internal static string DescribeProbeFailure(Exception failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        return failure switch
        {
            PostgresException pg => $"{nameof(PostgresException)} SQLSTATE {pg.SqlState}",
            { InnerException: TimeoutException } => nameof(TimeoutException),
            _ => failure.GetType().Name,
        };
    }

    /// <summary>
    /// #9894: connects once with a surface's own login (unpooled, bounded by <see cref="LoginProbeSeconds"/>) and asks the store whether it is a
    /// superuser or a member of the store owner role. The name check cannot see either, because a superuser or an owner-role member can carry
    /// any name. Never throws for a connection or query failure: that is <see cref="LoginProbeVerdict.Failed"/> with its error class. A
    /// cancellation while the service stops propagates.
    /// </summary>
    internal static async Task<LoginProbeResult> ProbeSurfaceLoginAsync(Surface surface, PostgresConfig postgres, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var (_, setting, _) = Describe(surface);
        try
        {
            var raw = UsesWebLogin(surface) ? postgres.WebConnectionString : postgres.McpConnectionString;
            var builder = new NpgsqlConnectionStringBuilder(DarlingSecretSource.Resolve(raw ?? "", setting))
            {
                Pooling = false,
                Timeout = LoginProbeSeconds,
                CommandTimeout = LoginProbeSeconds,
            };
            await using var connection = new NpgsqlConnection(DarlingStoreConnection.PinSessionTimeZoneUtc(builder.ConnectionString));
            await connection.OpenAsync(cancellationToken);
            await using var command = new NpgsqlCommand(LoginProbeSql, connection)
            {
                Parameters = { new NpgsqlParameter { Value = PgSchemaGenerator.ConfigSchema } },
            };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return new LoginProbeResult(LoginProbeVerdict.Failed, "the config schema was not found");
            }

            return ClassifyLoginProbe(reader.GetBoolean(0), reader.GetBoolean(1));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new LoginProbeResult(LoginProbeVerdict.Failed, DescribeProbeFailure(ex));
        }
    }

    /* #9894: the surfaces whose container line has been logged, so a supervisor retry does not repeat it. */
    private static readonly ConcurrentDictionary<Surface, bool> s_containerExposureLogged = new();

    /// <summary>
    /// #9894: in a container with a store the service does not manage, a surface that is exposed to the network keeps its exposure
    /// (the compose port mapping is the boundary). Where the surface then runs as the owner login, whether the login resolution fell
    /// back to it or the configured login names the owner's user, it says so once at Critical, with the text the refusal outside a
    /// container uses minus the refusal. A login that is not the owner's (the role the compose store provisioned, one an earlier start
    /// left, or a configured one) logs nothing, so the line is made where the login is chosen, not where the surface binds.
    /// Does nothing outside a container or for a surface that is not exposed (its login's own warning is all it logs).
    /// </summary>
    internal static void ReportContainerExposureOnOwnerLogin(
        Surface surface, StoreLogin login, string ownerConnectionString, bool exposed, bool inContainer, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(login);
        ArgumentNullException.ThrowIfNull(logger);

        if (!exposed || !inContainer || !RunsAsOwner(login, ownerConnectionString)
            || !s_containerExposureLogged.TryAdd(surface, true))
        {
            return;
        }

        logger.LogCritical("{Message}", DedicatedLoginRequiredText(surface, refusing: false));
    }

    /// <summary>Whether a resolved login connects as the owner: the owner fallback itself, or a configured string whose user is the owner's.</summary>
    private static bool RunsAsOwner(StoreLogin login, string ownerConnectionString) =>
        login.Source == LoginSource.Owner
        || (login.Source == LoginSource.Configured
            && TryReadLoginUsers(login.ConnectionString, ownerConnectionString, out var configuredUser, out var ownerUser)
            && !string.IsNullOrEmpty(configuredUser)
            && string.Equals(configuredUser, ownerUser, StringComparison.Ordinal));

    /// <summary>Lets a test see the container line, and each surface's login warning, again.</summary>
    internal static void ResetContainerExposureLogForTests()
    {
        s_containerExposureLogged.Clear();
        s_lastWarning.Clear();
    }

    /// <summary>
    /// #9894: the line for a surface that is asked to listen on the network on a store the service does not manage, without a login of its
    /// own. Outside a container the surface stays on loopback (<paramref name="refusing"/>); in a container the exposure is honored and
    /// the line says the same without the refusal. Critical either way. Surface-specific: names the setting to set, the role the roles
    /// script creates for it, and where the script is described.
    /// </summary>
    internal static string DedicatedLoginRequiredText(Surface surface, bool refusing)
    {
        var (what, setting, role) = Describe(surface);
        var block = UsesWebLogin(surface) ? "web.network.listen" : "mcp.network.listen";
        return $"{what} is set to listen on the network ({block} is non-loopback) with your own PostgreSQL store, but {setting} is not set to a login "
            + "other than the store owner: it is unset, or it logs in as the user postgres.connectionString uses"
            + (refusing ? ", so network exposure is refused and it binds loopback-only. " : ". ")
            + $"With your own PostgreSQL a network-facing surface needs its own least-privilege login. Set {setting} to the {role} role that "
            + "provision-roles.sql creates (the script that ships with the install, described in docs/darling/linux.md), "
            + "and make sure that login is not the owner's.";
    }

    /// <summary>
    /// #9894: the line for a surface whose own login the probe found to be a superuser or a member of the store owner role. Outside a
    /// container the surface then stays on loopback. Critical.
    /// </summary>
    internal static string PrivilegedLoginText(Surface surface)
    {
        var (what, setting, role) = Describe(surface);
        var block = UsesWebLogin(surface) ? "web.network.listen" : "mcp.network.listen";
        return $"{what} is set to listen on the network ({block} is non-loopback) with your own PostgreSQL store, but the login in {setting} is a superuser "
            + "or a member of the role that owns the store, so network exposure is refused and it binds loopback-only. "
            + $"With your own PostgreSQL a network-facing surface needs its own least-privilege login. Set {setting} to the {role} role that "
            + "provision-roles.sql creates (the script that ships with the install, described in docs/darling/linux.md), "
            + "and make sure that login is not a superuser and not a member of the store owner role.";
    }

    /// <summary>
    /// #9894: the line for a surface whose own login could not be checked before it was exposed: the probe could not connect, or its query
    /// failed. The surface stays on loopback. Critical. <paramref name="errorClass"/> is the exception type and SQLSTATE, never the
    /// connection string.
    /// </summary>
    internal static string LoginProbeFailedText(Surface surface, string? errorClass)
    {
        var (what, setting, role) = Describe(surface);
        var block = UsesWebLogin(surface) ? "web.network.listen" : "mcp.network.listen";
        return $"{what} is set to listen on the network ({block} is non-loopback) with your own PostgreSQL store, but the login in {setting} could not be "
            + $"checked first ({errorClass ?? "no error class"}), so network exposure is refused and it binds loopback-only. "
            + "Before a network-facing surface is exposed the service connects with that login and confirms it is not a superuser and not a member of the "
            + $"store owner role. Make sure {setting} connects (the {role} role provision-roles.sql creates), then restart the service.";
    }

    /// <summary>
    /// What a configured login needs said about it (#3914), or null: that it is the owner after all, which gives up
    /// the same three things as leaving it unset; or that it names a role the service provisions on this compose
    /// store and re-keys from its own credential file every start, so a password set by hand stops working.
    /// </summary>
    internal static string? ConfiguredLoginWarning(
        Surface surface, string configuredConnectionString, string ownerConnectionString, ComposeStoreVerdict? verdict,
        string? whatOverride = null)
    {
        if (!TryReadLoginUsers(configuredConnectionString, ownerConnectionString, out var configuredUser, out var ownerUser))
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
        what = whatOverride ?? what;
        if (string.Equals(configuredUser, ownerUser, StringComparison.Ordinal))
        {
            return $"{setting} logs in as '{configuredUser}', the owner login postgres.connectionString uses, so {char.ToLowerInvariant(what[0])}{what[1..]} still gives up "
                + $"what the least-privilege {role} role holds it to: the secret-column carve, the narrow write grants and the "
                + $"statement_timeout backstop. Point {setting} at the {role} role tools/provision-roles.sql creates.";
        }

        if (verdict?.Provisioned == true
            && (string.Equals(configuredUser, DarlingManagedPostgres.AdminRoleName, StringComparison.Ordinal)
                || string.Equals(configuredUser, DarlingManagedPostgres.ViewerRoleName, StringComparison.Ordinal)
                || string.Equals(configuredUser, DarlingManagedPostgres.WebRoleName, StringComparison.Ordinal)
                || string.Equals(configuredUser, DarlingManagedPostgres.McpRoleName, StringComparison.Ordinal)))
        {
            return $"{setting} names the '{configuredUser}' role, which the service provisions on this compose store and re-keys "
                + $"from its own credential file on every start, so a password set by hand stops working. Leave {setting} unset "
                + $"and {char.ToLowerInvariant(what[0])}{what[1..]} connects as the {role} role the service provisions.";
        }

        /* The web dashboard's writes belong to the web login. A webConnectionString that names viewer (what the script's example
           and the docs said before the web role existed) is the read-only seat login: once the script has been run again the
           web pages' writes are refused for it, and on a store not yet moved it can still write, which a seat login must not. */
        /* #9013: the metrics port only reads, so a viewer login is all it needs: the warning about refused writes is the web
           dashboard's, which resolves the same setting and says it in its own log line. */
        if (surface == Surface.Web
            && string.Equals(configuredUser, DarlingManagedPostgres.ViewerRoleName, StringComparison.Ordinal))
        {
            return $"{setting} logs in as '{configuredUser}', the read-only login meant for remote and locked-down Viewer seats. "
                + $"The web dashboard has its own '{role}' login that holds the writes its pages make (mute rules, dismissals, custom alerts, "
                + $"server changes and the run-now buttons). Set a password for the {role} role in tools/provision-roles.sql, run the script again, "
                + $"and point {setting} at the {role} login. Until then, once the script has been run again, those writes are refused.";
        }

        return null;
    }

    /// <summary>The role the service provisions on the compose store for a surface: the web dashboard's own <c>web</c> login (the one that holds its
    /// writes; <c>viewer</c> is read-only) or <c>mcp</c>.</summary>
    private static string ComposeRole(Surface surface) =>
        UsesWebLogin(surface) ? DarlingManagedPostgres.WebRoleName : DarlingManagedPostgres.McpRoleName;

    private static (string What, string Setting, string Role) Describe(Surface surface) => surface switch
    {
        Surface.Web => ("The web dashboard", "postgres.webConnectionString", DarlingManagedPostgres.WebRoleName),
        Surface.Mcp => ("The MCP server", "postgres.mcpConnectionString", DarlingManagedPostgres.McpRoleName),
        Surface.Metrics => ("The metrics port", "postgres.webConnectionString", DarlingManagedPostgres.WebRoleName),
        _ => throw new ArgumentOutOfRangeException(nameof(surface), surface, null),
    };

    /// <summary>
    /// The startup warnings for a resolved login. The login's own warning comes first. The web host on the owner
    /// because the role named <c>web</c> is one Darling did not create (no viewer login was saved either, so there is
    /// nothing nearer to fall back to) gets one more line that says so in operator terms and what to run.
    /// </summary>
    internal static void LogLoginWarnings(Surface surface, StoreLogin login, ComposeStoreVerdict? verdict, ILogger logger)
    {
        if (login.Warning is { } warning)
        {
            WarnOnce(surface, warning, logger);
        }

        if (UsesWebLogin(surface) && login.Source == LoginSource.Owner
            && DarlingManagedRoles.RefusalNamesForeignRole(verdict?.NotProvisionedReason, DarlingManagedPostgres.WebRoleName))
        {
            WarnOnce(surface, surface == Surface.Metrics ? ForeignWebRoleMetricsWarning : ForeignWebRoleWarning, logger);
        }
    }

    /// <summary>The line for a foreign <c>web</c> role (see <see cref="LogLoginWarnings"/>).</summary>
    internal const string ForeignWebRoleWarning =
        "The role named web in this store was not created by Darling, so Darling left it alone and created none of its own roles. "
        + "The web dashboard is using the store owner login (postgres.connectionString) until that role is renamed or dropped. "
        + "To fix it, run ALTER ROLE web RENAME TO <another name>; or DROP ROLE web; as the store's superuser, then restart the service so it creates its own web role.";

    /// <summary>#9013: <see cref="ForeignWebRoleWarning"/> for the metrics port, which resolves the same login.</summary>
    internal const string ForeignWebRoleMetricsWarning =
        "The role named web in this store was not created by Darling, so Darling left it alone and created none of its own roles. "
        + "The metrics port is using the store owner login (postgres.connectionString) until that role is renamed or dropped. "
        + "To fix it, run ALTER ROLE web RENAME TO <another name>; or DROP ROLE web; as the store's superuser, then restart the service so it creates its own web role.";

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
        string what, string notThisAttempt, ILogger logger, CancellationToken cancellationToken)
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
            "{What}: the collector has not reached role provisioning after {Minutes} minutes (a long migration does this); retrying",
            notThisAttempt, (int)(VerdictPollInterval.TotalMinutes * VerdictPollAttempts));
        return null;
    }
}
