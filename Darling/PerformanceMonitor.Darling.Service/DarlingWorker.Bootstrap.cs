/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Versioning;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.WindowsServices;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using ServerClock = PerformanceMonitor.Analysis.Baselines.ServerClock;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Service;

public sealed partial class DarlingWorker
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        /* A file: secret whose target ordinary users can read is reported through the service log; the resolver
           itself has no logger. Set before the first config read, which is the first resolve. */
        DarlingSecretSource.ReportFileAccessTo(message => _logger.LogWarning("{Warning}", message));

        _executeStartedUtc = DateTime.UtcNow;
        /* #5477: build the statement filter's first-call costs off the startup path, so the first alert with a
           large report does not pay them. Never throws, logs nothing. */
        _ = PerformanceMonitor.Alerting.AlertStatementFilter.WarmUpAsync();
        /* #2185: an install directory the service account cannot read is diagnosed HERE — first, ahead of
           reading darling.json, and a long way ahead of the managed-Postgres bootstrap. Order is the whole
           point. Every message the reporter saw was downstream of this one: an unreadable tree takes out
           darling.json ("Cannot load configuration") and the bundled PostgreSQL's initdb (an empty Output:
           and a bare loader status) before anything says WHERE the install is. Stated first, it is the line
           above the failure in the log an operator is reading bottom-up.

           Diagnose and continue, deliberately, rather than refuse to start — see DarlingInstallLocation for
           why (the installer asks rather than refuses on an upgrade for the same reason, and a service has
           nobody to ask). Silent unless this process really is running as a Windows service: a console
           test-drive from a Desktop folder runs as the profile owner, reads the tree fine, and is something
           the README suggests doing. */
        if (OperatingSystem.IsWindows())
        {
            DarlingInstallLocation.Report(
                AppContext.BaseDirectory,
                WindowsServiceHelpers.IsWindowsService(),
                _logger);
        }

        DarlingConfig config;
        string configPath;
        /* #2936: same triage as the store-migrate block below, same classifier, same budget. Load() is
           File.Exists then ReadAllText then deserialize, so a sharing violation on a darling.json that an
           installer, the Viewer's Settings save or a config-management tool is mid-write is transient and
           self-heals; a malformed file, an ACL problem, or a file that simply is not there never will.
           The old bare catch could not tell those apart and killed collection for the process lifetime for
           all of them. #2038 already reached this conclusion for the MCP and web hosts, whose supervisors
           retry a failed Load() on a 30 s backoff and whose comments point HERE for the critical case;
           this is that case learning the same lesson. StartupFailureTriage.IsRetryable keeps
           FileNotFoundException terminal even though it is an IOException, which on a first install is the
           likeliest way this ever fails.
           The terminal arm stays a bare catch (Exception), OperationCanceledException included, exactly as
           before — shutdown during Load() still reports the same way it always has. */
        var configRetryBudget = System.Diagnostics.Stopwatch.StartNew();
        /* #4508: set once, the first time the fast budget above runs out on a failure that is still
           retryable — the config-load mirror of the store site's storeSustainedRetryCriticalLogged. Config
           load already used StartupFailureTriage.IsRetryable, so this widens no retryable class; it only
           gives an already-retryable failure the same sustained arm the store site got, instead of the fast
           budget running out and falling through to the identical terminal stand-down a missing or
           malformed file gets. */
        var configSustainedRetryCriticalLogged = false;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                configPath = DarlingConfig.ResolveConfigPath();
                config = DarlingConfig.Load();
                _analyzerConfig = config.Analyzer ?? PerformanceMonitor.PlanAnalysis.AnalyzerConfig.Default;
                _logger.LogInformation("Loaded configuration from {Path}: {ServerCount} server(s)", configPath, config.Servers.Count);
                break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException
                && attempt < StartupFailureTriage.Attempts
                && configRetryBudget.Elapsed < StartupFailureTriage.RetryBudget
                && StartupFailureTriage.IsRetryable(ex))
            {
                _logger.LogWarning(
                    ex,
                    "Cannot load configuration yet ({Message}) — attempt {Attempt} of {Total}, retrying in " +
                    "{Delay}s. A file another process is mid-write recovers on its own; a missing, malformed " +
                    "or unreadable one does not and is not retried.",
                    ex.Message, attempt, StartupFailureTriage.Attempts,
                    (int)StartupFailureTriage.RetryDelay.TotalSeconds);
                _collectorState.PublishRetrying(
                    CollectorRuntimeState.StartupStep.Configuration, attempt, StartupFailureTriage.Attempts);
                await Task.Delay(StartupFailureTriage.RetryDelay, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException
                && StartupFailureTriage.IsRetryable(ex))
            {
                /* #4508: reached only once the fast arm above has stopped matching. A darling.json another
                   process is still mid-write past the two-minute fast budget keeps retrying on
                   SustainedRetryDelay instead of falling through to the terminal catch below, which used to
                   stand collection down for the life of the process identically to a missing or malformed
                   file. */
                var next = StartupFailureTriage.NextAction(attempt, configRetryBudget.Elapsed, ex);
                if (!configSustainedRetryCriticalLogged)
                {
                    // install-darling.ps1 waits for this text (#6470): reword both together.
                    _logger.LogCritical(
                        ex,
                        "Cannot load configuration within the {Budget}s retry budget ({Message}); this is " +
                        "still a recoverable failure, so it keeps retrying every {Delay}s instead of " +
                        "standing down. Collection will not start until it succeeds.",
                        (int)StartupFailureTriage.RetryBudget.TotalSeconds, ex.Message,
                        (int)StartupFailureTriage.SustainedRetryDelay.TotalSeconds);
                    configSustainedRetryCriticalLogged = true;
                }
                else
                {
                    _logger.LogWarning(
                        ex,
                        "Still cannot load configuration ({Message}) — attempt {Attempt}, retrying in " +
                        "{Delay}s.",
                        ex.Message, attempt, (int)StartupFailureTriage.SustainedRetryDelay.TotalSeconds);
                }

                _collectorState.PublishRetrying(
                    CollectorRuntimeState.StartupStep.Configuration, attempt, StartupFailureTriage.Attempts,
                    sustained: true);
                await Task.Delay(next.Delay, stoppingToken);
            }
            catch (Exception ex)
            {
                // install-darling.ps1 waits for this text (#6470): reword both together.
                _logger.LogCritical(ex, "Cannot load configuration: {Message}", ex.Message);
                _collectorState.PublishStopped(CollectorRuntimeState.StartupStep.Configuration);
                return;
            }
        }

        if (OperatingSystem.IsWindows())
        {
            TryHardenConfigFile(configPath);
            TryHardenConfigBackups(configPath, _logger);
        }

        var problems = config.Validate();
        if (problems.Count > 0)
        {
            foreach (var problem in problems)
            {
                // install-darling.ps1 waits for this text (#6470): reword both together.
                _logger.LogCritical("Configuration problem: {Problem}", problem);
            }

            /* #2953: every problem, joined, rather than the first — Validate is all-fatal and reports the whole
               set, so a ping body carrying one of several would send an operator to fix a config that still
               does not start. PublishConfigurationProblems takes the config, not this `problems` list — it
               calls Validate itself, so there is no string/list parameter here for a future caller to hand
               exception text instead (#4316 round 1 B1, round 2 L1-r2 a). `problems` above stays local, for
               the per-problem critical log lines just above. */
            _collectorState.PublishConfigurationProblems(config);
            return;
        }

        /* #6840: a front door has no store of its own, so nothing below applies: no store connection, no migration, no collector,
           no alerting, no retention and no heartbeat. The MCP and web hosts and the directory service are separate hosted
           services and keep running; this worker has nothing left to do. Validate runs first (it does not ask a front door for
           a postgres section) so a broken frontDoor block is reported the same way any other broken block is. */
        if (config.IsFrontDoor)
        {
            foreach (var warning in config.FrontDoor.Warnings())
            {
                _logger.LogWarning("{Warning}", warning);
            }

            _logger.LogInformation(
                "Front door mode: routing to {StoreCount} store(s). This process holds no store and runs no collectors, alerting, retention or heartbeat.",
                config.FrontDoor.Stores.Count);

            /* The worker task stays pending until shutdown instead of completing: this is a deliberate rest, not a stand-down
               after a failure, so it publishes no collector verdict (the ping body keeps its start-up answer). Shutdown
               cancels the wait, and that is the one silent exit. */
            try
            {
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }

        /* #2339: publish the declared peer stores as soon as a VALIDATED config is in hand, so the web
           dashboard's read dispatch (which reuses the MCP tool methods) discloses the same peers even when
           the MCP endpoint is disabled. The MCP host publishes the identical snapshot from its own load;
           whichever runs first wins and they cannot disagree, both reading darling.json.

           Publish re-validates and refuses rather than trusting the Validate() above: it cannot fire here
           (we already returned on any problem), but the check belongs to the publish, not to this call site,
           because the MCP host reaches Publish WITHOUT ever calling Validate. Logged if it ever does, rather
           than discarded, so an impossible state cannot become a silent one. */
        var peerPublish = DarlingPeerDirectory.Publish(config.Peers);
        foreach (var problem in peerPublish.RefusedProblems)
        {
            _logger.LogCritical("Peer disclosure refused (nothing published): {Problem}", problem);
        }

        /* #3712: the file-level analysis routing knob, published the same way so get_alert_settings can report
           the value this process applies rather than a constant (the MCP host publishes it too). */
        DarlingFileLevelAlertSettings.Publish(config.Analysis);

        /* Network-endpoint caller warnings (darling-network-endpoints, D-BYO / D7) — emitted AFTER
           Validate() passes and NEVER inside it (Validate is all-fatal; an optional-endpoint note must not
           abort startup). Covers BYO-mode network.* being ignored and the network.role=admin pivot risk. */
        foreach (var warning in GetNetworkStartupWarnings(config))
        {
            _logger.LogWarning("{Warning}", warning);
        }

        /* #6262: a remediation secret that is a DPAPI blob works for operator remediation but is refused by agent server actions. */
        foreach (var warning in DarlingSecrets.RemediationSecretWarnings(config.Servers))
        {
            _logger.LogWarning("{Warning}", warning);
        }

        /* #6583: a SQL Server entry's port is ignored (the connection uses the host as written); the store registry's entries are named when it is read. */
        foreach (var warning in SqlServerPortAdvice.Warnings(config.Servers))
        {
            _logger.LogWarning("{Warning}", warning);
        }

        /* #5307: a key under web.network / mcp.network (or their tls blocks, or web.network.oidc) that no config
           class declares is dropped at load; name each one so a mistyped key is visible. Paths only. */
        foreach (var warning in GetUnknownNetworkKeyWarnings(config))
        {
            _logger.LogWarning("{Warning}", warning);
        }

        /* #4220: web.publicBaseUrl shaped like it carries a credential (a query string, fragment, or
           userinfo — e.g. the dashboard's own sign-in link pasted in by mistake). Ruled: sending the link is
           the operator's call, not refused here; one warning at startup is the whole mitigation. */
        var triageLinkWarning = TriageLink.DescribeCredentialShapedBaseWarning(config.Web.PublicBaseUrl);
        if (triageLinkWarning is not null)
        {
            _logger.LogWarning("{Warning}", triageLinkWarning);
        }

        /* Bundled-Postgres bootstrap (the shipped zero-admin default): in managed mode the
           service unpacks/initializes/starts its own Postgres BEFORE the store connection
           below, and the connection string is DERIVED (localhost + port + the generated
           DPAPI credential), never configured. Windows-only, like every DPAPI surface here.
           A bootstrap failure is the existing no-store behavior: LogCritical + clean exit. */
        DarlingManagedPostgres? managedPostgres = null;
        var storeConnectionString = config.Postgres.ConnectionString;

        /* #1706: what the store runtime reconcile did this start, carried past the bootstrap so it can be
           raised as a real self-alert once the alert engine exists. Null when nothing happened, which is
           every ordinary start. */
        DarlingSelfAlertEvaluator.StoreUpgradeReport? storeUpgradeReport = null;

        /* #3908: what this start did to the store's TimescaleDB extension, alerted beside the upgrade report. */
        DarlingSelfAlertEvaluator.StoreTimescaleReport? storeTimescaleReport = null;

        if (config.Postgres.Managed)
        {
            if (!OperatingSystem.IsWindows())
            {
                // install-darling.ps1 waits for this text (#6470): reword both together.
                _logger.LogCritical(
                    "postgres.managed = true requires Windows (the bundled runtime and the DPAPI-protected credential); " +
                    "set postgres.managed = false and point postgres.connectionString at your own PostgreSQL instead.");
                _collectorState.PublishManagedStoreNeedsWindows();
                return;
            }

            managedPostgres = new DarlingManagedPostgres(config.Postgres, _logger);
            /* #2936: the sharpest of the three sites, because the judgment already existed and was being
               thrown away. OpenProbedMaintenanceConnectionAsync inside this bootstrap classifies transient connection
               faults and retries 6 times 2 s apart — and when that runs out it throws, and this catch
               discarded the fact that the failure had been RULED transient. Re-classifying here is what
               makes that verdict mean something.
               StartupFailureTriage is deliberately WIDER than the inner IsTransientConnectionFault, which
               rules that a PostgresException means the server replied and so is never transient. That is
               right at its own site and wrong one layer up: 57P01 and 57P03 are the server replying "going
               down" and "not up yet", which is how a store restart and a crash-recovering store present.
               So a bootstrap the inner layer gave up on can still be retried here, correctly.
               Safe to re-enter: the class documents it and the body shows it — EnsureRuntimeAsync unpacks
               only what is missing, initdb runs only without PG_VERSION, the conf append is
               marker-guarded, and an already-running postmaster is adopted rather than restarted, so "a
               second EnsureRunningAsync against an initialized, running cluster does no initdb, no
               restart, no credential rewrite".
               NOTE this refines a contract DarlingManagedPostgres' own comments still state as
               "throw => service-exit": for a classified-transient throw it is now retry-then-service-exit.
               Terminal throws behave exactly as before. */
            var bootstrapRetryBudget = System.Diagnostics.Stopwatch.StartNew();
            /* #4508: set once, the first time the fast budget above runs out on a failure that is still
               retryable — see the sustained arm below. Same gating shape as the store site's
               storeSustainedRetryCriticalLogged: one CRITICAL the moment the diagnosis is new, then a
               WARNING on every later attempt against the same still-failing bootstrap. */
            var bootstrapSustainedRetryCriticalLogged = false;
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    storeConnectionString = await managedPostgres.EnsureRunningAsync(stoppingToken);
                    storeUpgradeReport = BuildStoreUpgradeReport(managedPostgres.LastUpgradeOutcome);
                    storeTimescaleReport = BuildStoreTimescaleReport(managedPostgres.LastTimescaleOutcome);
                    break;
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex) when (attempt < StartupFailureTriage.Attempts
                    && bootstrapRetryBudget.Elapsed < StartupFailureTriage.RetryBudget
                    && StartupFailureTriage.IsRetryable(ex))
                {
                    _logger.LogWarning(
                        ex,
                        "Managed Postgres bootstrap failed, retrying ({Message}) — attempt {Attempt} of " +
                        "{Total}, retrying in {Delay}s. A transiently locked file or a store still coming " +
                        "up recovers on its own; a broken package or a stale credential does not and is " +
                        "not retried.",
                        ex.Message, attempt, StartupFailureTriage.Attempts,
                        (int)StartupFailureTriage.RetryDelay.TotalSeconds);
                    _collectorState.PublishRetrying(
                        CollectorRuntimeState.StartupStep.ManagedStore, attempt, StartupFailureTriage.Attempts);
                    await Task.Delay(StartupFailureTriage.RetryDelay, stoppingToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException
                    && StartupFailureTriage.IsRetryable(ex))
                {
                    /* #4508: reached only once the fast arm above has stopped matching — the managed-Postgres
                       bootstrap mirror of the store site's sustained arm just above this block. A bootstrap
                       still failing on a classified-transient cause (a store still coming up, a transiently
                       locked file) keeps retrying on SustainedRetryDelay instead of falling through to the
                       terminal catch below, which used to stand collection down for the life of the process
                       identically to a broken package or a stale credential. */
                    var next = StartupFailureTriage.NextAction(attempt, bootstrapRetryBudget.Elapsed, ex);
                    if (!bootstrapSustainedRetryCriticalLogged)
                    {
                        // install-darling.ps1 waits for this text (#6470): reword both together.
                        _logger.LogCritical(
                            ex,
                            "Managed Postgres bootstrap has not succeeded within the {Budget}s retry budget " +
                            "({Message}); this is still a recoverable failure, so it keeps retrying every " +
                            "{Delay}s instead of standing down. Collection will not start until it succeeds.",
                            (int)StartupFailureTriage.RetryBudget.TotalSeconds, ex.Message,
                            (int)StartupFailureTriage.SustainedRetryDelay.TotalSeconds);
                        bootstrapSustainedRetryCriticalLogged = true;
                    }
                    else
                    {
                        _logger.LogWarning(
                            ex,
                            "Managed Postgres bootstrap still has not succeeded ({Message}) — attempt " +
                            "{Attempt}, retrying in {Delay}s.",
                            ex.Message, attempt, (int)StartupFailureTriage.SustainedRetryDelay.TotalSeconds);
                    }

                    _collectorState.PublishRetrying(
                        CollectorRuntimeState.StartupStep.ManagedStore, attempt, StartupFailureTriage.Attempts,
                        sustained: true);
                    await Task.Delay(next.Delay, stoppingToken);
                }
                catch (Exception ex)
                {
                    // install-darling.ps1 waits for this text (#6470): reword both together.
                    _logger.LogCritical(ex, "Managed Postgres bootstrap failed: {Message}", ex.Message);
                    _collectorState.PublishStopped(CollectorRuntimeState.StartupStep.ManagedStore);
                    return;
                }
            }
        }

        /* #4215: this start's darling-managed.conf write result, carried past the
           bootstrap the same way storeUpgradeReport/storeTimescaleReport are, so LogStoreHostProfileAsync can
           fold a hand edit's changed keys into the stored verdict rows without re-reading the file. Null on a
           BYO store (managedPostgres itself is null) and on the adopted-listener path (the write never ran). */
        var managedDataDirectory = managedPostgres?.DataDirectory;
        var managedConfWriteResult = OperatingSystem.IsWindows() ? managedPostgres?.LastManagedConfWriteResult : null;

        /* #4215: whether THIS start fell back to darling-managed.conf.last-good, carried out of the
           bootstrap the same way managedConfWriteResult already is — see
           DarlingManagedPostgres.LastStartUsedLastGoodManagedConf. False (never true) on a BYO store, off
           Windows, and on the adopted-listener path, for the same reasons managedConfWriteResult is null
           there. */
        var managedUsedLastGoodConf = OperatingSystem.IsWindows() && (managedPostgres?.LastStartUsedLastGoodManagedConf ?? false);

        /* #4336: this start's darling-managed.conf verification outcome, carried out of the
           bootstrap the same way managedConfWriteResult and managedUsedLastGoodConf already are. Null on a
           BYO store, off Windows, and on the adopted-listener path, for the same reasons those are. */
        var managedConfVerification = OperatingSystem.IsWindows() ? managedPostgres?.LastManagedConfVerification : null;

        try
        {
            await RunCollectionLoopAsync(
                config, storeConnectionString, storeUpgradeReport, storeTimescaleReport,
                managedDataDirectory, managedConfWriteResult, managedUsedLastGoodConf, managedConfVerification,
                OperatingSystem.IsWindows() && managedPostgres is not null ? managedPostgres.ReconcileNetworkAfterRoleProvisioningAsync : null, stoppingToken);
        }
        finally
        {
            /* Stop the bundled server ONLY if this process started it — never one the operator
               (or a surviving previous run) owns. Runs on every exit path, including a failed
               migration, AFTER the loop's data source is disposed. The IsWindows re-check is a
               CA1416 guard only — a non-null managedPostgres already implies Windows. */
            if (managedPostgres is not null && OperatingSystem.IsWindows())
            {
                await managedPostgres.StopIfStartedByThisProcessAsync();
            }
        }
    }

    /// <summary>
    /// #1647: locks <c>darling.json</c> down to the posture the DPAPI credential files already get, then
    /// VERIFIES it. The config file is not ordinary config — it holds every monitored server's
    /// <c>encryptedPassword</c>, the MCP bearer token, the web dashboard access token, and in BYO mode the
    /// store connection string. Those blobs are DPAPI <b>LocalMachine</b> scope with an entropy constant that
    /// ships in an open-source repo, so anything that can READ the file can unprotect all of it — the ACL is
    /// the access boundary, exactly as <see cref="DarlingFileSecurity"/> says of the credential files. It never
    /// got one: every harden call site targeted the credential directory, while the config sat beside the
    /// binary, and the documented install (extract the zip to <c>C:\Program Files\PerformanceMonitorDarling</c>) inherits
    /// <c>BUILTIN\Users: Read &amp; Execute</c> from the root DACL. Any local unprivileged user could read it,
    /// decrypt every SQL password, and lift the tokens that unlock the MCP write surface.
    ///
    /// <para><c>allowInteractiveRead: true</c> — the same argument the admin/viewer credentials pass, and
    /// required here: the Viewer (<c>ViewerSettings.ResolveConfigPath</c>) and the CLI verbs run as the
    /// interactive operator and must still read this file.</para>
    ///
    /// <para>Best-effort like every other ACL call (a failure is logged, never fatal — a monitoring service must
    /// not refuse to monitor over a permissions problem), but a file that is STILL readable by
    /// Users/Authenticated Users after the attempt is a <see cref="LogLevel.Critical"/>: at that point the
    /// secrets in it are recoverable by anyone with a local logon.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    private void TryHardenConfigFile(string path)
    {
        try
        {
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead: true);
        }
        catch (Exception ex)
        {
            /* The remediation is spelled out as runnable commands, not described. This exact failure sat in
               a field box's log once per service start for months and nobody acted on it, because knowing
               the ACL is wrong is not the same as knowing what to type — and the service genuinely cannot
               fix it itself: re-ACLing needs WRITE_DAC, which it does not have, and taking ownership needs
               a privilege a virtual service account is not granted. An elevated human is the only actor
               who can resolve this, so give them the three lines. */
            /* Built as ONE argument rather than repeating {Path}: a structured-logging template binds
               placeholders POSITIONALLY, so a repeated name silently consumes the next argument and the
               tail of the message renders empty — in the one log line whose entire job is to be actionable.
               The /grant names the identity the service RUNS AS, not the default virtual account — on an
               install re-homed to a domain account for integrated auth (#1823), granting the virtual
               account would be a fix that cannot work. */
            var remediation =
                $"icacls \"{path}\" /inheritance:d   then   icacls \"{path}\" {DarlingFileSecurity.RemoveOrdinaryUserGrantsArgument}   " +
                $"then   icacls \"{path}\" /grant \"{DarlingFileSecurity.ServiceAccountDisplayName}:(F)\"";

            _logger.LogError(
                "Could not restrict the ACL on {Path}{Detail} ({Message}). If the owner is not this service, the " +
                "re-ACL can never succeed — it needs ownership or FullControl — so restarting will not clear this, " +
                "and the service cannot fix it alone: taking ownership needs a privilege a virtual service account " +
                "does not have. From an ELEVATED prompt: {Remediation} — after which this service re-asserts the " +
                "full ACL by itself on the next start.",
                path, DarlingFileSecurity.DescribeOwnerAndExposure(path), ex.Message, remediation);
        }

        /* Once per start, here rather than beside every credential file: the same group is missing for all of
           them, and the harden above already ran without it (SYSTEM, Administrators and the service only). */
        if (DarlingFileSecurity.OperatorGroup is null)
        {
            _logger.LogWarning("{Warning}", DarlingFileSecurity.OperatorGroupMissingWarning);
        }

        if (DarlingFileSecurity.IsReadableByOrdinaryUsers(path))
        {
            _logger.LogCritical(
                "{Path} is READABLE by Users/Authenticated Users/Everyone/INTERACTIVE. It holds every monitored server's " +
                "encrypted password plus the MCP and web access tokens, all protected with machine-scoped DPAPI — " +
                "so any local user who can open this file can recover ALL of it. Remove the inherited read access " +
                "(or move the install out of a world-readable folder such as one created directly under C:\\).",
                path);
        }
    }

    /// <summary>
    /// #1816: the same lockdown for every EXISTING <c>darling.json.bak-*</c> beside the config. The
    /// backup-CREATION path hardens new backups as it writes them (#1786), but backups made before that
    /// fix kept whatever the folder handed them — on the field box that surfaced this, inherited
    /// <c>BUILTIN\Users</c> read, which against machine-scoped DPAPI blobs means any local account could
    /// recover every stored credential and token. The installer's security check flags them but only
    /// prints the fix; this sweep applies it, so no install carries the exposure past its next service
    /// start. <c>allowInteractiveRead: false</c>, matching the creation path — backups are rollback
    /// artifacts, nothing interactive reads them. Static with the logger passed in so the test can drive
    /// it against a scratch directory.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static void TryHardenConfigBackups(string configPath, ILogger logger)
    {
        string[] backups;
        try
        {
            var directory = Path.GetDirectoryName(Path.GetFullPath(configPath));
            if (string.IsNullOrEmpty(directory))
            {
                return;
            }

            backups = DarlingConfigBackup.ListBackups(directory, Path.GetFileName(configPath));
        }
        catch (Exception ex)
        {
            /* Enumeration failing is not worth failing the start over — the live file's own hardening
               already ran, and the installer's check remains the independent witness. */
            logger.LogWarning("Could not enumerate config backups beside {Path} for ACL hardening: {Message}", configPath, ex.Message);
            return;
        }

        foreach (var backup in backups)
        {
            /* #2093 (ghauan): idempotence gate. The rewrite below needs OWNERSHIP, which an operator's
               manual icacls fix does not transfer — so a backup that had already been hardened by hand
               kept erroring on every start about an exposure that was already closed. The sweep exists
               to close the ordinary-users-can-read gap; when that gap is closed, there is nothing left
               to do and no error to report. The CRITICAL check below remains the independent witness
               for the still-exposed case. */
            if (!DarlingFileSecurity.IsReadableByOrdinaryUsers(backup))
            {
                continue;
            }

            try
            {
                DarlingFileSecurity.HardenFile(backup, allowInteractiveRead: false);
            }
            catch (Exception ex)
            {
                /* Same contract as the live file: best-effort, but the remediation is spelled out as
                   runnable commands so an elevated human can finish the job the service cannot — naming
                   the identity the service runs as, which is not the virtual account on a re-homed install. */
                var remediation =
                    $"icacls \"{backup}\" /inheritance:d   then   icacls \"{backup}\" {DarlingFileSecurity.RemoveOrdinaryUserGrantsArgument}   " +
                    $"then   icacls \"{backup}\" /grant \"{DarlingFileSecurity.ServiceAccountDisplayName}:(F)\"";

                logger.LogError(
                    "Could not restrict the ACL on config backup {Path}{Detail} ({Message}). It carries the same " +
                    "DPAPI-protected credentials as the live config. From an ELEVATED prompt: {Remediation}",
                    backup, DarlingFileSecurity.DescribeOwnerAndExposure(backup), ex.Message, remediation);
            }

            if (DarlingFileSecurity.IsReadableByOrdinaryUsers(backup))
            {
                logger.LogCritical(
                    "Config backup {Path} is READABLE by Users/Authenticated Users/Everyone. It holds the same " +
                    "machine-scoped DPAPI credential blobs as the live config — any local user who can open it " +
                    "can recover ALL of them. Remove the inherited read access.",
                    backup);
            }
        }
    }

    /* #5450: captured at the very top of ExecuteAsync, before any store work; the fallback when the process
       start time cannot be read. */
    private DateTime _executeStartedUtc = DateTime.UtcNow;

    /// <summary>The service PROCESS start in UTC, so migrations and a runtime upgrade are not counted as downtime.</summary>
    private DateTime ProcessStartUtc()
    {
        try
        {
            using var process = Process.GetCurrentProcess();
            return process.StartTime.ToUniversalTime();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return _executeStartedUtc;
        }
    }

    /// <summary>
    /// Reads the newest pre-start collection time (#5450) and pairs it with the process start. Null for a store with no
    /// collection rows (a first install) or when the read fails.
    /// </summary>
    internal async Task<DarlingSelfAlertEvaluator.CollectionGapReport?> ReadCollectionGapAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = new NpgsqlCommand(DarlingSelfAlertEvaluator.NewestCollectionTimeSql, connection);
            command.CommandTimeout = 30;
            var value = await command.ExecuteScalarAsync(cancellationToken);
            return value is DateTime last
                ? new DarlingSelfAlertEvaluator.CollectionGapReport(
                    DateTime.SpecifyKind(last, DateTimeKind.Utc), ProcessStartUtc())
                : null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("Could not read the newest collection time at start ({Message}); the start-up gap alert is skipped.", ex.Message);
            return null;
        }
    }

    /// <summary>
    /// Maps the Windows-only bootstrap's upgrade outcome to the platform-neutral alert payload (#1706).
    /// Null for the ordinary case where the runtime did not move, and null for an extension-only update,
    /// which is a routine maintenance step the log already records rather than something to page about.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static DarlingSelfAlertEvaluator.StoreUpgradeReport? BuildStoreUpgradeReport(
        DarlingStoreUpgrade.StoreUpgradeOutcome outcome)
        => outcome.Status switch
        {
            /* outcome.Message is carried through on SUCCESS too, not just failure: a post-commit bookkeeping
               failure returns Succeeded with the warning there, and dropping it here would leave the alert
               reassuring while the log alarms. The alert is the surface an operator actually receives. */
            DarlingStoreUpgrade.StoreUpgradeStatus.Succeeded => new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                true, outcome.FromMajor, outcome.ToMajor, outcome.FromTimescale, outcome.ToTimescale,
                null, outcome.Message, outcome.UsedLinkMode),
            /* #3927: and what a failure actually put back is carried too. Leaving these at their defaults here
               would hand the alert a clean revert whatever happened, the same dropped-at-the-seam shape the
               success arm's warning once had. */
            DarlingStoreUpgrade.StoreUpgradeStatus.Failed => new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                false, outcome.FromMajor, outcome.ToMajor, outcome.FromTimescale, outcome.ToTimescale,
                outcome.FailedStep, outcome.Message, false,
                RuntimeReverted: outcome.RuntimeReverted,
                ControlFileRestored: outcome.PreUpgradeData == DarlingStoreUpgrade.PreUpgradeDataDirectory.ControlFileRestored,
                DataDirectoryNotRestored: outcome.PreUpgradeData == DarlingStoreUpgrade.PreUpgradeDataDirectory.NotRestored),
            _ => null,
        };

    /// <summary>
    /// Maps the bootstrap's TimescaleDB outcome to the alert payload (#3908). Null unless the extension could not
    /// be moved or is behind the runtime after start: an update that landed is routine maintenance the log
    /// already records, the same call <see cref="BuildStoreUpgradeReport"/> makes.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static DarlingSelfAlertEvaluator.StoreTimescaleReport? BuildStoreTimescaleReport(
        DarlingStoreUpgrade.TimescaleUpdateOutcome outcome)
        => outcome.Status switch
        {
            DarlingStoreUpgrade.TimescaleUpdateStatus.Failed => new DarlingSelfAlertEvaluator.StoreTimescaleReport(
                true, outcome.From, outcome.To, outcome.Message),
            DarlingStoreUpgrade.TimescaleUpdateStatus.Behind => new DarlingSelfAlertEvaluator.StoreTimescaleReport(
                false, outcome.From, outcome.To, outcome.Message),
            _ => null,
        };

    /// <summary>
    /// Maps a listener host's published TLS-certificate snapshot (the web host's, #3514, or the MCP host's,
    /// #5288, which share one snapshot type) to the report the evaluator consumes (#3514):
    /// a null snapshot — nothing served, or <c>Clear()</c>ed when the dashboard stopped — becomes
    /// <c>Configured=false</c> (the evaluator's resolve arm), and a live snapshot carries its validity window,
    /// identity and the host's not-yet-valid verdict (#3517) and load-refusal verdict (#5288) through unchanged —
    /// the verdict is the host's to make and this mapping must not re-derive or drop it. Pure + static so the null-to-unconfigured seam
    /// pins in a unit test rather than only through the sweep loop — the <see cref="BuildStoreUpgradeReport"/>
    /// precedent, and the seam the #3514 review flagged as previously tested only from the sides.
    /// </summary>
    internal static DarlingSelfAlertEvaluator.WebTlsCertReport BuildWebTlsCertReport(
        ListenerTlsCertificateState.Snapshot? snapshot)
        => new(
            Configured: snapshot is not null,
            NotBeforeUtc: snapshot?.NotBeforeUtc ?? default,
            NotAfterUtc: snapshot?.NotAfterUtc ?? default,
            Subject: snapshot?.Subject ?? string.Empty,
            Thumbprint: snapshot?.Thumbprint ?? string.Empty,
            RefusedNotYetValid: snapshot?.RefusedNotYetValid ?? false,
            LoadRefusal: snapshot?.LoadRefusal);

    /// <summary>
    /// Maps the fleet gate's last-hour counts to the report the "Collection Falling Behind" arm consumes (#4732).
    /// Pure and static, the <see cref="BuildWebTlsCertReport"/> precedent, so the mapping pins in a unit test.
    /// </summary>
    internal static DarlingSelfAlertEvaluator.FleetGateReport BuildFleetGateReport(
        FleetGateSnapshot snapshot, int gateWidth, DateTime nowUtc, int judgedMinutes = FleetGateStats.WindowMinutes)
        => new(
            Run: snapshot.Run,
            Skipped: snapshot.Skipped,
            QueueWaits: snapshot.QueueWaits,
            QueueWaitTotal: snapshot.QueueWaitTotal,
            QueueWaitMax: snapshot.QueueWaitMax,
            GateWidth: gateWidth,
            WindowEndUtc: nowUtc,
            JudgedMinutes: judgedMinutes);

    /// <summary>#5597: whether the hourly line is a Warning ("Collection is falling behind"): the alert is standing, or the window the
    /// alert judges meets its fire threshold. The alert's own judgment (<see cref="DarlingSelfAlertEvaluator.FleetGateReport.IsBehind"/>
    /// leaves out the minutes right after a start), never the raw counts of the full hour the line prints.</summary>
    internal static bool FleetGateLogIsBehind(bool alertStanding, DarlingSelfAlertEvaluator.FleetGateReport report) =>
        alertStanding || report.IsBehind;

    /// <summary>
    /// #5597: reads the fleet gate twice: the full last hour (what the hourly log line reports, truthfully), and the counts the
    /// "Collection Falling Behind" alert judges, which leave out the slots recorded in the first
    /// <see cref="DarlingSelfAlertEvaluator.FleetGateStartupMinutes"/> minutes after the service started (<paramref name="uptime"/>,
    /// <see cref="SkipCreditFloor.Uptime"/>; a start, never a stall, a pause or a launch-guard release). The one place that decides,
    /// so the alert and the Warning level of the line cannot disagree.
    /// </summary>
    internal static (FleetGateSnapshot Full, DarlingSelfAlertEvaluator.FleetGateReport Report) ReadFleetGate(
        FleetGateStats stats, TimeSpan? uptime, int gateWidth, DateTime nowUtc)
    {
        var full = stats.Snapshot();
        var judgedMinutes = DarlingSelfAlertEvaluator.FleetGateJudgedMinutes(uptime);
        return (full, BuildFleetGateReport(stats.SnapshotJudged(), gateWidth, nowUtc, judgedMinutes));
    }

    /// <summary>
    /// #4732: reads the fleet gate's last-hour counts, hands them to the "Collection Falling Behind" self-alert, and
    /// writes the one-line summary to the service log: once an hour, and at once when the behind/not-behind state
    /// changes (Warning while behind, Information otherwise). Behind means the alert is standing or the last hour
    /// already meets its fire threshold, so the line still says so with the alert engine absent or the master
    /// alerts switch off. Failure-isolated inside the evaluator; nothing here reads the store.
    /// </summary>
    private async Task CheckFleetGateAsync(CancellationToken cancellationToken)
    {
        if (_fleetGateStats is null || _fleetGateLog is null)
        {
            return;
        }

        var now = DateTime.UtcNow;
        var (snapshot, report) = ReadFleetGate(_fleetGateStats, _skipCreditFloor.Uptime, EffectiveSweepWidth, now);

        var standing = _selfAlerts is not null
            && await _selfAlerts.EvaluateFleetGateAsync(report, cancellationToken);
        var behind = FleetGateLogIsBehind(standing, report);

        if (!_fleetGateLog.ShouldLog(behind, now))
        {
            return;
        }

        var line = FleetGateLine.Describe(snapshot, report.GateWidth, FleetGateLine.SpanMinutes(_skipCreditFloor.Uptime));
        if (behind)
        {
            _logger.LogWarning("Collection is falling behind: {Line}", line);
        }
        else
        {
            _logger.LogInformation("{Line}", line);
        }
    }

    /// <summary>
    /// The once-per-start store host/settings profile log (#4214 ruling 9): logs the host facts and a
    /// verdict for every sizing-relevant setting, then one <c>LogWarning</c> per
    /// <see cref="HostSettingVerdict.StaleAfterHardwareChange"/> setting. Read-only and best-effort — never
    /// throws except on the caller's own <paramref name="stoppingToken"/> (real shutdown) — because ruling 9
    /// requires this to "never fail or delay startup": a bad conf file, a permission problem, or a store
    /// that is merely slow to answer must degrade to "no log line this start," not to a delayed or failed
    /// one. <see cref="ServiceCommandDeadlines.StartupHostProfileSeconds"/> is what bounds "slow" the same
    /// way <see cref="DarlingStoreHostProfile.GatherStoreFactsAsync"/>'s store-size read is deliberately
    /// NEVER reached from here — see <see cref="DarlingStoreHostProfile.GatherStartupProfileAsync"/>.
    /// </summary>
    private async Task LogStoreHostProfileAsync(
        DarlingConfig config, NpgsqlDataSource postgres, string? managedDataDirectory,
        ManagedConfWriteResult? managedConfWriteResult, CancellationToken stoppingToken)
    {
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        budget.CancelAfter(TimeSpan.FromSeconds(ServiceCommandDeadlines.StartupHostProfileSeconds));

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(budget.Token);
            var profile = await DarlingStoreHostProfile.GatherStartupProfileAsync(config.Postgres, connection, budget.Token);

            _logger.LogInformation("Store host profile:\n{Profile}", DarlingStoreHostProfile.FormatStartupProfileText(profile));

            foreach (var setting in profile.Settings)
            {
                if (setting.Verdict == HostSettingVerdict.StaleAfterHardwareChange)
                {
                    _logger.LogWarning(
                        "{Setting} is stale-after-hardware-change: current {Current}, this host would now " +
                        "derive {Derived} ({Source}). Run --check-settings for the full picture.",
                        setting.Name, setting.CurrentValueDisplay, setting.DerivedValueDisplay, setting.SourceDescription);
                }
            }

            /* #4215: on the OWNER connection, right after start, compute and store every owned
               key's verdict (collect.managed_conf_verdicts, V146) so --check-settings, the MCP self-store
               reader and the stale-setting alert can all read it back — the mcp/viewer
               roles have neither file access nor pg_file_settings visibility to compute it themselves. Same
               budget, same try/catch as the read above: never fails or delays startup. BYO stores
               (managedDataDirectory null) and the adopted-listener path skip it — nothing this service wrote
               changed, and there is no NEW conf-write result to fold in. */
            if (config.Postgres.Managed && managedDataDirectory is not null)
            {
                await DarlingStoreHostProfile.ComputeAndStoreManagedConfVerdictsAsync(
                    connection, managedDataDirectory, profile.Memory.EffectiveBytes, profile.DataVolume.FreeBytes,
                    managedConfWriteResult, _logger, budget.Token);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected. */
        }
        catch (Exception ex)
        {
            /* Never fails or delays startup (ruling 9's own words) — RunCollectionLoopAsync proceeds to role
               provisioning and the collection loop whether this logged a profile or not. The budget CTS
               surfaces as OperationCanceledException here too (with the SERVICE token untripped, the arm
               above already ruled that out), so it gets the same "did not finish" wording the CLI verb's
               own store-read backstop uses. */
            var reason = ex is OperationCanceledException && budget.IsCancellationRequested
                ? $"did not finish within {ServiceCommandDeadlines.StartupHostProfileSeconds}s"
                : ex.Message;
            _logger.LogWarning(
                "Store host profile check failed at startup ({Reason}) — collection continues; run " +
                "--check-settings to retry.", reason);
        }
    }
}
