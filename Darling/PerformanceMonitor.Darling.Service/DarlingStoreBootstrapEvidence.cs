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
using System.Runtime.Versioning;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Whether a missing managed-store credential means "nobody has started the service yet" or "a bootstrap
/// already ran and FAILED" (#2197) — the question every missing-credential CLI message answered the same
/// way, and the wrong way in the case that actually produces it in the field.
///
/// <para><b>The bug this exists for:</b> when a managed bootstrap dies, the operator's LAST message is
/// rarely the bootstrap error — it is whatever verb they run next, and every one of those said "Start the
/// PerformanceMonitor Darling service once so its first run initializes the store". In #2185 that is the
/// message the reporter led with, and it sent them to darling.json, which was never the fault. Starting a
/// service whose bootstrap has already failed just fails it again; the two situations want opposite
/// advice, so a message that cannot tell them apart has to be wrong in one of them.</para>
///
/// <para><b>Why the evidence is only ever the store's own files.</b> The tempting signal — the service's
/// log directory exists, therefore the service has run — is machine-global while the question is about ONE
/// store: a box that has monitored happily for a year has that directory, so a genuinely new second store
/// configured on it would be told its bootstrap had failed. Everything read here sits under or beside
/// <c>postgres.dataDirectory</c> and so can only describe the store being asked about. Absent evidence is
/// therefore reported as "no evidence", never as "the service has never run" — which is why the first-run
/// branch still carries one sentence for the operator who HAS already started it.</para>
/// </summary>
/* Windows-only for the same reason DarlingManagedPostgres is: every path it reads belongs to a store the
   product only builds on Windows (DPAPI credentials, the bundled runtime), and every caller is already
   platform-guarded. */
[SupportedOSPlatform("windows")]
internal static class DarlingStoreBootstrapEvidence
{
    /// <summary>
    /// The service log the reason is in, named rather than referred to (the issue's ask), and named as a
    /// SHAPE rather than today's file: the run that failed may have been days ago, and one file per day is
    /// itself something the operator needs to know before they go looking.
    /// </summary>
    internal static string ServiceLogPath =>
        Path.Combine(DarlingFileLoggerProvider.DefaultLogDirectory(), "darling-service_yyyyMMdd.log");

    /// <summary>
    /// What on disk proves a bootstrap was ATTEMPTED against this store, as the phrase the message quotes —
    /// null when nothing does. Quoting it keeps the verdict from being a bare assertion the operator has to
    /// take on faith, which is the failure mode the old message had.
    /// <para>Ordered by how much each one settles. A cluster with a <c>PG_VERSION</c> is past initdb; a
    /// <c>pg.log</c> means pg_ctl ran; the store's own credential is written IMMEDIATELY BEFORE initdb
    /// (<see cref="DarlingManagedPostgres"/>'s InitializeClusterAsync), so it survives the exact field
    /// failure — an initdb that Windows killed in the loader — and is what makes this branch reachable at
    /// all for the role-credential verbs.</para>
    /// </summary>
    internal static string? FindBootstrapEvidence(string dataDirectory)
    {
        if (string.IsNullOrWhiteSpace(dataDirectory))
        {
            /* No store to look at — never let an empty path become a relative one probed against the
               working directory, which would answer about whatever the operator happened to cd into. */
            return null;
        }

        try
        {
            if (File.Exists(Path.Combine(dataDirectory, "PG_VERSION")))
            {
                return $"the cluster in {dataDirectory} is already initialized";
            }

            var storeFolder = Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(dataDirectory)));
            if (!string.IsNullOrEmpty(storeFolder))
            {
                var serverLog = Path.Combine(storeFolder, DarlingManagedPostgres.ServerLogFileName);
                if (File.Exists(serverLog))
                {
                    return $"the store's server log {serverLog} is already there";
                }

                var storeCredential = Path.Combine(storeFolder, DarlingManagedPostgres.CredentialFileName);
                if (File.Exists(storeCredential))
                {
                    return $"{storeCredential} is already there, and the service writes that one immediately before it runs initdb";
                }

                foreach (var roleCredential in s_roleCredentialFileNames)
                {
                    var path = Path.Combine(storeFolder, roleCredential);
                    if (File.Exists(path))
                    {
                        return $"{path} is already there";
                    }
                }
            }

            if (Directory.Exists(dataDirectory) && Directory.EnumerateFileSystemEntries(dataDirectory).Any())
            {
                return $"{dataDirectory} already holds a partly-built cluster";
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
        {
            /* A diagnostic must never become a second exception: an unreadable or malformed data directory
               costs the operator the sharper branch, not the message. */
        }

        return null;
    }

    /// <summary>The role credentials, provisioned AFTER the cluster is up — weaker evidence than the store's
    /// own credential, but any of them proves the bootstrap got past initdb.</summary>
    private static readonly string[] s_roleCredentialFileNames =
    [
        DarlingManagedPostgres.AdminCredentialFileName,
        DarlingManagedPostgres.ViewerCredentialFileName,
        DarlingManagedPostgres.McpCredentialFileName,
    ];

    /// <summary>
    /// The shared missing-credential message, in whichever of the two voices the evidence earns. The lead
    /// clause is deliberately the same in both ("&lt;subject&gt; does not exist"), because the old wording is
    /// already searchable in field reports and the issue tracker — the branch changes what follows it, not
    /// what an operator pastes into a search box.
    /// </summary>
    /// <param name="subject">The missing thing, naming its exact path.</param>
    /// <param name="firstRunAction">What a genuine first run does, e.g. "initializes the store".</param>
    /// <param name="dataDirectory">The store this verb is asking about.</param>
    internal static string MissingCredentialMessage(string subject, string firstRunAction, string dataDirectory)
    {
        var evidence = FindBootstrapEvidence(dataDirectory);
        if (evidence is null)
        {
            return
                $"{subject} does not exist yet. Start the PerformanceMonitor Darling service once so its first run " +
                $"{firstRunAction}, then re-run this command. If you have ALREADY started it, its first run never got " +
                $"this far and starting it again will not either — the reason is in the service log ({ServiceLogPath}), " +
                "not in darling.json.";
        }

        return
            $"{subject} does not exist, and this is NOT a first run: {evidence}. The service has already run against " +
            "this store and its bootstrap stopped before it got this far, so starting it again is not the fix. Read " +
            $"the newest service log ({ServiceLogPath}) and work the FIRST error in it — a bundled Postgres tool that " +
            "Windows killed is decoded there in words rather than left as a bare exit code. Nothing in darling.json " +
            "produces this.";
    }

    /// <summary>
    /// The message for the store's OWN credential — the one four managed-store verbs refuse on, and the one
    /// they never named a path for. Resolves the store's paths itself, and degrades (to the first-run branch,
    /// and to a subject without a path) rather than throwing when the configured <c>dataDirectory</c> is
    /// unresolvable: a message builder that can throw turns a diagnosable failure into an unhandled one.
    /// </summary>
    internal static string MissingStoreCredentialMessage(PostgresConfig postgres)
    {
        string dataDirectory;
        string subject;
        try
        {
            dataDirectory = DarlingManagedPostgres.ResolveDataDirectory(postgres);
            subject = $"The managed store credential ({DarlingManagedPostgres.CredentialPathFor(dataDirectory)})";
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or InvalidOperationException or IOException or UnauthorizedAccessException)
        {
            dataDirectory = string.Empty;
            subject = "The managed store credential";
        }

        return MissingCredentialMessage(subject, "initializes the store", dataDirectory);
    }
}
