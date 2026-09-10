/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The service-orchestrated in-place upgrade of an existing managed store (#1706) — the runtime half of
/// the ConfMarker/migrations discipline. Before this existed a field store ran whatever PostgreSQL and
/// TimescaleDB it was first initialized with, FOREVER: a deploy swaps the app binaries but
/// <see cref="DarlingManagedPostgres"/>'s runtime probe short-circuits on an already-extracted
/// <c>pg-runtime\pgsql\bin\pg_ctl.exe</c>, so a package carrying a newer runtime changed nothing. That is
/// the drift #1705 caught the hard way (a store still on its original extension while the bundle shipped
/// 2.28.1, which is how the <c>scheduled =&gt;</c> signature gap survived).
///
/// <para><b>The stamp is the trigger.</b> Every extraction records the SHA256 of the zip it came from in
/// <see cref="RuntimeStampFileName"/>. A start whose zip hash differs from the stamp means the package
/// carries a new runtime, and that ONE signal covers both halves of the problem: a TimescaleDB-only bump
/// (same PostgreSQL major, new extension) and a PostgreSQL MAJOR jump (17 to 18). Hashing 50 MB once per
/// service start is cheap next to being wrong about what is on disk.</para>
///
/// <para><b>Why both runtimes must survive.</b> <c>pg_upgrade</c> needs the OLD binaries and the NEW
/// binaries present at the same time — it starts each cluster with its own postmaster. So the swap
/// RESCUES the current <c>pg-runtime\pgsql</c> to <c>pg-runtime-prev\pgsql</c> before extracting the new
/// zip, and that rescued tree (not just <c>bin</c>: the old postmaster loads <c>$libdir</c> relative to
/// itself, so <c>lib</c> and <c>share</c> come too) is <c>--old-bindir</c>'s home. Rescue BEFORE extract is
/// not an ordering preference; get it backwards and the old binaries are gone and the store is
/// unupgradeable.</para>
///
/// <para><b>The TimescaleDB bridge is mandatory, not hygiene.</b> pg_upgrade's binary-upgrade dump
/// recreates the extension pinned at the version the OLD cluster has installed, and every TimescaleDB
/// function resolves to a VERSIONED library (<c>$libdir/timescaledb-2.28.1</c>). The new runtime ships
/// exactly one such library. A store sitting on 2.17.2 therefore fails the upgrade with a missing-library
/// error unless its extension is first updated, ON THE OLD CLUSTER, to the version the new bundle carries
/// — which is also what Timescale documents ("the version of TimescaleDB must be the same before and after
/// the PostgreSQL upgrade"). Hence: bridge first, upgrade second.</para>
///
/// <para><b>Failure isolation.</b> Copy mode leaves the old data directory byte-for-byte untouched, so
/// every failure path reverts to the old runtime + old data directory and the store keeps running on its
/// original major. A failure also RECORDS the failing zip's hash in <see cref="RuntimeBlockedFileName"/>
/// so the next start does not retry the same known-bad package on a loop — a different zip clears it.
/// Nothing here ever leaves the store down.</para>
/// </summary>
[SupportedOSPlatform("windows")]
internal sealed class DarlingStoreUpgrade
{
    /// <summary>Records the SHA256 of the zip the extracted runtime came from — the change detector.</summary>
    public const string RuntimeStampFileName = "pg-runtime.stamp";

    /// <summary>
    /// Records the SHA256 of a zip whose upgrade FAILED. A start that sees the same hash skips the swap
    /// (logging why) instead of retrying a known-bad package every time the service restarts; shipping a
    /// different zip changes the hash and re-arms the attempt.
    /// </summary>
    public const string RuntimeBlockedFileName = "pg-runtime.blocked";

    /// <summary>Suffix on the runtime root holding the rescued previous runtime (pg_upgrade's --old-bindir).</summary>
    public const string PreviousRuntimeSuffix = "-prev";

    /* Every directory naming this class can put BESIDE the data directory lives here, because
       ReportUnmanagedStoreCopies decides what is a stranger's by elimination — anything store-shaped that is
       not one of ours. A new sibling naming that forgets to register here does not fail loudly; it gets
       reported to the operator as something the product did not create, which is the one way that diagnostic
       can lie. DarlingStoreUpgradeSiblingNamesTests pins the pair against the source. The runtime namings
       (PreviousRuntimeSuffix, and the ".failed" move-aside) are deliberately NOT here: they hold binaries,
       never a PG_VERSION, so the structural test cannot reach them. */

    /// <summary>Suffix on the data directory holding a retained pre-upgrade copy, kept for rollback.</summary>
    public const string RetainedDataDirectorySuffix = "-old-";

    /// <summary>Suffix on the data directory holding the new cluster an in-place upgrade builds into.</summary>
    public const string UpgradeStagingDirectorySuffix = "-upgrade-";

    /// <summary>
    /// How many service starts the pre-upgrade data directory is kept before deletion. TWO, not one: the
    /// first start after the upgrade is the one that proves the new cluster serves real collection, and a
    /// store that came up, misbehaved, and got restarted still has its rollback copy on that second start.
    /// </summary>
    public const int RollbackRetentionStarts = 2;

    /// <summary>Slack above the measured 2x requirement for copy mode, so a copy never fills the volume dead.</summary>
    private const long CopyHeadroomSlackBytes = 1L * 1024 * 1024 * 1024;

    /// <summary>
    /// Wall-clock ceiling on measuring the store-shaped directories reported at every service start. Five
    /// seconds for the whole report, not per directory: what needs bounding is the delay before the store
    /// comes up. An ordinary cluster of a few thousand files walks in well under it, so this only engages on
    /// the pathological directory it exists to survive — and when it does, the log says the number is a lower
    /// bound rather than pretending otherwise.
    /// </summary>
    private static readonly TimeSpan s_sizeProbeBudget = TimeSpan.FromSeconds(5);

    /* pg_upgrade on a large field store is genuinely long-running in copy mode, and the store is offline
       for the duration — but a partial copy is worse than a slow one, so the budget is generous rather
       than tight. initdb/pg_controldata are quick; the bridge has to tolerate a big catalog rewrite. */
    private static readonly TimeSpan s_pgUpgradeTimeout = TimeSpan.FromHours(4);

    /// <summary>
    /// The DRY RUN gets a far tighter budget than the real pass. <c>--check</c> copies nothing — it reads
    /// catalogs and compares settings — so minutes is generous where the copy legitimately needs hours. The
    /// asymmetry matters because this is the step that runs FIRST and with the store already offline: giving
    /// a stuck check the copy's budget would hold the store down for hours before the revert, which is a
    /// worse outcome than any upgrade is worth.
    /// </summary>
    private static readonly TimeSpan s_pgUpgradeCheckTimeout = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan s_initDbTimeout = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan s_toolTimeout = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan s_bridgeTimeout = TimeSpan.FromHours(1);
    private static readonly TimeSpan s_analyzeTimeout = TimeSpan.FromHours(2);

    private readonly ILogger _logger;

    public DarlingStoreUpgrade(ILogger logger)
        => _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /* ============================ outcome ============================ */

    internal enum StoreUpgradeStatus
    {
        /// <summary>Nothing to do — the data directory already matches the bundled major.</summary>
        None,

        /// <summary>The data directory was upgraded to the bundled major and verified.</summary>
        Succeeded,

        /// <summary>A step failed; the store reverted to its previous runtime + data directory and keeps running.</summary>
        Failed,

        /// <summary>Only the TimescaleDB extension moved (same PostgreSQL major) — the #1705 case.</summary>
        ExtensionUpdated,
    }

    /// <summary>
    /// What one start's upgrade attempt did, carried out of the bootstrap so the worker can raise a real
    /// self-alert once the store (and therefore the alert engine) is up. The store is DOWN while the
    /// upgrade runs, so the start of the work can only be a log line; both terminal states happen with a
    /// live store and are alertable.
    /// </summary>
    internal sealed record StoreUpgradeOutcome(
        StoreUpgradeStatus Status,
        int FromMajor,
        int ToMajor,
        string? FromTimescale,
        string? ToTimescale,
        string? FailedStep,
        string? Message,
        bool UsedLinkMode)
    {
        public static StoreUpgradeOutcome None { get; } =
            new(StoreUpgradeStatus.None, 0, 0, null, null, null, null, false);
    }

    /* ============================ pure helpers (unit-tested) ============================ */

    /// <summary>
    /// Parses the major from a PostgreSQL tool's <c>--version</c> line ("pg_ctl (PostgreSQL) 18.4" =&gt; 18).
    /// Tolerates the beta/rc forms ("18beta1") and a bare major ("18"). Null when nothing parses, which the
    /// caller treats as "cannot determine" and refuses to act on — guessing a major is how you point 18
    /// binaries at a 17 data directory.
    /// </summary>
    internal static int? ParsePostgresMajor(string? versionOutput)
    {
        if (string.IsNullOrWhiteSpace(versionOutput))
        {
            return null;
        }

        /* Walk to the LAST parenthesised group's tail — the version always trails the product name — then
           take the leading digit run of the first token that starts with a digit. */
        foreach (var token in versionOutput.Split(new[] { ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
        {
            if (token.Length == 0 || !char.IsDigit(token[0]))
            {
                continue;
            }

            var digits = 0;
            while (digits < token.Length && char.IsDigit(token[digits]))
            {
                digits++;
            }

            if (int.TryParse(token[..digits], NumberStyles.None, CultureInfo.InvariantCulture, out var major) && major > 0)
            {
                return major;
            }
        }

        return null;
    }

    /// <summary>
    /// The major recorded in a data directory's <c>PG_VERSION</c> (a bare "17"). Same refuse-on-garbage
    /// posture as <see cref="ParsePostgresMajor"/>.
    /// </summary>
    internal static int? ParseDataDirectoryMajor(string? pgVersionFileContent)
        => ParsePostgresMajor(pgVersionFileContent?.Trim());

    /// <summary>
    /// The <c>default_version</c> a runtime's <c>share\extension\timescaledb.control</c> declares — the
    /// exact version the bundled library file is named for, and therefore the version the old cluster's
    /// extension must be bridged TO before pg_upgrade can resolve its functions.
    /// </summary>
    internal static string? ParseTimescaleDefaultVersion(string? controlFileText)
    {
        if (string.IsNullOrWhiteSpace(controlFileText))
        {
            return null;
        }

        foreach (var rawLine in controlFileText.Split('\n'))
        {
            var line = rawLine.Trim();
            if (line.StartsWith('#')
                || !line.StartsWith("default_version", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var equals = line.IndexOf('=');
            if (equals < 0)
            {
                continue;
            }

            var value = line[(equals + 1)..].Trim().Trim('\'', '"').Trim();
            return value.Length == 0 ? null : value;
        }

        return null;
    }

    /// <summary>How pg_upgrade should move the data files, and why.</summary>
    internal enum FileTransferMode
    {
        /// <summary>The safe default: the old cluster survives the upgrade completely intact.</summary>
        Copy,

        /// <summary>Hard links — fast and nearly free on space, but the old cluster is unusable once the
        /// new one starts, so there is no rollback but a restore from backup.</summary>
        Link,

        /// <summary>Neither is safe on this volume — do not upgrade, keep running the old major.</summary>
        Abort,
    }

    internal sealed record TransferDecision(FileTransferMode Mode, string Reason);

    /// <summary>
    /// Chooses the pg_upgrade file-transfer mode from measured space (PURE, so the arithmetic is pinned by
    /// tests rather than discovered on a field box at 3am). Copy needs room for a SECOND full copy of the
    /// data directory plus slack; when that is not there, hard links need only the new cluster's own
    /// catalogs, so link mode is offered — but only when the volume actually supports hard links, and
    /// always as a LOUD downgrade because it trades the rollback away. Neither affordable means abort, which
    /// leaves the store exactly as it was: running, on the old major.
    /// </summary>
    internal static TransferDecision DecideTransferMode(long dataDirectoryBytes, long freeBytes, bool hardLinksSupported)
    {
        var copyNeeds = dataDirectoryBytes + dataDirectoryBytes + CopyHeadroomSlackBytes;
        if (freeBytes >= copyNeeds)
        {
            return new TransferDecision(
                FileTransferMode.Copy,
                $"{FormatBytes(freeBytes)} free covers the {FormatBytes(copyNeeds)} a copy needs (data {FormatBytes(dataDirectoryBytes)} x2 + 1 GB slack)");
        }

        /* Link mode still writes a fresh cluster's catalogs and the copied non-relation files; a tenth of
           the data directory plus the slack is a deliberately conservative floor for that. */
        var linkNeeds = (dataDirectoryBytes / 10) + CopyHeadroomSlackBytes;
        if (!hardLinksSupported)
        {
            return new TransferDecision(
                FileTransferMode.Abort,
                $"only {FormatBytes(freeBytes)} free (a copy needs {FormatBytes(copyNeeds)}) and this volume does not support hard links, so link mode is unavailable");
        }

        if (freeBytes >= linkNeeds)
        {
            return new TransferDecision(
                FileTransferMode.Link,
                $"only {FormatBytes(freeBytes)} free (a copy needs {FormatBytes(copyNeeds)}) — falling back to hard-link mode, which does NOT leave a rollback copy");
        }

        return new TransferDecision(
            FileTransferMode.Abort,
            $"only {FormatBytes(freeBytes)} free — not even hard-link mode's {FormatBytes(linkNeeds)} is available");
    }

    internal static string FormatBytes(long bytes)
    {
        if (bytes >= 1024L * 1024 * 1024)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{bytes / (1024.0 * 1024 * 1024):0.#} GB");
        }

        return string.Create(CultureInfo.InvariantCulture, $"{bytes / (1024.0 * 1024):0.#} MB");
    }

    /// <summary>The locale/encoding/checksum identity of an existing cluster, read from it rather than assumed.</summary>
    internal sealed record ClusterIdentity(
        string Encoding,
        string Collate,
        string Ctype,
        string? LocaleProvider,
        string? Locale,
        bool DataChecksums);

    /// <summary>
    /// Builds the <c>initdb</c> arguments that reproduce <paramref name="identity"/> on the new cluster.
    /// EVERY locale/encoding/checksum knob is passed EXPLICITLY, never left to the new major's defaults,
    /// because those defaults move: PostgreSQL 18 flipped initdb to enable data checksums by default (a
    /// documented incompatibility), and pg_upgrade hard-refuses a checksum mismatch between clusters. The
    /// same reasoning covers the locale provider, which gained a "builtin" option in 17. Pure, so the exact
    /// argument string is pinned by tests.
    /// </summary>
    internal static string BuildInitDbArguments(
        string newDataDirectory, string userName, string passwordFilePath, ClusterIdentity identity, int newMajor)
    {
        var builder = new StringBuilder();
        builder.Append("-D \"").Append(newDataDirectory).Append('"');
        builder.Append(" -U ").Append(userName);
        builder.Append(" -A scram-sha-256");
        builder.Append(" --pwfile=\"").Append(passwordFilePath).Append('"');
        builder.Append(" -E ").Append(identity.Encoding);

        switch (identity.LocaleProvider)
        {
            case "i":
                builder.Append(" --locale-provider=icu");
                if (!string.IsNullOrWhiteSpace(identity.Locale))
                {
                    builder.Append(" --icu-locale=").Append(identity.Locale);
                }

                break;

            case "b":
                builder.Append(" --locale-provider=builtin");
                if (!string.IsNullOrWhiteSpace(identity.Locale))
                {
                    builder.Append(" --builtin-locale=").Append(identity.Locale);
                }

                break;

            case "c":
                builder.Append(" --locale-provider=libc");
                break;

            default:
                /* Unknown/absent provider column (a cluster older than the provider split): let the
                   lc-collate/lc-ctype pair below carry the locale, which is what those clusters used. */
                break;
        }

        /* Always explicit, for every provider: even ICU and builtin clusters carry an LC_COLLATE/LC_CTYPE
           pair that pg_upgrade compares. */
        builder.Append(" --lc-collate=").Append(identity.Collate);
        builder.Append(" --lc-ctype=").Append(identity.Ctype);

        if (identity.DataChecksums)
        {
            builder.Append(" --data-checksums");
        }
        else if (newMajor >= 18)
        {
            /* --no-data-checksums exists only from 18, which is also the first major whose default is ON,
               so an older new-cluster needs no flag to land checksum-less. */
            builder.Append(" --no-data-checksums");
        }

        return builder.ToString();
    }

    /// <summary>
    /// Server options pg_upgrade passes to BOTH clusters it starts, for the upgrade window only. The
    /// store's own postgresql.conf is never edited; <c>-c</c> on the command line simply outranks it, the
    /// same mechanism <see cref="DarlingManagedPostgres.BuildServerRuntimeOptions"/> already uses to force
    /// loopback at a normal start.
    ///
    /// <para><b>timescaledb.max_background_workers=0</b> — deadlock avoidance, not tuning. TimescaleDB's
    /// scheduler background worker connects to databases and takes locks on its own schedule, including on
    /// <c>template0</c>, which timescale/timescaledb#1593 documents deadlocking against a restore needing
    /// the same lock, in a cycle PostgreSQL's deadlock detector does not break. pg_upgrade is exactly that
    /// workload. Nothing the scheduler could do during an upgrade is wanted anyway — its jobs would operate
    /// on data being copied out from under them.</para>
    ///
    /// <para><b>listen_addresses=localhost</b> — the IPv4/IPv6 loopback trap, caught in pg_upgrade's own
    /// diagnostics: <c>connection to server at "localhost" (::1), port 50432 failed</c>. pg_upgrade has no
    /// Unix sockets on Windows, so it connects to its clusters BY NAME, and Windows resolves
    /// <c>localhost</c> to <c>::1</c> first. The managed v1 conf block pins
    /// <c>listen_addresses = '127.0.0.1'</c> — IPv4 ONLY — so on a real managed store there is nothing on
    /// <c>::1</c> for it to reach. Restoring the stock <c>localhost</c> value for the upgrade window binds
    /// both families, which is what pg_upgrade expects. This is the same trap
    /// <see cref="DarlingManagedPostgres.BuildConnectionString"/> already documents and dodges by using the
    /// literal <c>127.0.0.1</c> rather than the name — the lesson simply had never been applied to a tool
    /// that dials on our behalf.</para>
    /// </summary>
    internal const string QuiesceTimescaleServerOptions =
        "-c timescaledb.max_background_workers=0 -c listen_addresses=localhost";

    /// <summary>
    /// The ports pg_upgrade runs its throwaway old/new postmasters on. pg_upgrade defaults BOTH to
    /// <c>50432</c>, a fixed well-known value — so two upgrades on one host, or any leftover postmaster from
    /// an interrupted one, land on the same port and pg_upgrade silently talks to a STRANGER'S cluster. That
    /// is not hypothetical: it was caught here as <c>FATAL: role "darling" does not exist</c> coming back
    /// from a postmaster this service never started. These are deliberately obscure and distinct from each
    /// other, and from the store's own configured port. They are internal to the upgrade window — nothing
    /// connects to them but pg_upgrade itself.
    /// </summary>
    internal const int UpgradeOldClusterPort = 55432;
    internal const int UpgradeNewClusterPort = 55433;

    /// <summary>
    /// Which of <paramref name="ports"/> already have a listener among <paramref name="activeListeners"/>.
    /// PURE, so the decision is pinned by tests rather than by whatever happens to be bound on a dev box.
    /// </summary>
    internal static IReadOnlyList<int> FindOccupiedPorts(IEnumerable<IPEndPoint> activeListeners, params int[] ports)
    {
        var occupied = new List<int>();
        var listening = new HashSet<int>();
        foreach (var endpoint in activeListeners)
        {
            listening.Add(endpoint.Port);
        }

        foreach (var port in ports)
        {
            if (listening.Contains(port))
            {
                occupied.Add(port);
            }
        }

        return occupied;
    }

    /// <summary>
    /// Refuses to start pg_upgrade when either of its private ports already has a listener.
    ///
    /// <para>Moving off pg_upgrade's fixed default 50432 removed the collision with OTHER software; it did
    /// not remove the collision with OURSELVES. The likeliest squatter on 55432/55433 is a previous run of
    /// this very upgrade: <see cref="s_pgUpgradeTimeout"/> elapses, the runner gives up, and pg_upgrade's
    /// throwaway postmasters can outlive it (<see cref="TryStopAsync"/> stops the store's own cluster, not
    /// pg_upgrade's). The next start would then hand pg_upgrade a stranger's cluster to inspect — tonight's
    /// failure, one retry later, on a private port. Failing honest and naming the port is worth far more
    /// than a silent wrong answer, so this closes the class rather than relocating it.</para>
    ///
    /// <para>Not covered, deliberately: two Darling services on ONE host upgrading at the same moment share
    /// these ports. That is out of scope for a single-store-per-host product, and this check turns it into a
    /// clear error rather than cross-cluster corruption.</para>
    /// </summary>
    private void AssertUpgradePortsFree()
    {
        IPEndPoint[] listeners;
        try
        {
            listeners = System.Net.NetworkInformation.IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners();
        }
        catch (Exception ex) when (ex is NetworkInformationException or PlatformNotSupportedException)
        {
            /* Cannot enumerate listeners — do not block the upgrade on a diagnostic we could not run. */
            _logger.LogWarning("Could not check whether the upgrade ports are free ({Message}); continuing.", ex.Message);
            return;
        }

        var occupied = FindOccupiedPorts(listeners, UpgradeOldClusterPort, UpgradeNewClusterPort);
        if (occupied.Count == 0)
        {
            return;
        }

        throw new InvalidOperationException(
            $"port {string.Join(" and ", occupied)} already has a listener, and pg_upgrade needs " +
            $"{UpgradeOldClusterPort} and {UpgradeNewClusterPort} to itself for the clusters it starts. " +
            "Something else is bound there — most likely a postmaster left behind by an interrupted upgrade. " +
            "Stop it (or reboot) and restart the service; the store keeps running on its current major meanwhile. " +
            "Continuing would let pg_upgrade inspect that process's cluster instead of this store's, which it " +
            "cannot detect and which would produce a wrong answer with no error to act on.");
    }

    /// <summary>
    /// The pg_upgrade command line. <c>--check</c> first as a dry run (it validates locale/encoding/
    /// checksum compatibility and the loadable-library set WITHOUT touching either cluster), then the real
    /// pass. <c>-o</c>/<c>-O</c> carry <paramref name="serverOptions"/> to the old and new clusters
    /// respectively. Pure so every form is pinned by tests.
    /// </summary>
    internal static string BuildPgUpgradeArguments(
        string oldBinDirectory,
        string newBinDirectory,
        string oldDataDirectory,
        string newDataDirectory,
        string userName,
        FileTransferMode mode,
        bool checkOnly,
        int jobs,
        /* REQUIRED, no default. Omitting it silently reproduces the loopback hang and the compiler would not
           say a word — a third call site that forgets is exactly how this regresses. Callers that genuinely
           want no server options pass null explicitly, which is a decision a reviewer can see. */
        string? serverOptions)
    {
        var builder = new StringBuilder();
        builder.Append("--old-bindir \"").Append(oldBinDirectory).Append('"');
        builder.Append(" --new-bindir \"").Append(newBinDirectory).Append('"');
        builder.Append(" --old-datadir \"").Append(oldDataDirectory).Append('"');
        builder.Append(" --new-datadir \"").Append(newDataDirectory).Append('"');
        builder.Append(" --username ").Append(userName);

        /* Never the default 50432 — see UpgradeOldClusterPort. */
        builder.Append(" --old-port ").Append(UpgradeOldClusterPort.ToString(CultureInfo.InvariantCulture));
        builder.Append(" --new-port ").Append(UpgradeNewClusterPort.ToString(CultureInfo.InvariantCulture));

        if (mode == FileTransferMode.Link)
        {
            builder.Append(" --link");
        }

        if (checkOnly)
        {
            builder.Append(" --check");
        }
        else if (jobs > 1)
        {
            /* Jobs only help the real pass; --check does no file work. */
            builder.Append(" --jobs ").Append(jobs.ToString(CultureInfo.InvariantCulture));
        }

        if (!string.IsNullOrWhiteSpace(serverOptions))
        {
            /* -o = the OLD cluster's postmaster, -O = the NEW one. Both, because pg_upgrade starts both
               and the setting has to hold on whichever side is being connected to. Quoted as one argument;
               the value itself must contain no interior double quotes (the builders here never emit any). */
            builder.Append(" -o \"").Append(serverOptions).Append('"');
            builder.Append(" -O \"").Append(serverOptions).Append('"');
        }

        return builder.ToString();
    }

    /* ============================ runtime stamp + swap ============================ */

    /// <summary>SHA256 of a file as lowercase hex — the runtime zip's identity.</summary>
    internal static string ComputeFileHash(string path)
    {
        using var stream = File.OpenRead(path);
        using var sha = System.Security.Cryptography.SHA256.Create();
        return Convert.ToHexString(sha.ComputeHash(stream)).ToLowerInvariant();
    }

    internal static string PreviousRuntimeRootFor(string runtimeRoot)
        => Path.TrimEndingDirectorySeparator(Path.GetFullPath(runtimeRoot)) + PreviousRuntimeSuffix;

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CreateHardLinkW(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);

    /// <summary>
    /// Whether <paramref name="directory"/>'s volume actually supports hard links, answered by MAKING one
    /// rather than by inferring from the file-system name: ReFS and some redirected/network volumes report
    /// plausibly and then refuse. Both probe files are removed. Never throws — an unanswerable probe is a
    /// "no", which only ever costs an upgrade that would have needed link mode anyway.
    /// </summary>
    internal static bool SupportsHardLinks(string directory)
    {
        var source = Path.Combine(directory, $"pm-hardlink-probe-{Guid.NewGuid():N}.tmp");
        var link = source + ".link";
        try
        {
            File.WriteAllText(source, "probe");
            var created = CreateHardLinkW(link, source, IntPtr.Zero);
            return created && File.Exists(link);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException)
        {
            return false;
        }
        finally
        {
            TryDeleteFile(link);
            TryDeleteFile(source);
        }
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* A probe leftover is harmless. */
        }
    }

    /// <summary>
    /// Total bytes of every file under <paramref name="directory"/>; unreadable entries are skipped.
    ///
    /// <para>Walks <see cref="DirectoryInfo"/> rather than paths on purpose: the <see cref="FileInfo"/>
    /// objects it yields carry the size from the directory enumeration itself, where reading
    /// <c>new FileInfo(path).Length</c> costs a fresh metadata call PER FILE. On a store data directory —
    /// one relation file per chunk per index, so tens of thousands of files, and #1770 measures several
    /// copies of one on every service start — that is the difference between a directory walk and tens of
    /// thousands of syscalls on the startup path.</para>
    /// </summary>
    internal static long MeasureDirectoryBytes(string directory)
        => MeasureDirectoryBytes(directory, deadline: null, out _);

    /// <summary>
    /// <see cref="MeasureDirectoryBytes(string)"/> with a wall-clock ceiling. Returns what it managed to add
    /// up and sets <paramref name="complete"/> false when <paramref name="deadline"/> cut the walk short, so
    /// a caller can say "at least" instead of stating a number it did not finish computing.
    ///
    /// <para>The report path needs this because it measures foreign data directories on EVERY service start,
    /// before the store is up, on exactly the low-headroom hosts the feature exists for. A budget is the
    /// right shape rather than caching by mtime: it bounds the cost directly and needs no state that can go
    /// stale, and in the ordinary case — a few thousand files per cluster — the walk finishes far inside it,
    /// so the ceiling only ever engages on the pathological directory it exists to survive.</para>
    /// </summary>
    internal static long MeasureDirectoryBytes(string directory, DateTime? deadline, out bool complete)
    {
        long total = 0;
        complete = true;
        var checkedFiles = 0;
        try
        {
            foreach (var file in new DirectoryInfo(directory).EnumerateFiles("*", SearchOption.AllDirectories))
            {
                /* DateTime.UtcNow per file would itself be a cost on a walk this size; every 512 entries is
                   often enough to bound the overrun to a fraction of the budget. */
                if (deadline is not null && ++checkedFiles % 512 == 0 && DateTime.UtcNow > deadline.Value)
                {
                    complete = false;
                    break;
                }

                try
                {
                    total += file.Length;
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    /* A file that vanished mid-walk does not change the order of magnitude. */
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Partial measurement still beats no measurement; the caller's slack absorbs it. */
        }

        return total;
    }

    /* ============================ runtime advance (called from EnsureRuntimeAsync) ============================ */

    /// <summary>What <see cref="TryAdvanceRuntimeAsync"/> did with the shipped zip.</summary>
    internal sealed record RuntimeAdvance(bool Swapped, string? PreviousBinDirectory, string? ZipHash);

    /// <summary>
    /// The store's own PostgreSQL major, read from the data directory's <c>PG_VERSION</c>. The AUTHORITY on
    /// what the store needs, and — the reason it is used here rather than the binaries — readable without
    /// EXECUTING anything. On DARLING01 the extracted 17 binaries could not launch at all
    /// (<c>STATUS_DLL_NOT_FOUND</c>), so every check that asked the binaries what they were got "unreadable"
    /// and degraded to proceeding; the data directory answered "18" the whole time.
    /// </summary>
    internal static int? TryReadDataDirectoryMajor(string dataDirectory)
    {
        try
        {
            var pgVersion = Path.Combine(dataDirectory, "PG_VERSION");
            return File.Exists(pgVersion) ? ParseDataDirectoryMajor(File.ReadAllText(pgVersion)) : null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    /// <summary>
    /// Whether an unidentifiable runtime must STOP the service rather than be waved through, PURE so the
    /// decision is pinned without needing a broken runtime to reproduce (deleting the guard inline left the
    /// whole suite green).
    ///
    /// <para>TRUE for exactly one pairing: the store's need is KNOWN and the runtime cannot say what it is.
    /// That pairing was logged verbatim on DARLING01 as "data directory: 18, bundled runtime: unreadable —
    /// skipping the runtime version check. The store starts normally", one second before the bootstrap died
    /// on STATUS_DLL_NOT_FOUND. The degrade was backwards: the check could not run BECAUSE the binaries
    /// could not run, which is the strongest possible evidence they must not be used.</para>
    ///
    /// <para>An unreadable DATA DIRECTORY is different and does not stop anything — there is then no known
    /// requirement to violate, and refusing would brick a store over an unreadable file.</para>
    /// </summary>
    internal static bool MustRefuseUnidentifiableRuntime(int? dataMajor, int? runtimeMajor)
        => dataMajor is not null && runtimeMajor is null;

    /// <summary>
    /// The direction decision, PURE so it can be pinned without a fixture (the <see cref="DecideTransferMode"/>
    /// / <see cref="FindOccupiedPorts"/> idiom). Extracted deliberately: with the comparison buried in the I/O
    /// method, inverting it left the ENTIRE suite green — and that inversion refuses every legitimate upgrade
    /// while waving through every downgrade, which is the defect #1738 filed, doubled.
    ///
    /// <para>TRUE only for a PROVEN downgrade. Either major unknown returns false: this gate abstains rather
    /// than blocking an update it cannot evaluate, because the no-stamp branch already refuses to swap when it
    /// cannot tell the runtimes apart, and the post-extraction data-directory check is the backstop.</para>
    /// </summary>
    internal static bool IsDowngrade(int? storeMajor, int? packageMajor)
        => storeMajor is not null && packageMajor is not null && packageMajor < storeMajor;

    /// <summary>
    /// Whether the shipped package would take the store BACKWARDS. A package whose PostgreSQL major is lower
    /// than the store's data directory is never a valid update: pg_upgrade only goes up, so the data
    /// directory can never come back to meet it, and no older postmaster can open a newer cluster.
    ///
    /// <para>This is #1738, and it is not hypothetical — a PostgreSQL 17 package landed beside a PostgreSQL
    /// 18 store on DARLING01, the runtime was swapped because the majors merely DIFFERED, and the store was
    /// down for about seven minutes until the previous runtime was restored by hand. Nothing reached
    /// pg_upgrade, so no data was at risk; the store simply could not start.</para>
    ///
    /// <para>Refusing leaves the store on the runtime it already has — which is, by definition, the one that
    /// matches its data. The stamp is deliberately NOT written: recording this package as "seen" would make
    /// the refusal silent on every subsequent start, and an operator who shipped the wrong zip should keep
    /// hearing about it until they ship a right one.</para>
    /// </summary>
    private bool IsDowngradeAgainstStore(string dataDirectory, string runtimeZipPath)
    {
        var storeMajor = TryReadDataDirectoryMajor(dataDirectory);
        if (storeMajor is null)
        {
            /* No cluster yet (a fresh install), or an unreadable PG_VERSION. Nothing to downgrade. */
            return false;
        }

        var zipMajor = TryReadZipPostgresMajor(runtimeZipPath);
        if (zipMajor is null)
        {
            /* Cannot identify the package. Not provably a downgrade, so this gate abstains rather than
               blocking a legitimate update on a missing version resource — the no-stamp branch above already
               refuses to swap when it cannot tell the runtimes apart, and the data-directory-vs-runtime check
               after extraction is the backstop. */
            _logger.LogWarning(
                "Could not read the shipped package's PostgreSQL major from {Zip}, so it cannot be checked against the store's own major ({Store}). Proceeding, but a package whose version cannot be identified is worth verifying before it ships.",
                runtimeZipPath, storeMajor);
            return false;
        }

        if (!IsDowngrade(storeMajor, zipMajor))
        {
            return false;
        }

        _logger.LogCritical(
            "REFUSING the shipped Postgres runtime at {Zip}: the package is PostgreSQL {Package} but this store's data directory is PostgreSQL {Store}. A lower major is never a valid update — pg_upgrade only moves forward, so the data directory can never come back to meet it, and an older postmaster cannot open a newer cluster. The runtime is left exactly as it is, so the store keeps running on the binaries that match its data. Replace that zip with one whose PostgreSQL major is at least the store's. Its hash is deliberately NOT recorded, so this repeats on every start until it is fixed.",
            runtimeZipPath, zipMajor, storeMajor);

        /* Deliberately no TryWriteStamp: see the summary. A refusal that goes quiet is a refusal nobody acts on. */
        return true;
    }

    /// <summary>
    /// Reconciles the EXTRACTED runtime against the SHIPPED zip. When the zip's hash differs from the stamp
    /// the package carries a new runtime, so the current <c>pgsql</c> tree is rescued to
    /// <c>&lt;runtimeRoot&gt;-prev</c> (whole tree — the old postmaster resolves <c>$libdir</c> relative to
    /// its own binaries) and the new zip is extracted in its place. Returns the rescued bin directory, which
    /// is pg_upgrade's <c>--old-bindir</c>.
    ///
    /// <para>Refuses to swap under a LIVE postmaster: the running server holds its binaries open, and
    /// stopping a server this service did not start is not its call (the adopt-never-stop rule). The swap
    /// simply waits for the next service-owned start.</para>
    /// </summary>
    internal async Task<RuntimeAdvance> TryAdvanceRuntimeAsync(
        string runtimeRoot,
        string runtimeZipPath,
        string dataDirectory,
        Func<string, CancellationToken, Task<bool>> isServerRunningAsync,
        CancellationToken cancellationToken)
    {
        var pgsqlDirectory = Path.Combine(runtimeRoot, "pgsql");
        var binDirectory = Path.Combine(pgsqlDirectory, "bin");
        var stampPath = Path.Combine(runtimeRoot, RuntimeStampFileName);
        var blockedPath = Path.Combine(runtimeRoot, RuntimeBlockedFileName);

        var zipHash = await Task.Run(() => ComputeFileHash(runtimeZipPath), cancellationToken);
        var stamp = ReadTrimmedOrNull(stampPath);

        if (string.Equals(stamp, zipHash, StringComparison.OrdinalIgnoreCase))
        {
            return new RuntimeAdvance(false, null, zipHash);
        }

        if (stamp is null)
        {
            /* NO STAMP: this runtime was extracted before stamping existed (every install in the field
               today) or staged by hand. "No stamp" must NOT be read as "the zip is different" — on that
               reading, the first start after upgrading the service would swap the runtime on every host,
               including the overwhelming majority whose runtime already matches the zip byte for byte.
               Compare the actual PostgreSQL majors instead, and ADOPT a runtime that already matches by
               recording the stamp without touching anything. Costs one small extract, once per host,
               because from then on the stamp answers. */
            var installedMajor = await ReadRuntimeMajorFromBinariesAsync(binDirectory, cancellationToken);
            var zipMajor = TryReadZipPostgresMajor(runtimeZipPath);

            if (installedMajor is not null && zipMajor is not null && installedMajor == zipMajor)
            {
                /* Equal PostgreSQL majors do NOT mean equal runtimes, and adopting on the major alone would
                   re-open #1705's drift inside the machinery built to end it: a host on PG 18 + TimescaleDB
                   2.24.0 receiving PG 18 + 2.28.1 would be stamped as already-matching and keep 2.24.0
                   forever, because the new versioned library never lands and the same-major extension update
                   can then only reach the version already on disk. So the extension's library version is
                   compared too — same major but a different TimescaleDB means a same-major runtime swap
                   (rescue, extract, no pg_upgrade, extension update), which is a path that already exists. */
                var installedTimescale = TryReadInstalledTimescaleVersion(binDirectory);
                var zipTimescale = TryReadZipTimescaleVersion(runtimeZipPath);

                if (string.Equals(installedTimescale, zipTimescale, StringComparison.OrdinalIgnoreCase))
                {
                    _logger.LogInformation(
                        "Adopting the already-extracted runtime (PostgreSQL {Major}, TimescaleDB {Timescale}): it matches the shipped package, so nothing is swapped. Recording its stamp so future package changes are detectable.",
                        installedMajor, installedTimescale ?? "(none)");
                    TryWriteStamp(stampPath, zipHash);
                    return new RuntimeAdvance(false, null, zipHash);
                }

                _logger.LogWarning(
                    "The extracted runtime and the shipped package are both PostgreSQL {Major}, but their TimescaleDB differs ({Installed} on disk, {Package} in the package) — updating the runtime so the extension can actually move. This is the drift #1705 caught.",
                    installedMajor, installedTimescale ?? "(none)", zipTimescale ?? "(none)");
            }

            if (installedMajor is null || zipMajor is null)
            {
                /* Cannot tell them apart — do NOT swap on a guess. Stamp it so this is decided once, and
                   let the data-directory-vs-runtime major check be the authority on any real upgrade. */
                _logger.LogWarning(
                    "Could not determine whether the extracted runtime matches the shipped package (installed major {Installed}, package major {Package}) — leaving the runtime alone rather than swapping on a guess.",
                    installedMajor?.ToString(CultureInfo.InvariantCulture) ?? "unreadable",
                    zipMajor?.ToString(CultureInfo.InvariantCulture) ?? "unreadable");
                TryWriteStamp(stampPath, zipHash);
                return new RuntimeAdvance(false, null, zipHash);
            }

            _logger.LogWarning(
                "The extracted runtime is PostgreSQL {Installed} and the shipped package is PostgreSQL {Package} — this host has never had its runtime updated, and the update proceeds.",
                installedMajor, zipMajor);
        }

        var blocked = ReadTrimmedOrNull(blockedPath);
        if (string.Equals(blocked, zipHash, StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning(
                "The shipped Postgres runtime ({Zip}) is the SAME package whose store upgrade already failed on this host, so it will not be retried — the store keeps running its current runtime. Check the earlier upgrade failure, then ship a corrected package (a different zip re-arms the attempt) or delete {Blocked} to force a retry.",
                runtimeZipPath, blockedPath);
            return new RuntimeAdvance(false, null, zipHash);
        }

        if (await isServerRunningAsync(binDirectory, cancellationToken))
        {
            _logger.LogWarning(
                "The package ships a different Postgres runtime, but a postmaster is already running on {DataDirectory} — this service does not stop a server it did not start, so the runtime update is deferred to the next service-owned start.",
                dataDirectory);
            return new RuntimeAdvance(false, null, zipHash);
        }

        /* DIRECTION CHECK (#1738) — deliberately outside the no-stamp branch, because the stamp only says
           the zip CHANGED, never which way. A stamped host that later receives an older package downgrades
           just as surely as an unstamped one did on DARLING01. */
        if (IsDowngradeAgainstStore(dataDirectory, runtimeZipPath))
        {
            return new RuntimeAdvance(false, null, zipHash);
        }

        var previousRoot = PreviousRuntimeRootFor(runtimeRoot);
        var previousPgsql = Path.Combine(previousRoot, "pgsql");

        _logger.LogWarning(
            "The package ships a different Postgres runtime than the one extracted on this host — rescuing the current runtime to {Previous} and extracting the new one. This is the store runtime update (#1706); if the PostgreSQL major changed, an in-place pg_upgrade follows.",
            previousPgsql);

        if (Directory.Exists(previousRoot))
        {
            Directory.Delete(previousRoot, recursive: true);
        }

        Directory.CreateDirectory(previousRoot);

        try
        {
            Directory.Move(pgsqlDirectory, previousPgsql);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Something holds the runtime open (a postmaster mid-exit, an antivirus scan, a developer's
               shell sitting in bin\). A runtime we cannot rescue is a reason to SKIP the update, never a
               reason to refuse to start — the store runs perfectly well on the runtime already there.
               EnsureRuntimeAsync's contract is throw => the service exits, and a deferred runtime update
               must never spend that. */
            _logger.LogWarning(
                "Could not rescue the current runtime to {Previous} ({Message}) — something is holding it open. The store starts on its existing runtime and the update is retried on the next start.",
                previousPgsql, ex.Message);
            TryDeleteDirectory(previousRoot);
            return new RuntimeAdvance(false, null, zipHash);
        }

        try
        {
            await Task.Run(
                () => System.IO.Compression.ZipFile.ExtractToDirectory(runtimeZipPath, runtimeRoot, overwriteFiles: true),
                cancellationToken);

            if (!File.Exists(Path.Combine(binDirectory, "pg_ctl.exe")))
            {
                throw new InvalidOperationException(
                    $"Extracted {runtimeZipPath} but {binDirectory}\\pg_ctl.exe is missing — the archive does not contain pgsql\\bin.");
            }
        }
        catch (Exception)
        {
            /* The new runtime is not usable; put the old one back so the store still boots, and let the
               caller's existing error path report. Nothing has touched the data directory yet. Move-aside
               rather than delete-first, for the reason spelled out in RevertRuntime: a partial delete
               would leave an unbootable runtime behind. */
            var failedExtract = pgsqlDirectory + ".failed";
            TryDeleteDirectory(failedExtract);
            if (Directory.Exists(pgsqlDirectory))
            {
                Directory.Move(pgsqlDirectory, failedExtract);
            }

            Directory.Move(previousPgsql, pgsqlDirectory);
            TryDeleteDirectory(failedExtract);
            TryDeleteDirectory(previousRoot);
            throw;
        }

        File.WriteAllText(stampPath, zipHash);
        return new RuntimeAdvance(true, Path.Combine(previousPgsql, "bin"), zipHash);
    }

    /// <summary>
    /// Restores the rescued runtime over the newly-extracted one — the revert half of
    /// <see cref="TryAdvanceRuntimeAsync"/>. The path the service starts from is unchanged
    /// (<c>&lt;runtimeRoot&gt;\pgsql\bin</c>), so a caller holding that bin directory keeps working; only
    /// the binaries behind it go back to the previous major.
    ///
    /// <para><b>Refuses once the data directory has moved forward (#1737 item 3).</b> The two callers today
    /// cannot reach this after the swap commits — the pre-commit handler is unreachable when <c>swapped</c>
    /// because the filtered clause precedes it, and the cancel path is gated on <c>!swapped</c> — so this
    /// guard is redundant RIGHT NOW and exists for the third caller that forgets the flag. It is defence in
    /// depth against exactly one outcome, and it is the worst one available here: old binaries in front of a
    /// new data directory is a store that cannot start. #1738 is the empirical proof that the outcome is
    /// real rather than theoretical, reached by a different route.</para>
    ///
    /// <para><paramref name="expectedDataMajor"/> is the major the data directory should still be on for a
    /// revert to make sense — the pre-upgrade major. Compared against <c>PG_VERSION</c>, which needs no
    /// binaries to read, which matters because the situation that brings us here may be binaries that do not
    /// run.</para>
    /// </summary>
    internal void RevertRuntime(string runtimeRoot, string zipHash, string dataDirectory, int expectedDataMajor)
    {
        var pgsqlDirectory = Path.Combine(runtimeRoot, "pgsql");
        var previousRoot = PreviousRuntimeRootFor(runtimeRoot);
        var previousPgsql = Path.Combine(previousRoot, "pgsql");

        var actualDataMajor = TryReadDataDirectoryMajor(dataDirectory);
        if (actualDataMajor is not null && actualDataMajor != expectedDataMajor)
        {
            _logger.LogCritical(
                "REFUSING to revert the store runtime: the data directory {DataDirectory} is PostgreSQL {Actual}, not the PostgreSQL {Expected} this revert assumes. Restoring the older binaries now would leave them in front of a newer cluster and the store would not start. The current runtime is left in place. This is a bug in the caller — a revert was requested after the data directory had already moved forward.",
                dataDirectory, actualDataMajor, expectedDataMajor);
            return;
        }

        try
        {
            if (!Directory.Exists(previousPgsql))
            {
                _logger.LogCritical(
                    "Cannot revert the store runtime: the rescued copy {Previous} is gone. The service will start whatever is in {Current}.",
                    previousPgsql, pgsqlDirectory);
                return;
            }

            /* MOVE the failed runtime aside, never delete-then-move. A delete can partially succeed — a
               postmaster that has not finished exiting still holds its binaries — and the swallowed
               failure would leave a half-emptied pgsql directory that the following Move then refuses to
               overwrite. The store is then left with a runtime missing pg_ctl.exe: a self-inflicted
               unbootable install, produced by the very path that exists to make failure safe. A rename
               either works completely or fails without touching anything. */
            var failedRuntime = pgsqlDirectory + ".failed";
            TryDeleteDirectory(failedRuntime);
            if (Directory.Exists(pgsqlDirectory))
            {
                Directory.Move(pgsqlDirectory, failedRuntime);
            }

            Directory.Move(previousPgsql, pgsqlDirectory);
            TryDeleteDirectory(failedRuntime);
            TryDeleteDirectory(previousRoot);

            /* Record the failing package so the next start does not run the same doomed upgrade again, and
               drop the stamp so the runtime on disk is not claimed to be the shipped one. */
            File.WriteAllText(Path.Combine(runtimeRoot, RuntimeBlockedFileName), zipHash);
            TryDeleteFile(Path.Combine(runtimeRoot, RuntimeStampFileName));

            _logger.LogWarning("Reverted to the previous Postgres runtime — the store continues on its existing major.");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(
                "Could not revert the store runtime ({Message}). Restore {Previous} over {Current} by hand before restarting the service.",
                ex.Message, previousPgsql, pgsqlDirectory);
        }
    }

    /// <summary>
    /// The PostgreSQL major inside a runtime zip, WITHOUT extracting the whole thing: pull just
    /// <c>pgsql/bin/pg_ctl.exe</c> to a temp file and read its Windows file-version resource. Reading a
    /// version resource does not execute anything, which matters — the alternative (extract and run
    /// <c>--version</c>) would launch a binary purely to identify it. Null when the entry is missing or
    /// carries no usable version, and the caller then refuses to act on a guess.
    /// </summary>
    internal static int? TryReadZipPostgresMajor(string zipPath)
    {
        var temp = Path.Combine(Path.GetTempPath(), $"pm-runtime-probe-{Guid.NewGuid():N}.exe");
        try
        {
            using (var archive = System.IO.Compression.ZipFile.OpenRead(zipPath))
            {
                var entry = archive.GetEntry("pgsql/bin/pg_ctl.exe")
                    ?? archive.GetEntry("pgsql\\bin\\pg_ctl.exe");
                if (entry is null)
                {
                    return null;
                }

                using var source = entry.Open();
                using var destination = File.Create(temp);
                source.CopyTo(destination);
            }

            var info = System.Diagnostics.FileVersionInfo.GetVersionInfo(temp);
            return info.FileMajorPart > 0
                ? info.FileMajorPart
                : ParsePostgresMajor(info.ProductVersion ?? info.FileVersion);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidDataException or NotSupportedException)
        {
            return null;
        }
        finally
        {
            TryDeleteFile(temp);
        }
    }

    /// <summary>
    /// Pulls the TimescaleDB version out of a versioned library filename — <c>timescaledb-2.28.1.dll</c>
    /// yields <c>2.28.1</c>. The TSL sibling (<c>timescaledb-tsl-2.28.1.dll</c>) and the unversioned loader
    /// (<c>timescaledb.dll</c>) are deliberately not matched, so the answer comes from exactly one file shape.
    /// Pure.
    /// </summary>
    internal static string? ParseTimescaleLibraryVersion(string fileName)
    {
        const string prefix = "timescaledb-";
        const string suffix = ".dll";
        var name = Path.GetFileName(fileName);
        if (!name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            || !name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var version = name[prefix.Length..^suffix.Length];
        /* Reject the -tsl- variant and anything that is not a version. */
        return version.Length > 0 && char.IsDigit(version[0]) ? version : null;
    }

    /// <summary>The TimescaleDB version the EXTRACTED runtime carries, from its versioned library filename.</summary>
    private static string? TryReadInstalledTimescaleVersion(string binDirectory)
    {
        try
        {
            var pgsql = Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(binDirectory));
            if (pgsql is null)
            {
                return null;
            }

            foreach (var file in Directory.EnumerateFiles(Path.Combine(pgsql, "lib"), "timescaledb-*.dll"))
            {
                var version = ParseTimescaleLibraryVersion(file);
                if (version is not null)
                {
                    return version;
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Unreadable lib directory answers "unknown", which the caller treats as a difference. */
        }

        return null;
    }

    /// <summary>The TimescaleDB version a runtime ZIP carries, read from its entry names alone — no extract.</summary>
    internal static string? TryReadZipTimescaleVersion(string zipPath)
    {
        try
        {
            using var archive = System.IO.Compression.ZipFile.OpenRead(zipPath);
            foreach (var entry in archive.Entries)
            {
                if (entry.FullName.Replace('\\', '/').StartsWith("pgsql/lib/", StringComparison.OrdinalIgnoreCase))
                {
                    var version = ParseTimescaleLibraryVersion(entry.Name);
                    if (version is not null)
                    {
                        return version;
                    }
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidDataException)
        {
            /* Unreadable archive answers "unknown". */
        }

        return null;
    }

    /// <summary>The extracted runtime's PostgreSQL major, from <c>pg_ctl --version</c>.</summary>
    private static async Task<int?> ReadRuntimeMajorFromBinariesAsync(string binDirectory, CancellationToken cancellationToken)
    {
        try
        {
            var (exitCode, output) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(binDirectory, "pg_ctl.exe"), "--version", s_toolTimeout, cancellationToken);
            return exitCode == 0 ? ParsePostgresMajor(output) : null;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            return null;
        }
    }

    private void TryWriteStamp(string stampPath, string zipHash)
    {
        try
        {
            File.WriteAllText(stampPath, zipHash);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* An unwritable stamp only costs this probe again next start. */
            _logger.LogWarning("Could not record the runtime stamp at {Path} ({Message}).", stampPath, ex.Message);
        }
    }

    private static string? ReadTrimmedOrNull(string path)
    {
        try
        {
            return File.Exists(path) ? File.ReadAllText(path).Trim() : null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    internal static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Best effort — the caller logs the consequence that matters. */
        }
    }

    /* ============================ retained rollback copies ============================ */

    internal static string RetainedDataDirectoryFor(string dataDirectory, int oldMajor)
        => Path.TrimEndingDirectorySeparator(Path.GetFullPath(dataDirectory)) + RetainedDataDirectorySuffix + oldMajor.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Ages out the pre-upgrade data directories kept for rollback: each service start bumps a copy's
    /// counter, and the copy is deleted once it has survived <see cref="RollbackRetentionStarts"/> starts —
    /// with a log line naming the space reclaimed, because a silently vanishing multi-GB directory is its
    /// own support call. Runs on EVERY start, not only upgrade starts, since that is what makes the
    /// countdown advance.
    ///
    /// <para><b>Every retained copy is considered independently (#1770).</b> The failure handling used to sit
    /// outside the loop, so ONE copy that could not be measured or deleted — a file a not-yet-exited
    /// postmaster or an antivirus scan still holds, an ACL the service account lost — abandoned the sweep for
    /// every other copy as well, and kept abandoning it for as long as the condition lasted. Their counters
    /// stopped advancing too, so nothing aged out and multi-GB directories accumulated on exactly the hosts
    /// that can least afford them. A failure now costs that one directory its turn and nothing else.</para>
    /// </summary>
    internal void SweepRetainedDataDirectories(string dataDirectory)
    {
        string liveDataDirectory;
        string parent;
        string prefix;
        string[] retained;

        try
        {
            liveDataDirectory = Path.TrimEndingDirectorySeparator(Path.GetFullPath(dataDirectory));
            parent = Path.GetDirectoryName(liveDataDirectory) ?? string.Empty;
            prefix = Path.GetFileName(liveDataDirectory) + RetainedDataDirectorySuffix;
            if (string.IsNullOrEmpty(parent) || !Directory.Exists(parent))
            {
                return;
            }

            retained = Directory.GetDirectories(parent, prefix + "*");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Could not look for retained pre-upgrade data directories ({Message}) — any that exist are safe to delete by hand.",
                ex.Message);
            return;
        }

        foreach (var copy in retained)
        {
            /* A wildcard is not what a delete should be trusting. Directory.GetDirectories' pattern also
               matches a directory's Windows 8.3 SHORT name, so the real name is re-checked against the
               prefix before this touches anything: the only directories this deletes are the ones
               RetainedDataDirectoryFor names. */
            if (!Path.GetFileName(copy).StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            try
            {
                AgeOutRetainedDataDirectory(copy);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    "Could not age out the retained pre-upgrade store data directory {Path} ({Message}). The other retained copies were still swept, and this one is retried on the next service start — it is safe to delete by hand.",
                    copy, ex.Message);
            }
        }

        ReportUnmanagedStoreCopies(parent, liveDataDirectory, retained);
    }

    /// <summary>
    /// One retained copy's turn: bump its counter, or delete it once it has outlived
    /// <see cref="RollbackRetentionStarts"/> starts. Throws on I/O trouble so the caller can report THIS
    /// directory and carry on with the rest.
    /// </summary>
    private void AgeOutRetainedDataDirectory(string retained)
    {
        var counterPath = retained + ".starts";
        var starts = int.TryParse(ReadTrimmedOrNull(counterPath), NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)
            ? parsed + 1
            : 1;

        if (starts < RollbackRetentionStarts)
        {
            File.WriteAllText(counterPath, starts.ToString(CultureInfo.InvariantCulture));
            _logger.LogInformation(
                "Keeping the pre-upgrade store data directory {Path} for {Remaining} more service start(s) as a rollback copy.",
                retained, RollbackRetentionStarts - starts);
            return;
        }

        var reclaimed = MeasureDirectoryBytes(retained);
        Directory.Delete(retained, recursive: true);
        TryDeleteFile(counterPath);
        _logger.LogInformation(
            "Deleted the pre-upgrade store data directory {Path} after {Starts} service starts on the upgraded store, reclaiming {Size}.",
            retained, RollbackRetentionStarts, FormatBytes(reclaimed));
    }

    /// <summary>
    /// Names the store-shaped directories sitting beside the live data directory that this service did not
    /// create, with what they are costing in disk — and deletes none of them.
    ///
    /// <para>A production field instance was found carrying seven hand-made copies from a week of upgrade
    /// rehearsals, on a volume with roughly 175 GB free against a 286 GB data directory — far under the 2x a
    /// future major upgrade in copy mode needs — and nothing had ever mentioned they were there. Deleting
    /// them is not this service's call: a copy someone made by hand is a decision, and a product that
    /// silently reverses its operator's decisions is worse than one that wastes disk. But staying silent
    /// about tens of gigabytes is how a volume gets to that state unnoticed, so the copies are reported every
    /// start, by name and size, until someone removes them.</para>
    ///
    /// <para>Identified STRUCTURALLY — a directory holding a <c>PG_VERSION</c> file — and never by a name
    /// pattern. That file is what makes a directory a PostgreSQL data directory, so this cannot mistake an
    /// unrelated folder for a store copy however it happens to be named, and a report is in any case the one
    /// verdict that stays harmless if it is ever wrong.</para>
    /// </summary>
    private void ReportUnmanagedStoreCopies(string parent, string liveDataDirectory, string[] managed)
    {
        try
        {
            /* An upgrade's half-built cluster is OURS, not a stranger's, and saying otherwise in a
               diagnostic is how a diagnostic stops being believed. UpgradeDataDirectoryAsync deletes it on
               every failure path, but only best-effort — so a held file, or a machine that lost power
               mid-upgrade, leaves one behind and this is the next thing to see it. */
            var upgradePrefix = Path.GetFileName(liveDataDirectory) + UpgradeStagingDirectorySuffix;

            /* ONE budget for the whole report, not one per directory: what has to be bounded is the delay
               this adds to a service start, and seven directories each given their own ceiling would multiply
               it by seven. Whatever is left when the budget runs out is reported without a size rather than
               with a wrong one. */
            var deadline = DateTime.UtcNow + s_sizeProbeBudget;

            var found = new List<(string Path, long Bytes, bool Measured, bool Ours)>();
            foreach (var candidate in Directory.GetDirectories(parent))
            {
                if (string.Equals(candidate, liveDataDirectory, StringComparison.OrdinalIgnoreCase)
                    || IsOneOf(managed, candidate)
                    || !File.Exists(Path.Combine(candidate, "PG_VERSION")))
                {
                    continue;
                }

                var ours = Path.GetFileName(candidate).StartsWith(upgradePrefix, StringComparison.OrdinalIgnoreCase);
                var bytes = MeasureDirectoryBytes(candidate, deadline, out var measured);
                found.Add((candidate, bytes, measured, ours));
            }

            if (found.Count == 0)
            {
                return;
            }

            long total = 0;
            var allMeasured = true;
            foreach (var (_, bytes, measured, _) in found)
            {
                total += bytes;
                allMeasured &= measured;
            }

            found.Sort(static (left, right) => right.Bytes.CompareTo(left.Bytes));

            _logger.LogWarning(
                "{Count} store data director(ies) beside {Live} are not part of the running store, and are holding {Approximately}{Size}. NONE of them is deleted automatically. Remove the ones you no longer need: a major store upgrade in copy mode needs roughly twice the data directory in free space.",
                found.Count, liveDataDirectory, allMeasured ? string.Empty : "at least ", FormatBytes(total));

            foreach (var (path, bytes, measured, ours) in found)
            {
                if (!measured)
                {
                    /* Say what was actually established. A size probe that ran out of budget knows a lower
                       bound and nothing more, and rounding that up to a stated total is how a diagnostic
                       teaches people to distrust its numbers. */
                    _logger.LogWarning(
                        "Store data directory not part of the running store: {Path} (at least {Size}; the {Budget}-second size probe did not finish walking it).",
                        path, FormatBytes(bytes), (int)s_sizeProbeBudget.TotalSeconds);
                    continue;
                }

                if (ours)
                {
                    /* Deliberately NOT deleted, and this is the reason. The commit point is two moves — the
                       live directory aside, then the upgraded one into its place — so a process that died
                       between them leaves the UPGRADED cluster under this name with nothing at the live
                       path. Deleting it there would destroy the only good copy, and in hard-link mode the
                       pre-upgrade directory beside it is not a usable fallback either (pg_upgrade's linked
                       old cluster must not be started). A human can tell those apart from the log; a sweep
                       running before the store is up cannot. */
                    _logger.LogWarning(
                        "Leftover store data directory from an interrupted upgrade: {Path} ({Size}). Check that {Live} is the cluster you want BEFORE removing it — if a previous upgrade died between swapping the directories, this one is the upgraded store.",
                        path, FormatBytes(bytes), liveDataDirectory);
                    continue;
                }

                _logger.LogWarning(
                    "Store data directory this service did not create: {Path} ({Size}). It is left alone — a copy made by hand is someone's decision to reverse, not this service's.",
                    path, FormatBytes(bytes));
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Could not check for store data directories beside {Live} ({Message}).", liveDataDirectory, ex.Message);
        }
    }

    private static bool IsOneOf(string[] paths, string candidate)
    {
        foreach (var path in paths)
        {
            if (string.Equals(path, candidate, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /* ============================ the upgrade ============================ */

    /// <summary>Everything the orchestration needs, gathered once so the flow reads as steps rather than plumbing.</summary>
    internal sealed record UpgradeContext(
        string OldBinDirectory,
        string NewBinDirectory,
        string RuntimeRoot,
        string ZipHash,
        string DataDirectory,
        int Port,
        string UserName,
        string Password,
        int OldMajor,
        int NewMajor,
        string BundledTimescaleVersion,
        Action<string> AppendManagedConf);

    /// <summary>
    /// The in-place major upgrade, start to finish. Each step is labelled, and ANY failure lands in one
    /// place: revert the runtime, drop the half-built new cluster, leave the old data directory (untouched
    /// in copy mode) exactly where it was, and return a Failed outcome so the caller keeps running the store
    /// on its existing major. The one thing this must never do is leave the store down.
    /// </summary>
    internal async Task<StoreUpgradeOutcome> UpgradeDataDirectoryAsync(UpgradeContext context, CancellationToken cancellationToken)
    {
        var newDataDirectory = Path.TrimEndingDirectorySeparator(Path.GetFullPath(context.DataDirectory))
            + UpgradeStagingDirectorySuffix + context.NewMajor.ToString(CultureInfo.InvariantCulture);
        var parent = Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(context.DataDirectory)))!;
        var passwordFile = Path.Combine(parent, "pg-upgrade-pwfile.tmp");

        var step = "preflight";
        var oldStarted = false;

        /* Whether the data directory swap has COMMITTED. Load-bearing: past that point the configured path
           holds the new major and no failure path may revert the runtime, because old binaries in front of a
           new data directory is an unbootable store. See the commit point below. */
        var swapped = false;

        /* Set when the upgrade SUCCEEDS but some post-commit bookkeeping did not. Carried out on the outcome
           so the operator's ALERT says so too — a log line that alarms while the alert reassures is worse
           than either alone, because the alert is the surface someone actually receives. */
        string? postCommitWarning = null;
        string? fromTimescale = null;
        var mode = FileTransferMode.Copy;

        _logger.LogWarning(
            "STORE UPGRADE STARTING: PostgreSQL {Old} -> {New} on {DataDirectory}. Collection is paused until this finishes; the store is offline for the duration.",
            context.OldMajor, context.NewMajor, context.DataDirectory);

        try
        {
            /* ---- 1. space + hard-link capability, measured before anything is touched ---- */
            step = "disk-headroom";
            var dataBytes = MeasureDirectoryBytes(context.DataDirectory);
            var free = new DriveInfo(Path.GetPathRoot(parent)!).AvailableFreeSpace;
            var decision = DecideTransferMode(dataBytes, free, SupportsHardLinks(parent));
            mode = decision.Mode;

            if (mode == FileTransferMode.Abort)
            {
                throw new InvalidOperationException(
                    $"not enough disk space to upgrade safely: {decision.Reason}. Free space on the store volume and restart the service.");
            }

            if (mode == FileTransferMode.Link)
            {
                _logger.LogCritical(
                    "STORE UPGRADE USING HARD-LINK MODE: {Reason}. Hard-link mode does NOT leave a usable rollback copy of the pre-upgrade store — once the upgraded server starts, the only way back is a restore from backup. Take one now if you do not have a recent one.",
                    decision.Reason);
            }
            else
            {
                _logger.LogInformation("Store upgrade file transfer: copy mode — {Reason}.", decision.Reason);
            }

            /* ---- 2. start the OLD cluster on its OLD binaries: crash recovery if needed, and the only
                    place the cluster's real locale/encoding/checksum identity and its TimescaleDB version
                    can be read rather than assumed ---- */
            step = "start-old-cluster";
            await StartClusterAsync(context.OldBinDirectory, context.DataDirectory, context.Port, cancellationToken);
            oldStarted = true;

            step = "read-cluster-identity";
            var ownerConnection = DarlingManagedPostgres.BuildConnectionString(context.Port, context.Password);
            var identity = await ReadClusterIdentityAsync(ownerConnection, cancellationToken);
            _logger.LogInformation(
                "Old cluster identity: encoding {Encoding}, collate {Collate}, ctype {Ctype}, locale provider {Provider}, data checksums {Checksums} — the new cluster is initialized to match.",
                identity.Encoding, identity.Collate, identity.Ctype, identity.LocaleProvider ?? "(none)", identity.DataChecksums ? "on" : "off");

            /* ---- 3. the TimescaleDB bridge: pg_upgrade recreates the extension pinned at the version the
                    OLD cluster has, and the NEW runtime ships exactly one versioned library. Mismatch here
                    is a guaranteed upgrade failure later, so it is a hard gate, not best-effort. ---- */
            step = "timescaledb-bridge";
            fromTimescale = await BridgeTimescaleAsync(
                context.Port, context.UserName, context.Password, context.BundledTimescaleVersion, cancellationToken);

            /* ---- 4. stop the old cluster cleanly — pg_upgrade refuses to run against a live or
                    unclean-shutdown cluster ---- */
            step = "stop-old-cluster";
            await StopClusterAsync(context.OldBinDirectory, context.DataDirectory, cancellationToken);
            oldStarted = false;

            /* ---- 5. initdb the new cluster to the OLD cluster's identity, then give it the managed conf
                    BEFORE pg_upgrade runs: shared_preload_libraries = 'timescaledb' must already be set or
                    pg_upgrade's internal restore cannot load the extension it is restoring ---- */
            step = "initdb-new-cluster";
            TryDeleteDirectory(newDataDirectory);
            File.WriteAllText(passwordFile, context.Password + "\n");
            DarlingFileSecurity.HardenFile(passwordFile, allowInteractiveRead: false);

            var newInitDb = Path.Combine(context.NewBinDirectory, "initdb.exe");
            var (initExit, initOutput) = await DarlingManagedPostgres.RunToolAsync(
                newInitDb,
                BuildInitDbArguments(newDataDirectory, context.UserName, passwordFile, identity, context.NewMajor),
                s_initDbTimeout,
                cancellationToken);
            if (initExit != 0)
            {
                throw new InvalidOperationException(
                    $"initdb of the new cluster failed (exit {DarlingToolExitCode.Describe(initExit)}):" +
                    DarlingToolExitCode.Diagnose(initExit, newInitDb) +
                    $"\n{DarlingToolExitCode.FormatOutput(initOutput, initExit)}");
            }

            step = "conf-new-cluster";
            context.AppendManagedConf(newDataDirectory);

            /* ---- 6. pg_upgrade: --check first (it validates the locale/encoding/checksum match and the
                    loadable-library set without touching either cluster), then the real pass ---- */
            step = "upgrade-port-preflight";
            AssertUpgradePortsFree();

            step = "pg_upgrade-check";
            var environment = BuildLibpqCredentialEnvironment(context.Password);

            var pgUpgrade = Path.Combine(context.NewBinDirectory, "pg_upgrade.exe");
            var checkExit = await DarlingManagedPostgres.RunDetachingToolAsync(
                pgUpgrade,
                BuildPgUpgradeArguments(
                    context.OldBinDirectory, context.NewBinDirectory, context.DataDirectory, newDataDirectory,
                    context.UserName, mode, checkOnly: true, jobs: 1, QuiesceTimescaleServerOptions),
                s_pgUpgradeCheckTimeout,
                cancellationToken,
                environment,
                parent);
            if (checkExit != 0)
            {
                /* "The clusters are not compatible" is pg_upgrade's verdict, and only pg_upgrade can reach it.
                   A Windows status means pg_upgrade never ran, so the compatibility claim would be invented
                   (#2186) — the same wrong-blame the pg_ctl status message carried. */
                var checkDiagnosis = DarlingToolExitCode.Diagnose(checkExit, pgUpgrade);
                throw new InvalidOperationException(
                    $"pg_upgrade --check failed (exit {DarlingToolExitCode.Describe(checkExit)})" +
                    (checkDiagnosis.Length == 0
                        ? " — the clusters are not compatible and NOTHING has been changed."
                        : ". NOTHING has been changed." + checkDiagnosis) +
                    $"\n{ReadPgUpgradeLogTail(newDataDirectory)}");
            }

            step = "pg_upgrade";
            /* SERIAL, deliberately — but NOT because parallelism was proven harmful. It was suspected (the
               dry run, which never passes --jobs, completed while the real pass did not) and then tested:
               --jobs 1 fails identically, so the suspicion was wrong and is recorded here only so nobody
               re-derives it. Serial stays because a store upgrade is a once-per-major event that already
               has the server offline, and one fewer concurrency mode is one fewer thing that can differ
               between a developer's box and a field install. Revisit if a large store's copy time ever
               becomes the complaint. */
            const int jobs = 1;
            var upgradeExit = await DarlingManagedPostgres.RunDetachingToolAsync(
                pgUpgrade,
                BuildPgUpgradeArguments(
                    context.OldBinDirectory, context.NewBinDirectory, context.DataDirectory, newDataDirectory,
                    context.UserName, mode, checkOnly: false, jobs, QuiesceTimescaleServerOptions),
                s_pgUpgradeTimeout,
                cancellationToken,
                environment,
                parent);
            if (upgradeExit != 0)
            {
                throw new InvalidOperationException(
                    $"pg_upgrade failed (exit {DarlingToolExitCode.Describe(upgradeExit)})." +
                    DarlingToolExitCode.Diagnose(upgradeExit, pgUpgrade) +
                    $"\n{ReadPgUpgradeLogTail(newDataDirectory)}");
            }

            /* ---- 7. swap the directories so the configured path holds the upgraded cluster. The
                    credential/cert/log files live BESIDE the data directory, so they are untouched. ---- */
            step = "swap-data-directories";
            var retained = RetainedDataDirectoryFor(context.DataDirectory, context.OldMajor);
            TryDeleteDirectory(retained);
            Directory.Move(context.DataDirectory, retained);
            try
            {
                Directory.Move(newDataDirectory, context.DataDirectory);
            }
            catch (Exception)
            {
                /* Put the original back rather than leave the configured path empty. */
                Directory.Move(retained, context.DataDirectory);
                throw;
            }

            /* THE COMMIT POINT. The configured path now holds the NEW major's cluster, and from here the
               upgrade is irreversible by any means this method has: PostgreSQL 17 binaries cannot open a
               PostgreSQL 18 data directory, so reverting the runtime past this line would leave the store
               UNBOOTABLE. Everything after this is bookkeeping, and bookkeeping must never be able to undo
               a completed upgrade. */
            swapped = true;

            if (mode == FileTransferMode.Link)
            {
                /* Hard-link mode leaves an old directory that SHARES its files with the new cluster — it is
                   not a rollback copy and keeping it invites someone to try. Delete it now, loudly. */
                TryDeleteDirectory(retained);
                _logger.LogWarning(
                    "Removed the pre-upgrade data directory immediately: hard-link mode shares its files with the upgraded cluster, so it was never a usable rollback copy.");
            }
            else
            {
                /* Non-fatal on purpose, and belt-and-braces with the post-commit catch below. The upgrade is
                   already COMMITTED by the time this runs, so a marker file that will not write must not
                   divert out of the happy path — doing so would skip the STORE UPGRADE COMPLETE line the
                   operator looks for, over a countdown file. A missing marker is safe:
                   SweepRetainedDataDirectories parses an absent counter as 1, so the copy is simply kept one
                   start longer and is never deleted early. This is also the likeliest throw site in the whole
                   post-swap window — copy mode just doubled the store's footprint, so free space is at its
                   lowest right here. */
                try
                {
                    File.WriteAllText(retained + ".starts", "0");
                    _logger.LogInformation(
                        "Pre-upgrade data directory kept at {Path} as a rollback copy for {Starts} service starts.",
                        retained, RollbackRetentionStarts);
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    postCommitWarning =
                        $"the rollback copy's retention marker could not be written ({ex.Message}), so the pre-upgrade data directory at {retained} will not age out on its own";
                    _logger.LogWarning(
                        "The upgrade is complete, but the rollback copy's retention marker could not be written at {Path} ({Message}). The pre-upgrade data directory is KEPT and will not be deleted automatically — check free disk space and remove it by hand when you are satisfied with the upgraded store.",
                        retained + ".starts", ex.Message);
                }
            }

            _logger.LogWarning(
                "STORE UPGRADE COMPLETE: PostgreSQL {Old} -> {New}, TimescaleDB {FromTs} -> {ToTs}. Verifying the upgraded store now.",
                context.OldMajor, context.NewMajor, fromTimescale ?? "(none)", context.BundledTimescaleVersion);

            return new StoreUpgradeOutcome(
                StoreUpgradeStatus.Succeeded, context.OldMajor, context.NewMajor,
                fromTimescale, context.BundledTimescaleVersion, null, postCommitWarning, mode == FileTransferMode.Link);
        }
        catch (OperationCanceledException)
        {
            /* Service shutdown mid-upgrade. Before the commit point the old data directory is untouched, so
               the revert restores the previous runtime and the next start tries again. AFTER the commit point
               the same revert would brick the store, so shutdown must leave the new runtime in place and let
               the next start pick up an already-upgraded cluster — cancellation is not a licence to undo a
               completed upgrade any more than an exception is. */
            await TryStopAsync(context, oldStarted);

            if (!swapped)
            {
                TryDeleteDirectory(newDataDirectory);
                RevertRuntimeForCancel(context);
            }
            else
            {
                _logger.LogWarning(
                    "Shutdown interrupted the store upgrade AFTER the data directory swap committed. The store is PostgreSQL {New} and the runtime is NOT being reverted; the next start continues on the upgraded cluster.",
                    context.NewMajor);
            }

            throw;
        }
        catch (Exception ex) when (swapped)
        {
            /* POST-COMMIT failure. The data directory swap succeeded, so the store IS the new major and the
               only honest outcome is Succeeded — with the bookkeeping failure said out loud. Reverting the
               runtime here would put the OLD binaries in front of a NEW data directory and brick the store,
               which is precisely the class of self-inflicted damage the move-aside discipline exists to
               prevent; this is that same lesson applied past the swap. So: no revert, no deletion of the new
               data directory (it is the live store now), and no "nothing was modified" claim. */
            /* Safe here despite pointing at the OLD binaries: oldStarted was set false when the old cluster
               was stopped before initdb, well before the swap, so this is a no-op on every post-commit path.
               Spelled out so a future reader does not have to re-derive it and conclude it is a bug. */
            await TryStopAsync(context, oldStarted);

            var warning =
                $"the upgrade to PostgreSQL {context.NewMajor} COMPLETED, but post-upgrade bookkeeping failed at step '{step}': {ex.Message}";
            _logger.LogCritical(
                "STORE UPGRADE COMPLETED WITH A WARNING: the data directory swap succeeded, so the store is now PostgreSQL {New} and is NOT being reverted — reverting past this point would leave the old binaries in front of a new data directory and the store would not boot. But post-upgrade bookkeeping failed at step '{Step}': {Message}. Check free disk space on the store volume and the pre-upgrade rollback copy's retention counter by hand.",
                context.NewMajor, step, ex.Message);

            return new StoreUpgradeOutcome(
                StoreUpgradeStatus.Succeeded, context.OldMajor, context.NewMajor,
                fromTimescale, context.BundledTimescaleVersion, null, warning, mode == FileTransferMode.Link);
        }
        catch (Exception ex)
        {
            /* PRE-COMMIT failure: the data directory has not been swapped, so the old one is exactly where
               it was and reverting the runtime restores a working store. The "never modified" claim below is
               only true on this path, which is why it lives here and not in a shared message. */
            _logger.LogCritical(
                "STORE UPGRADE FAILED at step '{Step}': {Message}. Reverting to PostgreSQL {Old} — the store keeps running on its existing major and NO data has been lost (the pre-upgrade data directory was never modified).",
                step, ex.Message, context.OldMajor);

            await TryStopAsync(context, oldStarted);
            TryDeleteDirectory(newDataDirectory);
            RevertRuntime(context.RuntimeRoot, context.ZipHash, context.DataDirectory, context.OldMajor);

            return new StoreUpgradeOutcome(
                StoreUpgradeStatus.Failed, context.OldMajor, context.NewMajor,
                fromTimescale, context.BundledTimescaleVersion, step, ex.Message, false);
        }
        finally
        {
            TryDeleteFile(passwordFile);
        }
    }

    private async Task TryStopAsync(UpgradeContext context, bool oldStarted)
    {
        if (!oldStarted)
        {
            return;
        }

        try
        {
            await StopClusterAsync(context.OldBinDirectory, context.DataDirectory, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not stop the old cluster after a failed upgrade ({Message}); the next start adopts it.", ex.Message);
        }
    }

    private void RevertRuntimeForCancel(UpgradeContext context)
    {
        /* A cancelled upgrade is not a BAD package, so revert the binaries without recording the block —
           the next start should try again. */
        var blockedPath = Path.Combine(context.RuntimeRoot, RuntimeBlockedFileName);
        RevertRuntime(context.RuntimeRoot, context.ZipHash, context.DataDirectory, context.OldMajor);
        TryDeleteFile(blockedPath);
    }

    /// <summary>
    /// Starts a cluster with a specific runtime, loopback-only and with no network/TLS overrides — the
    /// upgrade window is not the time to reconcile exposure. Uses the detaching runner because pg_ctl's
    /// spawned postmaster outlives it and inherits redirected handles (the redirect-on-start hang).
    /// </summary>
    private async Task StartClusterAsync(string binDirectory, string dataDirectory, int port, CancellationToken cancellationToken)
    {
        var serverLog = Path.Combine(
            Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(dataDirectory)))!,
            DarlingManagedPostgres.ServerLogFileName);

        var pgCtl = Path.Combine(binDirectory, "pg_ctl.exe");
        var exitCode = await DarlingManagedPostgres.RunDetachingToolAsync(
            pgCtl,
            $"-D \"{dataDirectory}\" -o \"-p {port} -c listen_addresses=127.0.0.1\" -l \"{serverLog}\" -w -t 120 start",
            TimeSpan.FromMinutes(5),
            cancellationToken);

        if (exitCode != 0)
        {
            throw new InvalidOperationException(
                $"could not start the cluster in {dataDirectory} with the runtime at {binDirectory} (pg_ctl exit {DarlingToolExitCode.Describe(exitCode)})" +
                DarlingToolExitCode.Diagnose(exitCode, pgCtl));
        }
    }

    private async Task StopClusterAsync(string binDirectory, string dataDirectory, CancellationToken cancellationToken)
    {
        var pgCtl = Path.Combine(binDirectory, "pg_ctl.exe");
        var (exitCode, output) = await DarlingManagedPostgres.RunToolAsync(
            pgCtl,
            $"stop -D \"{dataDirectory}\" -m fast -w -t 120",
            s_toolTimeout,
            cancellationToken);

        if (exitCode != 0)
        {
            throw new InvalidOperationException(
                $"could not cleanly stop the cluster in {dataDirectory} (pg_ctl exit {DarlingToolExitCode.Describe(exitCode)}): {DarlingToolExitCode.FormatOutput(output, exitCode)}" +
                DarlingToolExitCode.Diagnose(exitCode, pgCtl));
        }
    }

    /// <summary>
    /// Reads the cluster's locale/encoding/checksum identity from the LIVE server. <c>template0</c> is the
    /// pristine record of what initdb was told (a user database may have been created with anything), and
    /// the <c>to_jsonb</c> lookups make the query tolerant of columns that only exist in some majors
    /// (<c>datlocale</c> in 17+, <c>daticulocale</c> in 15/16) instead of failing on the ones it lacks.
    /// </summary>
    internal static async Task<ClusterIdentity> ReadClusterIdentityAsync(string ownerConnectionString, CancellationToken cancellationToken)
    {
        var builder = new NpgsqlConnectionStringBuilder(ownerConnectionString) { Database = "postgres", Pooling = false };
        await using var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(
            """
            SELECT pg_encoding_to_char(d.encoding) AS encoding,
                   d.datcollate,
                   d.datctype,
                   to_jsonb(d) ->> 'datlocprovider' AS locprovider,
                   COALESCE(to_jsonb(d) ->> 'datlocale', to_jsonb(d) ->> 'daticulocale') AS locale,
                   current_setting('data_checksums') AS data_checksums
            FROM pg_database AS d
            WHERE d.datname = 'template0'
            """,
            connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException("template0 is missing from pg_database — the cluster's locale identity cannot be read.");
        }

        return new ClusterIdentity(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            !reader.IsDBNull(5) && string.Equals(reader.GetString(5), "on", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Updates the TimescaleDB extension to <paramref name="targetVersion"/> in EVERY database that has it
    /// — template1 included, because pg_upgrade carries template1 across too and a stale extension there
    /// fails the upgrade just as surely as one in the store database. Each database gets a FRESH,
    /// UNPOOLED connection whose FIRST statement is the ALTER: TimescaleDB refuses the update once its
    /// library has been loaded into the session, which is why its own documentation calls for a new
    /// connection. Returns the version found before the update (null when the extension is absent
    /// everywhere), for the alert text.
    /// </summary>
    internal async Task<string?> BridgeTimescaleAsync(
        int port, string userName, string password, string targetVersion, CancellationToken cancellationToken)
    {
        var ownerConnection = DarlingManagedPostgres.BuildConnectionString(port, password);
        var databases = new List<string>();

        var listBuilder = new NpgsqlConnectionStringBuilder(ownerConnection) { Database = "postgres", Pooling = false };
        await using (var connection = new NpgsqlConnection(listBuilder.ConnectionString))
        {
            await connection.OpenAsync(cancellationToken);
            await using var command = new NpgsqlCommand(
                "SELECT datname FROM pg_database WHERE datallowconn ORDER BY datname", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                databases.Add(reader.GetString(0));
            }
        }

        string? observedBefore = null;

        foreach (var database in databases)
        {
            var perDatabase = new NpgsqlConnectionStringBuilder(ownerConnection)
            {
                Database = database,
                Pooling = false,
                /* The store's search_path is irrelevant here and a missing schema on a maintenance database
                   would only add noise. */
                SearchPath = null,
                CommandTimeout = (int)s_bridgeTimeout.TotalSeconds,
            };

            await using var connection = new NpgsqlConnection(perDatabase.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            string? installed;
            await using (var probe = new NpgsqlCommand(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds })
            {
                installed = await probe.ExecuteScalarAsync(cancellationToken) as string;
            }

            if (installed is null)
            {
                continue;
            }

            observedBefore ??= installed;

            if (string.Equals(installed, targetVersion, StringComparison.Ordinal))
            {
                _logger.LogInformation(
                    "TimescaleDB in database '{Database}' is already at {Version} — no bridge needed.", database, installed);
                continue;
            }

            _logger.LogWarning(
                "Bridging TimescaleDB in database '{Database}': {From} -> {To}. This must happen on the OLD cluster: pg_upgrade recreates the extension pinned at whatever version is installed, and the new runtime ships exactly one version-suffixed library.",
                database, installed, targetVersion);

            /* The FIRST statement on this connection, deliberately — TimescaleDB rejects the update once
               its library has been loaded into the session. The probe above reads pg_extension, a plain
               catalog table, which does not load it. */
            await using (var update = new NpgsqlCommand("ALTER EXTENSION timescaledb UPDATE", connection)
            {
                CommandTimeout = (int)s_bridgeTimeout.TotalSeconds,
            })
            {
                await update.ExecuteNonQueryAsync(cancellationToken);
            }

            await using var verify = new NpgsqlCommand(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
            var after = await verify.ExecuteScalarAsync(cancellationToken) as string;
            if (!string.Equals(after, targetVersion, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"TimescaleDB in database '{database}' is {after ?? "(absent)"} after ALTER EXTENSION UPDATE, but the bundled runtime ships {targetVersion}. " +
                    "pg_upgrade would fail on the version-suffixed library, so the upgrade stops here.");
            }

            _logger.LogInformation("TimescaleDB in database '{Database}' is now {Version}.", database, after);
        }

        return observedBefore;
    }

    /// <summary>
    /// The credential every libpq tool in the upgrade authenticates with, handed over as <c>PGPASSWORD</c>
    /// in the child's environment.
    ///
    /// <para>This was a hardened <c>PGPASSFILE</c> first, on the reasoning that a file keeps the password out
    /// of a process environment block. That was wrong on both counts. It FAILED on a CI runner —
    /// <c>password authentication failed for user "darling"</c> from pg_upgrade's own log, while passing on a
    /// developer box — because the file approach has to get four separate things right on every host: the
    /// ACL (and pg_upgrade re-execs itself under a RESTRICTED token on Windows, so the reader is not quite
    /// the writer), the encoding, a path a restricted child can reach, and cleanup. And it is not actually
    /// safer: a process environment block is readable by the same user and by administrators, which is the
    /// identical audience that can already read the DPAPI credential file this password comes from — except
    /// the environment never touches disk, where a killed process could strand a cleartext temp file the
    /// <c>finally</c> never ran for.</para>
    ///
    /// <para>So: one mechanism, no file, nothing persisted, and the same exposure set. libpq reads
    /// <c>PGPASSWORD</c> ahead of any password file, and every tool the upgrade spawns (pg_upgrade and the
    /// pg_dump/pg_restore/psql it spawns in turn) inherits it.</para>
    ///
    /// <para><b>This deliberately departs from libpq's documented advice, and the licence for that is in the
    /// advice itself.</b> The docs say <c>PGPASSWORD</c> "is not recommended for security reasons, as some
    /// operating systems allow non-root users to see process environment variables via ps" — a warning
    /// conditioned on the OS exposing environments to other unprivileged users. Windows does not: reading
    /// another process's environment block needs PROCESS_VM_READ + PROCESS_QUERY_INFORMATION, which across a
    /// user boundary requires SeDebugPrivilege, i.e. administrator. The bundled runtime is Windows-only, so
    /// the premise of that warning never holds here. The value is also set on the CHILD's
    /// <c>ProcessStartInfo.Environment</c>, never via <c>Environment.SetEnvironmentVariable</c> — so the
    /// service's own environment never carries it, and it dies with the process tree that needed it.</para>
    /// </summary>
    private static Dictionary<string, string> BuildLibpqCredentialEnvironment(string password)
        => new(StringComparer.OrdinalIgnoreCase) { ["PGPASSWORD"] = password };

    /// <summary>
    /// pg_upgrade's own logs, which it writes under the NEW data directory and removes on success — so
    /// anything found here belongs to a failure and is exactly what explains it. The tool's console output
    /// is deliberately not captured (its child postmasters inherit redirected handles and never close them),
    /// making these files the whole diagnostic story.
    /// </summary>
    private static string ReadPgUpgradeLogTail(string newDataDirectory)
    {
        try
        {
            var outputRoot = Path.Combine(newDataDirectory, "pg_upgrade_output.d");
            if (!Directory.Exists(outputRoot))
            {
                return "(pg_upgrade left no output directory)";
            }

            var builder = new StringBuilder();
            foreach (var file in Directory.GetFiles(outputRoot, "*.txt", SearchOption.AllDirectories))
            {
                AppendTail(builder, file);
            }

            foreach (var file in Directory.GetFiles(outputRoot, "*.log", SearchOption.AllDirectories))
            {
                AppendTail(builder, file);
            }

            return builder.Length == 0 ? $"(no readable logs under {outputRoot})" : builder.ToString();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return $"(could not read the pg_upgrade logs: {ex.Message})";
        }

        static void AppendTail(StringBuilder builder, string file)
        {
            try
            {
                var lines = File.ReadAllLines(file);
                if (lines.Length == 0)
                {
                    return;
                }

                var take = Math.Min(30, lines.Length);
                builder.Append("--- ").Append(file).Append(" ---\n");
                builder.Append(string.Join('\n', lines[^take..])).Append('\n');
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                /* One unreadable log does not hide the others. */
            }
        }
    }

    /* ============================ post-start: verify, update, analyze ============================ */

    /// <summary>
    /// Everything that needs a LIVE server, run once the normal bootstrap has started it (so the server is
    /// owned by this process and shutdown still stops it — starting it here would silently turn the store
    /// into an "adopted" one this service refuses to stop).
    ///
    /// <para>Three jobs. It VERIFIES the upgrade actually landed — server major, extension version, and a
    /// real read of a collector table, because "pg_upgrade exited 0" and "the store works" are different
    /// claims. It applies the SAME-MAJOR TimescaleDB update, which is the #1705 case on its own: a runtime
    /// bump that changes only the extension needs no pg_upgrade, just the ALTER on a fresh connection. And
    /// it runs the post-upgrade analyze staging: PostgreSQL 18 carries most optimizer statistics across, so
    /// this fills the documented gaps (extended statistics, extension-owned statistics) rather than
    /// re-analyzing a whole store that already has them.</para>
    /// </summary>
    internal async Task<StoreUpgradeOutcome> CompleteAfterStartAsync(
        StoreUpgradeOutcome outcome,
        string ownerConnectionString,
        string newBinDirectory,
        int port,
        string userName,
        string password,
        int bundledMajor,
        string bundledTimescaleVersion,
        CancellationToken cancellationToken)
    {
        var upgraded = outcome.Status == StoreUpgradeStatus.Succeeded;

        try
        {
            var builder = new NpgsqlConnectionStringBuilder(ownerConnectionString) { Pooling = false };
            await using var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            if (upgraded)
            {
                await using var version = new NpgsqlCommand("SHOW server_version_num", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
                var raw = await version.ExecuteScalarAsync(cancellationToken) as string;
                var liveMajor = int.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var num) ? num / 10000 : 0;
                if (liveMajor != bundledMajor)
                {
                    _logger.LogCritical(
                        "Store upgrade verification: the running server reports major {Live}, not the expected {Expected}. The data directory swap may not have taken.",
                        liveMajor, bundledMajor);
                }
                else
                {
                    _logger.LogInformation("Store upgrade verified: the running server is PostgreSQL major {Major}.", liveMajor);
                }
            }

            /* The same-major extension update (#1705). After a pg_upgrade the bridge already moved it, so
               this is a no-op there; on a TimescaleDB-only runtime bump it IS the whole upgrade. First
               statement on this fresh connection, for the same alone-first reason as the bridge. */
            string? installed;
            await using (var probe = new NpgsqlCommand(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds })
            {
                installed = await probe.ExecuteScalarAsync(cancellationToken) as string;
            }

            var extensionMoved = false;

            /* An empty bundled version means the runtime carries no timescaledb.control to read — a
               plain-PostgreSQL bundle. Never "update" the extension toward a version we cannot name. */
            if (installed is not null
                && !string.IsNullOrEmpty(bundledTimescaleVersion)
                && !string.Equals(installed, bundledTimescaleVersion, StringComparison.Ordinal))
            {
                _logger.LogWarning(
                    "The store's TimescaleDB is {Installed} but the bundled runtime ships {Bundled} — updating the extension now (#1705: a store otherwise keeps whatever extension it was created with, forever).",
                    installed, bundledTimescaleVersion);

                await using (var update = new NpgsqlCommand("ALTER EXTENSION timescaledb UPDATE", connection)
                {
                    CommandTimeout = (int)s_bridgeTimeout.TotalSeconds,
                })
                {
                    await update.ExecuteNonQueryAsync(cancellationToken);
                }

                await using var verify = new NpgsqlCommand(
                    "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
                var after = await verify.ExecuteScalarAsync(cancellationToken) as string;
                if (string.Equals(after, bundledTimescaleVersion, StringComparison.Ordinal))
                {
                    _logger.LogWarning("TimescaleDB extension updated: {From} -> {To}.", installed, after);
                    extensionMoved = true;
                }
                else
                {
                    _logger.LogCritical(
                        "TimescaleDB is {After} after ALTER EXTENSION UPDATE, but the bundled runtime ships {Bundled}. The store still runs, but its extension does not match its binaries — investigate before the next release.",
                        after ?? "(absent)", bundledTimescaleVersion);
                }

                outcome = outcome with
                {
                    Status = upgraded ? outcome.Status : StoreUpgradeStatus.ExtensionUpdated,
                    FromTimescale = outcome.FromTimescale ?? installed,
                    ToTimescale = bundledTimescaleVersion,
                };
            }

            if (upgraded)
            {
                await VerifySentinelReadAsync(connection, cancellationToken);
            }

            if (upgraded || extensionMoved)
            {
                _logger.LogInformation(
                    "Store runtime is current: PostgreSQL major {Major}, TimescaleDB {Timescale}.",
                    bundledMajor, installed is null ? "(not installed)" : bundledTimescaleVersion);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* Verification is diagnosis, not a gate: the data is already migrated and the server is up, so
               a failed check must not take collection down — it must be LOUD. */
            _logger.LogCritical(
                "Post-upgrade store verification failed ({Message}). The store is running; confirm its version and TimescaleDB state by hand.",
                ex.Message);
        }

        if (upgraded)
        {
            await RunAnalyzeInStagesAsync(newBinDirectory, port, userName, password, bundledMajor, cancellationToken);
        }

        return outcome;
    }

    /// <summary>
    /// Proves the upgraded store can actually be READ, not merely connected to — a collector table's row
    /// count exercises the restored catalog, the TimescaleDB chunk machinery behind a hypertable, and the
    /// search path in one query. A store that upgraded but cannot read its own history is the failure this
    /// exists to catch.
    /// </summary>
    private async Task VerifySentinelReadAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var exists = new NpgsqlCommand("SELECT to_regclass('collect.collection_log') IS NOT NULL", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        if (await exists.ExecuteScalarAsync(cancellationToken) is not true)
        {
            _logger.LogWarning(
                "Post-upgrade sentinel read skipped: collect.collection_log does not exist yet (a store upgraded before its first migration).");
            return;
        }

        await using var count = new NpgsqlCommand("SELECT count(*) FROM collect.collection_log", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        var rows = Convert.ToInt64(await count.ExecuteScalarAsync(cancellationToken) ?? 0L, CultureInfo.InvariantCulture);
        _logger.LogInformation(
            "Post-upgrade sentinel read OK: collect.collection_log returned {Rows:N0} rows through the upgraded cluster.", rows);
    }

    /// <summary>
    /// The documented post-pg_upgrade statistics step. PostgreSQL 18 transfers most optimizer statistics, so
    /// this is the targeted follow-up its documentation calls for — <c>--missing-stats-only</c> touches only
    /// relations that came across without any, instead of re-analyzing a store that already has them.
    /// Best-effort: the store is up and correct with or without it, and a slow analyze must never look like
    /// a failed upgrade.
    /// </summary>
    private async Task RunAnalyzeInStagesAsync(
        string newBinDirectory, int port, string userName, string password, int bundledMajor, CancellationToken cancellationToken)
    {
        var vacuumdb = Path.Combine(newBinDirectory, "vacuumdb.exe");
        if (!File.Exists(vacuumdb))
        {
            _logger.LogWarning("vacuumdb.exe is not in the bundled runtime — skipping the post-upgrade analyze; the planner will catch up via autovacuum.");
            return;
        }

        try
        {

            var arguments = new StringBuilder();
            arguments.Append("--host 127.0.0.1 --port ").Append(port.ToString(CultureInfo.InvariantCulture));
            arguments.Append(" --username ").Append(userName);
            arguments.Append(" --all --analyze-in-stages");
            if (bundledMajor >= 18)
            {
                /* --missing-stats-only arrived with the statistics-preserving pg_upgrade in 18; on anything
                   older the staged analyze has to do the whole store. */
                arguments.Append(" --missing-stats-only");
            }

            _logger.LogInformation("Running the post-upgrade analyze staging (statistics PostgreSQL {Major} did not carry across).", bundledMajor);

            var (exitCode, output) = await DarlingManagedPostgres.RunToolAsync(
                vacuumdb,
                arguments.ToString(),
                s_analyzeTimeout,
                cancellationToken,
                BuildLibpqCredentialEnvironment(password));

            if (exitCode == 0)
            {
                _logger.LogInformation("Post-upgrade analyze complete.");
            }
            else
            {
                _logger.LogWarning(
                    "Post-upgrade analyze reported exit {ExitCode} ({ExitCodeMeaning}): {Output}. The store is fully usable; autovacuum will build the remaining statistics.",
                    exitCode,
                    DarlingToolExitCode.Describe(exitCode),
                    DarlingToolExitCode.FormatOutput(output, exitCode));
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Post-upgrade analyze could not run ({Message}); autovacuum will build the statistics.", ex.Message);
        }
    }
}
