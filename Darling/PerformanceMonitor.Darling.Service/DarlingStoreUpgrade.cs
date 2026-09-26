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
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

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
/// <para><b>Failure isolation.</b> Copy mode leaves the old data directory byte-for-byte untouched, and
/// hard-link mode changes exactly one thing in it before the commit point, pg_upgrade's rename of
/// <c>global\pg_control</c>, which every failure path undoes (#3927). So a failure puts the old data
/// directory back, reverts to the old runtime, and the store keeps running on its original major. A failure
/// also RECORDS the failing zip's hash in <see cref="RuntimeBlockedFileName"/> so the next start does not
/// retry the same known-bad package on a loop — a different zip clears it. Nothing here leaves the store
/// down by choice; when putting it back cannot be done (a file held open, a server that will not stop),
/// the log and the outcome say so and name the manual step rather than claim a running store.</para>
/// </summary>
[SupportedOSPlatform("windows")]
internal sealed class DarlingStoreUpgrade
{
    /// <summary>
    /// Records the SHA256 of the zip the extracted runtime came from: the change detector. Since #3908 it has
    /// its own name, because the name it used to have belongs to <see cref="LegacyRuntimeStampFileName"/>.
    /// </summary>
    public const string RuntimeStampFileName = "pg-runtime.sha256";

    /// <summary>
    /// The stamp file releases 3.3.0 through 3.8.x read (#3908). This release keeps it pinned to
    /// <see cref="LegacyRuntimePackageHash"/> on every start and never records its own package in it.
    ///
    /// <para>The reason is a rollback. After this release moves a store's TimescaleDB to its own version, an
    /// operator who re-installs 3.3 to 3.8 gets that release's runtime-update code, which swaps in its own
    /// runtime whenever this file disagrees with its package. That runtime cannot load the newer extension,
    /// so the store would stop opening, and no alert can fire at that point. Every one of those releases
    /// shipped the byte-identical pg-runtime.zip whose hash is <see cref="LegacyRuntimePackageHash"/> (verified
    /// against 3.3.0, 3.4.0, 3.5.0, 3.6.0, 3.7.0, 3.7.1 and 3.8.0), and each returns early, keeping the
    /// runtime it finds, when this file holds its own package's hash (verified by running 3.8.0's code
    /// against the tree this release leaves). Releases before 3.3 never swap an extracted runtime.</para>
    /// </summary>
    public const string LegacyRuntimeStampFileName = "pg-runtime.stamp";

    /// <summary>The SHA256 of the pg-runtime.zip shipped by every release from 3.3.0 through 3.8.x.</summary>
    public const string LegacyRuntimePackageHash = "b8d8bcb54a0f57e941e417708e8eb06e7bbc3c07d01d6a7fcf0b93e9f4d38640";

    /// <summary>
    /// Records the SHA256 of a zip whose upgrade FAILED. A start that sees the same hash skips the swap
    /// (logging why) instead of retrying a known-bad package every time the service restarts; shipping a
    /// different zip changes the hash and re-arms the attempt.
    /// </summary>
    public const string RuntimeBlockedFileName = "pg-runtime.blocked";

    /// <summary>
    /// The pre-upgrade postgresql.auto.conf, kept beside the new data directory (#4253) after
    /// <see cref="CarryAutoConfAsync"/> runs — the operator's own record of exactly what ALTER SYSTEM had
    /// set, whether or not every setting in it carried across.
    /// </summary>
    public const string PreUpgradeAutoConfFileName = "postgresql.auto.conf.pre-upgrade";

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

    /// <summary>
    /// One <c>postgres -C</c> probe (#4253) reads config files and exits — no server is left running — so
    /// this is generous only against a wedged disk, not against real work. Kept far short of
    /// <see cref="s_toolTimeout"/> so a run with many carried settings does not turn one bad probe into a
    /// multi-minute stall of an upgrade the store is already offline for.
    /// </summary>
    private static readonly TimeSpan s_confProbeTimeout = TimeSpan.FromSeconds(30);

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

        /// <summary>
        /// A step failed before the commit point. <see cref="StoreUpgradeOutcome.PreUpgradeData"/> and
        /// <see cref="StoreUpgradeOutcome.RuntimeReverted"/> say how much was put back; only the clean shape keeps the
        /// store running on its previous major (#3927).
        /// </summary>
        Failed,
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
        bool UsedLinkMode,
        /* #3927: what a FAILED upgrade actually managed to put back. Failed used to mean "reverted, and the
           store keeps running" unconditionally, and in hard-link mode, or with a server the upgrade could not
           stop, neither half was always true. A Failed outcome whose data directory was NOT put back is one
           no alert ever carries (the store cannot start to send it), so the log is where that case is said
           in full. The defaults are the clean revert, so every outcome built before these existed still
           means what it meant. */
        PreUpgradeDataDirectory PreUpgradeData = PreUpgradeDataDirectory.Untouched,
        bool RuntimeReverted = true)
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
    /// Parses major.minor from a PostgreSQL tool's <c>--version</c> line ("pg_ctl (PostgreSQL) 18.6" =&gt; 18.6)
    /// or a bare version string ("17.10"), reading the same first numeric token <see cref="ParsePostgresMajor"/>
    /// reads. A beta/rc or bare major ("19beta1", "18") carries no minor and yields null, as does anything
    /// unparseable; the caller then has only the major to go on, as it always did. The result is always two
    /// parts, so "18.6" and "18.6.0" compare equal whichever source they came from.
    /// </summary>
    internal static Version? ParsePostgresVersion(string? versionOutput)
    {
        if (string.IsNullOrWhiteSpace(versionOutput))
        {
            return null;
        }

        foreach (var token in versionOutput.Split(new[] { ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
        {
            if (!char.IsDigit(token[0]))
            {
                continue;
            }

            var length = 0;
            while (length < token.Length && (char.IsDigit(token[length]) || token[length] == '.'))
            {
                length++;
            }

            return Version.TryParse(token[..length].TrimEnd('.'), out var version) && version.Major > 0
                ? new Version(version.Major, version.Minor)
                : null;
        }

        return null;
    }

    /// <summary>
    /// Whether an extracted runtime that carries no stamp IS the shipped package, so it can be adopted without
    /// a swap. The full PostgreSQL version and the TimescaleDB version both have to match. When either side's
    /// minor cannot be read, only TimescaleDB is compared, which is what this check did before #3906.
    /// </summary>
    internal static bool ExtractedRuntimeMatchesPackage(
        Version? installedPostgres, Version? packagePostgres, string? installedTimescale, string? packageTimescale)
        => (installedPostgres is null || packagePostgres is null || installedPostgres == packagePostgres)
           && string.Equals(installedTimescale, packageTimescale, StringComparison.OrdinalIgnoreCase);

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
        /* A host's first start of this release has only the legacy stamp, which is its real package hash
           (every 3.3-3.8 host's is LegacyRuntimePackageHash). After that the legacy file is pinned and this
           release reads only its own. */
        var stamp = ReadTrimmedOrNull(stampPath) ?? ReadTrimmedOrNull(Path.Combine(runtimeRoot, LegacyRuntimeStampFileName));

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
            var installedVersionLine = await ReadRuntimeVersionLineAsync(binDirectory, cancellationToken);
            var installedMajor = ParsePostgresMajor(installedVersionLine);
            var zipMajor = TryReadZipPostgresMajor(runtimeZipPath);

            if (installedMajor is not null && zipMajor is not null && installedMajor == zipMajor)
            {
                /* Equal PostgreSQL majors do NOT mean equal runtimes, and adopting on the major alone would
                   re-open #1705's drift inside the machinery built to end it: a host on PG 18 + TimescaleDB
                   2.24.0 receiving PG 18 + 2.28.1 would be stamped as already-matching and keep 2.24.0
                   forever, because the new versioned library never lands and the same-major extension update
                   can then only reach the version already on disk. So the extension's library version is
                   compared too — same major but a different TimescaleDB means a same-major runtime swap
                   (rescue, extract, no pg_upgrade, extension update), which is a path that already exists.

                   #3906: the same holds for the PostgreSQL MINOR. A host on 18.4 receiving 18.6 with an
                   unchanged TimescaleDB would be stamped as current and keep 18.4, and every CVE 18.6 fixes,
                   until some later package happened to change. A minor release is usually security
                   servicing, so it is compared too, and a difference takes the same same-major swap. */
                var installedVersion = ParsePostgresVersion(installedVersionLine);
                var zipVersion = TryReadZipPostgresVersion(runtimeZipPath);
                var installedTimescale = TryReadInstalledTimescaleVersion(binDirectory);
                var zipTimescale = TryReadZipTimescaleVersion(runtimeZipPath);

                if (ExtractedRuntimeMatchesPackage(installedVersion, zipVersion, installedTimescale, zipTimescale))
                {
                    _logger.LogInformation(
                        "Adopting the already-extracted runtime (PostgreSQL {Version}, TimescaleDB {Timescale}): it matches the shipped package, so nothing is swapped. Recording its stamp so future package changes are detectable.",
                        installedVersion?.ToString() ?? installedMajor.Value.ToString(CultureInfo.InvariantCulture), installedTimescale ?? "(none)");
                    TryWriteStamp(stampPath, zipHash);
                    return new RuntimeAdvance(false, null, zipHash);
                }

                _logger.LogWarning(
                    "The extracted runtime and the shipped package are both PostgreSQL {Major} but are not the same runtime (PostgreSQL {InstalledVersion} + TimescaleDB {Installed} on disk, PostgreSQL {PackageVersion} + TimescaleDB {Package} in the package) — updating the runtime with a same-major swap, which runs no pg_upgrade. This is the drift #1705 and #3906 caught.",
                    installedMajor,
                    installedVersion?.ToString() ?? "(unreadable)", installedTimescale ?? "(none)",
                    zipVersion?.ToString() ?? "(unreadable)", zipTimescale ?? "(none)");
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

        /* #3908: the same refusal for the extension. A package that cannot load the store's recorded TimescaleDB
           is a rollback past what it carries, and the runtime on disk is the one that can open the store. */
        if (PackageCannotLoadStore(dataDirectory, runtimeZipPath))
        {
            return new RuntimeAdvance(false, null, zipHash);
        }

        var previousRoot = PreviousRuntimeRootFor(runtimeRoot);
        var previousPgsql = Path.Combine(previousRoot, "pgsql");

        _logger.LogWarning(
            "The package ships a different Postgres runtime than the one extracted on this host — rescuing the current runtime to {Previous} and extracting the new one. This is the store runtime update (#1706); if the PostgreSQL major changed, an in-place pg_upgrade follows.",
            previousPgsql);

        try
        {
            EmptyDirectory(previousRoot);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* #3919: the rescue's rule below applies here too, and this used to sit outside it. A file held
               open under the last update's rescued runtime (an antivirus scan, an operator shell sitting in
               the folder) makes the delete throw. Re-creating the folder can fail too: a scanner still
               holding the one just deleted leaves it delete-pending, and a stray file by that name blocks
               it outright. Either way there is nowhere to rescue the current runtime to, which is a
               reason to SKIP the update, never a reason to refuse to start. The live runtime has not been
               touched and the stamp is not written, so the next start tries again. */
            _logger.LogWarning(
                "Could not clear the previous runtime at {PreviousRoot} to rescue the current one into it ({Message}). This is usually a file under it held open by an antivirus scan or a shell sitting in the folder. The store starts on its existing runtime and the update is retried on the next start.",
                previousRoot, ex.Message);
            return new RuntimeAdvance(false, null, zipHash);
        }

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
            TryEmptyDirectory(previousRoot);
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
            TryEmptyDirectory(previousRoot);
            throw;
        }

        File.WriteAllText(stampPath, zipHash);
        PinLegacyRuntimeStamp(runtimeRoot, _logger);
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
    ///
    /// <para><b>Refuses while a server is still running on the data directory (#3927).</b> A postmaster starts
    /// every backend from its own binaries' path, and Windows lets a directory holding a RUNNING executable be
    /// moved. So a revert under a server the upgrade could not stop moves the binaries out from under it.
    /// Reproduced on a copy of a real store, the next connection was served by an 18.4 backend under an 18.6
    /// postmaster, and the next service start adopted that postmaster (<c>pg_ctl status</c> answers "running"
    /// for one on any port) and then could not connect on the configured one. Refusing leaves that server
    /// whole and names what to stop.</para>
    ///
    /// <para>Returns whether the previous runtime is now in place. Every false has logged a CRITICAL saying what
    /// to do by hand, and a caller must never report a revert this did not make.</para>
    /// </summary>
    internal bool RevertRuntime(string runtimeRoot, string zipHash, string dataDirectory, int expectedDataMajor)
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
            return false;
        }

        var livePostmaster = FindLivePostmaster(dataDirectory);
        if (livePostmaster is not null)
        {
            _logger.LogCritical(
                "REFUSING to revert the store runtime: a PostgreSQL server (PID {Pid}) is still running on {DataDirectory}. Moving the runtime now would leave that server unable to start new backends, and the next service start would attach to it anyway. The runtime is left as it is. Stop that server first ({StopCommand}, or end the process), then move {Current} aside and move {Previous} into its place by hand before restarting the service.",
                livePostmaster, dataDirectory,
                $"\"{Path.Combine(previousPgsql, "bin", "pg_ctl.exe")}\" stop -D \"{dataDirectory}\" -m fast",
                pgsqlDirectory, previousPgsql);
            return false;
        }

        if (!Directory.Exists(previousPgsql))
        {
            _logger.LogCritical(
                "Cannot revert the store runtime: the rescued copy {Previous} is gone. The service will start whatever is in {Current}.",
                previousPgsql, pgsqlDirectory);
            return false;
        }

        /* MOVE the failed runtime aside, never delete-then-move. A delete can partially succeed — a
           postmaster that has not finished exiting still holds its binaries — and the swallowed
           failure would leave a half-emptied pgsql directory that the following Move then refuses to
           overwrite. The store is then left with a runtime missing pg_ctl.exe: a self-inflicted
           unbootable install, produced by the very path that exists to make failure safe. A rename
           either works completely or fails without touching anything. */
        var failedRuntime = pgsqlDirectory + ".failed";
        var movedAside = false;
        try
        {
            TryDeleteDirectory(failedRuntime);
            if (Directory.Exists(pgsqlDirectory))
            {
                Directory.Move(pgsqlDirectory, failedRuntime);
                movedAside = true;
            }

            Directory.Move(previousPgsql, pgsqlDirectory);
        }
        catch (Exception ex)
        {
            if (!movedAside)
            {
                _logger.LogCritical(
                    "Could not revert the store runtime ({Message}). Restore {Previous} over {Current} by hand before restarting the service.",
                    ex.Message, previousPgsql, pgsqlDirectory);
                return false;
            }

            /* #3927: each rename is all-or-nothing, but the PAIR is not. The first one had happened, so there
               is no runtime at pgsql at all, which is worse than either runtime, and this catch used to stop at
               logging. Put back the one that was there: the store is then exactly as it was before the revert
               was tried, and the rescued copy is still where a hand revert needs it. */
            try
            {
                Directory.Move(failedRuntime, pgsqlDirectory);
                _logger.LogCritical(
                    "Could not revert the store runtime ({Message}), so the runtime it had moved aside was put back at {Current} rather than leave no runtime there at all. The revert did not happen: restore {Previous} over it by hand before restarting the service.",
                    ex.Message, pgsqlDirectory, previousPgsql);
            }
            catch (Exception undo)
            {
                _logger.LogCritical(
                    "Could not revert the store runtime ({Message}), and could not put back the runtime it had moved aside either ({UndoMessage}): there is NO runtime at {Current}. Move {Previous} to that path by hand before restarting the service; the runtime that was moved aside is at {Failed}.",
                    ex.Message, undo.Message, pgsqlDirectory, previousPgsql, failedRuntime);
            }

            return false;
        }

        TryDeleteDirectory(failedRuntime);
        TryEmptyDirectory(previousRoot);

        /* Record the failing package so the next start does not run the same doomed upgrade again, and
           drop the stamp so the runtime on disk is not claimed to be the shipped one. #3927: this is
           bookkeeping AFTER the revert, so a marker that will not write no longer reports the revert itself
           as failed. It did not fail, and the hand restore that message named would have pointed at a
           rescued copy that is already gone. The stamp stays in that case: it still names the package that
           failed, which is what keeps the next start from retrying it. */
        var blockedPath = Path.Combine(runtimeRoot, RuntimeBlockedFileName);
        var stampPath = Path.Combine(runtimeRoot, RuntimeStampFileName);
        try
        {
            File.WriteAllText(blockedPath, zipHash);
            TryDeleteFile(stampPath);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger.LogWarning(
                "Reverted the store runtime, but could not record the failed package at {Blocked} ({Message}). The runtime stamp is left in place instead, which also keeps the next start from retrying this package; delete {Stamp} to force a retry.",
                blockedPath, ex.Message, stampPath);
        }

        _logger.LogWarning("Reverted to the previous Postgres runtime, for PostgreSQL {Major}.", expectedDataMajor);
        return true;
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
        var info = TryReadZipPgCtlVersionInfo(zipPath);
        if (info is null)
        {
            return null;
        }

        return info.FileMajorPart > 0
            ? info.FileMajorPart
            : ParsePostgresMajor(info.ProductVersion ?? info.FileVersion);
    }

    /// <summary>
    /// The full PostgreSQL version inside a runtime zip (18.6), from the same version resource as
    /// <see cref="TryReadZipPostgresMajor"/>. It reads the version STRING, not the numeric parts: EDB's
    /// <c>pg_ctl.exe</c> says "18.4" in its string but encodes it as 18.0.4 in the fixed-size block, so
    /// <c>FileMinorPart</c> reads 0 on every release. Null when no major.minor parses.
    /// </summary>
    internal static Version? TryReadZipPostgresVersion(string zipPath)
    {
        var info = TryReadZipPgCtlVersionInfo(zipPath);
        return info is null ? null : ParsePostgresVersion(info.ProductVersion) ?? ParsePostgresVersion(info.FileVersion);
    }

    /// <summary>
    /// Pulls <c>pgsql/bin/pg_ctl.exe</c> out of a runtime zip to a temp file and reads its version resource,
    /// which <see cref="System.Diagnostics.FileVersionInfo"/> copies in full before the file is deleted. Null
    /// when the entry is missing or the archive cannot be read.
    /// </summary>
    private static System.Diagnostics.FileVersionInfo? TryReadZipPgCtlVersionInfo(string zipPath)
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

            return System.Diagnostics.FileVersionInfo.GetVersionInfo(temp);
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

    /// <summary>
    /// The TimescaleDB version the EXTRACTED runtime installs and updates to: its <c>timescaledb.control</c>
    /// <c>default_version</c>. NOT the name of "the" versioned library: since #3908 a runtime also carries the
    /// libraries of older releases (fetch-pg-runtime.ps1's <c>$tsCarried</c>), and whichever
    /// <c>timescaledb-*.dll</c> a directory listing returns first is then an accident of name order.
    /// </summary>
    internal static string? TryReadInstalledTimescaleVersion(string binDirectory)
    {
        try
        {
            var pgsql = Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(binDirectory));
            var control = pgsql is null ? null : Path.Combine(pgsql, "share", "extension", "timescaledb.control");
            return control is not null && File.Exists(control)
                ? ParseTimescaleDefaultVersion(File.ReadAllText(control))
                : null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Unreadable control file answers "unknown", which the caller treats as a difference. */
            return null;
        }
    }

    /// <summary>
    /// The TimescaleDB version a runtime ZIP installs and updates to, from its control file entry, read without
    /// extracting. See <see cref="TryReadInstalledTimescaleVersion"/> for why this is not a library name.
    /// </summary>
    internal static string? TryReadZipTimescaleVersion(string zipPath)
    {
        try
        {
            using var archive = System.IO.Compression.ZipFile.OpenRead(zipPath);
            var control = archive.GetEntry("pgsql/share/extension/timescaledb.control")
                ?? archive.GetEntry("pgsql\\share\\extension\\timescaledb.control");
            if (control is null)
            {
                return null;
            }

            using var reader = new StreamReader(control.Open());
            return ParseTimescaleDefaultVersion(reader.ReadToEnd());
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidDataException)
        {
            /* Unreadable archive answers "unknown". */
            return null;
        }
    }

    /// <summary>
    /// Every TimescaleDB version the extracted runtime has both versioned libraries for: its own default plus the
    /// carried builds of older releases (#3908). These are the versions a store's extension can be at and
    /// still load, and the ones pg_upgrade can restore a store at. Empty when the directory cannot be read.
    /// </summary>
    internal static IReadOnlyList<string> TryReadTimescaleLibraryVersions(string pgsqlDirectory)
    {
        var names = new List<string>();
        try
        {
            foreach (var file in Directory.EnumerateFiles(Path.Combine(pgsqlDirectory, "lib"), "timescaledb-*.dll"))
            {
                names.Add(file);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* An unreadable lib directory carries nothing anyone can rely on. */
        }

        return CompleteTimescaleLibraryVersions(names);
    }

    /// <summary>
    /// <see cref="TryReadTimescaleLibraryVersions"/> for a runtime ZIP, from its entry names alone.
    /// </summary>
    internal static IReadOnlyList<string> TryReadZipTimescaleLibraryVersions(string zipPath)
    {
        var names = new List<string>();
        try
        {
            using var archive = System.IO.Compression.ZipFile.OpenRead(zipPath);
            foreach (var entry in archive.Entries)
            {
                if (entry.FullName.Replace('\\', '/').StartsWith("pgsql/lib/", StringComparison.OrdinalIgnoreCase))
                {
                    names.Add(entry.Name);
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidDataException)
        {
            /* Unreadable archive carries nothing anyone can rely on. */
        }

        return CompleteTimescaleLibraryVersions(names);
    }

    /// <summary>
    /// The versions with BOTH versioned libraries among <paramref name="fileNames"/> (#3908):
    /// <c>timescaledb-V.dll</c> and <c>timescaledb-tsl-V.dll</c>. A version with only the first is not one a store
    /// can run on: compression and continuous aggregates, which every Darling store uses, live in the TSL library.
    /// </summary>
    internal static IReadOnlyList<string> CompleteTimescaleLibraryVersions(IEnumerable<string> fileNames)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var fileName in fileNames)
        {
            names.Add(Path.GetFileName(fileName));
        }

        var versions = new List<string>();
        foreach (var name in names)
        {
            if (ParseTimescaleLibraryVersion(name) is { } version
                && names.Contains(string.Concat("timescaledb-tsl-", version, ".dll")))
            {
                versions.Add(version);
            }
        }

        versions.Sort(StringComparer.Ordinal);
        return versions;
    }

    /// <summary>
    /// A TimescaleDB release version ("2.30.1"), or null for anything else, prerelease tags included. Strict,
    /// because the bridge interpolates a version into <c>ALTER EXTENSION ... UPDATE TO</c> and the data
    /// directory's record is parsed with it.
    /// </summary>
    internal static Version? ParseTimescaleVersion(string? text)
    {
        var parts = text?.Split('.');
        if (parts is not { Length: 3 })
        {
            return null;
        }

        foreach (var part in parts)
        {
            if (part.Length is 0 or > 4)
            {
                return null;
            }

            foreach (var character in part)
            {
                if (!char.IsAsciiDigit(character))
                {
                    return null;
                }
            }
        }

        return new Version(text!);
    }

    /* ============================ the store's TimescaleDB extension (#3908) ============================ */

    /// <summary>
    /// <c>ALTER EXTENSION timescaledb UPDATE</c> as the FIRST and ONLY statement of a fresh session (#3908). That
    /// is the only form TimescaleDB accepts. Its loader loads the versioned library for the installed version on
    /// the first statement of any session in the database, so every other shape fails, each measured:
    /// <list type="bullet">
    /// <item>any statement first, when that library is absent, fails with 58P01 "could not access file";</item>
    /// <item>when the library is present, TimescaleDB refuses the later ALTER ("cannot be updated after the old
    /// version has already been loaded");</item>
    /// <item>Npgsql's own type-loading query is a first statement, which is why the data source disables it;
    /// the connection-string form of that switch is obsolete in Npgsql 9 and later.</item>
    /// </list>
    /// With <paramref name="toVersion"/> null the target is the runtime control file's default_version;
    /// otherwise it is that version, which must parse as one because it is interpolated. The method decides
    /// nothing: the caller has already read, on a separate session, that the extension is installed and not at
    /// the target, and it verifies the result on another. Every failure throws, and a database without the
    /// extension is one (42704), because the update script raises that same code for a catalog object it
    /// expected and did not find (measured), so it cannot mean "absent" here. Only the CONNECTION is retried, on a
    /// transport fault; the ALTER is sent once, because a statement that may have run for most of an hour is not
    /// a transient to repeat.
    /// </summary>
    internal static async Task UpdateTimescaleAsFirstStatementAsync(
        string ownerConnectionString, string database, CancellationToken cancellationToken, string? toVersion = null)
    {
        if (toVersion is not null && ParseTimescaleVersion(toVersion) is null)
        {
            throw new ArgumentException($"'{toVersion}' is not a TimescaleDB release version.", nameof(toVersion));
        }

        var connectionString = new NpgsqlConnectionStringBuilder(ownerConnectionString)
        {
            Database = database,
            Pooling = false,
            SearchPath = null,
        }.ConnectionString;

        var sourceBuilder = new NpgsqlDataSourceBuilder(DarlingStoreConnection.PinSessionTimeZoneUtc(connectionString));
        sourceBuilder.ConfigureTypeLoading(typeLoading => typeLoading.EnableTypeLoading(false));
        await using var source = sourceBuilder.Build();
        await using var connection = await WithTransportRetryAsync(
            () => source.OpenConnectionAsync(cancellationToken).AsTask(), cancellationToken);
        await using var update = new NpgsqlCommand(
            toVersion is null ? "ALTER EXTENSION timescaledb UPDATE" : $"ALTER EXTENSION timescaledb UPDATE TO '{toVersion}'",
            connection) { CommandTimeout = (int)s_bridgeTimeout.TotalSeconds };
        await update.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// A database's installed TimescaleDB version, read on a session that tells the loader not to load any
    /// versioned library (<c>timescaledb.disable_load</c>, measured on 2.28.1 and 2.30.1). The read works
    /// whether or not this runtime carries the installed version's library, and it loads nothing, so a probe can
    /// never be the session that pins the old library. Null when the extension is absent. Any failure throws,
    /// because "unreadable" is not "absent".
    /// </summary>
    internal static async Task<string?> ReadTimescaleVersionUnloadedAsync(
        string ownerConnectionString, string database, CancellationToken cancellationToken)
    {
        var connectionString = new NpgsqlConnectionStringBuilder(ownerConnectionString)
        {
            Database = database,
            Pooling = false,
            SearchPath = null,
            Options = "-c timescaledb.disable_load=on",
        }.ConnectionString;

        await using var connection = await OpenWithTransportRetryAsync(connectionString, cancellationToken);
        return await ReadExtversionAsync(connection, cancellationToken);
    }

    /// <summary>
    /// The same read on an ORDINARY session, whose first statement loads the installed version's library. A
    /// successful read therefore also proves the database opens on this runtime, which is the claim a verify has
    /// to make.
    /// </summary>
    internal static async Task<string?> ReadTimescaleVersionLoadedAsync(
        string ownerConnectionString, string database, CancellationToken cancellationToken)
    {
        var connectionString = new NpgsqlConnectionStringBuilder(ownerConnectionString)
        {
            Database = database,
            Pooling = false,
            SearchPath = null,
        }.ConnectionString;

        await using var connection = await OpenWithTransportRetryAsync(connectionString, cancellationToken);
        return await ReadExtversionAsync(connection, cancellationToken);
    }

    private static async Task<string?> ReadExtversionAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    private static Task<NpgsqlConnection> OpenWithTransportRetryAsync(string connectionString, CancellationToken cancellationToken)
        => WithTransportRetryAsync(
            async () =>
            {
                var connection = new NpgsqlConnection(DarlingStoreConnection.PinSessionTimeZoneUtc(connectionString));
                try
                {
                    await connection.OpenAsync(cancellationToken);
                    return connection;
                }
                catch
                {
                    await connection.DisposeAsync();
                    throw;
                }
            },
            cancellationToken);

    /* ---- the data directory's record of it ---- */

    /// <summary>
    /// The file in the data directory recording the store database's TimescaleDB version as last READ (#3908). In
    /// the data directory rather than the runtime because it describes the data: it moves with a restored or
    /// copied store, and a cluster pg_upgrade produces starts without one, which is right, because nothing has
    /// read that cluster's extension yet.
    ///
    /// <para>An ABSENT extension is recorded together with the runtime version it was read under, and means
    /// "nothing to move" only while that runtime is the one shipping. The first start of a new store reads it
    /// before the worker creates the extension, at the runtime's own version, so the next start on the same
    /// runtime has nothing to move either. An unqualified "absent" skipped the update after a runtime change,
    /// which the gated tests caught.</para>
    /// </summary>
    internal const string TimescaleRecordFileName = "darling-timescaledb.version";

    internal enum TimescaleRecordState
    {
        /// <summary>The store database's extension was read at <see cref="TimescaleRecord.Version"/>.</summary>
        Current,

        /// <summary>
        /// An update from <see cref="TimescaleRecord.Version"/> to <see cref="TimescaleRecord.PendingTo"/> started
        /// and nothing has read the result, so the store is at one of the two.
        /// </summary>
        Pending,

        /// <summary>
        /// The store database had no timescaledb extension when read under the runtime whose TimescaleDB is
        /// <see cref="TimescaleRecord.Version"/>. It carries no library requirement.
        /// </summary>
        Absent,
    }

    internal sealed record TimescaleRecord(TimescaleRecordState State, string? Version = null, string? PendingTo = null)
    {
        /// <summary>The versions the store database can be at: what a runtime must carry libraries for to open it.</summary>
        public IReadOnlyList<string> StoreVersions => State switch
        {
            TimescaleRecordState.Current => [Version!],
            TimescaleRecordState.Pending => [Version!, PendingTo!],
            _ => [],
        };

        public string Format() => State switch
        {
            TimescaleRecordState.Current => Version!,
            TimescaleRecordState.Pending => $"pending {Version} {PendingTo}",
            _ => $"absent {Version}",
        };
    }

    /// <summary>The record's one line, parsed. Null for anything malformed, which every caller treats as no record.</summary>
    internal static TimescaleRecord? ParseTimescaleRecord(string? text)
    {
        var parts = text?.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries) ?? Array.Empty<string>();
        return parts switch
        {
            [var version] when ParseTimescaleVersion(version) is not null
                => new TimescaleRecord(TimescaleRecordState.Current, version),
            ["pending", var from, var to] when ParseTimescaleVersion(from) is not null && ParseTimescaleVersion(to) is not null
                => new TimescaleRecord(TimescaleRecordState.Pending, from, to),
            ["absent", var runtime] when ParseTimescaleVersion(runtime) is not null
                => new TimescaleRecord(TimescaleRecordState.Absent, runtime),
            _ => null,
        };
    }

    internal static TimescaleRecord? ReadTimescaleRecord(string dataDirectory)
        => ParseTimescaleRecord(ReadTrimmedOrNull(Path.Combine(dataDirectory, TimescaleRecordFileName)));

    /// <summary>Writes the record. Bookkeeping, so never a failure: one that will not write is logged and the start goes on.</summary>
    internal static void WriteTimescaleRecord(string dataDirectory, TimescaleRecord record, ILogger logger)
    {
        var path = Path.Combine(dataDirectory, TimescaleRecordFileName);
        try
        {
            File.WriteAllText(path, record.Format());
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            logger.LogWarning(
                "Could not record the store's TimescaleDB state in {Path} ({Message}). The next start reads it again, at the cost of one extra start and stop of the store.",
                path, ex.Message);
        }
    }

    /// <summary>
    /// Whether this start runs the quiesced update (#3908), PURE so the gate is pinned without a cluster. It runs
    /// unless the record says the store is already at the runtime's version, or had no extension under this same
    /// runtime. No record (a store this release has not read yet, a cluster pg_upgrade just produced) and a
    /// pending one (a start that stopped mid-update) both run it, and on a store that turns out current it moves
    /// nothing.
    /// </summary>
    internal static bool NeedsQuiescedTimescaleUpdate(TimescaleRecord? record, string? bundledTimescaleVersion)
    {
        if (string.IsNullOrEmpty(bundledTimescaleVersion))
        {
            return false;
        }

        return record is not { State: TimescaleRecordState.Current or TimescaleRecordState.Absent }
            || !string.Equals(record.Version, bundledTimescaleVersion, StringComparison.Ordinal);
    }

    /* ---- what a start did to it ---- */

    internal enum TimescaleUpdateStatus
    {
        /// <summary>Nothing to report: already current, no extension, or nothing ran and nothing needed to.</summary>
        None,

        /// <summary>The extension moved to the runtime's version before the store opened.</summary>
        Updated,

        /// <summary>
        /// The update ran and did not reach the runtime's version. The store opens on the version it had, whose
        /// library the runtime carries.
        /// </summary>
        Failed,

        /// <summary>After start, the extension is not the runtime's version, and no update ran this start.</summary>
        Behind,
    }

    /// <summary>
    /// What this start did to the store database's TimescaleDB extension (#3908), carried out of the bootstrap
    /// like <see cref="StoreUpgradeOutcome"/> and alerted separately from it: a start that upgrades the PostgreSQL
    /// major and then cannot move the extension has two things to say, and one alert would have to drop one.
    /// </summary>
    internal sealed record TimescaleUpdateOutcome(TimescaleUpdateStatus Status, string? From, string? To, string? Message)
    {
        public static TimescaleUpdateOutcome None { get; } = new(TimescaleUpdateStatus.None, null, null, null);
    }

    /* ---- the quiesced update ---- */

    /// <summary>
    /// The file that exists while a quiesced start (<see cref="UpdateTimescaleQuiescedAsync"/>'s TimescaleDB
    /// update, or <see cref="CarryAutoConfAsync"/>'s auto.conf trial) may have a server running on its private
    /// port, holding that port. See <see cref="StopQuiescedUpdateOrphanAsync"/>.
    /// </summary>
    internal const string QuiescedUpdateMarkerFileName = "darling-timescaledb-update.port";

    /// <summary>
    /// The quiesced start's server options, after the loopback listen <see cref="StartClusterAsync"/> sets.
    /// TimescaleDB's background workers off, so the scheduler cannot load the old library or run a job
    /// mid-update. Autovacuum off, because with its one-second naptime it otherwise ran sixteen times inside the
    /// window (measured), for nothing.
    /// </summary>
    private const string QuiescedUpdateServerOptions = " -c timescaledb.max_background_workers=0 -c autovacuum=off";

    /// <summary>
    /// How long the quiesced start may take to accept connections. Generous, because crash recovery lands here
    /// now: this is the first start after whatever stopped the store last.
    /// </summary>
    private const int QuiescedStartWaitSeconds = 900;

    /// <summary>
    /// Moves the store's TimescaleDB extension to the runtime's own version while nothing else can reach the
    /// store (#3908). The cluster is started with <paramref name="binDirectory"/> on a private loopback port, with
    /// TimescaleDB's background workers and autovacuum off. No service component, Viewer seat or scheduled job can
    /// reach it, so the only sessions are this method's. In every database that allows connections, the version is
    /// read on a session that loads nothing (<see cref="ReadTimescaleVersionUnloadedAsync"/>); a database behind
    /// the runtime gets <see cref="UpdateTimescaleAsFirstStatementAsync"/> and then an ordinary session that both
    /// reads the result and proves the new library loads. Then the cluster is stopped, confirmed, and the normal
    /// start brings the scheduler up on the new version.
    ///
    /// <para>Why not after the normal start, where the update used to run: measured, every session that had
    /// touched the old library died on its next statement once the extension moved ("already loaded with a
    /// different version"), parallel queries failed, and the update's own pre-update step killed running jobs,
    /// which the scheduler then held in crash backoff for up to an hour. The web host, MCP host, Viewer seats and
    /// the scheduler are all connected by then.</para>
    ///
    /// <para>The STORE database (<see cref="DarlingManagedPostgres.DatabaseName"/>) decides the outcome. Another
    /// database's failure is logged and ignored, because the only outage here is a store that cannot open. A
    /// failure never reverts anything: the runtime carries the library of every TimescaleDB version this service
    /// has shipped, so a store whose update failed opens on its own version, and the returned outcome says so.
    /// Never throws except on cancellation, and the cluster is stopped whatever happened. The record is written
    /// "pending" before the store's ALTER, so a start that stops after the ALTER commits still names both
    /// versions for the rollback guard; the post-start read writes the final one.</para>
    /// </summary>
    internal async Task<TimescaleUpdateOutcome> UpdateTimescaleQuiescedAsync(
        string binDirectory, string dataDirectory, string password, string bundledTimescaleVersion, CancellationToken cancellationToken)
    {
        var marker = Path.Combine(dataDirectory, QuiescedUpdateMarkerFileName);
        int port;
        try
        {
            port = FindFreeLoopbackPort();

            /* Without the marker, a server this start could not stop would be adopted as the store. */
            File.WriteAllText(marker, port.ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or System.Net.Sockets.SocketException)
        {
            _logger.LogCritical(
                "The TimescaleDB update did not start: {Message}. Nothing was touched, and the store opens on the version it has; the update is retried on the next service start.",
                ex.Message);
            return new TimescaleUpdateOutcome(
                TimescaleUpdateStatus.Failed, null, bundledTimescaleVersion,
                $"the update could not prepare its private start ({ex.Message})");
        }

        var owner = DarlingManagedPostgres.BuildConnectionString(port, password);

        string? before = null;
        try
        {
            _logger.LogInformation(
                "Checking the store's TimescaleDB against the runtime's {Version} before the store opens (#3908): starting it on private port {Port} with background workers off.",
                bundledTimescaleVersion, port);
            await StartClusterAsync(binDirectory, dataDirectory, port, cancellationToken, QuiescedUpdateServerOptions, QuiescedStartWaitSeconds);

            string? after = null;
            foreach (var database in await ListConnectableDatabasesAsync(owner, cancellationToken))
            {
                var isStore = string.Equals(database, DarlingManagedPostgres.DatabaseName, StringComparison.Ordinal);
                string? found = null;
                try
                {
                    found = await ReadTimescaleVersionUnloadedAsync(owner, database, cancellationToken);
                    if (isStore)
                    {
                        before = found;
                        after = found;
                    }

                    if (found is null || string.Equals(found, bundledTimescaleVersion, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (isStore)
                    {
                        WriteTimescaleRecord(
                            dataDirectory, new TimescaleRecord(TimescaleRecordState.Pending, found, bundledTimescaleVersion), _logger);
                    }

                    await UpdateTimescaleAsFirstStatementAsync(owner, database, cancellationToken);
                    var moved = await ReadTimescaleVersionLoadedAsync(owner, database, cancellationToken);
                    if (!string.Equals(moved, bundledTimescaleVersion, StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException(
                            $"TimescaleDB in database '{database}' is {moved ?? "(absent)"} after ALTER EXTENSION UPDATE, not the runtime's {bundledTimescaleVersion}");
                    }

                    if (isStore)
                    {
                        after = moved;
                    }

                    _logger.LogWarning(
                        "TimescaleDB in database '{Database}' updated {From} -> {To} before the store opened (#3908).",
                        database, found, moved);
                }
                catch (Exception ex) when (ex is not OperationCanceledException && !isStore)
                {
                    _logger.LogWarning(
                        "Could not update TimescaleDB in database '{Database}' ({Message}). It is not the store database, so the store opens regardless; '{Database}' keeps TimescaleDB {Found}, and opens only while the runtime carries that version's libraries.",
                        database, ex.Message, database, found ?? "(unread)");
                }
            }

            return before is not null && !string.Equals(before, after, StringComparison.Ordinal)
                ? new TimescaleUpdateOutcome(TimescaleUpdateStatus.Updated, before, after, null)
                : TimescaleUpdateOutcome.None;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogCritical(
                "Could not update the store's TimescaleDB from {From} to {To} ({Message}). Nothing is reverted: the store opens on the version it has, whose library the runtime carries. The update is retried on the next service start.",
                before ?? "(unread)", bundledTimescaleVersion, ex.Message);
            return new TimescaleUpdateOutcome(TimescaleUpdateStatus.Failed, before, bundledTimescaleVersion, ex.Message);
        }
        finally
        {
            /* Whatever happened, cancellation included, and whether or not the start reported success: a start
               that timed out may still have a postmaster recovering behind it. */
            if (await StopClusterConfirmedAsync(binDirectory, dataDirectory))
            {
                TryDeleteFile(marker);
            }
            else
            {
                _logger.LogCritical(
                    "The server started on private port {Port} to update TimescaleDB would not stop. {Marker} is kept, so the next start recognizes the server and stops it rather than adopting it as the store.",
                    port, marker);
            }
        }
    }

    /// <summary>
    /// Stops a server a quiesced start (<see cref="UpdateTimescaleQuiescedAsync"/>'s TimescaleDB update, or
    /// <see cref="CarryAutoConfAsync"/>'s auto.conf trial) left on its private port, which happens only when the
    /// process died, or the stop failed, between that start and its confirmed stop. The marker holds the port,
    /// and the running postmaster must be on it (<c>postmaster.pid</c>'s fourth line): a server on any other
    /// port was started by someone else after the marker was left, and is adopted as usual. Without this the
    /// normal start would adopt the orphan (<c>pg_ctl status</c> answers "running" for a postmaster on any
    /// port), then fail to reach it on the configured port, and every runtime update would be deferred behind
    /// it. Returns false only when such an orphan is running and will not stop.
    /// </summary>
    internal async Task<bool> StopQuiescedUpdateOrphanAsync(string binDirectory, string dataDirectory)
    {
        var marker = Path.Combine(dataDirectory, QuiescedUpdateMarkerFileName);
        var markedPort = ReadTrimmedOrNull(marker);
        if (markedPort is null)
        {
            return true;
        }

        if (string.Equals(TryReadPostmasterPort(dataDirectory), markedPort, StringComparison.Ordinal))
        {
            _logger.LogWarning(
                "Found the store on private port {Port}, where a quiesced start (TimescaleDB update or auto.conf trial) this service started left it. Stopping it before the normal start.",
                markedPort);
            if (!await StopClusterConfirmedAsync(binDirectory, dataDirectory))
            {
                return false;
            }
        }

        TryDeleteFile(marker);
        return true;
    }

    /// <summary>The port a <c>postmaster.pid</c> records (its fourth line), or null when there is no readable file.</summary>
    internal static string? TryReadPostmasterPort(string dataDirectory)
    {
        try
        {
            using var stream = new FileStream(
                Path.Combine(dataDirectory, "postmaster.pid"), FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            using var reader = new StreamReader(stream);
            for (var line = 1; ; line++)
            {
                var text = reader.ReadLine();
                if (text is null)
                {
                    return null;
                }

                if (line == 4)
                {
                    return text.Trim();
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    /// <summary>
    /// Stops the cluster on <paramref name="dataDirectory"/> and CONFIRMS it is down: a fast shutdown, then an
    /// immediate one if that did not take, and <c>pg_ctl status</c> must answer "not running" (3). Never throws.
    /// Confirmation is the point. A runtime directory moves under a live postmaster on Windows (measured), and a
    /// postmaster left behind on a private port is adopted by the next start, which then cannot reach it.
    /// </summary>
    private async Task<bool> StopClusterConfirmedAsync(string binDirectory, string dataDirectory)
    {
        var pgCtl = Path.Combine(binDirectory, "pg_ctl.exe");
        foreach (var mode in new[] { "fast", "immediate" })
        {
            if (await IsStoppedAsync())
            {
                return true;
            }

            try
            {
                var (exitCode, output) = await DarlingManagedPostgres.RunToolAsync(
                    pgCtl, $"stop -D \"{dataDirectory}\" -m {mode} -w -t 120", s_toolTimeout, CancellationToken.None);
                if (exitCode != 0)
                {
                    _logger.LogWarning(
                        "pg_ctl stop -m {Mode} reported exit code {ExitCode}: {Output}",
                        mode, exitCode, DarlingToolExitCode.FormatOutput(output, exitCode));
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("pg_ctl stop -m {Mode} failed: {Message}", mode, ex.Message);
            }
        }

        return await IsStoppedAsync();

        async Task<bool> IsStoppedAsync()
        {
            try
            {
                var (exitCode, _) = await DarlingManagedPostgres.RunToolAsync(
                    pgCtl, $"status -D \"{dataDirectory}\"", s_toolTimeout, CancellationToken.None);
                return exitCode == 3;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }

    /// <summary>
    /// The store's TimescaleDB after the normal start (#3908): read it, record it, and settle what this start
    /// reports. Read on an ordinary session of the running store, so the version is the one every session now
    /// loads. The record is written from what was READ, never from what was intended, so a stale record corrects
    /// itself here and the next start's gate acts on the truth. Returns the outcome and the version read. Never
    /// throws except on cancellation: the store is up, and the worst this can do is fail to report.
    /// </summary>
    internal async Task<(TimescaleUpdateOutcome Outcome, string? Installed)> VerifyTimescaleAfterStartAsync(
        string ownerConnectionString,
        string dataDirectory,
        string bundledTimescaleVersion,
        TimescaleUpdateOutcome quiesced,
        CancellationToken cancellationToken)
    {
        string? installed;
        try
        {
            installed = await ReadTimescaleVersionLoadedAsync(ownerConnectionString, DarlingManagedPostgres.DatabaseName, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogCritical(
                "Could not read the store's TimescaleDB version after start ({Message}). The store is running; check its extension with SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'.",
                ex.Message);
            return (quiesced, null);
        }

        if (installed is null)
        {
            /* Recorded under this runtime (see TimescaleRecordFileName), so the next start on it has nothing to
               read, and the first start after a runtime change reads it again. */
            WriteTimescaleRecord(dataDirectory, new TimescaleRecord(TimescaleRecordState.Absent, bundledTimescaleVersion), _logger);
            return (TimescaleUpdateOutcome.None, null);
        }

        WriteTimescaleRecord(dataDirectory, new TimescaleRecord(TimescaleRecordState.Current, installed), _logger);

        if (string.Equals(installed, bundledTimescaleVersion, StringComparison.Ordinal))
        {
            if (quiesced.Status != TimescaleUpdateStatus.Failed)
            {
                return (quiesced, installed);
            }

            /* A reported failure, and yet the store is current: either its ALTER committed and a later step
               failed (an update that landed), or the step failed before it read anything and the store was
               already current (nothing moved). Neither is anything to alert on, and the log says which. */
            if (quiesced.From is not null && !string.Equals(quiesced.From, installed, StringComparison.Ordinal))
            {
                _logger.LogWarning(
                    "The TimescaleDB update reported a failure after its ALTER committed ({Message}); the store is on {Installed}, the runtime's version, so the update {From} -> {Installed} landed.",
                    quiesced.Message, installed, quiesced.From, installed);
                return (quiesced with { Status = TimescaleUpdateStatus.Updated, To = installed, Message = null }, installed);
            }

            _logger.LogInformation(
                "The store's TimescaleDB is already {Installed}, the runtime's version; the update step that failed ({Message}) had nothing to move.",
                installed, quiesced.Message);
            return (TimescaleUpdateOutcome.None, installed);
        }

        if (quiesced.Status == TimescaleUpdateStatus.Failed)
        {
            return (quiesced with { From = installed }, installed);
        }

        _logger.LogCritical(
            "The store's TimescaleDB is {Installed}, but this runtime ships {Bundled}, and no update ran this start. It moves the next time this service starts the store itself, before anything can connect; a server started by something else, or one this service adopted, keeps this version until then.",
            installed, bundledTimescaleVersion);
        return (new TimescaleUpdateOutcome(TimescaleUpdateStatus.Behind, installed, bundledTimescaleVersion, null), installed);
    }

    /// <summary>
    /// Whether the shipped package cannot open this store's TimescaleDB (#3908's rollback guard, for the releases
    /// after this one). The data directory's record names the version or versions the store database can be at,
    /// and a package without both libraries for each cannot load it: swapping it in would take the store down with
    /// no alert able to say why. That is what re-installing this release over a later one looks like, once the
    /// later one has moved the extension past anything this package carries. Refusing keeps the runtime on disk,
    /// which is the one that opened the store last. The stamp is not written, so the refusal repeats until a
    /// package that can load the store ships. No record abstains.
    /// </summary>
    private bool PackageCannotLoadStore(string dataDirectory, string runtimeZipPath)
    {
        var missing = MissingTimescaleLibraries(dataDirectory, runtimeZipPath);
        if (missing.Count == 0)
        {
            return false;
        }

        var record = ReadTimescaleRecord(dataDirectory)!;
        var carried = TryReadZipTimescaleLibraryVersions(runtimeZipPath);

        _logger.LogWarning(
            "Keeping the extracted Postgres runtime instead of the shipped one at {Zip}: this store's TimescaleDB is {Store} (recorded in {Record}), and the package has no libraries for {Missing} (it carries {Carried}), so it could not open the store. This is what installing an older release over a newer one looks like. The runtime on disk, which opened the store last, stays until a package that carries TimescaleDB {Missing} ships.",
            runtimeZipPath,
            string.Join(" or ", record.StoreVersions),
            Path.Combine(dataDirectory, TimescaleRecordFileName),
            string.Join(" and ", missing),
            carried.Count == 0 ? "none" : string.Join(", ", carried),
            string.Join(" and ", missing));
        return true;
    }

    /// <summary>
    /// The store's recorded TimescaleDB versions that <paramref name="runtimeZipPath"/> has no libraries for
    /// (#3908). Empty when there is no record, or the record names no version. Shared by the swap path's
    /// refusal and the first-extraction guard, so the two cannot disagree about what "can open the store" means.
    /// </summary>
    internal static IReadOnlyList<string> MissingTimescaleLibraries(string dataDirectory, string runtimeZipPath)
    {
        var record = ReadTimescaleRecord(dataDirectory);
        if (record is null || record.StoreVersions.Count == 0)
        {
            return [];
        }

        var carried = TryReadZipTimescaleLibraryVersions(runtimeZipPath);
        var missing = new List<string>();
        foreach (var version in record.StoreVersions)
        {
            if (!carried.Contains(version, StringComparer.Ordinal))
            {
                missing.Add(version);
            }
        }

        return missing;
    }

    /// <summary>
    /// What the bridge does to one database's extension before pg_upgrade (#3908). <see cref="Needed"/> false: the
    /// new runtime carries the installed version's libraries, so pg_upgrade restores it as it is and the quiesced
    /// update moves it after the upgrade commits. Otherwise <see cref="Target"/> is the version to move it to, or
    /// null when there is none and the upgrade cannot proceed.
    /// </summary>
    internal sealed record TimescaleBridgePlan(bool Needed, string? Target);

    /// <summary>
    /// The bridge decision, PURE (#3908). The one version the OLD runtime can update to is its own default: a
    /// version it merely carries has libraries but no update scripts, so an ALTER cannot reach it. That default is
    /// the target when it is above the installed version and the new runtime carries its libraries, so it can
    /// restore the result. Anything else leaves no target.
    /// </summary>
    internal static TimescaleBridgePlan PlanTimescaleBridge(
        string installed, IReadOnlyCollection<string> newRuntimeLibraries, string? oldRuntimeDefaultVersion)
    {
        if (newRuntimeLibraries.Contains(installed, StringComparer.Ordinal))
        {
            return new TimescaleBridgePlan(false, null);
        }

        var target = ParseTimescaleVersion(oldRuntimeDefaultVersion);
        var floor = ParseTimescaleVersion(installed);
        var reachable = target is not null
            && (floor is null || target > floor)
            && newRuntimeLibraries.Contains(oldRuntimeDefaultVersion!, StringComparer.Ordinal);

        return new TimescaleBridgePlan(true, reachable ? oldRuntimeDefaultVersion : null);
    }

    /// <summary>
    /// The databases that allow connections, read from <c>postgres</c>. If that read fails (a <c>postgres</c> that
    /// itself carries an unloadable extension), the store database alone is returned: it is the one that matters.
    /// </summary>
    private async Task<IReadOnlyList<string>> ListConnectableDatabasesAsync(string ownerConnectionString, CancellationToken cancellationToken)
    {
        try
        {
            var builder = new NpgsqlConnectionStringBuilder(ownerConnectionString) { Database = "postgres", Pooling = false, SearchPath = null };
            await using var connection = await OpenWithTransportRetryAsync(builder.ConnectionString, cancellationToken);
            await using var command = new NpgsqlCommand(
                "SELECT datname FROM pg_database WHERE datallowconn ORDER BY datname", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var databases = new List<string>();
            while (await reader.ReadAsync(cancellationToken))
            {
                databases.Add(reader.GetString(0));
            }

            return databases;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("Could not list the cluster's databases ({Message}); updating TimescaleDB in the store database only.", ex.Message);
            return new[] { DarlingManagedPostgres.DatabaseName };
        }
    }

    /// <summary>
    /// Retries <paramref name="action"/> on a TRANSPORT fault only (a backend that lost the post-start
    /// shared-memory race, #2185), the same classification <c>OpenProbedMaintenanceConnectionAsync</c> uses. Every caller wraps a
    /// connection OPEN in it and nothing else, so what a retry repeats is a connection attempt, never a
    /// statement. Any PostgreSQL error is an answer, not a transient, and propagates at once.
    /// </summary>
    private static async Task<T> WithTransportRetryAsync<T>(Func<Task<T>> action, CancellationToken cancellationToken)
    {
        const int attempts = 6;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await action();
            }
            catch (Exception ex) when (attempt < attempts && PostgresTransportFault.IsTransportFault(ex))
            {
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
        }
    }

    /// <summary>A loopback TCP port nothing is listening on right now, for a cluster only this process will use.</summary>
    internal static int FindFreeLoopbackPort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    /// <summary>The extracted runtime's <c>pg_ctl --version</c> line, or null when it will not report one.</summary>
    private static async Task<string?> ReadRuntimeVersionLineAsync(string binDirectory, CancellationToken cancellationToken)
    {
        try
        {
            var (exitCode, output) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(binDirectory, "pg_ctl.exe"), "--version", s_toolTimeout, cancellationToken);
            return exitCode == 0 ? output : null;
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

    /// <summary>
    /// Keeps <see cref="LegacyRuntimeStampFileName"/> holding <see cref="LegacyRuntimePackageHash"/> (#3908), so
    /// a rollback to 3.3 through 3.8 keeps the runtime it finds instead of swapping in one that cannot load the
    /// store. Called on every start, the first extraction included. Best effort: a failure is logged, and the
    /// store this release runs is unaffected either way.
    /// </summary>
    internal static void PinLegacyRuntimeStamp(string runtimeRoot, ILogger logger)
    {
        var legacyPath = Path.Combine(runtimeRoot, LegacyRuntimeStampFileName);
        try
        {
            if (!string.Equals(ReadTrimmedOrNull(legacyPath), LegacyRuntimePackageHash, StringComparison.OrdinalIgnoreCase))
            {
                File.WriteAllText(legacyPath, LegacyRuntimePackageHash);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            logger.LogWarning(
                "Could not pin {Path} ({Message}). Only a rollback to 3.3 through 3.8 depends on it: such a release could swap in a runtime that cannot load this store.",
                legacyPath, ex.Message);
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

    /// <summary>
    /// Empties <paramref name="path"/> — deletes every child file and subfolder, recursively — without
    /// deleting the folder itself, and creates it only if it is missing. #4052: the install root is narrowed
    /// to Read &amp; Execute for the service account, which keeps Modify on <c>pg-runtime\</c> and
    /// <c>pg-runtime-prev\</c> themselves but not on the root above them — so a delete-then-recreate of
    /// <c>pg-runtime-prev</c> can delete (Modify on the folder covers that) and then fail to recreate (no
    /// rights on the root to add a new child there). Emptying in place needs no right on the root at all.
    /// </summary>
    /// <exception cref="UnauthorizedAccessException">
    /// The folder does not exist and cannot be created — the installer or upgrade script must be re-run to
    /// pre-create it under the now-narrowed root.
    /// </exception>
    internal static void EmptyDirectory(string path)
    {
        if (Directory.Exists(path))
        {
            foreach (var entry in Directory.EnumerateFileSystemEntries(path))
            {
                if (Directory.Exists(entry))
                {
                    Directory.Delete(entry, recursive: true);
                }
                else
                {
                    File.Delete(entry);
                }
            }

            return;
        }

        try
        {
            Directory.CreateDirectory(path);
        }
        catch (UnauthorizedAccessException ex)
        {
            throw new UnauthorizedAccessException(
                $"{path} does not exist and this service account cannot create it under the narrowed install root. Re-run the installer or upgrade script to pre-create it.",
                ex);
        }
    }

    /// <summary>
    /// Best-effort <see cref="EmptyDirectory"/> — never throws, matching <see cref="TryDeleteDirectory"/>'s
    /// contract for the cleanup call sites that only log a warning on failure.
    /// </summary>
    internal static void TryEmptyDirectory(string path)
    {
        try
        {
            EmptyDirectory(path);
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
        Action<string> AppendManagedConf,
        Func<string> SslServerOptions);

    /* ============================ postgresql.auto.conf carry (#4253) ============================ */

    /// <summary>
    /// One <c>name = value</c> assignment read from a postgresql.auto.conf. <see cref="RawLine"/> is the
    /// exact source line, carried through unchanged rather than re-serialized from <see cref="DisplayValue"/>,
    /// so a value this reader decodes imperfectly still reaches the new cluster byte-for-byte for UTF-8
    /// content — the source file is read as UTF-8, so a byte sequence that is not valid UTF-8 is already
    /// replaced with U+FFFD before RawLine is ever built from it (round-1 security review, #4280 doc note —
    /// not itself a security finding). Only the GOOD/BAD verdict depends on the decode; never the log line —
    /// a carried or rejected setting's value is never repeated there (round-1 review, Medium 1).
    /// </summary>
    internal readonly record struct AutoConfSetting(string Name, string DisplayValue, string RawLine);

    /// <summary>
    /// A valid PostgreSQL GUC name: a bare identifier, or an extension-qualified one like
    /// <c>timescaledb.max_background_workers</c>. <see cref="ParseAutoConf"/> skips any line whose name does
    /// not match this, rather than carry it — <see cref="CarryAutoConfAsync"/> embeds the name inside one
    /// double-quoted <c>-C "{name}"</c> argument string, and only a hand-edited postgresql.auto.conf (ALTER
    /// SYSTEM never writes one) could put a quote or other character there that splits that argument.
    /// </summary>
    private static readonly Regex s_validGucName =
        new("^[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*$", RegexOptions.Compiled);

    /// <summary>
    /// Parses postgresql.auto.conf's own restricted grammar: one <c>name = value</c> or <c>name value</c>
    /// assignment per non-blank, non-comment line — PostgreSQL's own grammar makes the <c>=</c> optional
    /// (round-1 security review, #4280 parse note); ALTER SYSTEM always writes one, but a hand-edited file
    /// need not. The two header lines ALTER SYSTEM itself writes ("Do not edit this file manually!" / "It
    /// will be overwritten by the ALTER SYSTEM command.") are '#' comments like any other — not
    /// special-cased, just skipped by the same rule.
    ///
    /// <para>ALTER SYSTEM always single-quotes the value, whatever the GUC's type — confirmed against a live
    /// write for #4253, where a plain boolean and an integer came back quoted exactly like a string — and
    /// escapes an embedded quote by doubling it ('') and an embedded backslash by doubling it (\\), the same
    /// pair <c>guc-file.l</c> reads back. An unquoted bare token (a hand-edited line, not one ALTER SYSTEM
    /// wrote) is accepted too, matching postgresql.conf's general syntax.</para>
    ///
    /// <para>A name assigned more than once keeps only the LAST occurrence — matching how PostgreSQL itself
    /// resolves repeated assignments within one config file, and how this class already describes its own
    /// v1-v5 postgresql.conf blocks ("last-occurrence-wins override").</para>
    ///
    /// <para>A line whose name fails <see cref="s_validGucName"/> is skipped, and so is a <c>name value</c>
    /// line (the <c>=</c>-less form) with no space or tab to split on — either way, only the 1-based line
    /// number (never its text) is returned in <paramref name="skippedLines"/>, for
    /// <see cref="CarryAutoConfAsync"/> to log as not carried. Line number, not name: with the <c>=</c>
    /// optional, the text before the point this parser treats as the name/value boundary can actually be
    /// part of the VALUE, so nothing about a skipped line's own text is safe to log (round-1 security
    /// review, #4280 Medium 1).</para>
    /// </summary>
    internal static IReadOnlyList<AutoConfSetting> ParseAutoConf(string content, out IReadOnlyList<int> skippedLines)
    {
        var order = new List<string>();
        var byName = new Dictionary<string, AutoConfSetting>(StringComparer.OrdinalIgnoreCase);
        var skipped = new List<int>();

        using var reader = new StringReader(content);
        string? line;
        var lineNumber = 0;
        while ((line = reader.ReadLine()) is not null)
        {
            lineNumber++;
            /* PostgreSQL's guc-file.l tokenizer treats only ' ' and '\t' as whitespace — NOT the full
               Unicode set string.Trim() strips (e.g. U+00A0 NBSP). Matching that keeps a line PostgreSQL
               itself would not split on from being mis-split here (#4280 round-2 Low 2). */
            var trimmed = line.Trim(' ', '\t');
            if (trimmed.Length == 0 || trimmed[0] == '#')
            {
                continue;
            }

            /* The '=' is optional in PostgreSQL's own grammar (guc-file.l) — ALTER SYSTEM always writes one,
               a hand-edited "name value" line need not. With none, the name is the first whitespace run and
               everything after it is the value; with one, it is the boundary, exactly as before. Either way
               this only decides where the NAME ends — never assumed safe to log; see the method summary. */
            string name;
            string valueField;
            var eq = trimmed.IndexOf('=');
            if (eq < 0)
            {
                var ws = 0;
                while (ws < trimmed.Length && trimmed[ws] != ' ' && trimmed[ws] != '\t')
                {
                    ws++;
                }

                if (ws >= trimmed.Length)
                {
                    /* A bare token with nothing after it — no space/tab for guc-file.l to split on, so
                       there is no value to carry. Not a silent continue: the line could not be parsed,
                       so it is reported the same way an invalid name is (line number only). */
                    skipped.Add(lineNumber);
                    continue;
                }

                name = trimmed[..ws];
                valueField = trimmed[(ws + 1)..].Trim(' ', '\t');
            }
            else
            {
                name = trimmed[..eq].Trim(' ', '\t');
                valueField = trimmed[(eq + 1)..].Trim(' ', '\t');
            }

            if (!s_validGucName.IsMatch(name))
            {
                skipped.Add(lineNumber);
                continue;
            }

            var display = DecodeAutoConfValue(valueField);
            if (display is null)
            {
                continue;
            }

            if (!byName.ContainsKey(name))
            {
                order.Add(name);
            }

            byName[name] = new AutoConfSetting(name, display, line);
        }

        skippedLines = skipped;
        return order.Select(n => byName[n]).ToList();
    }

    /// <summary>
    /// Decodes one auto.conf value: single-quoted with '' and \\ escapes, or an unquoted bare token.
    /// Returns null for a value this cannot make sense of (an unterminated quote), so the caller skips that
    /// line rather than carrying a guess.
    /// </summary>
    private static string? DecodeAutoConfValue(string rawValue)
    {
        if (rawValue.Length == 0)
        {
            return null;
        }

        if (rawValue[0] != '\'')
        {
            var end = 0;
            while (end < rawValue.Length && !char.IsWhiteSpace(rawValue[end]) && rawValue[end] != '#')
            {
                end++;
            }

            return end == 0 ? null : rawValue[..end];
        }

        var builder = new StringBuilder();
        var i = 1;
        while (i < rawValue.Length)
        {
            var c = rawValue[i];
            if (c == '\'')
            {
                if (i + 1 < rawValue.Length && rawValue[i + 1] == '\'')
                {
                    builder.Append('\'');
                    i += 2;
                    continue;
                }

                /* The closing quote. Anything after it (a trailing comment) is not part of the value —
                   ALTER SYSTEM never writes one, so there is nothing meaningful to keep. */
                return builder.ToString();
            }

            if (c == '\\' && i + 1 < rawValue.Length)
            {
                var next = rawValue[i + 1];
                builder.Append(next switch
                {
                    '\\' => '\\',
                    '\'' => '\'',
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    'b' => '\b',
                    'f' => '\f',
                    _ => next,
                });
                i += 2;
                continue;
            }

            builder.Append(c);
            i++;
        }

        /* Unterminated quote — malformed; the caller drops the line rather than carrying a guess. */
        return null;
    }

    /// <summary>What <see cref="CarryAutoConfAsync"/> did, for the caller's own summary line and for tests.</summary>
    internal readonly record struct AutoConfCarryResult(
        IReadOnlyList<string> CarriedNames,
        IReadOnlyList<string> RejectedNames);

    /// <summary>The header postgresql.auto.conf always carries, even with nothing else in it — ALTER SYSTEM's
    /// own two lines. The one place this literal is written; every header-only reset uses this constant
    /// rather than restating it (#4280 round-2 part 2, item 2).</summary>
    internal const string AutoConfHeaderOnly =
        "# Do not edit this file manually!\n# It will be overwritten by the ALTER SYSTEM command.\n";

    /// <summary>
    /// The file that records where <see cref="CarryAutoConfAsync"/>'s carry is, across a crash between its own
    /// steps or between this start and the next (#4280 items 2 and 4). Line 1 is the state —
    /// <see cref="AutoConfCarryStateCarrying"/> while candidates are still being tried, or
    /// <see cref="AutoConfCarryStateTrialPassed"/> once the trial WITH the carried settings has started — then
    /// the carried setting NAMES, one per line, never a value. Deleted at every reset back to header-only, and
    /// after the first good real start.
    /// </summary>
    internal const string AutoConfCarryStateMarkerFileName = "darling-autoconf-carry.state";

    internal const string AutoConfCarryStateCarrying = "carrying";
    internal const string AutoConfCarryStateTrialPassed = "trial-passed";

    /// <summary>One state/names read of <see cref="AutoConfCarryStateMarkerFileName"/>.</summary>
    internal readonly record struct AutoConfCarryMarker(string State, IReadOnlyList<string> Names);

    /// <summary>The marker's state and names, or null when it is missing or unreadable — a no-op read (#4280
    /// items 2 and 4): a marker this cannot make sense of must never block a start.</summary>
    internal AutoConfCarryMarker? TryReadAutoConfCarryMarker(string dataDirectory)
    {
        var path = Path.Combine(dataDirectory, AutoConfCarryStateMarkerFileName);
        try
        {
            if (!File.Exists(path))
            {
                return null;
            }

            var lines = File.ReadAllLines(path);
            return lines.Length == 0 ? null : new AutoConfCarryMarker(lines[0], lines[1..]);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger.LogWarning(
                "Could not read the auto.conf carry-state marker at {Path} ({Message}) — treated as absent.",
                path, ex.Message);
            return null;
        }
    }

    /// <summary>Writes <see cref="AutoConfCarryStateMarkerFileName"/>: <paramref name="state"/> on line 1, then
    /// <paramref name="names"/> one per line, never a value.</summary>
    private static void WriteAutoConfCarryMarker(string dataDirectory, string state, IEnumerable<string> names)
        => File.WriteAllText(
            Path.Combine(dataDirectory, AutoConfCarryStateMarkerFileName),
            state + "\n" + string.Join("\n", names));

    /// <summary>Deletes <see cref="AutoConfCarryStateMarkerFileName"/> if present. Every exit that no longer
    /// needs it — a good real start, the already-running path, or as part of
    /// <see cref="ResetAutoConfCarryAsync"/> — calls this rather than deleting the file a new way.</summary>
    internal static void TryDeleteAutoConfCarryMarker(string dataDirectory)
        => TryDeleteFile(Path.Combine(dataDirectory, AutoConfCarryStateMarkerFileName));

    /// <summary>Resets postgresql.auto.conf to header-only, logs <paramref name="marker"/>'s names as dropped
    /// (never their values — only names ever reach this log), and deletes the marker. Shared by
    /// <see cref="DarlingManagedPostgres"/>'s item 2 real-start fallback and item 4 leftover-"carrying"
    /// recovery, so neither restates the header literal or the drop-and-delete sequence a new way. Returns
    /// false only when the header-only write itself failed: then neither the "NOT carried" warning nor the
    /// delete runs, the marker is left in place so the next start retries the reset, and the caller must not
    /// claim the carry was dropped or retry a start against the same unwritable file. True otherwise.</summary>
    internal async Task<bool> ResetAutoConfCarryAsync(string dataDirectory, AutoConfCarryMarker marker, string reason)
    {
        var autoConfPath = Path.Combine(dataDirectory, "postgresql.auto.conf");
        try
        {
            await File.WriteAllTextAsync(autoConfPath, AutoConfHeaderOnly, CancellationToken.None);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger.LogWarning(
                "Could not reset {Path} to header-only ({Message}); the carry-state marker is kept so the next start tries again.",
                autoConfPath, ex.Message);
            return false;
        }

        if (marker.Names.Count > 0)
        {
            _logger.LogWarning(
                "NOT carried: {Names} — {Reason}. postgresql.auto.conf reset to header-only.",
                string.Join(", ", marker.Names), reason);
        }

        TryDeleteAutoConfCarryMarker(dataDirectory);
        return true;
    }

    /// <summary>
    /// Carries the pre-upgrade postgresql.auto.conf into the new cluster (#4253). pg_upgrade does not copy
    /// this file, and initdb starts the new cluster with none, so every ALTER SYSTEM setting an operator
    /// made is silently lost on a major upgrade unless something restores it — Tier 1 evidence: a store's
    /// operator-raised shared_buffers reverted to the service's default with no word said.
    ///
    /// <para>Call this AFTER pg_upgrade and the data-directory swap have committed, and BEFORE anything gives
    /// the new cluster its first real start. That "anything" is not only <see cref="StartClusterAsync"/>: the
    /// quiesced TimescaleDB update (#3908) that commonly runs right after an upgrade also starts the live new
    /// data directory, on its way to moving the extension to the runtime's version, so the carry has to be
    /// done and validated before that start too, not just before the operator-visible one.</para>
    ///
    /// <para>Each candidate setting is checked against the NEW binaries before it is kept: <c>postgres -C
    /// &lt;name&gt; -D &lt;newDataDirectory&gt;</c> parses the whole config the way a real start would,
    /// including whatever this call has just written to postgresql.auto.conf for the probe, and exits
    /// non-zero on an unrecognized name or an out-of-range value — exactly the two ways a setting good for
    /// the OLD major can be bad for the NEW one (both confirmed live for #4253). A setting that fails this is
    /// left out and logged by name.</para>
    ///
    /// <para><c>-C</c> exits right after it reads the config files though — before the checks between
    /// settings, <c>shared_preload_libraries</c>, and SSL/certificate setup a real start also runs (round-1
    /// security review, #4280 Medium 2: a carried <c>ssl = on</c> with no certificate files passes <c>-C
    /// ssl</c> fine and then stops the very next start). So once every candidate has passed its own probe,
    /// this also does a real start and stop of the NEW cluster on loopback (<see cref="StartClusterAsync"/>
    /// with <see cref="QuiescedUpdateServerOptions"/>, then <see cref="StopClusterAsync"/>) before returning.
    /// If THAT fails, every carried name is dropped, logged as such, and postgresql.auto.conf is left at the
    /// header only. No setting this method carries ever gets to hold the store down (the fix this issue
    /// exists for must not trade one silent loss for a new way to brick the store).</para>
    ///
    /// <para>The untouched original is always kept at <see cref="PreUpgradeAutoConfFileName"/> beside the new
    /// data directory (its parent, matching where this class already keeps the pg-upgrade password file and
    /// the retained pre-upgrade data directory) whenever there was a source file to read — whether or not
    /// anything in it was rejected. It is the operator's own record, not just this method's summary of it.</para>
    ///
    /// <para>If a probe THROWS instead of returning a bad exit code — <c>TimeoutException</c> past
    /// <see cref="s_confProbeTimeout"/>, or a rethrown <c>OperationCanceledException</c> on a service stop —
    /// the loop cannot finish, and postgresql.auto.conf may hold one candidate line nothing has verified. The
    /// post-commit handler wrapped around this call finishes the upgrade regardless of that exception (its
    /// own comment: "keeps the store running on the new major regardless"), so whatever this call leaves on
    /// disk IS what the next start reads. So on ANY exception the file is put back to the same
    /// empty-but-safe header the combined-check failure path below already uses, before the exception is
    /// rethrown: the caller must still see the failure, but never by way of a store that will not start.</para>
    /// </summary>
    internal Task<AutoConfCarryResult> CarryAutoConfAsync(
        string oldDataDirectory,
        string newDataDirectory,
        string newBinDirectory,
        CancellationToken cancellationToken,
        Func<string>? sslServerOptions = null)
        => CarryAutoConfAsync(
            oldDataDirectory, newDataDirectory, newBinDirectory,
            (exePath, arguments, timeout, token) => DarlingManagedPostgres.RunToolAsync(exePath, arguments, timeout, token),
            cancellationToken,
            sslServerOptions);

    /// <summary>
    /// <see cref="CarryAutoConfAsync(string, string, string, CancellationToken, Func{string})"/> with the
    /// per-setting <c>postgres -C</c> probe substitutable, so a test can make one throw without a real
    /// postgres.exe or a real 30-second wait. Production always uses the four-argument overload above, which
    /// wires <see cref="DarlingManagedPostgres.RunToolAsync"/> unchanged. The belt-and-braces real start near
    /// the end (round-2 review, #4280 Medium 1) uses its own private loopback port from
    /// <see cref="FindFreeLoopbackPort"/>, never the store's eventual configured port, which this call cannot
    /// assume is free. <paramref name="sslServerOptions"/> rides the SAME trial start (#4280 round-2 part 2,
    /// item 2): null (every existing caller) means no SSL options, same as before this parameter existed.
    /// </summary>
    internal async Task<AutoConfCarryResult> CarryAutoConfAsync(
        string oldDataDirectory,
        string newDataDirectory,
        string newBinDirectory,
        Func<string, string, TimeSpan, CancellationToken, Task<(int ExitCode, string Output)>> probe,
        CancellationToken cancellationToken,
        Func<string>? sslServerOptions = null)
    {
        var none = new AutoConfCarryResult(Array.Empty<string>(), Array.Empty<string>());

        var sourcePath = Path.Combine(oldDataDirectory, "postgresql.auto.conf");
        if (!File.Exists(sourcePath))
        {
            _logger.LogInformation(
                "No postgresql.auto.conf in the pre-upgrade data directory — nothing to carry into the upgraded store.");
            return none;
        }

        var content = await File.ReadAllTextAsync(sourcePath, cancellationToken);
        var settings = ParseAutoConf(content, out var skippedLines);

        var parent = Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(newDataDirectory)))!;
        var preUpgradeCopy = Path.Combine(parent, PreUpgradeAutoConfFileName);
        File.Copy(sourcePath, preUpgradeCopy, overwrite: true);
        try
        {
            DarlingFileSecurity.HardenFile(preUpgradeCopy, allowInteractiveRead: false);
        }
        catch (Exception ex)
        {
            /* Best-effort, like DarlingManagedPostgres.TryHardenCredentialFile this mirrors — a failure here
               must not cost the carry itself, only get logged so it can be fixed by hand. */
            _logger.LogWarning(
                "Could not restrict {Path} to the store's own ACL ({Message}) — it may be readable more " +
                "broadly than the data directory it was copied from.",
                preUpgradeCopy, ex.Message);
        }

        _logger.LogInformation(
            "Pre-upgrade postgresql.auto.conf saved to {Path} — kept until the NEXT major upgrade replaces " +
            "it, not deleted with the rest of the pre-upgrade data directory.",
            preUpgradeCopy);

        if (skippedLines.Count > 0)
        {
            _logger.LogWarning(
                "NOT carried: {Count} line(s) of the pre-upgrade postgresql.auto.conf skipped (line(s) " +
                "{Lines}) — not a valid PostgreSQL setting name, so skipped rather than risk one splitting " +
                "the \"-C\" probe's argument or logging part of a value. The original file is kept at {Path}.",
                skippedLines.Count, string.Join(", ", skippedLines), preUpgradeCopy);
        }

        if (settings.Count == 0)
        {
            _logger.LogInformation(
                "The pre-upgrade postgresql.auto.conf has no ALTER SYSTEM settings to carry (kept at {Path} for reference).",
                preUpgradeCopy);
            return none;
        }

        var postgresExe = Path.Combine(newBinDirectory, "postgres.exe");
        var newAutoConfPath = Path.Combine(newDataDirectory, "postgresql.auto.conf");

        var carried = new List<string>();
        var rejected = new List<string>();
        var goodLines = new List<string>();

        try
        {
            /* "carrying" before the first candidate write (#4280 item 2/4): the ONLY names known at this
               point are every candidate about to be tried, so that is what a crash mid-loop reports as
               dropped — conservative, matching the combined-check failure path below, which also drops
               everything together rather than guess which ones would have passed. */
            WriteAutoConfCarryMarker(newDataDirectory, AutoConfCarryStateCarrying, settings.Select(s => s.Name));

            foreach (var setting in settings)
            {
                /* One setting at a time: postgres -C fails the WHOLE file on any one bad line (confirmed live —
                   it cannot tell the caller which line without being handed just that line), so isolating each
                   candidate is the only way to identify which ones are bad rather than losing all of them to one. */
                await File.WriteAllTextAsync(newAutoConfPath, AutoConfHeaderOnly + setting.RawLine + "\n", cancellationToken);

                var (exitCode, output) = await probe(
                    postgresExe,
                    $"-C \"{setting.Name}\" -D \"{newDataDirectory}\"",
                    s_confProbeTimeout,
                    cancellationToken);

                if (exitCode == 0)
                {
                    carried.Add(setting.Name);
                    goodLines.Add(setting.RawLine);
                    /* Name only — the value stays out of every log (round-1 security review, #4280 Medium 1).
                       A reader who needs the value has it at preUpgradeCopy, which only SYSTEM, Administrators
                       and the service account can read; this log can reach more accounts than that. */
                    _logger.LogInformation(
                        "Carried {Name} from the pre-upgrade postgresql.auto.conf. Value kept at {Path}.",
                        setting.Name, preUpgradeCopy);
                }
                else
                {
                    rejected.Add(setting.Name);
                    if (NameMayHoldASecret(setting.Name) || setting.Name.Contains('.'))
                    {
                        /* PostgreSQL's own reject reason often repeats the offending value verbatim, so a
                           setting name that looks like it carries a credential gets no reason logged at all —
                           and neither does any extension-qualified name (anything with a dot): we cannot
                           enumerate every extension's own reject-reason wording well enough to know none of
                           them ever echoes a value either (#4280 round-2 Q3 hardening). */
                        _logger.LogWarning(
                            "NOT carried: {Name} — the new PostgreSQL binaries reject it (reason withheld: " +
                            "the name suggests it may hold a credential, or names an extension setting whose " +
                            "reason could). The original setting is kept at {Path}.",
                            setting.Name, preUpgradeCopy);
                    }
                    else
                    {
                        _logger.LogWarning(
                            "NOT carried: {Name} — the new PostgreSQL binaries reject it: {Reason}. " +
                            "The original setting is kept at {Path}.",
                            setting.Name, output, preUpgradeCopy);
                    }
                }
            }

            var finalContent = AutoConfHeaderOnly + string.Join(string.Empty, goodLines.Select(l => l + "\n"));
            await File.WriteAllTextAsync(newAutoConfPath, finalContent, cancellationToken);

            if (goodLines.Count == 0)
            {
                /* Nothing passed its own probe, so there is nothing pending to protect — the "carrying"
                   marker above is now stale rather than in-progress. Clear it so a later crash-recovery read
                   (#4280 item 4) never reports these same rejected names a second time as though the carry
                   itself had been interrupted. */
                TryDeleteAutoConfCarryMarker(newDataDirectory);
            }

            if (goodLines.Count > 0)
            {
                /* Belt-and-braces over the per-setting probes above, but a REAL start and stop, not another
                   -C probe: -C exits right after it reads the config files, before the checks between
                   settings, shared_preload_libraries, and SSL/certificate setup a real start also runs
                   (round-1 security review, #4280 Medium 2 — ssl = on with no certificate files is the
                   concrete case: it passes -C ssl fine and then stops the very next start). A private port,
                   never the store's configured one (round-2 review, #4280 Medium 1): the configured port may
                   still be held by whatever this upgrade is replacing, and a trial proving only the SETTINGS
                   must not fail over a port collision that says nothing about them. */
                if (!await TryStartTrialAsync())
                {
                    /* Header-only FIRST (item 3, round-2 review Medium 2) — the same empty file every other
                       failure path here resets to — so the retry just below starts on nothing the carried
                       settings touched, and proves whether THEY were the cause or something else was.
                       CancellationToken.None: this write is what keeps the store bootable, and a cancellation
                       landing right here must not be able to skip it. Marker deleted here too (#4280 item 2):
                       from this point on nothing this call still holds is unverified. */
                    await File.WriteAllTextAsync(newAutoConfPath, AutoConfHeaderOnly, CancellationToken.None);
                    TryDeleteAutoConfCarryMarker(newDataDirectory);

                    var pgLogPath = Path.Combine(
                        Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(newDataDirectory)))!,
                        DarlingManagedPostgres.ServerLogFileName);

                    if (await TryStartTrialAsync())
                    {
                        /* The empty-file retry started: the settings really were the cause. Never either
                           attempt's own exception message: it can embed the server log tail, and PostgreSQL's
                           own startup error can echo a failing setting's value — logging it here would reopen
                           Medium 1 through this new path (TryStartTrialAsync already swallows it for exactly
                           this reason). Names, and pg.log for whoever wants the detail, only. */
                        _logger.LogWarning(
                            "The carried settings did not let the new cluster start, even though each passed " +
                            "alone — leaving postgresql.auto.conf EMPTY rather than risk the store not starting. " +
                            "Dropped: {Names}. See {Log} for the failing start. The originals are kept at {Path}.",
                            string.Join(", ", carried), pgLogPath, preUpgradeCopy);

                        return new AutoConfCarryResult(Array.Empty<string>(), settings.Select(s => s.Name).ToList());
                    }

                    /* The empty-file retry ALSO failed — nothing about the carried settings explains that, so
                       this is not a reason to drop or blame them; something else about this cluster will not
                       start (round-2 review, #4280 Medium 2, Q3). The file is already header-only. A NEW,
                       fixed message, never either attempt's own exception (same reasoning as above): thrown so
                       the post-commit handler around this call reports a warning on the upgrade's outcome
                       instead of a silent success. This throw passes through the catch below on its way out,
                       which re-runs the same (idempotent) header-only reset and logs its own line again —
                       accepted rather than special-cased around. */
                    throw new InvalidOperationException(
                        $"The new PostgreSQL cluster at {newDataDirectory} would not start even with an empty " +
                        $"postgresql.auto.conf, so this is unrelated to the carried settings — see {pgLogPath} " +
                        "for the failing start.");
                }

                /* Reached only when the FIRST trial (the one WITH the carried settings) started — #4280 item 2:
                   the real start further down this call chain may still fail for a reason the trial's own
                   scope did not cover, and its fallback needs to know these exact names survived an isolated
                   verification, not just that a carry was attempted. */
                WriteAutoConfCarryMarker(newDataDirectory, AutoConfCarryStateTrialPassed, carried);
            }

            /* One private-port start, confirmed-stopped, using the SAME marker/orphan lifecycle
               UpdateTimescaleQuiescedAsync uses (:1769, #3908) — so a trial this call cannot stop is picked up
               and stopped by the next start's StopQuiescedUpdateOrphanAsync call, the same as an interrupted
               TimescaleDB update, rather than left running under a data directory nothing else expects a live
               server on. The marker is written BEFORE the start, so a crash between them still leaves a
               record. A failed stop after a SUCCESSFUL start is never rethrown and never reaches the outer
               catch below: that catch drops the settings this trial just verified, and the leftover marker
               is what carries the failure instead (round-2 review, #4280 Medium 1). A local function, not a
               private method, so a caller that needs to try it more than once still gets one attempt's state
               (trialPort, marker) fully scoped to that attempt. */
            async Task<bool> TryStartTrialAsync()
            {
                var trialPort = FindFreeLoopbackPort();
                var marker = Path.Combine(newDataDirectory, QuiescedUpdateMarkerFileName);
                var started = false;
                try
                {
                    File.WriteAllText(marker, trialPort.ToString(CultureInfo.InvariantCulture));
                    /* QuiescedStartWaitSeconds (900s), not the default 120s (item 3, round-2 review Medium 2):
                       this can be the retry below, on a cluster whose first start already used up part of any
                       generous wait, and a short timeout here would misreport an unrelated slow start as a
                       settings failure. */
                    /* sslServerOptions rides EVERY attempt this local function makes, the header-only retry
                       included (#4280 round-2 part 2, item 2) — the real start further down the call chain
                       always carries it too, so a trial that omitted it could pass on settings the real start
                       would not. Null (every caller before item 2) contributes nothing, same as before. */
                    await StartClusterAsync(
                        newBinDirectory, newDataDirectory, trialPort, cancellationToken,
                        QuiescedUpdateServerOptions + (sslServerOptions?.Invoke() ?? string.Empty), QuiescedStartWaitSeconds);
                    started = true;
                }
                catch (Exception) when (!cancellationToken.IsCancellationRequested)
                {
                    /* A cancellation (service stop mid-trial) is not a failed start — swallowing it here
                       would fall through to the header-only retry and then the fixed "would not start even
                       with an empty postgresql.auto.conf" message, both wrong for a stop that has nothing to
                       do with the carried settings (#4280 round-2 part 2, item 0b). Letting it propagate
                       still runs this finally, which stops the trial server the same as any other exit. */
                    started = false;
                }
                finally
                {
                    if (await StopClusterConfirmedAsync(newBinDirectory, newDataDirectory))
                    {
                        TryDeleteFile(marker);
                    }
                    else
                    {
                        _logger.LogCritical(
                            "The server started on private port {Port} to verify carried postgresql.auto.conf settings would not stop. {Marker} is kept, so the next start recognizes it and stops it rather than adopting it as the store.",
                            trialPort, marker);
                    }
                }

                return started;
            }
        }
        catch (Exception original)
        {
            /* A probe threw instead of returning a bad exit code — TimeoutException past s_confProbeTimeout, or
               OperationCanceledException on a service stop mid-loop — or one of the writes above faulted, or
               the verification start/stop above did. Whatever the cause, postgresql.auto.conf may hold one
               unverified candidate line right now, and the post-commit handler around this call finishes the
               upgrade regardless (see this method's own summary above), so whatever is on disk when this
               throws IS what the next start reads. Put it back to the same empty-but-safe header the
               combined-check failure path above uses. CancellationToken.None, not cancellationToken: the
               caller's own token may already be the reason this threw, and a cancelled token must not be able
               to block the one write that keeps the store bootable. */
            try
            {
                await File.WriteAllTextAsync(newAutoConfPath, AutoConfHeaderOnly, CancellationToken.None);
                _logger.LogWarning(
                    "postgresql.auto.conf carry did not finish — reset to empty rather than leave an unverified " +
                    "setting in place for the next start. The pre-upgrade original is kept at {Path}.",
                    preUpgradeCopy);
            }
            catch (Exception resetEx)
            {
                /* The reset write itself failed. PostgreSQL treats postgresql.auto.conf as an optional file,
                   so deleting it is just as safe as emptying it — try that before giving up. */
                try
                {
                    File.Delete(newAutoConfPath);
                    _logger.LogWarning(
                        "postgresql.auto.conf carry did not finish, and resetting {Path} to empty also failed " +
                        "({ResetReason}) — deleted it instead; PostgreSQL treats a missing postgresql.auto.conf " +
                        "as empty. The pre-upgrade original is kept at {OriginalPath}.",
                        newAutoConfPath, resetEx.Message, preUpgradeCopy);
                }
                catch (Exception deleteEx)
                {
                    /* original.Message joins the other two reasons (round-2 review, Low 1) so this exception's
                       own text is self-contained — an operator, or a log that only shows the top-level
                       message, still sees WHY the carry itself did not finish, not just why the two cleanup
                       attempts after it also failed. Safe here specifically: every exception that can reach
                       this catch (a probe timeout/cancellation, a file I/O fault, item 3's own fixed-message
                       throw — StopClusterConfirmedAsync never throws) is already one this class never lets
                       carry a setting's value or the server log tail, the same guarantee TryStartTrialAsync's
                       own swallow relies on above. */
                    throw new InvalidOperationException(
                        $"postgresql.auto.conf carry did not finish ({original.Message}), and neither resetting " +
                        $"{newAutoConfPath} to empty ({resetEx.Message}) nor deleting it ({deleteEx.Message}) " +
                        "succeeded. It may hold an unverified setting — delete or empty this file by hand before " +
                        "the next start.",
                        original);
                }
            }

            /* Unconditional (#4280 item 2): whichever of the two paths above ran, this call's own carry did
               not finish, so any marker it left behind — "carrying" from the loop above, or "trial-passed"
               from a trial that started before something else in this try block threw — is stale either way. */
            TryDeleteAutoConfCarryMarker(newDataDirectory);

            throw;
        }

        _logger.LogInformation(
            "postgresql.auto.conf carried: {Carried} of {Total} pre-upgrade setting(s) kept, {Rejected} rejected. Originals kept at {Path}.",
            carried.Count, settings.Count, rejected.Count, preUpgradeCopy);

        return new AutoConfCarryResult(carried, rejected);
    }

    /// <summary>
    /// Whether a GUC name might hold a credential or other secret in its value — <c>primary_conninfo</c> can
    /// carry a password, <c>archive_command</c>/<c>restore_command</c> often carry a storage token, and
    /// <c>ssl_passphrase_command</c> can carry a passphrase (round-1 security review, #4280 Medium 1).
    /// PostgreSQL's own reject reason for an out-of-range value often repeats the value verbatim, so a name
    /// that matches here gets no reason logged at all — matched loosely and case-insensitively against the
    /// whole name, extension-qualified names included, erring toward withholding more than strictly needed.
    /// </summary>
    private static bool NameMayHoldASecret(string name)
        => s_secretNameFragments.Any(fragment => name.Contains(fragment, StringComparison.OrdinalIgnoreCase));

    private static readonly string[] s_secretNameFragments =
        { "conninfo", "command", "password", "passphrase", "secret", "key", "token", "credential", "auth" };

    /* ==================== operator postgresql.conf below-include-line carry (#4358) ==================== */

    /// <summary>What <see cref="CarryOperatorConfLinesAsync"/> did — counts only, never a line's own text
    /// (same discipline as <see cref="AutoConfCarryResult"/>): a comment, or blank line among the
    /// below-include region always counts as carried since it is never probed.</summary>
    internal readonly record struct OperatorConfLineCarryResult(int CarriedCount, int RejectedCount);

    /// <summary>The GUC-directive names <see cref="ManagedConfMigration.ExtractOperatorLinesBelowInclude"/>
    /// can return that are never probed with <c>postgres -C</c>: an include directive names a FILE, not a
    /// runtime parameter, so <c>-C</c> has no setting name to ask it about. Carried unconditionally, the same
    /// way a comment or blank line among the extracted lines is — a bad include target fails the new
    /// cluster's real start exactly as a hand-edited <c>postgresql.conf</c> already can today, and this carry
    /// must never be the reason that risk is new.</summary>
    private static readonly string[] s_confIncludeDirectiveNames = { "include", "include_if_exists", "include_dir" };

    internal Task<OperatorConfLineCarryResult> CarryOperatorConfLinesAsync(
        string oldDataDirectory,
        string newDataDirectory,
        string newBinDirectory,
        CancellationToken cancellationToken)
        => CarryOperatorConfLinesAsync(
            oldDataDirectory, newDataDirectory, newBinDirectory,
            (exePath, arguments, timeout, token) => DarlingManagedPostgres.RunToolAsync(exePath, arguments, timeout, token),
            cancellationToken);

    /// <summary>
    /// Carries the OLD cluster's <c>postgresql.conf</c> lines that sat below the <c>darling-managed.conf</c>
    /// include into the NEW cluster's <c>postgresql.conf</c>, appended AFTER the legacy blocks
    /// <c>context.AppendManagedConf</c> already wrote there (#4358) — so an operator's line still wins over
    /// the legacy block's own copy of the same key, matching <see cref="ManagedConfMigration.Rewrite"/>'s
    /// rule 3 for the same-major case. Runs alongside <see cref="CarryAutoConfAsync"/>, post-swap, one
    /// pattern: reads the OLD (now-<c>retained</c>) data directory's <c>postgresql.conf</c>,
    /// <see cref="ManagedConfMigration.ExtractOperatorLinesBelowInclude"/> finds the candidates, and each
    /// ASSIGNMENT line is probed one at a time against the NEW binaries with <c>postgres -C</c>, the same
    /// isolation <see cref="CarryAutoConfAsync"/> gives auto.conf settings — a line the new major rejects is
    /// logged as not carried and skipped, never fatal to the upgrade. A comment, blank line, or operator
    /// <c>include</c>/<c>include_if_exists</c>/<c>include_dir</c> directive among the candidates is carried
    /// unconditionally (see <see cref="s_confIncludeDirectiveNames"/>). Every carried line is marked with
    /// <see cref="ManagedConfMigration.MovedOperatorLinesComment"/> (reused, per the ruling — provenance does
    /// not need to distinguish a same-major move from a cross-major carry). Never throws: same
    /// "must never brick a completed upgrade" posture the auto.conf carry follows for a probe failure — an
    /// unexpected exception here resets the new cluster's <c>postgresql.conf</c> back to its pre-carry
    /// (legacy-appended) baseline and logs a warning naming the retained old data directory as the manual
    /// fallback, rather than propagate.
    /// </summary>
    internal async Task<OperatorConfLineCarryResult> CarryOperatorConfLinesAsync(
        string oldDataDirectory,
        string newDataDirectory,
        string newBinDirectory,
        Func<string, string, TimeSpan, CancellationToken, Task<(int ExitCode, string Output)>> probe,
        CancellationToken cancellationToken)
    {
        var none = new OperatorConfLineCarryResult(0, 0);

        var sourcePath = Path.Combine(oldDataDirectory, "postgresql.conf");
        if (!File.Exists(sourcePath))
        {
            _logger.LogInformation(
                "No postgresql.conf in the pre-upgrade data directory — nothing to carry below the include into the upgraded store.");
            return none;
        }

        var oldConfText = await File.ReadAllTextAsync(sourcePath, cancellationToken);
        var candidateLines = ManagedConfMigration.ExtractOperatorLinesBelowInclude(oldConfText);
        if (candidateLines.Count == 0)
        {
            _logger.LogInformation(
                "The pre-upgrade postgresql.conf has no operator lines below the darling-managed.conf include to carry.");
            return none;
        }

        var newConfPath = Path.Combine(newDataDirectory, "postgresql.conf");
        var baseline = await File.ReadAllTextAsync(newConfPath, cancellationToken);
        if (baseline.Length > 0 && !baseline.EndsWith('\n'))
        {
            baseline += "\n";
        }

        var postgresExe = Path.Combine(newBinDirectory, "postgres.exe");
        var goodLines = new List<string>();
        var carried = 0;
        var rejected = 0;

        try
        {
            foreach (var rawLine in candidateLines)
            {
                var (_, name, _) = DarlingManagedPostgres.ParseConfText(rawLine).FirstOrDefault();
                var isProbeableSetting = name is not null &&
                    !s_confIncludeDirectiveNames.Contains(name, StringComparer.OrdinalIgnoreCase);

                if (!isProbeableSetting)
                {
                    /* A comment, blank line, or an operator include directive — never probed, always carried
                       (see s_confIncludeDirectiveNames and the method summary above). */
                    goodLines.Add(rawLine);
                    carried++;
                    continue;
                }

                /* One candidate at a time, against the LEGACY-APPENDED baseline only (never a previously
                   accepted candidate) — the same isolation CarryAutoConfAsync gives each auto.conf setting,
                   so one bad line cannot be blamed on, or hide behind, another. */
                await File.WriteAllTextAsync(newConfPath, baseline + rawLine + "\n", cancellationToken);

                var (exitCode, output) = await probe(
                    postgresExe,
                    $"-C \"{name}\" -D \"{newDataDirectory}\"",
                    s_confProbeTimeout,
                    cancellationToken);

                if (exitCode == 0)
                {
                    goodLines.Add(rawLine);
                    carried++;
                    _logger.LogInformation(
                        "Carried {Name} from the pre-upgrade postgresql.conf's below-include region.", name);
                }
                else
                {
                    rejected++;
                    if (NameMayHoldASecret(name) || name.Contains('.'))
                    {
                        /* Same withholding rule CarryAutoConfAsync applies to auto.conf settings (#4280 round-2
                           Q3): PostgreSQL's own reject reason can repeat the offending value verbatim. */
                        _logger.LogWarning(
                            "NOT carried: {Name} — the new PostgreSQL binaries reject it (reason withheld: the " +
                            "name suggests it may hold a credential, or names an extension setting whose reason " +
                            "could). The original line is kept in the retained pre-upgrade data directory.",
                            name);
                    }
                    else
                    {
                        _logger.LogWarning(
                            "NOT carried: {Name} — the new PostgreSQL binaries reject it: {Reason}. The original " +
                            "line is kept in the retained pre-upgrade data directory.",
                            name, output);
                    }
                }
            }

            var finalContent = baseline;
            if (goodLines.Count > 0)
            {
                finalContent += ManagedConfMigration.MovedOperatorLinesComment + "\n" +
                    string.Join(string.Empty, goodLines.Select(l => l + "\n"));
            }

            await File.WriteAllTextAsync(newConfPath, finalContent, cancellationToken);
        }
        catch (Exception ex)
        {
            /* Never brick a completed upgrade over this carry (same posture as CarryAutoConfAsync's own
               catch) — put postgresql.conf back to its pre-carry (legacy-appended) baseline and let the
               caller's post-commit handler finish the upgrade regardless. */
            try
            {
                await File.WriteAllTextAsync(newConfPath, baseline, CancellationToken.None);
            }
            catch (Exception resetEx)
            {
                _logger.LogWarning(
                    "Carrying operator postgresql.conf lines below the include did not finish ({Reason}), and " +
                    "resetting postgresql.conf to its pre-carry content also failed ({ResetReason}) — check " +
                    "{Path} by hand against the retained pre-upgrade data directory.",
                    ex.Message, resetEx.Message, newConfPath);
                return none;
            }

            _logger.LogWarning(
                "Carrying operator postgresql.conf lines below the include did not finish ({Reason}) — " +
                "postgresql.conf was reset to its pre-carry content. The originals are kept in the retained " +
                "pre-upgrade data directory.",
                ex.Message);
            return none;
        }

        _logger.LogInformation(
            "postgresql.conf below-include lines carried: {Carried} of {Total} pre-upgrade line(s) kept, {Rejected} rejected.",
            carried, candidateLines.Count, rejected);

        return new OperatorConfLineCarryResult(carried, rejected);
    }

    /// <summary>
    /// The in-place major upgrade, start to finish. Each step is labelled, and ANY failure before the commit
    /// point lands in one place, <see cref="RecoverFromPreCommitFailureAsync"/>: drop the half-built new
    /// cluster, put the old data directory back as the old runtime needs it (in hard-link mode that includes
    /// undoing pg_upgrade's rename of <c>global\pg_control</c>), revert the runtime, and return a Failed
    /// outcome so the caller keeps running the store on its existing major. The one thing this must never do
    /// is leave the store down, and where some part of putting it back cannot be done, the outcome and the
    /// log say so and name the manual step instead of claiming a running store (#3927).
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
        string? restoredTimescale = null;
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
            /* Quiesced, like the TimescaleDB update (#3908): nothing here needs the scheduler or autovacuum, and a
               bridge ALTER must not race a job that loaded the old library. */
            await StartClusterAsync(context.OldBinDirectory, context.DataDirectory, context.Port, cancellationToken, QuiescedUpdateServerOptions);
            oldStarted = true;

            step = "read-cluster-identity";
            var ownerConnection = DarlingManagedPostgres.BuildConnectionString(context.Port, context.Password);
            var identity = await ReadClusterIdentityAsync(ownerConnection, cancellationToken);
            _logger.LogInformation(
                "Old cluster identity: encoding {Encoding}, collate {Collate}, ctype {Ctype}, locale provider {Provider}, data checksums {Checksums} — the new cluster is initialized to match.",
                identity.Encoding, identity.Collate, identity.Ctype, identity.LocaleProvider ?? "(none)", identity.DataChecksums ? "on" : "off");

            /* ---- 3. the TimescaleDB bridge: pg_upgrade recreates the extension pinned at the version the
                    OLD cluster has, so the NEW runtime must carry that version's libraries. Mismatch here is a
                    guaranteed upgrade failure later, so it is a hard gate, not best-effort. ---- */
            step = "timescaledb-bridge";
            (fromTimescale, restoredTimescale) = await BridgeTimescaleAsync(
                context.Port,
                context.Password,
                TryReadTimescaleLibraryVersions(Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(context.NewBinDirectory))!),
                TryReadInstalledTimescaleVersion(context.OldBinDirectory),
                cancellationToken);

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

            /* ---- 8. carry the pre-upgrade postgresql.auto.conf (#4253) — BEFORE anything gives the new
                    cluster its first real start, the quiesced TimescaleDB update just below included. Read
                    from `retained`: the old data directory's content now lives there, since the swap above
                    already moved it. Any failure here is caught by the post-commit handler below, which
                    keeps the store running on the new major regardless — never a reason to brick it. */
            step = "carry-auto-conf";
            await CarryAutoConfAsync(
                retained, context.DataDirectory, context.NewBinDirectory, cancellationToken, context.SslServerOptions);

            /* #4358: alongside the auto.conf carry, same post-swap timing — one pattern. Reads the OLD
               cluster's postgresql.conf from `retained` (its content now lives there, since the swap above
               already moved it), extracts any operator lines below the darling-managed.conf include, and
               appends them to the NEW cluster's postgresql.conf AFTER the legacy blocks
               context.AppendManagedConf already wrote there (step "conf-new-cluster", above) — so an
               operator's override still wins over the legacy block's own copy of the same key. Any failure
               here is caught by the post-commit handler below, which keeps the store running on the new
               major regardless — never a reason to brick it. */
            step = "carry-operator-conf-lines";
            await CarryOperatorConfLinesAsync(retained, context.DataDirectory, context.NewBinDirectory, cancellationToken);

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
                context.OldMajor, context.NewMajor, fromTimescale ?? "(none)", restoredTimescale ?? "(none)");

            /* ToTimescale is the version pg_upgrade restored. The quiesced update moves it to the runtime's own
               version next, and the bootstrap reports where it ended up (#3908). */
            return new StoreUpgradeOutcome(
                StoreUpgradeStatus.Succeeded, context.OldMajor, context.NewMajor,
                fromTimescale, restoredTimescale, null, postCommitWarning, mode == FileTransferMode.Link);
        }
        catch (OperationCanceledException)
        {
            /* Service shutdown mid-upgrade. Before the commit point the old data directory is put back as it
               was and the revert restores the previous runtime, so the next start tries again. "Put back"
               used to be "untouched", which hard-link mode makes false once pg_upgrade begins linking and
               renames global\pg_control (#3927); the same put-back runs here as on a failure, and before the
               revert. AFTER the commit point the same revert would brick the store, so shutdown must leave the
               new runtime in place and let the next start pick up an already-upgraded cluster — cancellation
               is not a licence to undo a completed upgrade any more than an exception is. */
            await TryStopAsync(context, oldStarted);

            if (!swapped)
            {
                TryDeleteDirectory(newDataDirectory);
                var preUpgradeData = PutBackPreUpgradeDataDirectory(context, mode);
                var reverted = RevertRuntimeForCancel(context);
                ReportRecoveryBeforeCommit(context, preUpgradeData, reverted);
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
            /* PRE-COMMIT failure: the swap has not committed, so the store goes back to its old cluster on its
               old runtime. What that takes, and what may honestly be claimed once it is done, depends on how
               far the upgrade got. "The pre-upgrade data directory was never modified" used to be logged here
               before anything was put back; it is true in copy mode, and in hard-link mode only until
               pg_upgrade begins linking (#3927). The recovery is its own method so a test can inject exactly
               this failure. */
            return await RecoverFromPreCommitFailureAsync(
                context, mode, oldStarted, newDataDirectory, step, ex.Message, fromTimescale);
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
            /* #3927: this used to end "the next start adopts it", as though that were the plan. It is the
               problem: a start that adopts this server is running the old cluster on binaries the revert is
               about to move, so RevertRuntime refuses while it is up and says what to stop. */
            _logger.LogWarning(
                "Could not stop the old cluster after a failed upgrade ({Message}). If it is still running on {DataDirectory} when the runtime is reverted, the revert is refused rather than moving binaries out from under it.",
                ex.Message, context.DataDirectory);
        }
    }

    private bool RevertRuntimeForCancel(UpgradeContext context)
    {
        /* A cancelled upgrade is not a BAD package, so revert the binaries without recording the block —
           the next start should try again. */
        var blockedPath = Path.Combine(context.RuntimeRoot, RuntimeBlockedFileName);
        var reverted = RevertRuntime(context.RuntimeRoot, context.ZipHash, context.DataDirectory, context.OldMajor);
        TryDeleteFile(blockedPath);
        return reverted;
    }

    /// <summary>What a failure before the commit point left of the pre-upgrade data directory (#3927).</summary>
    internal enum PreUpgradeDataDirectory
    {
        /// <summary>Its contents needed nothing undone: copy mode never writes into it, or hard-link mode had not
        /// yet begun linking. The store starts from it exactly as it did before the upgrade.</summary>
        Untouched,

        /// <summary>Hard-link mode had begun linking, and pg_upgrade's rename of <c>global\pg_control</c> was
        /// undone. The new cluster that shared its files was never started outside pg_upgrade, so the old
        /// cluster is intact.</summary>
        ControlFileRestored,

        /// <summary>It could not be put back. The store cannot start on either runtime until someone does it
        /// by hand, and the log has named exactly what to move.</summary>
        NotRestored,
    }

    /// <summary>
    /// Everything a failure BEFORE the commit point does (#3927): stop the old cluster if this upgrade
    /// started it, drop the half-built new cluster, put the pre-upgrade data directory back as the old runtime
    /// needs it, revert the runtime, and return a Failed outcome that says how much of that actually worked.
    ///
    /// <para>The data directory is put back BEFORE the runtime is reverted, so the revert's own checks (the
    /// data-major guard, the live-server refusal) read the directory the store will really start from. And
    /// the claims come last, from what happened. They used to come first, as one CRITICAL line promising that
    /// the store kept running and that the pre-upgrade data directory "was never modified", logged before
    /// anything had been put back and false in hard-link mode.</para>
    /// </summary>
    internal async Task<StoreUpgradeOutcome> RecoverFromPreCommitFailureAsync(
        UpgradeContext context,
        FileTransferMode mode,
        bool oldStarted,
        string newDataDirectory,
        string step,
        string failure,
        string? fromTimescale)
    {
        _logger.LogCritical(
            "STORE UPGRADE FAILED at step '{Step}': {Message}. Putting the store back on PostgreSQL {Old}.",
            step, failure, context.OldMajor);

        await TryStopAsync(context, oldStarted);
        TryDeleteDirectory(newDataDirectory);
        var preUpgradeData = PutBackPreUpgradeDataDirectory(context, mode);
        var reverted = RevertRuntime(context.RuntimeRoot, context.ZipHash, context.DataDirectory, context.OldMajor);
        ReportRecoveryBeforeCommit(context, preUpgradeData, reverted);

        return new StoreUpgradeOutcome(
            StoreUpgradeStatus.Failed, context.OldMajor, context.NewMajor,
            fromTimescale, context.BundledTimescaleVersion, step, failure, mode == FileTransferMode.Link,
            preUpgradeData, reverted);
    }

    /// <summary>
    /// Puts the pre-upgrade data directory back where, and as, the old runtime needs it (#3927). Two things
    /// can stand in the way, and each is undone only when the evidence says this upgrade did it:
    /// <list type="bullet">
    /// <item>The directory swap moves it aside to its retained name, and moves it back when the second move
    /// fails; that move back can fail too. Nothing at the configured path and a cluster under the retained
    /// name is exactly that, and left alone it is the worst outcome available here: the next start finds no
    /// cluster, initializes an EMPTY store in its place, and the retention sweep deletes the real one two
    /// starts later. Moving it back is the swap's own undo, tried once more.</item>
    /// <item>In hard-link mode pg_upgrade renames <c>global\pg_control</c> once linking starts; see
    /// <see cref="RestoreLinkedControlFile"/>. Only in hard-link mode: copy mode never renames it, and a
    /// renamed control file this upgrade did not produce is not this service's to rename back.</item>
    /// </list>
    /// </summary>
    private PreUpgradeDataDirectory PutBackPreUpgradeDataDirectory(UpgradeContext context, FileTransferMode mode)
    {
        var retained = RetainedDataDirectoryFor(context.DataDirectory, context.OldMajor);
        if (!File.Exists(Path.Combine(context.DataDirectory, "PG_VERSION"))
            && File.Exists(Path.Combine(retained, "PG_VERSION")))
        {
            try
            {
                Directory.Move(retained, context.DataDirectory);
                _logger.LogWarning(
                    "Moved the pre-upgrade data directory back from {Retained} to {DataDirectory}: the directory swap had moved it aside and could not move it back itself.",
                    retained, context.DataDirectory);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                var linkedControlFile = File.Exists(Path.Combine(retained, "global", "pg_control.old"))
                    && !File.Exists(Path.Combine(retained, "global", "pg_control"));
                _logger.LogCritical(
                    "The store's data is at {Retained} and nothing is at {DataDirectory}: the directory swap moved it aside, and neither the swap nor this recovery could move it back ({Message}). Move it back by hand BEFORE restarting the service{ControlFileStep}. Do not restart first: a start that finds no cluster at the configured path initializes an EMPTY store there, and the real one is then deleted as an expired rollback copy two starts later.",
                    retained, context.DataDirectory, ex.Message,
                    linkedControlFile
                        ? ", then rename global\\pg_control.old inside it back to global\\pg_control (pg_upgrade renamed it when hard-link mode began linking)"
                        : string.Empty);
                return PreUpgradeDataDirectory.NotRestored;
            }
        }

        return mode == FileTransferMode.Link
            ? RestoreLinkedControlFile(context.DataDirectory, _logger)
            : PreUpgradeDataDirectory.Untouched;
    }

    /// <summary>
    /// Undoes the one change pg_upgrade makes to the OLD cluster in hard-link mode (#3927). Once linking
    /// starts it renames <c>global\pg_control</c> to <c>global\pg_control.old</c>, so that the old cluster
    /// cannot be started by accident while it shares files with the new one. A failure after that point used
    /// to leave the rename in place, and the store could not start on either runtime while the log said the
    /// data directory had never been modified. PostgreSQL's pg_upgrade documentation gives the way back: if
    /// the new cluster was never started, remove the suffix and the old cluster is usable again. This service
    /// never starts the new cluster before the commit point, and a copy of a real 17.10 + TimescaleDB 2.28.1
    /// store, taken through a full hard-link pg_upgrade and then restored this way, came back with every row
    /// and chunk it had before.
    ///
    /// <para>Acts only on exactly pg_upgrade's rename, <c>pg_control.old</c> present and <c>pg_control</c>
    /// absent, so it can never overwrite a control file. Static, with the logger passed in, so the rename is
    /// pinned by tests on a planted directory.</para>
    /// </summary>
    internal static PreUpgradeDataDirectory RestoreLinkedControlFile(string dataDirectory, ILogger logger)
    {
        var controlFile = Path.Combine(dataDirectory, "global", "pg_control");
        var renamed = controlFile + ".old";

        if (File.Exists(controlFile) || !File.Exists(renamed))
        {
            logger.LogInformation(
                "Nothing to restore in {Global}: pg_upgrade had not renamed the control file, so hard-link mode had not begun linking.",
                Path.GetDirectoryName(controlFile));
            return PreUpgradeDataDirectory.Untouched;
        }

        try
        {
            File.Move(renamed, controlFile);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            logger.LogCritical(
                "Could not rename {Renamed} back to {ControlFile} ({Message}). pg_upgrade renamed it when hard-link mode began linking, and until it is renamed back the store CANNOT start on either runtime. Rename it by hand before restarting the service. The new cluster that shared its files was never started outside pg_upgrade, so the old cluster is intact once the file is back.",
                renamed, controlFile, ex.Message);
            return PreUpgradeDataDirectory.NotRestored;
        }

        logger.LogWarning(
            "Renamed {Renamed} back to {ControlFile}. pg_upgrade renames it when hard-link mode begins linking, so the old cluster cannot be started while it shares files with the new one. The new cluster was never started outside pg_upgrade, so the old cluster is intact and starts again.",
            renamed, controlFile);
        return PreUpgradeDataDirectory.ControlFileRestored;
    }

    /// <summary>
    /// The last word on a failure before the commit point (#3927): what the store is left as, said only from
    /// what actually happened. Only a store whose data directory is back and whose runtime reverted is
    /// described as back, and only then is "no data has been lost" said without a condition. Every other
    /// shape is CRITICAL, because it needs a person before the next start.
    /// </summary>
    private void ReportRecoveryBeforeCommit(UpgradeContext context, PreUpgradeDataDirectory preUpgradeData, bool runtimeReverted)
    {
        if (preUpgradeData == PreUpgradeDataDirectory.NotRestored)
        {
            _logger.LogCritical(
                "The store CANNOT start on either runtime until its pre-upgrade data directory is put back by hand, as the CRITICAL entry above describes{RuntimeStep}. The data itself is intact on disk; nothing needs restoring from a backup.",
                runtimeReverted ? string.Empty : ", and the previous runtime has to be put back by hand as well");
            return;
        }

        if (!runtimeReverted)
        {
            _logger.LogCritical(
                "The store was NOT put back on PostgreSQL {Old}: the previous runtime could not be restored, and the CRITICAL entry above says why and what to do. {Current} still holds the PostgreSQL {New} binaries, which cannot open this data directory, so the next service start fails until the runtime is put back by hand. The data directory itself is intact.",
                context.OldMajor, Path.Combine(context.RuntimeRoot, "pgsql"), context.NewMajor);
            return;
        }

        _logger.LogWarning(
            "The store is back on PostgreSQL {Old}, and NO data has been lost: {Reason}.",
            context.OldMajor,
            preUpgradeData == PreUpgradeDataDirectory.ControlFileRestored
                ? "hard-link mode had begun linking, and pg_upgrade's rename of global\\pg_control has been undone; the new cluster that shared the old cluster's files was never started outside pg_upgrade"
                : "the pre-upgrade data directory was never modified");
    }

    /// <summary>
    /// The PID of a PostgreSQL server still running on <paramref name="dataDirectory"/>, or null (#3927):
    /// the PID on <c>postmaster.pid</c>'s first line, when that process is alive AND is a <c>postgres</c>
    /// process. The name check is not decoration. Windows reuses PIDs, so the pid file a crashed postmaster
    /// left behind can name an unrelated process by the time anyone reads it, and treating that as a live
    /// server would refuse a revert that nothing is blocking. An unreadable or malformed file names no process
    /// to check; PostgreSQL reports that itself when the store next starts.
    /// </summary>
    internal static int? FindLivePostmaster(string dataDirectory)
    {
        string? firstLine;
        try
        {
            var pidFile = Path.Combine(dataDirectory, "postmaster.pid");
            if (!File.Exists(pidFile))
            {
                return null;
            }

            /* Shared for writing and deletion too, so a postmaster updating the file never blocks the read. */
            using var stream = new FileStream(pidFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            using var reader = new StreamReader(stream);
            firstLine = reader.ReadLine();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }

        /* A standalone (single-user) backend records its PID negated, and holds the directory just the same. */
        if (!long.TryParse(firstLine?.Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var recorded)
            || recorded == 0
            || recorded > int.MaxValue
            || recorded < -int.MaxValue)
        {
            return null;
        }

        var pid = (int)Math.Abs(recorded);
        try
        {
            using var process = System.Diagnostics.Process.GetProcessById(pid);
            return string.Equals(process.ProcessName, "postgres", StringComparison.OrdinalIgnoreCase) ? pid : null;
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            /* Not running (ArgumentException), or it exited between the lookup and the name. */
            return null;
        }
    }

    /// <summary>
    /// Starts a cluster with a specific runtime, loopback-only and with no network/TLS overrides — the
    /// upgrade window is not the time to reconcile exposure. Uses the detaching runner because pg_ctl's
    /// spawned postmaster outlives it and inherits redirected handles (the redirect-on-start hang).
    /// </summary>
    private async Task StartClusterAsync(
        string binDirectory,
        string dataDirectory,
        int port,
        CancellationToken cancellationToken,
        string extraServerOptions = "",
        int waitSeconds = 120)
    {
        var serverLog = Path.Combine(
            Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(dataDirectory)))!,
            DarlingManagedPostgres.ServerLogFileName);

        var pgCtl = Path.Combine(binDirectory, "pg_ctl.exe");
        var exitCode = await DarlingManagedPostgres.RunDetachingToolAsync(
            pgCtl,
            $"-D \"{dataDirectory}\" -o \"-p {port} -c listen_addresses=127.0.0.1{extraServerOptions}\" -l \"{serverLog}\" -w -t {waitSeconds} start",
            TimeSpan.FromSeconds(waitSeconds + 180),
            cancellationToken);

        if (exitCode != 0)
        {
            throw new InvalidOperationException(
                $"could not start the cluster in {dataDirectory} with the runtime at {binDirectory} (pg_ctl exit {DarlingToolExitCode.Describe(exitCode)})" +
                DarlingToolExitCode.Diagnose(exitCode, pgCtl) +
                $"\nServer log tail:\n{ReadLogTail(DarlingManagedPostgres.PickNewestServerLog(serverLog, dataDirectory) ?? serverLog)}");
        }
    }

    /// <summary>
    /// The last lines of a server log, where PostgreSQL explains a failed start. Reads only the file's end, since
    /// the store's log is long-lived, and shares the file with the server that may still be writing it.
    /// </summary>
    private static string ReadLogTail(string logPath)
    {
        try
        {
            using var stream = new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            stream.Seek(Math.Max(0, stream.Length - 16384), SeekOrigin.Begin);
            using var reader = new StreamReader(stream);
            var lines = reader.ReadToEnd().Split('\n');
            return string.Join('\n', lines[Math.Max(0, lines.Length - 20)..]).TrimEnd();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return $"(could not read {logPath}: {ex.Message})";
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
        await using var connection = new NpgsqlConnection(DarlingStoreConnection.PinSessionTimeZoneUtc(builder.ConnectionString));
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
    /// Makes every database's TimescaleDB extension one the NEW runtime can restore, before pg_upgrade runs
    /// (#1706, reworked by #3908). pg_upgrade recreates the extension at the version the OLD cluster has in each
    /// database, template1 included, so each version needs complete libraries in the new runtime. When the new
    /// runtime carries the installed version (every store this service has shipped: 2.28.1 is carried), nothing
    /// moves here, and the quiesced update moves it once the upgrade has committed, where a failure costs no
    /// rollback copy. Otherwise the database is moved to <see cref="PlanTimescaleBridge"/>'s target, the old
    /// runtime's own default version, the ALTER first on a fresh session. Every probe loads nothing (<c>timescaledb.disable_load</c>): the bridge used to
    /// probe and update on one session, the probe loaded the old library, and TimescaleDB refused the update
    /// (0A000, measured). Returns the store database's version before and after (both null without the
    /// extension). A hard gate: any failure throws, because pg_upgrade would only fail later.
    /// </summary>
    internal async Task<(string? Before, string? After)> BridgeTimescaleAsync(
        int port,
        string password,
        IReadOnlyCollection<string> newRuntimeLibraries,
        string? oldRuntimeDefaultVersion,
        CancellationToken cancellationToken)
    {
        var ownerConnection = DarlingManagedPostgres.BuildConnectionString(port, password);
        var databases = new List<string>();

        var listBuilder = new NpgsqlConnectionStringBuilder(ownerConnection) { Database = "postgres", Pooling = false };
        await using (var connection = new NpgsqlConnection(DarlingStoreConnection.PinSessionTimeZoneUtc(listBuilder.ConnectionString)))
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

        string? before = null;
        string? after = null;

        foreach (var database in databases)
        {
            var isStore = string.Equals(database, DarlingManagedPostgres.DatabaseName, StringComparison.Ordinal);
            var installed = await ReadTimescaleVersionUnloadedAsync(ownerConnection, database, cancellationToken);
            if (isStore)
            {
                before = installed;
                after = installed;
            }

            if (installed is null)
            {
                continue;
            }

            var plan = PlanTimescaleBridge(installed, newRuntimeLibraries, oldRuntimeDefaultVersion);
            if (!plan.Needed)
            {
                _logger.LogInformation(
                    "TimescaleDB in database '{Database}' is {Version}, which the new runtime carries: pg_upgrade restores it as it is, and it moves to the runtime's own version after the upgrade commits.",
                    database, installed);
                continue;
            }

            if (plan.Target is null)
            {
                throw new InvalidOperationException(
                    $"TimescaleDB in database '{database}' is {installed}. The new runtime carries libraries for {Describe(newRuntimeLibraries)}, and this one can update to {oldRuntimeDefaultVersion ?? "nothing it can name"}, " +
                    $"and no version both carry is one {installed} can update to, so pg_upgrade could not restore the extension. The upgrade stops here.");
            }

            _logger.LogWarning(
                "Bridging TimescaleDB in database '{Database}': {From} -> {To}. This must happen on the OLD cluster: pg_upgrade recreates the extension at whatever version is installed, and the new runtime does not carry {From}.",
                database, installed, plan.Target, installed);

            await UpdateTimescaleAsFirstStatementAsync(ownerConnection, database, cancellationToken, plan.Target);
            var moved = await ReadTimescaleVersionLoadedAsync(ownerConnection, database, cancellationToken);
            if (!string.Equals(moved, plan.Target, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"TimescaleDB in database '{database}' is {moved ?? "(absent)"} after ALTER EXTENSION UPDATE TO '{plan.Target}'. " +
                    "pg_upgrade would fail on the version-suffixed library, so the upgrade stops here.");
            }

            if (isStore)
            {
                after = moved;
            }

            _logger.LogInformation("TimescaleDB in database '{Database}' is now {Version}.", database, moved);
        }

        return (before, after);

        static string Describe(IReadOnlyCollection<string> versions)
            => versions.Count == 0 ? "no TimescaleDB version" : string.Join(", ", versions);
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
    /// Everything a major upgrade needs from a LIVE server, run once the normal bootstrap has started it (so the
    /// server is owned by this process and shutdown still stops it — starting it here would silently turn the
    /// store into an "adopted" one this service refuses to stop).
    ///
    /// <para>Two jobs. It VERIFIES the upgrade actually landed — server major and a real read of a collector
    /// table, because "pg_upgrade exited 0" and "the store works" are different claims. And it runs the
    /// post-upgrade analyze staging: PostgreSQL 18 carries most optimizer statistics across, so this fills the
    /// documented gaps (extended statistics, extension-owned statistics) rather than re-analyzing a whole store
    /// that already has them.</para>
    ///
    /// <para>It used to have a third, the same-major TimescaleDB update (#1705), run here on the live store.
    /// #3908 moved that to before the store opens (<see cref="UpdateTimescaleQuiescedAsync"/>): run here, every
    /// session that had already loaded the old library failed its next statement once the extension moved, and
    /// the update killed the scheduler's running jobs, which then sat in crash backoff for up to an hour.</para>
    /// </summary>
    internal async Task<StoreUpgradeOutcome> CompleteAfterStartAsync(
        StoreUpgradeOutcome outcome,
        string ownerConnectionString,
        string newBinDirectory,
        int port,
        string userName,
        string password,
        int bundledMajor,
        CancellationToken cancellationToken)
    {
        if (outcome.Status != StoreUpgradeStatus.Succeeded)
        {
            return outcome;
        }

        try
        {
            var builder = new NpgsqlConnectionStringBuilder(ownerConnectionString) { Pooling = false };
            await using var connection = new NpgsqlConnection(DarlingStoreConnection.PinSessionTimeZoneUtc(builder.ConnectionString));
            await connection.OpenAsync(cancellationToken);

            await using (var version = new NpgsqlCommand("SHOW server_version_num", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds })
            {
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

            /* Read for the line below only: the quiesced update already ran, and VerifyTimescaleAfterStartAsync
               records and reports the extension. */
            string? installed;
            await using (var probe = new NpgsqlCommand(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds })
            {
                installed = await probe.ExecuteScalarAsync(cancellationToken) as string;
            }

            await VerifySentinelReadAsync(connection, cancellationToken);

            _logger.LogInformation(
                "Store runtime is current: PostgreSQL major {Major}, TimescaleDB {Timescale}.",
                bundledMajor, installed ?? "(not installed)");
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

        await RunAnalyzeInStagesAsync(newBinDirectory, port, userName, password, bundledMajor, cancellationToken);
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
