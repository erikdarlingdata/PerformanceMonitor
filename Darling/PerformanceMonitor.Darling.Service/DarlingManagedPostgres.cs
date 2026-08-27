/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The bundled-Postgres bootstrap — the shipped zero-admin default (headless plan Phase 8 /
/// v4.3 "bundled-managed"): when darling.json says <c>postgres.managed = true</c>, the worker
/// calls <see cref="EnsureRunningAsync"/> BEFORE touching the store, and this class unpacks the
/// runtime shipped beside the service (<c>pg-runtime\pgsql\</c>, self-healing from
/// <c>pg-runtime.zip</c>), initializes a cluster on first run (initdb), starts the server when
/// it is not already running (pg_ctl, loopback by default; an opt-in <c>postgres.network</c> block
/// exposes it on the LAN behind TLS + pg_hba — see the posture note below), creates the
/// <c>darling</c> database, and returns the derived connection string. On service shutdown the worker calls
/// <see cref="StopIfStartedByThisProcessAsync"/> — the flag matters: a server this process did
/// NOT start (an operator's own pg_ctl, a previous service crash's surviving postmaster) is
/// adopted for connections but never stopped, because stopping someone else's server is not
/// this service's call to make.
///
/// <para><b>Security posture — why not trust auth, even on localhost:</b> initdb runs with
/// <c>-A scram-sha-256</c> and a generated 32-character random password, never <c>-A trust</c>.
/// Trust would hand superuser DDL/DML to ANY local code that can open a loopback socket —
/// including network-capable-but-not-filesystem-capable attack primitives (SSRF from a co-hosted
/// web app, sandboxed code with socket access) and every other local user — silently and
/// unauditably. A shipped default must also survive an enterprise security scan, where a
/// trust-auth listener is an automatic finding. With scram the credential is required on the
/// wire, failed attempts are auditable, and access is confined to what can read the
/// DPAPI-LocalMachine-protected credential file (<c>pg-credential.dpapi</c> beside the data
/// directory — the same machine-bound posture as darling.json's <c>encryptedPassword</c>, so
/// the interactive Viewer on the same machine can derive the connection string too).</para>
///
/// <para><b>Network exposure — off by default, secure by default (darling-network-endpoints):</b>
/// with no <c>postgres.network</c> block the store binds <c>listen_addresses = '127.0.0.1'</c>
/// (forced every start via the <c>-o</c> runtime override, so it holds even against a hand-edited
/// <c>ALTER SYSTEM</c>), has no pg_hba network rule, no firewall rule, and <c>ssl=off</c> — byte-for-byte
/// today's loopback-only behavior. An opt-in <c>network</c> block (managed mode only) adds the bind IP to
/// listen_addresses, generates a self-signed cert for TLS <c>verify-full</c>, writes a marked
/// <c>hostssl darling &lt;role&gt; &lt;allowFrom&gt; scram-sha-256</c> pg_hba block (loopback rules stay
/// plain <c>host</c>), reloads, and best-effort adds a scoped firewall rule. Every layer is reconciled
/// each start, symmetric (removing the block closes the box), and fail-closed: any invalid/incomplete
/// exposure config degrades the store to loopback (the full disable-reconcile) + LogCritical — never
/// <c>ssl=off</c> while exposed, never a stale pg_hba rule left open.</para>
///
/// <para>Every step throws with an actionable message; the worker turns a bootstrap failure
/// into LogCritical + clean service exit (the existing no-store behavior). All idempotent:
/// a second <see cref="EnsureRunningAsync"/> against an initialized, running cluster does no
/// initdb, no restart, no credential rewrite. The conf append is marker-guarded and re-checked
/// every start, so a crash between initdb and the append self-heals on the next run instead of
/// silently degrading TimescaleDB to plain-PG mode.</para>
/// </summary>
[SupportedOSPlatform("windows")]
public sealed class DarlingManagedPostgres
{
    /// <summary>The cluster's bootstrap superuser AND the store's database name — the managed twin of the sample's unmanaged string.</summary>
    public const string UserName = "darling";
    public const string DatabaseName = "darling";

    /// <summary>DPAPI-LocalMachine blob holding the generated password, beside (not inside) the data directory.</summary>
    public const string CredentialFileName = "pg-credential.dpapi";

    /// <summary>
    /// DPAPI-LocalMachine blobs holding the generated passwords for the least-privilege login roles
    /// the V8 security hardening provisions (<see cref="DarlingManagedRoles"/>): <c>admin</c> reads
    /// both schemas + writes <c>config</c> (the Viewer's default identity), <c>viewer</c> reads only.
    /// Same location/posture as <see cref="CredentialFileName"/> — beside the data directory, machine
    /// bound. Generated idempotently and self-healing (a deleted file regenerates and the superuser
    /// re-asserts the role's password, unlike the owner's unrecoverable password).
    /// </summary>
    public const string AdminCredentialFileName = "pg-admin-credential.dpapi";
    public const string ViewerCredentialFileName = "pg-viewer-credential.dpapi";

    /// <summary>
    /// DPAPI-LocalMachine blob holding the generated password for the dedicated least-privilege
    /// <c>mcp</c> login role (darling-network-endpoints, D3-role) — the store pool identity the
    /// (optionally network-exposed) MCP host connects as. Same location/posture as the admin/viewer
    /// credentials, but hardened NON-interactive (<see cref="DarlingFileSecurity.HardenFile"/> with
    /// <c>allowInteractiveRead: false</c>): it is consumed only by the in-service MCP host, never an
    /// interactive Viewer, so it mirrors the superuser credential's ACL, not the admin/viewer one.
    /// </summary>
    public const string McpCredentialFileName = "pg-mcp-credential.dpapi";

    /// <summary>The least-privilege login role names provisioned into the managed cluster (V8 hardening;
    /// <c>mcp</c> added for the network endpoints, D3-role).</summary>
    public const string AdminRoleName = "admin";
    public const string ViewerRoleName = "viewer";
    public const string McpRoleName = "mcp";

    /// <summary>
    /// The search path (schemas in resolution order) the managed connection strings carry, so pooled
    /// connections resolve the bare table names to collect/config even if the database default was
    /// not (or could not be) set. Same schemas, same order as the SQL-side
    /// <c>PgSchemaGenerator.SearchPath</c> the V8 split writes as the database default — the
    /// connection-string form omits spaces; a test pins the order against the SQL form.
    /// </summary>
    public const string SearchPath = "collect,config,public";

    /// <summary>The server log pg_ctl appends to, beside (not inside) the data directory.</summary>
    public const string ServerLogFileName = "pg.log";

    /// <summary>Marker line guarding the idempotent postgresql.conf append.</summary>
    public const string ConfMarker = "# Managed by PerformanceMonitor Darling -- do not remove this block";

    /// <summary>
    /// Marker for the v2 worker-sizing conf block. A separate versioned block rather than an edit
    /// of the v1 block, so clusters initialized before the sizing existed self-heal on their next
    /// start — <see cref="EnsureConfAppended"/> checks each marker independently, and a
    /// marker-present-but-different-block conf is never rewritten in place.
    /// </summary>
    public const string ConfMarkerV2 = "# Managed by PerformanceMonitor Darling (v2 worker sizing) -- do not remove this block";

    /// <summary>
    /// Marker for the v3 memory-sizing conf block (derived from host RAM). A THIRD independently
    /// versioned block, separate for the same reason v2 is separate from v1: an already-provisioned
    /// cluster (v1 + v2 present, v3 absent) heals by GAINING this block on its next service-owned
    /// start rather than by an in-place rewrite, which <see cref="EnsureConfAppended"/> never does.
    /// Each marker is checked independently.
    /// </summary>
    public const string ConfMarkerV3 = "# Managed by PerformanceMonitor Darling (v3 memory sizing) -- do not remove this block";

    /// <summary>
    /// Marker for the v4 write-throughput block (connection headroom + WAL ceiling). A FOURTH
    /// independently versioned block, same heal discipline as v2/v3. Born from a 24-server field
    /// incident: the PG defaults of <c>max_connections = 100</c> and <c>max_wal_size = 1GB</c> are
    /// sized for a toy, and a fleet's first-bootstrap write burst (every collector's initial
    /// snapshot landing at once) forced back-to-back spread checkpoints while backend spawn
    /// failures (Windows error 487 under churn) surfaced as transient store write errors.
    /// </summary>
    public const string ConfMarkerV4 = "# Managed by PerformanceMonitor Darling (v4 write throughput) -- do not remove this block";

    /// <summary>
    /// Marker for the v5 co-located-sizing override (#1559). A FIFTH independently versioned block, same
    /// heal discipline: clusters whose v3 block wrote the old min(25%, 8 GB) shared_buffers gain this
    /// override on their next service-owned start — postgresql.conf takes the LAST occurrence of a
    /// setting, so appending wins without rewriting the v3 block. Carries the capped derivation
    /// (min(25% RAM, 1 GB)); on a box already at or under the cap it re-states the same figure, harmlessly.
    /// </summary>
    public const string ConfMarkerV5 = "# Managed by PerformanceMonitor Darling (v5 co-located sizing) -- do not remove this block";

    /// <summary>
    /// Marker for the v6 log-rotation block (#1652). Before this block the server's ONLY log was the single
    /// <c>pg.log</c> pg_ctl appends to — no rotation of any kind, growing unbounded across restarts on the
    /// same volume as the data directory. Same heal discipline as v2-v5: existing clusters gain the block on
    /// their next service-owned start (<c>logging_collector</c> is restart-only, so it applies right then —
    /// the append happens before pg_ctl start).
    /// </summary>
    public const string ConfMarkerV6 = "# Managed by PerformanceMonitor Darling (v6 log rotation) -- do not remove this block";

    /// <summary>
    /// Marker for the v7 compression-memory override (#1777). A SEVENTH independently versioned block,
    /// same heal discipline as v5: it re-states ONLY <c>maintenance_work_mem</c>, so a store already
    /// carrying a v3 block written under the old <c>min(5% RAM, 1 GB)</c> rule adopts the raised floor on
    /// its next service-owned start — postgresql.conf takes the LAST occurrence of a setting, so appending
    /// wins without rewriting v3. This is the half that reaches EXISTING stores: a formula change alone
    /// would only ever have applied to a fresh initdb, and the boxes that need it are the ones already
    /// collecting.
    /// </summary>
    public const string ConfMarkerV7 = "# Managed by PerformanceMonitor Darling (v7 compression memory) -- do not remove this block";

    /// <summary>
    /// Markers delimiting the Darling-managed network access block in pg_hba.conf
    /// (darling-network-endpoints, D5). <see cref="ReconcilePgHba"/> replaces exactly the lines
    /// between them and preserves every non-marked line, so the opt-in <c>hostssl</c> rule is
    /// symmetric — appended when exposed, removed when disabled — without disturbing the operator's
    /// own entries or initdb's loopback defaults.
    /// </summary>
    public const string PgHbaBeginMarker = "# BEGIN PerformanceMonitor Darling network access -- managed block, do not edit";
    public const string PgHbaEndMarker = "# END PerformanceMonitor Darling network access";

    /// <summary>The self-signed store TLS cert + key (PEM), generated beside the data directory for
    /// verify-full when exposed (darling-network-endpoints, D6). Delete both to rotate.</summary>
    public const string ServerCertFileName = "server.crt";
    public const string ServerKeyFileName = "server.key";

    /// <summary>~10-year self-signed validity (D6): a home-lab cert the operator pins; delete-to-rotate.</summary>
    private const int ServerCertValidityYears = 10;

    private const string PasswordAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private const int PasswordLength = 32;

    /* Process budgets. pg_ctl start/stop get -w -t 60 of their own, so the outer budget only
       has to outlive them; initdb on a cold disk can take tens of seconds. */
    private static readonly TimeSpan s_initDbTimeout = TimeSpan.FromSeconds(180);

    /* #2185: `--version` prints one line and exits, so this bounds a diagnostic probe, not real work. Short
       on purpose — it runs while a startup failure is already being reported. */
    private static readonly TimeSpan s_versionProbeTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan s_pgCtlTimeout = TimeSpan.FromSeconds(90);
    private static readonly TimeSpan s_statusTimeout = TimeSpan.FromSeconds(30);
    private const int PgCtlWaitSeconds = 60;

    private readonly PostgresConfig _config;
    private readonly ILogger _logger;
    private readonly string _runtimeRoot;
    private readonly string _runtimeZipPath;
    private readonly string _dataDirectory;
    private readonly string _credentialPath;
    private readonly string _serverLogPath;
    private readonly DarlingStoreUpgrade _storeUpgrade;

    private bool _startedByThisProcess;

    /// <summary>What the runtime probe did with the shipped zip this start (#1706) — non-null only when a
    /// newer runtime was extracted, and its PreviousBinDirectory is pg_upgrade's --old-bindir.</summary>
    private DarlingStoreUpgrade.RuntimeAdvance? _runtimeAdvance;

    /// <summary>The bundled runtime's identity, read once the runtime is settled and used by the post-start
    /// verification and the same-major TimescaleDB update.</summary>
    private int _bundledMajor;
    private string? _bundledTimescaleVersion;

    public DarlingManagedPostgres(PostgresConfig config, ILogger logger, string? runtimeRootOverride = null)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _runtimeRoot = runtimeRootOverride ?? Path.Combine(AppContext.BaseDirectory, "pg-runtime");
        /* The zip is always the runtime root's SIBLING — that is the shipped layout (both beside the
           service binary), and deriving it from the override rather than hardcoding AppContext keeps an
           overridden runtime a complete, self-consistent deployment. The store-upgrade path (#1706) needs
           exactly that: a staged runtime plus the zip that supersedes it. */
        _runtimeZipPath = runtimeRootOverride is null
            ? Path.Combine(AppContext.BaseDirectory, "pg-runtime.zip")
            : Path.Combine(
                Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(runtimeRootOverride)))
                    ?? AppContext.BaseDirectory,
                "pg-runtime.zip");
        _dataDirectory = ResolveDataDirectory(config);
        _credentialPath = CredentialPathFor(_dataDirectory);
        _serverLogPath = Path.Combine(ParentOf(_dataDirectory), ServerLogFileName);
        _storeUpgrade = new DarlingStoreUpgrade(_logger);
    }

    /// <summary>True when THIS process started the server — the only case shutdown may stop it.</summary>
    public bool StartedByThisProcess => _startedByThisProcess;

    /// <summary>
    /// What this start's store-runtime reconcile did (#1706) — carried out of the bootstrap so the worker
    /// can raise a real self-alert once the alert engine is up. The store is down while an upgrade runs, so
    /// its START can only be a log line; both terminal states happen with a live store and are alertable.
    /// </summary>
    internal DarlingStoreUpgrade.StoreUpgradeOutcome LastUpgradeOutcome { get; private set; }
        = DarlingStoreUpgrade.StoreUpgradeOutcome.None;

    public string DataDirectory => _dataDirectory;

    /// <summary>null/empty dataDirectory means %ProgramData%\PerformanceMonitorDarling\pg (created with inherited ACLs).</summary>
    public static string ResolveDataDirectory(PostgresConfig config)
    {
        if (config is null)
        {
            throw new ArgumentNullException(nameof(config));
        }

        return string.IsNullOrWhiteSpace(config.DataDirectory)
            ? Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
                "PerformanceMonitorDarling", "pg")
            : Path.GetFullPath(config.DataDirectory);
    }

    public static string CredentialPathFor(string dataDirectory)
        => Path.Combine(ParentOf(dataDirectory), CredentialFileName);

    /// <summary>Path to the <c>admin</c> role's DPAPI credential, beside the data directory.</summary>
    public static string AdminCredentialPathFor(string dataDirectory)
        => Path.Combine(ParentOf(dataDirectory), AdminCredentialFileName);

    /// <summary>Path to the <c>viewer</c> role's DPAPI credential, beside the data directory.</summary>
    public static string ViewerCredentialPathFor(string dataDirectory)
        => Path.Combine(ParentOf(dataDirectory), ViewerCredentialFileName);

    /// <summary>Path to the <c>mcp</c> role's DPAPI credential, beside the data directory (darling-network-endpoints).</summary>
    public static string McpCredentialPathFor(string dataDirectory)
        => Path.Combine(ParentOf(dataDirectory), McpCredentialFileName);

    /// <summary>
    /// 32 characters from [A-Za-z0-9] via the crypto RNG (~190 bits) — deliberately alphanumeric
    /// only, so the password survives initdb's --pwfile line, the connection string, and any
    /// future conf/pgpass surface without escaping bugs; the length carries the strength.
    /// </summary>
    public static string GeneratePassword()
        => RandomNumberGenerator.GetString(PasswordAlphabet, PasswordLength);

    /// <summary>
    /// The block appended once (marker-guarded) to the fresh cluster's postgresql.conf:
    /// TimescaleDB preloaded (the extension refuses to CREATE without it), the configured port,
    /// and loopback-only listening. The port line is a default for anyone starting the cluster
    /// by hand; the service itself passes -o "-p &lt;port&gt;" so a darling.json port change
    /// wins on the next start without editing the conf.
    /// </summary>
    public static string BuildConfAppend(int port)
    {
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarker).Append('\n');
        builder.Append("shared_preload_libraries = 'timescaledb'\n");
        builder.Append("port = ").Append(port).Append('\n');
        builder.Append("listen_addresses = '127.0.0.1'\n");
        /* #1681: per-run audit trail for TimescaleDB background jobs. Off by default, which meant
           timescaledb_information.job_errors and job_history returned ZERO rows for every job on a field
           store — including jobs with dozens of confirmed successful runs. When a compression job hung
           there (9 times in 6 days, always the two highest-write hypertables) there was no captured error,
           no worker PID, no start/finish record: nothing to diagnose from. The rows are small and written
           once per job run, not per row processed. */
        builder.Append("timescaledb.enable_job_execution_logging = on\n");
        /* LZ4 TOAST (PG14+; the bundled runtime is PG18): large text/XML values — query text, plan
           XML, deadlock/blocked-process XML — auto-compress on write faster than the pglz default
           and about as small, shrinking the ~1-day hot window before TimescaleDB's columnar
           compression takes over. This is PostgreSQL's automatic equivalent of the SQL-Server
           Dashboard's manual COMPRESS()/DECOMPRESS() on those columns — applied to every large
           value, not hand-picked ones. Managed mode only; a BYO store uses its own server default. */
        builder.Append("default_toast_compression = lz4\n");
        return builder.ToString();
    }

    /// <summary>
    /// The v2 worker-sizing block. PostgreSQL's default <c>max_worker_processes = 8</c> cannot
    /// launch TimescaleDB's 26 per-hypertable compression policy jobs — the postmaster logs
    /// "failed to launch job ... failed to start a background worker" storms and most policy runs
    /// fail (caught live: 21 failures vs 7 successes in timescaledb_information.job_stats on a
    /// fresh managed instance). Sizing follows the TimescaleDB guidance
    /// (max_worker_processes = 3 + timescaledb.max_background_workers + max_parallel_workers,
    /// background workers sized to hypertables + 2): DERIVED from the live hypertable count
    /// (<see cref="TimescaleSupport.HypertableCount"/> = the collector catalog PLUS collection_log, the V23
    /// non-catalog hypertable) so it never goes stale as collectors are added and is not under-sized by the
    /// collection_log compression policy (27 hypertables -> 29/40, 33 -> 35/46). Idle background workers
    /// cost a few MB each and no CPU. Both settings need a PostgreSQL restart, so an existing
    /// cluster picks this up on its next service-owned start — an adopted (not-started-by-us)
    /// server heals the conf now and applies it whenever its operator next restarts it.
    /// </summary>
    public static string BuildWorkerSizingConfAppend()
    {
        /* Derived from the TRUE hypertable count (never stale when a collector is added; includes
           collection_log, the V23 hypertable outside the collector catalog): one background worker per
           per-hypertable compression policy that can run concurrently + the scheduler + slack;
           max_worker_processes = 3 (other bg workers) + bg workers + 8 (default max_parallel_workers). */
        var maxBackgroundWorkers = TimescaleSupport.HypertableCount + 2;
        var maxWorkerProcesses = 3 + maxBackgroundWorkers + 8;
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV2).Append('\n');
        builder.Append("timescaledb.max_background_workers = ").Append(maxBackgroundWorkers).Append('\n');
        builder.Append("max_worker_processes = ").Append(maxWorkerProcesses).Append('\n');
        return builder.ToString();
    }

    /// <summary>
    /// The v4 write-throughput block. Two fixed settings, deliberately not derived:
    /// <c>max_connections = 200</c> doubles the PG default — headroom for the service pool, the
    /// co-located viewer's admin/viewer seats, MCP, and psql without approaching the ceiling
    /// (idle PG connections cost a few MB each; the real concurrency is bounded by the service's
    /// own pool). <c>max_wal_size = 4GB</c> quadruples the default 1GB, which a fleet's
    /// first-bootstrap burst (every collector's initial snapshot at once) blows through in
    /// seconds, forcing continuous spread checkpoints that stall every write behind them
    /// (observed live: back-to-back 4-minute checkpoints on a 24-server bootstrap). WAL space is
    /// a CEILING, not an allocation — a quiet store never uses it. Both are restart-only, healed
    /// on the next service-owned start like v2/v3.
    /// </summary>
    public static string BuildWriteThroughputConfAppend()
    {
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV4).Append('\n');
        builder.Append("max_connections = 200\n");
        builder.Append("max_wal_size = 4GB\n");
        return builder.ToString();
    }

    /// <summary>
    /// The v5 co-located-sizing override block (#1559): re-states <c>shared_buffers</c> at the CAPPED
    /// derivation (min(25% RAM, 1 GB) — see <see cref="DeriveMemorySettings"/> for the dedicated-server
    /// and Windows-487 rationale) so an EXISTING cluster provisioned under the old min(25%, 8 GB) rule
    /// heals down on its next service-owned start. postgresql.conf semantics: the LAST occurrence of a
    /// setting wins, so this override never edits the v3 block in place.
    /// </summary>
    public static string BuildColocatedSizingConfAppend(long totalPhysicalMemoryBytes)
    {
        var settings = DeriveMemorySettings(totalPhysicalMemoryBytes);
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV5).Append('\n');
        builder.Append("shared_buffers = ").Append(settings.SharedBuffersMb).Append("MB\n");
        return builder.ToString();
    }

    /// <summary>
    /// The v6 log-rotation block (#1652): hand server logging to PostgreSQL's own logging collector as a
    /// SELF-CAPPING weekday ring — <c>log_filename = 'postgresql-%a.log'</c> names files by weekday
    /// (postgresql-Mon.log … postgresql-Sun.log), <c>log_rotation_age = 1d</c> rolls daily, and
    /// <c>log_truncate_on_rotation = on</c> truncates each file when its weekday comes around again. Hard
    /// cap of seven files, one week of history, ZERO sweep code — the ring is the retention policy.
    /// <c>log_rotation_size = 0</c> disables size-based rotation on purpose: a size roll appends (the
    /// truncate applies only to age-based rotation), and the age ring already bounds the set.
    ///
    /// <para><c>pg.log</c> (the pg_ctl <c>-l</c> file) stays exactly where it is and keeps its job: pg_ctl's
    /// own chatter plus anything the server says BEFORE the collector starts — which is precisely the
    /// startup-failure window the diagnostics tail exists for. With the collector owning steady-state
    /// logging, pg.log gains only a few lines per restart instead of the entire server log.</para>
    /// </summary>
    public static string BuildLogRotationConfAppend()
    {
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV6).Append('\n');
        builder.Append("logging_collector = on\n");
        builder.Append("log_directory = 'log'\n");
        builder.Append("log_filename = 'postgresql-%a.log'\n");
        builder.Append("log_rotation_age = 1d\n");
        builder.Append("log_rotation_size = 0\n");
        builder.Append("log_truncate_on_rotation = on\n");
        return builder.ToString();
    }

    /* ===================== v3 memory sizing (derived from host RAM) ===================== */

    /// <summary>4 GB — the conservative fallback used only when the runtime RAM query fails, so sizing
    /// never divides a zero/garbage reading (yields shared_buffers 1 GB, work_mem 16 MB).</summary>
    private const long MemoryFallbackRamBytes = 4L * 1024 * 1024 * 1024;

    /// <summary>The four PostgreSQL memory settings derived from total physical RAM, all in whole MB.</summary>
    internal readonly record struct MemorySettings(
        int SharedBuffersMb,
        int EffectiveCacheSizeMb,
        int MaintenanceWorkMemMb,
        int WorkMemMb);

    /// <summary>
    /// Derives the four memory settings from total physical RAM — PURE and testable via an injected byte
    /// count, exactly the way <see cref="BuildWorkerSizingConfAppend"/> derives from the hypertable count.
    /// This is SCALE-READINESS, not a fix for observed pressure: the stock PostgreSQL defaults
    /// (shared_buffers 128 MB, work_mem 4 MB, maintenance_work_mem 64 MB, effective_cache_size 4 GB) are
    /// fine for a handful of monitored servers on an 8 GB box, but would bottleneck the "up to 500 servers"
    /// store. Formulas:
    /// <list type="bullet">
    /// <item><b>shared_buffers</b> = min(25% RAM, 1 GB) — the docs' 25% starting point is explicitly
    ///   conditioned on a DEDICATED database server, which the managed store is NOT: it is co-located
    ///   with the service, the viewer, and often Lite, so the OS cache is shared and a large PG buffer
    ///   pool double-caches against it. The 1 GB CAP is also the Windows 487 mitigation (#1559): every
    ///   backend process must re-reserve the shared memory region at the postmaster's base address, and
    ///   the pgsql-bugs history (BUG #14050 / #18954) documents larger shared_buffers exacerbating
    ///   could-not-reserve-shared-memory (error code 487) spawn failures under ASLR/DLL address
    ///   pressure — observed live on a 16 GB field box running the prior 4 GB segment. A smaller
    ///   segment also shrinks every backend's reattach surface and the checkpointer's sweep.
    ///   RESTART-ONLY, like max_worker_processes; it applies on the next server start.</item>
    /// <item><b>effective_cache_size</b> = 75% RAM — a PLANNER HINT (no allocation) telling the planner how
    ///   much data is likely cached (PG + OS cache), biasing it toward index scans.</item>
    /// <item><b>maintenance_work_mem</b> = min(max(5% RAM, 1.5 GB), 25% RAM, 2 GB) — headroom for VACUUM /
    ///   CREATE INDEX, and the setting TimescaleDB's compression sort runs on, which is what drove the
    ///   shape (#1777). MEASURED on a production field instance (16 GB RAM class), three points during a
    ///   one-time catch-up of large backlog chunks: at the old formula's landing point (~800 MB) compression
    ///   moved ~9.1 MB/s of uncompressed input; at 1536 MB it moved 15.5 MB/s (a pure-linear null hypothesis
    ///   predicted 2833s, actual was 1657s — a real effect, not noise); at 4096 MB it moved 16.1 MB/s, so
    ///   going 2.7x further past 1536 bought nothing measurable. Hence the terms: the <b>1.5 GB floor</b> is
    ///   the measured capture point and is the fix itself (the old 5%-of-RAM term landed UNDER the old 1 GB
    ///   cap on a 16 GB host, so raising the cap alone would have changed nothing); the <b>25%-of-RAM</b>
    ///   term keeps the floor from overcommitting a small host; the <b>2 GB cap</b> concedes nothing
    ///   measurable and bounds the big-RAM case. Honest limits, both recorded in #1777: the three points are
    ///   different tables at different sizes rather than a controlled experiment, so the exact threshold
    ///   between ~800 MB and 1536 MB is unknown; and the floor raises SMALL hosts more than the 16 GB host
    ///   it was measured on (a 4 GB host goes 204 -> 1024 MB, an 8 GB host 409 -> 1536 MB). That is
    ///   deliberate and bounded: this is a per-operation CEILING, not a reservation — PostgreSQL grows the
    ///   sort/TidStore allocation to fit the work, and a small host's chunks are small, so the ceiling is
    ///   simply never reached there. PG 17+ (the bundle pins 18.4) also made vacuum's dead-TID store grow
    ///   incrementally rather than allocating the full limit up front, which is what made the old comment's
    ///   "several autovacuum workers can each take up to this" the binding worry it no longer is.</item>
    /// <item><b>work_mem</b> = clamp(RAM/512, 16 MB, 64 MB) — the ONE with real downside (per-sort,
    ///   per-connection: worst-case ≈ max_connections × sorts × work_mem), so it is deliberately modest.
    ///   At the PG default max_connections = 100 with ~3 concurrent sort/hash nodes, the pathological
    ///   all-connections-busy case is 100 × 3 × (RAM/512) ≈ 58% of RAM until the 64 MB cap tightens it —
    ///   and each sort/hash SPILLS to a temp file rather than OOMing when it exceeds work_mem (PG13+ spills
    ///   hash aggregates too), while the real Darling store runs a handful of pooled connections, not 100
    ///   concurrent analytical queries.</item>
    /// </list>
    /// </summary>
    internal static MemorySettings DeriveMemorySettings(long totalPhysicalMemoryBytes)
    {
        var ram = totalPhysicalMemoryBytes > 0 ? totalPhysicalMemoryBytes : MemoryFallbackRamBytes;

        const long oneMb = 1024L * 1024L;
        const long oneGb = 1024L * oneMb;

        var sharedBuffers = Math.Min(ram / 4, oneGb);              /* 25% RAM, capped at 1 GB (co-located store + Windows 487 mitigation, #1559) — restart-only */
        var effectiveCache = ram / 4 * 3;                          /* 75% RAM — planner hint, not an allocation */
        /* #1777: 5% RAM with a MEASURED 1.5 GB floor (compression throughput rose ~70% reaching it and
           plateaued there), guarded by 25% of RAM so the floor cannot overcommit a small host, and capped
           at 2 GB where the field data showed nothing further to gain.

           THE CONSTRAINT THE SMALL-HOST LANDINGS REST ON: both of today's consumers allocate
           INCREMENTALLY — a tuplesort grows to fit its input and SPILLS past the ceiling rather than
           reserving it, and PG 17+ builds vacuum's dead-TID store (TidStore) the same way. So this number
           bounds what an operation MAY use, not what it WILL use, which is what makes a 4 GB host's
           1024 MB landing safe despite being 5x its old 204 MB. If a future consumer ever PRE-ALLOCATES
           maintenance_work_mem, that reasoning breaks and the small-host landings need revisiting here. */
        var maintenanceWorkMem = Math.Min(
            Math.Min(Math.Max(ram / 20, 1536 * oneMb), ram / 4),
            2048 * oneMb);
        var workMem = Math.Clamp(ram / 512, 16 * oneMb, 64 * oneMb);

        return new MemorySettings(
            (int)(sharedBuffers / oneMb),
            (int)(effectiveCache / oneMb),
            (int)(maintenanceWorkMem / oneMb),
            (int)(workMem / oneMb));
    }

    /// <summary>
    /// The v3 memory-sizing block, built the SAME marker-guarded way as
    /// <see cref="BuildWorkerSizingConfAppend"/> and appended by <see cref="EnsureConfAppended"/> on every
    /// start (so an already-provisioned cluster gains it on restart, not just a fresh initdb). Takes the
    /// RAM byte count so it is unit-testable; <see cref="GetTotalPhysicalMemoryBytes"/> supplies the live
    /// value at runtime. Values are emitted in whole MB. shared_buffers needs a PostgreSQL restart — the
    /// service applies the whole block on its next server-owned start, exactly as it does the restart-only
    /// worker sizing.
    /// </summary>
    public static string BuildMemorySizingConfAppend(long totalPhysicalMemoryBytes)
    {
        var settings = DeriveMemorySettings(totalPhysicalMemoryBytes);
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV3).Append('\n');
        builder.Append("shared_buffers = ").Append(settings.SharedBuffersMb).Append("MB\n");
        builder.Append("effective_cache_size = ").Append(settings.EffectiveCacheSizeMb).Append("MB\n");
        builder.Append("maintenance_work_mem = ").Append(settings.MaintenanceWorkMemMb).Append("MB\n");
        builder.Append("work_mem = ").Append(settings.WorkMemMb).Append("MB\n");
        return builder.ToString();
    }

    /// <summary>
    /// The v7 compression-memory override (#1777) — the propagation half of the raised
    /// <c>maintenance_work_mem</c> floor. Built exactly like the v5 shared_buffers override: it re-states
    /// ONE setting so a store whose v3 block was written under the old <c>min(5% RAM, 1 GB)</c> rule heals
    /// UP by conf last-occurrence-wins, without ever rewriting the v3 block (which
    /// <see cref="EnsureConfAppended"/> never does). Without this block the new formula would reach fresh
    /// initdbs only, and the stores that measurably need it are the ones already running.
    ///
    /// <para><c>maintenance_work_mem</c> is plain SIGHUP-reloadable rather than restart-only, but the append
    /// happens before <c>pg_ctl start</c>, so an existing store picks it up on that very start.</para>
    /// </summary>
    public static string BuildCompressionMemoryConfAppend(long totalPhysicalMemoryBytes)
    {
        var settings = DeriveMemorySettings(totalPhysicalMemoryBytes);
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV7).Append('\n');
        builder.Append("maintenance_work_mem = ").Append(settings.MaintenanceWorkMemMb).Append("MB\n");
        return builder.ToString();
    }

    /// <summary>
    /// The derived managed-mode connection string: <c>127.0.0.1</c> + port + darling/darling + the
    /// generated password, carrying the collect/config <see cref="SearchPath"/> so every pooled connection
    /// resolves the bare table names to the V8 schemas regardless of the database default. Uses the explicit
    /// IPv4 loopback rather than the name "localhost": listen_addresses binds IPv4 <c>127.0.0.1</c> (plus the
    /// optional network IP when exposed), NOT <c>::1</c>, so a host that resolves "localhost" to IPv6 first
    /// could otherwise miss the listener. (darling-network-endpoints)
    /// </summary>
    public static string BuildConnectionString(int port, string password)
        => BuildRoleConnectionString(port, UserName, password);

    /// <summary>
    /// Builds a managed loopback connection string for a specific login role — shared by
    /// <see cref="BuildConnectionString"/> (the owner) and
    /// <see cref="TryBuildMcpConnectionStringFromStoredCredential"/> (the <c>mcp</c> role). Same
    /// <c>127.0.0.1</c> + port + <see cref="SearchPath"/> shape; only the username/password differ.
    /// </summary>
    private static string BuildRoleConnectionString(int port, string username, string password)
    {
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = "127.0.0.1",
            Port = port,
            Username = username,
            Password = password,
            Database = DatabaseName,
            SearchPath = SearchPath,
            /* #1559: bound the service's backend count. Every pooled Npgsql connection is a live
               postgres.exe PROCESS on Windows, and each spawn must re-reserve the shared memory
               region (the 487 surface) — a field box showed 43 backends during a 24-server sweep.
               24 comfortably covers the 4-wide sweep + command/beacon/alert/analysis seams; Npgsql's
               default idle pruning shrinks the pool between bursts. */
            MaxPoolSize = 24,
        };
        return builder.ConnectionString;
    }

    /// <summary>
    /// Derives the managed connection string from the stored credential WITHOUT touching the
    /// server — for secondary consumers (the MCP host) that must never bootstrap; the worker
    /// owns the lifecycle. Null until the worker's first initdb has written the credential.
    /// </summary>
    public static string? TryBuildConnectionStringFromStoredCredential(PostgresConfig config)
    {
        var credentialPath = CredentialPathFor(ResolveDataDirectory(config));
        if (!File.Exists(credentialPath))
        {
            return null;
        }

        return BuildConnectionString(config.Port, DarlingSecrets.Unprotect(File.ReadAllText(credentialPath).Trim()));
    }

    /// <summary>
    /// Derives the MCP store connection string from the stored <c>mcp</c>-role credential WITHOUT touching
    /// the server (darling-network-endpoints, D3-role) — the least-privilege pool the MCP host connects as
    /// instead of the owner. Same <c>127.0.0.1</c> + port + <see cref="SearchPath"/> shape as the owner
    /// string, but <c>Username = mcp</c>. Null until <see cref="DarlingManagedRoles.EnsureProvisionedAsync"/>
    /// has written the credential — which happens AFTER migration, later than the owner credential, so the
    /// MCP host's first-boot poll budget must tolerate the delay.
    /// </summary>
    public static string? TryBuildMcpConnectionStringFromStoredCredential(PostgresConfig config)
    {
        var credentialPath = McpCredentialPathFor(ResolveDataDirectory(config));
        if (!File.Exists(credentialPath))
        {
            return null;
        }

        return BuildRoleConnectionString(config.Port, McpRoleName, DarlingSecrets.Unprotect(File.ReadAllText(credentialPath).Trim()));
    }

    /// <summary>
    /// Derives the store connection string from the stored <c>viewer</c>-role credential WITHOUT touching the
    /// server (#1562) — the least-privilege READ-ONLY pool the web dashboard host connects as (not the owner,
    /// not <c>mcp</c>). Same <c>127.0.0.1</c> + port + <see cref="SearchPath"/> shape as the owner string, but
    /// <c>Username = viewer</c>. Null until <see cref="DarlingManagedRoles.EnsureProvisionedAsync"/> has written
    /// the credential (AFTER migration), so the web host's first-boot poll budget must tolerate the delay — the
    /// twin of <see cref="TryBuildMcpConnectionStringFromStoredCredential"/>.
    /// </summary>
    public static string? TryBuildViewerConnectionStringFromStoredCredential(PostgresConfig config)
    {
        var credentialPath = ViewerCredentialPathFor(ResolveDataDirectory(config));
        if (!File.Exists(credentialPath))
        {
            return null;
        }

        return BuildRoleConnectionString(config.Port, ViewerRoleName, DarlingSecrets.Unprotect(File.ReadAllText(credentialPath).Trim()));
    }

    /// <summary>
    /// The whole first-run story, idempotent: locate/unpack the runtime, initdb if the data
    /// directory has no cluster, self-heal the conf append, start the server if nothing is
    /// listening on the data directory, create the darling database if missing, and return the
    /// ready-to-use connection string. Throws (actionably) on any failure — the worker logs it
    /// critical and exits cleanly.
    /// </summary>
    public async Task<string> EnsureRunningAsync(CancellationToken cancellationToken)
    {
        var binDirectory = await EnsureRuntimeAsync(cancellationToken);

        /* Create the directory that holds the data dir + the DPAPI credential files, then LOCK it
           down (V8 hardening): strip the inherited world-readable ACLs %ProgramData% would otherwise
           give it, leaving SYSTEM + Administrators + the service account, plus INTERACTIVE traverse so
           the operator's Viewer can reach the admin/viewer credential files beside the data directory.
           Re-applied every start (self-healing, like the conf append) so an existing loose install is
           tightened too. Done BEFORE initdb so the data-dir subtree inherits the locked-down ACL. */
        Directory.CreateDirectory(ParentOf(_dataDirectory));
        TryHardenDirectory(ParentOf(_dataDirectory));

        /* Age out any pre-upgrade rollback copy BEFORE this start's own upgrade can create a new one.
           Running it afterwards would bump the brand-new copy's counter on the very start that produced
           it, costing it one of the two starts it is supposed to survive. */
        _storeUpgrade.SweepRetainedDataDirectories(_dataDirectory);

        /* The install directory's own housekeeping report, beside the store's. Deliberately adjacent: the
           two answer the same operator question about two different parents, and a field instance proved
           that reporting only the data directory's siblings leaves directories under the install directory
           completely unmentioned. Never throws, never deletes. */
        DarlingInstallDirectoryReport.Report(AppContext.BaseDirectory, _logger);

        if (!File.Exists(Path.Combine(_dataDirectory, "PG_VERSION")))
        {
            await InitializeClusterAsync(binDirectory, cancellationToken);
        }
        else
        {
            /* #1706: an EXISTING data directory may have been created by an older PostgreSQL than the one
               the package now ships. This is the only window in which an in-place major upgrade can run —
               nothing is connected, and both runtimes are on disk. It either upgrades, does nothing, or
               reverts and leaves the store exactly as it was. */
            await EnsureDataDirectoryMajorAsync(binDirectory, cancellationToken);
        }

        EnsureConfAppended(_dataDirectory);

        var password = ReadStoredPassword();

        /* Resolve the opt-in network exposure (darling-network-endpoints) BEFORE start so listen_addresses
           and the ssl trio can ride the -o runtime override. Fail-closed: an invalid/incomplete exposure
           config (or a cert-gen failure) returns a LOOPBACK plan carrying the degrade reason, logged
           critical here; the reconcile below then runs the FULL disable-reconcile (loopback listen + pg_hba
           block removed + reload + verify), so a previously-exposed cluster actually closes. The whole
           network path is caught internally (BuildNetworkPlan swallows cert-gen failure into a degrade;
           ReconcileNetworkAsync never throws) because EnsureRunningAsync's contract is throw => service-exit,
           and a typo in an optional, default-off endpoint must NEVER take collection down (Round 4 #3). */
        var networkPlan = BuildNetworkPlan();
        if (networkPlan.DegradeReason is not null)
        {
            _logger.LogCritical(
                "Store network exposure DISABLED (degraded to loopback-only): {Reason}. Fix postgres.network and restart to expose the store.",
                networkPlan.DegradeReason);
        }

        if (await IsRunningAsync(binDirectory, cancellationToken))
        {
            /* Already running — a previous service crash's surviving postmaster, or an operator
               started it by hand. Use it, never stop it (the flag stays false). */
            _logger.LogInformation(
                "Managed Postgres is already running for {DataDirectory} — connecting to it (this service did not start it and will not stop it)",
                _dataDirectory);
        }
        else
        {
            await StartServerAsync(binDirectory, networkPlan, cancellationToken);
            _startedByThisProcess = true;
        }

        var connectionString = BuildConnectionString(_config.Port, password);
        await EnsureDatabaseAsync(connectionString, cancellationToken);

        /* #1706: everything that needs a LIVE server — verify the upgrade landed, apply a same-major
           TimescaleDB extension update (the #1705 case, which needs no pg_upgrade at all), and run the
           post-upgrade analyze staging. Deliberately AFTER the start above rather than inside the upgrade,
           so the server this process started stays one this process will stop. */
        if (_bundledMajor > 0)
        {
            LastUpgradeOutcome = await _storeUpgrade.CompleteAfterStartAsync(
                LastUpgradeOutcome,
                connectionString,
                binDirectory,
                _config.Port,
                UserName,
                password,
                _bundledMajor,
                _bundledTimescaleVersion ?? string.Empty,
                cancellationToken);
        }

        /* Reconcile pg_hba + reload + verify, the adopted-listener guard, and the firewall against the LIVE
           server — symmetric (present when exposed, absent when loopback/degraded). Never throws (Round 4 #3):
           a network reconcile failure logs + degrades, it does not abort the bootstrap. */
        await ReconcileNetworkAsync(binDirectory, networkPlan, connectionString, cancellationToken);

        return connectionString;
    }

    /// <summary>
    /// pg_ctl stop -m fast, ONLY when this process started the server. Called on worker
    /// shutdown; never throws (a failed stop at shutdown is a warning, not a crash) and never
    /// takes the (already-cancelled) stopping token.
    /// </summary>
    public async Task StopIfStartedByThisProcessAsync()
    {
        if (!_startedByThisProcess)
        {
            return;
        }

        _startedByThisProcess = false;
        try
        {
            var binDirectory = Path.Combine(_runtimeRoot, "pgsql", "bin");
            var pgCtl = Path.Combine(binDirectory, "pg_ctl.exe");
            var (exitCode, output) = await RunToolAsync(
                pgCtl,
                $"stop -D \"{_dataDirectory}\" -m fast -w -t {PgCtlWaitSeconds}",
                s_pgCtlTimeout,
                CancellationToken.None);

            if (exitCode == 0)
            {
                _logger.LogInformation("Managed Postgres stopped (fast shutdown)");
            }
            else
            {
                /* {ExitCode} stays the raw int so structured sinks keep a numeric field to filter on;
                   the decoded meaning rides its own field. */
                _logger.LogWarning(
                    "Managed Postgres stop reported exit code {ExitCode} ({ExitCodeMeaning}): {Output}",
                    exitCode,
                    DarlingToolExitCode.Describe(exitCode),
                    DarlingToolExitCode.FormatOutput(output, exitCode));
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Managed Postgres stop failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Locates pg-runtime\pgsql\bin beside the service; when only pg-runtime.zip is present
    /// (fresh install, or someone deleted the extracted copy) it self-heals by extracting.
    /// Neither present is a packaging problem with a packaging answer, not a retry loop.
    /// </summary>
    private async Task<string> EnsureRuntimeAsync(CancellationToken cancellationToken)
    {
        var pgsqlDirectory = Path.Combine(_runtimeRoot, "pgsql");
        var binDirectory = Path.Combine(pgsqlDirectory, "bin");
        var pgCtl = Path.Combine(binDirectory, "pg_ctl.exe");
        if (File.Exists(pgCtl))
        {
            /* #1706: an extracted runtime is NOT refreshed by a deploy — this early return is exactly why a
               field store ran its original PostgreSQL and TimescaleDB forever. Compare the shipped zip
               against the stamp recorded at extraction time; a difference means the package carries a new
               runtime, and the rescued previous one becomes pg_upgrade's --old-bindir. */
            if (File.Exists(_runtimeZipPath))
            {
                _runtimeAdvance = await _storeUpgrade.TryAdvanceRuntimeAsync(
                    _runtimeRoot, _runtimeZipPath, _dataDirectory, TryIsRunningAsync, cancellationToken);
            }

            return binDirectory;
        }

        if (File.Exists(_runtimeZipPath))
        {
            _logger.LogInformation("Extracting the bundled Postgres runtime from {Zip} (first run)", _runtimeZipPath);
            await Task.Run(
                () => ZipFile.ExtractToDirectory(_runtimeZipPath, _runtimeRoot, overwriteFiles: true),
                cancellationToken);

            if (File.Exists(pgCtl))
            {
                /* Stamp the extraction so a later package that ships a different runtime is detectable. */
                File.WriteAllText(
                    Path.Combine(_runtimeRoot, DarlingStoreUpgrade.RuntimeStampFileName),
                    DarlingStoreUpgrade.ComputeFileHash(_runtimeZipPath));
                return binDirectory;
            }

            throw new InvalidOperationException(
                $"Extracted {_runtimeZipPath} but {pgCtl} is still missing — the archive does not contain pgsql\\bin. " +
                "Rebuild it with Darling\\tools\\fetch-pg-runtime.ps1 and redeploy.");
        }

        throw new InvalidOperationException(
            $"Managed Postgres runtime not found: neither {pgsqlDirectory} nor {_runtimeZipPath} exists. " +
            "Packaging builds pg-runtime.zip with Darling\\tools\\fetch-pg-runtime.ps1 and ships it beside the service binary; " +
            "put it there, or set postgres.managed = false and point postgres.connectionString at your own PostgreSQL.");
    }

    /// <summary>
    /// First-run initdb. The credential is generated and DPAPI-persisted BEFORE initdb runs:
    /// if the order were reversed, a crash after a successful initdb would leave a cluster whose
    /// password nobody knows. A failed initdb leaves the credential file behind harmlessly —
    /// the next attempt regenerates and overwrites it (initdb itself cleans up its partial data
    /// directory on failure).
    /// </summary>
    private async Task InitializeClusterAsync(string binDirectory, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Initializing managed Postgres cluster in {DataDirectory} (first run)", _dataDirectory);

        var password = GeneratePassword();
        File.WriteAllText(_credentialPath, DarlingSecrets.Protect(password));
        /* The SUPERUSER credential — locked to SYSTEM + Administrators + the service account, NEVER an
           interactive user (post-split the Viewer connects as admin/viewer, never darling). */
        TryHardenCredentialFile(_credentialPath, allowInteractiveRead: false);

        /* --pwfile is the non-interactive way to hand initdb the superuser password; the file
           lives next to the (equally sensitive, DPAPI-protected) credential for its few seconds
           of life and is deleted in finally. Alphanumeric-only password, UTF8 no BOM — a BOM
           would corrupt the first (and only) line initdb reads. Same restrictive ACL: it briefly
           holds the superuser password in the clear. */
        var passwordFile = Path.Combine(ParentOf(_dataDirectory), "pg-pwfile.tmp");
        File.WriteAllText(passwordFile, password + "\n");
        TryHardenCredentialFile(passwordFile, allowInteractiveRead: false);
        try
        {
            var initDb = Path.Combine(binDirectory, "initdb.exe");
            var (exitCode, output) = await RunToolAsync(
                initDb,
                $"-D \"{_dataDirectory}\" -U {UserName} -A scram-sha-256 --pwfile=\"{passwordFile}\" -E UTF8 --locale=C --data-checksums",
                s_initDbTimeout,
                cancellationToken);

            if (exitCode != 0)
            {
                /* #2185: on a LOADER status, gather the evidence ourselves rather than asking the operator
                   to run two commands and report back. That thread took four exchanges and the decisive fact
                   — `initdb --version` working while the bootstrap died — only ever existed in the reporter's
                   shell. Two extra process launches on a path that has already failed fatally is free. */
                var runtimeProbe = DarlingToolExitCode.IsLoaderStatus(exitCode)
                    ? await ProbeRuntimeBinariesAsync(binDirectory, cancellationToken)
                    : string.Empty;

                throw new InvalidOperationException(
                    BuildInitDbFailureMessage(exitCode, initDb, _dataDirectory, output, runtimeProbe));
            }
        }
        finally
        {
            try
            {
                File.Delete(passwordFile);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                _logger.LogWarning("Could not delete the temporary password file {Path} — delete it by hand", passwordFile);
            }
        }

        _logger.LogInformation("Managed Postgres cluster initialized (scram-sha-256, data checksums, UTF8/C locale)");
    }

    /// <summary>
    /// The first-run initdb failure, as an operator reads it (#2186). The leading clause is unchanged on
    /// purpose — it is what the existing field reports and the issue tracker are searchable by — and
    /// everything the raw form withheld follows it: the exit code decoded, the loader diagnosis when
    /// Windows set that code, and an <c>Output:</c> field that says it is empty BECAUSE the process was
    /// killed before it could write, rather than looking like data that failed to arrive.
    /// </summary>
    internal static string BuildInitDbFailureMessage(
        int exitCode, string exePath, string dataDirectory, string output, string runtimeProbe = "")
        => $"initdb failed (exit code {DarlingToolExitCode.Describe(exitCode)}) for {dataDirectory}." +
           DarlingToolExitCode.Diagnose(exitCode, exePath) +
           runtimeProbe +
           $"\nOutput:\n{DarlingToolExitCode.FormatOutput(output, exitCode)}";

    /// <summary>
    /// Asks each of the two binaries for its version and reports which one could not load (#2185).
    ///
    /// <para><b>Never throws and never blocks meaningfully.</b> This runs on a path that has ALREADY failed
    /// fatally, and its only job is to add a sentence to an exception that is about to be raised. A probe that
    /// threw would replace a precise "initdb failed, here is why" with whatever the probe hit; a probe that
    /// hung would turn a fast failure into a service that appears wedged at startup. So every fault mode —
    /// missing file, unreadable directory, cancellation, timeout — resolves to the empty string, which
    /// composes to the exact message the product produced before this existed.</para>
    ///
    /// <para><c>--version</c> is the right probe because it is the ONE invocation that loads the binary and
    /// its full dependency chain without touching the data directory, the port, or the cluster: it is the
    /// loader test with no side effect. A five-second budget is generous for a process that prints one line.</para>
    /// </summary>
    private static async Task<string> ProbeRuntimeBinariesAsync(string binDirectory, CancellationToken cancellationToken)
    {
        try
        {
            /* The order matters for the reader, not the logic: initdb is what failed, postgres is the
               hypothesis. Both are probed even when the first one loads, because "both loaded" is itself a
               finding — it rules out a permanently missing dependency and redirects to the event log. */
            var (initDbCode, _) = await RunToolAsync(
                Path.Combine(binDirectory, "initdb.exe"), "--version", s_versionProbeTimeout, cancellationToken);
            var (postgresCode, _) = await RunToolAsync(
                Path.Combine(binDirectory, "postgres.exe"), "--version", s_versionProbeTimeout, cancellationToken);

            return DarlingToolExitCode.DescribeRuntimeProbe(initDbCode, postgresCode);
        }
        catch (Exception)
        {
            /* Deliberately unfiltered. See the summary: the caller is composing a fatal message and there is
               no fault here worth surfacing over the failure that is already being reported. */
            return string.Empty;
        }
    }

    /// <summary>
    /// Marker-guarded conf append, re-checked on EVERY start — heals the crash window between
    /// initdb and the first append, which would otherwise silently cost TimescaleDB
    /// (shared_preload_libraries missing = CREATE EXTENSION fails = plain-PG degradation).
    ///
    /// <para>Takes the data directory rather than reading the field, because the store upgrade (#1706)
    /// must apply the SAME blocks to the freshly-initdb'd cluster BEFORE pg_upgrade runs: pg_upgrade
    /// starts the new cluster internally to restore the dump, and restoring TimescaleDB into a server
    /// that has not preloaded its library fails outright. Healing it afterwards would be too late.</para>
    /// </summary>
    private void EnsureConfAppended(string dataDirectory)
    {
        var confPath = Path.Combine(dataDirectory, "postgresql.conf");
        if (!File.Exists(confPath))
        {
            throw new InvalidOperationException(
                $"{confPath} is missing although PG_VERSION exists — the data directory looks damaged. " +
                "Stop the service, move/delete the data directory to re-initialize (destroys collected history), " +
                "or restore it from backup.");
        }

        var conf = File.ReadAllText(confPath);
        if (!conf.Contains(ConfMarker, StringComparison.Ordinal))
        {
            File.AppendAllText(confPath, BuildConfAppend(_config.Port));
            _logger.LogInformation("Appended managed settings to postgresql.conf (timescaledb preload, port {Port}, loopback only)", _config.Port);
        }

        /* Checked independently of the v1 marker: clusters initialized before the worker sizing
           existed have v1 but not v2, and heal here on their next start. */
        if (!conf.Contains(ConfMarkerV2, StringComparison.Ordinal))
        {
            File.AppendAllText(confPath, BuildWorkerSizingConfAppend());
            _logger.LogInformation("Appended v2 worker sizing to postgresql.conf (derived from {Hypertables} hypertables; effective from the next PostgreSQL restart)", TimescaleSupport.HypertableTables.Count);
        }

        /* Checked independently of v1/v2: an already-provisioned cluster has v1 + v2 but not v3, and heals
           by GAINING the memory block here on its next start. shared_buffers is restart-only, so it applies
           whenever the service next owns the start — exactly the worker-sizing story. Derived from the host's
           physical RAM at runtime (SCALE-READINESS for the up-to-500-servers store, not a fix for pressure). */
        if (!conf.Contains(ConfMarkerV3, StringComparison.Ordinal))
        {
            var ramBytes = GetTotalPhysicalMemoryBytes();
            File.AppendAllText(confPath, BuildMemorySizingConfAppend(ramBytes));
            var derived = DeriveMemorySettings(ramBytes);
            _logger.LogInformation(
                "Appended v3 memory sizing to postgresql.conf (host RAM {RamMb} MB -> shared_buffers {SharedBuffers}MB, effective_cache_size {EffectiveCache}MB, maintenance_work_mem {Maintenance}MB, work_mem {WorkMem}MB; effective from the next PostgreSQL restart)",
                ramBytes / (1024L * 1024L), derived.SharedBuffersMb, derived.EffectiveCacheSizeMb, derived.MaintenanceWorkMemMb, derived.WorkMemMb);
        }

        /* Checked independently of v1/v2/v3: an already-provisioned cluster heals by GAINING the
           write-throughput block (connection headroom + WAL ceiling) on its next start. */
        if (!conf.Contains(ConfMarkerV4, StringComparison.Ordinal))
        {
            File.AppendAllText(confPath, BuildWriteThroughputConfAppend());
            _logger.LogInformation(
                "Appended v4 write throughput to postgresql.conf (max_connections 200, max_wal_size 4GB; effective from the next PostgreSQL restart)");
        }

        /* Checked independently of v1-v4: a cluster provisioned under the old min(25%, 8 GB)
           shared_buffers rule heals DOWN to the co-located cap (last-occurrence-wins override). */
        if (!conf.Contains(ConfMarkerV5, StringComparison.Ordinal))
        {
            var v5RamBytes = GetTotalPhysicalMemoryBytes();
            File.AppendAllText(confPath, BuildColocatedSizingConfAppend(v5RamBytes));
            _logger.LogInformation(
                "Appended v5 co-located sizing to postgresql.conf (shared_buffers = {SharedBuffers}MB, capped at min(25% RAM, 1GB); effective from the next PostgreSQL restart)",
                DeriveMemorySettings(v5RamBytes).SharedBuffersMb);
        }

        /* Checked independently of v1-v5: an existing cluster heals by GAINING the log-rotation block
           (#1652). logging_collector is restart-only, and this append runs before pg_ctl start, so it is
           effective immediately on this very start. */
        if (!conf.Contains(ConfMarkerV6, StringComparison.Ordinal))
        {
            File.AppendAllText(confPath, BuildLogRotationConfAppend());
            _logger.LogInformation(
                "Appended v6 log rotation to postgresql.conf (logging collector, 7-file weekday ring under {LogDir}; pg.log keeps only pg_ctl and pre-collector startup lines)",
                Path.Combine(dataDirectory, "log"));
        }

        /* Checked independently of v1-v6: a store whose v3 block wrote maintenance_work_mem under the old
           min(5% RAM, 1 GB) rule heals UP to the measured 1.5 GB compression floor (last-occurrence-wins
           override, #1777). This is what carries the fix to EXISTING stores — the formula alone would only
           ever have reached a fresh initdb. */
        if (!conf.Contains(ConfMarkerV7, StringComparison.Ordinal))
        {
            var v7RamBytes = GetTotalPhysicalMemoryBytes();
            File.AppendAllText(confPath, BuildCompressionMemoryConfAppend(v7RamBytes));
            _logger.LogInformation(
                "Appended v7 compression memory to postgresql.conf (maintenance_work_mem = {Maintenance}MB from min(max(5% RAM, 1536MB), 25% RAM, 2048MB); TimescaleDB compression sorts on this setting)",
                DeriveMemorySettings(v7RamBytes).MaintenanceWorkMemMb);
        }
    }

    /// <summary>
    /// Total physical RAM in bytes, read once per bootstrap to size the v3 memory block — the runtime
    /// analogue of the worker sizing's hypertable count. Uses the Win32 <c>GlobalMemoryStatusEx</c>
    /// (<c>ullTotalPhys</c> = the machine's installed physical memory); on the rare failure it falls back to
    /// the GC's view of total available memory, then to a conservative 4 GB, so sizing never runs on a
    /// zero/garbage reading. Windows-only, like the rest of this managed-mode class.
    /// </summary>
    private long GetTotalPhysicalMemoryBytes()
    {
        try
        {
            var status = new MemoryStatusEx();
            if (GlobalMemoryStatusEx(status) && status.ullTotalPhys > 0)
            {
                return (long)status.ullTotalPhys;
            }

            _logger.LogWarning(
                "GlobalMemoryStatusEx did not return total physical memory (Win32 error {Error}); sizing Postgres memory from a fallback.",
                Marshal.GetLastWin32Error());
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not query total physical memory ({Message}); sizing Postgres memory from a fallback.", ex.Message);
        }

        var gcTotal = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        return gcTotal > 0 ? gcTotal : MemoryFallbackRamBytes;
    }

#pragma warning disable CS0649 // fields are populated by the native GlobalMemoryStatusEx call, not in managed code
    [StructLayout(LayoutKind.Sequential)]
    private sealed class MemoryStatusEx
    {
        public uint dwLength = (uint)Marshal.SizeOf<MemoryStatusEx>();
        public uint dwMemoryLoad;
        public ulong ullTotalPhys;
        public ulong ullAvailPhys;
        public ulong ullTotalPageFile;
        public ulong ullAvailPageFile;
        public ulong ullTotalVirtual;
        public ulong ullAvailVirtual;
        public ulong ullAvailExtendedVirtual;
    }
#pragma warning restore CS0649

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GlobalMemoryStatusEx([In, Out] MemoryStatusEx lpBuffer);

    /// <summary>
    /// The runtime-swap gate's view of "is anything running here" (#1706): TRUE only for an unambiguous
    /// running postmaster. Unlike <see cref="IsRunningAsync"/> this never throws — it runs BEFORE the data
    /// directory is known to exist (a host with an extracted runtime but a failed initdb answers 4, not 3),
    /// and for the swap decision "cannot tell" and "not running" have the same safe answer: the swap only
    /// needs to know nobody is holding the binaries open.
    /// </summary>
    private async Task<bool> TryIsRunningAsync(string binDirectory, CancellationToken cancellationToken)
    {
        try
        {
            var (exitCode, _) = await RunToolAsync(
                Path.Combine(binDirectory, "pg_ctl.exe"),
                $"status -D \"{_dataDirectory}\"",
                s_statusTimeout,
                cancellationToken);
            return exitCode == 0;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>
    /// Detects and, when required, performs the in-place major upgrade of an EXISTING data directory
    /// (#1706). Three outcomes, all of which leave a startable store:
    /// <list type="bullet">
    /// <item>Data directory major EQUALS the bundled major — nothing to do, the overwhelmingly common case.</item>
    /// <item>Data directory is OLDER — run the orchestrated pg_upgrade, or (on any failure) revert and keep
    /// running the old major.</item>
    /// <item>Data directory is NEWER than the bundled runtime — REFUSE to start. PostgreSQL cannot open a
    /// newer cluster with older binaries; starting anyway is how a downgrade silently corrupts a store.</item>
    /// </list>
    /// </summary>
    private async Task EnsureDataDirectoryMajorAsync(string binDirectory, CancellationToken cancellationToken)
    {
        var dataMajor = DarlingStoreUpgrade.ParseDataDirectoryMajor(
            await File.ReadAllTextAsync(Path.Combine(_dataDirectory, "PG_VERSION"), cancellationToken));
        var (bundledMajor, probeExitCode) = await ReadRuntimeMajorAsync(binDirectory, cancellationToken);

        if (DarlingStoreUpgrade.MustRefuseUnidentifiableRuntime(dataMajor, bundledMajor))
        {
            /* #1738: this exact pairing — the store's need KNOWN, the runtime unidentifiable — was logged on
               DARLING01 as "skipping the runtime version check. The store starts normally." and was followed
               one second later by the bootstrap dying on exit code -1073741515 (STATUS_DLL_NOT_FOUND). The
               degrade was backwards: the version check could not run BECAUSE the binaries could not run, which
               is the strongest possible evidence they must not be used, not a reason to wave them through.
               Refusing here costs nothing that proceeding would have saved — the start was going to fail
               regardless — and it converts a cryptic Win32 status code into a message naming the rescued
               runtime to restore. #2186 finished the thought: the refusal used to assert that the binaries
               did not run without ever saying HOW it knew, so the one piece of evidence it held — the probe's
               own exit code — died in a local variable. It is now quoted, decoded, and diagnosed. */
            throw new InvalidOperationException(
                $"The store's data directory {_dataDirectory} is PostgreSQL {dataMajor}, but the runtime at {binDirectory} could not be identified — pg_ctl --version exited {DarlingToolExitCode.Describe(probeExitCode)} instead of reporting a version. " +
                "A runtime that cannot report its own version cannot start this store either, so the service is stopping here rather than failing deeper with a Win32 error code. " +
                $"This usually means the wrong package was deployed. Restore the previous runtime from {PreviousRuntimeHint()} over {Path.GetDirectoryName(binDirectory)}, or redeploy a package whose PostgreSQL major is {dataMajor} or newer, then restart the service. " +
                "The data directory has not been touched." +
                DarlingToolExitCode.Diagnose(probeExitCode, Path.Combine(binDirectory, "pg_ctl.exe")));
        }

        if (dataMajor is null || bundledMajor is null)
        {
            _logger.LogWarning(
                "Could not determine the store's PostgreSQL major (data directory: {Data}, bundled runtime: {Bundled}) — skipping the runtime version check. The store starts normally.",
                dataMajor?.ToString(CultureInfo.InvariantCulture) ?? "unreadable",
                bundledMajor?.ToString(CultureInfo.InvariantCulture) ?? "unreadable");
            return;
        }

        _bundledMajor = bundledMajor.Value;
        _bundledTimescaleVersion = ReadBundledTimescaleVersion(binDirectory);

        if (dataMajor == bundledMajor)
        {
            return;
        }

        if (dataMajor > bundledMajor)
        {
            throw new InvalidOperationException(
                $"The store data directory {_dataDirectory} was created by PostgreSQL {dataMajor}, but this package bundles PostgreSQL {bundledMajor}. " +
                "PostgreSQL cannot open a newer cluster with older binaries, and there is no supported downgrade. " +
                "Install the newer package again, or restore the store from a backup taken with a matching runtime.");
        }

        var previousBin = _runtimeAdvance?.PreviousBinDirectory;
        if (previousBin is null || !File.Exists(Path.Combine(previousBin, "pg_ctl.exe")))
        {
            throw new InvalidOperationException(
                $"The store data directory {_dataDirectory} was created by PostgreSQL {dataMajor} and this package bundles PostgreSQL {bundledMajor}, " +
                $"but the PostgreSQL {dataMajor} binaries are not on this host, so an in-place upgrade is impossible " +
                "(pg_upgrade needs both runtimes). This happens when the pg-runtime directory was deleted before the upgrade ran. " +
                $"Restore a PostgreSQL {dataMajor} runtime at {PreviousRuntimeHint()}, then restart the service to upgrade; " +
                "or restore the store from backup.");
        }

        var outcome = await _storeUpgrade.UpgradeDataDirectoryAsync(
            new DarlingStoreUpgrade.UpgradeContext(
                previousBin,
                binDirectory,
                _runtimeRoot,
                _runtimeAdvance!.ZipHash ?? string.Empty,
                _dataDirectory,
                _config.Port,
                UserName,
                ReadStoredPassword(),
                dataMajor.Value,
                bundledMajor.Value,
                _bundledTimescaleVersion ?? string.Empty,
                EnsureConfAppended),
            cancellationToken);

        LastUpgradeOutcome = outcome;

        if (outcome.Status == DarlingStoreUpgrade.StoreUpgradeStatus.Failed)
        {
            /* The revert put the PREVIOUS runtime back behind the same bin path, so the identity read
               above now describes binaries that are no longer there. Re-read it, or the post-start
               completion would try to move the extension to a version this runtime does not ship. */
            _bundledMajor = (await ReadRuntimeMajorAsync(binDirectory, cancellationToken)).Major ?? 0;
            _bundledTimescaleVersion = ReadBundledTimescaleVersion(binDirectory);
        }
    }

    private string PreviousRuntimeHint()
        => Path.Combine(DarlingStoreUpgrade.PreviousRuntimeRootFor(_runtimeRoot), "pgsql");

    /// <summary>The bundled runtime's PostgreSQL major, from the binaries themselves rather than from a
    /// manifest that could disagree with what is on disk. The probe's exit code rides out alongside it
    /// (#2186): when the answer is "unidentifiable", that code is the ONLY evidence of why, and the
    /// refusal that consumes it used to have to assert the reason instead of showing it.</summary>
    private static async Task<(int? Major, int ExitCode)> ReadRuntimeMajorAsync(string binDirectory, CancellationToken cancellationToken)
    {
        var (exitCode, output) = await RunToolAsync(
            Path.Combine(binDirectory, "pg_ctl.exe"), "--version", s_statusTimeout, cancellationToken);
        return (exitCode == 0 ? DarlingStoreUpgrade.ParsePostgresMajor(output) : null, exitCode);
    }

    /// <summary>
    /// The TimescaleDB version the bundled runtime ships, read from its own
    /// <c>share\extension\timescaledb.control</c>. This is the version the store's extension must reach:
    /// every TimescaleDB function resolves to a version-suffixed library, and the runtime carries exactly one.
    /// </summary>
    private static string? ReadBundledTimescaleVersion(string binDirectory)
    {
        try
        {
            var pgsql = Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(binDirectory));
            if (pgsql is null)
            {
                return null;
            }

            var control = Path.Combine(pgsql, "share", "extension", "timescaledb.control");
            return File.Exists(control)
                ? DarlingStoreUpgrade.ParseTimescaleDefaultVersion(File.ReadAllText(control))
                : null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    /// <summary>pg_ctl status: 0 = a postmaster is running on this data directory, 3 = not running, 4 = bad/inaccessible data directory.</summary>
    private async Task<bool> IsRunningAsync(string binDirectory, CancellationToken cancellationToken)
    {
        var pgCtl = Path.Combine(binDirectory, "pg_ctl.exe");
        var (exitCode, output) = await RunToolAsync(
            pgCtl,
            $"status -D \"{_dataDirectory}\"",
            s_statusTimeout,
            cancellationToken);

        return exitCode switch
        {
            0 => true,
            3 => false,
            _ => throw new InvalidOperationException(BuildStatusFailureMessage(exitCode, pgCtl, _dataDirectory, output)),
        };
    }

    /// <summary>
    /// The pg_ctl status failure (#2186). The data-directory verdict is CONDITIONAL: pg_ctl's own codes
    /// (4 = bad or inaccessible data directory) do say the directory is unusable, but a Windows status
    /// says only that pg_ctl never ran, and blaming the data directory for that sends an operator to
    /// delete a perfectly good store over a missing DLL.
    /// </summary>
    internal static string BuildStatusFailureMessage(int exitCode, string exePath, string dataDirectory, string output)
    {
        var diagnosis = DarlingToolExitCode.Diagnose(exitCode, exePath);
        return $"pg_ctl status reported exit code {DarlingToolExitCode.Describe(exitCode)} for {dataDirectory}" +
               (diagnosis.Length == 0 ? " — the data directory is not usable." : ".") +
               diagnosis +
               $"\nOutput:\n{DarlingToolExitCode.FormatOutput(output, exitCode)}";
    }

    /// <summary>
    /// pg_ctl start, windowed (-w): returns only when the server accepts connections. The <c>-o</c>
    /// runtime override carries the port (authoritative over the conf line), listen_addresses (always
    /// <c>127.0.0.1</c>, plus the network IP when exposed — <c>-c</c> outranks postgresql.auto.conf, so
    /// this forces loopback even against a hand-edited <c>ALTER SYSTEM</c>), and the ssl trio when exposed
    /// (darling-network-endpoints, D2/D6). pg_ctl itself handles a stale postmaster.pid from a crash (the
    /// new postmaster validates and replaces it); when start still fails, the server log tail is surfaced
    /// in the error because that is where Postgres explains itself.
    /// </summary>
    private async Task StartServerAsync(string binDirectory, NetworkPlan networkPlan, CancellationToken cancellationToken)
    {
        var exposed = networkPlan.Mode == NetworkMode.Exposed;
        var runtimeOptions = BuildServerRuntimeOptions(
            _config.Port,
            exposed ? networkPlan.ListenIp : null,
            exposed ? networkPlan.CertPath : null,
            exposed ? networkPlan.KeyPath : null);

        _logger.LogInformation(
            "Starting managed Postgres (listen_addresses={Listen}, ssl={Ssl}, port {Port}, log: {Log})",
            BuildListenAddresses(exposed ? networkPlan.ListenIp : null),
            exposed ? "on" : "off",
            _config.Port, _serverLogPath);

        /* #1652: on clusters that grew a large pre-rotation pg.log, roll it aside ONCE so the
           legacy file stays bounded too. Going forward the v6 logging collector owns the server
           log and pg.log gains only pg_ctl chatter + pre-collector startup lines. Runs while the
           server is down (this method only runs when nothing is listening), so nothing holds the file. */
        CapLegacyServerLog(_serverLogPath, LegacyServerLogCapBytes, _logger);

        var pgCtl = Path.Combine(binDirectory, "pg_ctl.exe");
        var exitCode = await RunDetachingToolAsync(
            pgCtl,
            $"-D \"{_dataDirectory}\" -o \"{runtimeOptions}\" -l \"{_serverLogPath}\" -w -t {PgCtlWaitSeconds} start",
            s_pgCtlTimeout,
            cancellationToken);

        if (exitCode != 0)
        {
            throw new InvalidOperationException(
                BuildStartFailureMessage(exitCode, pgCtl, _dataDirectory, ReadServerLogTail()));
        }

        _logger.LogInformation("Managed Postgres started");
    }

    /// <summary>
    /// The pg_ctl start failure (#2186). Its <c>Server log tail</c> has the same trap the initdb message's
    /// <c>Output</c> had: a loader status means pg_ctl died before it could start a postmaster, so the tail
    /// reads "(no server log written)" — accurate, and completely misleading about where to look. The
    /// diagnosis says which situation this is before the tail invites an operator to read a log that was
    /// never going to exist.
    /// </summary>
    internal static string BuildStartFailureMessage(int exitCode, string exePath, string dataDirectory, string serverLogTail)
        => $"pg_ctl start failed (exit code {DarlingToolExitCode.Describe(exitCode)}) for {dataDirectory}." +
           DarlingToolExitCode.Diagnose(exitCode, exePath) +
           $"\nServer log tail:\n{serverLogTail}";

    /// <summary>The one-time cap on the legacy pre-rotation <c>pg.log</c> (#1652): past this size it is
    /// rolled to <c>pg.log.old</c> (replacing any previous roll) before the next start. Two files, bounded
    /// forever; small files are left alone so a healthy post-rotation pg.log is never churned.</summary>
    internal const long LegacyServerLogCapBytes = 10L * 1024 * 1024;

    /// <summary>
    /// Rolls an oversized <c>pg.log</c> to <c>pg.log.old</c> (replace-existing, so the pair can never grow
    /// past two files). Static and failure-tolerant: a locked or missing file logs a warning and never
    /// blocks the server start — the cap is hygiene, not a precondition.
    /// </summary>
    internal static void CapLegacyServerLog(string serverLogPath, long capBytes, ILogger? logger)
    {
        try
        {
            var info = new FileInfo(serverLogPath);
            if (!info.Exists || info.Length <= capBytes)
            {
                return;
            }

            File.Move(serverLogPath, serverLogPath + ".old", overwrite: true);
            logger?.LogInformation(
                "Rolled the {Size:N0}-byte pre-rotation pg.log to pg.log.old (one-time cap; the v6 logging collector owns the server log now)",
                info.Length);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            logger?.LogWarning("Could not roll the oversized pg.log aside — it stays in place until the next start retries: {Message}", ex.Message);
        }
    }

    private string ReadServerLogTail()
    {
        var newest = PickNewestServerLog(_serverLogPath, _dataDirectory);
        if (newest is null)
        {
            return "(no server log written)";
        }

        try
        {
            var lines = File.ReadAllLines(newest);
            var take = Math.Min(40, lines.Length);
            return $"({newest})\n" + string.Join('\n', lines[^take..]);
        }
        catch (IOException ex)
        {
            return $"(could not read server log {newest}: {ex.Message})";
        }
    }

    /// <summary>
    /// The file that best answers "what did Postgres just say?" — the NEWEST-modified of the pg_ctl
    /// <c>pg.log</c> and the v6 logging collector's ring files (<c>&lt;data&gt;\log\*.log</c>). A start that
    /// fails before the collector comes up wrote its reason to pg.log (the newest by definition); a server
    /// that started and then complained wrote to the ring. Null when nothing exists yet. Static for tests.
    /// </summary>
    internal static string? PickNewestServerLog(string serverLogPath, string dataDirectory)
    {
        string? newest = null;
        var newestWrite = DateTime.MinValue;

        void Consider(string path)
        {
            try
            {
                var info = new FileInfo(path);
                if (info.Exists && info.LastWriteTimeUtc > newestWrite)
                {
                    newest = path;
                    newestWrite = info.LastWriteTimeUtc;
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                /* An unreadable candidate just isn't chosen. */
            }
        }

        Consider(serverLogPath);

        try
        {
            var ringDirectory = Path.Combine(dataDirectory, "log");
            if (Directory.Exists(ringDirectory))
            {
                foreach (var file in Directory.GetFiles(ringDirectory, "*.log"))
                {
                    Consider(file);
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Ring directory unreadable — pg.log (if any) still answers. */
        }

        return newest;
    }

    /// <summary>
    /// CREATE DATABASE darling if missing, via the maintenance database. Doubles as the
    /// bootstrap's end-to-end auth check: this is the first real connection with the derived
    /// credential, so a wrong/stale credential fails HERE with a clear Postgres auth error
    /// instead of somewhere inside the migration path.
    /// </summary>
    private async Task EnsureDatabaseAsync(string connectionString, CancellationToken cancellationToken)
    {
        /* The whole unit retries, not just the connect. A backend that loses the post-start race dies
           AFTER authenticating, so the Open succeeds and the first QUERY is what fails — retrying only the
           Open would never have helped. Each attempt gets a fresh connection because the old one's
           connector is dead. */
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await EnsureDatabaseOnceAsync(connectionString, cancellationToken);
                return;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex) when (attempt < FirstConnectionAttempts && IsTransientConnectionFault(ex))
            {
                _logger.LogWarning(
                    "The store dropped the first connection after start ({Message}) — attempt {Attempt} of {Total}. A backend that loses the shared-memory reservation race just after start does this; retrying.",
                    ex.Message, attempt, FirstConnectionAttempts);
                await Task.Delay(s_firstConnectionRetryDelay, cancellationToken);
            }
        }
    }

    private async Task EnsureDatabaseOnceAsync(string connectionString, CancellationToken cancellationToken)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { Database = "postgres" };
        await using var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        using (var exists = new NpgsqlCommand($"SELECT 1 FROM pg_database WHERE datname = '{DatabaseName}'", connection))
        {
            if (await exists.ExecuteScalarAsync(cancellationToken) is not null)
            {
                return;
            }
        }

        _logger.LogInformation("Creating the '{Database}' database", DatabaseName);

        /* Identifier from the class constant, never from input — same interpolation reasoning
           as TimescaleSupport/DarlingRetention. CREATE DATABASE cannot run in a transaction;
           plain ExecuteNonQuery is the correct shape. */
        using var create = new NpgsqlCommand($"CREATE DATABASE {DatabaseName}", connection);
        await create.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Attempts for the first store interaction after a server start, and the pause between them.
    ///
    /// <para><c>pg_ctl -w</c> returns when the postmaster reports it is accepting connections, but on
    /// Windows that is not quite the same as "the next backend will spawn and survive". Every backend is
    /// its own process that must re-reserve the shared-memory region at the postmaster's base address —
    /// the error-487 surface this class already documents at length behind the 1 GB <c>shared_buffers</c>
    /// cap (#1559). Losing that race presents to Npgsql not as a refused connection but as one that
    /// authenticates and then dies on its first query (<c>Exception while writing to stream</c>), which is
    /// exactly what a freshly upgraded cluster produced here while the server itself was demonstrably
    /// healthy and stayed up for minutes afterwards.</para>
    /// </summary>
    private const int FirstConnectionAttempts = 6;
    private static readonly TimeSpan s_firstConnectionRetryDelay = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Whether an exception is a transport-level fault worth retrying (socket reset, stream write failure,
    /// timeout) rather than a definitive answer from a working server (bad password, missing role).
    /// <see cref="PostgresException"/> means the server replied, so it is never transient by this test.
    /// </summary>
    private static bool IsTransientConnectionFault(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is PostgresException)
            {
                return false;
            }

            if (current is SocketException or IOException or TimeoutException)
            {
                return true;
            }
        }

        return exception is NpgsqlException;
    }

    private string ReadStoredPassword()
    {
        if (!File.Exists(_credentialPath))
        {
            throw new InvalidOperationException(
                $"The managed Postgres data directory {_dataDirectory} is initialized but its credential file " +
                $"{_credentialPath} is missing, so the service cannot authenticate. Restore the file from backup, " +
                "or stop the service and move/delete the data directory to re-initialize (destroys collected history), " +
                "or switch to unmanaged mode (postgres.connectionString) against a server you manage.");
        }

        /* Pre-plant guard: never trust a superuser credential file owned by an arbitrary local user
           (SYSTEM / Administrators / the service account only). A file someone else owns may have been
           planted to feed the service a password they know — refuse rather than authenticate with it.

           Two very different causes share this symptom, and the benign one is the common one: after an
           operator re-homes the service to a domain account or gMSA (#1823), the file is still owned by
           the PREVIOUS service account — the runbook's icacls grants change permissions, never ownership.
           The message leads with that case and its runnable fix (ownership to Administrators, which stays
           trusted across any future account change), because the field report behind it read "tampered
           with or pre-planted" and reached for re-initializing a healthy store. The service cannot fix
           this itself: taking ownership needs a privilege a service account is not granted. */
        if (!DarlingFileSecurity.IsTrustedOwner(_credentialPath))
        {
            throw new InvalidOperationException(
                $"The managed Postgres credential file {_credentialPath} is not owned by SYSTEM, Administrators, or the " +
                $"service account{DarlingFileSecurity.DescribeOwnerAndExposure(_credentialPath)}. If you changed the " +
                "service's Log On account, that is the previous service account still owning the file — from an ELEVATED " +
                $"prompt run   takeown /f \"{_credentialPath}\" /a   then   " +
                $"icacls \"{_credentialPath}\" /grant \"{DarlingFileSecurity.ServiceAccountDisplayName}:(F)\"   and start " +
                "the service again. If you did NOT change the service account, treat the file as tampered with or " +
                "pre-planted: investigate, then restore it from backup or re-initialize the data directory (destroys " +
                "collected history).");
        }

        return DarlingSecrets.Unprotect(File.ReadAllText(_credentialPath).Trim());
    }

    /// <summary>
    /// Best-effort restrictive ACL on the credential directory (V8 hardening). A failure is logged
    /// loud but never bricks the service — the fresh-install path (the service account owns the
    /// just-created directory) succeeds, and the trusted-owner read guard is the complementary defense.
    /// </summary>
    private void TryHardenDirectory(string path)
    {
        try
        {
            DarlingFileSecurity.HardenDirectory(path, allowInteractiveTraverse: true);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                "Could not restrict the ACL on {Path}{Detail} ({Message}). The DPAPI credential files it holds may be " +
                "readable by other local users. If the owner is not this service, the re-ACL can never succeed — it " +
                "needs ownership or FullControl — so restarting will not clear this; grant the service account " +
                "FullControl or make it the owner (SYSTEM/Administrators/the service account only).",
                path, DarlingFileSecurity.DescribeOwnerAndExposure(path), ex.Message);
        }
    }

    /// <summary>
    /// Best-effort restrictive ACL on one credential file — same posture as <see cref="TryHardenDirectory"/>,
    /// and the same verify-don't-assume rule the config file already had: the harden is attempted, then the
    /// RESULT is checked, because "we tried" is not the same claim as "the secret is not readable". These blobs
    /// are machine-scoped DPAPI, so read access IS the secret.
    /// </summary>
    private void TryHardenCredentialFile(string path, bool allowInteractiveRead)
    {
        try
        {
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                "Could not restrict the ACL on {Path}{Detail} ({Message}). If the owner is not this service, the " +
                "re-ACL can never succeed — it needs ownership or FullControl — so restarting will not clear this; " +
                "grant the service account FullControl or make it the owner.",
                path, DarlingFileSecurity.DescribeOwnerAndExposure(path), ex.Message);
        }

        if (DarlingFileSecurity.IsReadableByOrdinaryUsers(path))
        {
            _logger.LogCritical(
                "{Path} is READABLE by ordinary local users{Detail}. It holds a machine-scoped DPAPI credential, " +
                "which any local process can decrypt — so read access to this file IS the credential. Remove the " +
                "inherited read access.",
                path, DarlingFileSecurity.DescribeOwnerAndExposure(path));
        }
    }

    private static string ParentOf(string dataDirectory)
    {
        var parent = Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(dataDirectory)));
        if (string.IsNullOrEmpty(parent))
        {
            throw new InvalidOperationException(
                $"postgres.dataDirectory '{dataDirectory}' has no parent directory — use a subdirectory " +
                "(the credential and server log live beside the data directory), not a drive root.");
        }

        return parent;
    }

    /* ================= Opt-in network exposure (darling-network-endpoints) ================= */

    /// <summary>The store's effective bind: loopback-only (the default / a fail-closed degrade) or network-exposed.</summary>
    private enum NetworkMode
    {
        Loopback,
        Exposed,
    }

    /// <summary>
    /// The resolved network state for one start. <see cref="Mode"/> = Exposed carries the validated bind
    /// IP, canonical CIDR, pg_hba role, and generated cert/key paths; Loopback carries a non-null
    /// <see cref="DegradeReason"/> only when it is a FAIL-CLOSED degrade from an INTENDED exposure (null
    /// when the store is simply loopback-by-default). Resolved before start so listen/ssl ride the -o
    /// override and the reconcile can run the disable path for a degrade.
    /// </summary>
    private sealed record NetworkPlan(
        NetworkMode Mode,
        string? ListenIp,
        string? Cidr,
        IReadOnlyList<string>? Roles,
        string? CertPath,
        string? KeyPath,
        string? DegradeReason)
    {
        public static NetworkPlan Loopback(string? degradeReason = null)
            => new(NetworkMode.Loopback, null, null, null, null, null, degradeReason);

        public static NetworkPlan Exposed(string listenIp, string cidr, IReadOnlyList<string> roles, string certPath, string keyPath)
            => new(NetworkMode.Exposed, listenIp, cidr, roles, certPath, keyPath, null);
    }

    /// <summary>
    /// The store's exposure decision after PURE validation (no I/O, no cert gen). <see cref="Exposed"/>=false
    /// with a non-null <see cref="DegradeReason"/> is a FAIL-CLOSED degrade from an intended exposure;
    /// Exposed=false with a null reason is loopback-by-default; Exposed=true carries the validated
    /// ListenIp/Cidr/Role.
    /// </summary>
    internal sealed record NetworkExposureDecision(
        bool Exposed, string? ListenIp, string? Cidr, IReadOnlyList<string>? Roles, string? DegradeReason);

    /// <summary>
    /// PURE validation of postgres.network into a <see cref="NetworkExposureDecision"/> — NO I/O and NO cert
    /// generation (the caller does that on a valid decision), so it is unit-testable without a live server.
    /// Degrades to loopback WITH a reason on: a listen value that is not a parseable IP; a missing/invalid
    /// allowFrom CIDR or an address-family mismatch; a role outside {viewer, admin}; or a cert path
    /// (<paramref name="certPath"/>/<paramref name="keyPath"/>) that contains whitespace (the -o string
    /// cannot nest-quote a space -> PG fail-DEAD). Never a fatal Validate() (D-validate).
    /// </summary>
    internal static NetworkExposureDecision ResolveNetworkExposure(PostgresNetworkConfig? network, string certPath, string keyPath)
    {
        if (network is null || !DarlingNetwork.IsExposedListenAddress(network.Listen))
        {
            /* Loopback by default (no reason) — the byte-for-byte-today path. */
            return new NetworkExposureDecision(false, null, null, null, null);
        }

        var listenRaw = network.Listen!.Trim();
        if (!IPAddress.TryParse(listenRaw, out var listenIp))
        {
            return Degrade(
                $"postgres.network.listen '{network.Listen}' is not a valid IP address (use a specific IP, e.g. 192.168.1.205, or 0.0.0.0 for all interfaces)");
        }

        if (string.IsNullOrWhiteSpace(network.AllowFrom) || !IPNetwork.TryParse(network.AllowFrom.Trim(), out var cidr))
        {
            return Degrade(
                $"postgres.network.allowFrom '{network.AllowFrom}' is not a valid CIDR (e.g. 192.168.1.0/24, with host bits zeroed)");
        }

        if (cidr.BaseAddress.AddressFamily != listenIp.AddressFamily)
        {
            return Degrade(
                $"postgres.network.allowFrom '{network.AllowFrom}' address family does not match listen '{listenRaw}'");
        }

        /* #2665: one rule per role, so an admin Viewer and read-only ones can reach the same store. Null
           when ANY element is unrecognised rather than keeping the ones it understood — a typo in one of two
           roles must not open the store with the other and leave somebody believing both are reachable. */
        var roles = DarlingNetwork.NormalizeNetworkRoles(network.Role);
        if (roles is null || roles.Count == 0)
        {
            return Degrade(
                $"postgres.network.role '{network.Role}' must be 'viewer', 'admin', or both "
                + "(e.g. \"admin,viewer\") — never the superuser");
        }

        /* The cert path rides the -o string, which cannot nest-quote a space -> a spaced path fail-DEADS
           the postmaster. Gate on it (D6) and degrade rather than ever start ssl=on with a broken path. */
        if (ContainsWhitespace(certPath) || ContainsWhitespace(keyPath))
        {
            return Degrade(
                $"the TLS cert path '{certPath}' contains whitespace, which the pg_ctl -o override cannot pass to postgres; " +
                "move postgres.dataDirectory to a space-free path to expose the store over TLS");
        }

        /* Canonical base/prefix form (IPNetwork requires zeroed host bits) for the pg_hba line + firewall. */
        return new NetworkExposureDecision(true, listenIp.ToString(), $"{cidr.BaseAddress}/{cidr.PrefixLength}", roles, null);

        static NetworkExposureDecision Degrade(string reason) => new(false, null, null, null, reason);
    }

    /// <summary>
    /// Validates postgres.network (via <see cref="ResolveNetworkExposure"/>) and, on a valid exposure,
    /// generates the TLS cert — returning the effective <see cref="NetworkPlan"/>. NEVER throws: a cert-gen
    /// failure degrades to loopback with a reason (D6/D-validate).
    /// </summary>
    private NetworkPlan BuildNetworkPlan()
    {
        var certPath = Path.Combine(ParentOf(_dataDirectory), ServerCertFileName);
        var keyPath = Path.Combine(ParentOf(_dataDirectory), ServerKeyFileName);

        var decision = ResolveNetworkExposure(_config.Network, certPath, keyPath);
        if (!decision.Exposed)
        {
            return NetworkPlan.Loopback(decision.DegradeReason);
        }

        try
        {
            EnsureServerCertificate(IPAddress.Parse(decision.ListenIp!), certPath, keyPath);
        }
        catch (Exception ex)
        {
            /* Any cert-gen/write failure degrades to loopback — the network path must never throw out of
               EnsureRunningAsync (its contract is throw => service-exit), Round 4 #3. */
            return NetworkPlan.Loopback($"could not generate/read the store TLS cert ({ex.Message})");
        }

        return NetworkPlan.Exposed(decision.ListenIp!, decision.Cidr!, decision.Roles!, certPath, keyPath);
    }

    private static bool ContainsWhitespace(string value)
    {
        foreach (var c in value)
        {
            if (char.IsWhiteSpace(c))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Idempotent self-signed server cert for store TLS verify-full (D6): reuse the existing PEM pair ONLY if
    /// it loads AND its SAN still covers the current <paramref name="listenIp"/> (delete both to rotate),
    /// else generate a ~10-year cert with BOTH an iPAddress SAN (the listen IP, for verify-full by IP) AND a
    /// dnsName SAN (the machine hostname, so the operator can connect by name if IP-SAN validation ever
    /// disappoints, or when listen=0.0.0.0). Regenerating on an UNREADABLE cert avoids fail-DEADing an
    /// <c>ssl=on</c> start (exposure-adjacent, Round 4 #3); regenerating on a SAN/IP change keeps verify-full
    /// working after a bind-IP change. The private key is written unencrypted PKCS#8 PEM (what postgres reads)
    /// and hardened NON-interactive (SYSTEM + Administrators + service account only) — the postmaster reads
    /// it, never an interactive user.
    /// </summary>
    internal void EnsureServerCertificate(IPAddress listenIp, string certPath, string keyPath)
    {
        var rootPath = RootCertificatePathFor(certPath);

        if (File.Exists(certPath) && File.Exists(keyPath))
        {
            try
            {
                using var existing = X509Certificate2.CreateFromPem(File.ReadAllText(certPath));
                if (CertificateSanCoversIp(existing, listenIp))
                {
                    /* Present + loads + the SAN covers this listen IP -> reuse (delete-to-rotate). Re-harden
                       the key every start (self-healing), same discipline as the credential files. */
                    TryHardenCredentialFile(keyPath, allowInteractiveRead: false);

                    /* #2117: a cert pair WITHOUT root.crt beside it is the legacy single self-signed
                       end-entity shape, whose critical CA=false Basic Constraints Windows' chain engine
                       refuses as its own trust anchor under Npgsql's Root Certificate custom-root trust —
                       verify-full with the printed cert fails on exactly the machines viewers run on.
                       Deliberately NOT auto-rotated: operators who worked around it via the OS trust
                       store have a WORKING setup a silent regeneration would break. Advise instead. */
                    if (!File.Exists(rootPath))
                    {
                        _logger.LogWarning(
                            "The store TLS cert at {Cert} is the legacy single self-signed shape — remote viewers using " +
                            "SSL Mode=VerifyFull with Root Certificate fail certificate-chain validation on Windows " +
                            "(#2117). To rotate to the fixed chain shape: stop the service, delete {Cert} and {Key}, " +
                            "start the service, then re-run --print-viewer-connection and redistribute the new root " +
                            "certificate to viewer machines. Viewers that imported the old cert into the OS trust " +
                            "store keep working until you rotate.",
                            certPath, certPath, keyPath);
                    }

                    return;
                }

                _logger.LogInformation(
                    "Regenerating the store TLS cert: the existing cert's SAN does not cover the current listen IP {Ip} (a listen change needs a fresh cert for verify-full).",
                    listenIp);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    "Regenerating the store TLS cert: the existing {Cert} could not be read ({Message}) — an unreadable cert would fail-dead an ssl=on start.",
                    certPath, ex.Message);
            }

            /* Fall through to regenerate — overwrites the files (the service account owns them). */
        }

        /* #2117: a real two-cert chain — throwaway local CA signs the leaf, the CA key is discarded
           inside Create(), postgres serves leaf+CA, and root.crt is what the operator distributes.
           See StoreTlsCertificates for why the old single self-signed shape failed verify-full. */
        var generated = StoreTlsCertificates.Create(Environment.MachineName, listenIp, ServerCertValidityYears);

        File.WriteAllText(certPath, generated.ServerCertChainPem);
        File.WriteAllText(keyPath, generated.ServerKeyPem);
        File.WriteAllText(rootPath, generated.RootCertPem);
        TryHardenCredentialFile(keyPath, allowInteractiveRead: false);

        _logger.LogInformation(
            "Generated the store TLS chain (CN/DNS SAN {Host}, IP SAN {Ip}, ~{Years}yr): leaf+CA at {Cert}, distributable root at {Root}",
            Environment.MachineName, listenIp, ServerCertValidityYears, certPath, rootPath);
    }

    /// <summary>The distributable root's path — always beside the served cert (#2117). Public-key
    /// material only, so it is deliberately not hardened like the key.</summary>
    internal static string RootCertificatePathFor(string certPath)
        => Path.Combine(Path.GetDirectoryName(certPath) ?? ".", "root.crt");

    /// <summary>
    /// Whether <paramref name="certificate"/> carries an iPAddress SAN equal to <paramref name="listenIp"/>
    /// — the reuse gate for the store TLS cert (verify-full pins the IP SAN). Reads the SAN extension
    /// (OID 2.5.29.17) via <see cref="X509SubjectAlternativeNameExtension.EnumerateIPAddresses"/>. Pure.
    /// </summary>
    internal static bool CertificateSanCoversIp(X509Certificate2 certificate, IPAddress listenIp)
    {
        X509SubjectAlternativeNameExtension? san = null;
        foreach (var extension in certificate.Extensions)
        {
            if (!string.Equals(extension.Oid?.Value, "2.5.29.17", StringComparison.Ordinal))
            {
                continue;
            }

            /* cert.Extensions may hand back a generic X509Extension for the SAN; re-materialize the typed
               view from its raw DER when so, so EnumerateIPAddresses is always available. */
            san = extension as X509SubjectAlternativeNameExtension
                ?? new X509SubjectAlternativeNameExtension(extension.RawData);
            break;
        }

        if (san is null)
        {
            return false;
        }

        foreach (var ip in san.EnumerateIPAddresses())
        {
            if (ip.Equals(listenIp))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Builds the pg_ctl <c>-o</c> runtime-override payload (the INNER string, no wrapping quotes):
    /// always <c>-p {port} -c listen_addresses=127.0.0.1</c> (loopback FIRST, forced every start — <c>-c</c>
    /// outranks postgresql.auto.conf), the network IP appended when exposed, and the ssl trio
    /// (<c>ssl=on</c> + cert/key) when a cert is present. NO single quotes and NO spaces inside any value
    /// (Windows CRT arg parsing treats only double quotes as metacharacters, and postgres re-splits the
    /// -o payload on interior spaces); cert paths are forward-slashed here and space-free-gated by the
    /// caller. Pure + testable (D2/D6).
    /// </summary>
    internal static string BuildServerRuntimeOptions(int port, string? networkListenIp, string? sslCertFile, string? sslKeyFile)
    {
        var builder = new StringBuilder();
        builder.Append("-p ").Append(port);
        builder.Append(" -c listen_addresses=").Append(BuildListenAddresses(networkListenIp));

        if (!string.IsNullOrWhiteSpace(sslCertFile) && !string.IsNullOrWhiteSpace(sslKeyFile))
        {
            builder.Append(" -c ssl=on");
            builder.Append(" -c ssl_cert_file=").Append(ToForwardSlashes(sslCertFile));
            builder.Append(" -c ssl_key_file=").Append(ToForwardSlashes(sslKeyFile));
        }

        return builder.ToString();
    }

    /// <summary>
    /// The effective <c>listen_addresses</c> value for a given network bind — the SINGLE source of truth so
    /// the <c>-o</c> override and the start-log can never drift. Null/empty ⇒ <c>127.0.0.1</c> (loopback
    /// only). Exactly <c>0.0.0.0</c> ⇒ <c>0.0.0.0</c> ALONE: the IPv4 wildcard already covers 127.0.0.1 for
    /// the service's own owner connection, and Windows PG cannot ALSO bind an explicit 127.0.0.1 on the same
    /// port (overlapping IPv4 -> WSAEADDRINUSE). Everything else — a specific IPv4, a specific IPv6, OR the
    /// IPv6 wildcard <c>::</c> — keeps the <c>127.0.0.1,</c> prefix so the hardcoded-IPv4 owner ALWAYS
    /// connects (different families / non-overlapping IPv4 = no WSAEADDRINUSE; <c>::</c> binds IPv6-only on
    /// Windows and would otherwise strand the owner). Pure + testable.
    /// </summary>
    internal static string BuildListenAddresses(string? networkListenIp)
    {
        var listen = networkListenIp?.Trim();
        if (string.IsNullOrEmpty(listen))
        {
            return "127.0.0.1";
        }

        if (string.Equals(listen, "0.0.0.0", StringComparison.Ordinal))
        {
            return "0.0.0.0";
        }

        return $"127.0.0.1,{listen}";
    }

    private static string ToForwardSlashes(string path) => path.Replace('\\', '/');

    /// <summary>
    /// The Darling-managed pg_hba network rule: <c>hostssl darling &lt;role&gt; &lt;cidr&gt; scram-sha-256</c>
    /// — TLS-required (hostssl rejects a non-TLS network client), scoped to the darling database and exactly
    /// the given login role and CIDR; never <c>all</c>, never the superuser (D5/D6). Pure + testable.
    /// </summary>
    internal static string BuildNetworkPgHbaLine(string role, string cidr)
        => $"hostssl {DatabaseName} {role} {cidr} scram-sha-256";

    /// <summary>
    /// The managed pg_hba block for every admitted role (#2665) — one <c>hostssl</c> line each, in the
    /// order <see cref="DarlingNetwork.NormalizeNetworkRoles"/> settled on, so the text is identical however
    /// the field was written and the reconciler does not rewrite-and-reload on a reordering.
    ///
    /// <para>Each line still names exactly one role and one CIDR: never <c>all</c>, never the superuser
    /// (D5/D6). A list admits more roles, it does not widen what any one line grants — and because these
    /// live INSIDE the managed block, tightening <c>allowFrom</c> narrows all of them, which is exactly what
    /// a hand-added second line outside the markers would not do.</para>
    /// </summary>
    internal static string BuildNetworkPgHbaLines(IReadOnlyList<string> roles, string cidr)
        => string.Join("\n", roles.Select(role => BuildNetworkPgHbaLine(role, cidr)));

    /// <summary>
    /// Whether the live pg_hba.conf needs reconciling (darling-network-endpoints): true when there is a rule
    /// to apply (<paramref name="desiredLine"/> non-null = exposing) OR the file still carries a Darling
    /// managed block to remove (<see cref="PgHbaBeginMarker"/> present = a disable edge). FALSE for a store
    /// that was never exposed and is not being exposed — so the reconcile skips reading-normalizing-rewriting
    /// an untouched operator pg_hba (<see cref="ReconcilePgHba"/> normalizes CRLF and trims trailing blanks,
    /// which would otherwise be a spurious write + reload + firewall spawn on a store never opted in). Pure.
    /// </summary>
    public static bool NeedsPgHbaReconcile(string current, string? desiredLine)
        => desiredLine is not null
        || (current is not null && current.Contains(PgHbaBeginMarker, StringComparison.Ordinal));

    /// <summary>
    /// Pure marked-block reconcile for pg_hba.conf (D5): returns <paramref name="existing"/> with the
    /// Darling-managed block (between <see cref="PgHbaBeginMarker"/> and <see cref="PgHbaEndMarker"/>) set
    /// to <paramref name="desiredRuleLine"/> when non-empty, or REMOVED when null/empty (disable).
    /// Every non-marked line is preserved verbatim; a narrowing CIDR REPLACES the block (old removed, new
    /// appended); idempotent (re-running with the same desired yields identical text).
    /// <para>Since #2665 the desired text may be SEVERAL newline-separated rules (one per admitted role,
    /// from <see cref="BuildNetworkPgHbaLines"/>). That needs no code change here — this always replaced the
    /// whole block rather than a line — but it is why the parameter is a block, not a rule.</para>
    /// </summary>
    public static string ReconcilePgHba(string existing, string? desiredRuleLine)
    {
        existing ??= string.Empty;
        var normalized = existing.Replace("\r\n", "\n", StringComparison.Ordinal).Replace('\r', '\n');
        var lines = normalized.Split('\n');

        var kept = new List<string>(lines.Length);
        var insideBlock = false;
        foreach (var line in lines)
        {
            var trimmed = line.Trim();
            if (string.Equals(trimmed, PgHbaBeginMarker, StringComparison.Ordinal))
            {
                insideBlock = true;
                continue;
            }

            if (string.Equals(trimmed, PgHbaEndMarker, StringComparison.Ordinal))
            {
                insideBlock = false;
                continue;
            }

            if (!insideBlock)
            {
                kept.Add(line);
            }
        }

        /* Drop trailing blank lines so re-runs don't accumulate whitespace (idempotency). */
        while (kept.Count > 0 && string.IsNullOrWhiteSpace(kept[^1]))
        {
            kept.RemoveAt(kept.Count - 1);
        }

        var result = new StringBuilder();
        foreach (var line in kept)
        {
            result.Append(line).Append('\n');
        }

        if (!string.IsNullOrWhiteSpace(desiredRuleLine))
        {
            result.Append('\n');
            result.Append(PgHbaBeginMarker).Append('\n');
            result.Append(desiredRuleLine!.Trim()).Append('\n');
            result.Append(PgHbaEndMarker).Append('\n');
        }

        return result.ToString();
    }

    /// <summary>
    /// Reconciles the LIVE server's network access to <paramref name="plan"/> (D5), symmetric and
    /// fail-closed. Rewrites the pg_hba marked block (add the hostssl rule when exposed, remove it when
    /// loopback/degraded), reloads (exit-checked), verifies the intended rule really took via
    /// <c>pg_hba_file_rules</c> (a reload returns 0 for mere SIGHUP delivery even if a malformed file is
    /// rejected), guards an ADOPTED server whose live listen_addresses cannot be changed without a restart
    /// the service will not perform, and best-effort (dis)installs the firewall rule. NEVER throws
    /// (Round 4 #3): its whole body is caught so a reconcile failure logs + degrades, it does not abort the
    /// bootstrap (whose contract is throw => service-exit).
    /// </summary>
    private async Task ReconcileNetworkAsync(
        string binDirectory, NetworkPlan plan, string ownerConnectionString, CancellationToken cancellationToken)
    {
        try
        {
            var exposed = plan.Mode == NetworkMode.Exposed;
            var desiredLine = exposed ? BuildNetworkPgHbaLines(plan.Roles!, plan.Cidr!) : null;

            var hbaPath = Path.Combine(_dataDirectory, "pg_hba.conf");
            if (!File.Exists(hbaPath))
            {
                if (exposed)
                {
                    _logger.LogWarning("pg_hba.conf not found at {Path} — cannot apply the network access rule", hbaPath);
                }

                return;
            }

            var current = await File.ReadAllTextAsync(hbaPath, cancellationToken);

            /* Byte-for-byte today's behavior: a never-exposed store (no managed block) that is not being
               exposed needs NO reconcile — do not even normalize/rewrite pg_hba (ReconcilePgHba would trim
               CRLF + trailing blanks and trigger a spurious write + reload + verify + firewall spawn on a
               store the operator never opted into). A disable EDGE (marker still present) or an exposure to
               apply (desiredLine non-null) both return true and run the full reconcile below. */
            if (!NeedsPgHbaReconcile(current, desiredLine))
            {
                return;
            }

            var updated = ReconcilePgHba(current, desiredLine);
            var changed = !string.Equals(current, updated, StringComparison.Ordinal);
            if (changed)
            {
                await File.WriteAllTextAsync(hbaPath, updated, cancellationToken);
                var reloadPgCtl = Path.Combine(binDirectory, "pg_ctl.exe");
                var (reloadCode, reloadOutput) = await RunToolAsync(
                    reloadPgCtl,
                    $"reload -D \"{_dataDirectory}\"",
                    s_statusTimeout,
                    cancellationToken);
                if (reloadCode != 0)
                {
                    _logger.LogCritical(
                        "pg_ctl reload failed (exit {ExitCode}, {ExitCodeMeaning}) after updating pg_hba.conf — the network access change may not be live: {Output}{Diagnosis}",
                        reloadCode,
                        DarlingToolExitCode.Describe(reloadCode),
                        DarlingToolExitCode.FormatOutput(reloadOutput, reloadCode),
                        DarlingToolExitCode.Diagnose(reloadCode, reloadPgCtl));
                }
            }

            /* Verify the live rules match intent (reload can report success on a rejected malformed file). */
            await VerifyPgHbaAsync(ownerConnectionString, plan, changed, cancellationToken);

            /* Adopted server: we did not start it, so the -o listen_addresses/ssl override never applied.
               If its live listener cannot satisfy the desired exposure, say so loud and do not claim exposed. */
            if (!_startedByThisProcess)
            {
                await GuardAdoptedListenAsync(ownerConnectionString, plan, cancellationToken);
            }

            /* Defense-in-depth firewall rule (the boundary is pg_hba + TLS). CHECKED, never written (#1771):
               an exposed store reports a missing rule, and a disable EDGE (we just removed the block) reports
               the now-stale rule, each naming the elevated command. A store that was never exposed touches the
               firewall not at all (the early return above). */
            await CheckStoreFirewallAsync(exposed, exposed ? plan.Cidr : null, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            /* Shutdown mid-reconcile — the store still runs loopback-safe; nothing to clean up. */
        }
        catch (Exception ex)
        {
            /* Round 4 #3: never let a network reconcile failure abort the bootstrap. */
            _logger.LogCritical(
                "Store network reconcile failed ({Message}) — the store keeps running; verify pg_hba.conf / listen_addresses by hand.",
                ex.Message);
        }
    }

    /// <summary>
    /// Confirms the live pg_hba rules match intent via <c>pg_hba_file_rules</c>: no rule has a parse error,
    /// and a Darling hostssl rule exists for EVERY admitted network role when exposed / for none of them
    /// when loopback (#2665). A mismatch is logged critical (a reload delivers a SIGHUP that Postgres may
    /// then reject). Best-effort — a query failure degrades to a warning, not a throw.
    /// </summary>
    private async Task VerifyPgHbaAsync(string ownerConnectionString, NetworkPlan plan, bool reloaded, CancellationToken cancellationToken)
    {
        var exposed = plan.Mode == NetworkMode.Exposed;
        try
        {
            await using var connection = new NpgsqlConnection(ownerConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using (var errors = new NpgsqlCommand(
                "SELECT count(*) FROM pg_hba_file_rules WHERE error IS NOT NULL", connection))
            {
                var errorCount = Convert.ToInt64(await errors.ExecuteScalarAsync(cancellationToken) ?? 0L);
                if (errorCount > 0)
                {
                    _logger.LogCritical(
                        "pg_hba.conf has {Count} rule(s) Postgres could not parse (pg_hba_file_rules.error) — the live network access may not match intent; check {Path}",
                        errorCount, Path.Combine(_dataDirectory, "pg_hba.conf"));
                    return;
                }
            }

            long present;
            if (exposed)
            {
                /* EVERY admitted role must be live, not just one (#2665): a rule that failed to apply for the
                   second role leaves those clients locked out while the exposure looks healthy.

                   Counting DISTINCT ROLE NAMES rather than matching ROWS, because rows do not answer the
                   question. `count(*) ... WHERE user_name && $2` is satisfied by two rules naming the SAME
                   role, and that is the expected shape on an upgraded box: #2665's own workaround was a
                   hand-added second hostssl line outside the markers, which ReconcilePgHba deliberately
                   preserves. Such a file with a stale viewer line and no live admin rule counts 2 of 2 and
                   passes, which is precisely the failure this check exists to catch. Unnesting and counting
                   distinct names is >= Roles.Count only when every admitted role really has a rule. */
                await using var command = new NpgsqlCommand(
                    "SELECT count(DISTINCT u) FROM pg_hba_file_rules AS r, unnest(r.user_name) AS u "
                    + "WHERE r.type = 'hostssl' AND $1 = ANY(r.database) AND u = ANY($2::text[])",
                    connection);
                command.Parameters.AddWithValue(DatabaseName);
                command.Parameters.AddWithValue(plan.Roles!.ToArray());
                present = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken) ?? 0L);

                if (present < plan.Roles!.Count)
                {
                    _logger.LogCritical(
                        "pg_hba verification: only {Present} of the {Expected} configured role(s) '{Roles}' have a live 'hostssl {Db} <role> {Cidr} scram-sha-256' rule after reload — the store is not accepting network clients as intended",
                        present, plan.Roles!.Count, string.Join(", ", plan.Roles!), DatabaseName, plan.Cidr);
                    return;
                }

                _logger.LogInformation(
                    "Store network access reconciled: exposed to {Cidr} as '{Roles}' over TLS (pg_hba {Verb}).",
                    plan.Cidr, string.Join(", ", plan.Roles!), reloaded ? "reloaded + verified" : "already current");
            }
            else
            {
                /* Disable: no Darling-managed hostssl rule (darling database, viewer/admin) should remain. */
                await using var command = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_hba_file_rules WHERE type = 'hostssl' AND $1 = ANY(database) AND (user_name && ARRAY['viewer','admin'])",
                    connection);
                command.Parameters.AddWithValue(DatabaseName);
                present = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken) ?? 0L);

                if (present > 0)
                {
                    _logger.LogCritical(
                        "pg_hba verification: a Darling hostssl rule is STILL live after the disable reconcile — network auth may not be closed; check {Path}",
                        Path.Combine(_dataDirectory, "pg_hba.conf"));
                    return;
                }

                _logger.LogInformation(
                    "Store network access reconciled: loopback-only (pg_hba {Verb}).",
                    reloaded ? "reloaded + verified" : "already current");
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not verify pg_hba via pg_hba_file_rules ({Message}) — assuming the file write took", ex.Message);
        }
    }

    /// <summary>
    /// For an ADOPTED (not-started-by-us) server the -o listen_addresses override never applied, so its live
    /// binding is whatever its operator started it with. Compares <c>SHOW listen_addresses</c> to intent: an
    /// exposed plan needs the network IP live (else LogCritical — the pg_hba rule is in place but the port is
    /// not bound; the operator must restart their postmaster); a disable on a still-wide adopted listener is
    /// a warning (auth closed, but the port stays bound until the operator restarts — the documented residual).
    /// </summary>
    private async Task GuardAdoptedListenAsync(string ownerConnectionString, NetworkPlan plan, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = new NpgsqlConnection(ownerConnectionString);
            await connection.OpenAsync(cancellationToken);
            await using var command = new NpgsqlCommand("SHOW listen_addresses", connection);
            var liveListen = await command.ExecuteScalarAsync(cancellationToken) as string ?? string.Empty;

            if (plan.Mode == NetworkMode.Exposed)
            {
                if (!LiveListenIncludes(liveListen, plan.ListenIp!))
                {
                    _logger.LogCritical(
                        "Adopted Postgres listen_addresses is '{Live}', which does not include the requested {Ip}. This service will not restart a server it did not start, so the network listener is NOT active — the pg_hba rule is in place but the port is not bound. Restart your postmaster to apply, or let the service own the cluster.",
                        liveListen, plan.ListenIp);
                }
            }
            else if (LiveListenHasNonLoopback(liveListen))
            {
                _logger.LogWarning(
                    "Adopted Postgres listen_addresses is '{Live}' (wider than loopback). The network AUTH path is closed (pg_hba rule removed), but the TCP port stays bound until you restart your postmaster.",
                    liveListen);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not read the adopted server's listen_addresses ({Message})", ex.Message);
        }
    }

    private static bool LiveListenIncludes(string liveListenAddresses, string ip)
    {
        foreach (var token in liveListenAddresses.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (string.Equals(token, ip, StringComparison.Ordinal)
                || string.Equals(token, "*", StringComparison.Ordinal)
                || string.Equals(token, "0.0.0.0", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool LiveListenHasNonLoopback(string liveListenAddresses)
    {
        foreach (var token in liveListenAddresses.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var isLoopback =
                string.Equals(token, "localhost", StringComparison.OrdinalIgnoreCase)
                || (IPAddress.TryParse(token, out var ip)
                    && ip.AddressFamily == AddressFamily.InterNetwork
                    && ip.GetAddressBytes()[0] == 127);
            if (!isLoopback)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The scoped store firewall rule name (idempotent by DisplayName), port-specific.
    /// <c>internal static</c> so the elevated <c>--configure-firewall</c> verb creates the SAME rule this
    /// checks for, from darling.json alone and without a running store (#1771).</summary>
    internal static string StoreFirewallRuleName(int port) => $"PerformanceMonitor Darling store (port {port})";

    /// <summary>
    /// PowerShell single-quoted literal. Inside <c>'…'</c> PowerShell expands nothing — no <c>$</c>, no
    /// backtick escapes, no subexpressions — so the ONE metacharacter is the quote itself, escaped by
    /// doubling it. Every value the firewall builders interpolate goes through this (#1646): the builders
    /// are then safe no matter what a caller hands them, INDEPENDENT of the caller-side CIDR parse that is
    /// the primary fix. The rule names are internally generated and contain no quotes, so quoting them
    /// leaves the emitted command byte-for-byte what it has always been.
    /// <para><c>internal</c> so <see cref="DarlingFirewallCheck.BuildProbeCommand"/> escapes the rule name
    /// through this same one helper rather than growing a second, subtly different quoting rule (#1771).</para>
    /// </summary>
    internal static string SingleQuotedPowerShell(string value)
        => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    /// <summary>Idempotent-named enable command (remove-by-name then add) — the exact scoped command the docs
    /// lead with (D1). Pure + testable.</summary>
    internal static string BuildFirewallEnableCommand(string ruleName, int port, string remoteCidr)
        => $"Remove-NetFirewallRule -DisplayName {SingleQuotedPowerShell(ruleName)} -ErrorAction SilentlyContinue; " +
           $"New-NetFirewallRule -DisplayName {SingleQuotedPowerShell(ruleName)} -Direction Inbound -Action Allow -Protocol TCP -LocalPort {port} -RemoteAddress {SingleQuotedPowerShell(remoteCidr)} | Out-Null";

    /// <summary>
    /// Idempotent-named disable command (remove-by-name). Pure + testable.
    /// Deliberately NOT `-ErrorAction SilentlyContinue`: that suppresses the error OUTPUT but the cmdlet
    /// failure still makes powershell.exe exit 1, so removing an already-absent rule — the COMMON no-op
    /// (the rule was never created, or a prior shutdown already removed it) — logged
    /// "Could not remove the firewall rule automatically (exit 1: )" with an EMPTY message on every
    /// shutdown, on every host. "Rule absent" IS this command's desired end state, so ObjectNotFound is
    /// swallowed and we exit 0; any OTHER failure (e.g. access denied, which leaves a stale allow rule
    /// behind and is worth knowing about) rethrows so the caller still warns — with a real message.
    /// The catch alone is not enough: a caught error leaves the exit state non-zero, hence the exit 0.
    /// </summary>
    internal static string BuildFirewallDisableCommand(string ruleName)
        => $"try {{ Remove-NetFirewallRule -DisplayName {SingleQuotedPowerShell(ruleName)} -ErrorAction Stop }} " +
           $"catch {{ if ($_.CategoryInfo.Category -ne 'ObjectNotFound') {{ throw }} }}; exit 0";

    /// <summary>
    /// Removes EVERY rule matching a DisplayName wildcard — how the elevated verb clears one surface's rules
    /// for ALL ports before ensuring the current one (#1771). The port lives in the rule NAME, so a port change
    /// does not update a rule, it strands the old one as an inbound allow rule on a port nothing serves; only a
    /// wildcard sweep can reach it.
    /// <para>Shaped exactly like <see cref="BuildFirewallDisableCommand"/> rather than with
    /// <c>-ErrorAction SilentlyContinue</c>, and for the same reason: SilentlyContinue hides the error TEXT but
    /// still exits 1, so a genuine failure (access denied, leaving a stale allow rule behind) would report as
    /// "exit 1:" with an EMPTY message — the trap that builder documents. Catching ObjectNotFound and
    /// rethrowing anything else keeps a real failure loud and a no-op quiet. A wildcard matching nothing is not
    /// an error here anyway (verified on Windows 11 26200, where the exact-name form DOES raise
    /// ObjectNotFound), but the shape costs nothing and does not depend on that.</para>
    /// <para>This is a COMPLETE command, not a fragment: the trailing <c>exit 0</c> means it must be run as its
    /// own step and never string-concatenated ahead of another command, which would terminate the shell before
    /// that command ran. Callers MUST pass a wildcard from
    /// <see cref="DarlingFirewallCheck.SurfaceRuleWildcard"/>, never an operator-supplied string.</para>
    /// </summary>
    internal static string BuildFirewallSweepCommand(string displayNameWildcard)
        => $"try {{ Remove-NetFirewallRule -DisplayName {SingleQuotedPowerShell(displayNameWildcard)} -ErrorAction Stop }} " +
           $"catch {{ if ($_.CategoryInfo.Category -ne 'ObjectNotFound') {{ throw }} }}; exit 0";

    /// <summary>Last (rule, verdict) reported, so a repeated network reconcile restates a steady firewall
    /// state at most once (<see cref="DarlingFirewallCheck.ShouldReport"/>).</summary>
    private string? _lastFirewallRule;
    private FirewallRuleVerdict? _lastFirewallVerdict;

    /// <summary>
    /// Read-only firewall VERIFICATION for the store's scoped rule (#1771). This used to add/remove the rule
    /// itself, which the service account cannot do — the identical structural bug the MCP and web hosts had,
    /// fixed the same way: the elevated installer creates the rule, and the running service only reports what
    /// it finds. The firewall is defense-in-depth, NOT the boundary (pg_hba + TLS are), so a check that cannot
    /// run NEVER fails startup.
    /// </summary>
    [SupportedOSPlatform("windows")]
    private async Task CheckStoreFirewallAsync(bool exposed, string? cidr, CancellationToken cancellationToken)
        => (_lastFirewallRule, _lastFirewallVerdict) = await DarlingFirewallCheck.CheckAsync(
            StoreFirewallRuleName(_config.Port), _config.Port, exposed, cidr,
            _lastFirewallRule, _lastFirewallVerdict, _logger, cancellationToken);

    /// <summary>
    /// Runs a PowerShell command with captured, interleaved stdout+stderr and a timeout. <c>internal</c>
    /// so the elevated <c>--configure-firewall</c> verb and the runtime firewall CHECK
    /// (<see cref="DarlingFirewallCheck"/>) reuse it instead of duplicating it — the command shapes are shared
    /// via the pure <see cref="BuildFirewallEnableCommand"/>/<see cref="BuildFirewallDisableCommand"/>
    /// builders.
    /// <paramref name="timeout"/> is optional and defaults to the shared status timeout
    /// (<see cref="s_statusTimeout"/>); the <c>--configure-network</c> wizard passes a longer one for a
    /// service restart, which routinely exceeds the status budget. Existing callers are unaffected.
    /// </summary>
    internal static async Task<(int ExitCode, string Output)> RunPowerShellAsync(
        string command, CancellationToken cancellationToken, TimeSpan? timeout = null)
    {
        /* Full path (not the bare name) — avoid a PATH/CWD hijack of "powershell.exe", matching the house
           style of full-pathing every PG tool. */
        var powershellPath = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.System), "WindowsPowerShell", "v1.0", "powershell.exe");

        using var process = new Process();
        process.StartInfo = new ProcessStartInfo
        {
            FileName = powershellPath,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        process.StartInfo.ArgumentList.Add("-NoProfile");
        process.StartInfo.ArgumentList.Add("-NonInteractive");
        process.StartInfo.ArgumentList.Add("-Command");
        process.StartInfo.ArgumentList.Add(command);

        var output = new StringBuilder();
        var outputLock = new object();
        process.OutputDataReceived += (_, e) => { if (e.Data is not null) { lock (outputLock) { output.AppendLine(e.Data); } } };
        process.ErrorDataReceived += (_, e) => { if (e.Data is not null) { lock (outputLock) { output.AppendLine(e.Data); } } };

        if (!process.Start())
        {
            return (-1, "could not start powershell.exe");
        }

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout ?? s_statusTimeout);
        try
        {
            await process.WaitForExitAsync(timeoutSource.Token);
        }
        catch (OperationCanceledException)
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
                /* Exited between the timeout and the kill. */
            }

            cancellationToken.ThrowIfCancellationRequested();
            string soFar;
            lock (outputLock) { soFar = output.ToString().Trim(); }
            return (-1, $"timed out: {soFar}");
        }

        lock (outputLock)
        {
            return (process.ExitCode, output.ToString().Trim());
        }
    }

    /// <summary>
    /// Applies the optional per-invocation environment and working directory shared by both process
    /// runners. Values are ADDED to the inherited environment rather than replacing it — a PG tool still
    /// needs PATH, TEMP and the rest — and an existing name is overwritten so a caller's PGPASSFILE always
    /// wins over one that happens to be set for the service account.
    /// </summary>
    private static void ApplyProcessEnvironment(
        ProcessStartInfo startInfo, IReadOnlyDictionary<string, string>? environment, string? workingDirectory)
    {
        if (environment is not null)
        {
            foreach (var pair in environment)
            {
                startInfo.Environment[pair.Key] = pair.Value;
            }
        }

        if (!string.IsNullOrWhiteSpace(workingDirectory))
        {
            startInfo.WorkingDirectory = workingDirectory;
        }
    }

    /// <summary>
    /// Runs one PG tool with captured, interleaved stdout+stderr and a hard timeout. The
    /// service's stopping token cancels a bootstrap mid-flight; the shutdown stop path passes
    /// CancellationToken.None because its token is by definition already cancelled.
    ///
    /// <para><paramref name="environment"/> carries per-invocation variables the tool needs — today
    /// <c>PGPASSFILE</c> for the store-upgrade path (#1706), whose libpq-based tools must authenticate
    /// without a password prompt. <c>internal</c> for the same reason: <see cref="DarlingStoreUpgrade"/>
    /// runs the same class of tool and must not grow a second process runner with its own timeout,
    /// cancellation and capture semantics.</para>
    /// </summary>
    internal static async Task<(int ExitCode, string Output)> RunToolAsync(
        string exePath,
        string arguments,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        IReadOnlyDictionary<string, string>? environment = null,
        string? workingDirectory = null)
    {
        if (!File.Exists(exePath))
        {
            throw new InvalidOperationException(
                $"{exePath} is missing — the pg-runtime directory is incomplete. Rebuild pg-runtime.zip with " +
                "Darling\\tools\\fetch-pg-runtime.ps1 and redeploy (deleting the pg-runtime directory makes the " +
                "service re-extract the zip on its next start).");
        }

        using var process = new Process();
        process.StartInfo = new ProcessStartInfo
        {
            FileName = exePath,
            Arguments = arguments,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };

        ApplyProcessEnvironment(process.StartInfo, environment, workingDirectory);

        var output = new StringBuilder();
        var outputLock = new object();
        process.OutputDataReceived += (_, e) =>
        {
            if (e.Data is not null)
            {
                lock (outputLock) { output.AppendLine(e.Data); }
            }
        };
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data is not null)
            {
                lock (outputLock) { output.AppendLine(e.Data); }
            }
        };

        if (!process.Start())
        {
            throw new InvalidOperationException($"Could not start {exePath}.");
        }

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        try
        {
            await process.WaitForExitAsync(timeoutSource.Token);
        }
        catch (OperationCanceledException)
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
                /* Exited between the timeout and the kill. */
            }

            cancellationToken.ThrowIfCancellationRequested();

            string capturedSoFar;
            lock (outputLock) { capturedSoFar = output.ToString().Trim(); }
            throw new TimeoutException(
                $"{Path.GetFileName(exePath)} {arguments} did not finish within {timeout.TotalSeconds:0}s. Output so far:\n{capturedSoFar}");
        }

        lock (outputLock)
        {
            return (process.ExitCode, output.ToString().Trim());
        }
    }

    /// <summary>
    /// Runs the ONE tool whose spawned server outlives it — pg_ctl start — with NO output
    /// redirection, waiting on process exit alone. Redirecting here is a guaranteed hang on
    /// SUCCESS: pg_ctl launches postgres.exe with handle inheritance, the postmaster keeps the
    /// pipe write-ends open for its whole lifetime, so the pipes never reach EOF — and
    /// <see cref="Process.WaitForExitAsync(CancellationToken)"/> waits for redirected output to
    /// drain (dotnet/runtime#42556), so a healthy start times out after pg_ctl itself has long
    /// exited. Caught live by the gated bootstrap E2E. The lost pg_ctl console text is the
    /// throwaway "waiting for server to start..." narration; the real failure story is the -l
    /// server log, which the caller surfaces via <see cref="ReadServerLogTail"/>. initdb and
    /// pg_ctl stop/status stay on <see cref="RunToolAsync"/> — nothing they spawn survives them,
    /// so their pipes close and their captured output is worth having.
    ///
    /// <para><b>pg_upgrade rides this runner too</b> (#1706), for exactly the same reason: it starts each
    /// cluster's postmaster through pg_ctl, so redirecting its output inherits the handles into servers that
    /// hold them open for their lifetime. Its diagnostics come from the log files it writes under the new
    /// data directory, which is why they are read on failure instead of captured here.</para>
    /// </summary>
    internal static async Task<int> RunDetachingToolAsync(
        string exePath,
        string arguments,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        IReadOnlyDictionary<string, string>? environment = null,
        string? workingDirectory = null)
    {
        if (!File.Exists(exePath))
        {
            throw new InvalidOperationException(
                $"{exePath} is missing — the pg-runtime directory is incomplete. Rebuild pg-runtime.zip with " +
                "Darling\\tools\\fetch-pg-runtime.ps1 and redeploy (deleting the pg-runtime directory makes the " +
                "service re-extract the zip on its next start).");
        }

        using var process = new Process();
        process.StartInfo = new ProcessStartInfo
        {
            FileName = exePath,
            Arguments = arguments,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        ApplyProcessEnvironment(process.StartInfo, environment, workingDirectory);

        if (!process.Start())
        {
            throw new InvalidOperationException($"Could not start {exePath}.");
        }

        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        try
        {
            await process.WaitForExitAsync(timeoutSource.Token);
        }
        catch (OperationCanceledException)
        {
            /* A start stuck past pg_ctl's own -w -t budget is a real failure; the tree kill
               reaps the half-started postmaster while it is still pg_ctl's child. */
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
                /* Exited between the timeout and the kill. */
            }

            cancellationToken.ThrowIfCancellationRequested();
            throw new TimeoutException(
                $"{Path.GetFileName(exePath)} {arguments} did not finish within {timeout.TotalSeconds:0}s.");
        }

        return process.ExitCode;
    }
}
