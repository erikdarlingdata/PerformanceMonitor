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
///
/// <para>Managed mode is Windows-only (DPAPI credentials, ACL hardening, the Windows service
/// lifecycle), annotated on the lifecycle members rather than the type: the pure statics some
/// cross-platform paths reuse — the role-name consts, <see cref="CertificateSanCoversIp"/> (the web
/// dashboard's SAN check shares it) — are platform-neutral, and a member cannot widen a type-level
/// platform annotation.</para>
/// </summary>
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
    /// Marker for the v8 hardware-sizing block (#2845). The EIGHTH block, and the first keyed on
    /// something other than a version: v1-v7 each ask "is this marker absent?", a question answered once
    /// and then never again, so every one of them re-derives only on a FORMULA change. Nothing re-derived
    /// on a HARDWARE change, and a resized host kept its old sizing indefinitely — observed on all three
    /// monitoring boxes after a 16 GB -> 31.5 GB resize, which left <c>effective_cache_size</c> at
    /// 11.86 GB (75% of the RAM the box no longer had).
    ///
    /// <para>This block keys on <see cref="ConfHardwareFingerprintPrefix"/> — a line recording the
    /// DERIVATION INPUTS the settings below it were written under — so the question becomes "were these
    /// derived under the hardware we are running on NOW?". The two triggers are orthogonal and compose: a
    /// version marker heals "we changed our mind about the formula", this heals "the machine changed
    /// underneath it". Precedence is therefore never ambiguous even though both write the same file:
    /// v8 re-states the SAME formulas (it calls <see cref="DeriveMemorySettings"/> and
    /// <see cref="DeriveWorkerSettings"/>, the same helpers the version blocks use), so it can only ever
    /// differ from them by being FRESHER, never by disagreeing.</para>
    /// </summary>
    public const string ConfMarkerV8 = "# Managed by PerformanceMonitor Darling (v8 hardware sizing) -- do not remove this block";

    /// <summary>
    /// DEFENCE IN DEPTH for the naive-UTC/timestamptz split, not the fix for it. Every timestamp column in
    /// the store is <c>timestamp without time zone</c> holding naive UTC, but initdb takes
    /// <c>timezone</c> from the host OS — so a managed store on a Windows box in New York runs its sessions
    /// at <c>America/New_York</c>, and anything that compares a naive column against <c>now()</c> has the
    /// naive side converted at that zone. Pinning the session zone to UTC makes that conversion the identity
    /// it was always assumed to be, and makes <c>timestamptz::text</c> (TimescaleDB's catalog views, psql
    /// output) render on the same clock as the collected data instead of four hours off it.
    ///
    /// <para>It does NOT replace binding those bounds as parameters: it reaches MANAGED stores only, and a
    /// bring-your-own store keeps whatever zone its owner built it with. <c>StoreSqlClockDisciplineTests</c>
    /// is what actually holds the predicates; this is the belt behind it.</para>
    ///
    /// <para><c>timezone</c> is a SIGHUP-context setting and this append runs before pg_ctl start, so it
    /// takes effect on the very start that writes it — the log-rotation story, not the shared_buffers one.
    /// Existing clusters gain it by the marker being absent, which is what carries this to the stores
    /// already in the field rather than only to a fresh initdb.</para>
    /// </summary>
    public const string ConfMarkerV9 = "# Managed by PerformanceMonitor Darling (v9 session time zone) -- do not remove this block";

    /// <summary>
    /// Marker for the v10 message-locale block (#3053): pin <c>lc_messages</c> so the server writes its own
    /// messages — <b>and its severity labels</b> — untranslated. A TENTH independently versioned block, for
    /// the same reason v9 is separate from v3: an existing cluster gains it by the marker being absent on its
    /// next service-owned start, which is what carries the pin to the stores already in the field.
    ///
    /// <para><b>The severity label is the part that bites.</b> PostgreSQL translates the label as well as the
    /// body — a store running a German catalogue writes <c>FEHLER:</c> where an English one writes
    /// <c>ERROR:</c>. <see cref="StoreLogClassifier"/> anchors on that field and its residue class is gated on
    /// <see cref="StoreLogClassifier.IsAtLeastWarning"/>, so a token it does not recognise cannot reach
    /// <c>unclassified</c>-retained and lands in <c>routine</c>, counted with its text dropped. The design's
    /// property is that a rule the table forgot costs a heading and never a row; under a translated label it
    /// costs the row.</para>
    ///
    /// <para><b>What this block adds over the initdb line, stated precisely, because it is less than it
    /// looks.</b> <see cref="InitializeClusterAsync"/> already passes <c>--locale=C</c>, and initdb templates
    /// the resolved <c>lc_*</c> values into the conf it generates — so a cluster this build initialized was
    /// never running a translated catalogue, and the plain default-initdb exposure does not apply to it. This
    /// block buys three things that argument does not. It makes the parser's dependency EXPLICIT and testable,
    /// so changing <c>--locale</c> for the collation reason it exists for cannot silently re-localise messages
    /// as a side effect. It reaches data directories this build's initdb did not create — built before that
    /// argument landed, restored, adopted, or hand-initialized — which is the population the marker-absent
    /// heal exists for. And it wins over the generated line by last-occurrence, so an edited value is
    /// corrected rather than inherited.</para>
    ///
    /// <para><b>What it does not reach.</b> <c>postgresql.auto.conf</c> is read after <c>postgresql.conf</c>,
    /// so an <c>ALTER SYSTEM SET lc_messages</c> still wins; this is diagnostic fidelity, not a security
    /// boundary, so it is not forced through the <c>-o</c> runtime override the way <c>listen_addresses</c>
    /// is. And it reaches MANAGED stores only, while the classifier does not: <c>StoreLogSweep</c> runs on
    /// every store shape, so a bring-your-own store's log is classified under whatever locale its owner gave
    /// it.</para>
    ///
    /// <para><c>C</c> rather than <c>en_US.UTF-8</c>: <c>C</c> is guaranteed present with no locale
    /// installed on the host, and it is the locale under which PostgreSQL emits its untranslated message
    /// catalogue. Nothing in this product renders PostgreSQL message text to an end user in their own
    /// language — it goes to parsers and to English-throughout operator diagnostics — so the pin costs
    /// nothing it does not buy back. It is <c>lc_messages</c> ALONE: <c>lc_monetary</c>, <c>lc_numeric</c>
    /// and <c>lc_time</c> govern how the server renders values the product reads as typed parameters, not as
    /// text, and would be a behaviour change with no defect behind it.</para>
    ///
    /// <para><c>lc_messages</c> is a SIGHUP-context setting and this append runs before pg_ctl start, so it
    /// takes effect on the very start that writes it — the v9 story. The one exception is the adopted-listener
    /// path in <see cref="EnsureRunningAsync"/>: when a postmaster is already running this service neither
    /// stops nor signals it, so the pin waits for the next service-owned start. Nothing here reloads, and the
    /// class's only <c>pg_ctl reload</c> is gated on pg_hba.conf changing, so it cannot be relied on to
    /// carry this.</para>
    /// </summary>
    public const string ConfMarkerV10 = "# Managed by PerformanceMonitor Darling (v10 message locale) -- do not remove this block";

    /// <summary>
    /// Marker for the v11 job-execution-logging block (#3175): pin
    /// <c>timescaledb.enable_job_execution_logging</c> on, so <c>timescaledb_information.job_history</c>
    /// records one row per background-job run. An ELEVENTH independently versioned block, and it exists for
    /// a reason none of the others do — the setting was originally written into the <b>v1</b> block (#1681),
    /// the one block whose marker is already present on every pre-existing cluster. <see cref="EnsureConfAppended"/>
    /// skips a block whose marker it finds, so on any cluster that existed before #1681 the append that would
    /// have carried the GUC never ran, and the setting never arrived. Every setting after v1 got its own
    /// marker for exactly this reason; this one did not.
    ///
    /// <para><b>Why that is worse than a plain missing setting.</b> A maximum over an empty
    /// <c>job_history</c> returns ZERO ROWS, and zero rows reads as <i>"no run exceeded the line"</i> rather
    /// than as <i>"this instrument is off"</i> — an absence that reads as health, which is the failure shape
    /// the rest of this codebase guards against explicitly. Measured on two field stores running the same
    /// binary: the cluster initdb'd 2026-07-17 carried all ten markers, <b>no GUC line</b>, an effective
    /// <c>off</c> with <c>source = default</c>, and ONE history row for 110 jobs; the cluster initdb'd
    /// 2026-08-17 carried the line and 39,020 rows. Nothing in the first store's answer distinguished it
    /// from a clean one, which silently scoped every <c>job_history</c>-derived conclusion to the newer
    /// store.</para>
    ///
    /// <para><b>MOVED out of v1 rather than duplicated into v11.</b> Leaving a copy in
    /// <see cref="BuildConfAppend"/> would cost nothing at runtime (identical value, last occurrence wins)
    /// and would leave the repository asserting this setting in the block that provably cannot deliver it —
    /// which is the reading #1681 made, and the one the next person would copy. The GUC is stated once, in
    /// the block that heals. Two pins hold it: <c>TheJobExecutionLoggingGuc_IsInV11AndNotInTheUnhealableV1Block</c>
    /// asserts both halves of the move, and <c>ConfV1Block_ContentIsFrozen_ANewSettingNeedsItsOwnMarker</c>
    /// fails on any setting added to the v1 builder — the pin whose absence let this through.</para>
    ///
    /// <para><b>DELIBERATELY NOT A WIDENING OF THE V1 MARKER, and the harm is measured rather than
    /// asserted.</b> Making the v1 check ask "is the GUC line present?" instead of "is the v1 marker
    /// present?" would re-append the WHOLE v1 block to every pre-existing cluster, and that block is
    /// shared. <c>shared_preload_libraries</c> is list-valued and the last occurrence REPLACES the list
    /// rather than extending it: measured on TimescaleDB 2.30.0/PG17, a conf carrying an operator's
    /// <c>'timescaledb,pg_stat_statements'</c> came back up serving <c>'timescaledb'</c> alone once the v1
    /// block was appended behind it. <c>listen_addresses</c> would likewise re-assert loopback over a
    /// conf-configured exposure, and <c>port</c> would override a hand-edited one. A separate marker
    /// re-applies none of it.</para>
    ///
    /// <para><b>Reload semantics, measured rather than assumed.</b> The GUC's context is <c>sighup</c>
    /// (measured on 2.30.0: <c>pg_settings.context = 'sighup'</c>, and a conf append plus one
    /// <c>pg_reload_conf()</c> moved it from <c>off</c>/<c>source = default</c> to <c>on</c>/<c>source =
    /// configuration file</c>). This append runs BEFORE pg_ctl start, so on a service-owned start no reload
    /// is needed and the setting is live on the very start that writes it. The exception is the
    /// adopted-listener path in <see cref="EnsureRunningAsync"/>: a postmaster already running is neither
    /// stopped nor signalled, so there the heal waits for the next service-owned start. <b>No reload is
    /// issued and that is a decision, not an omission</b> — v9 and v10 carry the same exposure and the same
    /// choice, signalling a server this service did not start is the same class of act as stopping one, and
    /// a reload would apply this block while leaving the restart-only settings that the SAME heal may have
    /// just appended (v2/v3/v4/v5/v7) inert. A half-applied conf is worse than a consistently deferred one:
    /// it removes the operator's ability to reason about the server's state from "did the service own this
    /// start".</para>
    ///
    /// <para><b>Healing starts logging; it does not recover history.</b> A store that has been running
    /// without the GUC wrote no per-run rows and there is nothing to backfill — TimescaleDB does not retain
    /// what it was told not to record. So the honest outcome is "logging starts now", which the append's log
    /// line states rather than implies, and a <c>job_history</c> window that predates the heal stays empty
    /// on purpose. <c>timescaledb_information.job_stats</c> remains the surface that reports on an
    /// untouched store; it is maintained unconditionally, which is why every shipped read uses it.</para>
    ///
    /// <para><b>What this cannot beat.</b> <c>postgresql.auto.conf</c> is read after
    /// <c>postgresql.conf</c>, so an <c>ALTER SYSTEM SET timescaledb.enable_job_execution_logging = off</c>
    /// still wins — measured: with the appended block last in <c>postgresql.conf</c> the effective value was
    /// <c>off</c> with <c>sourcefile</c> naming <c>postgresql.auto.conf</c>. That is precisely why the
    /// read-side check reports the EFFECTIVE value and its source rather than the presence of this marker: a
    /// marker says the product did its part, and only the effective value says the instrument is on.</para>
    /// </summary>
    public const string ConfMarkerV11 = "# Managed by PerformanceMonitor Darling (v11 job execution logging) -- do not remove this block";

    /// <summary>
    /// Marker for the v12 WAL-sizing block (#3802): derive <c>max_wal_size</c> (and <c>min_wal_size</c> beside
    /// it) from the headroom on the volume that holds the data directory, and re-derive it on every
    /// service-owned start. A TWELFTH independently versioned block, and after v8 the second one keyed on
    /// something other than its own marker's absence — see <see cref="ConfWalSizingStampPrefix"/>.
    ///
    /// <para><b>What was in force before this, stated precisely, because the issue's own title gets it
    /// wrong.</b> #3802 says every managed store "runs PostgreSQL's default 1 GB". It does not: the v4 block
    /// (<see cref="BuildWriteThroughputConfAppend"/>) has written <c>max_wal_size = 4GB</c> as a fixed
    /// constant since the 24-server bootstrap incident, so a healed managed store sat at 4 GB, not 1 GB. The
    /// defect is real all the same — 4 GB is a constant chosen for a bootstrap burst and sized to no property
    /// of the box it runs on — and the mechanism the issue measured is exactly the one a constant cannot
    /// answer. This block SUPERSEDES v4's line by last-occurrence-wins, the way v5 supersedes v3's
    /// <c>shared_buffers</c>; v4 is not edited and keeps its <c>max_connections</c>.</para>
    ///
    /// <para><b>The measured mechanism (a production store, 2026-09-20 15:20–15:40Z, the #3745 exhibit).</b>
    /// One continuous-aggregate refresh wrote 8.3 M rows in a single transaction, ran through
    /// <c>max_wal_size</c> mid-checkpoint, forced a second WAL-triggered checkpoint (199 s), and for four
    /// minutes every heavy read on the store starved behind it — the fleet overview cancelled twice, four
    /// alert reads cancelled, a parallel worker failed to spawn (<c>could not reserve shared memory
    /// region</c>). The maintainer set <c>max_wal_size = 16GB</c> out of band on all three production stores
    /// (reload-only, verified) and ruled that the product should author it — <i>"as long as the box can
    /// afford it. so dynamic i guess."</i> — which is the whole brief: a derivation, not a constant.</para>
    ///
    /// <para><b>The formula.</b> <c>max_wal_size = clamp(free / 8, 1 GB, 16 GB)</c>, floored to the
    /// power-of-two ladder 1, 2, 4, 8, 16 GB, where <c>free</c> is the space available on the data volume at
    /// ensure time; <c>min_wal_size = max(80 MB, max_wal_size / 4)</c>. Both written in whole megabytes in
    /// PostgreSQL's unit grammar. The pieces:
    /// <list type="bullet">
    /// <item><b>/ 8</b> — the WAL directory must never be the thing that fills the data volume.
    ///   <c>max_wal_size</c> is a SOFT limit (PostgreSQL's documentation: <i>"WAL size can exceed max_wal_size
    ///   under special circumstances, such as heavy load"</i>), and the load that makes it matter here — a
    ///   compression-heavy TimescaleDB store whose materializations write millions of rows per transaction —
    ///   is precisely the heavy load that overshoots it. Checkpoints on that store are the write amplifier,
    ///   and the WAL ceiling is what spaces them. An eighth of what is free leaves seven eighths for the
    ///   overshoot, for the store's own growth between retention sweeps, and for the disk-pressure self-alert
    ///   to fire before anything is actually full.</item>
    /// <item><b>16 GB ceiling</b> — the maintainer's chosen ceiling from #3802, not a measured optimum: it is
    ///   the figure applied out of band to the store that exhibited the mechanism, and the point past which
    ///   checkpoint spacing was judged to stop buying anything for this write shape. A larger
    ///   <c>max_wal_size</c> also lengthens crash recovery on the bundled store — more WAL to replay — which
    ///   is the price of fewer forced checkpoints, so the ceiling is also where that price stops being worth
    ///   paying.</item>
    /// <item><b>1 GB floor</b> — PostgreSQL's own default. A volume with under 16 GB free cannot afford more
    ///   WAL than the server would have used anyway, and the log line says so rather than landing there
    ///   silently. On a volume with under 32 GB free the floor and the 2 GB rung land BELOW v4's fixed 4 GB;
    ///   that is deliberate. v4 sized for a burst on a box it never measured, and the block that does measure
    ///   the box heals it down.</item>
    /// <item><b>min_wal_size at a quarter</b> — the size below which PostgreSQL recycles old segments rather
    ///   than removing them, so recycling keeps pace with a raised ceiling instead of paying segment creation
    ///   on every burst. Floored at PostgreSQL's 80 MB default; on the ladder that floor is never the binding
    ///   term (the smallest rung yields 256 MB), and it is coded anyway so the formula is true of itself and
    ///   not merely of the ladder.</item>
    /// </list></para>
    ///
    /// <para><b>Why a power-of-two ladder and not the raw quotient.</b> This block heals on a change in the
    /// box, like v8, and v8's lesson applies with more force: a check that runs on every start and compares
    /// an exact figure turns every wobble in that figure into a fresh block appended to the file, forever.
    /// Free disk is not a wobble — it moves by gigabytes between any two starts on a store that compresses
    /// and drops chunks — so the raw quotient would re-author on essentially every start, and whole-GB steps
    /// would still flip on an 8 GB swing. The ladder makes a re-author need the free space to HALVE or
    /// DOUBLE, which is the scale at which the checkpoint spacing it governs actually changes; within a rung
    /// nothing is written, and the start's log line says so. The stamp line under the marker records the
    /// derived rung and the PostgreSQL major, and <see cref="ConfHasCurrentWalSizingStamp"/> compares the
    /// LAST stamp in the file — the v8 rule, for the v8 reason: postgresql.conf takes the last occurrence, so
    /// the question has to be asked of the block that is actually in force.</para>
    ///
    /// <para><b><c>checkpoint_completion_target</c> is pinned at 0.9 only where the default is not already
    /// 0.9.</b> The PostgreSQL 14 release notes (E.25.3.1.9): <i>"Change checkpoint_completion_target default
    /// to 0.9 (Stephen Frost). The previous default was 0.5."</i> On 14 and later the line is omitted —
    /// re-stating a default buys nothing and would read as a decision this block did not make; on 13 and
    /// earlier it is emitted, because a checkpoint that finishes in half its interval is the write spike this
    /// block exists to spread. The bundled runtime is PostgreSQL 18, so in the shipped product the line can
    /// only ever appear on a data directory this build's initdb did not create. An unreadable
    /// <c>PG_VERSION</c> is treated as pre-14: the pin is a no-op where the default is already 0.9 and the
    /// fix where it is not, so emitting it is the answer that cannot be wrong.</para>
    ///
    /// <para><b>What it does not reach.</b> <c>postgresql.auto.conf</c> is read after <c>postgresql.conf</c>,
    /// so an <c>ALTER SYSTEM SET max_wal_size</c> still wins — the precedence the v10 <c>lc_messages</c> and
    /// v11 job-logging notes document (v11 measured it), and the one the maintainer's own out-of-band 16 GB
    /// may be sitting under. This block does not fight it: <see cref="LogWalSizingAutoConfOverrides"/> reads
    /// the auto.conf, logs one WARNING per WAL key it assigns — naming the key, its value and the precedence —
    /// and the block is authored regardless, so the file records what the product derived even while an
    /// operator's override is what runs. Nothing here edits or deletes <c>postgresql.auto.conf</c>;
    /// <c>ALTER SYSTEM RESET</c> is the operator's move. And it reaches MANAGED stores only — a
    /// bring-your-own store's WAL is its owner's to size, consistent with the BYO posture everywhere else in
    /// this class.</para>
    ///
    /// <para><b>What it does when the disk cannot be read.</b> Nothing — the v8 rule. An unreadable
    /// <c>DriveInfo</c> is not evidence the headroom is unchanged, it is the absence of evidence either way,
    /// and re-deriving the WAL ceiling from a figure this service could not read is worse than leaving the
    /// last good block (or v4's 4 GB, on a store that has never healed) in force. The skip is logged as a
    /// warning naming what stays in force.</para>
    ///
    /// <para><b>Reload semantics.</b> All three settings are SIGHUP-context (the documentation's <i>"can only
    /// be set in the postgresql.conf file or on the server command line"</i>), and this append runs before
    /// <c>pg_ctl start</c>, so on a service-owned start the block is live on the very start that writes it —
    /// the v9 through v11 story. The adopted-listener path in <see cref="EnsureRunningAsync"/> is the same
    /// exception it is for them: a postmaster this service did not start is neither stopped nor signalled, so
    /// there the heal waits for the next service-owned start.</para>
    /// </summary>
    public const string ConfMarkerV12 = "# Managed by PerformanceMonitor Darling (v12 wal sizing) -- do not remove this block";

    /// <summary>
    /// The v13 marker (#3899): preload <c>pg_stat_statements</c>, so the store keeps per-statement timings and
    /// "the web viewer / MCP tools are slow" can be answered with a ranked list by role instead of a guess.
    /// Before this the store loaded only <c>timescaledb</c> and had <c>log_min_duration_statement = -1</c>, so
    /// nothing in the product could say which query was slow; attributing one took the owner's credential and
    /// a hand-set per-role GUC. The module ships in the bundled runtime (1.12 on PostgreSQL 18.4) and was
    /// simply never loaded. <see cref="StoreStatementStats"/> creates the extension and the reader function
    /// on each start once the preload is live.
    ///
    /// <para><b>A MERGE, never a literal.</b> <c>shared_preload_libraries</c> is list-valued and the last
    /// occurrence REPLACES the list (measured, and documented on <see cref="ConfMarkerV11"/>), so a block
    /// that wrote <c>'timescaledb,pg_stat_statements'</c> verbatim would drop anything an operator had added.
    /// The block re-states the EFFECTIVE list read from the file at append time plus
    /// <see cref="StatementStatisticsLibrary"/>; see <see cref="MergePreloadLibraries"/>.</para>
    ///
    /// <para><b><c>track_utility = off</c> is a security setting, not tuning.</b> pg_stat_statements records a
    /// utility statement's text without normalizing its literals, so any <c>ALTER ROLE ... PASSWORD '...'</c>
    /// run against the store (an operator's, or a bring-your-own provisioning script's) would be kept verbatim.
    /// The service's own provisioning sends SCRAM-SHA-256 verifiers, never a password, and only when one
    /// changes (#3910), so it no longer depends on this; the setting keeps every other utility statement out
    /// of the view too, and the reader function shows the text of normalized DML only, as a second
    /// guard.</para>
    ///
    /// <para><b>Restart semantics.</b> <c>shared_preload_libraries</c> is postmaster-context. This append runs
    /// before <c>pg_ctl start</c>, so a service-owned start loads the library on the very start that writes the
    /// block; the adopted-listener path in <see cref="EnsureRunningAsync"/> waits for the next service-owned
    /// start, as v2-v5 and v7 do. An <c>ALTER SYSTEM</c> override of the list in <c>postgresql.auto.conf</c>
    /// wins over this block and is logged with the exact statement that fixes it, never edited; so is an
    /// assignment added after the block, and a library named only on an earlier line the block replaces
    /// (<see cref="LogStatementStatisticsPreloadCoverage"/>).</para>
    /// </summary>
    public const string ConfMarkerV13 = "# Managed by PerformanceMonitor Darling (v13 statement statistics) -- do not remove this block";

    /// <summary>
    /// The v14 marker (#3909): a <c>maintenance_work_mem</c> line PostgreSQL 17 will accept, appended to a data
    /// directory still on 17 whose effective value is over 17's Windows limit of 2097151 kB. The v3/v7/v8
    /// blocks derived 2048 MB on large hosts, which 18 accepts and 17 refuses at startup (FATAL), so a 17 store
    /// that got those blocks, typically after a reverted upgrade, could no longer start.
    ///
    /// <para>Keyed on the VALUE, not on the marker: it is appended whenever the assignment in force is over the
    /// limit, so it heals once and then finds its own line in force. Written from two places.
    /// <see cref="HealLegacyMaintenanceWorkMem"/> runs before anything can start the cluster, the store
    /// upgrade's old-cluster start included, which <see cref="EnsureConfAppended"/> runs too late for.
    /// <see cref="EnsureConfAppended"/> covers every other start, the restart after a reverted upgrade
    /// included. Carries no fingerprint or stamp line, so the v8 and v12 checks never see it.</para>
    /// </summary>
    public const string ConfMarkerV14 = "# Managed by PerformanceMonitor Darling (v14 PostgreSQL 17 maintenance_work_mem limit) -- do not remove this block";

    /// <summary>
    /// The v15 marker (#4246): <c>wal_compression = lz4</c> only. Across two production stores, <c>pg_waldump
    /// --stats=record</c> over live WAL showed 66-90% of bytes were full-page images (FPI) — mostly
    /// <c>FPI_FOR_HINT</c>, the image data checksums force on a page's first hint-bit change, plus random-key
    /// btree leaf inserts. <c>wal_compression</c> shrinks every one of those images, <c>FPI_FOR_HINT</c>
    /// included, no matter how often checkpoints run.
    ///
    /// <para><b><c>lz4</c>, not <c>zstd</c> or <c>pglz</c>.</b> <c>default_toast_compression = lz4</c> (v1)
    /// already proves lz4 ships in the bundled runtime; it costs less CPU than zstd for a few GB/hour of
    /// image data, the same trade the TOAST setting already made.</para>
    ///
    /// <para><b>The checkpoint interval is held, not shipped.</b> A longer <c>checkpoint_timeout</c> would cut
    /// WAL further by re-imaging each hot page less often — #4246 found roughly two-thirds of one 5-minute
    /// cycle's images repeated the previous cycle's — but it risks the store's own checkpointer self-alert:
    /// <see cref="DarlingSelfAlertEvaluator.CheckpointSyncBarMs"/> (#4037) fires when a checkpoint's sync phase
    /// averages more than 10 seconds, a bar that exists because sync phases of 14.0s and 25.2s killed reads on
    /// a production store. #3892 found that a longer interval puts more files into each checkpoint, which
    /// makes each sync phase longer, so a 15-minute interval risks trading WAL volume for killed reads and for
    /// alerts firing on a healthy store. One production store measures the interval first, through <c>ALTER
    /// SYSTEM</c> and a reload rather than this block; it ships here later, in its own marker, only if that
    /// measurement stays under the sync bar.</para>
    ///
    /// <para><b>Does not touch <c>max_wal_size</c>.</b> <see cref="ConfMarkerV12"/> (#3802) already bounds it
    /// by free disk, and this block leaves that bound alone.</para>
    ///
    /// <para><b>Field note.</b> The heaviest measured sample followed a restart: most of its images were
    /// <c>FPI_FOR_HINT</c> from the first cycle's reads setting hint bits on pages nothing had touched since
    /// the previous shutdown. <c>wal_compression</c> compresses those images too, so the heaviest hour a
    /// store sees after a restart is also where it pays off most.</para>
    ///
    /// <para><c>wal_compression</c> is <c>superuser</c>-context, not <c>sighup</c> (confirmed live) — but like
    /// a <c>sighup</c> setting it still takes effect from <c>postgresql.conf</c> on a reload, and this append
    /// runs before <c>pg_ctl start</c>, so a service-owned start applies it on the very start that writes the
    /// block, the v9-v11 story. Managed stores only; a bring-your-own store keeps whatever
    /// <c>wal_compression</c> its owner set. A later change to this value needs a NEW marker (the v11/v14
    /// precedent): this block heals by its marker's absence, so an edited value in an already-marked file
    /// would never be seen.</para>
    /// </summary>
    public const string ConfMarkerV15 = "# Managed by PerformanceMonitor Darling (v15 WAL compression) -- do not remove this block";

    /// <summary>
    /// Every marker this class ever appends to postgresql.conf, in append order (#4214). A generic scan that
    /// asks "is this line inside SOME managed block" (the host-profile check's per-setting source attribution)
    /// walks this list rather than naming a marker per setting — which setting a given block carries is exactly
    /// what <see cref="BuildMemorySizingConfAppend"/>/<see cref="BuildHardwareSizingConfAppend"/>/etc. decide,
    /// and a second list keyed the other way (setting -> marker) would be one more place those two could drift.
    /// v1 (<see cref="ConfMarker"/>) is included even though it never carries one of the seven checked
    /// settings — harmless, since a scan for a setting v1 never sets simply never lands inside its span.
    /// </summary>
    internal static readonly string[] AllManagedConfMarkers =
    [
        ConfMarker, ConfMarkerV2, ConfMarkerV3, ConfMarkerV4, ConfMarkerV5, ConfMarkerV6, ConfMarkerV7,
        ConfMarkerV8, ConfMarkerV9, ConfMarkerV10, ConfMarkerV11, ConfMarkerV12, ConfMarkerV13, ConfMarkerV14,
        ConfMarkerV15,
    ];

    /// <summary>
    /// Prefix of the v8 fingerprint line — the record of what the sizing beneath it was derived FROM,
    /// which is the whole mechanism: a marker can only say "a block exists", a fingerprint says "a block
    /// exists FOR THIS MACHINE". Compared by <see cref="ConfHasCurrentHardwareFingerprint"/> against the
    /// LAST occurrence rather than any occurrence, which is what makes a resize BACK to a previous size
    /// re-derive: a plain Contains would find the stale earlier fingerprint and skip, leaving the larger
    /// host's block still winning by last-occurrence-wins.
    /// </summary>
    public const string ConfHardwareFingerprintPrefix = "# darling-hardware-fingerprint: ";

    /// <summary>
    /// Prefix of the v12 stamp line (#3802) — the record of what the WAL sizing beneath it was derived TO: the
    /// <c>max_wal_size</c> rung, the <c>min_wal_size</c> that follows from it, and the PostgreSQL major that
    /// decided whether <c>checkpoint_completion_target</c> was pinned. The same mechanism as
    /// <see cref="ConfHardwareFingerprintPrefix"/>, compared the same way against the LAST occurrence
    /// (<see cref="ConfHasCurrentWalSizingStamp"/>), under a DIFFERENT prefix on purpose: the v8 check keys on
    /// the last line carrying its own prefix in the text read at the top of <see cref="EnsureConfAppended"/>,
    /// and a v12 line that shared that prefix would be the line v8 read on the next start. Neither prefix is a
    /// substring of the other, and a pin holds it so.
    ///
    /// <para>Records OUTPUTS where v8 records inputs, for the reason the v12 marker's doc gives: the raw
    /// free-disk figure moves on every start and the rung does not. Recording the input would make every
    /// start a change; recording the rung makes only a real change one.</para>
    /// </summary>
    public const string ConfWalSizingStampPrefix = "# darling-wal-sizing: ";

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

    /* #4215: `postgres -C <key> -D <data>` parses every configuration file and exits — the same shape of
       probe as s_versionProbeTimeout, so it gets the same short budget. */
    private static readonly TimeSpan s_confValidateTimeout = TimeSpan.FromSeconds(15);
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

    [SupportedOSPlatform("windows")]
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
    [SupportedOSPlatform("windows")]
    internal DarlingStoreUpgrade.StoreUpgradeOutcome LastUpgradeOutcome { get; private set; }
        = DarlingStoreUpgrade.StoreUpgradeOutcome.None;

    /// <summary>
    /// What this start did to the store's TimescaleDB extension (#3908), carried out of the bootstrap beside
    /// <see cref="LastUpgradeOutcome"/> and alerted on separately.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal DarlingStoreUpgrade.TimescaleUpdateOutcome LastTimescaleOutcome { get; private set; }
        = DarlingStoreUpgrade.TimescaleUpdateOutcome.None;

    public string DataDirectory => _dataDirectory;

    /// <summary>What <see cref="WriteManagedConfFile"/> did with <c>darling-managed.conf</c> on this start
    /// (#4215) — carried out of the bootstrap the same way
    /// <see cref="LastUpgradeOutcome"/> is, so <c>DarlingWorker</c> can fold a hand edit's changed keys into
    /// the stored verdict rows without re-reading the file itself. Null when the service-owned conf-write path
    /// never ran this start (the adopted-listener branch of <see cref="EnsureRunningAsync"/>).</summary>
    [SupportedOSPlatform("windows")]
    internal ManagedConfWriteResult? LastManagedConfWriteResult { get; private set; }

    /// <summary>Whether THIS start ran PostgreSQL on <see cref="ManagedConfFile.LastGoodFileName"/> rather than
    /// the file <see cref="WriteManagedConfFile"/> just rendered (#4215) — set only in the recovery
    /// branch of <see cref="EnsureManagedConfReadyAsync"/>, the one place that copies the last-good file back
    /// over the rejected one. Reset to false at the top of every <see cref="EnsureManagedConfReadyAsync"/> call
    /// so a later, clean start clears it without a process restart — carried out to the store-settings self-alert
    /// the same way <see cref="LastManagedConfWriteResult"/> already is.</summary>
    [SupportedOSPlatform("windows")]
    internal bool LastStartUsedLastGoodManagedConf { get; private set; }

    /// <summary>The #4215/#4336 migration's outcome for THIS start — null when
    /// <see cref="MigrateManagedConfAsync"/> never ran this start (the adopted-listener branch; a Verified
    /// conf runs Step B instead). Carried out of the bootstrap the same way
    /// <see cref="LastManagedConfWriteResult"/> already is.</summary>
    [SupportedOSPlatform("windows")]
    internal ManagedConfMigrationOutcome? LastManagedConfVerification { get; private set; }

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
            /* Trimmed (round-1 security review, #4280 Low 3): Path.GetFullPath keeps a trailing separator, and
               every caller that builds a "-D" argument from this value quotes it as `"{path}"` — a trailing
               backslash then escapes that closing quote. Hardening only: every one of those callers already
               fails its own first use of the broken value long before anything downstream reads it. */
            : Path.TrimEndingDirectorySeparator(Path.GetFullPath(config.DataDirectory));
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
        /* timescaledb.enable_job_execution_logging lives in the v11 block, NOT here (#3175). It was here
           (#1681) and that is why it never reached a single pre-existing cluster: this block's marker is
           already present on any cluster that predates the setting, so the append carrying it is skipped
           and the GUC arrives only on a fresh initdb. Every other setting added after v1 has its own
           marker for exactly that reason. See ConfMarkerV11 for the measurement and for why the fix is a
           new marker rather than a looser match on this one. */
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
        var workers = DeriveWorkerSettings(TimescaleSupport.HypertableCount);
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV2).Append('\n');
        builder.Append("timescaledb.max_background_workers = ").Append(workers.MaxBackgroundWorkers).Append('\n');
        builder.Append("max_worker_processes = ").Append(workers.MaxWorkerProcesses).Append('\n');
        return builder.ToString();
    }

    /// <summary>The two worker settings derived from the live hypertable count, both restart-only.</summary>
    internal readonly record struct WorkerSettings(int MaxBackgroundWorkers, int MaxWorkerProcesses);

    /// <summary>
    /// Derives the worker sizing from the hypertable count — extracted from
    /// <see cref="BuildWorkerSizingConfAppend"/> (#2845) so the v2 block and the v8 hardware re-derivation
    /// share ONE formula and cannot drift apart. One background worker per per-hypertable compression
    /// policy that can run concurrently + the scheduler + slack; max_worker_processes = 3 (other bg
    /// workers) + bg workers + 8 (the PostgreSQL default max_parallel_workers, which this class does not
    /// set — see the v8 block for why raising it is deliberately NOT bundled here).
    ///
    /// <para>The v2 doc comment claims this "never goes stale as collectors are added". That was true of
    /// the FORMULA and false of its application: v2 is marker-keyed, so an existing store computed these
    /// once and kept the answer no matter how many collectors arrived afterwards. The v8 block is what
    /// makes the claim true, by re-deriving whenever the hypertable count changes.</para>
    /// </summary>
    internal static WorkerSettings DeriveWorkerSettings(int hypertableCount)
    {
        var maxBackgroundWorkers = hypertableCount + 2;
        return new WorkerSettings(maxBackgroundWorkers, 3 + maxBackgroundWorkers + 8);
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
        builder.Append("max_connections = ").Append(TargetMaxConnections).Append('\n');
        builder.Append("max_wal_size = 4GB\n");
        return builder.ToString();
    }

    /// <summary>
    /// The fixed <c>max_connections</c> the v4 block writes (#4214): a single named constant instead of the
    /// literal <c>200</c> living in two places (this append, and the host-profile check's "value derived for
    /// this host" for the same setting), which is not RAM/hypertable-derived like the settings in
    /// <see cref="DeriveMemorySettings"/> — it is a fixed headroom figure, so there is no <c>Derive*</c>
    /// function to share; this constant is the shared source instead.
    /// </summary>
    internal const int TargetMaxConnections = 200;

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
           just under 2 GB, where the field data showed nothing further to gain.

           The cap is 2047 MB, not 2048 (#3909). PostgreSQL 17 on Windows accepts at most 2097151 kB here,
           one kB under 2 GB, and a conf line above that is FATAL at startup, not a warning. PostgreSQL 18
           accepts 2048 MB, but a store can still be on 17 (it predates the 18 bundle, or its upgrade
           reverted), so the one cap has to hold for both. 1 MB is noise against a benefit that plateaued
           by 1.5 GB. A 17 conf that already carries 2048 MB is healed by the v14 block (ConfMarkerV14).

           THE CONSTRAINT THE SMALL-HOST LANDINGS REST ON: both of today's consumers allocate
           INCREMENTALLY — a tuplesort grows to fit its input and SPILLS past the ceiling rather than
           reserving it, and PG 17+ builds vacuum's dead-TID store (TidStore) the same way. So this number
           bounds what an operation MAY use, not what it WILL use, which is what makes a 4 GB host's
           1024 MB landing safe despite being 5x its old 204 MB. If a future consumer ever PRE-ALLOCATES
           maintenance_work_mem, that reasoning breaks and the small-host landings need revisiting here. */
        var maintenanceWorkMem = Math.Min(
            Math.Min(Math.Max(ram / 20, 1536 * oneMb), ram / 4),
            MaintenanceWorkMemCapMb * oneMb);
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

    /// <summary>The derived <c>maintenance_work_mem</c> cap in MB (#1777's 2 GB, less 1 MB for #3909).</summary>
    internal const int MaintenanceWorkMemCapMb = 2047;

    /// <summary>
    /// The largest <c>maintenance_work_mem</c> PostgreSQL 17 accepts on Windows, in kB (#3909): 2097151,
    /// one kB under 2 GB. Measured on 17.10, where <c>2048MB</c> is FATAL ("2097152 kB is outside the valid
    /// range ... (64 kB .. 2097151 kB)"). PostgreSQL 18 accepts 2048 MB.
    /// </summary>
    internal const long LegacyMaintenanceWorkMemMaxKb = 2097151;

    /// <summary>The setting the v14 block (#3909) caps.</summary>
    internal const string MaintenanceWorkMemSetting = "maintenance_work_mem";

    /// <summary>
    /// A <c>maintenance_work_mem</c> value as <see cref="ReadConfAssignments"/> returns it (quotes and comment
    /// already stripped), in kB: <c>2048MB</c>, <c>2GB</c>, <c>65536</c> (no unit means kB, the parameter's
    /// base unit), with or without a space before the unit. Null for anything else, which the caller leaves
    /// alone rather than guessing at. Pure.
    /// </summary>
    internal static long? ParseMaintenanceWorkMemKb(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var text = value.Trim();
        var digits = 0;
        while (digits < text.Length && char.IsAsciiDigit(text[digits]))
        {
            digits++;
        }

        if (digits == 0 || !long.TryParse(text[..digits], NumberStyles.None, CultureInfo.InvariantCulture, out var number))
        {
            return null;
        }

        return text[digits..].Trim().ToUpperInvariant() switch
        {
            "" or "KB" => number,
            "B" => number / 1024,
            "MB" => number * 1024,
            "GB" => number * 1024 * 1024,
            "TB" => number * 1024 * 1024 * 1024,
            _ => null,
        };
    }

    /// <summary>
    /// Whether the value in force would stop PostgreSQL 17 or earlier from starting (#3909): the major is known
    /// and at most 17, and the value is over <see cref="LegacyMaintenanceWorkMemMaxKb"/>. An unknown major or an
    /// unparsable value is left alone. Pure.
    /// </summary>
    internal static bool NeedsLegacyMaintenanceWorkMemCap(int? dataMajor, string? effectiveValue)
    {
        if (dataMajor is not (> 0 and <= 17))
        {
            return false;
        }

        /* A null (unparsable) value compares false, so it is left alone. */
        return ParseMaintenanceWorkMemKb(effectiveValue) > LegacyMaintenanceWorkMemMaxKb;
    }

    /// <summary>
    /// The <c>maintenance_work_mem</c> assignment a v14 block has to follow in this data directory, or null
    /// when none is needed (#3909). It is the assignment in force, read the way PostgreSQL reads it:
    /// postgresql.conf with its includes, then postgresql.auto.conf. An over-limit value set by
    /// <c>ALTER SYSTEM</c> is in postgresql.auto.conf, which the server reads after postgresql.conf, so no
    /// appended block can override it. That case is logged at Critical with the fix, and returns null.
    /// </summary>
    [SupportedOSPlatform("windows")]
    private ConfAssignment? FindLegacyMaintenanceWorkMemOverLimit(string dataDirectory)
    {
        var major = DarlingStoreUpgrade.TryReadDataDirectoryMajor(dataDirectory);
        if (major is not (> 0 and <= 17))
        {
            return null;
        }

        var autoConfPath = Path.GetFullPath(Path.Combine(dataDirectory, "postgresql.auto.conf"));
        var chain = ReadConfAssignments(Path.Combine(dataDirectory, "postgresql.conf"), MaintenanceWorkMemSetting);
        chain.AddRange(ReadConfAssignments(autoConfPath, MaintenanceWorkMemSetting));
        if (chain.Count == 0 || !NeedsLegacyMaintenanceWorkMemCap(major, chain[^1].Value))
        {
            return null;
        }

        var inForce = chain[^1];
        if (string.Equals(inForce.File, autoConfPath, StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogCritical(
                "maintenance_work_mem = {Value} in {File} (line {Line}) is over PostgreSQL {Major}'s limit of {LimitKb} kB, so the server will not start. It was set by ALTER SYSTEM, which is read after postgresql.conf, so the service cannot override it: delete that line (the server is not running to take ALTER SYSTEM RESET) and restart the service.",
                inForce.Value, inForce.File, inForce.Line, major, LegacyMaintenanceWorkMemMaxKb);
            return null;
        }

        return inForce;
    }

    private void LogLegacyMaintenanceWorkMemCap(ConfAssignment overLimit)
        => _logger.LogWarning(
            "maintenance_work_mem = {Value} ({File}, line {Line}) is over PostgreSQL 17's limit of {LimitKb} kB, which stops the server from starting. Appended maintenance_work_mem = {CapMb}MB after it (#3909).",
            overLimit.Value, overLimit.File, overLimit.Line, LegacyMaintenanceWorkMemMaxKb, MaintenanceWorkMemCapMb);

    /// <summary>
    /// The v14 block (#3909): one <c>maintenance_work_mem</c> line at the cap, appended after whatever
    /// assignment is over PostgreSQL 17's limit so it becomes the last occurrence. See
    /// <see cref="ConfMarkerV14"/> for when it is written.
    /// </summary>
    internal static string BuildLegacyMaintenanceWorkMemCapConfAppend()
    {
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV14).Append('\n');
        builder.Append("maintenance_work_mem = ").Append(MaintenanceWorkMemCapMb).Append("MB\n");
        return builder.ToString();
    }

    /// <summary>
    /// Makes a PostgreSQL 17 (or earlier) data directory's conf one its server will open (#3909), before
    /// anything starts that server: the store upgrade's old-cluster start, a reverted upgrade's restart, or a
    /// plain start of a store still on 17. A 17 conf reaches an over-limit value because the v3/v7/v8 blocks
    /// derived 2048 MB on hosts with 40 GB of RAM or more until this change, and they are written to whatever
    /// data directory the service is running. Once that happened after a reverted upgrade, every start failed,
    /// and a later release could not upgrade the store either, because its first step starts the old cluster.
    ///
    /// <para>The fix has to be in the file. A <c>-c maintenance_work_mem=...</c> on the command line does not
    /// help: measured on 17.10, the server still validates the file's value and refuses to start. Appending a
    /// later assignment does help, because PostgreSQL uses only the last occurrence. Never throws: the files
    /// are only read and appended to, and an I/O failure leaves the store failing the way it already would
    /// have, with a warning explaining why.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal void HealLegacyMaintenanceWorkMem(string dataDirectory)
    {
        try
        {
            if (FindLegacyMaintenanceWorkMemOverLimit(dataDirectory) is { } overLimit)
            {
                File.AppendAllText(Path.Combine(dataDirectory, "postgresql.conf"), BuildLegacyMaintenanceWorkMemCapConfAppend());
                LogLegacyMaintenanceWorkMemCap(overLimit);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger.LogWarning(
                "Could not check or fix maintenance_work_mem in {DataDirectory} ({Message}). On PostgreSQL 17, a value over {LimitKb} kB stops the server from starting.",
                dataDirectory, ex.Message, LegacyMaintenanceWorkMemMaxKb);
        }
    }

    /// <summary>
    /// The v8 fingerprint line for a given set of derivation inputs (#2845) — RAM and the hypertable
    /// count, the two quantities every setting this block emits is a function of. Formatted invariantly so
    /// the comparison is a plain ordinal string match on a machine with any locale.
    ///
    /// <para>RAM is normalised through the same non-positive fallback <see cref="DeriveMemorySettings"/>
    /// applies, so a zero reading fingerprints as the 4 GB value it would actually have derived under. Note
    /// that normalisation is NOT what makes the fingerprint stable across a failed read, and must not be
    /// relied on for it: <see cref="GetTotalPhysicalMemoryBytes"/> falls back to a LIVE GC figure before it
    /// reaches that sentinel, so a failed read arrives here as a varying positive number this guard cannot
    /// see. Stability comes from <see cref="ShouldAppendHardwareSizing"/> refusing to act at all without an
    /// authoritative reading.</para>
    /// </summary>
    internal static string BuildHardwareFingerprint(long totalPhysicalMemoryBytes, int hypertableCount)
        => FormattableString.Invariant(
            $"{ConfHardwareFingerprintPrefix}ram_mb={QuantizeRam(totalPhysicalMemoryBytes) / (1024L * 1024L)} hypertables={hypertableCount}");

    /// <summary>
    /// Rounds a raw RAM reading to the nearest GB for the v8 path (#2845 review), and is applied to BOTH
    /// the fingerprint and the derivation so a block is exactly reproducible from the fingerprint above it.
    ///
    /// <para><b>Why quantize.</b> The fingerprint is an exact comparison and v8 runs on EVERY start, so any
    /// jitter in the reported total reads as a hardware change: a fresh block appended per restart, seven
    /// lines of postgresql.conf growth each time, forever. <c>ullTotalPhys</c> is not guaranteed
    /// bit-identical across reboots — a balloon/Dynamic-Memory guest can report a different current total
    /// with no operator resize — and these ARE cloud VMs. The fleet's own readings already show the total is
    /// not a round number (31.5 GB on a nominally 32 GB host, firmware reservation), which is the same class
    /// of wobble one size larger. Rounding also recovers the NOMINAL size the sizing formulas conceptually
    /// want, rather than the slightly-short figure the OS reports.</para>
    ///
    /// <para>A GB is the right granularity because it is far above any plausible reporting jitter and far
    /// below any real resize — the smallest step this class can be resized by is 4 -> 8 GB. It applies to the
    /// v8 path ONLY: v3/v5/v7 keep deriving from the raw reading exactly as before, so this cannot shift a
    /// value on a store that never reaches v8.</para>
    /// </summary>
    internal static long QuantizeRam(long totalPhysicalMemoryBytes)
    {
        const long oneGb = 1024L * 1024L * 1024L;
        var ram = totalPhysicalMemoryBytes > 0 ? totalPhysicalMemoryBytes : MemoryFallbackRamBytes;
        return (ram + oneGb / 2) / oneGb * oneGb;
    }

    /// <summary>
    /// True when the MOST RECENT fingerprint in the conf matches the current hardware — the test that
    /// decides whether <see cref="BuildHardwareSizingConfAppend"/> needs to run (#2845).
    ///
    /// <para>Deliberately NOT <c>conf.Contains(fingerprint)</c>. postgresql.conf takes the LAST occurrence
    /// of a setting, so what is in force is whatever the newest block said. A host resized 16 -> 32 -> 16 GB
    /// would, under a Contains test, find its original 16 GB fingerprint still present and skip — leaving
    /// the 32 GB block as the last occurrence and therefore still in force on a box that no longer has
    /// 32 GB. Comparing only the last fingerprint makes the check ask the question that matches the file's
    /// own semantics, and is what lets this converge instead of latching.</para>
    /// </summary>
    internal static bool ConfHasCurrentHardwareFingerprint(string conf, string expectedFingerprint)
        => LastLineWithPrefixEquals(conf, ConfHardwareFingerprintPrefix, expectedFingerprint);

    /// <summary>
    /// Whether the LAST line in <paramref name="conf"/> that starts with <paramref name="prefix"/> is exactly
    /// <paramref name="expectedLine"/> — the one comparison both every-start heals share (v8's fingerprint,
    /// v12's stamp; #3802 extracted it so the two cannot drift in what "current" means). Last, not any: see
    /// <see cref="ConfHasCurrentHardwareFingerprint"/> for why a Contains test latches on a stale block.
    /// </summary>
    private static bool LastLineWithPrefixEquals(string conf, string prefix, string expectedLine)
    {
        var lastIndex = conf.LastIndexOf(prefix, StringComparison.Ordinal);
        if (lastIndex < 0)
        {
            return false;
        }

        var lineEnd = conf.IndexOf('\n', lastIndex);
        var line = lineEnd < 0 ? conf[lastIndex..] : conf[lastIndex..lineEnd];
        return string.Equals(line.TrimEnd('\r'), expectedLine, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether the v8 block should be appended on this start (#2845; the second and third conditions are
    /// #4207's heal for a store resized BEFORE that fix shipped) — the whole decision as one pure function
    /// so the property can be pinned without a data directory.
    ///
    /// <para>The RAM reading must be authoritative for ANY of the three conditions below to act — checked
    /// first, and short-circuiting the rest. A non-authoritative reading is not evidence that the hardware
    /// is unchanged, it is the absence of evidence either way — and re-deriving production sizing from a
    /// number we could not read is worse than leaving the last good block in force. It also stops a
    /// flapping Win32 call from minting a novel fingerprint on every blip and appending a block each time,
    /// which a value-only guard cannot do because the fallback it would guard against is a live, varying
    /// quantity rather than a fixed sentinel.</para>
    ///
    /// <para><b>Given an authoritative reading, any of three conditions triggers a heal:</b></para>
    /// <list type="bullet">
    /// <item>the newest fingerprint in the conf does not match today's inputs (#2845's original condition —
    /// a genuine hardware or hypertable-count change).</item>
    /// <item>the conf holds MORE THAN ONE v8 block (<see cref="FindHardwareSizingBlockSpans"/>). #4225 made
    /// a fingerprint change collapse to a single rewritten block, but a store that had already accumulated
    /// duplicates before that fix shipped has no fingerprint change left to trigger on — its newest
    /// fingerprint already matches, so the first condition alone would leave the duplicates in place
    /// forever.</item>
    /// <item>the newest block's content does not match what THIS BUILD would write for those same inputs
    /// (<see cref="NewestHardwareSizingBlockIsCurrent"/>), even though its fingerprint line matches. A block
    /// written by an older build — before <c>work_mem</c> rejoined this list in #4207 — fingerprints as
    /// "current" for its RAM and hypertable count, because the fingerprint encodes only those two inputs,
    /// never the formula version that turned them into settings. Without this condition, that store would
    /// never re-derive: nothing about its hardware ever changes again, so the first condition never fires
    /// either.</item>
    /// </list>
    /// </summary>
    internal static bool ShouldAppendHardwareSizing(
        string conf, bool ramReadingIsAuthoritative, string expectedFingerprint, string expectedBlockAppend)
        => ramReadingIsAuthoritative
            && (!ConfHasCurrentHardwareFingerprint(conf, expectedFingerprint)
                || FindHardwareSizingBlockSpans(conf).Count > 1
                || !NewestHardwareSizingBlockIsCurrent(conf, expectedBlockAppend));

    /// <summary>
    /// True when the LAST v8 block in <paramref name="conf"/> is, line for line, the text
    /// <see cref="BuildHardwareSizingConfAppend"/> would write for the current inputs (#4207) — the
    /// stale-CONTENT half of <see cref="ShouldAppendHardwareSizing"/>'s decision, checked even when the
    /// fingerprint line itself already matches (see that method's remarks for why fingerprint-only misses a
    /// block written by an older formula).
    ///
    /// <para>False when there is no v8 block at all: that reads as "not current" and defers to
    /// <see cref="ReplaceOrAppendHardwareSizingBlock"/>'s plain-append fallback, the same v2-v7 shape as
    /// before.</para>
    ///
    /// <para>Line endings are normalised before comparing. <paramref name="conf"/> can be CRLF — this file
    /// is written and hand-edited on Windows — while every <c>Build*ConfAppend</c> in this class emits LF
    /// only, so a byte comparison would read every CRLF conf as permanently stale and rewrite it on every
    /// single start.</para>
    /// </summary>
    internal static bool NewestHardwareSizingBlockIsCurrent(string conf, string expectedBlockAppend)
    {
        var spans = FindHardwareSizingBlockSpans(conf);
        if (spans.Count == 0)
        {
            return false;
        }

        var (start, end) = spans[^1];
        var actual = conf[start..end];

        /* expectedBlockAppend carries the same leading blank-line separator BuildHardwareSizingConfAppend
           always does; a block SPAN never includes that separator (see FindHardwareSizingBlockEnd), so it
           is stripped here to compare like with like. */
        var expected = expectedBlockAppend.StartsWith('\n') ? expectedBlockAppend[1..] : expectedBlockAppend;

        return string.Equals(
            actual.Replace("\r\n", "\n", StringComparison.Ordinal),
            expected.Replace("\r\n", "\n", StringComparison.Ordinal),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The v8 hardware-sizing block (#2845): re-states the settings that are a pure function of the
    /// HOST, at the host we are on now, and records the inputs it used so the next start can tell whether
    /// they still hold.
    ///
    /// <para><b>What it emits, and why only these.</b> <c>effective_cache_size</c> is the setting the issue
    /// was raised for — a planner hint with no allocation, found at 11.86 GB (75% of 16 GB) on hosts that
    /// now have 31.5 GB, which biases the planner toward sequential scans on a store serving ~670k small
    /// index lookups a day. <c>maintenance_work_mem</c> is a per-operation CEILING that PostgreSQL grows
    /// into rather than reserves, so re-deriving it cannot overcommit. <c>work_mem</c> re-joined this list in
    /// #4207: see the bullet below for why v3's original exclusion in #2845 does not hold up. The two worker
    /// settings are restart-only counts of background slots that only ever grow as collectors are added, and
    /// re-stating them is what finally makes the v2 block's "never goes stale" claim true.</para>
    ///
    /// <para><b>work_mem WAS excluded (#2845); #4207 measured why that was wrong.</b> The original argument
    /// was that the formula would take it 31 MB -> ~63 MB at 31.5 GB and the only measurements above 31 MB on
    /// the heaviest read were WORSE: PlanRegressionSql at default 26,565 ms, at 31 MB 25,617 ms, and at
    /// <b>512 MB</b> 59,323 ms. That comparison never tested the value this formula actually derives — 512 MB
    /// is 8x <see cref="DeriveMemorySettings"/>'s own 64 MB ceiling, a value nothing in this codebase would
    /// ever write, so the regression it found says nothing about the ~63 MB case. What #2845 left unmeasured,
    /// #4207 measured directly: three field stores stuck at the v3 block's 31 MB (16 GB-derived) after a
    /// resize spilled <b>33 TB and 7 TB</b> of <c>pg_stat_database.temp_bytes</c> to disk since creation, on
    /// hosts reporting 33,788,809,216 bytes — nominally "31.5 GiB", actually 31.47 GiB, which
    /// <see cref="QuantizeRam"/> rounds DOWN to 31 GB (the 31.5 GB midpoint rounds up; this reading is half a
    /// GB short of it) and <see cref="DeriveMemorySettings"/> turns into <b>62 MB</b>, not the round "63 MB"
    /// the issue's own back-of-envelope RAM/512 gave for a bare 31.5 GiB. Either figure is what nothing
    /// re-applied — the exact staleness this whole block exists to heal, just for the one setting it skipped.
    /// The claim that
    /// <c>work_mem</c> is "not a property of the machine" is also narrower than it reads: the formula's own
    /// ceiling (RAM/512, clamped 16-64 MB) is deliberately modest specifically BECAUSE it is a per-connection,
    /// per-sort cost against a machine with a fixed amount of RAM (see <see cref="DeriveMemorySettings"/>),
    /// and a spill that costs disk I/O and wall-clock time is worse than the same query having had the RAM
    /// its own host was sized to offer.</para>
    ///
    /// <para><b>What it deliberately does NOT emit, and why the omissions are the load-bearing part.</b></para>
    /// <list type="bullet">
    /// <item><b>shared_buffers</b> — EXCLUDED STRUCTURALLY, not by relying on the formula's cap. The 1 GB
    ///   cap is the Windows error-487 mitigation (#1559, pgsql-bugs BUG #14050 / #18954): larger segments
    ///   exacerbate <c>could not reserve shared memory region</c> when every backend re-reserves at the
    ///   postmaster's base address, and that condition is LIVE on this fleet (measured 2026-09-03: use2
    ///   112-205/day, use1 36-70, pgmon 4-34, with zero <c>could not fork</c> — the retry path is holding,
    ///   which is precisely the margin a bigger segment would spend). min(25% RAM, 1 GB) is already 1 GB on
    ///   any host above 4 GB, so a hardware change cannot move it and emitting it would buy nothing. The
    ///   reason to leave it out is the FUTURE one: if the cap is ever raised deliberately, that is a formula
    ///   change and belongs to a version-keyed block where it gets reviewed, not something a resize should
    ///   silently propagate to production. work_mem has no such structural reason: nothing caps its formula
    ///   to a value a hardware change cannot move, which is exactly why letting it go stale had a cost.</item>
    /// <item><b>max_parallel_workers</b> — not emitted because this class has never set it; it sits at the
    ///   PostgreSQL default of 8 regardless of core count. Deriving it from cores is a plausible want on a
    ///   16-core host, but it is a behaviour change rather than a staleness fix, and it multiplies the
    ///   memory story above: each parallel worker gets its OWN work_mem for its share of a node, so raising
    ///   parallelism raises peak sort memory on exactly the query that already degrades with more of it.
    ///   It wants its own evidence and its own PR. Note this is also why the fingerprint records RAM and
    ///   hypertables and not cores: with nothing core-derived to re-state, a core-only change has no work
    ///   to do, and fingerprinting it would append a block of identical values on every resize.</item>
    /// </list>
    ///
    /// <para><b>Reload semantics.</b> <c>effective_cache_size</c>, <c>maintenance_work_mem</c> and
    /// <c>work_mem</c> are all SIGHUP-reloadable; the two worker settings are restart-only. The append runs
    /// before <c>pg_ctl start</c> on a service-owned start, so in practice the whole block takes effect on
    /// that very start — the same story as v3 and v7.</para>
    /// </summary>
    internal static string BuildHardwareSizingConfAppend(long totalPhysicalMemoryBytes, int hypertableCount)
    {
        /* The SAME quantized value the fingerprint records, so the block is exactly reproducible from it. */
        var settings = DeriveMemorySettings(QuantizeRam(totalPhysicalMemoryBytes));
        var workers = DeriveWorkerSettings(hypertableCount);
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV8).Append('\n');
        builder.Append(BuildHardwareFingerprint(totalPhysicalMemoryBytes, hypertableCount)).Append('\n');
        builder.Append("effective_cache_size = ").Append(settings.EffectiveCacheSizeMb).Append("MB\n");
        builder.Append("maintenance_work_mem = ").Append(settings.MaintenanceWorkMemMb).Append("MB\n");
        builder.Append("work_mem = ").Append(settings.WorkMemMb).Append("MB\n");
        builder.Append("timescaledb.max_background_workers = ").Append(workers.MaxBackgroundWorkers).Append('\n');
        builder.Append("max_worker_processes = ").Append(workers.MaxWorkerProcesses).Append('\n');
        return builder.ToString();
    }

    /// <summary>
    /// The [start, end) span of the v8 block whose marker begins at <paramref name="markerStart"/>: from the
    /// marker line through the last content line before the next blank line, or end of file (#4207).
    ///
    /// <para><b>Why this is the rule, when the marker carries no end sentinel of its own</b> (unlike the
    /// begin/end pair <see cref="ReconcilePgHba"/> replaces between). <see cref="BuildHardwareSizingConfAppend"/>,
    /// like every <c>Build*ConfAppend</c> in this file, writes its block as ONE leading blank line — the
    /// separator from whatever came before, itself OUTSIDE the block — followed by the marker and then
    /// content lines with NO blank line between them. So the first blank line found after the marker is
    /// always the start of what follows: either the next block's own leading separator, or trailing
    /// whitespace at end of file. That holds for a v8 block written by ANY version of the builder, past or
    /// future, not only today's line count — a setting added to or removed from the block moves where the
    /// next blank line falls without this rule having to change.</para>
    ///
    /// <para><b>Known edge case.</b> An operator line spliced in directly after a v8 block's last setting
    /// line, with NO blank line of its own before it, reads as more content of that block rather than as
    /// something outside it — every block this codebase writes is blank-line-separated from what follows
    /// (each <c>Build*ConfAppend</c> begins with its own leading blank line), so this only bites a hand edit
    /// that does not follow that convention. <c>ALTER SYSTEM</c> (postgresql.auto.conf) is unaffected either
    /// way, since this function never reads that file.</para>
    /// </summary>
    /// <remarks>Internal, not private (#4214): the body never mentions v8 specifically — it walks from a
    /// marker line to the next blank line or EOF, which is the same shape every <c>Build*ConfAppend</c> in
    /// this class writes. The host-profile check's generic managed-block scan reuses this exact walk for
    /// EVERY marker rather than re-implementing it, so the two cannot drift on what "a block's span" means.</remarks>
    internal static int FindHardwareSizingBlockEnd(string conf, int markerStart)
    {
        var cursor = conf.IndexOf('\n', markerStart);
        if (cursor < 0)
        {
            return conf.Length;
        }

        cursor++;
        while (cursor < conf.Length)
        {
            var lineEnd = conf.IndexOf('\n', cursor);
            var line = lineEnd < 0 ? conf[cursor..] : conf[cursor..lineEnd];
            if (line.TrimEnd('\r').Length == 0)
            {
                return cursor;
            }

            if (lineEnd < 0)
            {
                return conf.Length;
            }

            cursor = lineEnd + 1;
        }

        return cursor;
    }

    /// <summary>
    /// Every v8 block's [start, end) span in <paramref name="conf"/>, in file order — file order being
    /// append order, so the first span is also the chronologically first block (#4207). On each of the three
    /// field stores this returns three spans, one per fingerprint change since the store's creation, because
    /// the prior code appended a fresh block on every change instead of replacing the one it superseded.
    /// <see cref="ReplaceOrAppendHardwareSizingBlock"/> is what collapses them.
    /// </summary>
    internal static List<(int Start, int End)> FindHardwareSizingBlockSpans(string conf)
    {
        var spans = new List<(int Start, int End)>();
        var searchFrom = 0;
        while (true)
        {
            var markerStart = conf.IndexOf(ConfMarkerV8, searchFrom, StringComparison.Ordinal);
            if (markerStart < 0)
            {
                break;
            }

            /* Defensive: the marker only means "a v8 block starts here" at the start of a line — it is
               never written any other way — so a match that is not line-initial (impossible today, but
               cheap to rule out) is skipped rather than treated as a block. */
            if (markerStart > 0 && conf[markerStart - 1] != '\n')
            {
                searchFrom = markerStart + ConfMarkerV8.Length;
                continue;
            }

            var end = FindHardwareSizingBlockEnd(conf, markerStart);
            spans.Add((markerStart, end));
            searchFrom = end;
        }

        return spans;
    }

    /// <summary>
    /// Collapses however many v8 blocks <paramref name="conf"/> carries into exactly one, at the position of
    /// the FIRST (#4207). Every block after the first is a leftover from the append-not-replace bug — a
    /// stale copy the code once left behind on every fingerprint change — and is removed outright; the first
    /// is rewritten in place with <paramref name="newBlockAppend"/>'s content (the same string
    /// <see cref="BuildHardwareSizingConfAppend"/> returns for a plain append, leading blank line included).
    /// No v8 block at all falls back to a plain append — the v2-v7 shape — so a cluster's first v8 write is
    /// unchanged.
    ///
    /// <para>Nothing outside a v8 span is touched, INCLUDING the blank line that separates one block from the
    /// next: that separator is not part of either block under <see cref="FindHardwareSizingBlockEnd"/>'s
    /// rule, so removing a duplicate can leave a doubled blank line where three blocks once stood. That is
    /// cosmetic — PostgreSQL ignores blank lines — and the alternative (also consuming the separator) would
    /// touch a byte that is provably not part of any v8 block, which the pin on this function
    /// (<c>ReplaceOrAppendHardwareSizingBlock_LinesOutsideBlocks_AreByteIdenticalAfterRewrite</c>) forbids.</para>
    ///
    /// <para><b>Why rewriting the FIRST block's position, not the last, keeps manual overrides winning
    /// exactly as before.</b> postgresql.conf takes the LAST occurrence of a setting, so what decides a
    /// manual edit's fate is only ITS position relative to wherever the v8 lines end up — and collapsing can
    /// only move that position EARLIER in the file (to the first block) or leave it unchanged (already one
    /// block), never later. An edit that already sat after every v8 block still sits after the single
    /// survivor; an edit that already lost to a later v8 block was losing before this function ever ran, for
    /// the same reason. <c>ALTER SYSTEM</c> values in <c>postgresql.auto.conf</c> are unaffected either way —
    /// that file is read after postgresql.conf in its entirety and outranks anything this function does.</para>
    /// </summary>
    internal static string ReplaceOrAppendHardwareSizingBlock(string conf, string newBlockAppend)
    {
        var spans = FindHardwareSizingBlockSpans(conf);
        if (spans.Count == 0)
        {
            return conf + newBlockAppend;
        }

        /* newBlockAppend carries the same leading blank line every Build*ConfAppend does; splicing it in at
           an existing marker's position would double that separator, since the blank line already there
           (untouched, being outside the span by definition) still precedes it. */
        var content = newBlockAppend.StartsWith('\n') ? newBlockAppend[1..] : newBlockAppend;

        var builder = new StringBuilder(conf.Length + content.Length);
        var cursor = 0;
        for (var i = 0; i < spans.Count; i++)
        {
            var (start, end) = spans[i];
            builder.Append(conf, cursor, start - cursor);
            if (i == 0)
            {
                builder.Append(content);
            }

            cursor = end;
        }

        builder.Append(conf, cursor, conf.Length - cursor);
        return builder.ToString();
    }

    /* ===================== v9 session time zone ===================== */

    /// <summary>
    /// The v9 block: pin the cluster's session <c>timezone</c> to UTC. See <see cref="ConfMarkerV9"/> for
    /// why, and for what this deliberately does not cover. Appended last and carrying no fingerprint line,
    /// so the v8 staleness check's invariant about what it reads is untouched.
    /// </summary>
    public static string BuildTimeZoneConfAppend()
    {
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV9).Append('\n');
        builder.Append("timezone = 'UTC'\n");
        return builder.ToString();
    }

    /* ===================== v10 message locale ===================== */

    /// <summary>
    /// The v10 block: pin the cluster's <c>lc_messages</c> to <c>C</c> so the server writes its messages, and
    /// the severity label that <see cref="StoreLogClassifier"/> anchors on, untranslated. See
    /// <see cref="ConfMarkerV10"/> for why, why <c>C</c> and not a named English locale, and what this
    /// deliberately does not pin. Carries no fingerprint line, so the v8 staleness check's invariant about
    /// what it reads is untouched.
    /// </summary>
    public static string BuildMessageLocaleConfAppend()
    {
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV10).Append('\n');
        builder.Append("lc_messages = 'C'\n");
        return builder.ToString();
    }

    /* ===================== v11 job execution logging ===================== */

    /// <summary>
    /// The v11 block: turn <c>timescaledb.enable_job_execution_logging</c> on so
    /// <c>timescaledb_information.job_history</c> records one row per background-job run. This is the
    /// setting #1681 put in the v1 block, where its marker guard meant it could only ever reach a fresh
    /// initdb — see <see cref="ConfMarkerV11"/> for the two-store measurement, why the fix is a new marker
    /// rather than a looser match on v1, what a heal does and does not recover, and the reload semantics.
    /// Carries no fingerprint line, so the v8 staleness check's invariant about what it reads is untouched.
    /// </summary>
    public static string BuildJobExecutionLoggingConfAppend()
    {
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV11).Append('\n');
        builder.Append(StoreSelfMetrics.JobExecutionLoggingSetting).Append(" = on\n");
        return builder.ToString();
    }

    /* ===================== v13 statement statistics (#3899) ===================== */

    /// <summary>The library the v13 block adds to <c>shared_preload_libraries</c>.</summary>
    public const string StatementStatisticsLibrary = "pg_stat_statements";

    /// <summary>The library v1 preloads, and the base the v13 merge falls back to when the conf carries no
    /// active assignment. A managed conf always carries one by then, because v1 is appended first.</summary>
    internal const string TimescaleLibrary = "timescaledb";

    /// <summary>The list-valued setting the v13 block restates.</summary>
    internal const string PreloadSetting = "shared_preload_libraries";

    /// <summary>PostgreSQL's own cap on configuration-file nesting (<c>CONF_FILE_MAX_DEPTH</c>), so an include
    /// cycle ends where the server's own read of it would.</summary>
    internal const int MaxConfIncludeDepth = 10;

    /// <summary>
    /// The v13 block (#3899): <c>shared_preload_libraries</c> re-stated as the effective list plus
    /// <see cref="StatementStatisticsLibrary"/>, and <c>pg_stat_statements.track_utility = off</c>. See
    /// <see cref="ConfMarkerV13"/> for why the list is merged rather than written, and why utility tracking
    /// must be off. Carries no fingerprint or stamp line, so neither every-start check reads it.
    /// </summary>
    public static string BuildStatementStatisticsConfAppend(string? effectivePreloadList)
    {
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV13).Append('\n');
        builder.Append(PreloadSetting).Append(" = '")
            .Append(EscapeConfValue(MergePreloadLibraries(effectivePreloadList)))
            .Append("'\n");
        builder.Append(StatementStatisticsLibrary).Append(".track_utility = off\n");
        return builder.ToString();
    }

    /// <summary>
    /// The effective preload list plus <see cref="StatementStatisticsLibrary"/>, order and spelling preserved,
    /// duplicates dropped, in the conf-file form (<see cref="FormatPreloadList"/>). An absent or empty list
    /// merges from <see cref="TimescaleLibrary"/> rather than from nothing: on a fresh cluster the only active
    /// assignment is v1's, and a merge that lost it would stop the store loading TimescaleDB.
    /// </summary>
    internal static string MergePreloadLibraries(string? effectivePreloadList) =>
        FormatPreloadList(MergePreloadLibraryNames(effectivePreloadList));

    /// <summary><see cref="MergePreloadLibraries"/>'s names, before they are written in either form.</summary>
    internal static List<string> MergePreloadLibraryNames(string? effectivePreloadList)
    {
        var libraries = ParsePreloadList(effectivePreloadList);
        if (libraries.Count == 0)
        {
            libraries.Add(TimescaleLibrary);
        }

        if (!libraries.Contains(StatementStatisticsLibrary, StringComparer.OrdinalIgnoreCase))
        {
            libraries.Add(StatementStatisticsLibrary);
        }

        return libraries;
    }

    /// <summary>
    /// A <c>shared_preload_libraries</c> value as its library names, read the way PostgreSQL's own
    /// <c>SplitDirectoriesString</c> reads it: comma-separated, an unquoted name trimmed, a double-quoted name
    /// taken whole (commas included, <c>""</c> an escaped quote), no case folding. Blanks and case-insensitive
    /// duplicates are dropped. The quoted form matters because it is how <c>ALTER SYSTEM</c> stores a name it
    /// had to quote, including the one-literal mistake (<c>'"timescaledb,pg_stat_statements"'</c> is ONE
    /// library name, which PostgreSQL cannot load); splitting it on its commas, as the first version did, hid
    /// exactly that mistake.
    /// </summary>
    internal static List<string> ParsePreloadList(string? preloadList)
    {
        var libraries = new List<string>();
        if (string.IsNullOrWhiteSpace(preloadList))
        {
            return libraries;
        }

        var i = 0;
        while (i < preloadList.Length)
        {
            while (i < preloadList.Length && char.IsWhiteSpace(preloadList[i]))
            {
                i++;
            }

            if (i >= preloadList.Length)
            {
                break;
            }

            string name;
            if (preloadList[i] == '"')
            {
                var quoted = new StringBuilder();
                i++;
                while (i < preloadList.Length)
                {
                    if (preloadList[i] == '"')
                    {
                        if (i + 1 < preloadList.Length && preloadList[i + 1] == '"')
                        {
                            quoted.Append('"');
                            i += 2;
                            continue;
                        }

                        i++;
                        break;
                    }

                    quoted.Append(preloadList[i]);
                    i++;
                }

                name = quoted.ToString();
                while (i < preloadList.Length && preloadList[i] != ',')
                {
                    i++;
                }
            }
            else
            {
                var start = i;
                while (i < preloadList.Length && preloadList[i] != ',')
                {
                    i++;
                }

                name = preloadList[start..i].Trim();
            }

            i++;
            if (name.Length > 0 && !libraries.Contains(name, StringComparer.OrdinalIgnoreCase))
            {
                libraries.Add(name);
            }
        }

        return libraries;
    }

    /// <summary>
    /// A value as the inside of a postgresql.conf single-quoted string: a backslash and a quote each escaped,
    /// the two characters the conf file's own string syntax treats specially (<c>DeescapeQuotedString</c>
    /// reads <c>\\</c> as one backslash). A library path such as <c>C:\libs\x</c> would otherwise come back as
    /// <c>C:libsx</c> (#3915's review).
    /// </summary>
    internal static string EscapeConfValue(string value) =>
        value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("'", "''", StringComparison.Ordinal);

    /// <summary>
    /// Whether PostgreSQL itself accepts <paramref name="preloadList"/> as a list, by <c>SplitDirectoriesString</c>'s
    /// rules: empty is a valid list of nothing; otherwise every element is a non-empty unquoted name or a
    /// closed double-quoted one, separated by commas, with nothing else between. PostgreSQL loads NOTHING from
    /// a list it rejects (it logs "invalid list syntax" at LOG and carries on), TimescaleDB included, which is
    /// why the coverage check reports it (#3915's review: a trailing comma read as fine here).
    /// </summary>
    internal static bool IsValidPreloadList(string? preloadList)
    {
        if (string.IsNullOrWhiteSpace(preloadList))
        {
            return true;
        }

        var i = 0;
        while (i < preloadList.Length && char.IsWhiteSpace(preloadList[i]))
        {
            i++;
        }

        while (true)
        {
            if (i < preloadList.Length && preloadList[i] == '"')
            {
                i++;
                while (true)
                {
                    var close = preloadList.IndexOf('"', i);
                    if (close < 0)
                    {
                        return false;
                    }

                    if (close + 1 < preloadList.Length && preloadList[close + 1] == '"')
                    {
                        i = close + 2;
                        continue;
                    }

                    i = close + 1;
                    break;
                }
            }
            else
            {
                var start = i;
                var nameEnd = i;
                while (i < preloadList.Length && preloadList[i] != ',')
                {
                    if (!char.IsWhiteSpace(preloadList[i]))
                    {
                        nameEnd = i + 1;
                    }

                    i++;
                }

                if (nameEnd == start)
                {
                    return false;
                }
            }

            while (i < preloadList.Length && char.IsWhiteSpace(preloadList[i]))
            {
                i++;
            }

            if (i >= preloadList.Length)
            {
                return true;
            }

            if (preloadList[i] != ',')
            {
                return false;
            }

            i++;
            while (i < preloadList.Length && char.IsWhiteSpace(preloadList[i]))
            {
                i++;
            }
        }
    }

    /// <summary>Library names as a conf-file list value: comma-joined, a name double-quoted only when it has to
    /// be (a comma, a double quote, or edge whitespace in it), which <see cref="ParsePreloadList"/> reads back
    /// whole.</summary>
    internal static string FormatPreloadList(IEnumerable<string> libraries) =>
        string.Join(",", libraries.Select(library =>
            library.IndexOfAny([',', '"']) >= 0 || library.Trim().Length != library.Length
                ? "\"" + library.Replace("\"", "\"\"", StringComparison.Ordinal) + "\""
                : library));

    /// <summary>
    /// Library names as the right-hand side of <c>ALTER SYSTEM SET shared_preload_libraries = ...</c>: ONE
    /// single-quoted literal per library, comma-separated. The setting is <c>GUC_LIST_QUOTE</c>, so a single
    /// literal holding the whole list is stored as one library name and the store will not start again
    /// (reproduced on 18.4 by #3904's review, which caught the first version's warning advising exactly that).
    /// </summary>
    internal static string FormatAlterSystemPreloadList(IEnumerable<string> libraries) =>
        string.Join(", ", libraries.Select(library => "'" + library.Replace("'", "''", StringComparison.Ordinal) + "'"));

    /// <summary>
    /// Every assignment line in postgresql.conf-format text, in order, as PostgreSQL reads it: its 1-based line,
    /// its name and its value. Commented and blank lines are skipped, the <c>=</c> is optional (PostgreSQL
    /// accepts <c>name value</c>), a quoted value is de-escaped the server's way (<see cref="ParseConfValue"/>)
    /// and an unquoted one ends at whitespace or a trailing comment. Include directives come back as ordinary
    /// assignments; <see cref="ReadConfAssignments"/> is what follows them.
    /// </summary>
    internal static IEnumerable<(int Line, string Name, string Value)> ParseConfText(string? confText)
    {
        if (string.IsNullOrEmpty(confText))
        {
            yield break;
        }

        var lineNumber = 0;
        foreach (var raw in confText.Split('\n'))
        {
            lineNumber++;
            if (TryParseConfLine(raw, out var key, out var value))
            {
                yield return (lineNumber, key, value);
            }
        }
    }

    /// <summary>One active assignment of a setting: the file it is in (a full path), its 1-based line, and its
    /// value as PostgreSQL reads it.</summary>
    internal readonly record struct ConfAssignment(string File, int Line, string Value);

    /// <summary>
    /// Every active assignment of <paramref name="name"/> reachable from <paramref name="confPath"/>, in the order
    /// PostgreSQL processes them, so the LAST is the one in force (before <c>postgresql.auto.conf</c>, which is
    /// read after all of it). Follows <c>include</c>, <c>include_if_exists</c> and <c>include_dir</c> the way
    /// the server does: a relative path resolves against the including file's directory, a directory
    /// contributes its <c>*.conf</c> files not starting with a dot in name order, and nesting stops at
    /// <see cref="MaxConfIncludeDepth"/>. A file that cannot be read contributes nothing (a missing
    /// <c>include</c> stops the server itself, which is not this reader's to report).
    ///
    /// <para>Why includes are followed (#3904's review): the v13 block is appended at the END of
    /// postgresql.conf, after every include above it, so a preload list an operator set in an included file is
    /// one the block replaces. Merging from postgresql.conf's own text alone would drop that operator's
    /// libraries without a word.</para>
    /// </summary>
    internal static List<ConfAssignment> ReadConfAssignments(string confPath, string name)
    {
        var found = new List<ConfAssignment>();
        var filesRead = 0;
        CollectConfAssignments(Path.GetFullPath(confPath), name, found, depth: 0, ref filesRead);
        return found;
    }

    /// <summary>The most files one read of the conf chain opens. PostgreSQL refuses an include cycle once it
    /// nests past <see cref="MaxConfIncludeDepth"/>, but a cycle that fans out (a directory including itself
    /// twice) would be re-read exponentially before that depth; this runs on the start path, so it stops at a
    /// budget no real configuration comes near (#3915's review).</summary>
    internal const int MaxConfFilesRead = 64;

    private static void CollectConfAssignments(string path, string name, List<ConfAssignment> found, int depth, ref int filesRead)
    {
        if (depth > MaxConfIncludeDepth || filesRead >= MaxConfFilesRead)
        {
            return;
        }

        filesRead++;
        string text;
        try
        {
            text = File.ReadAllText(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return;
        }

        var directory = Path.GetDirectoryName(path) ?? string.Empty;
        foreach (var (lineNumber, key, value) in ParseConfText(text))
        {
            if (key.Equals("include", StringComparison.OrdinalIgnoreCase)
                || key.Equals("include_if_exists", StringComparison.OrdinalIgnoreCase))
            {
                if (TryResolveConfPath(directory, value, out var included))
                {
                    CollectConfAssignments(included, name, found, depth + 1, ref filesRead);
                }
            }
            else if (key.Equals("include_dir", StringComparison.OrdinalIgnoreCase))
            {
                if (!TryResolveConfPath(directory, value, out var includeDirectory))
                {
                    continue;
                }

                string[] files;
                try
                {
                    files = Directory.Exists(includeDirectory) ? Directory.GetFiles(includeDirectory) : [];
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    files = [];
                }

                foreach (var file in files
                    .Where(f => Path.GetFileName(f) is { } n && n.EndsWith(".conf", StringComparison.Ordinal) && !n.StartsWith('.'))
                    .OrderBy(f => Path.GetFileName(f), StringComparer.Ordinal))
                {
                    CollectConfAssignments(file, name, found, depth + 1, ref filesRead);
                }
            }
            else if (key.Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                found.Add(new ConfAssignment(path, lineNumber, value));
            }
        }
    }

    /// <summary>An include directive's path, resolved against the including file's directory the way the
    /// server resolves it; false for an empty value, one that is not a path this platform can resolve, or a UNC
    /// path (the service would reach out to a network share as its own identity on the start path), each of
    /// which contributes nothing rather than throwing or leaving the machine.</summary>
    private static bool TryResolveConfPath(string directory, string value, out string fullPath)
    {
        fullPath = string.Empty;
        if (value.Length == 0)
        {
            return false;
        }

        try
        {
            fullPath = Path.GetFullPath(Path.Combine(directory, value));
            return !fullPath.StartsWith(@"\\", StringComparison.Ordinal);
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            return false;
        }
    }

    /// <summary>One postgresql.conf line as <c>name [=] value</c>, or false for a blank, a comment, or a line
    /// that is not an assignment. The name is PostgreSQL's identifier shape, dots included for a module's own
    /// settings.</summary>
    private static bool TryParseConfLine(string raw, out string key, out string value)
    {
        key = string.Empty;
        value = string.Empty;

        var line = raw.Trim();
        if (line.Length == 0 || line[0] == '#')
        {
            return false;
        }

        var end = 0;
        while (end < line.Length && (char.IsLetterOrDigit(line[end]) || line[end] is '_' or '.' || line[end] >= '\u0080'))
        {
            end++;
        }

        if (end == 0 || (end < line.Length && line[end] != '=' && !char.IsWhiteSpace(line[end])))
        {
            return false;
        }

        key = line[..end];
        var rest = line[end..].TrimStart();
        if (rest.StartsWith('='))
        {
            rest = rest[1..].TrimStart();
        }

        value = ParseConfValue(rest);
        return true;
    }

    /// <summary>
    /// One value as the conf file's lexer and <c>DeescapeQuotedString</c> read it: an unquoted value ends at
    /// whitespace or a comment; a quoted one ends at its closing quote, with <c>''</c> a quote and a backslash
    /// escaping the next character (<c>\b \f \n \r \t</c>, up to three octal digits, and anything else, a
    /// backslash and a quote included, as itself). The first version read <c>\\'</c> as an escaped quote and ran
    /// a value ending in a backslash, such as an include directory, on to the end of the line (#3915's review).
    /// </summary>
    private static string ParseConfValue(string text)
    {
        if (!text.StartsWith('\''))
        {
            var end = 0;
            while (end < text.Length && !char.IsWhiteSpace(text[end]) && text[end] != '#')
            {
                end++;
            }

            return text[..end];
        }

        var builder = new StringBuilder();
        for (var i = 1; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '\\' && i + 1 < text.Length)
            {
                var escaped = text[++i];
                switch (escaped)
                {
                    case 'b':
                        builder.Append('\b');
                        break;
                    case 'f':
                        builder.Append('\f');
                        break;
                    case 'n':
                        builder.Append('\n');
                        break;
                    case 'r':
                        builder.Append('\r');
                        break;
                    case 't':
                        builder.Append('\t');
                        break;
                    case >= '0' and <= '7':
                        var octal = escaped - '0';
                        for (var digits = 1; digits < 3 && i + 1 < text.Length && text[i + 1] is >= '0' and <= '7'; digits++)
                        {
                            octal = (octal * 8) + (text[++i] - '0');
                        }

                        builder.Append((char)(octal & 0xFF));
                        break;
                    default:
                        builder.Append(escaped);
                        break;
                }
            }
            else if (c == '\'')
            {
                if (i + 1 < text.Length && text[i + 1] == '\'')
                {
                    builder.Append('\'');
                    i++;
                }
                else
                {
                    break;
                }
            }
            else
            {
                builder.Append(c);
            }
        }

        return builder.ToString();
    }

    /// <summary>
    /// The v13 every-start check (#3899): which <c>shared_preload_libraries</c> assignment is IN FORCE, across
    /// postgresql.conf with its includes and then <c>postgresql.auto.conf</c> (read last, so an
    /// <c>ALTER SYSTEM</c> override wins), and whether it still loads what it should. Changes nothing, the v12
    /// precedent: neither file is edited here. Four outcomes are logged, each with its exact fix:
    /// <list type="bullet">
    /// <item><description>The list in force is not a list PostgreSQL accepts (<see cref="IsValidPreloadList"/>,
    /// a trailing comma say): the server loads nothing from it, TimescaleDB included. An Error.</description></item>
    /// <item><description>The list in force names a library holding a comma: the one-literal <c>ALTER SYSTEM</c>
    /// mistake, stored as ONE name PostgreSQL cannot load, so the store will not start. An Error, and a remedy
    /// that works on a store that is down.</description></item>
    /// <item><description>The list in force lacks <see cref="StatementStatisticsLibrary"/>: an <c>ALTER SYSTEM</c>
    /// override, or an assignment an operator put after the v13 block, and the store records no statement
    /// statistics until it names the library.</description></item>
    /// <item><description>A library named by an EARLIER assignment is missing from the one in force. The v13
    /// block restated the list once, when it was appended, so an operator who later adds a library to an earlier
    /// line (v1's, or their own above the block) has an edit PostgreSQL ignores, because the later assignment
    /// replaces the whole list. #3904's review found nothing said so.</description></item>
    /// </list>
    /// Never throws; a file that cannot be read contributes nothing.
    /// </summary>
    internal void LogStatementStatisticsPreloadCoverage(string dataDirectory)
    {
        var autoConfPath = Path.GetFullPath(Path.Combine(dataDirectory, "postgresql.auto.conf"));
        var chain = ReadConfAssignments(Path.Combine(dataDirectory, "postgresql.conf"), PreloadSetting);
        chain.AddRange(ReadConfAssignments(autoConfPath, PreloadSetting));
        if (chain.Count == 0)
        {
            return;
        }

        var inForce = chain[^1];
        var inForceLibraries = ParsePreloadList(inForce.Value);
        var inAutoConf = string.Equals(inForce.File, autoConfPath, StringComparison.OrdinalIgnoreCase);

        if (!IsValidPreloadList(inForce.Value))
        {
            var repaired = MergePreloadLibraryNames(inForce.Value);
            _logger.LogError(
                "{File} line {Line} sets shared_preload_libraries = '{Value}', which is not a list PostgreSQL accepts (an empty element, an unclosed double quote, or text after a closing one): the server logs 'invalid list syntax' and loads NO library from it, TimescaleDB included. {Fix}",
                inForce.File, inForce.Line, inForce.Value,
                inAutoConf
                    ? $"Run ALTER SYSTEM SET shared_preload_libraries = {FormatAlterSystemPreloadList(repaired)} (one quoted literal per library) and restart the store."
                    : $"Edit that line to shared_preload_libraries = '{EscapeConfValue(FormatPreloadList(repaired))}' and restart the store.");
            return;
        }

        if (inForceLibraries.Any(library => library.Contains(',', StringComparison.Ordinal)))
        {
            var split = MergePreloadLibraryNames(string.Join(",", inForceLibraries));
            _logger.LogError(
                "{File} line {Line} sets shared_preload_libraries = '{Value}', which names ONE library holding commas: that is what ALTER SYSTEM stores when a whole list is passed as a single quoted literal. PostgreSQL cannot load it, so the store will not start (FATAL: could not access file). {Fix}",
                inForce.File, inForce.Line, inForce.Value,
                inAutoConf
                    ? $"The store cannot start to run ALTER SYSTEM, so delete that line from postgresql.auto.conf by hand and start the store; then, to keep the list, run ALTER SYSTEM SET shared_preload_libraries = {FormatAlterSystemPreloadList(split)} (one quoted literal per library) and restart it again."
                    : $"Edit that line to shared_preload_libraries = '{EscapeConfValue(FormatPreloadList(split))}' and start the store.");
            return;
        }

        if (!inForceLibraries.Contains(StatementStatisticsLibrary, StringComparer.OrdinalIgnoreCase))
        {
            var suggested = MergePreloadLibraryNames(inForce.Value);
            if (inAutoConf)
            {
                _logger.LogWarning(
                    "postgresql.auto.conf sets shared_preload_libraries = '{Value}' (an ALTER SYSTEM override) without {Library}. PostgreSQL reads postgresql.auto.conf AFTER postgresql.conf, so the v13 block's preload is inert and the store records no statement statistics until the override includes it: ALTER SYSTEM SET shared_preload_libraries = {Suggested}, then restart the store. One quoted literal per library: a single literal holding the whole list is stored as one library name, and the store would not start. This service does not edit postgresql.auto.conf.",
                    inForce.Value, StatementStatisticsLibrary, FormatAlterSystemPreloadList(suggested));
            }
            else
            {
                _logger.LogWarning(
                    "{File} line {Line} sets shared_preload_libraries = '{Value}' without {Library}, and it is the assignment in force: it is read after the v13 block, and a later assignment replaces the whole list, so the store records no statement statistics until it names the library. Change that line to shared_preload_libraries = '{Suggested}' and restart the store.",
                    inForce.File, inForce.Line, inForce.Value, StatementStatisticsLibrary,
                    EscapeConfValue(FormatPreloadList(suggested)));
            }

            return;
        }

        var overridden = new List<(string Library, ConfAssignment Where)>();
        foreach (var earlier in chain.Take(chain.Count - 1))
        {
            foreach (var library in ParsePreloadList(earlier.Value))
            {
                if (!inForceLibraries.Contains(library, StringComparer.OrdinalIgnoreCase)
                    && !overridden.Any(o => o.Library.Equals(library, StringComparison.OrdinalIgnoreCase)))
                {
                    overridden.Add((library, earlier));
                }
            }
        }

        foreach (var (library, where) in overridden)
        {
            _logger.LogWarning(
                "{Library} is named by the shared_preload_libraries assignment at {File} line {Line}, but the assignment in force is {InForceFile} line {InForceLine} ('{InForceValue}'), which replaces the whole list, so it is not loaded. Add it to that line if it should load, or remove it from the earlier one if it should not.",
                library, where.File, where.Line, inForce.File, inForce.Line, inForce.Value);
        }
    }

    /* ===================== v15 WAL compression (#4246) ===================== */

    /// <summary>
    /// The v15 block: <c>wal_compression = lz4</c> only. See <see cref="ConfMarkerV15"/> for the measurement,
    /// why lz4, why the checkpoint interval is held rather than shipped here, and why this deliberately does
    /// not touch <c>max_wal_size</c>. Carries no fingerprint or stamp line, so the v8 and v12 staleness checks
    /// are untouched by this block.
    /// </summary>
    public static string BuildWalVolumeConfAppend()
    {
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV15).Append('\n');
        builder.Append("wal_compression = lz4\n");
        return builder.ToString();
    }

    /* ===================== v12 wal sizing (derived from data-volume headroom, #3802) ===================== */

    /// <summary>1 GB — the floor under the derived <c>max_wal_size</c>, and PostgreSQL's own default for it:
    /// a volume that cannot afford more WAL than the server would have used anyway gets exactly that, and the
    /// start's log line says so (see <see cref="ConfMarkerV12"/>).</summary>
    internal const long WalSizingFloorBytes = 1L * 1024 * 1024 * 1024;

    /// <summary>16 GB — the maintainer's chosen ceiling from #3802 (the figure applied out of band to the store
    /// that exhibited the mechanism), NOT a measured optimum. Also where the crash-recovery price of a larger
    /// WAL stops being worth paying for this write shape; see <see cref="ConfMarkerV12"/>.</summary>
    internal const long WalSizingCeilingBytes = 16L * 1024 * 1024 * 1024;

    /// <summary>80 MB — PostgreSQL's <c>min_wal_size</c> default, the floor under the quarter rule. Never the
    /// binding term on the power-of-two ladder (the 1 GB rung yields 256 MB), coded so the formula is true of
    /// itself rather than of the ladder.</summary>
    internal const long MinWalSizeFloorBytes = 80L * 1024 * 1024;

    /// <summary>The share of the data volume's free space the WAL ceiling may claim: one eighth, leaving seven
    /// for the soft limit's overshoot, the store's own growth, and the disk-pressure self-alert.</summary>
    internal const int WalSizingFreeDiskDivisor = 8;

    /// <summary>
    /// The PostgreSQL major whose release notes moved <c>checkpoint_completion_target</c>'s default from 0.5 to
    /// 0.9 (PostgreSQL 14, E.25.3.1.9: <i>"Change checkpoint_completion_target default to 0.9 (Stephen Frost).
    /// The previous default was 0.5."</i>). The v12 block pins 0.9 only on majors BELOW this one.
    /// </summary>
    internal const int CheckpointCompletionTargetDefaultChangedMajor = 14;

    /// <summary>The value the v12 block pins on pre-14 majors — the default every later major ships with.</summary>
    internal const string CheckpointCompletionTargetPin = "0.9";

    /// <summary>
    /// The three settings the v12 block may author, and therefore the keys
    /// <see cref="FindWalSizingAutoConfOverrides"/> looks for in <c>postgresql.auto.conf</c>: an
    /// <c>ALTER SYSTEM</c> on any of them outranks the block by PostgreSQL's precedence, and the start says so.
    /// </summary>
    internal static readonly string[] WalSizingSettingNames = { "max_wal_size", "min_wal_size", "checkpoint_completion_target" };

    /// <summary>The two WAL sizes the v12 block derives from data-volume headroom, in whole MB.</summary>
    internal readonly record struct WalSettings(int MaxWalSizeMb, int MinWalSizeMb);

    /// <summary>
    /// Derives the WAL sizing from the free space on the data volume — PURE and testable via an injected byte
    /// count, the way <see cref="DeriveMemorySettings"/> derives from RAM (#3802).
    ///
    /// <para><c>max_wal_size</c> = <c>free / 8</c>, clamped to [1 GB, 16 GB], then FLOORED to the power-of-two
    /// ladder 1, 2, 4, 8, 16 GB. The clamp is the formula; the ladder is what makes a check that runs on every
    /// start converge instead of appending — a re-author needs the free space to halve or double, not to move
    /// (see <see cref="ConfMarkerV12"/>). The rungs therefore sit at 16, 32, 64 and 128 GB free.
    /// <c>min_wal_size</c> = <c>max(80 MB, max_wal_size / 4)</c>.</para>
    ///
    /// <para>A non-positive reading derives as ZERO free — the floor, PostgreSQL's own default — rather than
    /// as some fallback volume, because "cannot afford it" is the only honest answer to "could not measure it".
    /// The caller never passes one: <see cref="EnsureConfAppended"/> skips the whole v12 check without an
    /// authoritative disk reading, exactly as v8 does without an authoritative RAM reading.</para>
    /// </summary>
    internal static WalSettings DeriveWalSettings(long freeDiskBytesOnDataVolume)
    {
        const long oneMb = 1024L * 1024L;

        var free = Math.Max(0L, freeDiskBytesOnDataVolume);
        var target = Math.Clamp(free / WalSizingFreeDiskDivisor, WalSizingFloorBytes, WalSizingCeilingBytes);

        /* Floor to the ladder: the largest power-of-two multiple of the floor that does not exceed the clamped
           target. Bounded by the ceiling, so this is at most four doublings. */
        var maxWal = WalSizingFloorBytes;
        while (maxWal * 2 <= target)
        {
            maxWal *= 2;
        }

        var minWal = Math.Max(MinWalSizeFloorBytes, maxWal / 4);

        return new WalSettings((int)(maxWal / oneMb), (int)(minWal / oneMb));
    }

    /// <summary>
    /// Whether the v12 block emits <c>checkpoint_completion_target = 0.9</c> for a store on this PostgreSQL
    /// major: only BELOW 14, where the default was 0.5 (see
    /// <see cref="CheckpointCompletionTargetDefaultChangedMajor"/>). An unknown major (0, from an unreadable
    /// <c>PG_VERSION</c>) pins it — the pin is a no-op where the default is already 0.9 and the fix where it is
    /// not, so emitting is the answer that cannot be wrong.
    /// </summary>
    internal static bool PinsCheckpointCompletionTarget(int postgresMajor)
        => postgresMajor < CheckpointCompletionTargetDefaultChangedMajor;

    /// <summary>
    /// The <c>checkpoint_completion_target</c> clause of the v12 start line — what the block did about it and
    /// why, for the major it saw. Three shapes: pinned on a known pre-14 major (naming the major and its old
    /// 0.5 default), pinned on an unreadable major (saying so, and that the pin is harmless on 14+), or left at
    /// the named major's own 0.9 default. Pure so the wording is pinned alongside the decision it reports.
    /// </summary>
    internal static string DescribeCheckpointCompletionTarget(int postgresMajor)
    {
        if (!PinsCheckpointCompletionTarget(postgresMajor))
        {
            return FormattableString.Invariant($"left at PostgreSQL {postgresMajor}'s default {CheckpointCompletionTargetPin}");
        }

        return postgresMajor > 0
            ? FormattableString.Invariant($"pinned at {CheckpointCompletionTargetPin} (PostgreSQL {postgresMajor} defaulted to 0.5; 14 raised the default)")
            : FormattableString.Invariant($"pinned at {CheckpointCompletionTargetPin} (PG_VERSION unreadable, so the pre-14 pin is emitted: a no-op on 14+, the fix before it)");
    }

    /// <summary>
    /// The v12 stamp line for a derived sizing on a given major (#3802) — the outputs the block beneath it
    /// carries, formatted invariantly so the comparison is a plain ordinal match on a machine with any locale.
    /// Every input the block's CONTENT depends on is in here (both sizes and the major that decides the
    /// checkpoint pin), so "the last stamp equals this one" means "the block in force is the block we would
    /// write". The raw free-disk figure is deliberately NOT in it; see <see cref="ConfWalSizingStampPrefix"/>.
    /// </summary>
    internal static string BuildWalSizingStamp(WalSettings settings, int postgresMajor)
        => FormattableString.Invariant(
            $"{ConfWalSizingStampPrefix}max_wal_size_mb={settings.MaxWalSizeMb} min_wal_size_mb={settings.MinWalSizeMb} pg_major={postgresMajor}");

    /// <summary>
    /// True when the MOST RECENT v12 stamp in the conf equals <paramref name="expectedStamp"/> — the test that
    /// decides whether <see cref="BuildWalSizingConfAppend"/> needs to run (#3802). Last, not any, for the v8
    /// reason (<see cref="ConfHasCurrentHardwareFingerprint"/>): postgresql.conf takes the last occurrence, so a
    /// volume that shrank and grew back would otherwise find its old stamp still present, skip, and leave the
    /// shrunken block in force.
    /// </summary>
    internal static bool ConfHasCurrentWalSizingStamp(string conf, string expectedStamp)
        => LastLineWithPrefixEquals(conf, ConfWalSizingStampPrefix, expectedStamp);

    /// <summary>
    /// The v12 block (#3802): the marker, the stamp, one comment line recording the headroom it was derived
    /// from (so an operator reading postgresql.conf later can see WHY 8192MB without the service log), then
    /// <c>max_wal_size</c> and <c>min_wal_size</c> in whole MB, and <c>checkpoint_completion_target = 0.9</c>
    /// only on a pre-14 major. Pure: the same inputs write the same bytes, which is what lets the stamp stand
    /// for the block. Takes the total-disk figure for the comment only — nothing is derived from it.
    ///
    /// <para>Carries no <see cref="ConfHardwareFingerprintPrefix"/> line, so the v8 staleness check's invariant
    /// about what it reads is untouched; its own stamp sits under a different prefix for exactly that reason.
    /// Supersedes v4's fixed <c>max_wal_size = 4GB</c> by last-occurrence-wins and restates nothing else — the
    /// blocks compose, they do not compete.</para>
    /// </summary>
    internal static string BuildWalSizingConfAppend(long freeDiskBytesOnDataVolume, long totalDiskBytesOnDataVolume, int postgresMajor)
    {
        var settings = DeriveWalSettings(freeDiskBytesOnDataVolume);
        var builder = new StringBuilder();
        builder.Append('\n');
        builder.Append(ConfMarkerV12).Append('\n');
        builder.Append(BuildWalSizingStamp(settings, postgresMajor)).Append('\n');
        builder.Append("# derived from ").Append(FormatGb(freeDiskBytesOnDataVolume)).Append(" GB free of ")
            .Append(FormatGb(totalDiskBytesOnDataVolume)).Append(" GB on the data volume: free / ")
            .Append(WalSizingFreeDiskDivisor).Append(", floored to a power of two, clamped to ")
            .Append(WalSizingFloorBytes / (1024L * 1024L)).Append("MB..").Append(WalSizingCeilingBytes / (1024L * 1024L)).Append("MB\n");
        builder.Append("max_wal_size = ").Append(settings.MaxWalSizeMb).Append("MB\n");
        builder.Append("min_wal_size = ").Append(settings.MinWalSizeMb).Append("MB\n");
        if (PinsCheckpointCompletionTarget(postgresMajor))
        {
            builder.Append("checkpoint_completion_target = ").Append(CheckpointCompletionTargetPin).Append('\n');
        }

        return builder.ToString();
    }

    /// <summary>
    /// The WAL keys <c>postgresql.auto.conf</c> assigns, with the value each carries — the pure half of the
    /// ALTER SYSTEM check (#3802). <c>ALTER SYSTEM</c> writes one <c>name = 'value'</c> line per setting and
    /// rewrites the file on every change, so there is normally one assignment per key; the LAST one is taken
    /// regardless, because that is what PostgreSQL honours. Comment lines (the file's own "Do not edit this
    /// file manually!" header) and lines naming other settings are ignored. Null or empty text (no file, or an
    /// empty one) yields no overrides. Values are returned as written, quotes included, so the log line shows
    /// the operator exactly what the file says.
    /// </summary>
    internal static IReadOnlyList<(string Name, string Value)> FindWalSizingAutoConfOverrides(string? autoConfText)
    {
        var overrides = new List<(string Name, string Value)>();
        if (string.IsNullOrWhiteSpace(autoConfText))
        {
            return overrides;
        }

        foreach (var raw in autoConfText.Split('\n'))
        {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
            {
                continue;
            }

            var separator = line.IndexOf('=', StringComparison.Ordinal);
            if (separator <= 0)
            {
                continue;
            }

            var name = line[..separator].Trim();
            var matched = Array.Find(WalSizingSettingNames, n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase));
            if (matched is null)
            {
                continue;
            }

            var assignment = line[(separator + 1)..];
            var comment = assignment.IndexOf('#', StringComparison.Ordinal);
            var value = (comment >= 0 ? assignment[..comment] : assignment).Trim();

            /* Last assignment wins, so replace an earlier one for the same key rather than adding a second. */
            overrides.RemoveAll(o => string.Equals(o.Name, matched, StringComparison.Ordinal));
            overrides.Add((matched, value));
        }

        return overrides;
    }

    /// <summary>Bytes as whole-and-tenth gigabytes, invariant — the figure the v12 block's comment line and
    /// the start's log line both carry, so they cannot disagree about what the block was derived from.</summary>
    internal static string FormatGb(long bytes)
        => (Math.Max(0L, bytes) / (1024d * 1024d * 1024d)).ToString("F1", CultureInfo.InvariantCulture);

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
    /// Builds a non-pooled connection string for the #4215 migration's <c>pg_file_settings</c> snapshot. The
    /// snapshot is a one-shot read that must land on the server this start just launched, never on a pooled
    /// socket left over from an earlier server lifetime in the same process — the same stale-pool failure
    /// fixed in <c>DarlingStoreUpgradeTests</c> (#4397).
    /// </summary>
    private static string MigrationSnapshotConnectionString(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        builder.Pooling = false;
        return builder.ConnectionString;
    }

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
               default idle pruning shrinks the pool between bursts.

               #2819 re-derived this rather than raising it, and 24 is still right — but the arithmetic
               above was only ever true by accident. Each swept server holds ONE store connection for its
               whole body, so the sweep's demand is MaxConcurrentSweeps, not 24. Until #2819 the Query Store
               plan and text fetches each opened another connection per database on top of it.

               Two different multipliers, and only one of them is what this bound governs. Per database the
               fetches ACQUIRED three connections (body + plan + text), which is the ~228-per-cycle figure
               #2819 measured the 673-893ms floor against. But they acquire SEQUENTIALLY — readItem awaits
               the plan fetch to completion, disposing its connection, before the text fetch starts — so
               peak CONCURRENT holds per swept server were body + one in-flight fetch = 2. MaxPoolSize
               bounds concurrency, so 2x the sweep width is the number that had to fit: fine at the 4-wide
               default (8), over this bound at the 16-wide ClampConcurrentSweeps limit (32). Borrowing puts
               peak concurrent demand back at the sweep width itself, so even a 16-wide sweep now fits inside
               24 with the seams — which it demonstrably did not before.

               Raising this number would have been the wrong fix for the same reason it is bounded at all:
               every pooled connection is a postgres.exe PROCESS, and this store logged 8 "could not reserve
               shared memory region" retries on 2026-09-03. Fewer acquisitions, not a bigger pool. */
            MaxPoolSize = 24,
        };
        return builder.ConnectionString;
    }

    /// <summary>
    /// Derives the managed connection string from the stored credential WITHOUT touching the
    /// server — for secondary consumers (the MCP host) that must never bootstrap; the worker
    /// owns the lifecycle. Null until the worker's first initdb has written the credential.
    /// </summary>
    [SupportedOSPlatform("windows")]
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
    [SupportedOSPlatform("windows")]
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
    [SupportedOSPlatform("windows")]
    public static string? TryBuildViewerConnectionStringFromStoredCredential(PostgresConfig config)
    {
        var credentialPath = ViewerCredentialPathFor(ResolveDataDirectory(config));
        if (!File.Exists(credentialPath))
        {
            return null;
        }

        return BuildRoleConnectionString(config.Port, ViewerRoleName, DarlingSecrets.Unprotect(File.ReadAllText(credentialPath).Trim()));
    }

    /// <summary>#4280: the real-start fallback runs only for a "trial-passed" carry, and never for a requested
    /// cancellation. A service stop during the first real start after an upgrade must not drop settings that passed
    /// their trial.</summary>
    [SupportedOSPlatform("windows")]
    internal static bool ShouldFallBackToHeaderOnly(DarlingStoreUpgrade.AutoConfCarryMarker? marker, CancellationToken cancellationToken) =>
        marker is { State: DarlingStoreUpgrade.AutoConfCarryStateTrialPassed } && !cancellationToken.IsCancellationRequested;

    /// <summary>
    /// The whole first-run story, idempotent: locate/unpack the runtime, initdb if the data
    /// directory has no cluster, self-heal the conf append, start the server if nothing is
    /// listening on the data directory, create the darling database if missing, and return the
    /// ready-to-use connection string. Throws (actionably) on any failure — the worker logs it
    /// critical and exits cleanly.
    /// </summary>
    [SupportedOSPlatform("windows")]
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

        /* Declared before the upgrade branch (#4280 round-2 part 2, item 2), not called eagerly: cert
           generation only needs to run once BuildServerRuntimeOptions or the auto.conf carry trial actually
           needs the SSL options, and BuildNetworkPlan itself never throws, so deferring it costs nothing.
           NetworkPlan/NetworkMode stay private to this class — every consumer outside it (UpgradeContext's own
           delegate included) sees only the Func<string> built from networkPlan.Value below. */
        var networkPlan = new Lazy<NetworkPlan>(BuildNetworkPlan);

        /* #3908: whether this start found a cluster, captured before initdb can create one. A new cluster has no
           extension for the quiesced update to move. */
        var existingCluster = File.Exists(Path.Combine(_dataDirectory, "PG_VERSION"));
        if (!existingCluster)
        {
            await InitializeClusterAsync(binDirectory, cancellationToken);

            /* Read here too, so this first start records the new store's TimescaleDB state under this runtime and
               the second start has nothing to read (#3908). */
            _bundledTimescaleVersion = ReadBundledTimescaleVersion(binDirectory);
        }
        else
        {
            /* #1706: an EXISTING data directory may have been created by an older PostgreSQL than the one
               the package now ships. This is the only window in which an in-place major upgrade can run —
               nothing is connected, and both runtimes are on disk. It either upgrades, does nothing, or
               reverts and leaves the store exactly as it was. */
            /* #3909: before ANYTHING can start this cluster on PostgreSQL 17 binaries (the upgrade's old-cluster
               start just below, a reverted upgrade's restart, or a plain start of a store still on 17), make
               sure its conf is one 17 will open. Legacy conf only (#4215): once the conf has migrated,
               maintenance_work_mem's value lives in darling-managed.conf, which this heal never touches — a
               migrated store's cap is the render's own job, not this append. */
            if (ManagedConfMigrationState.Classify(_dataDirectory) == ManagedConfMigrationState.Kind.Legacy)
            {
                HealLegacyMaintenanceWorkMem(_dataDirectory);
            }

            await EnsureDataDirectoryMajorAsync(binDirectory, networkPlan, cancellationToken);
        }

        /* #4280: a server CarryAutoConfAsync's auto.conf trial left on a private port (the confirmed stop in
           its own try/finally failed) must be stopped before IsRunningAsync below, which cannot tell it apart
           from the store's own postmaster — pg_ctl status answers "running" for a postmaster on ANY port.
           Unconditional: an absent marker (the overwhelmingly common case — the trial confirms its own stop)
           is a no-op read, same as the existing post-Timescale-update call further down. */
        if (!await _storeUpgrade.StopQuiescedUpdateOrphanAsync(binDirectory, _dataDirectory))
        {
            throw new InvalidOperationException(QuiescedOrphanMessage(binDirectory));
        }

        /* #4280 item 4: a "carrying" marker here means a PREVIOUS start's carry never reached trial-passed —
           crash, power loss, or an SCM kill between the marker write and the trial. Read once and kept for the
           real-start fallback below too: nothing between here and there touches this marker, and a
           "trial-passed" marker is this same call's own carry (or a previous one that got as far as a verified
           trial), which item 4 leaves alone — only item 2's fallback, at the real start, consumes that one. */
        var autoConfCarryMarker = _storeUpgrade.TryReadAutoConfCarryMarker(_dataDirectory);
        if (autoConfCarryMarker is { State: DarlingStoreUpgrade.AutoConfCarryStateCarrying })
        {
            /* #4280 item 2: a failed reset (the header-only write itself threw) never blocks the start — the
               unverified settings ride into the real start either way, same as before this recovery existed.
               Only clear the marker when the reset actually ran, so a write failure leaves it for the next
               start to retry rather than losing track of the stuck carry. */
            if (await _storeUpgrade.ResetAutoConfCarryAsync(
                _dataDirectory, autoConfCarryMarker.Value,
                "a previous start's postgresql.auto.conf carry never finished"))
            {
                autoConfCarryMarker = null;
            }
        }

        /* #4215: the classifier reads the conf's own state ONCE, before the
           legacy appenders can run. Only a Legacy conf (a v-marker present, or the include missing) may
           append — a PendingVerify/Verified/MigratedUnstamped conf already carries the migrated file, and
           EnsureConfAppended appends at the END of postgresql.conf, which would override both the managed
           file's values AND any operator line this migration moved below the include. */
        var confState = ManagedConfMigrationState.Classify(_dataDirectory);
        if (confState == ManagedConfMigrationState.Kind.Legacy)
        {
            EnsureConfAppended(_dataDirectory);
        }

        var password = ReadStoredPassword();

        /* #3908: move the store's TimescaleDB extension to this runtime's version BEFORE the store opens, on a
           private port with TimescaleDB's background workers off, so the only sessions are the update's own. After
           the conf append, so the cluster starts on the conf it will run with; before the network plan and the
           start, so the configured port is unbound and no web, MCP or Viewer session can reach it. Gated on the
           data directory's record (DarlingStoreUpgrade.NeedsQuiescedTimescaleUpdate): a store already on the
           runtime's version costs nothing, and one not read yet (a new store's second start included) costs one
           extra start and stop. Never on a server
           this service did not start, which cannot be quiesced; the post-start read reports that one. It never
           reverts the runtime: every store this service has shipped is on a TimescaleDB whose libraries the
           runtime carries, so a store whose update failed opens on its own version, and the alert says so. */
        var alreadyRunning = await IsRunningAsync(binDirectory, cancellationToken);

        /* A re-entry (the worker's bootstrap retry) finds the server this process already started. The quiesced
           step cannot run under it, and the attempt that did run it holds the outcome to report. */
        if (!(alreadyRunning && _startedByThisProcess))
        {
            LastTimescaleOutcome = DarlingStoreUpgrade.TimescaleUpdateOutcome.None;
        }

        /* The data directory's major must be the runtime's: after an upgrade whose runtime revert could not run
           (#3927), a start here would put the new binaries on the old cluster. */
        if (existingCluster
            && !alreadyRunning
            && _bundledMajor > 0
            && DarlingStoreUpgrade.TryReadDataDirectoryMajor(_dataDirectory) == _bundledMajor
            && DarlingStoreUpgrade.NeedsQuiescedTimescaleUpdate(DarlingStoreUpgrade.ReadTimescaleRecord(_dataDirectory), _bundledTimescaleVersion))
        {
            LastTimescaleOutcome = await _storeUpgrade.UpdateTimescaleQuiescedAsync(
                binDirectory, _dataDirectory, password, _bundledTimescaleVersion!, cancellationToken);

            /* It confirms its own stop. When it could not, one more attempt, and then a refusal rather than
               adopting the private-port server as the store. */
            if (!await _storeUpgrade.StopQuiescedUpdateOrphanAsync(binDirectory, _dataDirectory))
            {
                throw new InvalidOperationException(QuiescedOrphanMessage(binDirectory));
            }
        }

        /* Resolve the opt-in network exposure (darling-network-endpoints) BEFORE start so listen_addresses
           and the ssl trio can ride the -o runtime override. Fail-closed: an invalid/incomplete exposure
           config (or a cert-gen failure) returns a LOOPBACK plan carrying the degrade reason, logged
           critical here; the reconcile below then runs the FULL disable-reconcile (loopback listen + pg_hba
           block removed + reload + verify), so a previously-exposed cluster actually closes. The whole
           network path is caught internally (BuildNetworkPlan swallows cert-gen failure into a degrade;
           ReconcileNetworkAsync never throws) because EnsureRunningAsync's contract is throw => service-exit,
           and a typo in an optional, default-off endpoint must NEVER take collection down (Round 4 #3). */
        if (networkPlan.Value.DegradeReason is not null)
        {
            _logger.LogCritical(
                "Store network exposure DISABLED (degraded to loopback-only): {Reason}. Fix postgres.network and restart to expose the store.",
                networkPlan.Value.DegradeReason);
        }

        if (alreadyRunning)
        {
            /* Already running — a previous service crash's surviving postmaster, or an operator
               started it by hand. Use it, never stop it (the flag stays false). */
            _logger.LogInformation(
                "Managed Postgres is already running for {DataDirectory} — connecting to it (this service did not start it and will not stop it)",
                _dataDirectory);
        }
        else
        {
            /* #4215: the one service-owned settings file, rendered and validated right before the start it
               takes effect on — never for the adopted-listener branch above, which does not start anything
               this file could take effect on until the next service-owned start anyway. Skipped
               on Legacy (no managed file exists yet — the old blocks are still what's in force) and on
               PendingVerify (a crash left the migrated files exactly where the last attempt wrote them; Step A
               must not re-derive before the stamp exists, or ResumePending's before/after comparison below
               would be comparing against a snapshot the render itself just changed). */
            if (confState == ManagedConfMigrationState.Kind.Verified || confState == ManagedConfMigrationState.Kind.MigratedUnstamped)
            {
                await EnsureManagedConfReadyAsync(binDirectory, _dataDirectory, cancellationToken);
            }

            try
            {
                await StartServerAsync(binDirectory, networkPlan.Value, cancellationToken);
            }
            catch (Exception) when (ShouldFallBackToHeaderOnly(autoConfCarryMarker, cancellationToken))
            {
                /* #4280 item 2: the trial proved these names alone, on a private port with its own SSL
                   options — the real start can still fail for a reason outside that scope (the configured
                   port, the actual network exposure, timing). One retry on an empty postgresql.auto.conf,
                   the same recovery the trial's own combined check uses; a second failure throws as-is,
                   unwrapped, same as before this fallback existed. */
                /* autoConfCarryMarker! — ShouldFallBackToHeaderOnly above already proved this non-null (its
                   whole first clause is a null-checking pattern match on it); the compiler cannot see that
                   through the opaque method call the way it narrows an inline `is {...}` pattern. If the
                   reset itself could not write header-only, retrying against the same unwritable file cannot
                   help — rethrow the original start failure rather than mask it behind a doomed retry. */
                if (!await _storeUpgrade.ResetAutoConfCarryAsync(
                    _dataDirectory, autoConfCarryMarker!.Value,
                    "the real start failed even though these settings passed an isolated trial"))
                {
                    throw;
                }

                await StartServerAsync(binDirectory, networkPlan.Value, cancellationToken);
            }

            _startedByThisProcess = true;

            /* Guarded — Legacy and PendingVerify never ran EnsureManagedConfReadyAsync above, so
               darling-managed.conf may not exist yet on this start. Copying a missing file would throw and take
               the whole start down over what SaveLastGoodManagedConf's own doc comment already treats as a
               no-op-worthy failure. On a Verified confState this start's server started on a FRESH render
               that Step B (MigrateManagedConfAsync, below) has not verified yet (#4336) — saving here would
               let a render Step B goes on to reject become the fallback a future rejected render restores
               to. That save happens only once Step B verifies, further down. Every other confState (Legacy,
               PendingVerify, MigratedUnstamped) has no Step B to wait on, so the save still belongs here. */
            if (confState != ManagedConfMigrationState.Kind.Verified
                && File.Exists(Path.Combine(_dataDirectory, ManagedConfFile.FileName)))
            {
                SaveLastGoodManagedConf(_dataDirectory);
            }
        }

        /* Covers all three ways this point is reached with nothing left pending: already running (no start
           attempted here), the real start succeeding outright, or the fallback's retry succeeding (which
           already deleted the marker as part of ResetAutoConfCarryAsync above — a harmless no-op here). */
        if (autoConfCarryMarker is not null)
        {
            DarlingStoreUpgrade.TryDeleteAutoConfCarryMarker(_dataDirectory);
        }

        var connectionString = BuildConnectionString(_config.Port, password);
        await EnsureDatabaseAsync(connectionString, cancellationToken);

        /* #1706: everything a major upgrade needs from a LIVE server — verify it landed and run the post-upgrade
           analyze staging. Deliberately AFTER the start above rather than inside the upgrade, so the server this
           process started stays one this process will stop. The TimescaleDB update this used to include runs
           before the start now (#3908). */
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
                cancellationToken);
        }

        /* #3908: read back where the store's TimescaleDB ended up, record it, and settle what this start reports. */
        if (!string.IsNullOrEmpty(_bundledTimescaleVersion))
        {
            var (timescale, installed) = await _storeUpgrade.VerifyTimescaleAfterStartAsync(
                connectionString, _dataDirectory, _bundledTimescaleVersion, LastTimescaleOutcome, cancellationToken);
            LastTimescaleOutcome = timescale;

            /* A major upgrade's own alert names the TimescaleDB versions too. pg_upgrade restored the store's
               version and the quiesced update moved it, or did not: report where it ended up, and leave the
               extension out of that alert when it did not move, so the two alerts cannot disagree. */
            if (LastUpgradeOutcome.Status == DarlingStoreUpgrade.StoreUpgradeStatus.Succeeded && installed is not null)
            {
                LastUpgradeOutcome = LastUpgradeOutcome with
                {
                    ToTimescale = string.Equals(installed, LastUpgradeOutcome.FromTimescale, StringComparison.Ordinal) ? null : installed,
                };
            }
        }

        /* Reconcile pg_hba + reload + verify, the adopted-listener guard, and the firewall against the LIVE
           server — symmetric (present when exposed, absent when loopback/degraded). Never throws (Round 4 #3):
           a network reconcile failure logs + degrades, it does not abort the bootstrap. */
        await ReconcileNetworkAsync(binDirectory, networkPlan.Value, connectionString, cancellationToken);

        /* Only when THIS process started the server — an adopted listener's conf takes effect
           on the next service-owned start, same rule EnsureManagedConfReadyAsync above already follows, and
           the migration's own re-snapshot needs a server that is actually up on the files this start wrote. */
        if (_startedByThisProcess)
        {
            var migrationOutcome = await MigrateManagedConfAsync(confState, connectionString, cancellationToken);

            /* #4336: a bootstrap retry can land here on a Verified confState with no write result (Step B's
               own null-return case, above) — nothing new to report, not a fact that the earlier attempt's
               outcome is now unknown. Keep the prior non-null outcome rather than erasing it with null. */
            if (migrationOutcome is not null || LastManagedConfVerification is null)
            {
                LastManagedConfVerification = migrationOutcome;
            }
        }

        return connectionString;
    }

    /// <summary>
    /// Runs Step A, resumes a pending Step A, or re-verifies a hand-edited migrated conf — whichever
    /// <paramref name="confState"/> calls for. <see
    /// cref="ManagedConfMigrationState.Kind.Verified"/> does nothing here; Step B runs separately. Everything
    /// is caught: a migration failure logs and reports, it never throws — the store this start already
    /// brought up must not go down over a verification step.
    /// </summary>
    [SupportedOSPlatform("windows")]
    private async Task<ManagedConfMigrationOutcome?> MigrateManagedConfAsync(
        ManagedConfMigrationState.Kind confState, string connectionString, CancellationToken cancellationToken)
    {
        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot = async ct =>
        {
            await using var connection = new NpgsqlConnection(
                DarlingStoreConnection.PinSessionTimeZoneUtc(MigrationSnapshotConnectionString(connectionString)));
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand(ManagedConfFileSettings.SnapshotSql, connection)
            {
                CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds,
            };
            await using var reader = await command.ExecuteReaderAsync(ct);
            var rows = new List<FileSettingRow>();
            while (await reader.ReadAsync(ct))
            {
                rows.Add(new FileSettingRow(
                    SourceFile: reader.IsDBNull(0) ? null : reader.GetString(0),
                    SourceLine: reader.IsDBNull(1) ? null : reader.GetInt32(1),
                    Name: reader.IsDBNull(2) ? null : reader.GetString(2),
                    Setting: reader.IsDBNull(3) ? null : reader.GetString(3),
                    Applied: !reader.IsDBNull(4) && reader.GetBoolean(4),
                    Error: reader.IsDBNull(5) ? null : reader.GetString(5)));
            }

            return rows;
        };

        try
        {
            ManagedConfMigrationOutcome outcome;
            switch (confState)
            {
                case ManagedConfMigrationState.Kind.Legacy:
                {
                    var postgresMajor = DarlingStoreUpgrade.TryReadDataDirectoryMajor(_dataDirectory) ?? 0;
                    var inputs = GatherManagedConfRenderInputs(_dataDirectory, postgresMajor);
                    var derived = new Dictionary<string, string>(StringComparer.Ordinal);
                    foreach (var (_, name, value) in ParseConfText(ManagedConfFile.RenderBody(inputs)))
                    {
                        derived[name] = value;
                    }

                    outcome = await ManagedConfMigrationRunner.RunStepA(
                        _dataDirectory, snapshot, derived, inputs, _config.Port, DateTime.UtcNow, _logger, cancellationToken);
                    break;
                }

                case ManagedConfMigrationState.Kind.PendingVerify:
                {
                    var backupPath = Directory.GetFiles(_dataDirectory, "postgresql.conf.pre-4215.*.bak");
                    if (backupPath.Length == 0)
                    {
                        /* #4336: a PendingVerify conf with no backup has nothing this method can restore
                           to — the migrated files stay exactly where the last attempt left them, unverified.
                           Reporting Failed (not Unknown) so the store-settings alert actually fires; Unknown
                           never fires it alone, and a stuck PendingVerify must not go silent forever. */
                        _logger.LogWarning(
                            "{DataDirectory} has a pending #4215 migration but no backup file — cannot resume; reporting Failed.",
                            _dataDirectory);
                        return new ManagedConfMigrationOutcome(
                            ManagedConfVerificationStatus.Failed, Array.Empty<string>(), null, ManagedConfMigrationStep.A,
                            "resume: no backup file found for a PendingVerify conf");
                    }

                    Array.Sort(backupPath, StringComparer.Ordinal);
                    outcome = await ManagedConfMigrationRunner.ResumePending(_dataDirectory, snapshot, backupPath[0], cancellationToken, _logger);
                    break;
                }

                case ManagedConfMigrationState.Kind.MigratedUnstamped:
                {
                    /* A hand edit of darling-managed.conf, or a crash inside Step B — those two cases look the
                       same here: migrated, no pending file, stale stamp.
                       Re-verify against what is on disk NOW: no new error row may come from darling-managed.conf
                       relative to the file's own current bytes — the file itself is the ground truth once no
                       pending snapshot survives to compare against. */
                    var rows = await snapshot(cancellationToken);
                    var managedConfPath = Path.Combine(_dataDirectory, ManagedConfFile.FileName);
                    var newErrorFromManagedFile = false;
                    var mismatchedKeys = new List<string>();
                    foreach (var row in rows)
                    {
                        if (row.Error is not null && row.SourceFile is not null
                            && string.Equals(Path.GetFileName(row.SourceFile), ManagedConfFile.FileName, StringComparison.OrdinalIgnoreCase))
                        {
                            newErrorFromManagedFile = true;
                            if (row.Name is not null)
                            {
                                mismatchedKeys.Add(row.Name);
                            }
                        }
                    }

                    if (newErrorFromManagedFile)
                    {
                        outcome = new ManagedConfMigrationOutcome(
                            ManagedConfVerificationStatus.Failed, mismatchedKeys, null, ManagedConfMigrationStep.A);
                        break;
                    }

                    var managedConfText = File.Exists(managedConfPath) ? File.ReadAllText(managedConfPath) : string.Empty;
                    ManagedConfMigrationSteps.WriteVerifiedStamp(_dataDirectory, managedConfText);
                    _logger.LogWarning(
                        "{Path} was changed outside the service; operator settings belong below the include in postgresql.conf, or in ALTER SYSTEM; the service re-renders this file.",
                        managedConfPath);
                    outcome = new ManagedConfMigrationOutcome(
                        ManagedConfVerificationStatus.Verified, Array.Empty<string>(), null, ManagedConfMigrationStep.A);
                    break;
                }

                case ManagedConfMigrationState.Kind.Verified:
                {
                    /* Step B: only when this start's own WriteManagedConfFile call actually
                       wrote a new darling-managed.conf does it have a previous text and the RenderInputs to
                       verify against; a start that found the same bytes already in force has nothing to do. */
                    if (LastManagedConfWriteResult is not { Written: true, PreviousText: var previousText, Inputs: { } inputs })
                    {
                        return null;
                    }

                    var renderedText = LastManagedConfWriteResult.Value.RenderedText;
                    var rows = await snapshot(cancellationToken);
                    outcome = ManagedConfMigrationRunner.VerifyStepB(_dataDirectory, rows, renderedText, previousText);

                    var changes = ManagedConfMigrationRunner.DiffStepBChanges(previousText, renderedText);
                    var managedConfPathB = Path.Combine(_dataDirectory, ManagedConfFile.FileName);
                    if (outcome.Status == ManagedConfVerificationStatus.Verified)
                    {
                        _logger.LogInformation(
                            "{Path} verified against pg_file_settings:\n{Changes}",
                            managedConfPathB, ManagedConfMigrationRunner.FormatStepBChangeLog(changes, inputs));
                    }
                    else
                    {
                        _logger.LogWarning(
                            "{Path} failed verification against pg_file_settings for {Keys}; the previous verified file was restored.",
                            managedConfPathB, string.Join(", ", outcome.MismatchedKeys));
                    }

                    break;
                }

                default:
                    return null;
            }

            LogMigrationOutcome(outcome);

            if (outcome.Status == ManagedConfVerificationStatus.Verified)
            {
                if (File.Exists(Path.Combine(_dataDirectory, ManagedConfFile.FileName)))
                {
                    SaveLastGoodManagedConf(_dataDirectory);
                }
            }

            return outcome;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var detail = FormattableString.Invariant($"outer: {ex.GetType().Name}: {ex.Message}");
            _logger.LogWarning(ex, "The #4215 conf migration failed for {DataDirectory}; the store keeps running on its current conf. {Detail}", _dataDirectory, detail);
            var failedStep = confState == ManagedConfMigrationState.Kind.Verified
                ? ManagedConfMigrationStep.B
                : ManagedConfMigrationStep.A;
            return new ManagedConfMigrationOutcome(
                ManagedConfVerificationStatus.Unknown, Array.Empty<string>(), null, failedStep, detail);
        }
    }

    /// <summary>Logs a <see cref="MigrateManagedConfAsync"/> outcome once: Information for a
    /// clean Verified, Warning for Failed or Unknown — naming the backup path and the mismatched keys so an
    /// operator has somewhere to look.</summary>
    [SupportedOSPlatform("windows")]
    private void LogMigrationOutcome(ManagedConfMigrationOutcome outcome)
    {
        if (outcome.Status == ManagedConfVerificationStatus.Verified)
        {
            _logger.LogInformation(
                "#4215 conf migration verified for {DataDirectory} (step {Step}).", _dataDirectory, outcome.Step);
            return;
        }

        _logger.LogWarning(
            "#4215 conf migration {Status} for {DataDirectory} (step {Step}); backup {BackupPath}; mismatched keys: {MismatchedKeys}; detail: {Detail}.",
            outcome.Status, _dataDirectory, outcome.Step, outcome.BackupPath ?? "(none)", string.Join(", ", outcome.MismatchedKeys), outcome.Detail ?? "(none)");
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
    [SupportedOSPlatform("windows")]
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
            /* #3908: a server the quiesced TimescaleDB update left on its private port is stopped first. Adopted,
               it would defer the runtime update below and every one after it, and the normal start could not
               reach it on the configured port. */
            if (!await _storeUpgrade.StopQuiescedUpdateOrphanAsync(binDirectory, _dataDirectory))
            {
                throw new InvalidOperationException(QuiescedOrphanMessage(binDirectory));
            }

            if (File.Exists(_runtimeZipPath))
            {
                _runtimeAdvance = await _storeUpgrade.TryAdvanceRuntimeAsync(
                    _runtimeRoot, _runtimeZipPath, _dataDirectory, TryIsRunningAsync, cancellationToken);
            }

            DarlingStoreUpgrade.PinLegacyRuntimeStamp(_runtimeRoot, _logger);
            return binDirectory;
        }

        if (File.Exists(_runtimeZipPath))
        {
            /* #3908: an existing store whose runtime folder is gone (a clean reinstall of this release over a
               later one) must not get a runtime that cannot open its TimescaleDB. The swap path already refuses
               that; this is the same check for the path that extracts with nothing to swap. */
            if (DarlingStoreUpgrade.MissingTimescaleLibraries(_dataDirectory, _runtimeZipPath) is { Count: > 0 } missing)
            {
                throw new InvalidOperationException(
                    $"The store at {_dataDirectory} is on TimescaleDB {string.Join(" or ", missing)} (recorded in {Path.Combine(_dataDirectory, DarlingStoreUpgrade.TimescaleRecordFileName)}), " +
                    $"and {_runtimeZipPath} carries no libraries for it, so the runtime it would extract could not open the store. " +
                    "Install a release whose runtime carries that TimescaleDB version, or restore the pg-runtime folder that last opened this store. Nothing has been extracted or changed.");
            }

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
                /* #3908: a fresh install needs the pin too. Its store is created on this runtime's TimescaleDB,
                   which a rolled-back 3.3-3.8 runtime cannot load. */
                DarlingStoreUpgrade.PinLegacyRuntimeStamp(_runtimeRoot, _logger);
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
    [SupportedOSPlatform("windows")]
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
    ///
    /// <para>Windows-attributed as of #3802 because the v12 heal reads the data directory's major through
    /// <see cref="DarlingStoreUpgrade.TryReadDataDirectoryMajor"/>, whose class is Windows-only. Nothing about
    /// the attribute is new in substance: the constructor already carries it, so every instance method here
    /// has only ever been reachable on Windows.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal void EnsureConfAppended(string dataDirectory)
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
                "Appended v7 compression memory to postgresql.conf (maintenance_work_mem = {Maintenance}MB from min(max(5% RAM, 1536MB), 25% RAM, 2047MB); TimescaleDB compression sorts on this setting)",
                DeriveMemorySettings(v7RamBytes).MaintenanceWorkMemMb);
        }

        /* v8 (#2845): the ONE block not keyed on a version marker. Every check above asks "is this marker
           absent?", which is true exactly once in a cluster's life, so none of them notices that the host
           it derived from has been replaced underneath it. This asks "was the sizing derived under the
           hardware we are on now?" by comparing the LAST fingerprint in the file, and re-states the
           host-derived settings when it was not.

           Runs unconditionally rather than only for pre-existing clusters, including straight after a
           fresh initdb has just written v3 with identical values. The redundant first block is the price of
           a simple invariant — after any start, the conf carries a fingerprint for the CURRENT host — and
           without recording one on the first start there would be nothing for the second start to compare
           against. It converges immediately: the next start finds its own fingerprint and rewrites nothing.

           REPLACES rather than appends (#4207). A fingerprint change used to append a fresh block, and
           because the fingerprint includes the worker count, which moves with the hypertable count, each
           field store had grown three copies by the time #4207 was filed. ReplaceOrAppendHardwareSizingBlock
           rewrites the FIRST existing block in place and drops every other copy, so any fingerprint change —
           a resize or a hypertable-count change alike — now costs one rewritten block, never a growing file.
           That also removes the one remaining reason #2845 considered for splitting the worker count into
           its own fingerprint (so a hypertable-count change would not re-trigger the memory lines): with an
           in-place rewrite a worker-only change is exactly as cheap as a memory-only one, so the single
           fingerprint stays single rather than gaining a second axis with nothing left to buy. */
        /* INVARIANT this check depends on: `conf` was read ONCE at the top of this method, before v1-v7
           may have appended. That is safe only because none of them emits a line carrying
           ConfHardwareFingerprintPrefix, so nothing appended above can change this answer. A future version
           block that DID write a fingerprint line would be silently invisible here and the staleness check
           would quietly stop checking — re-read the file at that point rather than adding the block above.
           v12 (#3802) is the second every-start heal and carries its OWN stamp under ConfWalSizingStampPrefix,
           which is not a substring of this prefix (pinned), so it neither disturbs this read nor is disturbed
           by it; it runs below and reads the same once-read `conf` under the same reasoning. */
        var hypertableCount = TimescaleSupport.HypertableCount;
        var v8Authoritative = TryGetAuthoritativePhysicalMemoryBytes(out var v8RamBytes);
        var v8Fingerprint = BuildHardwareFingerprint(v8RamBytes, hypertableCount);
        if (!v8Authoritative)
        {
            /* No authoritative RAM reading, so we cannot tell whether the hardware changed. Leave whatever
               block is currently in force alone rather than re-deriving from the best-effort guess: the
               guess is a live GC figure well under true RAM, and unlike the marker-gated blocks this check
               runs on EVERY start, so acting on it would both append a block per blip and shrink the
               planner's cache estimate on a box that is fine. */
            _logger.LogWarning(
                "Skipped the v8 hardware-sizing check: total physical memory could not be read authoritatively, so a hardware change cannot be distinguished from a failed reading. The existing sizing block stays in force.");
        }
        else
        {
            /* Quantize ONCE here and pass the result down, so the values logged are necessarily the values
               written. Deriving the log line separately from the raw reading made them disagree near a GB
               boundary — a 31.5 GB host writes effective_cache_size 24576MB but logged 24192MB, a number that
               appears nowhere in the file. QuantizeRam is idempotent, so the call below still quantizes and
               still gets the same answer. Built unconditionally (not only once ShouldAppendHardwareSizing
               says yes): #4207's stale-content condition needs the text THIS build would write to compare
               against what is already there, so the decision itself depends on this value. */
            var v8QuantizedRam = QuantizeRam(v8RamBytes);
            var v8Append = BuildHardwareSizingConfAppend(v8QuantizedRam, hypertableCount);

            if (ShouldAppendHardwareSizing(conf, v8Authoritative, v8Fingerprint, v8Append))
            {
                /* Which of #4207's three conditions fired, for the log line below. Classified against the
                   same `conf` snapshot ShouldAppendHardwareSizing just decided on, in the same priority
                   order that function checks them in — not against the re-read below, though the answer is
                   identical either way (see the INVARIANT comment above: none of v1-v7 can introduce, remove
                   or move a v8 span). */
                var v8Reason =
                    !ConfHasCurrentHardwareFingerprint(conf, v8Fingerprint) ? "hardware fingerprint changed"
                    : FindHardwareSizingBlockSpans(conf).Count > 1 ? "duplicate v8 blocks found, #4207"
                    : "existing block content is stale, #4207";

                /* Re-read rather than reuse the `conf` snapshot from the top of this method: v1-v7 above may
                   have just appended their own healing blocks straight to disk (File.AppendAllText, bypassing
                   `conf` entirely), and rewriting the whole file from the stale snapshot would silently drop
                   them. None of v1-v7 can itself contain a v8 span, so this re-read cannot move or hide one. */
                var v8CurrentConf = File.ReadAllText(confPath);
                var v8PriorCopies = FindHardwareSizingBlockSpans(v8CurrentConf).Count;
                File.WriteAllText(confPath, ReplaceOrAppendHardwareSizingBlock(v8CurrentConf, v8Append));

                var v8Settings = DeriveMemorySettings(v8QuantizedRam);
                var v8Workers = DeriveWorkerSettings(hypertableCount);
                _logger.LogInformation(
                    "{Action} v8 hardware sizing in postgresql.conf ({Reason}; host RAM {RamMb} MB, {Hypertables} hypertables -> effective_cache_size {EffectiveCache}MB, maintenance_work_mem {Maintenance}MB, work_mem {WorkMem}MB, timescaledb.max_background_workers {BgWorkers}, max_worker_processes {WorkerProcesses}; shared_buffers deliberately NOT re-derived, see #2845){CollapseNote}",
                    v8PriorCopies == 0 ? "Appended" : "Rewrote", v8Reason, v8QuantizedRam / (1024L * 1024L), hypertableCount,
                    v8Settings.EffectiveCacheSizeMb, v8Settings.MaintenanceWorkMemMb, v8Settings.WorkMemMb,
                    v8Workers.MaxBackgroundWorkers, v8Workers.MaxWorkerProcesses,
                    v8PriorCopies > 1 ? $" (collapsed {v8PriorCopies} copies into 1, #4207)" : string.Empty);
            }
        }

        /* Checked independently of v1-v8, and placed AFTER v8 on purpose: v8 keys on the last fingerprint
           line in the text it read at the top of this method, so a block appended before it must not carry
           one. Every block from here down carries no sizing and no fingerprint, which is what keeps that
           check reading what it thinks it reads. Effective on this start (SIGHUP-context, appended before
           pg_ctl start). */
        if (!conf.Contains(ConfMarkerV9, StringComparison.Ordinal))
        {
            File.AppendAllText(confPath, BuildTimeZoneConfAppend());
            _logger.LogInformation(
                "Appended v9 session time zone to postgresql.conf (timezone = 'UTC'): the store's timestamp columns hold naive UTC, so a host-derived session zone would shift any comparison against now() and render timestamptz output on a different clock than the collected data.");
        }

        /* Checked independently of v1-v9: a data directory this build's initdb did not create heals by
           GAINING the message-locale block (#3053). PostgreSQL translates the SEVERITY LABEL as well as the
           body, so a translated catalogue writes a token StoreLogClassifier does not recognise — which then
           cannot reach its unclassified-retained residue class and is counted as routine with its text
           dropped. The initdb call already passes --locale=C, so this states the parser's dependency rather
           than repairing a fresh install; see ConfMarkerV10 for what that distinction does and does not buy.
           SIGHUP-context and appended before pg_ctl start, so effective on this very start. */
        if (!conf.Contains(ConfMarkerV10, StringComparison.Ordinal))
        {
            File.AppendAllText(confPath, BuildMessageLocaleConfAppend());
            _logger.LogInformation(
                "Appended v10 message locale to postgresql.conf (lc_messages = 'C'): PostgreSQL translates its severity labels under lc_messages, and the store's own log parser matches them as English tokens.");
        }

        /* Checked independently of v1-v10: an existing cluster heals by GAINING the job-execution-logging
           block (#3175). This setting was written into the v1 block by #1681, and the v1 check above is the
           one that a pre-existing cluster always answers "present" — so the GUC only ever reached a fresh
           initdb, and every store older than that release has had timescaledb_information.job_history empty
           the whole time. Its own marker is the whole fix; see ConfMarkerV11 for why widening v1's match
           would have re-applied that shared block's list-valued shared_preload_libraries and its
           listen_addresses, both measured to change behaviour. SIGHUP-context and appended before pg_ctl
           start, so effective on this very start when the service owns it. */
        if (!conf.Contains(ConfMarkerV11, StringComparison.Ordinal))
        {
            File.AppendAllText(confPath, BuildJobExecutionLoggingConfAppend());
            _logger.LogInformation(
                "Appended v11 job execution logging to postgresql.conf ({Setting} = on): timescaledb_information.job_history records one row per background-job run, and without this it stays EMPTY — a maximum over it returns no rows, which reads as 'no run exceeded the line' rather than 'this instrument is off'. Logging starts from this start onward if the service owns it, otherwise from the next start it owns; runs before that point wrote nothing and CANNOT be recovered. job_stats remains the unconditional surface for a store that has not yet healed.",
                StoreSelfMetrics.JobExecutionLoggingSetting);
        }

        /* v12 (#3802): the second every-start heal, keyed like v8 on a stamp rather than on its marker's
           absence, because what it derives from — the free space on the data volume — is a property of the box
           that changes under a running store. It re-states max_wal_size (superseding v4's fixed 4GB by
           last-occurrence-wins) and min_wal_size from that headroom, on a power-of-two ladder so ordinary
           free-disk drift is not a change; and it pins checkpoint_completion_target only on a pre-14 major,
           where the default was 0.5. Placed LAST, after v9-v11, for the reason those are placed after v8: it
           carries a stamp line, and every block between v8 and here must not. Its stamp sits under its own
           prefix, so the v8 read above is untouched (pinned: neither prefix is a substring of the other).

           Two inputs, both read from the data directory handed in rather than from the field, so the store
           upgrade's callback (#1706) sizes the freshly-initdb'd cluster from ITS volume and ITS major.
           The major comes from PG_VERSION — readable without executing anything, the DarlingStoreUpgrade rule
           — and an unreadable one derives as 0, which pins the checkpoint target (a no-op on 14+, the fix
           on anything older). The disk figure is the gate: without an authoritative DriveInfo reading this does NOTHING,
           exactly as v8 does without an authoritative RAM reading, because re-deriving a production WAL ceiling
           from a figure we could not read is worse than leaving the block in force. All three settings are
           SIGHUP-context and this runs before pg_ctl start, so a service-owned start applies them at once. */
        var v12Major = DarlingStoreUpgrade.TryReadDataDirectoryMajor(dataDirectory) ?? 0;
        if (!TryReadDataVolumeSpace(dataDirectory, out var v12FreeBytes, out var v12TotalBytes))
        {
            _logger.LogWarning(
                "Skipped the v12 WAL-sizing check: the free space on the volume holding {DataDirectory} could not be read, so a change in headroom cannot be distinguished from a failed reading. The WAL settings currently in force (the last v12 block if one exists, otherwise v4's max_wal_size = 4GB) stay in force.",
                dataDirectory);
        }
        else
        {
            var v12Settings = DeriveWalSettings(v12FreeBytes);
            var v12Stamp = BuildWalSizingStamp(v12Settings, v12Major);
            var v12CheckpointNote = DescribeCheckpointCompletionTarget(v12Major);

            /* The ALTER SYSTEM check runs whether or not the block is re-authored: the override outranks the
               block on every start, not only on the start that writes it, and an operator reading the log
               for "why is the effective value not what the product derived" needs the answer on the start
               they are looking at. */
            LogWalSizingAutoConfOverrides(dataDirectory, v12Settings);

            if (!ConfHasCurrentWalSizingStamp(conf, v12Stamp))
            {
                File.AppendAllText(confPath, BuildWalSizingConfAppend(v12FreeBytes, v12TotalBytes, v12Major));
                _logger.LogInformation(
                    "Appended v12 WAL sizing to postgresql.conf: max_wal_size {MaxWal}MB, min_wal_size {MinWal}MB from {FreeGb} GB free of {TotalGb} GB on the data volume (free / {Divisor} on the 1 GB..16 GB power-of-two ladder; supersedes v4's fixed 4GB by last-occurrence-wins); checkpoint_completion_target {CheckpointNote}. SIGHUP-context, so effective on this start when the service owns it.",
                    v12Settings.MaxWalSizeMb, v12Settings.MinWalSizeMb, FormatGb(v12FreeBytes), FormatGb(v12TotalBytes), WalSizingFreeDiskDivisor, v12CheckpointNote);
            }
            else
            {
                /* The self-proving shape (#3802): a re-derivation that changes nothing still says what it
                   derived and from what, so a start with no append is distinguishable from a start that never
                   checked. */
                _logger.LogInformation(
                    "Managed store WAL sizing (v12): max_wal_size {MaxWal}MB, min_wal_size {MinWal}MB from {FreeGb} GB free of {TotalGb} GB on the data volume — unchanged, the block in force was derived to the same rung; checkpoint_completion_target {CheckpointNote}.",
                    v12Settings.MaxWalSizeMb, v12Settings.MinWalSizeMb, FormatGb(v12FreeBytes), FormatGb(v12TotalBytes), v12CheckpointNote);
            }
        }

        /* v13 (#3899): statement statistics. Keyed on its marker's absence like v9-v11, and placed after v12
           because, like them, it carries no fingerprint or stamp line for either every-start check to misread.
           The ONE block that re-reads the file instead of trusting `conf`: its preload value is MERGED from the
           effective list, and on a fresh cluster that list was written by the v1 append above, after `conf`
           was read. The once-read text would hold only initdb's commented default, and a merge from it would
           write a list without timescaledb. The re-read follows include directives (ReadConfAssignments), so a
           list set in an included file above the block is merged rather than replaced. Restart-only, and
           appended before pg_ctl start, so a service-owned start loads the library on this start. The coverage
           check after it runs on EVERY start, because the block restates the list once and an edit made after
           that is the one it would otherwise silently override. */
        if (!conf.Contains(ConfMarkerV13, StringComparison.Ordinal))
        {
            var preloadChain = ReadConfAssignments(confPath, PreloadSetting);
            var effectivePreload = preloadChain.Count == 0 ? null : preloadChain[^1].Value;
            if (!IsValidPreloadList(effectivePreload))
            {
                /* The list in force is one PostgreSQL rejects, so it has been loading nothing from it; the block
                   below restates it as a valid list, which changes what loads at the next start. Said, not done
                   silently. */
                _logger.LogWarning(
                    "{File} line {Line} set shared_preload_libraries = '{Value}', which is not a list PostgreSQL accepts, so the store has been loading no library from it. The v13 block below restates it as '{Corrected}', which loads from the next start.",
                    preloadChain[^1].File, preloadChain[^1].Line, effectivePreload, MergePreloadLibraries(effectivePreload));
            }

            File.AppendAllText(confPath, BuildStatementStatisticsConfAppend(effectivePreload));
            _logger.LogInformation(
                "Appended v13 statement statistics to postgresql.conf (shared_preload_libraries = '{Libraries}', {Library}.track_utility = off): the store keeps per-statement timings, so a slow web-viewer or MCP read can be named by get_store_query_stats instead of guessed at. The preload is restart-only: it loads on this start when the service owns it, otherwise on the next start it owns.",
                MergePreloadLibraries(effectivePreload), StatementStatisticsLibrary);
        }

        /* v14 (#3909): keyed on the effective VALUE, not on its marker, so it heals once and then finds its own line
           in force. HealLegacyMaintenanceWorkMem runs the same check before the store upgrade's old-cluster start,
           which this method runs too late for; here it covers every other start of a store still on 17, the
           restart after a reverted upgrade included. Carries no fingerprint or stamp line for v8 or v12 to read. */
        if (FindLegacyMaintenanceWorkMemOverLimit(dataDirectory) is { } v14OverLimit)
        {
            File.AppendAllText(confPath, BuildLegacyMaintenanceWorkMemCapConfAppend());
            LogLegacyMaintenanceWorkMemCap(v14OverLimit);
        }

        /* v15 (#4246): keyed on its marker's absence like v9-v11 and v13, and placed last so it stays the
           block this method appends LAST on any start that fires it, matching its place at the end of
           AllManagedConfMarkers. Carries no fingerprint or stamp line, so v8 and v12 read exactly what they
           did before this block existed. wal_compression is superuser-context (confirmed live, not sighup),
           but still takes effect from postgresql.conf on a reload, and this is appended before pg_ctl start,
           so a service-owned start applies it on the very start that writes the block. */
        if (!conf.Contains(ConfMarkerV15, StringComparison.Ordinal))
        {
            File.AppendAllText(confPath, BuildWalVolumeConfAppend());
            _logger.LogInformation(
                "Appended v15 WAL compression to postgresql.conf (wal_compression = lz4): most of this store's WAL is full-page images, and compression shrinks every one of them. Effective on this start when the service owns it.");
        }

        LogStatementStatisticsPreloadCoverage(dataDirectory);
    }

    /* ===================== darling-managed.conf (#4215): the one service-owned settings file =====================
       EnsureConfAppended above and its v1-v15 blocks run ONLY on a Legacy conf (ManagedConfMigrationState.Classify):
       the blocks are appended when a postgresql.conf still carries them, or is missing the managed include.
       ManagedConfMigrationRunner.Rewrite (#4336) then migrates that conf, post-start, dropping every block's own
       line and adding the managed include — from the NEXT start on, darling-managed.conf is the only file that
       carries these settings. */

    /// <summary>
    /// Gathers this host's current values for <see cref="ManagedConfFile.RenderInputs"/> — the SAME readers
    /// v3/v5/v7/v8/v12/v13 already use above (<see cref="GetTotalPhysicalMemoryBytes"/>,
    /// <see cref="TryGetAuthoritativePhysicalMemoryBytes"/>, <see cref="TryReadDataVolumeSpace"/>,
    /// <see cref="TimescaleSupport.HypertableCount"/>, <see cref="ReadConfAssignments"/> for the preload list in
    /// force), so a fresh render never disagrees with what those readers would have told the old blocks.
    /// </summary>
    [SupportedOSPlatform("windows")]
    private ManagedConfFile.RenderInputs GatherManagedConfRenderInputs(string dataDirectory, int postgresMajor)
    {
        var confPath = Path.Combine(dataDirectory, "postgresql.conf");
        var ramBytes = GetTotalPhysicalMemoryBytes();
        var ramAuthoritative = TryGetAuthoritativePhysicalMemoryBytes(out _);
        var diskAuthoritative = TryReadDataVolumeSpace(dataDirectory, out var freeBytes, out var totalBytes);
        var preloadChain = ReadConfAssignments(confPath, PreloadSetting);
        var effectivePreload = preloadChain.Count == 0 ? null : preloadChain[^1].Value;

        return new ManagedConfFile.RenderInputs(
            ManagedConfFile.CurrentFormulaVersion,
            "Windows",
            ramBytes,
            ramAuthoritative,
            Environment.ProcessorCount,
            TimescaleSupport.HypertableCount,
            postgresMajor,
            freeBytes,
            totalBytes,
            diskAuthoritative,
            _config.Port,
            effectivePreload);
    }

    /// <summary>
    /// Test-only seam (#4215): when the CURRENT async flow sets this, <see
    /// cref="WriteManagedConfFile"/> applies it to the freshly rendered text before hand-edit detection or
    /// writing — so a live test can prove the rejected-value / last-good fallback path without depending on a
    /// real bug to produce a bad render. <c>AsyncLocal</c>, not a plain static field: its value flows only with
    /// the call stack that sets it (through every <c>await</c>), so a value one test's flow sets is invisible to
    /// any other test's flow running concurrently on a different one — xunit parallelizes test classes by
    /// default, and this class's own gated tests start real managed servers too. Internal, so only
    /// <c>Darling.Tests</c> can reach it (<c>InternalsVisibleTo</c>) — nothing a config file sets ever touches
    /// this; it is a delegate reference a unit test installs directly.
    /// </summary>
    internal static readonly AsyncLocal<Func<string, string>?> TestOnlyRenderOverride = new();

    /// <summary>
    /// Renders and, unless the file on disk is a hand edit (<see cref="ManagedConfFile.IsHandEdited"/>) or its
    /// BODY already matches the fresh render's body, replaces <c>darling-managed.conf</c> (<see
    /// cref="ManagedConfFile.ShouldReplaceManagedConf"/> — #4215's flake fix: a header-only difference, such as
    /// <c>data-volume-free-gib</c> crossing a rounding boundary between two starts with nothing else changed,
    /// never triggers a rewrite, since the header describes the inputs as of the last body change and a
    /// display field can legitimately lag). Always ensures the <c>include</c> line is present in
    /// <c>postgresql.conf</c> — there is no opt-out — whichever branch it takes. Never throws: an I/O failure is
    /// reported in the result and the file already in force stays in force, exactly like a failed <see
    /// cref="EnsureConfAppended"/> append would today.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal ManagedConfWriteResult WriteManagedConfFile(string dataDirectory, int postgresMajor)
    {
        var managedPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
        var inputs = GatherManagedConfRenderInputs(dataDirectory, postgresMajor);
        var rendered = ManagedConfFile.Render(inputs);
        var renderOverride = TestOnlyRenderOverride.Value;
        if (renderOverride is not null)
        {
            rendered = renderOverride(rendered);
        }

        string? existingText = null;
        try
        {
            if (File.Exists(managedPath))
            {
                existingText = File.ReadAllText(managedPath);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger.LogWarning("Could not read {Path} ({Message}); writing a fresh one.", managedPath, ex.Message);
        }

        if (existingText is not null && ManagedConfFile.IsHandEdited(existingText))
        {
            var existingBody = ManagedConfFile.ParseExisting(existingText).Body;
            var renderedBody = ManagedConfFile.ParseExisting(rendered).Body;
            var diffs = ManagedConfFile.DiffBodyKeys(existingBody, renderedBody);
            foreach (var diff in diffs)
            {
                _logger.LogWarning(
                    "{Path} was hand-edited: {Key} = {FileValue} stays in force (a fresh render would write {RenderedValue}).",
                    managedPath, diff.Key, diff.FileValue ?? "(absent)", diff.RenderedValue ?? "(absent)");
            }

            EnsureManagedIncludeLine(dataDirectory);
            return new ManagedConfWriteResult(Written: false, HandEdited: true, WriteFailed: false, rendered, diffs);
        }

        if (!ManagedConfFile.ShouldReplaceManagedConf(existingText, rendered))
        {
            EnsureManagedIncludeLine(dataDirectory);
            return new ManagedConfWriteResult(Written: false, HandEdited: false, WriteFailed: false, rendered, []);
        }

        if (!ManagedConfFile.TryReplaceAtomic(
                managedPath, rendered, ManagedConfFile.DefaultMaxReplaceAttempts, ManagedConfFile.DefaultReplaceRetryDelay, out var writeError))
        {
            _logger.LogError(
                writeError,
                "Could not update {Path}; the file already in force stays in force.",
                managedPath);
            EnsureManagedIncludeLine(dataDirectory);
            return new ManagedConfWriteResult(Written: false, HandEdited: false, WriteFailed: true, rendered, []);
        }

        _logger.LogInformation(
            existingText is null ? "Wrote {Path}" : "Updated {Path}",
            managedPath);
        EnsureManagedIncludeLine(dataDirectory);
        return new ManagedConfWriteResult(Written: true, HandEdited: false, WriteFailed: false, rendered, [], PreviousText: existingText, Inputs: inputs);
    }

    /// <summary>
    /// Appends <see cref="ManagedConfFile.IncludeLine"/> to <c>postgresql.conf</c> when it is missing.
    /// There is no opt-out, so an operator who removes it gets it back, with a warning, on the next start.
    /// A present include in any form PostgreSQL itself would parse the same way
    /// (<see cref="ManagedConfFile.HasManagedInclude"/>) is left exactly where it is — never moved, never
    /// duplicated.
    /// </summary>
    internal void EnsureManagedIncludeLine(string dataDirectory)
    {
        var confPath = Path.Combine(dataDirectory, "postgresql.conf");
        var conf = File.ReadAllText(confPath);
        if (ManagedConfFile.HasManagedInclude(conf))
        {
            return;
        }

        File.AppendAllText(confPath, "\n" + ManagedConfFile.IncludeLine + "\n");
        _logger.LogWarning(
            "{ConfPath} had no include of {ManagedFile} — appended it back. There is no way to opt out of the managed settings file.",
            confPath, ManagedConfFile.FileName);
    }

    /// <summary>
    /// The <c>postgres -C</c> validation: parses every
    /// configuration file this data directory's postgresql.conf reaches, <c>darling-managed.conf</c> included,
    /// and exits without starting a postmaster. <c>-C</c> FIRST is load-bearing: PostgreSQL only skips its
    /// "refuses to run as an administrator" check when <c>-C</c> is the very first argument, and this service
    /// can run under LocalSystem or an admin domain account.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static async Task<(bool Valid, string Output)> ValidateManagedConfAsync(
        string binDirectory, string dataDirectory, CancellationToken cancellationToken)
    {
        var postgresExe = Path.Combine(binDirectory, "postgres.exe");
        var (exitCode, output) = await RunToolAsync(
            postgresExe, $"-C shared_buffers -D \"{dataDirectory}\"", s_confValidateTimeout, cancellationToken);
        return (exitCode == 0, output);
    }

    /// <summary>
    /// The recovery message for a rejected <c>darling-managed.conf</c>: names
    /// the two ways an operator can fix a value the product's own formula got wrong for this host — a line
    /// after the include in <c>postgresql.conf</c>, or <c>ALTER SYSTEM</c> once a store is running on it.
    /// </summary>
    internal static string BuildManagedConfValidationFailureMessage(string dataDirectory, string postgresOutput)
        => $"{ManagedConfFile.FileName} in {dataDirectory} was rejected by postgres -C: {postgresOutput}\n" +
           "Fix the rejected setting with a line after 'include ''darling-managed.conf''' in " +
           $"{Path.Combine(dataDirectory, "postgresql.conf")}, or with ALTER SYSTEM once the store is running.";

    /// <summary>
    /// Everything <see cref="EnsureRunningAsync"/> needs before it can start a server on this data directory
    /// (#4215): render/write the managed file, validate it, and fall back to the last file that started
    /// cleanly rather than repeat a start failure forever. Runs
    /// only on the service-owned start path — see the caller; the adopted-listener path never calls this.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal async Task EnsureManagedConfReadyAsync(string binDirectory, string dataDirectory, CancellationToken cancellationToken)
    {
        var postgresMajor = DarlingStoreUpgrade.TryReadDataDirectoryMajor(dataDirectory) ?? 0;
        LastManagedConfWriteResult = WriteManagedConfFile(dataDirectory, postgresMajor);
        LastStartUsedLastGoodManagedConf = false;

        var (valid, output) = await ValidateManagedConfAsync(binDirectory, dataDirectory, cancellationToken);
        if (valid)
        {
            return;
        }

        var managedPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
        var lastGoodPath = Path.Combine(dataDirectory, ManagedConfFile.LastGoodFileName);
        if (File.Exists(lastGoodPath))
        {
            File.Copy(lastGoodPath, managedPath, overwrite: true);
            var (validAfterRestore, outputAfterRestore) = await ValidateManagedConfAsync(binDirectory, dataDirectory, cancellationToken);
            if (validAfterRestore)
            {
                LastStartUsedLastGoodManagedConf = true;
                _logger.LogError(
                    "{Message} Restored {LastGood}, which still starts.",
                    BuildManagedConfValidationFailureMessage(dataDirectory, output), lastGoodPath);
                return;
            }

            throw new InvalidOperationException(BuildManagedConfValidationFailureMessage(dataDirectory, outputAfterRestore));
        }

        throw new InvalidOperationException(BuildManagedConfValidationFailureMessage(dataDirectory, output));
    }

    /// <summary>
    /// Copies the file this start just proved PostgreSQL accepts to <see cref="ManagedConfFile.LastGoodFileName"/>
    /// (design step 2), so the NEXT start has something to fall back to if a formula or a constant later
    /// produces a value this runtime refuses. Never throws: a failed copy leaves the previous last-good file
    /// (if any) exactly as it was, which is strictly better than crashing a start that just succeeded.
    /// </summary>
    internal void SaveLastGoodManagedConf(string dataDirectory)
    {
        var managedPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
        var lastGoodPath = Path.Combine(dataDirectory, ManagedConfFile.LastGoodFileName);
        try
        {
            File.Copy(managedPath, lastGoodPath, overwrite: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger.LogWarning(
                "Could not update {LastGood} after a successful start ({Message}). A future rejected render would have nothing to fall back to.",
                lastGoodPath, ex.Message);
        }
    }

    /// <summary>
    /// The v12 ALTER SYSTEM check (#3802): reads <c>postgresql.auto.conf</c> in the data directory and logs ONE
    /// warning per WAL key it assigns, naming the key, the value as written, the figure the product derived
    /// instead, and the precedence that makes the file's value the one that runs. It changes nothing — the
    /// block is authored regardless and the auto.conf is never edited or deleted; the v10 <c>lc_messages</c>
    /// note is the precedent for stating the precedence rather than fighting it, and <c>ALTER SYSTEM RESET</c>
    /// is the operator's move. Split from <see cref="EnsureConfAppended"/> so the log line can be pinned with a
    /// fake auto.conf and a capturing logger, without a data directory that can start.
    ///
    /// <para>Never throws: an unreadable auto.conf is logged at Debug and treated as "no overrides" — the block
    /// is still authored, and this is a diagnostic, not a gate. WARNING rather than INFORMATION because the
    /// block is INERT for that key while the override stands, and an operator reading the log for "why is the
    /// effective value not what the product derived" needs the answer to stand out from the v12 line above
    /// it that reports the derivation as if it applied.</para>
    /// </summary>
    internal void LogWalSizingAutoConfOverrides(string dataDirectory, WalSettings derived)
    {
        var autoConfPath = Path.Combine(dataDirectory, "postgresql.auto.conf");
        string? autoConf;
        try
        {
            autoConf = File.Exists(autoConfPath) ? File.ReadAllText(autoConfPath) : null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger.LogDebug("v12 WAL sizing: could not read {AutoConf} to check for ALTER SYSTEM overrides ({Message}); the block is authored regardless.", autoConfPath, ex.Message);
            return;
        }

        foreach (var (name, value) in FindWalSizingAutoConfOverrides(autoConf))
        {
            var derivedText = name switch
            {
                "max_wal_size" => FormattableString.Invariant($"{derived.MaxWalSizeMb}MB"),
                "min_wal_size" => FormattableString.Invariant($"{derived.MinWalSizeMb}MB"),
                _ => CheckpointCompletionTargetPin + " (or PostgreSQL's default on 14+)",
            };
            _logger.LogWarning(
                "postgresql.auto.conf sets {Setting} = {Value} (an ALTER SYSTEM override). PostgreSQL reads postgresql.auto.conf AFTER postgresql.conf, so that value wins over the v12 WAL-sizing block's derived {Derived} and the block is inert for this setting until the override is removed (ALTER SYSTEM RESET {Setting}, then reload). This service does not edit postgresql.auto.conf; the block is authored regardless so the file records what the product derived.",
                name, value, derivedText, name);
        }
    }

    /// <summary>
    /// The AUTHORITATIVE free/total read of the volume holding <paramref name="dataDirectory"/> (#3802): the
    /// same <c>DriveInfo</c>-on-the-path-root idiom the store upgrade's headroom check and the disk-pressure
    /// self-alert already use, so three readers of one volume cannot disagree about which volume.
    /// <c>AvailableFreeSpace</c> rather than <c>TotalFreeSpace</c>: it honours a quota on the service account,
    /// and the WAL is written by the postmaster running AS that account, so it is the figure that bounds what
    /// the server can actually write — the upgrade's headroom decision makes the same choice.
    ///
    /// <para>False, with both figures zero, when the root cannot be resolved, the drive is not ready, or the
    /// read throws — and false is the v8 discipline's "do nothing" signal, not a value to size from. Logged at
    /// Warning here so the skip in <see cref="EnsureConfAppended"/> has its cause beside it.</para>
    /// </summary>
    private bool TryReadDataVolumeSpace(string dataDirectory, out long freeBytes, out long totalBytes)
    {
        try
        {
            var root = Path.GetPathRoot(Path.GetFullPath(dataDirectory));
            if (!string.IsNullOrEmpty(root))
            {
                var drive = new DriveInfo(root);
                if (drive.IsReady)
                {
                    freeBytes = drive.AvailableFreeSpace;
                    totalBytes = drive.TotalSize;
                    if (freeBytes >= 0 && totalBytes > 0)
                    {
                        return true;
                    }
                }
            }

            _logger.LogWarning("Could not read the free space on the volume holding {DataDirectory} (root {Root} not ready or reported no size).", dataDirectory, root ?? "(unresolved)");
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException)
        {
            _logger.LogWarning("Could not read the free space on the volume holding {DataDirectory} ({Message}).", dataDirectory, ex.Message);
        }

        freeBytes = 0;
        totalBytes = 0;
        return false;
    }

    /// <summary>
    /// Total physical RAM in bytes, read once per bootstrap to size the v3 memory block — the runtime
    /// analogue of the worker sizing's hypertable count. Uses the Win32 <c>GlobalMemoryStatusEx</c>
    /// (<c>ullTotalPhys</c> = the machine's installed physical memory); on the rare failure it falls back to
    /// the GC's view of total available memory, then to a conservative 4 GB, so sizing never runs on a
    /// zero/garbage reading. Windows-only, like the rest of this managed-mode class.
    /// </summary>
    private long GetTotalPhysicalMemoryBytes()
        => TryGetAuthoritativePhysicalMemoryBytes(out var authoritative)
            ? authoritative
            : GC.GetGCMemoryInfo().TotalAvailableMemoryBytes is var gcTotal && gcTotal > 0
                ? gcTotal
                : MemoryFallbackRamBytes;

    /// <summary>
    /// The AUTHORITATIVE physical-RAM read: true only when <c>GlobalMemoryStatusEx</c> actually reported
    /// the machine's installed memory. Split out from <see cref="GetTotalPhysicalMemoryBytes"/> for #2845,
    /// because the v8 hardware block needs to distinguish "the RAM is X" from "we could not read the RAM
    /// and are guessing", and the guess is not a stable quantity.
    ///
    /// <para><b>Why the distinction is load-bearing.</b> The fallback tier below is
    /// <c>GC.GetGCMemoryInfo().TotalAvailableMemoryBytes</c> — a LIVE snapshot of what the runtime believes
    /// is available, not a hardware property. It varies between calls and sits well under true physical RAM.
    /// v1-v7 are marker-gated, so a bad reading could only ever stick once and the best-effort guess was the
    /// right trade for them. v8 re-evaluates on EVERY start for the life of the cluster, which turns the
    /// same rare Win32 failure into unbounded chances to (a) mint a novel fingerprint and append a block on
    /// each blip, and (b) derive effective_cache_size/maintenance_work_mem from the low guess and have them
    /// take effect on that very start. So v8 asks for the authoritative reading and does NOTHING without
    /// one: if we cannot read the RAM we cannot know whether it changed, and leaving the last known-good
    /// block in force is strictly safer than re-deriving from a number we do not trust.</para>
    /// </summary>
    private bool TryGetAuthoritativePhysicalMemoryBytes(out long totalPhysicalMemoryBytes)
    {
        if (TryReadWindowsPhysicalMemoryBytes(out totalPhysicalMemoryBytes, out var win32Error, out var thrown))
        {
            return true;
        }

        if (thrown is not null)
        {
            _logger.LogWarning("Could not query total physical memory ({Message}); sizing Postgres memory from a fallback.", thrown.Message);
        }
        else
        {
            _logger.LogWarning(
                "GlobalMemoryStatusEx did not return total physical memory (Win32 error {Error}); sizing Postgres memory from a fallback.",
                win32Error);
        }

        return false;
    }

    /// <summary>
    /// The RAW <c>GlobalMemoryStatusEx</c> read, with no logger dependency (#4214) — split out of
    /// <see cref="TryGetAuthoritativePhysicalMemoryBytes"/> so the host-profile check's RAM fact can call the
    /// SAME authoritative read this class sizes Postgres from, rather than a second P/Invoke of the same API
    /// (the "RAM: reuse the authoritative read on Windows" ruling). <paramref name="thrown"/> carries the
    /// exception on the rare throw path so each caller can log its own wording without this method taking a
    /// logger; <paramref name="win32Error"/> is <see cref="Marshal.GetLastWin32Error"/> on a clean false.
    /// </summary>
    internal static bool TryReadWindowsPhysicalMemoryBytes(out long totalPhysicalMemoryBytes, out int win32Error, out Exception? thrown)
    {
        try
        {
            var status = new MemoryStatusEx();
            if (GlobalMemoryStatusEx(status) && status.ullTotalPhys > 0)
            {
                totalPhysicalMemoryBytes = (long)status.ullTotalPhys;
                win32Error = 0;
                thrown = null;
                return true;
            }

            win32Error = Marshal.GetLastWin32Error();
        }
        catch (Exception ex)
        {
            thrown = ex;
            totalPhysicalMemoryBytes = 0;
            win32Error = 0;
            return false;
        }

        thrown = null;
        totalPhysicalMemoryBytes = 0;
        return false;
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
    [SupportedOSPlatform("windows")]
    private async Task EnsureDataDirectoryMajorAsync(string binDirectory, Lazy<NetworkPlan> networkPlan, CancellationToken cancellationToken)
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
                EnsureConfAppended,
                /* Func<string>, never the NetworkPlan/NetworkMode types themselves — both are private to this
                   class, and UpgradeContext is read by DarlingStoreUpgrade (#4280 round-2 part 2, item 2). The
                   auto.conf carry trial appends this to its own start so a carried setting that only fails
                   under SSL (the round-1 review's ssl_ca_file example) is caught before the real start ever
                   sees it, not just before an SSL-less trial would have. */
                () => networkPlan.Value.Mode == NetworkMode.Exposed
                    ? BuildSslServerOptions(networkPlan.Value.CertPath, networkPlan.Value.KeyPath)
                    : string.Empty),
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

    /// <summary>The refusal when a server a quiesced start (TimescaleDB update or auto.conf trial) left running
    /// on its private port will not stop (#3908, #4280).</summary>
    private string QuiescedOrphanMessage(string binDirectory)
        => $"The store at {_dataDirectory} is running on a private port, left there by a quiesced start (a TimescaleDB update or an auto.conf carry-forward trial) this service started, and it would not stop. " +
           $"Stop it with \"{Path.Combine(binDirectory, "pg_ctl.exe")}\" stop -D \"{_dataDirectory}\" -m immediate, then restart the service. The store's data is not affected.";

    [SupportedOSPlatform("windows")]
    private string PreviousRuntimeHint()
        => Path.Combine(DarlingStoreUpgrade.PreviousRuntimeRootFor(_runtimeRoot), "pgsql");

    /// <summary>The bundled runtime's PostgreSQL major, from the binaries themselves rather than from a
    /// manifest that could disagree with what is on disk. The probe's exit code rides out alongside it
    /// (#2186): when the answer is "unidentifiable", that code is the ONLY evidence of why, and the
    /// refusal that consumes it used to have to assert the reason instead of showing it.</summary>
    [SupportedOSPlatform("windows")]
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
    [SupportedOSPlatform("windows")]
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
        await using var connection = await OpenProbedMaintenanceConnectionAsync(connectionString, cancellationToken);
        if (connection is null)
        {
            return;
        }

        _logger.LogInformation("Creating the '{Database}' database", DatabaseName);

        /* Once, on the connection whose backend just answered the probe, and outside the retry (#4352).
           The retry is for the post-start race, and that race kills a backend on its FIRST query, which is
           the probe; this backend has already survived it. A CREATE DATABASE timeout means the template
           copy is slow, and retrying cannot help: the timeout cancels the statement, the server rolls the
           partial copy back, and a new attempt copies template1 from the start under the same deadline, so
           a copy slower than the deadline never finishes. Un-retried, it takes the bootstrap group's
           deadline; if that fires too, the bootstrap throws and the worker's startup triage retries the
           whole start.
           Identifier from the class constant, never from input — same interpolation reasoning
           as TimescaleSupport/DarlingRetention. CREATE DATABASE cannot run in a transaction;
           plain ExecuteNonQuery is the correct shape. */
        using var create = new NpgsqlCommand($"CREATE DATABASE {DatabaseName}", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        await create.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Opens the maintenance database and asks whether the store's database exists. Returns null when it
    /// does, and otherwise the open connection, whose backend has answered its first query.
    /// </summary>
    private async Task<NpgsqlConnection?> OpenProbedMaintenanceConnectionAsync(string connectionString, CancellationToken cancellationToken)
    {
        /* The connect and the first query retry as one unit. A backend that loses the post-start race dies
           AFTER authenticating, so the Open succeeds and the first QUERY is what fails — retrying only the
           Open would never have helped. Each attempt gets a fresh connection because the old one's
           connector is dead. */
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { Database = "postgres" };
        for (var attempt = 1; ; attempt++)
        {
            var connection = new NpgsqlConnection(DarlingStoreConnection.PinSessionTimeZoneUtc(builder.ConnectionString));
            try
            {
                await connection.OpenAsync(cancellationToken);
                using var exists = new NpgsqlCommand($"SELECT 1 FROM pg_database WHERE datname = '{DatabaseName}'", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapConnectProbeSeconds };
                if (await exists.ExecuteScalarAsync(cancellationToken) is not null)
                {
                    await connection.DisposeAsync();
                    return null;
                }

                return connection;
            }
            catch (Exception ex) when (ex is not OperationCanceledException && attempt < FirstConnectionAttempts && IsTransientConnectionFault(ex))
            {
                await connection.DisposeAsync();
                _logger.LogWarning(
                    "The store dropped the first connection after start ({Message}) — attempt {Attempt} of {Total}. A backend that loses the shared-memory reservation race just after start does this; retrying.",
                    ex.Message, attempt, FirstConnectionAttempts);
                await Task.Delay(s_firstConnectionRetryDelay, cancellationToken);
            }
            catch
            {
                await connection.DisposeAsync();
                throw;
            }
        }
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
    internal const int FirstConnectionAttempts = 6;
    internal static readonly TimeSpan s_firstConnectionRetryDelay = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Whether an exception is a transport-level fault worth retrying (socket reset, stream write failure,
    /// timeout) rather than a definitive answer from a working server (bad password, missing role).
    /// <see cref="PostgresException"/> means the server replied, so it is never transient by this test.
    ///
    /// <para>The predicate itself lives in <see cref="PostgresTransportFault"/>, shared with the collector
    /// runner's store-write re-attempt. The RETRY POLICY stays here — six attempts two seconds apart, sized
    /// for the post-start shared-memory race above — because that is what differs between the two regimes;
    /// the question asked of the exception does not.</para>
    /// </summary>
    private static bool IsTransientConnectionFault(Exception exception)
        => PostgresTransportFault.IsTransportFault(exception);

    [SupportedOSPlatform("windows")]
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
    [SupportedOSPlatform("windows")]
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
    [SupportedOSPlatform("windows")]
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
    [SupportedOSPlatform("windows")]
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
    [SupportedOSPlatform("windows")]
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
        builder.Append(BuildSslServerOptions(sslCertFile, sslKeyFile));
        return builder.ToString();
    }

    /// <summary>
    /// The SSL trio ("-c ssl=on -c ssl_cert_file=... -c ssl_key_file=..."), leading space and all — the same
    /// shape as <see cref="DarlingStoreUpgrade"/>'s own extraServerOptions constants, so it drops straight
    /// into StartClusterAsync's extraServerOptions parameter. Factored out of
    /// <see cref="BuildServerRuntimeOptions"/> (no behavior change there) so the auto.conf carry trial (#4280
    /// round-2 part 2, item 2) can append the SAME option string to its own start, rather than restate the
    /// "-c" names a second time. Empty when either path is missing.
    /// </summary>
    internal static string BuildSslServerOptions(string? sslCertFile, string? sslKeyFile)
    {
        if (string.IsNullOrWhiteSpace(sslCertFile) || string.IsNullOrWhiteSpace(sslKeyFile))
        {
            return string.Empty;
        }

        var builder = new StringBuilder();
        builder.Append(" -c ssl=on");
        builder.Append(" -c ssl_cert_file=").Append(ToForwardSlashes(sslCertFile));
        builder.Append(" -c ssl_key_file=").Append(ToForwardSlashes(sslKeyFile));
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
    [SupportedOSPlatform("windows")]
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
            await using var connection = new NpgsqlConnection(DarlingStoreConnection.PinSessionTimeZoneUtc(ownerConnectionString));
            await connection.OpenAsync(cancellationToken);

            await using (var errors = new NpgsqlCommand(
                "SELECT count(*) FROM pg_hba_file_rules WHERE error IS NOT NULL", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds })
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
                    connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
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
                    connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
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
            await using var connection = new NpgsqlConnection(DarlingStoreConnection.PinSessionTimeZoneUtc(ownerConnectionString));
            await connection.OpenAsync(cancellationToken);
            await using var command = new NpgsqlCommand("SHOW listen_addresses", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
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
