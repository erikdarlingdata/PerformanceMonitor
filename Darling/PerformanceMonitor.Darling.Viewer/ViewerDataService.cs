/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One row of the servers table, as the viewer's server list shows it. Was a positional record; it is now
/// a class because the ported Lite server-row chrome needs mutable, change-notifying runtime state on each
/// row — the favorite star (<see cref="IsFavorite"/>, matched from the viewer's registry) and the
/// collection-freshness status dot (<see cref="IsOnline"/> / <see cref="CollectionStale"/> /
/// <see cref="AwaitingFirstCollection"/> → <see cref="CardStatus"/> → <see cref="DotStatus"/>) update in
/// place on the refresh timers without resetting the list's selection.
/// The Postgres-sourced fields stay immutable (get-only); only the sidebar overlay state is settable.
/// Equality is now reference-based, which every consumer already relies on (they key on
/// <see cref="ServerId"/> or hold the list's own instances).
/// </summary>
public sealed class DarlingServer : INotifyPropertyChanged
{
    /// <param name="engineKind">
    /// <c>servers.engine_kind</c> as the store holds it (#2530) - one of
    /// <see cref="MonitoredEngineKind.All"/>, or null for a row no connect has stamped since the V82 rung
    /// landed. Defaulted so the two reader call sites are the only places that have to know the column
    /// exists, and so the test fakes that construct a SQL Server row keep compiling unchanged.
    /// </param>
    /// <param name="engineEdition">
    /// <c>servers.sql_engine_edition</c>. Carried beside the kind because
    /// <see cref="CollectorEngineCapability.NotCollectedMessage"/> takes BOTH axes and asks kind first: a
    /// PostgreSQL target's edition is 0, which the edition axis correctly reads as "no claim". Passing 0
    /// here for a server whose edition has not been read is exactly that same silence.
    /// </param>
    /// <param name="postgresMajorVersion">
    /// <c>servers.postgres_major_version</c> (V100). Defaulted like the two above so the test fakes that
    /// construct a SQL Server row keep compiling, and so the two reader call sites stay the only places that
    /// have to know the column exists.
    /// </param>
    /// <param name="registeredAt">
    /// <c>servers.created_date</c> (#3967), defaulted for the same two reasons. Null for a server the operator
    /// added that the service has not connected to yet, which is the honest answer: nothing has registered it.
    /// </param>
    public DarlingServer(
        int serverId,
        string serverName,
        string displayName,
        bool isEnabled,
        int? sqlMajorVersion,
        decimal monthlyCostUsd = 0,
        string? engineKind = null,
        int engineEdition = CollectorEngineCapability.UnknownEngineEdition,
        int? postgresMajorVersion = null,
        DateTime? registeredAt = null)
    {
        ServerId = serverId;
        ServerName = serverName;
        DisplayName = displayName;
        IsEnabled = isEnabled;
        SqlMajorVersion = sqlMajorVersion;
        MonthlyCostUsd = monthlyCostUsd;
        EngineKind = engineKind;
        EngineEdition = engineEdition;
        PostgresMajorVersion = postgresMajorVersion;
        RegisteredAt = registeredAt;
    }

    public int ServerId { get; }
    public string ServerName { get; }
    public string DisplayName { get; }

    /// <summary>What a screen reader announces for a server in a list or drop-down box (the item's text is the item's name in UI
    /// Automation): the display name, not the type name ("PerformanceMonitor.Darling.Viewer.DarlingServer").</summary>
    public override string ToString() => DisplayName;

    public bool IsEnabled { get; }

    /// <summary>The names of the OTHER enabled registrations that last reported the same machine identity as this one (#6351), empty
    /// when none or when the store predates the identity column. Worked out when the list is read, never stored. Settable like the
    /// sidebar overlay state: it is filled by a second read after the registry rows are built.</summary>
    public IReadOnlyList<string> SameMachineAs { get; set; } = Array.Empty<string>();

    /// <summary>What <c>servers.is_aws_rds</c> (V225, #6894) and <c>servers.is_aws_rds_web_or_express</c> (V226, #6893) say this SQL Server target
    /// is: Amazon RDS, and whether on the Web or Express edition. NULL (never stamped), a store older than V225 and a failed read are all
    /// <see cref="AwsRdsKind.None"/>, which keeps the old text, and a store at V225 reads Amazon RDS but never the Web or Express edition. Filled by a
    /// second read after the registry rows are built, like <see cref="SameMachineAs"/>, so the two registry queries stay as they are. The surfaces
    /// of a collector RDS gates off use it to say the collector does not run there, in the MCP tools' words, instead of a "possible cause" that
    /// no setup step can act on.</summary>
    public AwsRdsKind AwsRdsKind { get; set; } = AwsRdsKind.None;

    /// <summary>The sidebar's details line (#6351): the version label, then the same-machine flag when there is one. The flag is part of
    /// the row's text rather than a tooltip alone, so it is on the screen without hovering.</summary>
    public string SidebarDetail => SameMachineAs.Count == 0
        ? VersionLabel
        : (string.IsNullOrEmpty(VersionLabel) ? "" : VersionLabel + " | ") + "same machine as " + string.Join(", ", SameMachineAs);

    /// <summary>What the same-machine flag means, for the tooltip on the row's name; empty when there is no flag.</summary>
    public string SameMachineTooltip => SameMachineAs.Count == 0
        ? ""
        : "This registration last reported the same machine as " + string.Join(", ", SameMachineAs)
          + ". An address can start reaching another machine (an availability group listener failing over, a DNS change). "
          + "An agent-scoped notification route on one registration does not cover the other.";

    public int? SqlMajorVersion { get; }
    public decimal MonthlyCostUsd { get; }

    /// <summary>The raw <c>servers.engine_kind</c> token, or null when the store makes no claim. Kept raw
    /// rather than decoded to an enum so an unrecognised token written by a NEWER service survives the trip
    /// to <see cref="EngineDescription"/>, which shows the operator the literal string to search for.</summary>
    public string? EngineKind { get; }

    /// <summary><c>servers.sql_engine_edition</c>; <see cref="CollectorEngineCapability.UnknownEngineEdition"/>
    /// when the probe has not run or the target has no such property (every PostgreSQL target).</summary>
    public int EngineEdition { get; }

    /// <summary>
    /// True only when the store SAYS this target is PostgreSQL. Absence is not evidence for either engine,
    /// so a null or unrecognised token is false and the server keeps the SQL Server surface - the
    /// pre-#2530 behaviour, which is the only safe default for the servers that have not reconnected since
    /// the engine-kind rung landed.
    /// </summary>
    public bool IsPostgres => MonitoredEngineKind.IsPostgres(EngineKind);

    /// <summary>The FinOps picker's text: <see cref="DisplayName"/>, with " (PostgreSQL)" after a PostgreSQL target's, as the web page's server list does.</summary>
    public string PickerLabel => FinOpsServerChoice.Label(this);

    /// <summary>
    /// True only when the store says this target is Amazon Aurora PostgreSQL specifically. Same asymmetry as
    /// <see cref="IsPostgres"/>. Read for the Overview card's CPU row (#3267): instance CPU comes from
    /// Performance Insights, which <c>PgCpuUtilizationCollector.AppliesTo</c> gates to Aurora, so this is
    /// what separates "no reading yet" from "this build collects no instance CPU here at all".
    /// </summary>
    public bool IsAurora => MonitoredEngineKind.IsAurora(EngineKind);

    /// <summary>
    /// How the engine reads in the per-server header, or null when the store makes no claim - in which case
    /// the header shows NO engine label rather than "SQL Server", because the tabs such a server gets are a
    /// default rather than a finding. An unrecognised token renders as the raw token: the describer's
    /// "an unrecognised engine" is worded to sit mid-sentence in the capability messages and reads wrong as
    /// a label, and the literal string is what an operator would grep their store for.
    /// </summary>
    public string? EngineDescription =>
        !MonitoredEngineKind.IsKnown(EngineKind)
            ? (string.IsNullOrWhiteSpace(EngineKind) ? null : EngineKind.Trim())
            : MonitoredEngineKind.DescribeEngineKind(EngineKind);

    /// <summary><c>servers.postgres_major_version</c> (V100, #2653) — the probed PostgreSQL major, or null
    /// on a SQL Server target (where it is not a fact about the server) and on a PostgreSQL target that has
    /// not reconnected since that rung. Read here so <see cref="VersionLabel"/> has the PostgreSQL
    /// vocabulary's own number to render instead of the SQL Server one.</summary>
    public int? PostgresMajorVersion { get; }

    /// <summary>
    /// "SQL Server 2022" / "PostgreSQL 18"-style label for the server list; empty when no version is known.
    ///
    /// <para>Engine-aware (#3145): it asks <see cref="EngineKind"/> which version vocabulary this row's
    /// numbers belong to. Before that it fed <see cref="SqlMajorVersion"/> through a SQL-Server-only table,
    /// and a PostgreSQL target — whose <c>sql_major_version</c> is <c>0</c> — rendered "SQL Server v0" in the
    /// fleet sidebar while this very object's <see cref="IsPostgres"/> and <see cref="EngineDescription"/>
    /// already knew better.</para>
    ///
    /// <para>It also asks <see cref="EngineEdition"/>: an Azure SQL Database reports major <c>12</c>, which the
    /// year table would label "SQL Server 2014", so the two Azure editions (5 and 8) read as "Azure SQL
    /// Database" and "Azure SQL Managed Instance" instead. Every other edition keeps the year.</para>
    /// </summary>
    public string VersionLabel =>
        MonitoredEngineVersion.DescribeEngineVersion(EngineKind, SqlMajorVersion, PostgresMajorVersion, EngineEdition);

    /// <summary>
    /// <c>servers.created_date</c>: the service's first successful connect to this server, or null when it has
    /// not connected yet (#3967). The newest-collection reads behind the dot and the Overview card have no
    /// window, but the collection log's retention bounds what they can see, and this is what tells a server
    /// whose whole history retention has dropped (Offline) from one that has never collected (awaiting). See
    /// <see cref="ServerSummaryItem.ClassifyFreshness(DateTime?, DateTime?, DateTime)"/>.
    /// </summary>
    public DateTime? RegisteredAt { get; }

    // ── Runtime-only sidebar state (not from Postgres; drives the ported Lite server-row chrome) ──

    private bool _isFavorite;

    /// <summary>Whether the user pinned this server (from the viewer's registry, matched by server id). Drives the star.</summary>
    public bool IsFavorite
    {
        get => _isFavorite;
        set
        {
            if (_isFavorite != value)
            {
                _isFavorite = value;
                OnPropertyChanged(nameof(IsFavorite));
            }
        }
    }

    private bool? _isOnline;

    /// <summary>Freshness "reachability": true = fresh/stale (dot green/amber), false = offline (red), null = unknown.</summary>
    public bool? IsOnline
    {
        get => _isOnline;
        set
        {
            if (_isOnline != value)
            {
                _isOnline = value;
                OnPropertyChanged(nameof(IsOnline));
                RaiseDotChanged();
            }
        }
    }

    private bool _collectionStale;

    /// <summary>Warning (amber) state: the newest collection has lagged past
    /// <see cref="ServerHealthThresholds.StaleThreshold"/>. Freshness only, and named for it — a collector that
    /// is failing is a different axis, reported by the Collection Health tab (#3098).</summary>
    public bool CollectionStale
    {
        get => _collectionStale;
        set
        {
            if (_collectionStale != value)
            {
                _collectionStale = value;
                OnPropertyChanged(nameof(CollectionStale));
                RaiseDotChanged();
            }
        }
    }

    private bool _awaitingFirstCollection;

    /// <summary>
    /// True when no collection has EVER landed for this server: the service hasn't reached it yet — a
    /// registered-but-queued server during bootstrap, not a dead one. The row had no such flag until #2473,
    /// which is precisely why its dot went grey "Unknown" while the Overview card one panel over said amber
    /// "Awaiting first collection" about the same server, off the same freshness call.
    /// </summary>
    public bool AwaitingFirstCollection
    {
        get => _awaitingFirstCollection;
        set
        {
            if (_awaitingFirstCollection != value)
            {
                _awaitingFirstCollection = value;
                OnPropertyChanged(nameof(AwaitingFirstCollection));
                RaiseDotChanged();
            }
        }
    }

    /// <summary>
    /// The sidebar row's status, as a VALUE — the same <see cref="ServerCollectionStatus"/> the Overview card
    /// renders (#2473). This row used to derive its own copy of the status ladder from the same flags, on a
    /// different type, on a different surface, and with only four of the five states: the sidebar and the card
    /// could therefore say different things about one server, and for a never-collected server they did. Both
    /// now read <see cref="ServerCollectionStatusRules.Classify"/>, which is the collapse #2429 argued for —
    /// with one discriminant there is no flag combination left for the renderings to disagree about.
    /// </summary>
    public ServerCollectionStatus CardStatus =>
        ServerCollectionStatusRules.Classify(IsOnline, CollectionStale, AwaitingFirstCollection);

    /// <summary>
    /// Sidebar status-dot vocabulary — the SAME words the Overview card's <c>StatusDisplay</c> shows, because
    /// both render <see cref="CardStatus"/>. They are also the <c>DataTrigger</c> values MainWindow.xaml keys
    /// the Ellipse fill off, so a state with no trigger falls through to the muted grey default rather than
    /// failing anything — which is how "Awaiting first collection" was silently grey. The trigger set is
    /// pinned against the enum in <c>ViewerSidebarDotRendersTheCardStatusTests</c>.
    /// </summary>
    public string DotStatus => CardStatus.Word();

    /// <summary>
    /// What the dot means, for the ToolTip the sidebar Ellipse carries — the answer to #2422 one surface over,
    /// and the thing that makes an amber dot legible at all. The card's amber covers two states (a stale
    /// collection and a never-collected server) and the card disambiguates them with a WORD; a dot has no room
    /// for one, so the tooltip does that job instead. The first line is
    /// <see cref="ServerCollectionStatusRules.Headline"/>, word-for-word what the Overview card says for the
    /// same state.
    ///
    /// <para>The second line names the axis. In Darling every one of these words is a COLLECTION answer —
    /// there is no live ping to a monitored server — which is the opposite of Lite, where the identically
    /// coloured dot reports a connection check. A reader who uses both should not have to infer which. The
    /// third names the gesture THIS surface supports: <c>ServerList_MouseDoubleClick</c> opens the tab, and a
    /// single click only selects, so naming one would be naming a no-op.</para>
    /// </summary>
    public string DotTooltip => string.Join(
        "\n",
        CardStatus.Headline(),
        "Darling has no live ping: this is how old the newest collection is, not a connection check.",
        "Double-click the row to open this server's tab");

    /// <summary>Raises the change notifications for everything derived from the three status flags. One
    /// helper rather than three call sites per setter: <see cref="DotTooltip"/> was added after
    /// <see cref="DotStatus"/>, and a derived member that a setter forgets to announce is a dot that stops
    /// updating in place — silent, and only visible on a list refresh.</summary>
    private void RaiseDotChanged()
    {
        OnPropertyChanged(nameof(CardStatus));
        OnPropertyChanged(nameof(DotStatus));
        OnPropertyChanged(nameof(DotTooltip));
    }

    /// <summary>
    /// Sets the dot from the same collection-freshness classification the Overview cards use
    /// (<see cref="ServerSummaryItem.ClassifyFreshness"/>), through the same
    /// <see cref="ServerCollectionStatusRules.FlagsFor"/> mapping: Fresh → Online, Stale → the amber Warning,
    /// Offline → red, NeverCollected → the amber "Awaiting first collection" (the service hasn't reached the
    /// server yet — during a fleet bootstrap that is "queued", not "dead"). Both instants are UTC (the store
    /// is naive UTC; nowUtc is <see cref="DateTime.UtcNow"/>).
    ///
    /// <para>This method used to set two flags out of three by hand and drop the awaiting marker on the floor.
    /// Nothing about a block of assignments makes a missing one visible, which is why the flags now arrive as
    /// a single value (#2473).</para>
    /// </summary>
    public void ApplyFreshness(DateTime? lastCollectionUtc, DateTime nowUtc)
    {
        var flags = ServerCollectionStatusRules.FlagsFor(
            ServerSummaryItem.ClassifyFreshness(lastCollectionUtc, RegisteredAt, nowUtc));
        IsOnline = flags.IsOnline;
        CollectionStale = flags.CollectionStale;
        AwaitingFirstCollection = flags.AwaitingFirstCollection;
    }

    // ── Per-server alert "needs attention" badge state (from the polled alert history, ack-aware) ──
    // Set by MainWindow.UpdateServerAttention from the ServerAttentionDeriver output, gated through the
    // viewer-local ViewerAlertStateService (ack-until-worse). Drives the sidebar row badge, mirroring
    // Lite's per-tab alert badge.

    private int _attentionCount;
    private bool _attentionIsCritical;
    private string? _attentionTooltip;

    /// <summary>Active (unacknowledged, un-muted) alert count for this server; 0 hides the badge.</summary>
    public int AttentionCount => _attentionCount;

    /// <summary>True when the badge should show (there is at least one active alert to surface).</summary>
    public bool HasAttention => _attentionCount > 0;

    /// <summary>The badge count text, capped like Lite's tab badge ("99+" past 99).</summary>
    public string AttentionText => _attentionCount > 99
        ? "99+"
        : _attentionCount.ToString(CultureInfo.InvariantCulture);

    /// <summary>The badge's screen-reader name ("1 unread alert", "3 unread alerts"); the painted count alone is a bare number.</summary>
    public string AttentionAutomationName => PerformanceMonitor.Ui.UnreadAlertsName.For(_attentionCount);

    /// <summary>
    /// Badge severity vocabulary — "Critical" / "Warning" / "None" — so the sidebar row's DataTriggers colour
    /// the badge (ErrorBrush / WarningBrush) the same way Lite maps deadlock→red, else→orange-red.
    /// </summary>
    public string AttentionSeverity => _attentionCount <= 0
        ? "None"
        : (_attentionIsCritical ? "Critical" : "Warning");

    /// <summary>The badge tooltip: the metric breakdown for this server (empty when no attention).</summary>
    public string? AttentionTooltip => _attentionTooltip;

    /// <summary>
    /// Updates the sidebar alert badge in place (no list reset). Pass <paramref name="count"/> 0 to clear it
    /// (acknowledged / no alerts). Raises change notifications only when something actually changed.
    /// </summary>
    public void SetAttention(int count, bool isCritical, string? tooltip)
    {
        if (_attentionCount == count
            && _attentionIsCritical == isCritical
            && string.Equals(_attentionTooltip, tooltip, StringComparison.Ordinal))
        {
            return;
        }

        _attentionCount = count;
        _attentionIsCritical = isCritical;
        _attentionTooltip = tooltip;

        OnPropertyChanged(nameof(AttentionCount));
        OnPropertyChanged(nameof(HasAttention));
        OnPropertyChanged(nameof(AttentionText));
        OnPropertyChanged(nameof(AttentionAutomationName));
        OnPropertyChanged(nameof(AttentionSeverity));
        OnPropertyChanged(nameof(AttentionTooltip));
    }

    // ── Whole-server alert silence indicator (#2031) ──
    // Set by MainWindow's silenced-state refresh (riding the same alert poll that drives the badge) and
    // immediately by the Silence/Unsilence handlers. Drives the sidebar row's muted-bell glyph and the
    // context menu's Silence/Unsilence exclusivity.

    private bool _isSilenced;

    /// <summary>True when a whole-server alert silence (store-side mute rule) is active for this server.</summary>
    public bool IsSilenced => _isSilenced;

    /// <summary>Updates the silenced indicator in place; raises change notification only on a real flip.</summary>
    public void SetSilenced(bool silenced)
    {
        if (_isSilenced == silenced)
        {
            return;
        }

        _isSilenced = silenced;
        OnPropertyChanged(nameof(IsSilenced));
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged(string name) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}

/// <summary>
/// The viewer's reads of the Darling Postgres store — the server list, plus the surfaces in the
/// partials (the Overview lanes' total-wait + memory trends and per-lane baselines in
/// <c>ViewerDataService.OverviewLanes.cs</c>, the per-tab reads in <c>.Cpu.cs</c>, <c>.Waits.cs</c>,
/// <c>.BlockingTrends.cs</c>, <c>.FileIo.cs</c>, <c>.TempDb.cs</c>, <c>.Config.cs</c>, and
/// <c>.RunningJobs.cs</c>, the Daily Summary + Collection Health reads in <c>.DailySummary.cs</c> /
/// <c>.CollectionHealth.cs</c>, and the wave-2/3 reads in <c>.QueryStats.cs</c>, <c>.Blocking.cs</c>,
/// <c>.Findings.cs</c>, <c>.AlertHistory.cs</c>, and <c>.MuteRules.cs</c>).
/// Connections come from a pooled <see cref="NpgsqlDataSource"/>, so the window can run its
/// per-tab queries concurrently. The SQL lives in public constants so tests can pin the
/// load-bearing clauses without a live Postgres.
/// All timestamps in the store are naive UTC (`timestamp without time zone`), so DateTime
/// parameters are sent with DateTimeKind.Unspecified — since Npgsql 6.0 a Kind=Utc DateTime
/// maps strictly to timestamptz and throws against naive columns.
/// </summary>
public sealed partial class ViewerDataService : IAsyncDisposable
{
    /// <summary>
    /// The observed server registry. <c>engine_kind</c> (#2530) and <c>sql_engine_edition</c> ride along
    /// because the per-server tab set and every PostgreSQL panel's own explanation are derived from them -
    /// see <see cref="ViewerPostgresTabs"/>. Both are read here AND in
    /// <c>ViewerDataService.MonitoredServers.cs</c>'s <c>ManagedServersSql</c>: that one is what the sidebar
    /// actually uses on a seeded store, so a discriminator added to only this query would have left every
    /// real deployment on the SQL Server tab set.
    ///
    /// <para><c>postgres_major_version</c> (V100) rides along for the same reason and with the same
    /// both-queries requirement (#3145): it is the PostgreSQL vocabulary's own major, and without it the
    /// sidebar's version label has nothing but <c>sql_major_version</c> — which is <c>0</c> on every
    /// PostgreSQL target — to describe the row with.</para>
    ///
    /// <para><c>created_date</c> (#3967) rides along with the same both-queries requirement: it is the server's
    /// first successful connect, and <see cref="DarlingServer.RegisteredAt"/> is what lets the sidebar dot and
    /// the Overview card read a server whose history retention has dropped as Offline rather than awaiting its
    /// first collection.</para>
    /// </summary>
    public const string ServersSql =
        "SELECT server_id, server_name, display_name, is_enabled, sql_major_version, COALESCE(monthly_cost_usd, 0), engine_kind, COALESCE(sql_engine_edition, 0), postgres_major_version, created_date FROM servers ORDER BY display_name";

    /// <summary>
    /// The authoritative read-only probe (V8 security hardening): does the connected role hold UPDATE on
    /// <c>config_alert_log</c> — the alert-dismiss write itself? True → the admin role (or an owner) — the
    /// mute / alert-dismiss / analysis-mute writes are available. False → the read-only viewer role — those
    /// surfaces degrade. This is the source of truth over <c>connectAs</c> (which only picks a credential and
    /// doesn't apply in BYO mode), because it reflects the connection's ACTUAL privileges. The bare table name
    /// resolves through search_path to <c>config.config_alert_log</c>.
    ///
    /// <para>It probed <c>config_mute_rules</c> INSERT until #3450 granted the viewer ROLE exactly that
    /// privilege (the web dashboard's dedicated mute-rule endpoints run as viewer), which would have flipped
    /// every <c>connectAs = "viewer"</c> Viewer to "writable" while its alert-dismiss writes still 42501'd —
    /// buttons offered, writes refused. <c>config_alert_log</c> UPDATE is one of the writes this probe actually
    /// gates and stays with admin/owner at TABLE level. The viewer role now holds a COLUMN-level
    /// <c>UPDATE (dismissed)</c> (the web dismiss route), which <c>has_table_privilege(..., 'UPDATE')</c> does not
    /// see, so the probe still reads false for a viewer seat and keeps discriminating the two roles it exists to tell apart — and the locked-down Viewer's read-only UX,
    /// mute-rule buttons included, is unchanged by the web grant.</para>
    /// </summary>
    public const string ReadOnlyProbeSql = "SELECT has_table_privilege('config_alert_log', 'UPDATE')";

    private readonly NpgsqlDataSource _dataSource;

    /// <summary>The store connection a saved password key belongs to (host, port and database), see
    /// <see cref="ViewerPasswordKey.StoreIdentityOf"/>.</summary>
    public string StoreConnectionIdentity { get; }

    /// <summary>Review D4R M3: <see cref="GetStoreSchemaVersionAsync"/>'s result, cached per instance (this
    /// instance is per store connection) once the Queries grid's #3953 table-read gate has probed it, so a
    /// long-window grid refresh pays the 121-column catalog probe once per session rather than on every load.
    /// A store upgraded mid-session keeps reading raw until the viewer reconnects — errs toward raw, never
    /// toward a stale "table" claim, same as every other input to this gate.</summary>
    private int? _cachedStoreSchemaVersion;

    /// <summary>The fleet's server clocks for the alert-history reads (#4766), held between polls: see
    /// <see cref="ServerClockCache"/>. Per instance, like the store connection it reads.</summary>
    private readonly ServerClockCache _alertClocks;

    /// <param name="connectionString">The Postgres connection string (managed-derived or BYO from darling.json).</param>
    /// <param name="connectionTimeoutSeconds">
    /// The viewer's "Connection timeout" preference (<see cref="ViewerAppSettings.ConnectionTimeoutSeconds"/>,
    /// Lite's "increase for VPN/remote" knob). When supplied it is applied to the pooled store connections as
    /// Npgsql's connect <c>Timeout</c> — set on the managed-derived string (which carries none) and appended to a
    /// BYO string only when the operator did not already specify one (their explicit Timeout wins). Null leaves
    /// the string untouched (Npgsql's 15s default). This restores the setting the Settings window has always
    /// saved but nothing consumed.
    /// </param>
    public ViewerDataService(string connectionString, int? connectionTimeoutSeconds = null)
    {
        /* #6630: the store's search path comes from the ONE helper the service uses (set-if-absent, appended). */
        var effectiveConnectionString = DarlingStoreConnection.EnsureStoreSearchPath(connectionTimeoutSeconds is int seconds
            ? ApplyConnectionTimeout(connectionString, seconds)
            : connectionString);

        /* #3016: the read-deadline family and the fan-out bound both need the pool size this seat really
           gets, and the managed constant is only one of the two strings the viewer can be handed. Published
           from the EFFECTIVE string, here, because this is the line that decides how many permits exist —
           a call site could be added later that forgets, and there is nowhere else the two can be kept
           from disagreeing. */
        ViewerStorePool.Publish(effectiveConnectionString);

        /* #4479: the managed derivation already carries ApplicationName (ViewerSettings.ApplicationName,
           set on the builder there); this is the BYO connection string's turn — set-if-absent, so an
           operator's own ApplicationName on a bring-your-own store wins. */
        _dataSource = NpgsqlDataSource.Create(
            DarlingStoreConnection.PinSessionTimeZoneUtc(
                DarlingStoreConnection.WithApplicationName(effectiveConnectionString, ViewerSettings.ApplicationName)));
        StoreIsOnThisMachine = StoreHostIsLoopback(connectionString);
        StoreConnectionIdentity = ViewerPasswordKey.StoreIdentityOf(connectionString);
        _alertClocks = new ServerClockCache(ct => GetServerClocksAsync(serverId: null, ct), AlertClockLifetime);
    }

    /// <summary>
    /// Builds the data service, or says why the store connection string cannot be used. The constructor
    /// throws when Npgsql cannot use the string, and the caller (<c>MainWindow.OnLoaded</c>, an <c>async void</c>
    /// handler) let that escape into the dispatcher's unhandled-exception log, which writes the exception's message
    /// and its inner exception. Npgsql's text for that can quote part of the string (a password with an unquoted ';'
    /// makes the text after it the "keyword", and a password tail that becomes a Host value with a ':' in it makes
    /// the text after the ':' a bad port), so the string is proved first, with Npgsql's own construction step (the
    /// guard builds and disposes a data source, and opens nothing), and <paramref name="unusable"/> is
    /// <see cref="DarlingStoreConnection.DescribeParseFailure"/>'s text, which the caller shows and logs. A string
    /// the guard rejects never reaches the constructor, so <see cref="ViewerStorePool.Publish"/> (the constructor's
    /// first step) does not run for it and the pool state does not change. The constructor adds a connect timeout, an
    /// application name and a session time zone before it creates the data source, and none of those takes part in
    /// Npgsql's construction checks.
    /// </summary>
    internal static bool TryCreate(
        string connectionString,
        int? connectionTimeoutSeconds,
        [NotNullWhen(true)] out ViewerDataService? dataService,
        out string unusable)
    {
        dataService = null;
        if (DarlingStoreConnection.TryDescribeParseFailure(connectionString) is { } parseFailure)
        {
            unusable = "postgres.connectionString could not be used: " + parseFailure;
            return false;
        }

        dataService = new ViewerDataService(connectionString, connectionTimeoutSeconds);
        unusable = string.Empty;
        return true;
    }

    /// <summary>
    /// Whether this viewer's store is reached over loopback — the best available proxy for "the Darling service
    /// runs on THIS machine" (#2279).
    ///
    /// <para><b>Why it is a proxy for that at all.</b> A DPAPI blob is <c>LocalMachine</c>-scoped, so a
    /// credential this viewer encrypts is decryptable only on this machine, and the service is the thing that
    /// has to decrypt it. The managed deploy builds its store connection on literal <c>127.0.0.1</c>
    /// (<c>ViewerSettings</c>, matching the service's own <c>DarlingManagedPostgres.BuildConnectionString</c>),
    /// and the service runs where its managed store runs. So a loopback store means viewer and service share a
    /// machine and a saved credential will work — which is the single-box deploy the DPAPI design targets, and
    /// where a warning would be pure noise.</para>
    ///
    /// <para><b>What it deliberately does not claim.</b> A non-loopback store does NOT prove the viewer is
    /// remote — a bring-your-own store on another host with the service local is a real configuration, and it
    /// reads as false here. That is why #2279 warns rather than refuses: this signal is good enough to decide
    /// whether to SAY something, and not good enough to decide whether to BLOCK. Getting that backwards would
    /// refuse a legitimate first-run Add on the service host.</para>
    /// </summary>
    public bool StoreIsOnThisMachine { get; }

    /// <summary>
    /// True when a store connection string names a loopback host, or names none at all (#2279).
    ///
    /// <para>Static and pure so the rule is testable without a store — it is the whole basis of the warning, and
    /// a viewer cannot be stood up in a unit test. Uses the base <see cref="DbConnectionStringBuilder"/> rather
    /// than Npgsql's, for the reason documented on <see cref="ApplyConnectionTimeout"/>: the Npgsql builder
    /// answers <c>ContainsKey</c> for every KNOWN key rather than only the present ones, so it cannot tell an
    /// omitted host from a specified one.</para>
    ///
    /// <para>An omitted host counts as loopback because that is what it MEANS — Npgsql defaults to localhost, so
    /// a string with no <c>Host</c> is a local store and must not warn. <c>::1</c> and <c>localhost</c> are
    /// included alongside <c>127.0.0.1</c> even though the managed path always writes the literal IPv4 form,
    /// because a hand-written BYO string pointing at the local box is still local and warning about it would be
    /// wrong.</para>
    /// </summary>
    internal static bool StoreHostIsLoopback(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return true;
        }

        string? host = null;
        try
        {
            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
            if (builder.ContainsKey("Host"))
            {
                host = builder["Host"]?.ToString();
            }
            else if (builder.ContainsKey("Server"))
            {
                host = builder["Server"]?.ToString();
            }
        }
        catch (ArgumentException)
        {
            /* An unparseable string is not evidence the store is remote, and this decides only whether to show
               a hint — so fail toward silence rather than toward a warning nobody can act on. */
            return true;
        }

        if (string.IsNullOrWhiteSpace(host))
        {
            return true;
        }

        var trimmed = host.Trim();
        return trimmed.Equals("127.0.0.1", StringComparison.Ordinal)
            || trimmed.Equals("::1", StringComparison.Ordinal)
            || trimmed.Equals("localhost", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Applies the viewer's connect-timeout preference to a connection string, but ONLY when the string does not
    /// already specify a <c>Timeout</c> — so an operator's explicit Timeout in a BYO <c>postgres.connectionString</c>
    /// always wins, while the managed-derived string (which never sets one) and a BYO string that omits it both
    /// pick up the preference. <paramref name="timeoutSeconds"/> is Npgsql's connect timeout in seconds (the
    /// caller clamps it to 5–60). Pure + string-only, so it is unit-tested without a live Postgres. Detection
    /// uses the base <see cref="DbConnectionStringBuilder"/>, whose <c>ContainsKey</c> reflects exactly the keys
    /// present in the string (NpgsqlConnectionStringBuilder overrides ContainsKey to answer for every KNOWN
    /// keyword, which can't tell "set" from "settable"). When the keyword is absent, the preference is APPENDED
    /// to the caller's own string rather than emitted through a builder round trip — round-1 review on #4285's
    /// PR measured the base builder's round trip changing 17 of 24 test strings, because its writer erases a
    /// keyword the caller set to an explicit empty value the same way <see cref="DarlingStoreConnection"/>'s
    /// remarks describe for <c>PinSessionTimeZoneUtc</c>; appending leaves every other keyword untouched.
    /// </summary>
    internal static string ApplyConnectionTimeout(string connectionString, int timeoutSeconds)
    {
        var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
        if (!builder.ContainsKey("Timeout"))
        {
            return connectionString + ";Timeout=" + timeoutSeconds.ToString(CultureInfo.InvariantCulture);
        }

        return connectionString;
    }

    /// <summary>
    /// True when the connected role cannot write the operator-config tables (the read-only
    /// <c>viewer</c> role, or any connection lacking config INSERT). Set by
    /// <see cref="DetectReadOnlyAsync"/>; the write surfaces gate on it. Defaults false (writable) until
    /// probed, then fails safe to true if the probe cannot run.
    /// </summary>
    public bool IsReadOnly { get; private set; }

    /// <summary>
    /// Runs the <see cref="ReadOnlyProbeSql"/> capability probe and records <see cref="IsReadOnly"/>.
    /// Called once after the service connects, before the write affordances are shown. A probe that
    /// throws (table missing on a mis-provisioned store, permission quirk, transient error) fails safe
    /// to read-only, so the UI hides writes rather than dead-clicking into a permission error; the
    /// reactive 42501 catch in the write paths is the backstop if the probe and reality ever disagree.
    /// </summary>
    public async Task<bool> DetectReadOnlyAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = _dataSource.CreateCommand(ReadOnlyProbeSql);
            command.CommandTimeout = ViewerCommandDeadlines.ConnectGateSeconds;
            var canInsert = await command.ExecuteScalarAsync(cancellationToken);
            IsReadOnly = canInsert is not true;
        }
        catch (Exception ex) when (IsConnectionFailure(ex))
        {
            /* Finding B3: a failure to REACH the store is not a read-only seat — surface it as unreachable so
               the shell shows the "is the service running?" message instead of hiding every write affordance
               behind a false read-only verdict. */
            throw new ViewerStoreUnreachableException(ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A server RESPONSE we couldn't interpret (a permission quirk, a missing table on a
               mis-provisioned store, a transient error) fails safe to read-only, so the UI hides writes
               rather than dead-clicking into a permission error. */
            IsReadOnly = true;
        }

        return IsReadOnly;
    }

    /// <summary>
    /// Opens one real connection to prove the store is reachable, so the shell can tell an UNREACHABLE store
    /// (the service down, or a wrong host/port/database in darling.json's postgres section) apart from a legit
    /// read-only seat — the two the old connect path collapsed into a false "read-only" verdict (finding B3).
    /// A connection-level failure is rethrown as <see cref="ViewerStoreUnreachableException"/>; a server that
    /// RESPONDS (even to refuse authentication) is "reachable" and any such error propagates for the caller's
    /// generic handling. The <see cref="NpgsqlDataSource"/> is lazy, so this is the first live connect.
    /// </summary>
    public async Task EnsureStoreReachableAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        }
        catch (Exception ex) when (IsConnectionFailure(ex))
        {
            throw new ViewerStoreUnreachableException(ex);
        }
    }

    /// <summary>
    /// True when <paramref name="ex"/> is a failure to TALK to Postgres (socket refused, host not found,
    /// connect timeout) rather than a server error RESPONSE. Npgsql surfaces a server response as a
    /// <see cref="PostgresException"/> (it carries a SQLSTATE); any other <see cref="NpgsqlException"/> — or a
    /// bare socket / timeout — means the server was never reached. Drives the unreachable-vs-read-only split
    /// (finding B3), so a down store shows "is the service running?" not a false read-only verdict.
    /// </summary>
    internal static bool IsConnectionFailure(Exception ex) =>
        (ex is NpgsqlException and not PostgresException)
        || ex is System.Net.Sockets.SocketException
        || ex is TimeoutException;

    /// <summary>
    /// Probes the store's effective schema version through <c>information_schema</c> (plus <c>pg_indexes</c>
    /// for the one index sentinel) — the version the viewer gates on at connect (finding B1). The authoritative
    /// <c>darling_schema_version</c> table is owner-only (a viewer/admin role can't read it, and even
    /// <c>MAX(version)</c> can itself throw 42501), so rather than read the version number this checks whether
    /// the store carries the schema OBJECTS the recent migrations added — objects any role can see. Newest
    /// first: V27's <c>deadlocks.database_name</c> column (the Azure per-database watermark key, #1535),
    /// V26's <c>config_notification.generic_url</c> column (the generic webhook channel, #1506), V25's
    /// <c>agent_status</c> table, V24's <c>job_history</c> table, V22's <c>idx_index_object_stats_latest</c> index (the FinOps Index Analysis
    /// supporting index — indexes are not listed in <c>information_schema</c>, so this one reads the
    /// world-readable <c>pg_indexes</c> catalog), V21's <c>default_trace_events</c> table (the shared Default
    /// Trace collector), V20's <c>notify_connection_changes</c> column (config_alert_settings), V19's
    /// <c>analysis_state</c> table, V18's <c>alert_delivery_mode_override</c> column (the very column whose
    /// absence throws the raw 42703 the finding reproduces on Add/Edit Server), and V17's
    /// <c>config_monitored_servers</c> table (the config control plane). <see cref="MapProbedSchemaVersion"/>
    /// reduces the flags to the highest satisfied version. Each sentinel appears only in a store at its
    /// migration or later: a fresh store gets them all at once (it runs straight through to the latest
    /// version), an upgraded store gets each exactly at its migration.
    ///
    /// <para>The seventh sentinel (V23) is different in kind: V23 makes <c>collection_log</c> a TimescaleDB
    /// hypertable, and its only schema effect is engine-dependent. So this checks <c>collection_log</c> IS a
    /// hypertable OR the store has no <c>timescaledb</c> extension — because on plain PostgreSQL V23 is a no-op
    /// (the guarded migration skips the conversion), so a plain-PG store at V23 is object-identical to V22 and
    /// must not be gated. <b>Crucially it is written PLAIN-POSTGRESQL-SAFE:</b> it reads the core <c>pg_inherits</c>
    /// catalog for collection_log's chunk children (TimescaleDB 2.x links chunks to the hypertable root via
    /// inheritance — note a freshly-converted, still-empty hypertable has no chunks yet, a negligible window since
    /// collection_log gets a row every collection cycle) rather than <c>timescaledb_information.hypertables</c> — that view does not exist without the
    /// extension, and referencing it here would make the WHOLE probe throw on plain PG (returning null → the
    /// gate fails open → EVERY plain-PG store, even genuinely-behind ones, loses connect-time gating). This
    /// composite is only meaningful once V22's index is present (its <c>NOT EXISTS timescaledb</c> arm is true
    /// for ALL plain-PG stores regardless of version), so <see cref="MapProbedSchemaVersion"/> gates it behind
    /// the V22 sentinel rather than treating it as a standalone newest-first arm.
    /// </summary>
    public const string StoreSchemaProbeSql = @"
SELECT
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'config_monitored_servers'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_monitored_servers' AND column_name = 'alert_delivery_mode_override'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'analysis_state'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'notify_connection_changes'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'default_trace_events'),
    EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_index_object_stats_latest'),
    (
        EXISTS (SELECT 1 FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhparent
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = 'collection_log' AND n.nspname = 'collect')
        OR NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')
    ),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'job_history'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'agent_status'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_notification' AND column_name = 'generic_url'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'deadlocks' AND column_name = 'database_name'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_store_stats' AND column_name = 'replica_role'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'long_query_completions'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'web_enabled'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'custom_views'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'server_tags'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'notify_connection_down_at_startup'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'ag_database_replica_states'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'notify_ag_health'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'ag_disconnect_refire_minutes'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'ag_database_replica_states' AND column_name = 'est_send_drain_time_min'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'query_plan_dim'),
    EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'ix_query_stats_digest_floor'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'blocking_wait_seconds_threshold'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_store_stats' AND column_name = 'runtime_stats_interval_id'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_notification' AND column_name = 'pagerduty_routing_key'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_notification' AND column_name = 'pagerduty_proxy'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'collector_state'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'plan_correction'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pvs_stats'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'pvs_enabled'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'database_state_enabled'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'server_tags' AND column_name = 'colour'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_stats' AND column_name = 'host_object_name'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'analysis_findings' AND column_name = 'drill_down_json'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'store_metrics'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_plan_dim' AND column_name = 'query_plan_gz'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'self_disk_free_warn_percent'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'store_metrics' AND column_name = 'last_run_duration_ms'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'store_job_cadence_warn_percent'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'query_store_backfill_enabled'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'query_store_text_budget_mb'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'database_state_expected' AND column_name = 'last_alerted_state'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'incident_occurrences'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'plan_xml_compression'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_monitored_servers' AND column_name = 'engine'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_blocking_edges'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'query_store_plan_map'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_statement_text'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'query_store_text'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'plan_content_retention_days'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'query_store_health'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_store_text' AND column_name = 'query_hash'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'compose_statement_timeout_seconds'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'file_growth_enabled'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'collection_log' AND column_name = 'slowest_item_ms'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tempdb_stats' AND column_name = 'max_size_mb'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'servers' AND column_name = 'engine_kind'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_database_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_index_usage_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_table_bloat_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_session_states'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_plan_capture_readiness'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_write_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_extension_availability'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_lock_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_column_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_replication_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_buffer_usage'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_index_bloat'),
    /* V95 probes a COLUMN, not a table. The three tables it touches all already exist at V94, so table
       existence cannot separate the rungs and information_schema.columns is the only sentinel that can. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_column_stats'
                                                     AND   column_name = 'database_name'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_wait_sampling'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_kernel_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_predicate_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_plan_capture'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'servers' AND column_name = 'postgres_major_version'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_io_stats' AND column_name = 'read_bytes'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_server_config'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_deadlocks'),
    EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_pg_deadlocks_identity'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'collector_cost'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_cpu_utilization'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'plan_force_actions'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'collection_log' AND column_name = 'sql_drain_ms'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'collection_log' AND column_name = 'drain_last_read_ms'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'collection_log' AND column_name = 'plan_fetch_target_ms'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'store_log_events'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'collector_stall_probes'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'plan_force_actions' AND column_name = 'actor'),
    /* V114 probes a COLUMN for V95's reason: collect.pg_index_bloat has existed since V94, so table
       existence cannot separate the rungs. est_reclaimable_bytes is the sentinel rather than one of the
       other seven added columns because it is the one the read ranks on - if it is absent the panel has
       nothing to order by, which is the failure this probe exists to prevent. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_index_bloat'
                                                     AND   column_name = 'est_reclaimable_bytes'),
    /* V115 probes a COLUMN for V114's reason: the table has existed since V106, so table existence cannot
       separate the rungs. acu_utilization_percent is the sentinel rather than one of the other two added
       columns because it is the one every CPU band reads — without it a card and the High CPU alert both
       fall to Unknown, which is the failure this probe exists to prevent. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_cpu_utilization'
                                                     AND   column_name = 'acu_utilization_percent'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'custom_alert_rules'),
    /* V117 probes a TRIGGER, the only object the rung creates. Read from pg_trigger rather than
       information_schema.triggers, which lists only triggers on relations the CURRENT role owns or holds a
       non-SELECT privilege on — the read-only viewer role holds neither on this table, so that view would
       report the rung absent on a fully-migrated store and the connect gate would refuse it. pg_trigger is
       world-readable, the same reasoning the pg_indexes and pg_inherits sentinels above rest on. */
    EXISTS (SELECT 1 FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE t.tgname = 'trg_bump_mute_rules' AND n.nspname = 'config'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'alert_persistence_state'),
    /* V119 probes a COLUMN for V57's reason: config_alert_settings has existed since V17, so table
       existence cannot separate the rungs. The warn ratio is the sentinel rather than its critical sibling
       because it is the one that decides whether the alert fires at all — the critical column only grades a
       fire that already happened. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings'
                                                     AND   column_name = 'retention_hold_warn_ratio'),
    /* V120 probes a COLUMN for V57's reason: config_alert_settings has existed since V17, so table
       existence cannot separate the rungs. The warn tier is the sentinel rather than its critical sibling
       because it is the one that decides whether the band leaves Healthy at all — the critical column only
       grades a server the warn tier already moved. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings'
                                                     AND   column_name = 'deadlock_warn_per_hour'),
    /* V121 probes a TABLE, which separates the rungs cleanly because this one is new in it. Its column
       sibling on the two plan-capturing fact tables would serve as well; the table is the simpler question
       to ask of information_schema. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'oversized_plan_backlog'),
    /* V122 probes a COLUMN for V57's reason: config_alert_settings has existed since V17, so table
       existence cannot separate the rungs. Either column would answer for the pair — both land in this one
       rung's transaction — so there is no warn/critical asymmetry to prefer one by, unlike the V119 and
       V120 sentinels above; the deadlock threshold is chosen simply because it is the rung's first ALTER,
       and that choice is stated so nobody goes looking for a significance it does not have. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings'
                                                     AND   column_name = 'pg_deadlock_count_threshold'),
    /* V123 probes a TABLE, which separates the rungs cleanly because all four of the rung's objects
       are new in it. The run table is the sentinel rather than one of its three siblings because it is
       the parent every read of this feature starts from — a store with the children but not the runs
       has nothing to hang them on, so its absence is the honest answer that the store predates the
       rung. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'fleet_sweep_runs'),
    /* V124 probes a COLUMN for V57's reason: config_alert_settings has existed since V17, so table
       existence cannot separate the rungs. Either column would answer for the pair — both land in this
       one rung's transaction — so the enabled switch is chosen simply because it is the rung's first
       ALTER, and that choice is stated so nobody goes looking for a significance it does not have. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings'
                                                     AND   column_name = 'fleet_sweep_enabled'),
    /* V125 probes a COLUMN for V57's reason: config_collector_schedules has existed since V17, so
       table existence cannot separate the rungs, and the rung adds exactly one column — there is no
       sibling to prefer or to explain not preferring. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_collector_schedules'
                                                     AND   column_name = 'databases'),
    /* V126 probes a COLUMN for V57's reason: config_alert_settings has existed since V17, so table
       existence cannot separate the rungs, and the rung adds exactly one column — there is no
       sibling to prefer or to explain not preferring. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings'
                                                     AND   column_name = 'self_disk_free_warn_gb'),
    /* V127 probes a COLUMN for V57's reason: the four delta-family tables have existed since V4/V10, so
       table existence cannot separate the rungs. The rung adds the same column to four tables in one
       transaction, so any of them would answer; the wait table is chosen because it is the rung's first
       ALTER and the family the finding was written against, and that choice is stated so nobody goes
       looking for a significance it does not have. Named only in this probe line, never in prose, per
       the V71 finding: the coverage ratchet strips information_schema lines but cannot strip a comment. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'wait_stats'
                                                     AND   column_name = 'sample_interval_seconds'),
    /* V128 probes a COLUMN for the same reason as V127: its five tables have existed since V4/V10 and
       V63/V64, so table existence cannot separate the rungs. The rung adds the same interval column to
       four tables and two offset columns to a fifth in one transaction, so any of the six would answer;
       the procedure table is chosen because it is the rung's first ALTER, and that choice is stated so
       nobody goes looking for a significance it does not have. The column NAME is shared with V127's
       sentinel — only the TABLE differs — which is exactly why the table is part of the predicate. Named
       only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'procedure_stats'
                                                     AND   column_name = 'sample_interval_seconds'),
    /* V129 probes a TABLE, which separates the rungs cleanly because the table is new in it — the same
       shape as V103 and V106, the two log-reader tables before it. Named only in this probe line, never
       in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_log_events'),
    /* V130 probes a COLUMN for V127's reason: the table is V129's, so table existence cannot separate the
       two rungs. The rung adds thirteen columns in one ALTER, so any would answer; the WAL-bytes column is
       chosen because it is the last term of the last clause the parser lifts, and that choice is stated so
       nobody goes looking for a significance it does not have. Named only in this probe line, never in
       prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_log_events'
                                                     AND   column_name = 'wal_bytes'),
    /* V131 probes a TABLE — new in it, so table existence separates the rungs cleanly (the V116/V123 shape
       for a config-plane table). Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'config_notification_routes'),
    /* V132 probes a COLUMN for V57's reason: the perfmon table has existed since V4, so table existence
       cannot separate the rungs, and the rung adds exactly one column — there is no sibling to prefer or to
       explain not preferring. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'perfmon_stats'
                                                     AND   column_name = 'cntr_type'),
    /* V133 probes a COLUMN for V57's reason: both of its tables have existed since V83 / V96, so table
       existence cannot separate the rungs. The rung adds one column to each of two tables in one
       transaction, so either would answer; the per-database counters table is chosen because it is the
       rung's first ALTER, and that choice is stated so nobody goes looking for a significance it does not
       have. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_database_stats'
                                                     AND   column_name = 'numbackends'),
    /* V134 probes a COLUMN for V57's reason: both of its tables have existed since V1, so table existence
       cannot separate the rungs. The rung adds one column to each of two tables in one transaction, so
       either would answer; the CPU table's UTC twin is chosen because it is the rung's first ALTER and the
       column the viewer's own CPU read now names, and that choice is stated so nobody goes looking for a
       significance it does not have. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'cpu_utilization_stats'
                                                     AND   column_name = 'sample_time_utc'),
    /* V135 probes a COLUMN for V57's reason: config_alert_settings has existed since V17, so table existence
       cannot separate the rungs. The rung adds two columns to that one table in one transaction, so either
       would answer; the program-name-prefix list is chosen because it is the rung's first ALTER, and that
       choice is stated so nobody goes looking for a significance it does not have. Named only in this probe
       line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings'
                                                     AND   column_name = 'long_running_query_excluded_program_name_prefixes'),
    /* V136 probes a TABLE — its one new table, so table existence separates the rungs cleanly (the V103 /
       V106 / V129 shape for a collector table). The rung also adds six columns to a table that has existed
       since V106, and either would answer; the new table is chosen because it is the rung's first statement,
       and that choice is stated so nobody goes looking for a significance it does not have. Named only in
       this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_database_size_stats'),
    /* V137 probes a COLUMN for V57's reason: all three of its tables have existed since V76 / V17 / V53, so
       table existence cannot separate the rungs. The rung adds eight columns across three tables in one
       transaction, so any would answer; the Query Store health table's capture mode is chosen because it
       is the rung's first ALTER and the one column of the six this viewer's own grid will read, and that
       choice is stated so nobody goes looking for a significance it does not have. Named only in this
       probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_store_health'
                                                     AND   column_name = 'query_capture_mode'),
    /* V138 probes a COLUMN for V57's reason: its one table has existed since V102, so table existence
       cannot separate the rungs. The rung adds two columns and either would answer; the database scope is
       chosen because it is the rung's first ADD COLUMN and the one the shared config reader's own predicate
       names first, and that choice is stated so nobody goes looking for a significance it does not have.
       Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_server_config'
                                                     AND   column_name = 'database_name'),
    /* V139 probes a COLUMN for V57's reason: both of its tables have existed since V53 / V88, so table existence
       cannot separate the rungs. The rung adds one column to each of two tables in one transaction, so either
       would answer; the write-side counters table is chosen because it is the one a viewer read names (the
       write-side panel's shared reader), which is what makes this gate load-bearing, and that choice is stated so
       nobody goes looking for a significance it does not have. Named only in this probe line, never in prose,
       per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_write_stats'
                                                     AND   column_name = 'postmaster_start_time'),
    /* V140 probes a COLUMN for V57's reason: its one table has existed since V53, so table existence cannot
       separate the rungs. The rung adds one column, so there is no choice of which to name; it is not yet
       read by any viewer surface, so this gate rests on the standing invariant alone. Named only in this
       probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'store_metrics'
                                                     AND   column_name = 'checkpoints_timed'),
    /* V141 probes a TABLE — its one new table, so table existence separates the rungs cleanly (the V103 /
       V106 / V121 / V129 / V136 shape). It is not yet read by any viewer surface, so this gate rests on the
       standing invariant alone. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'analysis_collection_caveats'),
    /* V142 (#4196) probes an INDEX the same way V22 does: idx_index_object_stats_server_time is additive
       (indexes are not listed in information_schema, so this reads the world-readable pg_indexes catalog like
       V22's own arm does). Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_index_object_stats_server_time'),
    /* V143 creates three tables at once, so any of them would answer; the replay table is chosen because the rung
       creates it last, and that choice is stated so nobody goes looking for a significance it does not have.
       Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'query_store_interval_latest_pending'),
    /* V144 (#4211) probes a TABLE — its two new tables, so table existence separates the rungs cleanly (the
       V103 / V106 / V121 / V129 / V136 / V141 shape). It is not yet read by any viewer surface, so this gate
       rests on the standing invariant alone. Named only in this probe line, never in prose, per the V71
       finding. */
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'raw_chunk_interval_rung_history'),
    /* V145 (#3953) also creates three tables at once (the wide interval table beside V143's), so any would
       answer; the replay table is chosen for the same reason as V143's own arm. Named only in this probe line,
       never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'query_store_interval_wide_pending'),
    /* V146 (#4215) probes a TABLE — its one new table, so table existence separates the rungs cleanly (the
       V103 / V106 / V121 / V129 / V136 / V141 / V144 shape). It is not yet read by any viewer surface, so
       this gate rests on the standing invariant alone. Named only in this probe line, never in prose, per
       the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'managed_conf_verdicts'),
    /* V147 (#4442 scope 1) probes a COLUMN DEFAULT the same way it changed one: the rung's ALTER COLUMN
       ... SET DEFAULT is additive-in-effect (a store that already carries an operator-set value keeps it;
       only the column's own default moves), so the probe reads world-readable information_schema.columns
       rather than a value that could be masked by an operator override. It is not yet read by any viewer
       surface, so this gate rests on the standing invariant alone. Named only in this probe line, never in
       prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_service' AND column_name = 'compose_statement_timeout_seconds' AND column_default = '60'),
    /* V148 (#4442 scope 2) probes the new read-latency histogram table. It is not yet read by any viewer
       surface, so this gate rests on the standing invariant alone. Named only in this probe line, never in
       prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'read_latency'),
    /* V149 (#4250) drops query_store_plan_map's last_seen btree index and sets fillfactor 90 on it, so the
       liveness touch's UPDATE can go HOT. This probes NEGATIVELY — the index's ABSENCE plus the fillfactor
       reloption — unlike every other sentinel in this list, which probes an object's presence. It is not
       yet read by any viewer surface, so this gate rests on the standing invariant alone. Named only in
       this probe line, never in prose, per the V71 finding. */
    ((SELECT c.reloptions FROM pg_class c WHERE c.oid = 'collect.query_store_plan_map'::regclass) @> ARRAY['fillfactor=90']
        AND NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = 'idx_query_store_plan_map_last_seen')),
    /* V150 (#4469, #4477) adds two supporting indexes: idx_collection_log_watermark (the per-collector
       watermark lookup) and idx_job_history_server_run (the Viewer's Job History tab read). It is not yet
       read by any viewer surface, so this gate rests on the standing invariant alone. Named only in this
       probe line, never in prose, per the V71 finding. */
    (EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = 'idx_collection_log_watermark')
        AND EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = 'idx_job_history_server_run')),
    /* V151 (#4475) probes a COLUMN for the V36/V37 reason: ag_replica_states has existed since V34, so
       table existence cannot separate the rungs. It is not yet read by any viewer surface, so this gate
       rests on the standing invariant alone. Named only in this probe line, never in prose, per the V71
       finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'ag_replica_states' AND column_name = 'group_id'),
    /* V152 (#4503) probes NEGATIVELY, the V149 shape: the six Query Store rollups' auto-created two-key
       group index (first key one of the rung's drop-list columns, second key bucket) is ABSENT. This walks
       query_store_stats_hourly's own pg_rewrite/pg_depend dependency to its materialization hypertable
       rather than the TimescaleDB information views, so it needs no pg_extension guard. ANDed with V151's
       own sentinel, copied verbatim, rather than a to_regclass(...) IS NOT NULL existence check on the
       view: a plain-PostgreSQL store has no rollup at all, so to_regclass(...) is NULL there at every
       version, the dependency walk finds no rows, and the bare NOT EXISTS reads that absence as ""index
       dropped"" — misreporting a plain-PostgreSQL store at V151 as V152. That store is harmless to leave
       unresolved here: V152 is a no-op without TimescaleDB, so a plain-PostgreSQL store's schema at V151
       and V152 is identical, and ANDing with V151's own sentinel makes this arm require V151 first, so a
       store below V151 fails that sentinel and falls through to the next arm instead. It is not yet read
       by any viewer surface, so this gate rests on the standing invariant alone. Named only in this probe
       line, never in prose, per the V71 finding. */
    (NOT EXISTS (
        SELECT 1
        FROM pg_rewrite r
        JOIN pg_depend d ON d.objid = r.oid
        JOIN pg_class matc ON matc.oid = d.refobjid AND matc.oid <> r.ev_class
        JOIN pg_index i ON i.indrelid = matc.oid AND i.indnkeyatts = 2
        JOIN pg_attribute a1 ON a1.attrelid = matc.oid AND a1.attnum = i.indkey[0] AND a1.attname = 'query_hash'
        JOIN pg_attribute a2 ON a2.attrelid = matc.oid AND a2.attnum = i.indkey[1] AND a2.attname = 'bucket'
        WHERE r.ev_class = to_regclass('collect.query_store_stats_hourly')
    )
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'ag_replica_states' AND column_name = 'group_id')),
    /* V153 (#4608) probes an INDEX the same way V22/V142 do: idx_query_store_interval_latest_first_exec is
       additive (indexes are not listed in information_schema, so this reads the world-readable pg_indexes
       catalog like those arms do). It is not yet read by any viewer surface, so this gate rests on the
       standing invariant alone. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_query_store_interval_latest_first_exec'),
    /* V154 (#4608, split #4615) probes the second index the V153 rung's build once created in the same
       rung, now its own rung with its own MigrationCommandTimeoutSeconds window — the same additive-index
       shape as V153's arm above. Not yet read by any viewer surface, so this gate rests on the standing
       invariant alone. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_query_store_interval_wide_first_exec'),
    /* V155 (#4765) adds interval_end_time_utc to the Query Store stats table and to the interval-wide table.
       The sentinel is the interval-wide table's column, not the stats table's: a fresh store's stats table is
       created from the collector's current column list and so has the column before this rung ever runs,
       while the interval-wide table only ever gets it from the rung itself. The viewer's Query Store duration trend
       reads the column to rate each interval over its own length: its raw read (QueryStoreDurationTrendSql),
       its table-routed twin (QueryStoreDurationTrendTableSql) and the rollup route's raw class (#4765). So this
       sentinel is what keeps those reads off a store that lacks the column: the connect-time gate blocks a store
       below V155 (and fails open only when the probe itself fails). Named only in this probe line, never in
       prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_store_interval_wide' AND column_name = 'interval_end_time_utc'),
    /* V156 (#4834) stores the hour's longest single checkpoint sync on the store's own checkpointer row. The
       sentinel is the milliseconds column; its instant column arrives in the same statement. Not yet read by any
       viewer surface, so this gate rests on the standing invariant alone. Named only in this probe line, never in
       prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'store_metrics' AND column_name = 'checkpoint_longest_sync_ms'),
    /* V157 gives a mute rule an optional store server id. The column is the sentinel. Named only in this probe line,
       never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_mute_rules' AND column_name = 'server_id'),
    /* V158 (#4961) holds the install id in a one-row table of its own. The table is the sentinel. Named only in this
       probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'config' AND table_name = 'config_install_id'),
    /* V159 (#4961) adds the install id table's own OID as a third binding. The column is the sentinel. Named only in
       this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_install_id' AND column_name = 'table_oid'),
    /* V160 (#4938) gives a collector an optional run time, in a table of its own. The table is the sentinel, found by
       name in the catalog so the answer does not depend on which privileges the connecting role holds on it. The
       schedule window reads that table (its Run at column) and saving a run time writes it, so a store below V160
       would answer them with a missing-table error, and the connect gate refuses such a store before the window can
       open. This probe is the one thing that can fail open: when it throws, the gate lets the store in, so the run-time
       read treats a missing table as no run times and the run-time save says the store is older than this viewer,
       each on its own. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('config.config_collector_run_times') IS NOT NULL),
    /* V161 (#5094) adds the daily summary tables for the Query Store top read. The first of them is the sentinel,
       found by name in the catalog so the answer does not depend on the connecting role's privileges on it. Named
       only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.query_store_top_daily') IS NOT NULL),
    /* V162 (#5097) adds the slow-read record. The table is the sentinel, found by name in the catalog so the answer
       does not depend on the connecting role's privileges on it. Named only in this probe line, never in prose, per
       the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.slow_reads') IS NOT NULL),
    /* V163 (#5097) adds the store's statement history. The history table is the sentinel, found by name in the catalog
       so the answer does not depend on the connecting role's privileges on it. Named only in this probe line, never in
       prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.store_statement_history') IS NOT NULL),
    /* V164 (#4605) adds the hourly row-count ledger of the query_stats table. The ledger table is the sentinel, found by
       name in the catalog so the answer does not depend on the connecting role's privileges on it. Named only in this
       probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.query_stats_hour_ledger') IS NOT NULL),
    /* V165 (#5366) adds the tables that publish the service's password key. The key table is the sentinel, found by
       name in the catalog so the answer does not depend on the connecting role's privileges on it. Named only in this
       probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('config.password_key') IS NOT NULL),
    /* V166 probes a COLUMN for V57's reason: config_notification has existed since V17, so table
       existence cannot separate the rungs, and the rung adds exactly one column — there is no
       sibling to prefer or to explain not preferring. Named only in this probe line, never in
       prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_notification'
                                                     AND   column_name = 'pagerduty_auto_resolve'),
    /* V167 (#5456) adds the table that records which old-format saved passwords were present at the upgrade. That table
       is the sentinel, found by name in the catalog so the answer does not depend on the connecting role's privileges on
       it. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('config.legacy_secret_pin_candidate') IS NOT NULL),
    /* V168 (#5448) adds the per-day plan totals for PLAN_REGRESSION. The built table is the sentinel, found by name
       in the catalog so the answer does not depend on the connecting role's privileges on it. Named only in this
       probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.plan_regression_daily_built') IS NOT NULL),
    /* V169 probes a COLUMN for V57's reason: config_monitored_servers has existed since V2, so table
       existence cannot separate the rungs. The role column is the sentinel; its two siblings arrive in
       the same statement and are never separately present. Named only in this probe line, never in
       prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_monitored_servers'
                                                     AND   column_name = 'aws_role_arn'),
    /* V170 (#5495) adds the hourly rollup of the differenced PostgreSQL I/O counters. The state table is the sentinel, found by name
       in the catalog so the answer does not depend on the connecting role's privileges on it. Named only in this probe line, never in
       prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.pg_io_stats_hourly_state') IS NOT NULL),
    /* V171 (#5571) makes the wide Query Store interval table a day-partitioned table, with the old table attached as its legacy
       partition. The sentinel is the parent's relkind ('p', a partitioned table), read from the catalog by name so the answer does
       not depend on the connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_class WHERE oid = to_regclass('collect.query_store_interval_wide') AND relkind = 'p'),
    /* V172 (#5571) does the same for the latest-interval table. Same probe shape as V171's. */
    EXISTS (SELECT 1 FROM pg_class WHERE oid = to_regclass('collect.query_store_interval_latest') AND relkind = 'p'),
    /* V173 (#5582) adds the exact stamp-grain rollup of the wide Query Store interval table. The hours table is the sentinel, found
       by name in the catalog so the answer does not depend on the connecting role's privileges on it. Named only in this probe line,
       never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.query_store_compose_stamp_hours') IS NOT NULL),
    /* V174 (#5655) adds the table in which each service host records its product version. That table is the sentinel, found by
       name in the catalog so the answer does not depend on the connecting role's privileges on it. Named only in this probe line,
       never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('config.service_version') IS NOT NULL),
    /* V175 (#5631) adds the per-day rows-per-call sum to the daily plan regression totals. That column is the sentinel, found by
       catalog so the answer does not depend on the connecting role's privileges. Named only in this probe line, never in prose,
       per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('collect.plan_regression_daily') AND attname = 'rowcount_sum' AND NOT attisdropped),
    /* V176 (#5638) adds the per-collection procedure totals table. That table is the sentinel, found by name in the catalog so the answer does not
       depend on the connecting role's privileges on it. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.procedure_stats_totals') IS NOT NULL),
    /* V177 (#5641) adds the Runnable Queue alert's settings to the alert settings row. The enabled column is the sentinel, found by
       catalog so the answer does not depend on the connecting role's privileges. Named only in this probe line, never in prose, per
       the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_alert_settings') AND attname = 'runnable_queue_enabled' AND NOT attisdropped),
    /* V178 (#5684) adds the per-batch Query Store watermark table. That table is the sentinel, found by name in the catalog so the answer does not
       depend on the connecting role's privileges on it. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.query_store_watermarks') IS NOT NULL),
    /* V179 (#5640) adds the Client Timeouts alert's settings to the alert settings row. The enabled column is the sentinel, found by
       catalog so the answer does not depend on the connecting role's privileges. Named only in this probe line, never in prose, per
       the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_alert_settings') AND attname = 'client_timeouts_enabled' AND NOT attisdropped),
    /* V180 (#5654) adds the registry of the services that raise alerts through this store. That table is the sentinel, found by
       name in the catalog so the answer does not depend on the connecting role's privileges on it. Named only in this probe line,
       never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('config.external_alert_source') IS NOT NULL),
    /* V181 (#5750) adds the route scope columns to the notification routes. The server scope column is the sentinel, found by
       catalog so the answer does not depend on the connecting role's privileges. Named only in this probe line, never in prose,
       per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_notification_routes') AND attname = 'scope_server_id' AND NOT attisdropped),
    /* V182 (#5506) adds the window band, the in-flight peak and the connection wait to the read-latency table. The window band
       column is the sentinel, found by catalog so the answer does not depend on the connecting role's privileges. Named only in
       this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('collect.read_latency') AND attname = 'window_band' AND NOT attisdropped),
    /* V183 (#5793) adds the read caller to the slow-read table and the distinct caller count to the read-latency table. The
       distinct caller column is the sentinel, found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('collect.read_latency') AND attname = 'distinct_callers' AND NOT attisdropped),
    /* V184 (#5632) adds the investigation tables and the agent columns on the notification routes. The request table is the
       sentinel, found by catalog so the answer does not depend on the connecting role's privileges. Named only in this probe
       line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('config.investigation_requests') IS NOT NULL),
    /* V185 (#5812) adds the partial-day watermark to the plan regression built table. Found by catalog so the answer does not
       depend on the connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('collect.plan_regression_daily_built') AND attname = 'built_through' AND NOT attisdropped),
    /* V186 (#5810) adds the hourly rollup of the collection log. The table is the sentinel, found by catalog so the answer
       does not depend on the connecting role's privileges. Named only in this probe line, never in prose, per the V71
       finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.collection_log_hourly') IS NOT NULL),
    /* V187 (#5825) adds the per-channel resolution flags, the route override and the alert log's resolution columns. Found by
       catalog so the answer does not depend on the connecting role's privileges. Named only in this probe line, never in prose,
       per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_alert_log') AND attname = 'resolution_outcome' AND NOT attisdropped),
    /* V188 (#5821) adds the alert's values to the investigation link rows. Found by catalog so the answer does not depend on
       the connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.investigation_request_alerts') AND attname = 'context_json' AND NOT attisdropped),
    /* V189 (#5867) adds the failure record of the Query Store compose builder. That table is the sentinel, found by name in the
       catalog so the answer does not depend on the connecting role's privileges on it. Named only in this probe line, never in
       prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.query_store_compose_stamp_failures') IS NOT NULL),
    /* V190 (#5857) adds the size-estimate marker to the store's own size series. Found by catalog so the answer does not depend on
       the connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('collect.store_metrics') AND attname = 'size_estimated' AND NOT attisdropped),
    /* V191 (#5909) adds the Level 2 store. The feedback table is the sentinel, found by name in the catalog so the answer does
       not depend on the connecting role's privileges on it. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('config.investigation_feedback') IS NOT NULL),
    /* V192 (#5977) adds the query-tag store. The tag table is the sentinel, found by name in the catalog so the answer does not
       depend on the connecting role's privileges on it. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('config.query_tags') IS NOT NULL),
    /* V193 (#5997) adds the Phase C store. The evidence table is the sentinel, found by name in the catalog so the answer does
       not depend on the connecting role's privileges on it. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('config.investigation_evidence') IS NOT NULL),
    /* V194 (#5985) rebuilds the query-tag sightings key to include the database. The new unique index is the sentinel, found by
       name in the catalog so the answer does not depend on the connecting role's privileges on it. Named only in this probe line,
       never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.ux_query_tag_sightings_key_db') IS NOT NULL),
    /* V195 (#5632) adds the forced-plans collector table. Found by name in the catalog so the answer does not depend on the
       connecting role's privileges on it. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('collect.query_store_forced_plans') IS NOT NULL),
    /* V196 (#5632) adds the per-server action switch table. Found by name in the catalog so the answer does not depend on the
       connecting role's privileges on it. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('config.config_server_actions') IS NOT NULL),
    /* V197 (#5632) adds the force-check link to the investigation requests. Found by catalog so the answer does not depend on the
       connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.investigation_requests') AND attname = 'force_check_of' AND NOT attisdropped),
    /* V198 (#6066) adds the pseudonym key table. Found by name in the catalog (the probe never reads the table, which only the store owner
       can) so the answer does not depend on the connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 WHERE to_regclass('config.pseudonym_key') IS NOT NULL),
    /* V199 (#5632) adds the head-blocker evidence columns to the blocking snapshots. Found by catalog so the answer does not depend on the
       connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('collect.dmv_blocking_snapshots') AND attname = 'blocking_tran_log_bytes_used' AND NOT attisdropped),
    /* V200 (#6098) adds the Volume Free Space alert's excluded-volumes list to the alert settings row. Found by catalog so the answer does not depend on the
       connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_alert_settings') AND attname = 'excluded_volumes' AND NOT attisdropped),
    /* V201 (#6160) adds the plan-guide columns to the forced-plan capture. Found by catalog so the answer does not depend on the
       connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('collect.query_store_forced_plans') AND attname = 'plan_guide_scope' AND NOT attisdropped),
    /* V202 (#6159) adds the primary replica's name to the AG replica states. Found by catalog so the answer does not depend on the
       connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('collect.ag_replica_states') AND attname = 'primary_replica' AND NOT attisdropped),
    /* V203 (#6170) adds the hypothetical-index login pair to the server registry. Found by catalog so the answer does not depend on the
       connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_monitored_servers') AND attname = 'hypothetical_index_encrypted_password' AND NOT attisdropped),
    /* V204 (#6214) adds the folder, personal flag and creator to the saved custom views. Found by catalog so the answer does not depend on
       the connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.custom_views') AND attname = 'created_by' AND NOT attisdropped),
    /* V205 (#6235) adds the optional expiry to the custom alert rules. Found by catalog so the answer does not depend on the
       connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.custom_alert_rules') AND attname = 'expires_at_utc' AND NOT attisdropped),
    /* V206 (#6227) lets the collector schedule rows hold a NULL enabled state (inherit). Found by catalog, as a column that no longer
       carries NOT NULL, so the answer does not depend on the connecting role's privileges. Named only in this probe line, never in prose,
       per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_collector_schedules') AND attname = 'enabled' AND NOT attnotnull AND NOT attisdropped),
    /* V207 (#6234) adds the change watch tables. Found by catalog so the answer does not depend on the connecting role's privileges.
       Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.change_watches') AND attname = 'watch_id' AND NOT attisdropped),
    /* V208 (#6231) adds the start time to the mute rules. Found by catalog so the answer does not depend on the
       connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_mute_rules') AND attname = 'starts_at_utc' AND NOT attisdropped),
    /* V209 (#6211) adds the Query Store fetch top-N setting. Found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_service') AND attname = 'query_store_fetch_top_n' AND NOT attisdropped),
    /* V210 (#6281) adds the guard on the MCP tag writes. Found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = to_regclass('config.server_tag_map') AND tgname = 'trg_agent_route_tag_guard' AND NOT tgisinternal),
    /* V211 (#5736) adds the relative Client Timeouts bars' multiples. Found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_alert_settings') AND attname = 'client_timeouts_warning_multiple' AND NOT attisdropped),
    /* V212 (#6285) adds the machine identity to the server registry. Found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('collect.servers') AND attname = 'machine_identity' AND NOT attisdropped),
    /* V213 (#6254) adds the Query Store replica setting. Found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_monitored_servers') AND attname = 'query_store_replica' AND NOT attisdropped),
    /* V214 (#6334) adds the known-pattern entries. Found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    to_regclass('config.config_known_patterns') IS NOT NULL,
    /* V215 adds the agent-table indexes. Found by catalog (pg_indexes is world-readable) so the answer does not depend on the connecting
       role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'config' AND indexname = 'ix_investigation_reports_family_of'),
    /* V216 (#5736) turns the Client Timeouts alert on by default. Found by catalog: the column default of the switch reads TRUE only once the
       rung has run, and pg_attrdef is world-readable, so the answer does not depend on the connecting role's privileges. Named only in this
       probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attrdef AS d JOIN pg_attribute AS a ON a.attrelid = d.adrelid AND a.attnum = d.adnum WHERE d.adrelid = to_regclass('config.config_alert_settings') AND a.attname = 'client_timeouts_enabled' AND NOT a.attisdropped AND pg_get_expr(d.adbin, d.adrelid) = 'true'),
    /* V217 (#6581) adds the Query Store hints table. Found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    to_regclass('collect.query_store_hints') IS NOT NULL,
    /* V218 (#6569) widens the server action switch list with the module recompile and the plan guides. Found by catalog (the stored CHECK names it) so the
       answer does not depend on the connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_config_server_actions_action' AND conrelid = to_regclass('config.config_server_actions') AND pg_get_constraintdef(oid) ~ 'drop_plan_guide'),
    /* V219 (#6750) adds the Query Store self-statement key tables. Found by catalog so the answer does not depend on the connecting
       role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    to_regclass('collect.query_store_self_keys_state') IS NOT NULL,
    /* V220 (#6798) adds the Job History statistics index. Found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    to_regclass('collect.idx_job_history_job_success') IS NOT NULL,
    /* V221 (#6800) adds the Query Store hash-hourly table for the fleet tag roll-up. Found by catalog so the answer does not depend on the
       connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    to_regclass('collect.query_store_hash_hourly') IS NOT NULL,
    /* V222 (#6874) adds the SQL Server Agent alerts table. Found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    to_regclass('collect.agent_alerts') IS NOT NULL,
    /* V223 (#6873) adds the database protection table. Found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    to_regclass('collect.database_protection') IS NOT NULL,
    /* V224 (#6875) adds the identity capacity table. Found by catalog so the answer does not depend on the connecting role's privileges.
       Named only in this probe line, never in prose, per the V71 finding. */
    to_regclass('collect.identity_capacity') IS NOT NULL,
    /* V225 (#6894) adds the AWS RDS fact to the server registry. Found by catalog so the answer does not depend on the connecting role's
       privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('collect.servers') AND attname = 'is_aws_rds' AND NOT attisdropped),
    /* V226 (#6893) adds the AWS RDS Web or Express fact to the server registry. Found by catalog like the V225 column above. Named only in
       this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('collect.servers') AND attname = 'is_aws_rds_web_or_express' AND NOT attisdropped),
    /* V227 (#6926) adds the tag scope to the threshold overrides. Found by catalog like the V225 column above. Named only in this probe line, never in
       prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_threshold_overrides') AND attname = 'scope_tag_id' AND NOT attisdropped),
    /* V228 (#6927) adds the four settable blocking band columns to the alert settings row. Found by catalog like the V226 column above. The
       warn per hour column is the sentinel, the first of the four. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_alert_settings') AND attname = 'blocking_warn_per_hour' AND NOT attisdropped),
    /* V229 (#6928) adds the severity filter to the notification routes. Found by catalog like the V225 column above. Named only in
       this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_notification_routes') AND attname = 'severities' AND NOT attisdropped),
    /* V230 (#7028) adds the two snapshot wait tiers of the blocking band to the alert settings row. Found by catalog like the V228 column above. The
       warn tier is the sentinel, the first of the two. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_alert_settings') AND attname = 'blocking_warn_snapshot_wait_seconds' AND NOT attisdropped),
    /* V231 (#7040) adds the recurring schedule to the mute rules. The start time column is the sentinel, found by catalog so the answer does
       not depend on the connecting role's privileges. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_mute_rules') AND attname = 'recur_start_time' AND NOT attisdropped),
    /* V232 (#7041) adds the Daily Health Report columns to the alert settings row. Found by catalog like the V228 column above. The enabled
       column is the sentinel, the first of the four. Named only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_alert_settings') AND attname = 'daily_report_enabled' AND NOT attisdropped),
    /* V233 (#6979) adds the report notice setting to the notification routes. Found by catalog like the V229 column. Named only in this probe
       line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_notification_routes') AND attname = 'report_notice' AND NOT attisdropped),
    /* V235 (#7083) moves the blocking count bar's column default from 1 to 3. Found by the column default like the V216 sentinel above. Named
       only in this probe line, never in prose, per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attrdef AS d JOIN pg_attribute AS a ON a.attrelid = d.adrelid AND a.attnum = d.adnum WHERE d.adrelid = to_regclass('config.config_alert_settings') AND a.attname = 'blocking_count_threshold' AND NOT a.attisdropped AND pg_get_expr(d.adbin, d.adrelid) = '3'),
    /* V236 (#7083) adds the action to the mute rules. Found by catalog like the V229 column. Named only in this probe line, never in prose,
       per the V71 finding. */
    EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.config_mute_rules') AND attname = 'action' AND NOT attisdropped)";

    /// <summary>The store schema version this viewer build requires — the highest migration it knows
    /// (<see cref="StorageVersion.SchemaVersion"/>). The connect-time gate blocks a store below this.</summary>
    public static int RequiredStoreSchemaVersion => StorageVersion.SchemaVersion;

    /// <summary>
    /// Reads the store's effective schema version via <see cref="StoreSchemaProbeSql"/>. Returns null when it
    /// can't be determined (any non-cancellation error — an unreachable store, an <c>information_schema</c>
    /// quirk) so the connect-time gate FAILS OPEN: a possibly-healthy store is never blocked by a probe
    /// hiccup. A truly-down store is caught first by <see cref="EnsureStoreReachableAsync"/> (finding B3), and
    /// the executor 42703/42P01 translation (finding B2) is the write-path backstop.
    /// </summary>
    public async Task<int?> GetStoreSchemaVersionAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = _dataSource.CreateCommand(StoreSchemaProbeSql);
            command.CommandTimeout = ViewerCommandDeadlines.ConnectGateSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                return MapProbedSchemaVersion(reader.GetBoolean(0), reader.GetBoolean(1), reader.GetBoolean(2), reader.GetBoolean(3), reader.GetBoolean(4), reader.GetBoolean(5), reader.GetBoolean(6), reader.GetBoolean(7), reader.GetBoolean(8), reader.GetBoolean(9), reader.GetBoolean(10), reader.GetBoolean(11), reader.GetBoolean(12), reader.GetBoolean(13), reader.GetBoolean(14), reader.GetBoolean(15), reader.GetBoolean(16), reader.GetBoolean(17), reader.GetBoolean(18), reader.GetBoolean(19), reader.GetBoolean(20), reader.GetBoolean(21), reader.GetBoolean(22), reader.GetBoolean(23), reader.GetBoolean(24), reader.GetBoolean(25), reader.GetBoolean(26), reader.GetBoolean(27), reader.GetBoolean(28), reader.GetBoolean(29), reader.GetBoolean(30), reader.GetBoolean(31), reader.GetBoolean(32), reader.GetBoolean(33), reader.GetBoolean(34), reader.GetBoolean(35), reader.GetBoolean(36), reader.GetBoolean(37), reader.GetBoolean(38), reader.GetBoolean(39), reader.GetBoolean(40), reader.GetBoolean(41), reader.GetBoolean(42), reader.GetBoolean(43), reader.GetBoolean(44), reader.GetBoolean(45), reader.GetBoolean(46), reader.GetBoolean(47), reader.GetBoolean(48), reader.GetBoolean(49), reader.GetBoolean(50), reader.GetBoolean(51), reader.GetBoolean(52), reader.GetBoolean(53), reader.GetBoolean(54), reader.GetBoolean(55), reader.GetBoolean(56), reader.GetBoolean(57), reader.GetBoolean(58), reader.GetBoolean(59), reader.GetBoolean(60), reader.GetBoolean(61), reader.GetBoolean(62), reader.GetBoolean(63), reader.GetBoolean(64), reader.GetBoolean(65), reader.GetBoolean(66), reader.GetBoolean(67), reader.GetBoolean(68), reader.GetBoolean(69), reader.GetBoolean(70), reader.GetBoolean(71), reader.GetBoolean(72), reader.GetBoolean(73), reader.GetBoolean(74), reader.GetBoolean(75), reader.GetBoolean(76), reader.GetBoolean(77), reader.GetBoolean(78), reader.GetBoolean(79), reader.GetBoolean(80), reader.GetBoolean(81), reader.GetBoolean(82), reader.GetBoolean(83), reader.GetBoolean(84), reader.GetBoolean(85), reader.GetBoolean(86), reader.GetBoolean(87), reader.GetBoolean(88), reader.GetBoolean(89), reader.GetBoolean(90), reader.GetBoolean(91), reader.GetBoolean(92), reader.GetBoolean(93), reader.GetBoolean(94), reader.GetBoolean(95), reader.GetBoolean(96), reader.GetBoolean(97), reader.GetBoolean(98), reader.GetBoolean(99), reader.GetBoolean(100), reader.GetBoolean(101), reader.GetBoolean(102), reader.GetBoolean(103), reader.GetBoolean(104), reader.GetBoolean(105), reader.GetBoolean(106), reader.GetBoolean(107), reader.GetBoolean(108), reader.GetBoolean(109), reader.GetBoolean(110), reader.GetBoolean(111), reader.GetBoolean(112), reader.GetBoolean(113), reader.GetBoolean(114), reader.GetBoolean(115), reader.GetBoolean(116), reader.GetBoolean(117), reader.GetBoolean(118), reader.GetBoolean(119), reader.GetBoolean(120), reader.GetBoolean(121), reader.GetBoolean(122), reader.GetBoolean(123), reader.GetBoolean(124), reader.GetBoolean(125), reader.GetBoolean(126), reader.GetBoolean(127), reader.GetBoolean(128), reader.GetBoolean(129), reader.GetBoolean(130), reader.GetBoolean(131), reader.GetBoolean(132), reader.GetBoolean(133), reader.GetBoolean(134), reader.GetBoolean(135), reader.GetBoolean(136), reader.GetBoolean(137), reader.GetBoolean(138), reader.GetBoolean(139), reader.GetBoolean(140), reader.GetBoolean(141), reader.GetBoolean(142), reader.GetBoolean(143), reader.GetBoolean(144), reader.GetBoolean(145), reader.GetBoolean(146), reader.GetBoolean(147), reader.GetBoolean(148), reader.GetBoolean(149), reader.GetBoolean(150), reader.GetBoolean(151), reader.GetBoolean(152), reader.GetBoolean(153), reader.GetBoolean(154), reader.GetBoolean(155), reader.GetBoolean(156), reader.GetBoolean(157), reader.GetBoolean(158), reader.GetBoolean(159), reader.GetBoolean(160), reader.GetBoolean(161), reader.GetBoolean(162), reader.GetBoolean(163), reader.GetBoolean(164), reader.GetBoolean(165), reader.GetBoolean(166), reader.GetBoolean(167), reader.GetBoolean(168), reader.GetBoolean(169), reader.GetBoolean(170), reader.GetBoolean(171), reader.GetBoolean(172), reader.GetBoolean(173), reader.GetBoolean(174), reader.GetBoolean(175), reader.GetBoolean(176), reader.GetBoolean(177), reader.GetBoolean(178), reader.GetBoolean(179), reader.GetBoolean(180), reader.GetBoolean(181), reader.GetBoolean(182), reader.GetBoolean(183), reader.GetBoolean(184), reader.GetBoolean(185), reader.GetBoolean(186), reader.GetBoolean(187), reader.GetBoolean(188), reader.GetBoolean(189), reader.GetBoolean(190), reader.GetBoolean(191), reader.GetBoolean(192), reader.GetBoolean(193), reader.GetBoolean(194), reader.GetBoolean(195), reader.GetBoolean(196), reader.GetBoolean(197), reader.GetBoolean(198), reader.GetBoolean(199), reader.GetBoolean(200), reader.GetBoolean(201), reader.GetBoolean(202), reader.GetBoolean(203), reader.GetBoolean(204), reader.GetBoolean(205), reader.GetBoolean(206), reader.GetBoolean(207), reader.GetBoolean(208), reader.GetBoolean(209), reader.GetBoolean(210));
            }

            return null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// Maps the <see cref="StoreSchemaProbeSql"/> sentinels to the store's effective version, newest present
    /// wins. V23 is folded INTO the V22 arm rather than being a standalone newest-first arm: once the V22 index
    /// is present, <paramref name="hasCollectionLogHypertableOrPlainPg"/> (collection_log is a hypertable, OR
    /// the store is plain PostgreSQL where V23 is an object-invisible no-op) → 23, else → 22. It cannot be a
    /// top arm because its plain-PG side is true for EVERY plain-PG store regardless of version, so it must be
    /// gated behind V22's engine-agnostic index. Below V22: <paramref name="hasDefaultTraceEvents"/> (V21) → 21,
    /// else <paramref name="hasAlertTuningKnobs"/> (V20) → 20, else <paramref name="hasAnalysisState"/> (V19) →
    /// 19, else <paramref name="hasAlertDeliveryOverride"/> (V18) → 18, else
    /// <paramref name="hasConfigControlPlane"/> (V17) → 17, else 16 — the "older than the V17 config control
    /// plane" floor (the exact pre-17 version isn't probed, but it is below what the viewer needs). Pure, so it
    /// is unit-tested without a live store; any schema bump past the newest arm trips the pinning test that keeps
    /// this in step with <see cref="StorageVersion.SchemaVersion"/>.
    /// </summary>
    internal static int MapProbedSchemaVersion(bool hasConfigControlPlane, bool hasAlertDeliveryOverride, bool hasAnalysisState, bool hasAlertTuningKnobs, bool hasDefaultTraceEvents, bool hasIndexObjectStatsLatestIndex, bool hasCollectionLogHypertableOrPlainPg, bool hasJobHistory, bool hasAgentStatus, bool hasGenericWebhook, bool hasDeadlocksDatabaseName, bool hasQueryStoreReplicaRole, bool hasLongQueryCompletions, bool hasWebDashboardConfig, bool hasCustomViews, bool hasServerTags, bool hasConnectionRefireKnobs = false, bool hasAgCollectors = false, bool hasAgAlertKnobs = false, bool hasAgLatencyColumns = false, bool hasAgDisconnectRefire = false, bool hasPayloadDimensions = false, bool hasDimFloorIndexes = false, bool hasBlockingWaitThreshold = false, bool hasQueryStoreIntervalIdentity = false, bool hasPagerDutyWebhook = false, bool hasPagerDutyProxy = false, bool hasCollectorState = false, bool hasPlanCorrection = false, bool hasPvsStats = false, bool hasPvsPressureKnobs = false, bool hasDatabaseStateAlert = false, bool hasServerTagColour = false, bool hasQueryStatsHostObject = false, bool hasFindingDrillDown = false, bool hasStoreMetrics = false, bool hasPlanDimGzip = false, bool hasSelfAlertKnobs = false, bool hasJobMetricsColumns = false, bool hasJobCadenceKnob = false, bool hasBackfillSwitch = false, bool hasCollectorMemoryKnobs = false, bool hasDatabaseStateEdgeMemory = false, bool hasIncidentOccurrences = false, bool hasPlanXmlCompressionKnob = false, bool hasMonitoredServerEngine = false, bool hasPgBlockingEdges = false, bool hasQueryStorePlanMap = false, bool hasPgStatementText = false, bool hasQueryStoreText = false, bool hasPlanContentRetentionKnob = false, bool hasQueryStoreHealth = false, bool hasQueryStoreTextHash = false, bool hasComposeTimeoutKnob = false, bool hasFileGrowthAlert = false, bool hasCollectionLogFanoutRollup = false, bool hasTempDbMaxSize = false, bool hasServerEngineKind = false, bool hasPgDatabaseStats = false, bool hasPgIndexUsageStats = false, bool hasPgTableBloatStats = false, bool hasPgSessionStates = false, bool hasPgPlanCaptureReadiness = false, bool hasPgWriteStats = false, bool hasPgExtensionAvailability = false, bool hasPgLockStats = false, bool hasPgColumnStats = false, bool hasPgReplicationStats = false, bool hasPgBufferUsage = false, bool hasPgIndexBloat = false, bool hasPgPerDatabaseAttribution = false, bool hasPgWaitSampling = false, bool hasPgKernelStats = false, bool hasPgPredicateStats = false, bool hasPgPlanCapture = false, bool hasPgMajorVersion = false, bool hasPg18IoBytes = false, bool hasPgServerConfig = false, bool hasPgDeadlocks = false, bool hasPgDeadlockIdentity = false, bool hasCollectorCost = false, bool hasPgCpuUtilization = false, bool hasPlanForceActions = false, bool hasCollectionLogPhaseSplit = false, bool hasCollectionLogDrainForensics = false, bool hasCollectionLogFetchPhaseSums = false, bool hasStoreLogSelfMonitoring = false, bool hasCollectorStallProbes = false, bool hasRemediationCredentialAndActor = false, bool hasPgIndexBloatEstimate = false, bool hasPgCpuCapacityHeadroom = false, bool hasCustomAlertCore = false, bool hasMuteRuleReloadBeacon = false, bool hasBuiltinAlertPersistence = false, bool hasRetentionHoldRatioKnobs = false, bool hasDeadlockRateBandKnobs = false, bool hasOversizedPlanBacklog = false, bool hasPgAlertCountKnobs = false, bool hasFleetSweepState = false, bool hasFleetSweepCadenceKnobs = false, bool hasCollectorScheduleDatabases = false, bool hasSelfDiskWarnGbFloor = false, bool hasDeltaFamilyIntervalColumns = false, bool hasDeltaFamilyIntervalCompletion = false, bool hasPgLogEvents = false, bool hasPgLogEventMetrics = false, bool hasNotificationRoutes = false, bool hasPerfmonCounterType = false, bool hasPgNumbackendsAndSampledMs = false, bool hasTimeHonesty = false, bool hasLrqExclusionKnob = false, bool hasPgDatabaseSizeStatsAndHostMemory = false, bool hasQsCaptureModeRouteKnobToast = false, bool hasPgServerConfigDatabaseRoleOverrides = false, bool hasPostmasterStartTime = false, bool hasCheckpointsTimed = false, bool hasCollectionCaveats = false, bool hasIndexObjectStatsServerTimeIndex = false, bool hasQueryStoreIntervalLatest = false, bool hasRawChunkIntervalRungHistory = false, bool hasQueryStoreIntervalWide = false, bool hasManagedConfVerdicts = false, bool hasComposeTimeoutSixty = false, bool hasReadLatency = false, bool hasHotLivenessTouch = false, bool hasCollectionLogWatermarkAndJobHistoryIndexes = false, bool hasAgGroupId = false, bool hasCaggGroupIndexDrop = false, bool hasIntervalFirstExecIndexes = false, bool hasIntervalWideFirstExecIndex = false, bool hasQueryStoreIntervalEnd = false, bool hasCheckpointLongestSync = false, bool hasMuteRuleServerId = false, bool hasInstallId = false, bool hasInstallIdTableOid = false, bool hasCollectorRunAt = false, bool hasQueryStoreTopDaily = false, bool hasSlowReads = false, bool hasStoreStatementHistory = false, bool hasQueryStatsHourLedger = false, bool hasPasswordKey = false, bool hasPagerDutyAutoResolve = false, bool hasLegacyPinCandidates = false, bool hasPlanRegressionDaily = false, bool hasAwsRole = false, bool hasPgIoStatsHourly = false, bool hasQueryStoreIntervalWidePartitioned = false, bool hasQueryStoreIntervalLatestPartitioned = false, bool hasQueryStoreComposeStamp = false, bool hasServiceVersion = false, bool hasPlanRegressionDailyRowcount = false, bool hasProcedureStatsTotals = false, bool hasRunnableQueueSettings = false, bool hasQueryStoreWatermarks = false, bool hasClientTimeoutSettings = false, bool hasExternalAlerts = false, bool hasNotificationRouteScope = false, bool hasReadLatencyCapacityFields = false, bool hasReadCaller = false, bool hasInvestigations = false, bool hasPlanRegressionPartialDays = false, bool hasCollectionLogHourly = false, bool hasAlertResolutionDelivery = false, bool hasInvestigationAlertFacts = false, bool hasComposeStampFailures = false, bool hasStoreMetricsSizeEstimated = false, bool hasAgentLevel2Store = false, bool hasQueryTagsStore = false, bool hasPhaseCStore = false, bool hasQueryTagSightingsDatabaseKey = false, bool hasQueryStoreForcedPlans = false, bool hasServerActionSwitches = false, bool hasForceChecks = false, bool hasPseudonymKey = false, bool hasBlockingHeadEvidence = false, bool hasExcludedVolumes = false, bool hasForcedPlanVariantsAndGuides = false, bool hasAgPrimaryReplica = false, bool hasHypotheticalIndexLogin = false, bool hasCustomViewFolders = false, bool hasCustomAlertRuleExpiry = false, bool hasInheritableCollectorEnabled = false, bool hasChangeWatches = false, bool hasMuteRuleStartsAt = false, bool hasQueryStoreFetchTopN = false, bool hasAgentRouteTagGuard = false, bool hasClientTimeoutsMultiples = false, bool hasServerMachineIdentity = false, bool hasQueryStoreReplica = false, bool hasKnownPatterns = false, bool hasAgentTableIndexes = false, bool hasClientTimeoutsOnByDefault = false, bool hasQueryStoreHints = false, bool hasServerActionRecompile = false, bool hasQueryStoreSelfKeys = false, bool hasJobHistorySuccessIndex = false, bool hasQueryStoreHashHourly = false, bool hasAgentAlerts = false, bool hasDatabaseProtection = false, bool hasIdentityCapacity = false, bool hasServerAwsRds = false, bool hasServerAwsRdsWebOrExpress = false, bool hasThresholdOverrideTagScope = false, bool hasBlockingBands = false, bool hasRouteSeverities = false, bool hasBlockingSnapshotBands = false, bool hasMuteRecurrence = false, bool hasDailyReportSettings = false, bool hasReportNotice = false, bool hasBlockingBarsOnWaitTime = false, bool hasMuteRuleAction = false)
    {
        /* V71 (the PostgreSQL blocking-edges rung): a table-existence sentinel and now the newest-first arm.
           A collector table would ordinarily get no arm at all — see the V63-V69 note below — but the TOP
           rung always needs one, whatever it happens to be, because RequiredStoreSchemaVersion is
           StorageVersion.SchemaVersion and a fully-migrated store must map to EXACTLY that or the version
           banner reports a mismatch on a store that is current. Being a collector table makes it no less
           reliable as a sentinel; it simply is not interesting for any other reason. */
        /* V72 (the Query Store plan map, #2210): table-existence sentinel, and it has to sit ABOVE the V71
           arm because these are evaluated newest-first — a V72 store also has V71's table, so testing V71
           first would report every V72 store as V71 and the viewer would show a spurious upgrade banner
           against a store that is actually current.

           The gate matters rather than being bookkeeping: the SERVICE writes this map on every plan fetch and
           resolves plan XML through it, so a viewer pointed at a V71 store while the service expects V72 would
           find no map rows and render every plan as "not yet collected" — indistinguishable from a healthy
           store that simply has not fetched yet, which is the one failure mode this design works hardest to
           avoid being silent about.

           The table is named in the probe line above and deliberately NOT in this prose, per the V71 arm's
           finding: the coverage ratchet strips information_schema lines but cannot strip a comment, so a
           prose mention would exempt the table from it. */
        /* #2150: newest first, and the arm below STAYS for the same reason every previous one did — a store
           migrated to exactly 73 must map to 73 rather than falling through. The table is named only in the
           probe line, not in this prose, per the V71 finding: the coverage ratchet strips
           information_schema lines but cannot strip a comment, so a prose mention would exempt the table
           from it. */
        /* #2316: newest first. The arm below STAYS — a store migrated to exactly 74 must map to 74
           rather than falling through. The column is named only in the probe line, not in this prose,
           per the V71 finding: the coverage ratchet strips information_schema lines but cannot strip a
           comment, so a prose mention would exempt it. */
        /* #2319: newest first. The arm below STAYS — a store migrated to exactly 75 must map to 75
           rather than falling through. The table is named only in the probe line, not in this prose,
           per the V71 finding: the coverage ratchet strips information_schema lines but cannot strip
           a comment, so a prose mention would exempt it. */
        /* #2312: newest first. The arm below STAYS — a store migrated to exactly 76 must map to 76
           rather than falling through. The column is named only in the probe line, not in this prose,
           per the V71 finding: the coverage ratchet strips information_schema lines but cannot strip
           a comment, so a prose mention would exempt it. */
        /* #2357: newest first. The arm below STAYS — a store migrated to exactly 77 must map to 77 rather
           than falling through. The column is named only in the probe line, not in this prose, per the V71
           finding: the coverage ratchet strips information_schema lines but cannot strip a comment, so a
           prose mention would exempt it. */
        /* #2349: newest first. The arm below STAYS — a store migrated to exactly 78 must map to 78 rather
           than falling through. The column is named only in the probe line, not in this prose, per the V71
           finding: the coverage ratchet strips information_schema lines but cannot strip a comment. */
        /* #2472: newest first. The arm below STAYS — a store migrated to exactly 79 must map to 79 rather
           than falling through. The column is named only in the probe line, not in this prose, per the V71
           finding: the coverage ratchet strips information_schema lines but cannot strip a comment.

           This rung's gate earns its place rather than being bookkeeping. The service writes the fan-out
           rollup on every productive per-database run and Collection Health reads it; a viewer on a V79
           store would find the column missing and the read would throw rather than degrade, so the banner
           has to fire before the tab does. */
        /* #2515: newest first. The arm below STAYS — a store migrated to exactly 80 must map to 80 rather
           than falling through. The column is named only in the probe line, not in this prose, per the V71
           finding: the coverage ratchet strips information_schema lines but cannot strip a comment.

           The reason to gate is the standing invariant rather than a read that would throw — no viewer query
           names this column; the SERVICE's alert adapter is what reads it. RequiredStoreSchemaVersion is
           StorageVersion.SchemaVersion, so a fully-migrated store has to map to EXACTLY this or the banner
           reports a mismatch on a store that is current. What the banner buys on the way is worth having: a
           store still at 80 has no ceiling recorded anywhere in its history, so every tempdb percentage in it
           is the old distance-to-next-autogrow measurement, and the operator should know that before reading
           one. */
        /* #2540: newest first, and the arms below STAY — a store migrated to exactly 85 must map to 85
           rather than falling through. The table is named only in the probe line, not in this prose, per
           the V71 finding: the coverage ratchet strips information_schema lines but cannot strip a comment.

           A collector-table rung would ordinarily get no arm at all — see the V63-V69 note below — and this
           one is here for the standing invariant: it is the TOP rung, RequiredStoreSchemaVersion is
           StorageVersion.SchemaVersion, and a fully-migrated store must map to EXACTLY that or the version
           banner reports a mismatch against a store that is current. Nothing in the viewer reads the table;
           the read that does is on the MCP surface.

           The three comment blocks that used to stack here were moved down onto the arms they describe
           (#2530 → V82, #2539 → V83, #2542 → V85). They had drifted upward as each new rung inserted its
           own block above them, which is how a comment ends up explaining an arm two rungs away from the
           one it was written for. Keep the block with its arm. */
        /* V109 (#2864): collect.collection_log gains drain_rows_read / drain_bytes_read /
           drain_last_read_ms / target_session_id / sweep_peer_max_ms, so an abandoned cycle records what it
           was DOING rather than only that it stopped. COLUMN-existence sentinel on drain_last_read_ms
           specifically - the table has existed since V1, only the columns are new, and that column is the
           one the rung exists to expose (subtracted from sql_drain_ms it gives the time the reader spent
           with nothing arriving, which is what separates a slow stream from a stalled one). The TOP rung
           now, so it must map EXACTLY or the connect-time gate refuses a store that is perfectly current.
           Nothing in the viewer reads these columns yet - the read is on the MCP surface - so this arm is
           the don't-under-report guard, the V44/V53 reasoning. */
        /* V110 (#2860): collect.collection_log gains the per-database plan/text fetch split summed across a
           run's fan-out - plan_fetch_probe_ms / _target_ms / _write_ms / _ids_attempted / _probe_ids and the
           five text twins - so #2811's sub-split can be aggregated and trended instead of living only in an
           app-log line scraped per server over SSM. COLUMN-existence sentinel on plan_fetch_target_ms: the
           table has existed since V1 so only columns can separate the rungs, and TARGET is the one the
           choice of shape turned on. A slowest-database rollup would have named the right winning phase
           92.8% of the time, but its residual error ran ~6.5 : 1 toward reading a store probe as a target
           cost - the precise misreading this instrumentation family exists to end - so the sums are stored
           and this column is where that bias would have shown. The TOP rung now, so it must map EXACTLY or
           the connect-time gate refuses a store that is perfectly current. Nothing in the viewer reads these
           columns - the read is on the MCP surface - so this arm is the don't-under-report guard, the
           V44/V53 reasoning that V108 and V109 both restated. */
        /* V111 (#3021): the store's own server log becomes a self-monitoring source - a per-class census
           (store_log_events), the capture denominator that qualifies it (store_log_captures), and the
           per-file resume marker (config.store_log_read_marker). TABLE-existence sentinel on the census
           table: all three objects are new at this rung, and the census is the one every read of this
           feature goes through, so its absence is the honest "this store predates the rung". The TOP rung
           now, so it must map EXACTLY or the connect-time gate refuses a store that is perfectly current.
           Nothing in the viewer reads these tables - the read is on the MCP surface (get_store_log) - so
           this arm is the don't-under-report guard, the V44/V53 reasoning that V108, V109 and V110 all
           restated. */
        /* V112 (#2880): the out-of-band server-wide wait sample taken during a collector stall lands in its
           own table, because a probe folded onto the stalled run's log row would have to be awaited by that
           run. TABLE-existence sentinel — the table is the only object the rung creates, so its absence is
           the honest "this store predates the rung". The TOP rung now, so it must map EXACTLY or the
           connect-time gate refuses a store that is perfectly current. Nothing in the viewer reads it - the
           read is on the MCP surface - so this arm is the don't-under-report guard, the V44/V53 reasoning
           that V108 through V111 all restated. The table is named only in the probe line above and NOT in
           this prose, per the V71 finding: the coverage ratchet strips information_schema lines but cannot
           strip a comment. */
        /* V113 (#2138 phase 1): config.config_monitored_servers gains the per-server remediation
           credential and collect.plan_force_actions gains the journal's actor. COLUMN-existence sentinel on
           the actor column: both objects the rung touches already exist (the registry since V17, the
           journal since V107), so table existence cannot separate the rungs, and the actor is the one the
           own-forces-only invariant turns on - a store without it cannot tell an operator's force from the
           bot's, which is the property the rung exists to establish. Deliberately not a credential column:
           those are the secret and non-secret halves of one optional feature, and a probe line naming one
           reads as though the viewer needed to see it. The TOP rung now, so it must map EXACTLY or the
           connect-time gate refuses a store that is perfectly current. Nothing in the viewer reads the
           actor column yet - the own-forces-only filter is service-side - so this arm is the
           don't-under-report guard, the V44/V53 reasoning that V108 through V112 all restated. */
        /* V115 (#3281): collect.pg_cpu_utilization gains the Aurora Serverless v2 capacity-headroom
           columns, because the CPU column it already had is percent of the capacity CURRENTLY ALLOCATED
           and so reads 100% on a routine scale-up. COLUMN-existence sentinel — the table has existed since
           V106, so table existence cannot separate the rungs. The TOP rung now, so it must map EXACTLY or
           the connect-time gate refuses a store that is perfectly current. The column is named only in the
           probe line above and NOT in this prose, per the V71 finding: the coverage ratchet strips
           information_schema lines but cannot strip a comment. */
        /* V116 (#3285): config.custom_alert_rules — the user-authored custom-alert rules. Table-existence
           sentinel, newest-first, and now the TOP rung, so a fully-migrated store maps to EXACTLY
           StorageVersion.SchemaVersion (116) rather than falling through to 115 and showing a spurious upgrade
           banner on a store that is current. The table is named only in the probe line, not this prose, per the
           V71 finding (the coverage ratchet strips information_schema lines but cannot strip a comment). */
        /* V128 (#3540): the completion of V127 — the measured sample interval on the four delta families
           V127 left naked, and the two statement offsets on the query table that its delta key is made of.
           After this rung every delta family stores the interval, so a restart's fabricated (0, 0) row can
           be told from a genuinely idle one at EVERY read, and the restart seed can rebuild the one key
           the store could not reproduce. COLUMN-existence sentinel (the five tables have existed since
           V4/V10 and V63/V64, so table existence cannot separate the rungs), newest-first, one rung
           behind the top since V129 landed — a store carrying this and not V129 above maps to 128, which
           is the honest answer for it and also what makes the upgrade banner correct in both directions.

           The gate earns its place beyond that standing invariant: the viewer's procedure duration trend
           and procedure history reads now name the column (the stored interval is preferred over the LAG
           derivation), so a viewer pointed below this rung would throw a raw 42703 on the Performance
           Trends and Top Procedures surfaces — the banner has to fire before those do. The column and its
           tables are named only in the probe line, not this prose, per the V71 finding: the coverage
           ratchet strips information_schema lines but cannot strip a comment. */
        /* V146 (#4215): the per-key verdict a managed store's owner connection computes once at every
           service-owned start, one new table. TABLE-existence sentinel, newest-first, and now the TOP rung,
           so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling
           through to the rung below and showing a spurious upgrade banner on a store that is current.

           The WPF viewer runs no analysis, so no viewer read names the new table; this arm exists so the version
           banner stays truthful, which is the only effect the rung has on the viewer. Named only in the probe
           line, not this prose, per the V71 finding: the coverage ratchet strips information_schema lines
           but cannot strip a comment. */
        /* V148 (#4442 scope 2): the hourly read-latency histogram table, and now the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to
           the rung below and showing a spurious upgrade banner on a store that is current.

           The WPF viewer runs no analysis, so no viewer read names the new table; this arm exists so the
           version banner stays truthful, which is the only effect the rung has on the viewer. Named only
           in the probe line, not this prose, per the V71 finding. */
        /* V152 (#4503): the six Query Store rollups' auto-created two-key group index is now ABSENT, and
           now the TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather
           than falling through to the rung below and showing a spurious upgrade banner on a store that is
           current.

           The WPF viewer runs no analysis, so no viewer read names any of the six rollups or their
           indexes; this arm exists so the version banner stays truthful, which is the only effect the rung
           has on the viewer. Named only in the probe line, not this prose, per the V71 finding. */
        /* V172 (#5571): the latest Query Store interval table is partitioned by day. RELKIND sentinel (the table has existed since V143,
           so existence cannot separate the rungs), newest-first, and the TOP rung, so a fully-migrated store maps to EXACTLY
           StorageVersion.SchemaVersion rather than falling through to 171 and showing a spurious upgrade banner on a store that is
           current. Named only in the probe line, not this prose, per the V71 finding. */
        /* V173 (#5582): the exact stamp-grain rollup of the wide Query Store interval table. TABLE-existence sentinel (the hours table),
           newest-first, and the TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling
           through to 172 and showing a spurious upgrade banner on a store that is current. The WPF viewer runs no compose read, so no
           viewer read names the new tables; this arm exists so the version banner stays truthful. Named only in the probe line, not this
           prose, per the V71 finding. */
        /* V174 (#5655): the table in which each service host records its product version. TABLE-existence sentinel, newest-first,
           and the TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to
           173 and showing a spurious upgrade banner on a store that is current. Named only in the probe line, not this prose, per
           the V71 finding. */
        /* V181 (#5750): the scope columns on the notification routes. Column-existence sentinel, newest-first, and the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 180 and showing a spurious
           upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        /* V182 (#5506): the window band, in-flight and connection-wait columns on the read-latency table. Column-existence sentinel,
           newest-first, and the TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling
           through to 181 and showing a spurious upgrade banner on a store that is current. Named only in the probe line, not this
           prose, per the V71 finding. */
        /* V224 (#6875): the identity capacity table. Table-existence sentinel, newest-first, and the TOP rung, so a fully-migrated store maps
           to EXACTLY StorageVersion.SchemaVersion rather than falling through to 223 and showing a spurious upgrade banner on a store that is
           current. Named only in the probe line, not this prose, per the V71 finding. */
        /* V225 (#6894): the AWS RDS fact on the server registry. Column-existence sentinel, newest-first, and the TOP rung, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 224 and showing a spurious upgrade banner on a
           store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        /* V226 (#6893): the AWS RDS Web or Express fact on the server registry. Column-existence sentinel, newest-first, and the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 225 and showing a spurious upgrade
           banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        /* V235 (#7083): the blocking count bar's column default is 3. Column-default sentinel, newest-first, and the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 233 and showing a spurious upgrade
           banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        /* V236 (#7083): the action on the mute rules. Column-existence sentinel, newest-first, and the TOP rung, so a fully-migrated store maps
           to EXACTLY StorageVersion.SchemaVersion rather than falling through to 235 and showing a spurious upgrade banner on a store that is
           current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasMuteRuleAction)
        {
            return 236;
        }
        if (hasBlockingBarsOnWaitTime)
        {
            return 235;
        }
        /* V233 (#6979): the report notice setting on the notification routes. Column-existence sentinel, newest-first, and the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 232 and showing a spurious upgrade
           banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasReportNotice)
        {
            return 233;
        }
        /* V232 (#7041): the Daily Health Report columns on the alert settings row. Column-existence sentinel, newest-first, and the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 231 and showing a spurious upgrade
           banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasDailyReportSettings)
        {
            return 232;
        }
        /* V231 (#7040): the recurring schedule on the mute rules. Column-existence sentinel, newest-first, and the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through and showing a spurious upgrade
           banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasMuteRecurrence)
        {
            return 231;
        }
        /* V229 (#6928): the severity filter on the notification routes. Column-existence sentinel, newest-first, and the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 226 and showing a spurious upgrade
           banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        /* V230 (#7028): the snapshot wait tiers of the blocking band on the alert settings row. Column-existence sentinel, newest-first, and the TOP
           rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 229 and showing a spurious
           upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasBlockingSnapshotBands)
        {
            return 230;
        }
        if (hasRouteSeverities)
        {
            return 229;
        }
        /* V228 (#6927): the settable blocking band columns on the alert settings row. Column-existence sentinel, newest-first, and the TOP
           rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 227 and showing a
           spurious upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasBlockingBands)
        {
            return 228;
        }
        /* V227 (#6926): the tag scope on the threshold overrides. Catalog sentinel, one rung under V228. */
        if (hasThresholdOverrideTagScope)
        {
            return 227;
        }
        if (hasServerAwsRdsWebOrExpress)
        {
            return 226;
        }
        if (hasServerAwsRds)
        {
            return 225;
        }
        if (hasIdentityCapacity)
        {
            return 224;
        }
        /* V223 (#6873): the database protection table. Table-existence sentinel, newest-first, and a rung below the top, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 222 and showing a spurious upgrade banner on a
           store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasDatabaseProtection)
        {
            return 223;
        }
        /* V222 (#6874): the Agent alerts table. Table-existence sentinel, newest-first, and a rung below the top, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 221 and showing a spurious upgrade banner on a
           store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasAgentAlerts)
        {
            return 222;
        }
        /* V221 (#6800): the Query Store hash-hourly table. Table-existence sentinel, newest-first, and a rung below the top, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 220 and showing a spurious upgrade banner on a
           store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasQueryStoreHashHourly)
        {
            return 221;
        }
        /* V220 (#6798): the Job History statistics index. Index-existence sentinel, newest-first, and a rung below the top, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 219 and showing a spurious upgrade banner on a
           store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasJobHistorySuccessIndex)
        {
            return 220;
        }
        /* V219 (#6750): the Query Store self-statement key tables. Table-existence sentinel, newest-first, and a rung below the top, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion. Named only in the probe line, not this prose, per the V71
           finding. */
        if (hasQueryStoreSelfKeys)
        {
            return 219;
        }
        /* V218 (#6569): the module recompile switch. CHECK-definition sentinel, newest-first, and a rung below the top, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasServerActionRecompile)
        {
            return 218;
        }
        /* V217 (#6581): the Query Store hints table. Table-existence sentinel, newest-first, and a rung below the top, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasQueryStoreHints)
        {
            return 217;
        }
        /* V216 (#5736): the Client Timeouts switch's column default is TRUE. Column-default sentinel, newest-first, and a rung below the top, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 215 and showing a spurious
           upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasClientTimeoutsOnByDefault)
        {
            return 216;
        }
        /* V215: the agent-table indexes. Index-existence sentinel, newest-first, and a rung below the top, so a fully-migrated store maps to
           EXACTLY StorageVersion.SchemaVersion rather than falling through to 214 and showing a spurious upgrade banner on a store that
           is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasAgentTableIndexes)
        {
            return 215;
        }
        /* V214 (#6334): the known-pattern entries. Table-existence sentinel, newest-first, and a rung below the top, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 213 and showing a spurious upgrade banner
           on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasKnownPatterns)
        {
            return 214;
        }
        /* V213 (#6254): the Query Store replica setting. Column-existence sentinel, newest-first, and a rung below the top, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 212 and showing a spurious upgrade banner
           on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasQueryStoreReplica)
        {
            return 213;
        }
        /* V212 (#6285): the machine identity on the server registry. Column-existence sentinel, newest-first, and a rung below the top, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 211 and showing a spurious upgrade banner
           on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasServerMachineIdentity)
        {
            return 212;
        }
        /* V211 (#5736): the relative Client Timeouts bars' multiples. Column-existence sentinel, newest-first, and a rung below the top, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 210 and showing a spurious
           upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasClientTimeoutsMultiples)
        {
            return 211;
        }
        /* V210 (#6281): the guard on the MCP tag writes. Trigger-existence sentinel, newest-first, and the TOP rung, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 209 and showing a spurious upgrade banner
           on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasAgentRouteTagGuard)
        {
            return 210;
        }
        /* V209 (#6211): the Query Store fetch top-N setting. Column-existence sentinel, newest-first, and the TOP rung, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 208 and showing a spurious upgrade banner
           on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasQueryStoreFetchTopN)
        {
            return 209;
        }
        /* V208 (#6231): the start time on the mute rules. Column-existence sentinel, newest-first, and the TOP rung, so a fully-migrated
           store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 207 and showing a spurious upgrade banner
           on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasMuteRuleStartsAt)
        {
            return 208;
        }
        /* V207 (#6234): the change watch tables. Newest-first, and the TOP rung, so a fully-migrated store maps to EXACTLY
           StorageVersion.SchemaVersion rather than falling through to 204 and showing a spurious upgrade banner on a store that is current.
           Named only in the probe line, not this prose, per the V71 finding. */
        if (hasChangeWatches)
        {
            return 207;
        }
        /* V203 (#6170): the hypothetical-index login columns on the server registry. Column-existence sentinel, newest-first, and the TOP
           rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 201 and showing a
           spurious upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        /* V206 (#6227): the collector schedule rows may hold a NULL enabled state. Column-nullability sentinel, newest-first, and the TOP
           rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 205 and showing a
           spurious upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasInheritableCollectorEnabled)
        {
            return 206;
        }
        /* V205 (#6235): the optional expiry on the custom alert rules. Column-existence sentinel, newest-first, and the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 203 and showing a spurious
           upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasCustomAlertRuleExpiry)
        {
            return 205;
        }
        /* V204 (#6214): the folder, personal flag and creator on the custom views. Column-existence sentinel, newest-first, and the TOP
           rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 203 and showing a
           spurious upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasCustomViewFolders)
        {
            return 204;
        }
        if (hasHypotheticalIndexLogin)
        {
            return 203;
        }
        /* V202 (#6159): the primary replica's name on the AG replica states. Column-existence sentinel, newest-first, and the TOP rung,
           so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 201 and showing a spurious
           upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasAgPrimaryReplica)
        {
            return 202;
        }
        /* V201 (#6160): the plan-guide columns on the forced-plan capture. Column-existence sentinel, newest-first, so a store at 201
           maps to EXACTLY 201 rather than falling through to 200. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasForcedPlanVariantsAndGuides)
        {
            return 201;
        }
        /* V200 (#6098): the excluded-volumes column on the alert settings row. Column-existence sentinel, newest-first, so a store at 200
           maps to EXACTLY 200 rather than falling through to 199. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasExcludedVolumes)
        {
            return 200;
        }
        /* V199 (#5632): the head-blocker evidence columns on the blocking snapshots. Column-existence sentinel, newest-first, and the TOP rung,
           so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 198 and showing a spurious
           upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasBlockingHeadEvidence)
        {
            return 199;
        }
        /* V198 (#6066): the pseudonym key table. Table-existence sentinel, newest-first, and the TOP rung, so a fully-migrated store maps to
           EXACTLY StorageVersion.SchemaVersion rather than falling through to 197 and showing a spurious upgrade banner on a store that is
           current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasPseudonymKey)
        {
            return 198;
        }
        /* V197 (#5632): the force-check link columns. Column-existence sentinel, newest-first, so a store at 197 maps to EXACTLY 197 rather
           than falling through to 196. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasForceChecks)
        {
            return 197;
        }
        /* V196 (#5632): the per-server action switch table. Table-existence sentinel, newest-first, so a store at 196 maps to EXACTLY 196
           rather than falling through to 195. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasServerActionSwitches)
        {
            return 196;
        }
        /* V195 (#5632): the forced-plans collector table. Table-existence sentinel, newest-first, so a store at 195 maps to EXACTLY 195
           rather than falling through to 194. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasQueryStoreForcedPlans)
        {
            return 195;
        }
        /* V194 (#5985): the query-tag sightings key gains the database. Index-existence sentinel, newest-first, so a store at 194 maps to
           EXACTLY 194 rather than falling through to 193. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasQueryTagSightingsDatabaseKey)
        {
            return 194;
        }
        /* V193 (#5997): the Phase C store (investigation evidence, follow-ups, signature days, signing keys). Table-existence
           sentinel, newest-first, so a store at 193 maps to EXACTLY 193 rather than falling through to 192 and showing a
           spurious upgrade banner. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasPhaseCStore)
        {
            return 193;
        }
        /* V192 (#5977): the query-tag store. Table-existence sentinel, newest-first, so a store at 192 maps to EXACTLY 192 rather
           than falling through to 191 and showing a spurious upgrade banner. Named only in the probe line, not this prose, per the
           V71 finding. */
        if (hasQueryTagsStore)
        {
            return 192;
        }
        /* V191 (#5909): the Level 2 store (the feedback table, the threshold overrides, the agent-actions switch). Table-existence
           sentinel, newest-first, so a fully-migrated store at 191 maps to EXACTLY 191 rather than falling through to 190 and
           showing a spurious upgrade banner. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasAgentLevel2Store)
        {
            return 191;
        }
        /* V190 (#5857): the size-estimate marker on the store's own size series. Column-existence sentinel, newest-first, and the
           TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 188 and
           showing a spurious upgrade banner on a store that is current. Named only in the probe line, not this prose, per the
           V71 finding. */
        if (hasStoreMetricsSizeEstimated)
        {
            return 190;
        }
        /* V189 (#5867): the failure record of the Query Store compose builder. Table-existence sentinel, newest-first, and the TOP
           rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 188 and
           showing a spurious upgrade banner, or the connect-time gate refusing a store that is current. Named only in the probe
           line, not this prose, per the V71 finding. */
        if (hasComposeStampFailures)
        {
            return 189;
        }
        /* V188 (#5821): the alert values on the investigation link rows. Column-existence sentinel, newest-first, and the TOP
           rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 186 and
           showing a spurious upgrade banner on a store that is current. Named only in the probe line, not this prose, per the
           V71 finding. */
        if (hasInvestigationAlertFacts)
        {
            return 188;
        }
        /* V187 (#5825): the alert-resolution delivery columns. Column-existence sentinel, newest-first, and the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 186 and showing a
           spurious upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71
           finding. */
        if (hasAlertResolutionDelivery)
        {
            return 187;
        }
        /* V186 (#5810): the hourly rollup of the collection log. Table-existence sentinel, newest-first, and the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 185 and showing a
           spurious upgrade banner, or the connect-time gate refusing a store that is current. Named only in the probe line, not
           this prose, per the V71 finding. */
        if (hasCollectionLogHourly)
        {
            return 186;
        }
        /* V185 (#5812): the partial-day watermark on the plan regression built table. Column-existence sentinel, newest-first,
           and the TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to
           184 and showing a spurious upgrade banner on a store that is current. Named only in the probe line, not this prose,
           per the V71 finding. */
        if (hasPlanRegressionPartialDays)
        {
            return 185;
        }
        /* V184 (#5632): the investigation tables and the agent route columns. Table-existence sentinel, newest-first, and
           the TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to
           183 and showing a spurious upgrade banner on a store that is current. Named only in the probe line, not this prose,
           per the V71 finding. */
        if (hasInvestigations)
        {
            return 184;
        }
        /* V183 (#5793): the read caller columns on the slow-read and read-latency tables. Column-existence sentinel,
           newest-first, and the TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than
           falling through to 182 and showing a spurious upgrade banner on a store that is current. Named only in the probe
           line, not this prose, per the V71 finding. */
        if (hasReadCaller)
        {
            return 183;
        }
        if (hasReadLatencyCapacityFields)
        {
            return 182;
        }
        if (hasNotificationRouteScope)
        {
            return 181;
        }
        /* V180 (#5654): the registry of the services that raise alerts through this store. TABLE-existence sentinel, newest-first,
           and the TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to
           179 and showing a spurious upgrade banner on a store that is current. Named only in the probe line, not this prose, per
           the V71 finding. */
        if (hasExternalAlerts)
        {
            return 180;
        }
        /* V179 (#5640): the Client Timeouts alert's settings columns on the alert settings row. Column-existence sentinel, newest-first,
           and the TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 177.
           (V178, #5684, the watermark table, sits just below this arm; V177, #5641, below that.) Named only in the probe
           line, not this prose, per the V71 finding. */
        if (hasClientTimeoutSettings)
        {
            return 179;
        }
        /* V178 (#5684): the per-batch Query Store watermark table. Table-existence sentinel, newest-first; the V179 arm (#5640)
           now sits above it as the top rung, and this one is the rung a store at 178 maps to rather than falling through to 177.
           Named only in the probe line, not this prose, per the V71 finding. */
        if (hasQueryStoreWatermarks)
        {
            return 178;
        }
        /* V177 (#5641): the Runnable Queue alert's settings columns on the alert settings row. Column-existence sentinel, newest-first.
           The V178 arm (#5684) and the V179 arm (#5640) now sit above it, V179 as the top rung; this one is the rung a store at 177 maps to rather than falling
           through to 176. (V176, #5638, sits just below this arm.) Named only in the probe line, not this prose, per the V71 finding. */
        if (hasRunnableQueueSettings)
        {
            return 177;
        }
        /* V176 (#5638): the per-collection procedure totals table. Table-existence sentinel, newest-first; a store
           at 176 maps here rather than falling through to 175 and showing a spurious
           upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasProcedureStatsTotals)
        {
            return 176;
        }
        /* V175 (#5631): the rows-per-call column of the daily plan regression totals. Column-existence sentinel, newest-first. Named
           only in the probe line, not this prose, per the V71 finding. */
        if (hasPlanRegressionDailyRowcount)
        {
            return 175;
        }
        if (hasServiceVersion)
        {
            return 174;
        }
        if (hasQueryStoreComposeStamp)
        {
            return 173;
        }
        if (hasQueryStoreIntervalLatestPartitioned)
        {
            return 172;
        }
        /* V171 (#5571): the wide Query Store interval table is partitioned by day. RELKIND sentinel, as V172's. */
        if (hasQueryStoreIntervalWidePartitioned)
        {
            return 171;
        }
        /* V170 (#5495): the hourly rollup of the differenced PostgreSQL I/O counters. TABLE-existence sentinel (the state table), newest-first,
           and the TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 169
           and showing a spurious upgrade banner on a store that is current. The viewer's own I/O read uses the rollup only after its
           count guard passes and otherwise reads raw rows. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasPgIoStatsHourly)
        {
            return 170;
        }
        /* V169: the per-server AWS role on config_monitored_servers: which IAM role Darling assumes to reach an
           RDS or Aurora target, with an optional external ID. COLUMN-existence sentinel (the table has existed
           since V2, so table existence cannot separate the rungs), newest-first, and the TOP rung, so a
           fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than falling through to 168
           and showing a spurious upgrade banner on a store that is current. The column is named only in the
           probe line, not this prose, per the V71 finding. */
        if (hasAwsRole)
        {
            return 169;
        }

        /* V168 (#5448): the per-day plan totals for PLAN_REGRESSION's closed days, and no longer the top rung now
           that V169 has landed above it. The viewer reads nothing from the tables yet, so this arm exists so that a store
           that stopped here does not fall through to the rung below and show a spurious upgrade banner. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasPlanRegressionDaily)
        {
            return 168;
        }

        /* V166: the PagerDuty auto-resolve opt-in on config_notification — when set, the closing edge of an
           edge-type alert pair (a connection recovery, a replica reconnect) is delivered as a PagerDuty
           resolve on the SAME dedup_key the firing edge's trigger opened, closing the incident; unset (the
           default) the closing edge is an info-severity trigger and the incident stays open, so the tool
           never auto-resolves a third-party incident unasked. COLUMN-existence sentinel (the table has
           existed since V17, so table existence cannot separate the rungs), newest-first, and no longer the
           top rung now that V167 has landed above it; it keeps its own arm so a store that stopped here
           does not fall through to 165 and show a spurious upgrade banner.

           The gate earns its place beyond the standing invariant: the service's notification read now names
           the column, so a viewer pointed below this rung would throw a raw 42703 when the Settings window
           prefills the PagerDuty section. The column is named only in the probe line, not this prose, per
           the V71 finding: the coverage ratchet strips information_schema lines but cannot strip a comment. */
        /* V167 (#5456): the table that records which old-format saved passwords were present at the upgrade, and no longer the
           top rung now that V168 and V169 have landed above it; it keeps its own arm so a store that stopped here does not fall
           through to the rung below and show a spurious upgrade banner. No viewer read names it; this
           arm exists so the version banner stays truthful. Named only in the probe line, not this prose, per the V71
           finding. */
        if (hasLegacyPinCandidates)
        {
            return 167;
        }

        if (hasPagerDutyAutoResolve)
        {
            return 166;
        }

        /* V165 (#5366): the tables that publish the service's password key, and no longer the top rung now that V166 has
           landed above it; it still gets its own arm so a store that stopped here does not fall through to the rung below and
           show a spurious upgrade banner. The viewer's read of the published key goes through the
           PasswordKey partial. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasPasswordKey)
        {
            return 165;
        }

        /* V164 (#4605): the hourly row-count ledger of the query_stats table, and no longer the top rung now that V165 has
           landed above it. No viewer read names it; this arm exists so the version banner stays truthful. Named only in
           the probe line, not this prose, per the V71 finding. */
        if (hasQueryStatsHourLedger)
        {
            return 164;
        }

        /* V163 (#5097): the store's statement history, and no longer the top rung now that V164 has landed above it. No
           viewer read names it; this arm exists so the version banner stays truthful. Named only in the probe line, not
           this prose, per the V71 finding. */
        if (hasStoreStatementHistory)
        {
            return 163;
        }

        /* V162 (#5097): the slow-read record, and no longer the top rung now that V163 has landed above it. No viewer
           read names it; this arm exists so the version banner stays truthful. Named only in the probe line, not this
           prose, per the V71 finding. */
        if (hasSlowReads)
        {
            return 162;
        }

        /* V161 (#5094): the daily summary tables for the Query Store top read, and no longer the top rung now that V162
           has landed above it. No viewer read names the tables yet; this arm exists so the version banner stays
           truthful. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasQueryStoreTopDaily)
        {
            return 161;
        }

        /* V160 (#4938): a collector's optional run time, and no longer the top rung now that V161 has landed above it, so a store that reaches exactly V160 falls through
           to here rather than to the rung below. The schedule window reads and writes it, so a store that maps below
           this arm is refused at connect, before the window can open. Named only in the probe line, not this prose, per
           the V71 finding. */
        if (hasCollectorRunAt)
        {
            return 160;
        }

        /* V159 (#4961): the install id table's own OID, and no longer the top rung now that V160 has landed above it, so
           a store that reaches exactly V159 falls through to here rather than to the rung below. No viewer read names the
           column (the readers of the row never select it, so one on a store still at the rung below keeps working); this
           arm exists so the version banner stays truthful. Named only in the probe line, not this prose, per the V71
           finding. */
        if (hasInstallIdTableOid)
        {
            return 159;
        }

        /* V158 (#4961): the install id's one-row store table, and no longer the top rung now that V159 has landed above
           it, so a store that reaches exactly V158 falls through to here rather than to the rung below. This arm exists
           so the version banner stays truthful and a viewer that reads the row is never pointed at a store without it.
           Named only in the probe line, not this prose, per the V71 finding. */
        if (hasInstallId)
        {
            return 158;
        }

        /* V157: a mute rule's optional store server id, and now the TOP rung, so a fully-migrated store maps to
           EXACTLY StorageVersion.SchemaVersion rather than falling through to the rung below and showing a spurious
           upgrade banner on a store that is current. Named only in the probe line, not this prose, per the V71 finding. */
        if (hasMuteRuleServerId)
        {
            return 157;
        }

        /* V156 (#4834): the hour's longest single checkpoint sync on the store's own checkpointer row, and now
           the TOP rung, so a fully-migrated store maps to EXACTLY StorageVersion.SchemaVersion rather than
           falling through to the rung below and showing a spurious upgrade banner on a store that is current.

           No viewer read names the columns yet; this arm exists so the version banner stays truthful, which is
           the only effect the rung has on the viewer. Named only in the probe line, not this prose, per the
           V71 finding. */
        if (hasCheckpointLongestSync)
        {
            return 156;
        }

        /* V155 (#4765): each Query Store interval's end, and no longer the top rung now that V156 has landed
           above it, so a store that reaches exactly V155 falls through to here rather than to the rung below
           and shows a spurious upgrade banner on a store that is current.

           No viewer read names the column yet; this arm exists so the version banner stays truthful, which
           is the only effect the rung has on the viewer. Named only in the probe line, not this prose, per
           the V71 finding. */
        if (hasQueryStoreIntervalEnd)
        {
            return 155;
        }

        /* V154 (#4608, split #4615): query_store_interval_wide's twin of V153's index, in its own rung
           and no longer the top rung now that V155 has landed above it, so a store that reaches exactly
           V154 falls through to here rather than to V153 and shows a spurious upgrade banner on a store
           that is current.

           The WPF viewer runs no analysis, so no viewer read names either index; this arm exists so the
           version banner stays truthful, which is the only effect the rung has on the viewer. Named only
           in the probe line, not this prose, per the V71 finding. */
        if (hasIntervalWideFirstExecIndex)
        {
            return 154;
        }

        /* V153 (#4608, split #4615): a plain btree on first_execution_time for query_store_interval_latest
           only — no longer the top rung now that V154 (query_store_interval_wide's twin) has landed above
           it, so a store that reaches exactly V153 falls through to here rather than to V152.

           The WPF viewer runs no analysis, so no viewer read names the index; this arm exists so the
           version banner stays truthful, which is the only effect the rung has on the viewer. Named only
           in the probe line, not this prose, per the V71 finding. */
        if (hasIntervalFirstExecIndexes)
        {
            return 153;
        }

        if (hasCaggGroupIndexDrop)
        {
            return 152;
        }

        /* V151 (#4475): ag_replica_states.group_id, a column that identifies which physical AG a replica
           row belongs to. Formerly the TOP rung — RequiredStoreSchemaVersion is StorageVersion.SchemaVersion
           and a store below this arm now falls through to V150 instead of stopping here.

           The WPF viewer runs no analysis, so no viewer read names the new column; this arm exists so the
           version banner stays truthful, which is the only effect the rung has on the viewer. Named only
           in the probe line, not this prose, per the V71 finding. */
        if (hasAgGroupId)
        {
            return 151;
        }

        /* V150 (#4469, #4477): two supporting indexes, idx_collection_log_watermark and
           idx_job_history_server_run. Formerly the TOP rung — RequiredStoreSchemaVersion is
           StorageVersion.SchemaVersion and a store below this arm now falls through to V149 instead of
           stopping here.

           The WPF viewer runs no analysis, so no viewer read names either index; this arm exists so the
           version banner stays truthful, which is the only effect the rung has on the viewer. Named only
           in the probe line, not this prose, per the V71 finding. */
        if (hasCollectionLogWatermarkAndJobHistoryIndexes)
        {
            return 150;
        }

        /* V149 (#4250): the Query Store liveness touch drops query_store_plan_map's last_seen index and
           sets fillfactor 90. Formerly the TOP rung — RequiredStoreSchemaVersion is StorageVersion.SchemaVersion
           and a store below this arm now falls through to V148 instead of stopping here.

           The WPF viewer runs no analysis, so no viewer read names this table; this arm exists so the
           version banner stays truthful, which is the only effect the rung has on the viewer. Named only
           in the probe line, not this prose, per the V71 finding. */
        if (hasHotLivenessTouch)
        {
            return 149;
        }

        if (hasReadLatency)
        {
            return 148;
        }

        if (hasComposeTimeoutSixty)
        {
            return 147;
        }

        if (hasManagedConfVerdicts)
        {
            return 146;
        }

        /* V145 (#3953): the wide latest-snapshot-per-interval table beside V143's — every outcome, every column
           the three new reads need, its own coverage and replay-record tables. TABLE-existence sentinel,
           newest-first, one rung behind the top since V146 landed — a store carrying this and not V146 above
           maps to 145, which is the honest answer for it and also what makes the upgrade banner correct in the
           window between the two.

           The WPF viewer runs no analysis, so no viewer read names the new tables; this arm exists so the version
           banner stays truthful, which is the only effect the rung has on the viewer. The tables are named only in
           the probe line, not this prose, per the V71 finding: the coverage ratchet strips information_schema lines
           but cannot strip a comment. */
        if (hasQueryStoreIntervalWide)
        {
            return 145;
        }

        /* V143 (#3953): the latest Query Store snapshot per interval, kept as it is written, with its per-server
           coverage and the replay record for a batch whose apply failed. TABLE-existence sentinel, newest-first,
           one rung behind the top since V144 landed — a store carrying this and not V144 above maps to 143,
           which is the honest answer for it and also what makes the upgrade banner correct in the window
           between the two.

           The WPF viewer runs no analysis, so no viewer read names the new tables; this arm exists so the version
           banner stays truthful, which is the only effect the rung has on the viewer. The tables are named only in
           the probe line, not this prose, per the V71 finding: the coverage ratchet strips information_schema lines
           but cannot strip a comment. */
        /* V144 (#4211): the raw chunk-interval reconcile's own rung history and per-run WAL-bytes record.
           TABLE-existence sentinel, newest-first, one rung behind the top since V145 landed — a store carrying
           this and not V145 above maps to 144, which is the honest answer for it and also what makes the
           upgrade banner correct in the window between the two.

           The WPF viewer runs no analysis, so no viewer read names the new tables; this arm exists so the
           version banner stays truthful, which is the only effect the rung has on the viewer. The tables are
           named only in the probe line, not this prose, per the V71 finding: the coverage ratchet strips
           information_schema lines but cannot strip a comment. */
        if (hasRawChunkIntervalRungHistory)
        {
            return 144;
        }

        if (hasQueryStoreIntervalLatest)
        {
            return 143;
        }

        /* V139 (#3955): the postmaster start time beside every sample of a server's cumulative checkpointer
           counters, on the store's own self-metrics row and on the monitored targets' write-side row, so a read
           can tell an interval that spans a restart (PostgreSQL counts the shutdown checkpoint as requested and
           keeps the count) from one that does not. COLUMN-existence sentinel (both tables predate it, so table
           existence cannot separate this from any rung), newest-first, one rung behind the top since V140 landed —
           a store carrying this and not V140 above maps to 139, which is the honest answer for it and also what
           makes the upgrade banner correct in the window between the two.

           The gate is load-bearing: the write-side panel reads through the shared DarlingPgWriteStatsReader,
           whose statement now names the new column to find the restarts inside its window, so a viewer pointed
           at a store below this rung WOULD throw on that panel. The connect-time gate is what keeps it from
           getting there. The column and its tables are named only in the probe line, not this prose, per the
           V71 finding: the coverage ratchet strips information_schema lines but cannot strip a comment. */
        /* V140 (#4037): the timed-checkpoint count beside the store's own checkpointer counters, so the
           checkpointer pressure rule can judge the interval's PER-CHECKPOINT sync average rather than the
           summed sync milliseconds. COLUMN-existence sentinel (its table predates it by eighty-seven rungs,
           so table existence cannot separate this from any rung), newest-first, one rung behind the top since
           V141 landed. No viewer surface reads the column yet, so this gate rests on the standing invariant
           alone. The column and its table are named only in the probe line, not this prose, per the V71
           finding. */
        /* V141 (#3691, part a1): collect.analysis_collection_caveats, the store side of a scheduled pass's
           collection caveats. TABLE-existence sentinel (its own new table), newest-first, one rung behind the
           top since V142 landed. No viewer surface reads the table yet, so this gate rests on the standing
           invariant alone. The table is named only in the probe line, not this prose, per the V71 finding. */
        /* V142 (#4196): the supporting index for the anomaly detector's latest-two-snapshots read.
           INDEX-existence sentinel, the same shape as V22's own arm below (indexes are not listed in
           information_schema, so pg_indexes is read instead), newest-first, one rung below the top since V143
           landed. No viewer surface reads the index directly (the analysis service does), so this gate rests on
           the standing invariant alone. The index is named only in the probe line, not this prose, per the V71
           finding. */
        if (hasIndexObjectStatsServerTimeIndex)
        {
            return 142;
        }


        if (hasCollectionCaveats)
        {
            return 141;
        }

        if (hasCheckpointsTimed)
        {
            return 140;
        }

        if (hasPostmasterStartTime)
        {
            return 139;
        }

        /* V138 (#3691): the two scope columns on the PostgreSQL configuration snapshot, so a stored setting
           row distinguishes the server-wide population from a per-database or per-role override.
           COLUMN-existence sentinel (its table predates it by thirty-six rungs, so table existence cannot
           separate this from any rung), newest-first, one rung behind the top since V139 landed — a store
           carrying this and not V139 above maps to 138, which is the honest answer for it and also what makes
           the upgrade banner correct in the window between the two.

           The gate is load-bearing here for the V137 reason one rung on: the Overview tab's configuration
           grid reads through the shared DarlingPgServerConfigReader, whose server-wide statement now names
           both columns in its predicate, so a viewer pointed at a store below this rung WOULD throw on that
           grid. The connect-time gate is what keeps it from getting there. The grid itself gains no column
           and the overrides are published only by get_pg_server_config in this lane. The columns and their
           table are named only in the probe line, not this prose, per the V71 finding: the coverage ratchet
           strips information_schema lines but cannot strip a comment. */
        if (hasPgServerConfigDatabaseRoleOverrides)
        {
            return 138;
        }

        /* V137 (#3796 / #3712 / #3783): four column sets on three existing tables — the two Query Store
           capture modes on the health row, the store-backed twin of the uncorroborated-finding route knob on
           the alert-settings row, and the plan dimension's TOAST bytes plus the store's own checkpointer
           phases on the store self-metrics row. COLUMN-existence sentinel (every one of its tables predates
           it, so table existence cannot separate this from any rung), newest-first, one rung behind the top
           since V138 landed — a store carrying this and not V138 above maps to 137, which is the honest
           answer for it and also what makes the upgrade banner correct in the window between the two.

           The gate is load-bearing here, not only the standing invariant: since #3796's code half the Query
           Store health grid's read (ViewerDataService.Config.cs, QueryStoreHealthSql) names the two capture
           modes, so a viewer pointed below this rung WOULD throw on that grid — the connect-time gate is what
           keeps it from getting there. The other six columns — the Settings window's route toggle and the
           store-size utilisation / checkpointer reads — are each their own lane and are named by no viewer
           read yet. The column and its table are named only in the probe line, not this prose,
           per the V71 finding: the coverage ratchet strips information_schema lines but cannot strip a
           comment. */
        if (hasQsCaptureModeRouteKnobToast)
        {
            return 137;
        }

        /* V136 (#3691): the two PostgreSQL-target series the analysis engine had no source for — the hourly
           per-database size series (a new table) and the host's memory from Performance Insights (six
           columns on the CPU collector's row). TABLE-existence sentinel (the new table, the V129 shape),
           newest-first, one rung behind the top since V137 landed — a store carrying this and not V137 above
           maps to 136, which is the honest answer for it and also what makes the upgrade banner correct in
           both directions.

           The gate rests on that standing invariant alone: no viewer read names the new table or the six
           columns yet — the consumer lanes are #3691's later slices — so a viewer pointed below this rung
           would not throw on any surface today. The table is named only in the probe line, not this prose,
           per the V71 finding: the coverage ratchet strips information_schema lines but cannot strip a
           comment. */
        if (hasPgDatabaseSizeStatsAndHostMemory)
        {
            return 136;
        }

        /* V135 (#3653 A5, Q5): the Long-Running Query alert's opt-out knob — two text[] lists on the singleton
           alert-settings row, program-name PREFIXES and exact logins, whose sessions the alert does not
           evaluate; DEFAULT the production read's seeds (the SQL Agent job-step prefix; the two NT AUTHORITY
           service logins). COLUMN-existence sentinel (the table has existed since V17, so table existence
           cannot separate this from the knob rungs before it), newest-first, one rung behind the top since
           V136 landed — a store carrying this and not V136 above maps to 135, which is the honest answer for
           it and also what makes the upgrade banner correct in both directions.

           The gate earns its place beyond that standing invariant: the viewer's alert-settings select and
           upsert name both columns (the Settings window's two boxes read and write them), so a viewer pointed
           below this rung would throw a raw 42703 the moment the operator opened Settings — the banner has to
           fire before that does. The columns and their table are named only in the probe line, not this
           prose, per the V71 finding: the coverage ratchet strips information_schema lines but cannot strip
           a comment. */
        if (hasLrqExclusionKnob)
        {
            return 135;
        }

        /* V134 (#3653 item 13, Q7 + Q8 — the "time honesty" rung): the UTC twin of the CPU sample's
           server-local stamp, and the engine's time-zone id beside the offset. COLUMN-existence sentinel
           (both tables have existed since V1, so table existence cannot separate this from any rung),
           newest-first, one rung behind the top since V135 landed — a store carrying this and not V135 above
           maps to 134, which is the honest answer for it and also what makes the upgrade banner correct in
           both directions.

           The gate earns its place beyond that standing invariant: the viewer's raw CPU read now names the
           UTC column (COALESCEd ahead of the #1262 per-batch de-skew), so a viewer pointed below this rung
           would throw a raw 42703 the moment a server tab opened its CPU chart or the Overview's CPU lane —
           the banner has to fire before either does. The columns and their tables are named only in the
           probe line, not this prose, per the V71 finding: the coverage ratchet strips information_schema
           lines but cannot strip a comment. */
        if (hasTimeHonesty)
        {
            return 134;
        }

        /* V133 (#3691): numbackends on the PostgreSQL per-database counters and sampled_ms on the sampled
           waits — the connection-saturation numerator and the sampler arm's duty-cycle denominator, written
           by the collectors and read by nothing yet. COLUMN-existence sentinel (both tables have existed
           since their own collector rungs, so table existence cannot separate this from them), newest-first,
           one rung behind the top since V134 landed — a store carrying this and not V134 above maps to 133,
           which is the honest answer for it and also what makes the upgrade banner correct in both
           directions.

           The gate rests on that standing invariant alone: no viewer read names either column yet, so a
           viewer pointed below this rung would throw nothing — the arm exists because RequiredStoreSchemaVersion
           is StorageVersion.SchemaVersion and a current store has to map to it. The columns and their tables
           are named only in the probe line, not this prose, per the V71 finding: the coverage ratchet strips
           information_schema lines but cannot strip a comment. */
        if (hasPgNumbackendsAndSampledMs)
        {
            return 133;
        }

        /* V132 (#3653 A7): the perfmon counter's type beside its value, so the store can say which of its
           perfmon rows are counts and which are levels and the collector stops differencing gauges.
           COLUMN-existence sentinel (the table has existed since V4, so table existence cannot separate the
           rungs), newest-first, one rung behind the top since V133 landed — a store carrying this and not
           V133 above maps to 132, which is the honest answer for it and also what makes the upgrade banner
           correct in both directions.

           The gate earns its place beyond that standing invariant: the perfmon trend read names the column
           (the chart classifies each series by it, the #3702 name proxy only as the NULL fallback), so a
           viewer pointed below this rung would throw a raw 42703 the moment the operator ticked a counter
           on the Perfmon tab — the banner has to fire before the tab does. The column and its table are
           named only in the probe line, not this prose, per the V71 finding: the coverage ratchet strips
           information_schema lines but cannot strip a comment. */
        if (hasPerfmonCounterType)
        {
            return 132;
        }

        /* V131 (#3598): config.config_notification_routes, the sparse routes that let alert FAMILIES land on
           different channels than the parent config_notification row's. TABLE-existence sentinel (the
           table is new in this rung, the V116/V123 shape for a config-plane table), newest-first, one rung
           behind the top since V132 landed — a store carrying this and not V132 above maps to 131, which is
           the honest answer for it and also what makes the upgrade banner correct in both directions.

           The gate earns its place beyond that standing invariant: the Settings window's Manage
           Notification Routes grid reads the table by name, so a viewer pointed below this rung would
           throw a raw 42P01 the moment the operator opened it — the banner has to fire before the grid
           does. */
        if (hasNotificationRoutes)
        {
            return 131;
        }

        /* V130 (#3602, #3603): the family-specific numbers on collect.pg_log_events — a spill's bytes, an
           autovacuum run's relation, duration, pages, tuples, buffers and WAL — thirteen nullable columns
           the two parser families lift out of the prose V129 stored whole. COLUMN-existence sentinel (the
           table is V129's, so table existence cannot separate the rungs), newest-first, one rung behind the
           top since V131 landed — a store carrying this and not V131 above maps to 130, which is the honest
           answer for it and also what makes the upgrade banner correct in both directions.

           The gate earns its place beyond that standing invariant: the service's log-events reader names
           the columns (the Viewer's panel is served by the same table and will name them when it grows the
           spill / run columns), so a store below this rung would throw a raw 42703 on the first read that
           does — the banner has to fire before that read. The column and table are named only in the probe
           line, not this prose, per the V71 finding. */
        if (hasPgLogEventMetrics)
        {
            return 130;
        }

        /* V129 (#3601): collect.pg_log_events, the classified log-event pipeline's table — the third
           reader of the PostgreSQL server log and the first to carry more than one family (errors,
           connections, lock waits, and the recognised-only spill / autovacuum / checkpoint shapes).
           TABLE-existence sentinel, like V103's and V106's before it, newest-first, one rung behind the top
           since V130 landed — a store carrying this and not V130 above maps to 129, which is the honest
           answer for it and also what makes the upgrade banner correct in both directions.

           The gate earns its place beyond that standing invariant: the PostgreSQL Activity tab's log-events
           panel reads the table by name, so a viewer pointed below this rung would throw a raw 42P01 on
           that tab — the banner has to fire before the tab does. */
        if (hasPgLogEvents)
        {
            return 129;
        }

        if (hasDeltaFamilyIntervalCompletion)
        {
            return 128;
        }

        /* V127 (#3540): the measured sample interval on the four delta families that persisted their
           deltas naked — the measurement-layer keystone, so a restart's fabricated (0, 0) row can be told
           from a genuinely idle one at every read. COLUMN-existence sentinel (the four tables have existed
           since V4/V10, so table existence cannot separate the rungs), newest-first, one rung behind the
           top since V128 landed — a store carrying this and not V128 above maps to 127, which is the
           honest answer for it and also what makes the upgrade banner correct in both directions.

           The gate earns its place beyond that standing invariant: every viewer trend read over these four
           families names the column (the stored interval is preferred over the LAG derivation), so a
           viewer pointed below this rung would throw a raw 42703 on the Waits, File I/O and Latch/Spinlock
           tabs — the banner has to fire before those tabs do. The column and its tables are named only in
           the probe line, not this prose, per the V71 finding: the coverage ratchet strips
           information_schema lines but cannot strip a comment. */
        if (hasDeltaFamilyIntervalColumns)
        {
            return 127;
        }

        /* V126 (#3528): the Store Disk Pressure warning's GB floor on config_alert_settings — the AND
           qualifier that stops a large store volume at a low percent paging CRITICAL. COLUMN-existence
           sentinel (the table has existed since V17, so table existence cannot separate the rungs),
           newest-first, one rung behind the top since V127 landed — a store carrying this and not V127
           above maps to 126, which is the honest answer for it and also what makes the upgrade banner
           correct in both directions.

           The gate earns its place beyond that standing invariant since the viewer pass (#3563): the
           alert-settings select now names the column, so a viewer pointed below this rung would throw
           a raw 42703 when the Settings window prefills — the knob landed backend-first, and the
           Settings window's box is wired now. The column is named only in the probe line, not this
           prose, per the V71 finding: the coverage ratchet strips information_schema lines but cannot
           strip a comment. */
        if (hasSelfDiskWarnGbFloor)
        {
            return 126;
        }

        /* V125 (#3477): the per-collector database scope on config_collector_schedules — the
           allow-list that lets an expensive per-database collector run against a representative
           sample instead of being turned off for the whole server. COLUMN-existence sentinel (the
           table has existed since V17, so table existence cannot separate the rungs), newest-first,
           one rung behind the top — a store carrying this and not V126 above maps to 125, which is
           the honest answer for it and also what makes the upgrade banner correct in both
           directions.

           The gate earns its place beyond that standing invariant: the schedule editor's select now
           names the column, so a viewer pointed below this rung would throw a raw 42703 on OPENING
           the Collector Schedules window — the banner has to fire before that window does. The
           column is named only in the probe line, not this prose, per the V71 finding: the coverage
           ratchet strips information_schema lines but cannot strip a comment. */
        if (hasCollectorScheduleDatabases)
        {
            return 125;
        }

        /* V124 (#3466): the fleet sweep's cadence knobs on config_alert_settings — lane 2's control
           plane, beside the analysis cadence they are modeled on. COLUMN-existence sentinel (the table
           has existed since V17, so table existence cannot separate the rungs), newest-first, one rung
           behind the top — a store carrying this and not V125 above maps to 124, which is the honest
           answer for it and also what makes the upgrade banner correct in both directions.

           The gate earns its place beyond that standing invariant: below this rung the Settings
           window's Fleet Sweep boxes have nowhere to save to and the worker's cadence falls back to
           the shipped default, so an operator who slows the sweep sees no change and is told nothing —
           the "setting did not stick" reading V119/V120/V122 below exist to remove. The column is
           named only in the probe line, not this prose, per the V71 finding: the coverage ratchet
           strips information_schema lines but cannot strip a comment. */
        if (hasFleetSweepCadenceKnobs)
        {
            return 124;
        }

        /* V123 (#3466): the fleet sweep's state store — the sweep-run tables and the watch-item
           worklist, lane 1 of the approved four-lane sequence. TABLE-existence sentinel — all four
           objects are new in this rung, and the run table is the parent every read starts from —
           newest-first, one rung behind the top: a store carrying this and not V124 above maps to 123,
           which is the honest answer for it and also what makes the upgrade banner correct in both
           directions.

           Nothing in the viewer reads these tables yet — the web feed is lane 3 and the MCP surface
           is lane 4 — so this arm is the don't-under-report guard, the V44/V53 reasoning that V108
           through V113 all restated. The table is named only in the probe line above and NOT in this
           prose, per the V71 finding: the coverage ratchet strips information_schema lines but cannot
           strip a comment. */
        if (hasFleetSweepState)
        {
            return 123;
        }

        /* V122 (#3444): config.config_alert_settings gains the PostgreSQL Deadlocks/Blocking count
           thresholds, which were compile-time constants in the worker with no settings path while their SQL
           Server twins have been settable since V1. COLUMN-existence sentinel (the table has existed since
           V17, so table existence cannot separate the rungs), newest-first, one rung behind the top — a
           store carrying this and not V123 above maps to 122, which is the honest answer for it and also
           what makes the upgrade banner correct in both directions.

           The gate earns its place beyond that standing invariant. Below this rung the Settings window's two
           new boxes have nowhere to save to and the worker's gates fall back to their shipped defaults, so
           an operator who raises a threshold for a retry-heavy PostgreSQL workload sees no change and is
           told nothing — the same "the setting did not stick" reading V119 and V120 below exist to remove.

           The column is named only in the probe line, not this prose, per the V71 finding: the coverage
           ratchet strips information_schema lines but cannot strip a comment. */
        if (hasPgAlertCountKnobs)
        {
            return 122;
        }

        /* V121 (#3392): the backlog of cached plans the per-row capture cap declined, plus the measured
           plan size on the two fact tables that feed it. TABLE-existence sentinel — the table is new in this
           rung, so existence separates it cleanly — newest-first, one rung behind the top — a store carrying
           this and not V122 above maps to 121, which is the honest answer for it and also what makes the
           upgrade banner correct in both directions.

           The gate earns its place beyond that standing invariant. Below this rung the viewer's two
           stored-plan reads have no backlog to fall back to, and the grids' plan-presence flags read a column
           that does not exist — so the Query Plan button would offer a plan the store cannot produce for
           exactly the rows this rung exists to serve.

           The table is named only in the probe line, not this prose, per the V71 finding: the coverage
           ratchet strips information_schema lines but cannot strip a comment. */
        if (hasOversizedPlanBacklog)
        {
            return 121;
        }

        /* V120 (#3368): config.config_alert_settings gains the deadlock health band's warn/critical tiers in
           deadlocks per hour. The band was count > 0 ? Critical : Healthy, so one resolved deadlock made a
           server Critical and Critical stopped discriminating on any OLTP fleet. COLUMN-existence sentinel
           (the table has existed since V17, so table existence cannot separate the rungs), newest-first, one
           rung behind the top — a store carrying this and not V121 above maps to 120, which is the honest
           answer for it and also what makes the upgrade banner correct in both directions.

           The gate earns its place beyond that standing invariant. Below this rung the Settings window's two
           new boxes have nowhere to save to and both cards fall back to the shipped tiers, so an operator who
           raises a tier for a retry-on-deadlock workload sees no change and is told nothing — the same "the
           setting did not stick" reading V119 above exists to remove.

           The column is named only in the probe line, not this prose, per the V71 finding: the coverage
           ratchet strips information_schema lines but cannot strip a comment. */
        if (hasDeadlockRateBandKnobs)
        {
            return 120;
        }

        /* V119 (#3297): config.config_alert_settings gains the Retention Held warn/critical ratios, which
           were compile-time constants — so the alert an operator most needs to tune was the one alert that
           could not be. COLUMN-existence sentinel (the table has existed since V17, so table existence
           cannot separate the rungs), newest-first, and one below the top rung: a V120 store also carries
           this column, so it must be tested AFTER V120's or every V120 store would report as 119 and show
           an upgrade banner against a store that is current.

           The gate earns its place beyond that standing invariant. On a store below this rung the Settings
           window's two new boxes have nowhere to save to and the service's seams fall back to their shipped
           defaults, so an operator who lowers or raises a tier is told nothing and sees no change — the same
           "the setting did not stick" reading #3296 already produced once from the other direction.

           The column is named only in the probe line, not this prose, per the V71 finding: the coverage
           ratchet strips information_schema lines but cannot strip a comment. */
        if (hasRetentionHoldRatioKnobs)
        {
            return 119;
        }

        /* V118 (#3282): config.alert_persistence_state — the BUILT-IN alert catalog's persistence-gate
           state. Table-existence sentinel, sitting one below the top rung: a V119 store also carries this
           table, so it must be tested AFTER V119's column or every V119 store would report as 118 and show
           an upgrade banner against a store that is current.

           The reason to gate is that standing invariant rather than a viewer read that would throw: nothing
           in the viewer reads this table — the SERVICE's alert pass is its only consumer. What the banner
           buys on the way is worth having anyway. On a store still at 117 the CPU alert has no memory of
           an open incident across a service restart, so every High CPU row in that history was produced by
           the pre-#3282 single-sample rule, and an operator comparing alert volume before and after should
           know which rule wrote which rows before drawing a conclusion from the drop.

           The table is named only in the probe line, not this prose, per the V71 finding: the coverage
           ratchet strips information_schema lines but cannot strip a comment, so a prose mention would
           exempt it. */
        if (hasBuiltinAlertPersistence)
        {
            return 118;
        }

        /* V117 (#3315): config.config_mute_rules joins the config_version reload beacon, so a mute write of
           any kind makes the service re-load its in-memory mute cache on the next sweep. Trigger-existence
           sentinel — the trigger is the only object the rung creates — and newest-first, so it sits above
           the V116 arm: a V117 store also carries V116's table, and testing V116 first would report every
           V117 store as 116 and show an upgrade banner against a store that is current.

           The gate earns its place beyond the standing invariant. On a store below this rung a mute created
           anywhere but the Viewer takes effect at an unbounded later time, and the surfaces that would
           reassure the operator — the rule list, the tool's own success reply — read the table rather than
           the cache, so they agree the mute is in place while alerts keep being delivered. An operator
           reading Manage Mute Rules against such a store should be told the store is behind before they
           trust what it shows them. */
        if (hasMuteRuleReloadBeacon)
        {
            return 117;
        }

        if (hasCustomAlertCore)
        {
            return 116;
        }

        if (hasPgCpuCapacityHeadroom)
        {
            return 115;
        }

        if (hasPgIndexBloatEstimate)
        {
            return 114;
        }

        if (hasRemediationCredentialAndActor)
        {
            return 113;
        }

        if (hasCollectorStallProbes)
        {
            return 112;
        }

        if (hasStoreLogSelfMonitoring)
        {
            return 111;
        }

        if (hasCollectionLogFetchPhaseSums)
        {
            return 110;
        }

        if (hasCollectionLogDrainForensics)
        {
            return 109;
        }

        /* V108 (#2851 made queryable): collect.collection_log gains sql_open_ms / sql_drain_ms /
           watermark_ms, so the server-scoped phase split can be aggregated and trended instead of living
           only in an app-log line. COLUMN-existence sentinel on sql_drain_ms, because the table itself has
           existed since V1 and only the columns are new; drain is the phase the rung exists to expose. The
           TOP rung now, so it must map EXACTLY or the connect-time gate refuses a store that is perfectly
           current. Nothing in the viewer reads these columns yet - the read is on the MCP surface - so this
           arm is the don't-under-report guard, the V44/V53 reasoning. */
        if (hasCollectionLogPhaseSplit)
        {
            return 108;
        }

        /* V107 (#2138): collect.plan_force_actions, the auto force-plan bot's journal (plus the per-server
           opt-in column on the registry, which the same rung adds). Table-existence sentinel. The TOP rung
           now, so it must map exactly or the connect-time gate refuses a store that is perfectly current.
           The viewer itself reads nothing from the table yet — the read is on the MCP/web surface — so this
           arm is the don't-under-report guard, the V44/V53 reasoning. */
        if (hasPlanForceActions)
        {
            return 107;
        }

        /* V106 (#2719): collect.pg_cpu_utilization, instance-level CPU for a managed PostgreSQL/Aurora
           target via AWS Performance Insights. Table-existence sentinel. Was the TOP rung until V107. */
        if (hasPgCpuUtilization)
        {
            return 106;
        }

        /* V105 (#2674): collect.collector_cost, the tool's own per-collector cost self-metric. Table-existence
           sentinel. Was the TOP rung until V106. */
        if (hasCollectorCost)
        {
            return 105;
        }

        /* V104 (#2661): the deadlock lookup index. An INDEX sentinel rather than a table one, because
           V103 created the table and V104 only indexes it — a store stopped between the two has the table
           and not the index, which is a real interrupted-upgrade state and exactly what these arms exist to
           distinguish. Was the top rung until V105. */
        if (hasPgDeadlockIdentity)
        {
            return 104;
        }

        /* V103 (#2661): collect.pg_deadlocks itself. */
        if (hasPgDeadlocks)
        {
            return 103;
        }

        /* V102 (#2658): the pg_settings snapshot. Table-existence sentinel. Was the top rung until V103
           landed and keeps its own arm, because a store stopped between them is a real state during an
           interrupted upgrade. */
        if (hasPgServerConfig)
        {
            return 102;
        }

        /* V101 (#2655): PostgreSQL 18's measured I/O byte columns. Column sentinel. Was the top rung until
           V102 landed and keeps its own arm, because a store stopped between the two is a real state
           during an interrupted upgrade. */
        if (hasPg18IoBytes)
        {
            return 101;
        }

        /* V100 (#2653): the PostgreSQL major on the registry. A COLUMN sentinel, like V82's next door. Was
           the top rung until V101 landed and keeps its own arm, because a store stopped between the two is
           a real state during an interrupted upgrade. */
        if (hasPgMajorVersion)
        {
            return 100;
        }

        /* V99 (#2566): pg_plan_capture. Was the top rung until V100 landed and keeps its own arm, because a
           store stopped between the two is a real state during an interrupted upgrade. */
        if (hasPgPlanCapture)
        {
            return 99;
        }

        /* V98 (#2603): pg_predicate_stats. Was the top rung until V99 landed and keeps its own arm, because
           a store stopped between the two is a real state during an interrupted upgrade. */
        if (hasPgPredicateStats)
        {
            return 98;
        }

        /* V97 (#2603): pg_kernel_stats. Was the top rung until V98 landed and keeps its own arm, because a
           store stopped between the two is a real state during an interrupted upgrade. */
        if (hasPgKernelStats)
        {
            return 97;
        }

        /* V96 (#2603): pg_wait_sampling. Was the top rung until V97 landed and keeps its own arm, because a
           store stopped between the two is a real state during an interrupted upgrade. */
        if (hasPgWaitSampling)
        {
            return 96;
        }

        /* V95 (#2599): database_name on the three per-database PostgreSQL tables. Was the top rung until
           V96 landed and keeps its own arm, because a store stopped between the two is a real state during
           an interrupted upgrade. */
        if (hasPgPerDatabaseAttribution)
        {
            return 95;
        }

        /* V94 (#2561): b-tree index bloat, measured. Was the top rung until V95 landed and keeps its own
           arm, because a store stopped between the two is a real state during an interrupted upgrade. */
        if (hasPgIndexBloat)
        {
            return 94;
        }

        /* V93 (#2544, buffers): what is resident in shared buffers. Was the top rung until V94 landed and
           keeps its own arm, because a store stopped between the two is a real state during an interrupted
           migration. Formerly the TOP rung — RequiredStoreSchemaVersion is StorageVersion.SchemaVersion and a
           fully-migrated store must map to exactly that, or the version banner reports a mismatch on a store
           that is current. */
        if (hasPgBufferUsage)
        {
            return 93;
        }

        /* V92 (#2544, replication): connected standbys and their distance from the primary. Was the top rung
           until V93 landed and keeps its own arm, because a store stopped between the two is a real state
           during an interrupted migration. */
        if (hasPgReplicationStats)
        {
            return 92;
        }

        /* V91 (#2543): per-column planner statistics. Was the top rung until V92 landed and keeps its own
           arm, because a store stopped between the two is a real state during an interrupted migration. */
        if (hasPgColumnStats)
        {
            return 91;
        }

        /* V90 (#2544, locks): lock state by mode, type and relation. Was the top rung until V91 landed and
           keeps its own arm, because a store stopped between the two is a real state during an interrupted
           migration. */
        if (hasPgLockStats)
        {
            return 90;
        }

        /* V89 (#2545): which extensions this target has, could have, or cannot have. Was the top rung until
           V90 landed and keeps its own arm, because a store stopped between the two is a real state during
           an interrupted migration. */
        if (hasPgExtensionAvailability)
        {
            return 89;
        }

        /* V88 (#2544): the write side — checkpoints, background writer, WAL. Was the top rung until V89
           landed and keeps its own arm, because a store stopped between the two is a real state during an
           interrupted migration. */
        if (hasPgWriteStats)
        {
            return 88;
        }

        /* V87 (#2564): whether a PostgreSQL target could capture plans at all. Was the top rung until V88
           landed and is kept as its own arm rather than folded away, because a store stopped between the two
           is a real state during an interrupted migration. */
        if (hasPgPlanCaptureReadiness)
        {
            return 87;
        }

        if (hasPgSessionStates)
        {
            return 86;
        }

        /* #2542: below the top arm now, and still not redundant — a store stopped between the V84 and V85
           rungs is a real state during an interrupted migration, and without its own arm it would report 84
           while carrying V85's table. */
        if (hasPgTableBloatStats)
        {
            return 85;
        }

        /* #2541: the index-usage rung. Below V85, above V83, for the reason every arm here is ordered
           newest-first — a V86 store also has V84's table, so testing V84 first would report every current
           store as two rungs behind and raise an upgrade banner against a store that is fine. */
        if (hasPgIndexUsageStats)
        {
            return 84;
        }

        /* #2539: the arm below STAYS — a store migrated to exactly 82 must map to 82 rather than falling
           through. The table is named only in the probe line, not in this prose, per the V71 finding.

           A collector-table rung would ordinarily get no arm at all; this one was the TOP rung when it
           landed, which is why it has one. Nothing in the viewer reads the table; the read that does is on
           the MCP surface. */
        if (hasPgDatabaseStats)
        {
            return 83;
        }

        /* #2530: the arm below STAYS — a store migrated to exactly 81 must map to 81 rather than falling
           through. The column is named only in the probe line, not in this prose, per the V71 finding.

           This rung's gate is worth having for what it tells the OPERATOR, not only for the standing
           RequiredStoreSchemaVersion invariant: a store below it records nothing that says a target is
           PostgreSQL, so every PostgreSQL server in it is indistinguishable from a SQL Server that has
           never connected — and every surface that branches on engine kind is therefore reading a fact
           that store cannot supply. */
        if (hasServerEngineKind)
        {
            return 82;
        }

        if (hasTempDbMaxSize)
        {
            return 81;
        }

        if (hasCollectionLogFanoutRollup)
        {
            return 80;
        }

        if (hasFileGrowthAlert)
        {
            return 79;
        }

        if (hasComposeTimeoutKnob)
        {
            return 78;
        }

        if (hasQueryStoreTextHash)
        {
            return 77;
        }

        if (hasQueryStoreHealth)
        {
            return 76;
        }

        if (hasPlanContentRetentionKnob)
        {
            return 75;
        }

        if (hasQueryStoreText)
        {
            return 74;
        }

        /* #2219: newest first. The previous arm STAYS — a store migrated to exactly 72 must still map to 72
           rather than falling through to the pre-PostgreSQL floor. */
        if (hasPgStatementText)
        {
            return 73;
        }

        if (hasQueryStorePlanMap)
        {
            return 72;
        }

        if (hasPgBlockingEdges)
        {
            return 71;
        }

        /* V70 (the monitored-server engine + port columns): column-existence sentinel, newest-first arm, and
           the reason to gate is the standing invariant rather than a 42703 — RequiredStoreSchemaVersion is
           StorageVersion.SchemaVersion, so a fully-migrated store must map to EXACTLY that or the version
           banner reports a mismatch on a store that is current.

           V63-V69 (the seven PostgreSQL collector tables) get no arms of their own deliberately: they add
           tables in `collect` that no viewer read names, so a store between them is only ever transient
           mid-migration, and the invariant that has to hold is "fully migrated maps to the top rung" — which
           V70, the top rung, is what senses. */
        if (hasMonitoredServerEngine)
        {
            return 70;
        }

        /* V62 (the #2171 plan-XML codec knob): column-existence sentinel, newest-first arm.
           config_service.plan_xml_compression exists only at V62 or later. Sits directly above the
           V61 arm it merged over - a store carrying both sentinels is V62 and must map there, not to
           the first older arm that happens to match. */
        if (hasPlanXmlCompressionKnob)
        {
            return 62;
        }

        /* V61 (per-fingerprint occurrence counters, #2216): table-existence sentinel, newest-first arm.
           config.incident_occurrences exists only at V61 or later.

           Same reason to gate as V60 rather than V59: nothing in the VIEWER names this table (it is the
           alert engine's accumulator memory, written and read by the service), so the viewer would not
           42703 against a V60 store. The gate exists to keep the standing invariant —
           RequiredStoreSchemaVersion is StorageVersion.SchemaVersion, so a fully-migrated store has to map
           to exactly that or the version banner reports a mismatch on a store that is current. The
           consequence of a V60 store is on the SERVICE side: the occurrence load/save would fail, the
           accumulator would fall back to reporting the total as the window count, and #2216's counter would
           silently be a gauge again. */
        if (hasIncidentOccurrences)
        {
            return 61;
        }

        /* V60 (database-state edge memory, #2166): column-existence sentinel, newest-first arm.
           config.database_state_expected.last_alerted_state exists only at V60 or later.

           Unlike the V59 arm below, the reason to gate here is NOT that the viewer would 42703: the
           expected-state editor names only expected_state / is_user_override, so nothing in the viewer
           touches either V60 column. The gate is the standing invariant instead — RequiredStoreSchemaVersion
           is StorageVersion.SchemaVersion, so a fully-migrated store must map to exactly that or the version
           banner reports a mismatch on a store that is actually current. The columns are written by the
           SERVICE (the alert engine's edge memory) and read by its deviation query, so it is the service that
           would fail on a V59 store, which is precisely why the viewer must not report V59 as good. Stated
           explicitly because the V59 wording does not transfer, and someone deciding later whether this gate
           can be relaxed needs the real reason. */
        if (hasDatabaseStateEdgeMemory)
        {
            return 60;
        }

        /* V59 (the collector memory knobs, #2164 + #2170): column-existence sentinel, newest-first arm.
           config_service.query_store_text_budget_mb exists only at V59 or later. The viewer NAMES both new
           columns in ServiceConfigSelectSql/UpdateFlagsSql, so against a V58 store the Settings read would
           fail 42703 — the gate must refuse it, and a fully-migrated V59 store must map to exactly
           RequiredStoreSchemaVersion. */
        if (hasCollectorMemoryKnobs)
        {
            return 59;
        }

        /* V58 (the Query Store backfill off switch, #2167): column-existence sentinel, newest-first arm.
           config_service.query_store_backfill_enabled exists only at V58 or later. The viewer NAMES it in
           ServiceConfigSelectSql/UpdateFlagsSql, so against a V57 store the Settings read would fail
           42703 — the gate must refuse it, and a fully-migrated V58 store must map to exactly
           RequiredStoreSchemaVersion. */
        if (hasBackfillSwitch)
        {
            return 58;
        }

        /* V57 (the Store Job Over Cadence warning knob, #2136): column-existence sentinel, newest-first
           arm. config_alert_settings.store_job_cadence_warn_percent exists only at V57 or later. The
           viewer NAMES it in AlertSettingsSelectSql/Upsert, so against a V56 store the Settings read
           would fail 42703 — the gate must refuse it, and a fully-migrated V57 store must map to
           exactly RequiredStoreSchemaVersion. */
        if (hasJobCadenceKnob)
        {
            return 57;
        }

        /* V56 (background-job self-metrics columns, #2136): column-existence sentinel, newest-first arm.
           store_metrics.last_run_duration_ms exists only at V56 or later. The viewer never reads these
           columns — they feed the service's own capacity series (job duration vs schedule cadence) over
           MCP/REST — so like V53's rung nothing in the viewer would fail against a V55 store. The rung
           exists so a fully-migrated store maps to exactly RequiredStoreSchemaVersion instead of capping
           at 55 and tripping the connect-time gate against a healthy store. Under-reporting is the
           guarded failure. */
        if (hasJobMetricsColumns)
        {
            return 56;
        }

        /* V55 (self-alert threshold knobs, #2107): column-existence sentinel, newest-first arm.
           config_alert_settings.self_disk_free_warn_percent exists only at V55 or later. The viewer
           NAMES the V55 columns in AlertSettingsSelectSql/Upsert, so against a V54 store the
           Settings read would fail 42703 — the gate must refuse it, and a fully-migrated V55 store
           must map to exactly RequiredStoreSchemaVersion. */
        if (hasSelfAlertKnobs)
        {
            return 55;
        }

        /* V54 (gzip plan-dim content, #2069): column-existence sentinel, newest-first arm.
           query_plan_dim.query_plan_gz exists only at V54 or later. The viewer NAMES the column in
           its plan-fetch reads (gz-else-text coalesce), so against a V53 store those reads would
           fail 42703 — the gate must refuse it, and a fully-migrated V54 store must map to exactly
           RequiredStoreSchemaVersion. */
        if (hasPlanDimGzip)
        {
            return 54;
        }

        /* V53 (store self-metrics, #2068): table-existence sentinel, newest-first arm.
           collect.store_metrics exists only at V53 or later. The viewer never reads this table — it is
           the service's own hourly capacity series (per-hypertable size/compression, dimension row
           counts, whole-store size), surfaced over MCP/REST — so like V44's rung nothing in the viewer
           would fail against a V52 store. The rung exists so a fully-migrated store maps to exactly
           RequiredStoreSchemaVersion instead of capping at 52 and tripping the connect-time gate against
           a healthy store. Under-reporting is the guarded failure. */
        if (hasStoreMetrics)
        {
            return 53;
        }

        /* V52 (persisted finding drill-down, #2060): column-existence sentinel, newest-first arm.
           analysis_findings.drill_down_json exists only at V52 or later. The viewer never names the
           column (it reads findings through its own SELECT list), so nothing in the viewer fails
           against a V51 store — the rung exists for the same reason V44's does: a probe that cannot
           SEE the newest migration maps every fully-migrated store below RequiredStoreSchemaVersion
           and the connect-time gate refuses healthy stores. Under-reporting is the guarded failure. */
        if (hasFindingDrillDown)
        {
            return 52;
        }

        /* V51 (query-stats host object, #2012 stage 2): column-existence sentinel, newest-first arm.
           collect.query_stats.host_object_name exists only at V51 or later. The viewer NAMES this column
           in its Top Queries read (TopQueriesSql groups by it and the LATERAL filters on it), so against
           a V50 store the grid read would fail 42703 outright — the gate must refuse it, and a
           fully-migrated V51 store must map to exactly RequiredStoreSchemaVersion. */
        if (hasQueryStatsHostObject)
        {
            return 51;
        }

        /* V50 (server-tag colour, #2008 2a): column-existence sentinel, newest-first arm.
           config.server_tags.colour exists only at V50 or later. The viewer names the colour column in its
           tag SELECT, so a fully-migrated V50 store must map to exactly RequiredStoreSchemaVersion rather
           than capping at 49 and tripping the connect-time gate against a healthy store — the same rule as
           every newest-column rung below. */
        if (hasServerTagColour)
        {
            return 50;
        }

        /* V49 (database-state alert): engine-agnostic column-existence sentinel, newest-first arm.
           config_alert_settings.database_state_enabled exists only at V49 or later. The viewer names this
           column in its alert-settings SELECT and upsert, so a fully-migrated V49 store maps to exactly
           RequiredStoreSchemaVersion rather than capping at 48 and tripping the connect-time gate against a
           healthy store. */
        if (hasDatabaseStateAlert)
        {
            return 49;
        }

        /* V48 (PVS-pressure alert knobs, #1984): column-existence sentinel, newest-first arm.
           config_alert_settings.pvs_enabled exists only at V48 or later. The viewer names all three
           V48 columns in its alert-settings SELECT and upsert, so against a V47 store the Settings
           window read would fail outright with 42703 rather than degrade — the same reason the V40
           blocking_wait rung gates. */
        if (hasPvsPressureKnobs)
        {
            return 48;
        }

        /* V47 (ADR persistent version store, #1951): table-existence sentinel, newest-first arm.
           collect.pvs_stats exists only at V47 or later. Unlike V44 the viewer DOES read this table —
           the FinOps PVS grid queries it by name — so a V46 store would fail 42P01 rather than degrade,
           and the gate must refuse it. */
        if (hasPvsStats)
        {
            return 47;
        }

        /* V46 (automatic plan correction, #1952): table-existence sentinel, newest-first arm.
           collect.plan_correction exists only at V46 or later. Note the gap — 45 is permanently unused
           (it was reserved for #1951 before that lane was renumbered to 47), so there is no V45 rung and
           there never will be a store carrying one. */
        if (hasPlanCorrection)
        {
            return 46;
        }

        /* V44 (collector state, #1962): table-existence sentinel, newest-first arm.
           collect.collector_state exists only at V44 or later. The viewer never READS this table — it is
           service-only state (default_trace_events' last-seen trace file) with no view and no viewer query
           — so unlike the arms below, nothing in the viewer would fail against a V43 store. The rung
           exists so a FULLY-migrated V44 store maps to 44 rather than capping at 43: the connect-time gate
           compares the probe against RequiredStoreSchemaVersion, and a probe that cannot see the newest
           migration reports every healthy store as skewed and refuses to open it. Under-reporting is the
           failure mode this ladder guards, and it does not care whether the viewer reads the table. */
        if (hasCollectorState)
        {
            return 44;
        }

        /* V43 (PagerDuty proxy, #1945): column-existence sentinel, newest-first arm.
           config_notification.pagerduty_proxy exists only at V43 or later. The viewer MUST gate on it: the
           Settings notification read and upsert name the column, so pointed at a V42 store they would fail
           42703 rather than degrade. */
        if (hasPagerDutyProxy)
        {
            return 43;
        }

        /* V42 (PagerDuty webhook): column-existence sentinel, newest-first arm.
           config_notification.pagerduty_routing_key exists only at V42 or later. */
        if (hasPagerDutyWebhook)
        {
            return 42;
        }

        /* V41 (#1841 tier 2 Query Store interval identity): engine-agnostic column-existence sentinel,
           newest-first arm. query_store_stats.runtime_stats_interval_id exists only at V41 or later. The
           viewer MUST gate on it: every Query Store aggregate read now names that column inside its dedup
           partition, so pointed at a V40 store the Query Store tab would fail outright (42703) rather than
           degrade. The COLUMN is the sentinel, not the table — V41 only widens a table that has existed
           since V1. */
        if (hasQueryStoreIntervalIdentity)
        {
            return 41;
        }

        /* V40 (#1839 total-blocked-wait gate): engine-agnostic column-existence sentinel, newest-first arm.
           config_alert_settings.blocking_wait_seconds_threshold exists only at V40 or later. The viewer MUST
           gate on it: its Settings window reads and upserts that column by name, so pointed at a V39 store it
           would fail the alert-settings read outright (42703) rather than degrade. */
        if (hasBlockingWaitThreshold)
        {
            return 40;
        }

        /* V39 (#1795 dimension GC measured bound): index-existence sentinel, newest-first arm — the
           same pg_indexes idiom as the V22 sentinel. ix_query_stats_digest_floor exists only at V39
           or later. The viewer itself never reads the index; the arm exists so a fully-migrated V39
           store maps to exactly RequiredStoreSchemaVersion instead of capping at 38 and tripping the
           connect-time gate against a perfectly healthy store. */
        if (hasDimFloorIndexes)
        {
            return 39;
        }

        /* V38 (#1767 query payload dimensions): table-existence sentinel, newest-first arm.
           query_plan_dim exists only at V38 or later. The viewer MUST gate on it: at V38 the
           collectors stop writing query_text/query_plan_xml inline, so a pre-V38 viewer pointed at a
           V38 store would read NULL for every new row's text and plan — no error, just a product
           that quietly shows nothing. */
        if (hasPayloadDimensions)
        {
            return 38;
        }

        /* V37 (#1696 AG disconnect re-fire): engine-agnostic column-existence sentinel, newest-first arm.
           config_alert_settings.ag_disconnect_refire_minutes exists only at V37 or later. */
        if (hasAgDisconnectRefire)
        {
            return 37;
        }

        /* V36 (#991 addendum, AG latency columns): engine-agnostic COLUMN-existence sentinel — V36 only
           widens the V34 table, so the table-existence arm below cannot distinguish the two. Newest-first. */
        if (hasAgLatencyColumns)
        {
            return 36;
        }

        /* V35 (#991 Availability Group alert knobs): engine-agnostic column-existence sentinel, newest-first
           arm. config_alert_settings.notify_ag_health exists only at V35 or later. */
        if (hasAgAlertKnobs)
        {
            return 35;
        }

        /* V34 (#991 Availability Group collectors): engine-agnostic table-existence sentinel, newest-first
           arm. The AG database-grain collector table exists only at V34 or later. */
        if (hasAgCollectors)
        {
            return 34;
        }

        /* V33 (#1659 connection-alert opt-ins): engine-agnostic column-existence sentinel, newest-first
           arm. config_alert_settings.notify_connection_down_at_startup exists only at V33 or later. */
        if (hasConnectionRefireKnobs)
        {
            return 33;
        }

        /* V32 (fleet tags): engine-agnostic table-existence sentinel, newest-first arm.
           config.server_tags exists only at V32 or later. Adding a migration WITHOUT adding an arm here
           is the trap this ladder exists to prevent: the probe would cap at the previous version, so a
           fully-migrated store would still read as older than RequiredStoreSchemaVersion and the
           connect-time gate would refuse to open the viewer — permanently, and deploying the service
           first would not help. */
        if (hasServerTags)
        {
            return 32;
        }

        /* V31 (#1563 custom views): engine-agnostic table-existence sentinel, newest-first arm.
           config.custom_views exists only at V31 or later. */
        if (hasCustomViews)
        {
            return 31;
        }

        /* V30 (#1562 web dashboard toggle): engine-agnostic column-existence sentinel, newest-first arm.
           config_service.web_enabled exists only at V30 or later. */
        if (hasWebDashboardConfig)
        {
            return 30;
        }

        /* V29 (#1496 long-query completion trace): engine-agnostic table-existence sentinel, newest-first
           arm. The long_query_completions table exists only at V29 or later. */
        if (hasLongQueryCompletions)
        {
            return 29;
        }

        /* V28 (#1546 Query Store replica attribution): engine-agnostic column-existence sentinel,
           newest-first arm. query_store_stats.replica_role exists only at V28 or later. */
        if (hasQueryStoreReplicaRole)
        {
            return 28;
        }

        /* V27 (#1535 Azure per-database deadlock capture): engine-agnostic column-existence sentinel,
           newest-first arm. deadlocks.database_name exists only at V27 or later. */
        if (hasDeadlocksDatabaseName)
        {
            return 27;
        }

        /* V26 (#1506 generic webhook): another engine-agnostic column-existence sentinel.
           config_notification.generic_url exists only at V26 or later. */
        if (hasGenericWebhook)
        {
            return 26;
        }

        /* V24/V25 (#1433 Job History tab) are engine-agnostic table-existence sentinels, so they ARE
           standalone newest-first arms (unlike the V23 hypertable composite): agent_status (V25) present →
           25, else job_history (V24) present → 24, else fall through to the V22/V23 composite below. A
           fully-migrated store has both. */
        if (hasAgentStatus)
        {
            return 25;
        }

        if (hasJobHistory)
        {
            return 24;
        }

        if (hasIndexObjectStatsLatestIndex)
        {
            /* V22's index is the newest ENGINE-AGNOSTIC sentinel and the floor for V23. V23's only schema
               effect — collection_log becoming a hypertable — is visible ONLY on a TimescaleDB store; on plain
               PostgreSQL V23 is a no-op, so a plain-PG store at V23 is object-identical to V22 and the composite
               is true via its no-extension arm (correctly reporting 23 — nothing for the viewer to gate on). On
               a Timescale store the hypertable distinguishes a real V23 from a store still at V22. */
            return hasCollectionLogHypertableOrPlainPg ? 23 : 22;
        }

        if (hasDefaultTraceEvents)
        {
            return 21;
        }

        if (hasAlertTuningKnobs)
        {
            return 20;
        }

        if (hasAnalysisState)
        {
            return 19;
        }

        if (hasAlertDeliveryOverride)
        {
            return 18;
        }

        if (hasConfigControlPlane)
        {
            return 17;
        }

        return 16;
    }

    /// <summary>
    /// #4530/#4597: the server's edition/MAXDOP/cost threshold/max memory for plan analysis, plus the
    /// database's name and compat level when <paramref name="databaseName"/> is known, for the viewer's own
    /// plan-opening sites (<see cref="PerformanceMonitor.Ui.PlanViewerControl.ServerMetadata"/>) — this app
    /// has no live connection to the monitored server at plan-view time, so it reads the collected copy the
    /// same way the MCP plan tools and the drill-downs do. Non-fatal: a missing row or a read failure
    /// returns null, same as <see cref="DarlingServerMetadataReader.ReadAsync"/> itself.
    /// </summary>
    public Task<PerformanceMonitor.PlanAnalysis.ServerMetadata?> GetPlanAnalysisServerMetadataAsync(
        int serverId, string? databaseName = null, CancellationToken cancellationToken = default) =>
        DarlingServerMetadataReader.ReadAsync(_dataSource, serverId, databaseName, cancellationToken);

    /// <summary>All registered servers, ordered as the server list displays them.</summary>
    public async Task<List<DarlingServer>> GetServersAsync(CancellationToken cancellationToken = default)
    {
        var servers = new List<DarlingServer>();

        await using var command = _dataSource.CreateCommand(ServersSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverName = reader.GetString(1);
            servers.Add(new DarlingServer(
                reader.GetInt32(0),
                serverName,
                reader.IsDBNull(2) ? serverName : reader.GetString(2),
                !reader.IsDBNull(3) && reader.GetBoolean(3),
                reader.IsDBNull(4) ? null : reader.GetInt32(4),
                reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5)),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? CollectorEngineCapability.UnknownEngineEdition : reader.GetInt32(7),
                reader.IsDBNull(8) ? null : reader.GetInt32(8),
                reader.IsDBNull(9) ? null : reader.GetDateTime(9)));
        }

        await AttachSameMachineAsync(servers, cancellationToken);
        await AttachAwsRdsAsync(servers, cancellationToken);
        return servers;
    }

    /// <summary>
    /// The active server's UTC offset in minutes from its most recent <c>server_properties</c> row — the
    /// value the Server-time display mode adds to the naive-UTC store (UTC + offset = server local). The
    /// naive-UTC → display conversion every rendered timestamp uses lives in <see cref="ViewerTimeHelper"/>
    /// (it replaced this class's former fixed <c>ToLocalTime</c>); this only fetches the per-server offset
    /// that mode feeds into it. Returns null when the column has not been collected yet (an older store
    /// just migrated, or a server whose properties collector has not re-run since the V16 upgrade); the
    /// caller then keeps the viewer's machine-local offset so Server mode degrades to ~Local until then.
    /// </summary>
    public const string ServerUtcOffsetSql = @"
SELECT utc_offset_minutes
FROM server_properties
WHERE server_id = $1
AND   utc_offset_minutes IS NOT NULL
ORDER BY collection_time DESC
LIMIT 1";

    public async Task<int?> GetServerUtcOffsetMinutesAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServerUtcOffsetSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null or DBNull
            ? null
            : Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// <see cref="ServerUtcOffsetSql"/> plus the zone: the newest <c>server_properties</c> row that has an
    /// offset, with its <c>time_zone_id</c> (V134 — <c>CURRENT_TIMEZONE_ID()</c>, a Windows zone id such as
    /// "Eastern Standard Time" on SQL Server 2022 and later, NULL before). Both columns come from the SAME
    /// row, so the id and the offset describe one snapshot. The zone is what lets the Server-time display
    /// mode follow a daylight-saving change; the offset is the fallback when the id is NULL or does not
    /// resolve on the viewer's machine (#4766).
    /// </summary>
    public const string ServerClockSql = @"
SELECT utc_offset_minutes, time_zone_id
FROM server_properties
WHERE server_id = $1
AND   utc_offset_minutes IS NOT NULL
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>
    /// The active server's clock (#4766): its time zone where the newest snapshot carries a resolvable id,
    /// else the snapshot's fixed offset. Returns null when no offset has been collected yet, so the caller
    /// keeps the viewer machine's offset. A store below V134 has no <c>time_zone_id</c> column (42703): that
    /// falls back to the offset-only read, exactly what this method did before the zone existed.
    /// </summary>
    public async Task<ServerClock?> GetServerClockAsync(int serverId, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = _dataSource.CreateCommand(ServerClockSql);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return ServerClock.Resolve(
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(0) ? null : reader.GetInt32(0));
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedColumn)
        {
            var offset = await GetServerUtcOffsetMinutesAsync(serverId, cancellationToken);
            return offset.HasValue ? ServerClock.FixedOffset(offset.Value) : null;
        }
    }

    /// <summary>
    /// Binds the standard per-server window parameters shared by the settable-window tab reads:
    /// $1 server_id, $2 window start, $3 window end. Both bounds are sent Kind=Unspecified because the
    /// store's columns are naive UTC (<c>timestamp without time zone</c>) — a Kind=Utc DateTime would
    /// map to timestamptz and throw since Npgsql 6.0 (see the class remarks).
    /// </summary>
    internal static void AddWindowParameters(NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
    }

    /// <summary>Postgres SQLSTATE 42501 (insufficient_privilege) — a write refused on a read-only connection.</summary>
    internal const string InsufficientPrivilegeSqlState = "42501";

    /// <summary>Postgres SQLSTATE 42703 (undefined_column) — a write referenced a column a lagging store
    /// has not migrated in yet (schema skew).</summary>
    internal const string UndefinedColumnSqlState = "42703";

    /// <summary>Postgres SQLSTATE 42P01 (undefined_table) — a write referenced a table a lagging store has
    /// not migrated in yet (schema skew).</summary>
    internal const string UndefinedTableSqlState = "42P01";

    /// <summary>
    /// Executes a write command, translating a permission-denied failure — a write attempted on the
    /// read-only viewer role, or after grants changed under a running app — into a
    /// <see cref="ViewerReadOnlyException"/> the UI shows as a clear "read-only connection" message
    /// instead of a raw Postgres error. The reactive backstop to the proactive hide/disable; any other
    /// failure propagates unchanged.
    /// </summary>
    internal async Task<int> ExecuteWriteAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        try
        {
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>
    /// The <c>RETURNING</c>-shaped sibling of <see cref="ExecuteWriteAsync"/>: runs a write that yields a
    /// scalar (e.g. the <c>config_command.command_id</c> a command enqueue returns), translating a 42501 on a
    /// read-only seat into <see cref="ViewerReadOnlyException"/> the same way. Any other failure propagates.
    /// </summary>
    internal async Task<object?> ExecuteWriteScalarAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        try
        {
            return await command.ExecuteScalarAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    public ValueTask DisposeAsync() => _dataSource.DisposeAsync();
}

/// <summary>
/// A write was attempted on a read-only Darling connection (the least-privilege <c>viewer</c> role,
/// or any connection lacking <c>config</c> INSERT). Thrown by the write paths when Postgres returns
/// 42501 so the UI shows a clear, actionable message rather than a raw permission error. The proactive
/// <see cref="ViewerDataService.IsReadOnly"/> gating normally hides the affordances first; this covers
/// the race where the probe and the live grants disagree.
/// </summary>
public sealed class ViewerReadOnlyException : Exception
{
    public ViewerReadOnlyException(Exception innerException)
        : base(
            "This viewer is connected with a read-only role, so it can't change mute rules, dismiss alerts, " +
            "or mute findings. Set postgres.connectAs to \"admin\" in darling.json and restart the viewer to " +
            "enable these actions.",
            innerException)
    {
    }
}

/// <summary>
/// The Darling store could not be reached at connect — the service is down, or the postgres section of
/// darling.json points at the wrong host/port/database (a connection-level failure, not a server error
/// response). Thrown by <see cref="ViewerDataService.EnsureStoreReachableAsync"/> and, as a backstop, by
/// <see cref="ViewerDataService.DetectReadOnlyAsync"/> so the shell shows a dedicated "is the service
/// running?" message instead of misreading an unreachable store as a read-only seat (finding B3).
/// </summary>
public sealed class ViewerStoreUnreachableException : Exception
{
    public ViewerStoreUnreachableException(Exception innerException)
        : base(
            "Can't reach the Darling store — is the Darling service running? Check the postgres section of " +
            "darling.json (the host, port, and database must point at the running service's store). " +
            /* #2117: the swallowed detail cost a field operator hours — a TLS chain rejection, a wrong
               password, a pg_hba refusal, and a dead host all read identically without it. */
            $"Underlying error: {innerException?.Message?.Split('\n')[0].TrimEnd('\r') ?? "(none)"}",
            innerException)
    {
    }
}

/// <summary>
/// A control-plane write referenced a column or table the connected store has not migrated in yet — the
/// store's schema is BEHIND the version this viewer build needs (Postgres 42703 undefined_column / 42P01
/// undefined_table). Thrown by the write executors so the UI shows a clear "update the service" message
/// instead of a raw "column ... does not exist" error (finding B2). The connect-time gate
/// (<see cref="ViewerDataService.GetStoreSchemaVersionAsync"/>, finding B1) normally blocks a skewed store
/// before any write; this is the write-path backstop for a column referenced ahead of a lagging store.
/// </summary>
public sealed class ViewerSchemaSkewException : Exception
{
    public ViewerSchemaSkewException(Exception innerException)
        : base(
            $"This viewer needs Darling store schema v{ViewerDataService.RequiredStoreSchemaVersion}, but the " +
            "store is missing an expected column or table. Update or restart the Darling service so it " +
            "migrates the store, then reopen the viewer.",
            innerException)
    {
    }
}
