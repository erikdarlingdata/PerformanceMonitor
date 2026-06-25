using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// The single source of the blocked-process-report pair-row query used to reconstruct blocking chains.
/// Shared by the three Dashboard consumers — the drill-down collector, the BLOCKING_CHAIN fact collector,
/// and the viewer's data-service fetch — so the apex-determining <c>blocking_spid IS NOT NULL</c> filter
/// (and the SELECT/ordinals) stay in lockstep. <c>activity = 'blocked'</c> picks the canonical per-event
/// side; <c>blocking_spid IS NOT NULL</c> drops rows whose source XML had an empty
/// <c>&lt;blocking-process&gt;&lt;process/&gt;&lt;/blocking-process&gt;</c> (system task / torn-down session),
/// which cannot contribute to a chain.
/// </summary>
internal static class BlockingPairRowQuery
{
    private const string ReadUncommitted = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;\n";

    /// <summary>
    /// The core pair SELECT (no SET preamble; ordinals 0-14). Kept separate from <see cref="Sql"/> so the
    /// viewer variant <see cref="SqlWithBlockerIdentity"/> can reuse it verbatim as a derived table — the
    /// column list and the apex filter live in exactly one place and can't drift between the two queries.
    /// Ordinals 11-13 are the BLOCKED row's own identity (login/host/app); ordinal 14 is the contended
    /// object (resolved by the collector / sp_HumanEventsBlockViewer). The blocker's identity is not stored
    /// on the blocked row, so a pure apex has no identity from this query alone (see the variant).
    /// </summary>
    public const string SelectBody = @"
SELECT TOP (5000)
    event_time,
    database_name,
    spid,
    last_transaction_started,
    blocking_spid,
    blocking_last_tran_started,
    wait_time_ms,
    lock_mode,
    blocking_status,
    blocked_sql_text,
    blocking_sql_text,
    login_name,
    host_name,
    client_app,
    contentious_object,
    ecid,
    blocking_ecid,
    monitor_loop
FROM collect.blocking_BlockedProcessReport
WHERE collection_time >= @collectionWindow
AND   event_time >= @startTime
AND   event_time <= @endTime
AND   activity = 'blocked'
AND   blocking_spid IS NOT NULL
ORDER BY collection_time DESC";

    /// <summary>The collector/fact query: the core SELECT under READ UNCOMMITTED. Maps via <see cref="Read"/>.</summary>
    public const string Sql = ReadUncommitted + SelectBody;

    /// <summary>
    /// Viewer-only variant that also resolves the BLOCKER's identity (login / host / client app) per pair,
    /// so the apex node — which only ever appears as a blocker, never as a blocked row — shows WHO it is
    /// instead of a bare SPID. The Dashboard table stores identity per process row, so the blocker's lives
    /// on its <c>activity='blocking'</c> row for the same event; we correlate to it by (event_time, spid)
    /// via OUTER APPLY. TOP (1) collapses the one-to-many 'blocking' rows a lead blocker emits when it
    /// blocks several victims in one report (identity is identical across them). The apply is bounded by the
    /// same <c>collection_time</c> window as the core query, so the clustered PK (collection_time,
    /// blocking_id) range-limits the lookup. The background collectors keep the lighter <see cref="Sql"/>
    /// (they score chains and never display identity), so this extra correlation is paid only on the
    /// infrequent, small-window viewer open. Maps via <see cref="ReadWithBlockerIdentity"/> (adds 15-17).
    /// Result row order is intentionally unspecified: the inner TOP (5000) / ORDER BY selects the
    /// most-recent pairs, reconstruction is order-independent, and the sole caller's maxPairs equals the
    /// cap — so no outer ORDER BY is added (and collection_time is deliberately not projected, so don't
    /// "fix" this with ORDER BY c.collection_time — it isn't a column of the derived table).
    /// </summary>
    public const string SqlWithBlockerIdentity = ReadUncommitted + @"
SELECT
    c.*,
    blocking_login_name = bk.login_name,
    blocking_host_name = bk.host_name,
    blocking_client_app = bk.client_app
FROM
(" + SelectBody + @"
) AS c
OUTER APPLY
(
    SELECT TOP (1)
        k.login_name,
        k.host_name,
        k.client_app
    FROM collect.blocking_BlockedProcessReport AS k
    WHERE k.activity = 'blocking'
    AND   k.spid = c.blocking_spid
    AND   k.event_time = c.event_time
    AND   k.collection_time >= @collectionWindow
    ORDER BY k.collection_time DESC
) AS bk";

    /// <summary>
    /// Adds the three parameters. The collection-time floor is a generous bound (window start minus an
    /// hour) so rows whose event_time is inside the window but whose collection_time lags slightly are
    /// still caught. Shared by both <see cref="Sql"/> and <see cref="SqlWithBlockerIdentity"/> (the apply
    /// reuses @collectionWindow to bound its own lookup).
    /// </summary>
    public static void AddParameters(SqlCommand cmd, DateTime start, DateTime end)
    {
        cmd.Parameters.Add(new SqlParameter("@collectionWindow", start.AddHours(-1)));
        cmd.Parameters.Add(new SqlParameter("@startTime", start));
        cmd.Parameters.Add(new SqlParameter("@endTime", end));
    }

    /// <summary>
    /// Maps ordinals 0-13 of the core query: per-event fields plus the BLOCKED row's own identity. The
    /// blocking-side identity stays empty here — the Dashboard table parses only the blocker's
    /// status/tran/SQL from the report XML, not its login/host/app — so a pure apex read via the collector
    /// path shows SQL + db but no identity. The viewer fills it via <see cref="ReadWithBlockerIdentity"/>.
    /// </summary>
    public static BlockingPairRow Read(DbDataReader reader) => new()
    {
        EventTime = reader.IsDBNull(0) ? default : reader.GetDateTime(0),
        DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
        BlockedSpid = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
        BlockedTranStarted = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3),
        BlockingSpid = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
        BlockingTranStarted = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5),
        WaitTimeMs = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
        LockMode = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
        BlockingStatus = reader.IsDBNull(8) ? string.Empty : reader.GetString(8),
        BlockedSqlText = reader.IsDBNull(9) ? string.Empty : reader.GetString(9),
        BlockingSqlText = reader.IsDBNull(10) ? string.Empty : reader.GetString(10),
        BlockedLoginName = reader.IsDBNull(11) ? string.Empty : reader.GetString(11),
        BlockedHostName = reader.IsDBNull(12) ? string.Empty : reader.GetString(12),
        BlockedClientApp = reader.IsDBNull(13) ? string.Empty : reader.GetString(13),
        ContentiousObject = reader.IsDBNull(14) ? string.Empty : reader.GetString(14),
        // 15-17: spid:ecid + monitor_loop typed columns (populated at collect time by install/23). The
        // collector reconstructs cumulatively (scopeByMonitorLoop:false) so MonitorLoop is read but ignored;
        // the viewer scopes by it.
        BlockedEcid = reader.IsDBNull(15) ? 0 : Convert.ToInt32(reader.GetValue(15)),
        BlockingEcid = reader.IsDBNull(16) ? 0 : Convert.ToInt32(reader.GetValue(16)),
        MonitorLoop = reader.IsDBNull(17) ? (int?)null : Convert.ToInt32(reader.GetValue(17))
    };

    /// <summary>
    /// Maps the viewer's <see cref="SqlWithBlockerIdentity"/> result: the core <see cref="Read"/> (0-17)
    /// plus the correlated blocker identity at ordinals 18-20, so the apex node carries login/host/app.
    /// </summary>
    public static BlockingPairRow ReadWithBlockerIdentity(DbDataReader reader)
    {
        var row = Read(reader);
        row.BlockingLoginName = reader.IsDBNull(18) ? string.Empty : reader.GetString(18);
        row.BlockingHostName = reader.IsDBNull(19) ? string.Empty : reader.GetString(19);
        row.BlockingClientApp = reader.IsDBNull(20) ? string.Empty : reader.GetString(20);
        return row;
    }
}
