using System;
using System.Data.Common;
using System.Numerics;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Shared pieces of the blocked-process-report pair-row query used to reconstruct blocking chains, so
/// Lite's three consumers — the drill-down collector, the BLOCKING_CHAIN fact collector, and the viewer's
/// data-service fetch — agree on the apex AND on the column order the shared <see cref="Read"/> depends on.
///
/// <para><see cref="SpidFilter"/> is the behavioral fix: Lite maps a missing blocker to spid 0 (a phantom
/// root); without filtering it out, the fact/drill-down/viewer would each invent a SPID-0 apex. This brings
/// Lite in line with Dashboard's long-standing <c>blocking_spid IS NOT NULL</c>.</para>
///
/// <para><see cref="LeadingColumns"/> is the single source of truth for ordinals 0-8 so the three queries
/// can't drift and silently feed <see cref="Read"/> mismapped columns. Only the two SQL-text expressions
/// (ordinals 9-10) stay per-site — drill-down truncates with <c>LEFT(...,500)</c>; the fact collector and
/// the viewer fetch select the full text.</para>
/// </summary>
internal static class BlockingPairRowQuery
{
    /// <summary>Ordinals 0-8 of the pair-row SELECT. Each query appends its own SQL-text expressions (9-10).</summary>
    public const string LeadingColumns = @"event_time,
    database_name,
    blocked_spid,
    blocked_last_tran_started,
    blocking_spid,
    blocking_last_tran_started,
    wait_time_ms,
    lock_mode,
    blocking_status";

    /// <summary>Ordinals 11-16: session identity for each side. Appended after the per-site SQL-text
    /// expressions so all three queries share one column order for <see cref="Read"/>. Lite denormalizes
    /// both sides, so unlike Dashboard the apex's identity is available too.</summary>
    public const string IdentityColumns = @"blocked_login_name,
    blocked_host_name,
    blocked_client_app,
    blocking_login_name,
    blocking_host_name,
    blocking_client_app";

    /// <summary>Ordinals 18-20: spid:ecid + monitor_loop, the reconstruction's session identity. Every
    /// pair-row site appends this after contentious_object (ordinal 17) so the shared <see cref="Read"/> sees
    /// one column order across all three call sites.</summary>
    public const string TrailingIdentityColumns = @"blocked_ecid,
    blocking_ecid,
    monitor_loop";

    /// <summary>Append to the WHERE clause of every pair-row query (covers NULL and the 0 sentinel).</summary>
    public const string SpidFilter = @"AND blocking_spid IS NOT NULL
AND blocking_spid <> 0";

    public static BlockingPairRow Read(DbDataReader reader) => new()
    {
        EventTime = reader.IsDBNull(0) ? default : reader.GetDateTime(0),
        DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
        BlockedSpid = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
        BlockedTranStarted = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
        BlockingSpid = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
        BlockingTranStarted = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
        WaitTimeMs = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6)),
        LockMode = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
        BlockingStatus = reader.IsDBNull(8) ? string.Empty : reader.GetString(8),
        BlockedSqlText = reader.IsDBNull(9) ? string.Empty : reader.GetString(9),
        BlockingSqlText = reader.IsDBNull(10) ? string.Empty : reader.GetString(10),
        BlockedLoginName = reader.IsDBNull(11) ? string.Empty : reader.GetString(11),
        BlockedHostName = reader.IsDBNull(12) ? string.Empty : reader.GetString(12),
        BlockedClientApp = reader.IsDBNull(13) ? string.Empty : reader.GetString(13),
        BlockingLoginName = reader.IsDBNull(14) ? string.Empty : reader.GetString(14),
        BlockingHostName = reader.IsDBNull(15) ? string.Empty : reader.GetString(15),
        BlockingClientApp = reader.IsDBNull(16) ? string.Empty : reader.GetString(16),
        // Ordinal 17: the contended object (every pair-row site appends contentious_object after IdentityColumns).
        ContentiousObject = reader.IsDBNull(17) ? string.Empty : reader.GetString(17),
        // 18-20: spid:ecid + monitor_loop (every site appends TrailingIdentityColumns after contentious_object).
        BlockedEcid = reader.IsDBNull(18) ? 0 : Convert.ToInt32(reader.GetValue(18)),
        BlockingEcid = reader.IsDBNull(19) ? 0 : Convert.ToInt32(reader.GetValue(19)),
        MonitorLoop = reader.IsDBNull(20) ? (int?)null : Convert.ToInt32(reader.GetValue(20))
    };

    /// <summary>
    /// BigInteger-tolerant long conversion — DuckDB can hand wide / aggregate values back boxed as a
    /// <see cref="BigInteger"/>, which is not <see cref="IConvertible"/>, so plain Convert.ToInt64 throws.
    /// The single home for the idiom every Lite numeric reader uses (DuckDbFactCollector delegates here).
    /// </summary>
    public static long ToInt64(object value)
    {
        if (value is BigInteger bi)
            return (long)bi;
        return Convert.ToInt64(value);
    }
}
