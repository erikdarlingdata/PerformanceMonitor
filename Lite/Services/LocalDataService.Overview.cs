/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows.Media;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets a summary of server health for the overview dashboard.
    /// </summary>
    public async Task<ServerSummaryItem?> GetServerSummaryAsync(int serverId, string displayName)
    {
        using var connection = await OpenConnectionAsync();

        double? cpuPercent = null;
        double? otherProcessCpuPercent = null;
        double? memoryMb = null;
        int blockingCount = 0;
        int deadlockCount = 0;
        DateTime? lastCollection = null;

        /* Latest CPU — read both SQL Server CPU and other-process CPU so the UI can surface
           total non-idle CPU alongside the SQL-only number. */
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SELECT sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time
FROM v_cpu_utilization_stats
WHERE server_id = $1
ORDER BY sample_time DESC
LIMIT 1";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                cpuPercent = reader.IsDBNull(0) ? null : ToDouble(reader.GetValue(0));
                otherProcessCpuPercent = reader.IsDBNull(1) ? null : ToDouble(reader.GetValue(1));
                lastCollection = reader.IsDBNull(2) ? null : reader.GetDateTime(2);
            }
        }

        /* Latest SQL Memory */
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SELECT total_server_memory_mb
FROM v_memory_stats
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                memoryMb = reader.IsDBNull(0) ? null : ToDouble(reader.GetValue(0));
            }
        }

        /* Blocking count in last hour - uses XE blocked process reports */
        using (var cmd = connection.CreateCommand())
        {
            /* Prefer the blocked-process-report; fall back to the always-on DMV snapshot (AWS RDS). */
            cmd.CommandText = @"
SELECT COALESCE(NULLIF(
    (SELECT COUNT(*) FROM v_blocked_process_reports WHERE server_id = $1 AND event_time >= $2), 0),
    (SELECT COUNT(*) FROM v_dmv_blocking_snapshots WHERE server_id = $1 AND event_time >= $2))";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-1) });
            var result = await cmd.ExecuteScalarAsync();
            blockingCount = result != null ? Convert.ToInt32(result) : 0;
        }

        /* Deadlock count in last hour */
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SELECT COUNT(*)
FROM v_deadlocks
WHERE server_id = $1
AND   deadlock_time >= $2";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-1) });
            var result = await cmd.ExecuteScalarAsync();
            deadlockCount = result != null ? Convert.ToInt32(result) : 0;
        }

        /* Last collection time from collection_log */
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SELECT MAX(collection_time)
FROM v_collection_log
WHERE server_id = $1";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            var result = await cmd.ExecuteScalarAsync();
            if (result != null && result != DBNull.Value)
            {
                lastCollection = Convert.ToDateTime(result);
            }
        }

        var summary = new ServerSummaryItem
        {
            DisplayName = displayName,
            ServerId = serverId,
            CpuPercent = cpuPercent,
            OtherProcessCpuPercent = otherProcessCpuPercent,
            MemoryMb = memoryMb,
            BlockingCount = blockingCount,
            DeadlockCount = deadlockCount,
            LastCollectionTime = lastCollection
        };

        /* Band the collection's freshness HERE and not at the Overview loader, because this is the one
           place a ServerSummaryItem is built: the two MCP reads call this method too, and a band only the
           Overview applied would be a fact that depended on which caller asked. The clock is handed in
           rather than read inside the band, so the classification stays a pure function of
           (last collection, now) — the shape the viewer's ApplyFreshness already has. */
        summary.ApplyCollectionFreshness(DateTime.UtcNow);
        return summary;
    }
}

/// <summary>One tag pill on an Overview card (#2020 2b-i): the tag name plus the brushes to render it,
/// resolved once from the stored colour via <see cref="TagColorBrushes"/> (neutral when the tag has no
/// colour). The Lite twin of the viewer's ServerTagPill; the constant light label reads in either theme.</summary>
public sealed class ServerTagPill
{
    public ServerTagPill(string name, string? colour)
    {
        Name = name;
        Fill = TagColorBrushes.Fill(colour);
    }

    public string Name { get; }

    public System.Windows.Media.Brush Fill { get; }

    public System.Windows.Media.Brush TextBrush => TagColorBrushes.PillText;
}

/// <summary>
/// The Overview card's status-line state, as a VALUE rather than a rendered string. The word on the card
/// (<see cref="ServerSummaryItem.StatusDisplay"/>), the colour it is painted
/// (<see cref="ServerSummaryItem.StatusBrush"/>), the card's border
/// (<see cref="ServerSummaryItem.CardBorderBrush"/>), the offline overlay
/// (<see cref="ServerSummaryItem.IsOffline"/>) and the first line of its tooltip
/// (<see cref="ServerSummaryItem.StatusTooltip"/>) are all renderings OF THIS — the
/// (<c>IsOnline</c>, <c>HasCollectorErrors</c>) pair is read by <see cref="ServerCardStatusRules.Classify"/>
/// and nowhere else in Lite, so no two of them can land on different answers for the same server — including
/// the sidebar row's dot, which carried its own copy of this ladder until #2458 and could therefore disagree
/// with the card about a server both of them were showing. The Darling viewer's twin exists
/// because two review passes on #2429 each found a flag combination where two independent readings disagreed;
/// the word and the colour here already carried their own copy of the ladder, and the tooltip would have been
/// a third.
///
/// <para>Review on #2451 found the fifth: <c>CardBorderBrush</c> read <c>IsOffline</c> and
/// <c>HasCollectorErrors</c> raw, agreeing with the status word by coincidence rather than by construction.
/// It disagreed on one pair already — an unchecked card carrying a collector-error marker drew the amber
/// "collectors failing" border while its word read "Unknown". The Overview loader only sets that marker when
/// the connection check SUCCEEDED, so the pair is unreached in practice, which is exactly the argument for
/// making it unrepresentable rather than leaving it to a caller to keep avoiding.</para>
///
/// <para><b>The amber state is NOT the viewer's.</b> The viewer derives its status from collection freshness
/// and calls this member <c>Stale</c>; Lite's <c>IsOnline</c> comes from a live connection check and
/// <c>HasCollectorErrors</c> from <c>ErroringCollectors > 0</c> — collectors that are actually failing. Same
/// amber, same word "Warning", different cause. That is precisely why the tooltip has to say which one it is
/// rather than leaving the reader to infer it from a colour.</para>
/// </summary>
public enum ServerCardStatus
{
    /// <summary>The last connection check succeeded and no collector is erroring.</summary>
    Online,

    /// <summary>Connected, but one or more collectors have consecutive errors — the card's amber "Warning".
    /// Nothing to do with the metric rows, which is the whole reason it needs explaining.</summary>
    CollectorErrors,

    /// <summary>The last connection check failed — the card's red offline overlay.</summary>
    Offline,

    /// <summary>The server has not been connection-checked yet (<c>IsOnline</c> is null): a card built before
    /// the first check completed, not a dead server.</summary>
    Unknown,
}

/// <summary>
/// The ladder itself, in ONE function, plus the two renderings every surface shares. #2451 collapsed the
/// Overview card's word, colour, border, overlay and tooltip onto <see cref="ServerCardStatus"/>; #2458 found
/// the sidebar row's dot (<c>ServerConnection.DotStatus</c>) deriving the same four words from its own copy of
/// the same flag pair, on a different type, on a different surface — so the sidebar and the card could say
/// different things about one server and nothing would notice. They now read this.
///
/// <para><b>The word is a CONNECTION word on both surfaces, and that is deliberate.</b> #2457 kept collection
/// freshness out of it — freshness bands its own row on the card, in its own vocabulary — because #2429 found
/// the Darling viewer rendering one amber word for a stale collection AND for a metric breach with nothing
/// telling them apart, which is the card @ehaar wrote in about in #2422. So nothing here takes a freshness
/// argument, and it must not grow one: a green dot means the last connection check succeeded and says nothing
/// about whether anything is being collected. <see cref="Headline"/> is where the reader is told that.</para>
/// </summary>
public static class ServerCardStatusRules
{
    /// <summary>The (<c>IsOnline</c>, <c>HasCollectorErrors</c>) pair, resolved. The only reader of that pair
    /// in Lite — which is an assertion in <c>LiteOverviewCardExplainsItselfTests</c> rather than a claim in
    /// prose, because the prose version came to overstate what was true and had to be caught by review on
    /// #2451.</summary>
    public static ServerCardStatus Classify(bool? isOnline, bool hasCollectorErrors) => isOnline switch
    {
        true when hasCollectorErrors => ServerCardStatus.CollectorErrors,
        true => ServerCardStatus.Online,
        false => ServerCardStatus.Offline,
        _ => ServerCardStatus.Unknown
    };

    /// <summary>The four words. They are also the DataTrigger values the sidebar keys its dot colour off in
    /// <c>Lite/MainWindow.xaml</c>, so a fifth word added here would silently fall through to the muted
    /// default dot rather than fail anything.</summary>
    public static string Word(this ServerCardStatus status) => status switch
    {
        ServerCardStatus.CollectorErrors => "Warning",
        ServerCardStatus.Online => "Online",
        ServerCardStatus.Offline => "Offline",
        _ => "Unknown"
    };

    /// <summary>What the word means, in words — the first line of whichever tooltip renders it. Every arm names
    /// the connection check or the collectors explicitly, because the complaint in #2422 was precisely that a
    /// word and a colour left the reader guessing which axis they were about.</summary>
    public static string Headline(this ServerCardStatus status) => status switch
    {
        ServerCardStatus.CollectorErrors => "Warning — one or more collectors are failing on this server",
        ServerCardStatus.Online => "Online — the last connection check succeeded",
        ServerCardStatus.Offline => "Offline — the last connection check failed",
        _ => "Unknown — this server has not been connection-checked yet",
    };
}

public class ServerSummaryItem
{
    public string DisplayName { get; set; } = "";
    public string ServerName { get; set; } = "";
    public int ServerId { get; set; }
    public bool? IsOnline { get; set; }

    /// <summary>True when a whole-server alert silence is active for this server (#2031) — drives the card's
    /// muted-bell, mirroring the sidebar row. Stamped by the Overview loader and the silence-indicator
    /// refresh; the card rebinds on a real flip.</summary>
    public bool IsSilenced { get; set; }

    /// <summary>The server's tags as coloured pills, empty when it has none. Stamped by the Overview loader
    /// from the loaded tag list, so it survives a re-sort (which reuses these item instances).</summary>
    public System.Collections.Generic.IReadOnlyList<ServerTagPill> TagPills { get; set; } =
        System.Array.Empty<ServerTagPill>();
    /// <summary>True when the server is reachable but one or more collectors have consecutive errors.</summary>
    public bool HasCollectorErrors { get; set; }
    /// <summary>SQL Server scheduler ProcessUtilization from sys.dm_os_ring_buffers. NULL on Azure SQL DB.</summary>
    public double? CpuPercent { get; set; }
    /// <summary>Non-SQL-Server CPU on the host (computed as 100 - SystemIdle - ProcessUtilization). NULL on Azure SQL DB.</summary>
    public double? OtherProcessCpuPercent { get; set; }
    /// <summary>Total non-idle CPU on the host = sql_server + other_process. Tracks closer to OS user+system counters.</summary>
    public double? TotalCpuPercent =>
        CpuPercent.HasValue ? CpuPercent.Value + (OtherProcessCpuPercent ?? 0) : null;
    /// <summary>The CPU value the alert evaluator and headline display use. Driven by App.AlertCpuMode.</summary>
    public double? CpuPercentForAlert =>
        App.AlertCpuMode == CpuAlertMode.Total ? (TotalCpuPercent ?? CpuPercent) : CpuPercent;
    public double? MemoryMb { get; set; }
    public int BlockingCount { get; set; }
    public int DeadlockCount { get; set; }
    public DateTime? LastCollectionTime { get; set; }

    /// <summary>
    /// Headline CPU display. Shows total non-idle CPU prominently with the SQL-only number alongside,
    /// e.g. "64% (SQL 60%)". Falls back to a single number when only one value is available.
    /// </summary>
    public string CpuDisplay
    {
        get
        {
            if (!CpuPercent.HasValue) return "--";
            if (!OtherProcessCpuPercent.HasValue) return $"{CpuPercent:F0}%";
            return $"{TotalCpuPercent:F0}% (SQL {CpuPercent:F0}%)";
        }
    }
    public string MemoryDisplay => MemoryMb.HasValue ? $"{MemoryMb / 1024.0:F1} GB" : "--";
    public string BlockingDisplay => BlockingCount > 0 ? BlockingCount.ToString() : "0";
    public string DeadlockDisplay => DeadlockCount > 0 ? DeadlockCount.ToString() : "0";

    /// <summary>
    /// How old the newest collection_log row for this server is, banded by the SHARED
    /// <see cref="ServerHealthClassifier.ClassifyFreshness"/> — the same Fresh / Stale / Offline /
    /// NeverCollected ladder the Darling viewer bands, off the same <see cref="ServerHealthThresholds"/>
    /// numbers, so the two SKUs cannot drift on when a collection has gone quiet (#2452).
    ///
    /// <para>NULL means nobody classified it. <see cref="ApplyCollectionFreshness"/> always produces a band
    /// and every ServerSummaryItem the app builds goes through it, so null is unreachable in the shipped
    /// app; a hand-built item can still produce one, which is why it is a named "not classified" rather
    /// than a fall-through onto a band that would be a guess. It renders as the card's existing unknown
    /// grey with no band word — exactly what this row rendered before it had a band.</para>
    /// </summary>
    public ServerFreshness? CollectionFreshness { get; set; }

    /// <summary>
    /// Stamp <see cref="CollectionFreshness"/> from this card's own <see cref="LastCollectionTime"/>. Pure
    /// over (last collection, now): both instants are UTC (the DuckDB store is naive UTC and
    /// <paramref name="nowUtc"/> is <see cref="DateTime.UtcNow"/>), so the subtraction inside the classifier
    /// is a true elapsed time regardless of Kind. Stamped rather than computed on read so the band cannot
    /// change between the row's brush binding and the tooltip that explains it.
    /// </summary>
    public void ApplyCollectionFreshness(DateTime nowUtc) =>
        CollectionFreshness = ServerHealthClassifier.ClassifyFreshness(LastCollectionTime, nowUtc);

    /// <summary>
    /// The Last Collect row. Names its band in words when the collection is not current, because a colour
    /// alone is what this row already had against it: it was a bare "HH:mm:ss" in the plain foreground
    /// brush, so a timestamp from four hours ago looked exactly like one from four seconds ago and nothing
    /// on the card asked anyone to read it (#2452).
    /// </summary>
    public string LastCollectionDisplay
    {
        get
        {
            if (!LastCollectionTime.HasValue) return "Never";

            var stamp = ServerTimeHelper.FormatServerTime(LastCollectionTime, "HH:mm:ss");
            return CollectionFreshness switch
            {
                ServerFreshness.Stale => $"{stamp} (stale)",
                ServerFreshness.Offline => $"{stamp} (stopped)",
                _ => stamp,
            };
        }
    }

    /// <summary>
    /// The Last Collect row's band brush — the row's own palette, alongside CPU / Blocking / Deadlocks,
    /// rather than the neutral foreground it used to be painted in. NeverCollected is amber, never red: a
    /// server that has not been collected yet is queued, not dead, and red there sent a 24-server field
    /// report chasing a phantom scheduler bug on the viewer side (see
    /// <see cref="ServerFreshness.NeverCollected"/>). An unclassified band gets the card's unknown grey.
    /// </summary>
    public SolidColorBrush LastCollectionBrush => MakeBrush(CollectionFreshness switch
    {
        ServerFreshness.Fresh => "#81C784",
        ServerFreshness.Stale => "#FFB74D",
        ServerFreshness.Offline => "#E57373",
        ServerFreshness.NeverCollected => "#FFB74D",
        _ => "#888888",
    });

    /// <summary>
    /// What the Last Collect row's band MEANS, for the tooltip that row carries.
    ///
    /// <para>The second sentence is the point of the wording rather than padding. #2429 found the Darling
    /// viewer says the same word — "Warning" — for a stale collection and for a metric breach, with nothing
    /// on the card telling them apart; that was the card @ehaar wrote in about (#2422). So Lite says which
    /// axis it is about, in words, on the row the band is on. It is deliberately NOT folded into the status
    /// word: see the note on <see cref="CardStatus"/>.</para>
    ///
    /// <para>The thresholds are read from <see cref="ServerHealthThresholds"/> rather than typed, so the
    /// sentence cannot come to disagree with the band it is explaining.</para>
    /// </summary>
    public string? CollectionFreshnessTooltip => CollectionFreshness switch
    {
        ServerFreshness.Fresh =>
            $"Collection is current — something landed within the last {Minutes(ServerHealthThresholds.StaleThreshold)}.",
        ServerFreshness.Stale =>
            $"Collection is stale — nothing has landed for over {Minutes(ServerHealthThresholds.StaleThreshold)}.\n" +
            "This is about collection stopping, not about the server's metrics: the connection check can " +
            "still be passing and no collector reporting an error.",
        ServerFreshness.Offline =>
            $"Collection has stopped — nothing has landed for over {Minutes(ServerHealthThresholds.OfflineThreshold)}.\n" +
            "This is about collection stopping, not about the server's metrics: the connection check can " +
            "still be passing and no collector reporting an error.",
        ServerFreshness.NeverCollected =>
            "No collection has ever landed for this server.\n" +
            "This is about collection, not about the server's metrics.",
        _ => null,
    };

    /// <summary>Renders a threshold as "2 minutes" / "15 minutes" so the tooltip quotes the shared numbers
    /// instead of restating them.</summary>
    private static string Minutes(TimeSpan span) =>
        $"{span.TotalMinutes:0.#} minute{(Math.Abs(span.TotalMinutes - 1) < 0.001 ? "" : "s")}";

    /* Connection status. The (IsOnline, HasCollectorErrors) pair is resolved by ServerCardStatusRules.Classify
       and nowhere else in Lite — the sidebar row's dot carried its own copy until #2458 — and the word, the
       colour and the tooltip's first line all render the result. See ServerCardStatus.

       Collection freshness is deliberately NOT one of the states here, and that is option 2 in #2452 being
       turned down. This word is a CONNECTION word: IsOnline comes from ServerManager.GetConnectionStatus, a
       live check that succeeds whether or not anything is being collected, and CollectorErrors already means
       one specific thing. Folding freshness in would make one amber word mean two unrelated failures, which
       is the conflation #2429 spent four review rounds untangling on the viewer and the reason #2422 was
       written. Freshness is a third axis: it bands its own row, and the tooltip names it there. */
    public ServerCardStatus CardStatus => ServerCardStatusRules.Classify(IsOnline, HasCollectorErrors);

    public string StatusDisplay => CardStatus.Word();
    public SolidColorBrush StatusBrush => MakeBrush(CardStatus switch
    {
        ServerCardStatus.CollectorErrors => "#FFD54F",  // amber — connected but collectors failing
        ServerCardStatus.Online => "#81C784",
        ServerCardStatus.Offline => "#E57373",
        _ => "#888888"
    });
    public bool IsOffline => CardStatus == ServerCardStatus.Offline;

    /* ── Per-row concern gates ───────────────────────────────────────────────────────────────────────
       Each metric row's "this is not green" test, evaluated ONCE. The row's own brush reads it and so does
       StatusReason, which is what makes the tooltip incapable of naming a metric whose row is green — or of
       staying silent about one that is not. A tooltip built from an independent severity calculation would
       lose exactly that property, and the one place it would disagree is a card the reader is staring at.

       Note the gates follow the ROW, not the card border: the CPU row goes amber at 50 while the border only
       escalates at 80. The reason names rows, so it uses the row's threshold. */
    private bool CpuIsElevated => CpuPercentForAlert >= 50;
    private bool CpuIsCritical => CpuPercentForAlert >= 80;
    private bool BlockingIsElevated => BlockingCount > 0;
    private bool DeadlocksAreElevated => DeadlockCount > 0;

    /// <summary>The Last Collect row's gate — stale, stopped, or never started (#2452). Public because the
    /// card border reads it too; the other three are private only because nothing outside needed them. Like
    /// them it is the row's OWN "not green" test, so the border, the row brush and the tooltip cannot come
    /// to disagree about whether this card's collection is in trouble.</summary>
    public bool CollectionIsNotFresh =>
        CollectionFreshness is ServerFreshness.Stale or ServerFreshness.Offline or ServerFreshness.NeverCollected;

    /* Color coding */
    public SolidColorBrush CpuBrush => MakeBrush(CpuIsCritical ? "#E57373" : CpuIsElevated ? "#FFB74D" : "#81C784");
    public SolidColorBrush BlockingBrush => MakeBrush(BlockingIsElevated ? "#FFB74D" : "#81C784");
    public SolidColorBrush DeadlockBrush => MakeBrush(DeadlocksAreElevated ? "#E57373" : "#81C784");
    /* The border ranks the same states the status word names, so it reads the SAME discriminant rather than
       the flags behind it — see ServerCardStatus for the pair the raw reads disagreed on. The precedence is
       unchanged: a dark server first, then the metric rows worst-first, then failing collectors. */
    public SolidColorBrush CardBorderBrush => MakeBrush(
        CardStatus == ServerCardStatus.Offline ? "#E57373" :
        DeadlocksAreElevated ? "#E57373" :
        BlockingIsElevated ? "#FFB74D" :
        CpuIsCritical ? "#FFB74D" :
        CardStatus == ServerCardStatus.CollectorErrors ? "#FFD54F" :   // amber border when collectors are failing
        /* Collector errors and collector SILENCE are the same class of fault — the monitoring of this server
           is not working — so they get the same amber, on their own arm rather than folded into the one
           above. Two named causes reading one colour is the honest shape here, and it keeps #2451's pin on
           the CollectorErrors arm reading the discriminant it was written to pin.

           Silence is the harder of the two to notice, which is the whole of #2452: nothing on this card
           moved when a server went quiet. It stays amber even for a STOPPED collection, because the red
           border belongs to a dark server and a red border under a green "Online" word would be the loudest
           contradiction on the card; the row underneath carries the severity in its own colour and its own
           word. */
        CollectionIsNotFresh ? "#FFD54F" :
        "#2a2d35");

    /* ── The card explains itself (#2437 / #2422) ────────────────────────────────────────────────────
       Reported against the Darling viewer and fixed there in #2429; Lite's card had the identical bare
       status binding. A reader saw a word and a colour and had to scan the metric rows guessing which one
       the card meant — once per card, on every card.

       Lite's version has one more thing to say than the viewer's, because Lite's status word is a CONNECTION
       word, not a health band: a card with 96% CPU and two deadlocks still says "Online" in green while its
       border is red, and a card saying "Warning" in amber is reporting failing COLLECTORS and nothing about
       the metrics at all. Fusing the two into one clause would reproduce the ambiguity in prose, so the
       tooltip keeps them on separate lines: what the status word means, then what the rows say. */

    /// <summary>What the status word means, in words — the tooltip's first line. Renders
    /// <see cref="CardStatus"/>, the same discriminant <see cref="StatusDisplay"/> renders, so a tooltip that
    /// hangs off a word can never contradict it — and since #2458 it is the SAME sentence the sidebar dot
    /// shows for that state, so a reader moving between the two surfaces meets one vocabulary.</summary>
    private string StatusHeadline => CardStatus.Headline();

    /// <summary>
    /// The metric rows that are not green, in the card's OWN display strings — "CPU 96% (SQL 90%), Deadlocks 2".
    /// Assembled from the displays rather than from the underlying numbers so the sentence and the rows cannot
    /// render differently, and gated on the same predicates the row brushes use so it cannot name a different
    /// set of rows than the ones that are coloured. Empty when every row is green.
    ///
    /// <para>Memory is absent deliberately: the Memory row carries no severity brush on this card, so there is
    /// no band to report. Inventing a threshold here would be the one thing this property exists to prevent —
    /// a tooltip asserting something the rows underneath it do not.</para>
    ///
    /// <para>Last Collect joined the list when it gained a band (#2452), and it goes FIRST. It is the row
    /// that says whether the other four can be believed at all: a card whose collection stopped four hours
    /// ago is showing four-hour-old CPU, and reading "CPU 4%" before learning that is the wrong order to
    /// meet the two facts in. It quotes <see cref="LastCollectionDisplay"/> verbatim, like every other part
    /// here, so the clause and the row cannot render different things — and because that display already
    /// carries the band word, the clause says which axis it is about without borrowing a metric's
    /// vocabulary.</para>
    /// </summary>
    public string StatusReason
    {
        get
        {
            var parts = new List<string>();

            if (CollectionIsNotFresh)
            {
                parts.Add("Last collect " + LastCollectionDisplay);
            }

            if (CpuIsElevated)
            {
                parts.Add("CPU " + CpuDisplay);
            }

            if (BlockingIsElevated)
            {
                parts.Add("Blocking " + BlockingDisplay);
            }

            if (DeadlocksAreElevated)
            {
                parts.Add("Deadlocks " + DeadlockDisplay);
            }

            return string.Join(", ", parts);
        }
    }

    /// <summary>
    /// The Overview card's status tooltip (#2437, answering #2422 on Lite's surface): what the status word
    /// means, what the metric rows say, and what to do next.
    ///
    /// <para>The metric line is omitted on an Offline card. The card draws a dimming overlay across those rows
    /// precisely because the numbers under it are the last ones collected before the server went dark, so
    /// demanding attention for them would contradict the card while the reader is looking at it.</para>
    /// </summary>
    public string StatusTooltip
    {
        get
        {
            var lines = new List<string> { StatusHeadline };

            if (CardStatus != ServerCardStatus.Offline)
            {
                var reason = StatusReason;
                lines.Add(reason.Length > 0
                    ? "Needs attention: " + reason
                    : "Every metric on this card is inside its threshold");
            }

            lines.Add(CardTooltipAction);

            return string.Join("\n", lines);
        }
    }

    /// <summary>The line every card tooltip ends on — the gesture the card actually supports.
    /// <c>OverviewCard_MouseLeftButtonDown</c> acts only on <c>ClickCount == 2</c>, so naming a single click
    /// would be naming a no-op. The viewer's card tooltip ends on this same sentence, so a reader moving
    /// between the two apps is told the same thing in the same words.</summary>
    private const string CardTooltipAction = "Double-click the card to open this server's tab";

    private static SolidColorBrush MakeBrush(string hex)
    {
        var color = (Color)ColorConverter.ConvertFromString(hex);
        var brush = new SolidColorBrush(color);
        brush.Freeze();
        return brush;
    }
    public bool HasAlerts => BlockingCount > 0 || DeadlockCount > 0;
}
