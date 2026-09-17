/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The identity half of a Long-Running Query card's Agent-job annotation (#3497): which msdb job and
/// which step a session's <c>program_name</c> says it is running under. Parsed once, engine-side, so
/// the resolver call and the card render agree about what was asked for.
/// </summary>
/// <param name="JobId">msdb's <c>sysjobs.job_id</c>, recovered from the program name's hex form —
/// see <see cref="AgentJobStepQuery.TryParseProgramName"/> for the byte-order contract.</param>
/// <param name="StepId">The 1-based step number from the program name's <c>Step n</c> suffix.</param>
public readonly record struct AgentJobStepKey(Guid JobId, int StepId);

/// <summary>
/// The name half: what one msdb lookup answered for an <see cref="AgentJobStepKey"/>.
/// <see cref="StepName"/> is null when <c>sysjobsteps</c> had no row for the step id — a step
/// deleted or renumbered since the session started — and the card then names the job and the step
/// NUMBER without inventing a step name.
/// </summary>
public sealed record AgentJobStepNames(string JobName, string? StepName);

/// <summary>
/// The Long-Running Query card's Agent-job annotation feed (#3495's sibling, #3497): the
/// <c>program_name</c> parse and the live-msdb name lookup behind
/// <c>Running under Agent job: &lt;job name&gt;, step &lt;n&gt; (&lt;step name&gt;)</c>.
///
/// <para><b>Annotation, never suppression.</b> Nothing here gates, re-tiers, or filters an alert:
/// the thresholds stay where they are and every card still fires. The annotation states what IS —
/// this session is a SQL Agent job step, and here is the job's name — and never a verdict; a
/// maintenance job running long during its own window is routine and the same job at noon is a
/// finding, and that judgment belongs to the reader, who can only make it in one glance if the card
/// names the job.</para>
///
/// <para>This is NOT a collected read, exactly like <see cref="FailedJobsQuery"/> one file over:
/// job names live in the monitored server's msdb, not in any collected table, so hosts run
/// <see cref="BuildSql"/> on their own connections at fire time with their own permission gating,
/// and degrade EVERY failure — a login without SELECT on the msdb job tables, a transient fault, a
/// job deleted between capture and fire — to an empty map, which the card renders as the unresolved
/// form carrying the raw job-id marker. A failed lookup can cost the card a job NAME; it can never
/// cost the card.</para>
/// </summary>
public static class AgentJobStepQuery
{
    /// <summary>
    /// The machine-emitted prefix of a T-SQL job step's session (<c>sys.dm_exec_sessions.program_name</c>).
    /// Matched with <see cref="StringComparison.Ordinal"/> deliberately: SQL Agent emits exactly this
    /// casing, and a case-fuzzy match would start annotating strings a human merely typed to look like it.
    /// The shared collector's CDC filter matches the same prefix server-side
    /// (<c>QuerySnapshotsCollector</c>: <c>LIKE N'SQLAgent - TSQL JobStep (Job 0x%'</c>), so the two
    /// recognizers cannot disagree about what an Agent session looks like.
    /// </summary>
    public const string ProgramNamePrefix = "SQLAgent - TSQL JobStep (Job 0x";

    /// <summary>binary(16) rendered as hex — always 32 characters in the program name.</summary>
    private const int JobIdHexLength = 32;

    private const string StepSeparator = " : Step ";

    /// <summary>
    /// Parses the standard <c>SQLAgent - TSQL JobStep (Job 0x&lt;hex&gt; : Step &lt;n&gt;)</c> form.
    /// False for anything else — including SQL Agent's non-job-step sessions and malformed lookalikes
    /// (wrong hex length, non-hex characters, a missing step suffix) — so a parse failure degrades to
    /// no annotation rather than a wrong one.
    ///
    /// <para><b>The byte order is the load-bearing part.</b> The hex in <c>program_name</c> is msdb's
    /// <c>job_id</c> (a <c>uniqueidentifier</c>) CONVERTed to <c>binary(16)</c>: SQL Server's GUID
    /// storage layout, Data1/Data2/Data3 little-endian and the final eight bytes in order — job_id
    /// <c>AB6D9F63-3B01-4E15-9F34-B0A0F0B355A2</c> renders as <c>0x639F6DAB013B154E9F34B0A0F0B355A2</c>.
    /// .NET's <see cref="Guid(byte[])"/> constructor reads EXACTLY that layout (and
    /// <see cref="Guid.ToByteArray()"/> emits it), so no byte swapping happens here; the repo's prior
    /// art does the same conversion server-side via
    /// <c>TRY_CONVERT(uniqueidentifier, TRY_CONVERT(binary(16), SUBSTRING(program_name, 32, 32), 2))</c>
    /// (the CDC filter in <c>QuerySnapshotsCollector</c>). Parsing the hex as a GUID STRING instead
    /// (<c>new Guid(hex)</c>) would produce a different, wrong guid that silently matches no job — the
    /// lookup would degrade to the unresolved form on every card and nothing would error — which is why
    /// the round trip is pinned in <c>AgentJobStepQueryTests</c> rather than trusted.</para>
    /// </summary>
    public static bool TryParseProgramName(string? programName, out AgentJobStepKey key)
    {
        key = default;
        if (programName is null || !programName.StartsWith(ProgramNamePrefix, StringComparison.Ordinal))
        {
            return false;
        }

        int hexStart = ProgramNamePrefix.Length;
        int separatorIndex = programName.IndexOf(StepSeparator, hexStart, StringComparison.Ordinal);
        if (separatorIndex - hexStart != JobIdHexLength) /* -1 (absent) also fails: negative ≠ 32 */
        {
            return false;
        }

        byte[] jobIdBytes;
        try
        {
            jobIdBytes = Convert.FromHexString(programName.AsSpan(hexStart, JobIdHexLength));
        }
        catch (FormatException)
        {
            return false;
        }

        /* The step number runs from the separator to the closing paren, which must be the LAST
           character — a strict tail parse, so trailing garbage fails rather than half-matching. */
        int stepStart = separatorIndex + StepSeparator.Length;
        if (programName.Length < stepStart + 2 || programName[^1] != ')')
        {
            return false;
        }

        var stepSpan = programName.AsSpan(stepStart, programName.Length - stepStart - 1);
        if (!int.TryParse(stepSpan, NumberStyles.None, CultureInfo.InvariantCulture, out int stepId))
        {
            return false;
        }

        key = new AgentJobStepKey(new Guid(jobIdBytes), stepId);
        return true;
    }

    /// <summary>
    /// The job id spelled back the way <c>program_name</c> spells it (uppercase hex of the GUID's
    /// binary(16) layout) — the raw marker the UNRESOLVED annotation form carries, so an operator can
    /// eyeball-match the card's line against the session's program name without re-deriving byte order.
    /// Round-trips <see cref="TryParseProgramName"/> exactly; pinned in <c>AgentJobStepQueryTests</c>.
    /// </summary>
    public static string ToProgramNameHex(Guid jobId) => Convert.ToHexString(jobId.ToByteArray());

    /// <summary>Parameter name for pair <paramref name="index"/>'s job id — hosts bind it as
    /// <c>uniqueidentifier</c>, so the engine's own type system does the compare (no string joins).</summary>
    public static string JobIdParameter(int index) => "@job_id" + index.ToString(CultureInfo.InvariantCulture);

    /// <summary>Parameter name for pair <paramref name="index"/>'s step id (int).</summary>
    public static string StepIdParameter(int index) => "@step_id" + index.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// ONE round trip for every (job, step) pair a firing card needs — the display cap bounds
    /// <paramref name="pairCount"/> at 3, so the VALUES list never grows past a card's own render
    /// budget. INNER JOIN on <c>sysjobs</c> is the deleted-job arm: a job removed between capture and
    /// fire returns no row and the card keeps the unresolved form, which is honest, rather than a
    /// fabricated name. LEFT JOIN on <c>sysjobsteps</c> so a missing or renumbered step still names
    /// the JOB — the fact that closes the triage — and only the step name is omitted.
    /// </summary>
    public static string BuildSql(int pairCount)
    {
        if (pairCount < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(pairCount), pairCount, "At least one (job, step) pair is required.");
        }

        var pairs = new StringBuilder();
        for (int i = 0; i < pairCount; i++)
        {
            if (i > 0)
            {
                pairs.Append(", ");
            }

            pairs.Append('(').Append(JobIdParameter(i)).Append(", ").Append(StepIdParameter(i)).Append(')');
        }

        return @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    job_id = CONVERT(varchar(36), j.job_id),
    step_id = k.step_id,
    job_name = j.name,
    step_name = js.step_name
FROM (VALUES " + pairs + @") AS k (job_id, step_id)
JOIN msdb.dbo.sysjobs AS j
  ON j.job_id = k.job_id
LEFT JOIN msdb.dbo.sysjobsteps AS js
  ON  js.job_id = k.job_id
  AND js.step_id = k.step_id
OPTION(RECOMPILE);";
    }

    /// <summary>
    /// Maps <see cref="BuildSql"/>'s rows (job_id, step_id, job_name, step_name — in that ordinal
    /// order) into the lookup the card render consumes. A pair the result does not contain simply has
    /// no entry, which the render treats as unresolved — absence is the degrade, never an exception.
    /// </summary>
    public static async Task<Dictionary<AgentJobStepKey, AgentJobStepNames>> ReadAsync(
        DbDataReader reader, CancellationToken cancellationToken)
    {
        var names = new Dictionary<AgentJobStepKey, AgentJobStepNames>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var key = new AgentJobStepKey(
                Guid.Parse(reader.GetString(0)),
                Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
            names[key] = new AgentJobStepNames(
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3));
        }

        return names;
    }
}
