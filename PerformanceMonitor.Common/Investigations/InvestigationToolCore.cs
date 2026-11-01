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
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Common;

/// <summary>
/// The product-neutral core of the Level 1 investigation tools (#5632 Phase A; #5820 moved it here from Darling's
/// <c>DarlingMcpInvestigationTools</c> so Lite's tools answer the same way): the states, families and verdicts, the report
/// caps and their validation, the reports answer, the request payload and the claim status words and messages. Each
/// product keeps its own <c>[McpServerTool]</c> wrappers and store, and calls this core.
/// </summary>
internal static class InvestigationToolCore
{
    /// <summary>The request states a list can filter on.</summary>
    internal static readonly string[] States = ["open", "claimed", "reported", "closed", "expired"];

    /// <summary>The investigation families (V184's check constraint).</summary>
    internal static readonly string[] Families =
        ["blocking", "query_performance", "resources", "availability", "jobs", "self_monitor", "other"];

    /// <summary>
    /// #6462: the parameters <c>submit_investigation_report</c> requires, in the tool's own order, written once. The brief's
    /// <c>report_contract.required</c> and the tool contract table in the tests both read it, and the tests that reflect the contract
    /// against both products' real tool methods therefore check the brief too. A tool method that gains or loses a required parameter
    /// fails them until this list follows.
    /// </summary>
    internal static readonly string[] SubmitReportRequired =
        ["request_id", "claim_id", "report_format", "verdict", "root_cause", "evidence", "summary"];

    /// <summary>The verdicts a report may give (V184's check constraint).</summary>
    internal static readonly string[] Verdicts = ["real", "false_positive", "needs_person"];

    /// <summary>The longest proposed note (V184's check constraint).</summary>
    internal const int MaxNoteChars = 2000;

    /// <summary>The longest summary (V184's check constraint).</summary>
    internal const int MaxSummaryChars = 4000;

    /// <summary>The longest root cause (V184's check constraint, #5632 review round 2, M2).</summary>
    internal const int MaxRootCauseChars = 8000;

    /// <summary>The most evidence items (V184's check constraint).</summary>
    internal const int MaxEvidenceItems = 50;

    /// <summary>The longest string value anywhere in an evidence item (V184's check constraint).</summary>
    internal const int MaxEvidenceValueChars = 1000;

    /// <summary>The most bytes the evidence array takes as PostgreSQL prints it (<c>octet_length(evidence::text)</c>,
    /// V184's check constraint).</summary>
    internal const int MaxEvidenceBytes = 32768;

    /// <summary>The most requests one list returns.</summary>
    internal const int MaxLimit = 100;

    /// <summary>The most journal rows one report read returns.</summary>
    internal const int JournalLimit = 200;

    /// <summary>The agent a submit refusal is journaled under when its claim_id resolves to no claim holder.</summary>
    internal const string UnclaimedAgent = "unclaimed";

    /// <summary>The longest <c>agent_action.reason</c> (#5912), as a note.</summary>
    internal const int MaxActionReasonChars = 2000;

    /// <summary>
    /// Validates a Level 2 tool's <c>agent_action</c> (#5912): <c>{request_id, claim_id, agent_id, reason, evidence}</c>. The
    /// reason is 1 to <see cref="MaxActionReasonChars"/> characters; the evidence is a report's evidence, by
    /// <see cref="ValidateEvidence"/> and <see cref="CapProblems"/>; the agent id is <see cref="InvestigationRules.IsValidAgentId"/>'s.
    /// Null or blank answers <see cref="AgentActionCodes.Required"/>; anything else wrong answers
    /// <see cref="AgentActionCodes.Invalid"/> with a message that names the field. <paramref name="agentAction"/> is the
    /// argument's JSON text; <c>evidence</c> is a JSON array, or a string holding one (as the report takes it).
    /// </summary>
    internal static AgentActionRefusal? ParseAgentAction(string? agentAction, out AgentAction? action)
    {
        action = null;
        if (string.IsNullOrWhiteSpace(agentAction) || agentAction.Trim() == "null")
            return new AgentActionRefusal(AgentActionCodes.Required,
                "agent_action is required: {request_id, claim_id, agent_id, reason, evidence}, under the claim you hold.");

        JsonObject? root;
        try
        {
            root = JsonNode.Parse(agentAction) as JsonObject;
        }
        catch (JsonException)
        {
            root = null;
        }

        if (root is null)
            return Invalid("agent_action", "must be a JSON object {request_id, claim_id, agent_id, reason, evidence}.");

        if (!Guid.TryParse(Text(root, "request_id"), out var requestId))
            return Invalid("request_id", "missing or not a request id.");
        if (!Guid.TryParse(Text(root, "claim_id"), out var claimId))
            return Invalid("claim_id", "missing or not a claim id.");
        var agentId = Text(root, "agent_id");
        if (!InvestigationRules.IsValidAgentId(agentId))
            return Invalid("agent_id", InvestigationRules.AgentIdRule);
        var reason = Text(root, "reason");
        if (string.IsNullOrWhiteSpace(reason) || reason.Length > MaxActionReasonChars)
            return Invalid("reason", $"give 1 to {MaxActionReasonChars} characters saying why.");

        var evidenceText = root["evidence"] switch
        {
            JsonValue v when v.TryGetValue(out string? s) => s,
            JsonNode node => node.ToJsonString(),
            null => null,
        };
        var evidenceProblem = ValidateEvidence(evidenceText, out var evidenceJson);
        if (evidenceProblem is not null)
            return Invalid("evidence", evidenceProblem);
        var caps = CapProblems(null, evidenceJson);
        if (caps.Count > 0)
            return Invalid("evidence", caps[0].Problem["evidence: ".Length..]); /* the cap text carries its own "evidence: " lead, as a report lists it */

        action = new AgentAction(requestId, claimId, agentId!, reason, evidenceJson!);
        return null;

        static string? Text(JsonObject o, string key) =>
            o[key] is JsonValue v && v.TryGetValue(out string? s) ? s : null;

        /* The argument itself is "agent_action:"; a field in it is "agent_action.<field>:" (never "agent_action.agent_action"). */
        static AgentActionRefusal Invalid(string field, string problem) =>
            new(AgentActionCodes.Invalid, field == "agent_action" ? $"agent_action: {problem}" : $"agent_action.{field}: {problem}");
    }

    /// <summary>The <c>invalid</c> answer's message: every problem at once, each as <c>field: sentence</c>, so an agent
    /// fixes the report in one round; the refusal's <c>hints.parameter</c> names the first field.</summary>
    internal static string InvalidReportMessage(IReadOnlyList<string> problems) =>
        "The report was not stored. Fix every field listed: " + string.Join(" ", problems);

    /// <summary>
    /// Every problem with a report, as <c>field: sentence</c>, in parameter order: an empty list is a valid report. Lists
    /// them all at once, so an agent fixes the report in one round. Any action proposal is refused at Level 1.
    /// <paramref name="requireBasis"/> (#6004) makes each evidence item carry a <c>basis</c> of verified or inferred: Darling's
    /// submit passes true; Lite's, advise-only, keeps the older shape until its own slice (#6011). <paramref name="serverActionTools"/>
    /// (#7109, Darling) makes the <c>action_proposals</c> refusal name the action tools: a server action is proposed through its own tool,
    /// so it passes the gate and a dry run, never through the report.
    /// </summary>
    internal static List<string> ValidateReport(string? request_id, string? claim_id, string? verdict, string? root_cause,
        string? evidence, string? summary, string? proposed_note, string? action_proposals,
        out Guid requestId, out Guid claimId, out string? evidenceJson, bool requireBasis = false, bool serverActionTools = false)
    {
        var problems = new List<string>();
        evidenceJson = null;
        if (!Guid.TryParse(request_id, out requestId))
            problems.Add("request_id: missing or not a request id.");
        if (!Guid.TryParse(claim_id, out claimId))
            problems.Add("claim_id: missing or not a claim id.");
        if (string.IsNullOrWhiteSpace(verdict))
            problems.Add("verdict: missing; give real, false_positive or needs_person.");
        else if (!Verdicts.Contains(verdict, StringComparer.Ordinal))
            problems.Add($"verdict: '{verdict}' is not real, false_positive or needs_person.");
        if (string.IsNullOrWhiteSpace(root_cause))
            problems.Add("root_cause: missing; say what caused the alert, or that you could not tell.");
        if (string.IsNullOrWhiteSpace(summary))
            problems.Add("summary: missing.");
        else if (summary.Trim().Length > MaxSummaryChars)
            problems.Add($"summary: {summary.Trim().Length} characters, over {MaxSummaryChars}.");
        if (!string.IsNullOrWhiteSpace(proposed_note) && proposed_note.Trim().Length > MaxNoteChars)
            problems.Add($"proposed_note: {proposed_note.Trim().Length} characters, over {MaxNoteChars}.");

        var evidenceProblem = ValidateEvidence(evidence, out evidenceJson, requireBasis);
        if (evidenceProblem is not null)
            problems.Add("evidence: " + evidenceProblem);

        if (!string.IsNullOrWhiteSpace(action_proposals) && action_proposals.Trim() is not ("[]" or "null"))
            problems.Add(serverActionTools
                ? "action_proposals: refused. A report proposes no action. To propose a server action, call its tool with dry_run false while your claim is live: force_query_store_plan, unforce_query_store_plan, evict_cached_plan, kill_session, recompile_module, create_plan_guide or drop_plan_guide. When the action needs a person's approval, the tool stores the proposal. Omit action_proposals."
                : "action_proposals: refused at Level 1; an investigation proposes no action. Omit it.");

        return problems;
    }

    /// <summary>
    /// Each size cap a report is over (#5632 review round 2, M2), with its field and cap, in parameter order:
    /// <c>root_cause</c> over <see cref="MaxRootCauseChars"/> characters; <c>evidence</c> over
    /// <see cref="MaxEvidenceItems"/> items, with a string value over <see cref="MaxEvidenceValueChars"/> characters, or
    /// over <see cref="MaxEvidenceBytes"/> bytes as PostgreSQL prints it; <c>queries_run</c> (#6004) over
    /// <see cref="InvestigationReportFields.MaxQueriesRun"/> items or with an item over
    /// <see cref="InvestigationReportFields.MaxQueryChars"/> characters. Evidence that is not a JSON array is left to
    /// <see cref="ValidateReport"/>. Characters are UTF-16 units, never fewer than PostgreSQL's code points, so a report
    /// these caps pass also passes V184's checks.
    /// </summary>
    internal static List<(string Field, int Cap, string Problem)> CapProblems(string? root_cause, string? evidence, string? queries_run = null)
    {
        var problems = new List<(string Field, int Cap, string Problem)>();
        var rootCause = root_cause?.Trim() ?? string.Empty;
        if (rootCause.Length > MaxRootCauseChars)
            problems.Add(("root_cause", MaxRootCauseChars, $"root_cause: {rootCause.Length} characters, over {MaxRootCauseChars}."));

        JsonNode? root = null;
        try
        {
            if (!string.IsNullOrWhiteSpace(evidence))
                root = JsonNode.Parse(evidence);
        }
        catch (JsonException)
        {
            root = null;
        }

        if (root is JsonArray items)
        {
            if (items.Count > MaxEvidenceItems)
                problems.Add(("evidence", MaxEvidenceItems, $"evidence: {items.Count} items, over {MaxEvidenceItems}."));
            var longest = LongestString(items);
            if (longest > MaxEvidenceValueChars)
                problems.Add(("evidence", MaxEvidenceValueChars,
                    $"evidence: a string value of {longest} characters, over {MaxEvidenceValueChars}."));
            var bytes = PgJsonbTextBytes(items);
            if (bytes > MaxEvidenceBytes)
                problems.Add(("evidence", MaxEvidenceBytes, $"evidence: {bytes} bytes as stored, over {MaxEvidenceBytes}."));
        }

        /* #6004: queries_run, refused over its caps and never clamped. */
        foreach (var (cap, problem) in InvestigationReportFields.CapProblems(queries_run))
            problems.Add(("queries_run", cap, problem));

        return problems;
    }

    /// <summary>The longest string value anywhere under <paramref name="node"/> (object keys are not values).</summary>
    private static int LongestString(JsonNode? node) => node switch
    {
        JsonObject o => o.Select(p => LongestString(p.Value)).DefaultIfEmpty(0).Max(),
        JsonArray a => a.Select(LongestString).DefaultIfEmpty(0).Max(),
        JsonValue v when v.GetValueKind() == JsonValueKind.String => v.GetValue<string>().Length,
        _ => 0,
    };

    /// <summary>
    /// The bytes <paramref name="node"/> takes as PostgreSQL's <c>jsonb::text</c> prints it, never fewer: <c>": "</c> and
    /// <c>", "</c> between members, strings in UTF-8 with JSON escapes (the relaxed encoder escapes every character
    /// PostgreSQL escapes, and widens a few it keeps raw), and each number at its printed width (<c>1e3</c> prints as
    /// <c>1000</c>), so an exponent cannot hide a long number from the cap.
    /// </summary>
    internal static long PgJsonbTextBytes(JsonNode? node) => node switch
    {
        JsonObject o => 2 + (o.Count == 0 ? 0 : 2L * (o.Count - 1)) + o.Sum(p => QuotedBytes(p.Key) + 2 + PgJsonbTextBytes(p.Value)),
        JsonArray a => 2 + (a.Count == 0 ? 0 : 2L * (a.Count - 1)) + a.Sum(PgJsonbTextBytes),
        JsonValue v => v.GetValueKind() switch
        {
            JsonValueKind.String => QuotedBytes(v.GetValue<string>()),
            JsonValueKind.Number => PgNumberChars(v.ToJsonString()),
            JsonValueKind.False => 5,
            _ => 4,
        },
        _ => 4,
    };

    private static readonly JsonSerializerOptions RelaxedJson =
        new() { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private static long QuotedBytes(string text) =>
        System.Text.Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(text, RelaxedJson));

    /// <summary>The width PostgreSQL prints a JSON number at, never less: the sign, the integer digits the exponent
    /// moves, and the scale <c>max(0, fraction digits - exponent)</c>. A huge exponent saturates.</summary>
    internal static long PgNumberChars(string number)
    {
        var body = number.Trim();
        long sign = body.StartsWith('-') ? 1 : 0;
        body = body.TrimStart('-');
        var e = body.IndexOfAny(['e', 'E']);
        long exponent = 0;
        if (e >= 0)
        {
            if (!long.TryParse(body[(e + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out exponent)
                || Math.Abs(exponent) > 1_000_000)
                return int.MaxValue;
            body = body[..e];
        }

        var dot = body.IndexOf('.');
        long intDigits = dot >= 0 ? dot : body.Length;
        if (intDigits == 1 && body[0] == '0')
            intDigits = 0; // JSON writes no leading zero but this one, and PostgreSQL drops it once an exponent moves digits in
        long fracDigits = dot >= 0 ? body.Length - dot - 1 : 0;
        var whole = Math.Max(1, intDigits + exponent);
        var scale = Math.Max(0, fracDigits - exponent);
        return sign + whole + (scale > 0 ? 1 + scale : 0);
    }

    internal static string? ValidateEvidence(string? evidence, out string? evidenceJson, bool requireBasis = false)
    {
        evidenceJson = null;
        var shape = requireBasis ? "{tool, arguments, key_numbers, basis}" : "{tool, arguments, key_numbers}";
        if (string.IsNullOrWhiteSpace(evidence))
            return $"missing; give a JSON array of at least one {shape}.";

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(evidence);
        }
        catch (JsonException ex)
        {
            return "not valid JSON: " + ex.Message;
        }

        if (root is not JsonArray items || items.Count == 0)
            return $"must be a JSON array of at least one {shape}.";

        var missing = new List<string>();
        for (var i = 0; i < items.Count; i++)
        {
            if (items[i] is not JsonObject item)
            {
                missing.Add($"item {i} is not an object");
                continue;
            }

            if (item["tool"] is not JsonValue tool || string.IsNullOrWhiteSpace(tool.ToString()))
                missing.Add($"item {i} has no tool");
            if (item["arguments"] is null)
                missing.Add($"item {i} has no arguments");
            if (item["key_numbers"] is null)
                missing.Add($"item {i} has no key_numbers");
            /* #6004 (D17): a new report labels each item verified or inferred; evidence stored before has no label and is read as unlabelled. */
            if (requireBasis)
            {
                if (item["basis"] is null)
                    missing.Add($"item {i} has no basis (give {InvestigationReportFields.BasesText})");
                else if (item["basis"] is not JsonValue basis || !basis.TryGetValue(out string? word)
                    || !InvestigationReportFields.Bases.Contains(word, StringComparer.Ordinal))
                    missing.Add($"item {i} basis '{item["basis"]!.ToString()}' is not {InvestigationReportFields.BasesText}");
            }
        }

        if (missing.Count > 0)
            return string.Join("; ", missing) + ".";

        evidenceJson = items.ToJsonString();
        return null;
    }

    /// <summary>
    /// The <c>ok</c> answer for <paramref name="read"/>, masked at <paramref name="mode"/> with one masker through
    /// <see cref="InvestigationFieldMasking"/>, the brief composer's rule (#5632 review round 2, M1): a report's
    /// <c>root_cause</c>, <c>summary</c> and each evidence item's <c>key_numbers</c>, and a journal row's <c>reason</c>, are
    /// free text; the login of a person's action is masked as a login; the rest is masked by key. A stored brief composed at
    /// a weaker level is re-masked at <paramref name="mode"/>. Reports are stored as submitted, so the level that applies
    /// is the one the host serves now.
    /// </summary>
    internal static string ReportsAnswer(InvestigationReportsRead read, int? hoursBack, McpDataDetail mode, PseudonymKey? key = null)
    {
        var masker = McpDataDetailMasker.ForAnswer(mode, key);
        var reports = new JsonArray();
        foreach (var r in read.Reports)
        {
            var storedEvidence = JsonNode.Parse(r.EvidenceJson);
            var evidence = InvestigationFieldMasking.Evidence(storedEvidence, masker);

            var node = InvestigationFieldMasking.Mask(new JsonObject
            {
                ["request_id"] = r.RequestId.ToString(),
                ["report_format"] = r.ReportFormat,
                ["agent_id"] = r.AgentId,
                ["submitted_at"] = JsonSerializer.SerializeToNode(r.SubmittedAtUtc, McpHelpers.JsonOptions),
                ["verdict"] = r.Verdict,
                ["root_cause"] = null,
                ["root_cause_found"] = r.RootCauseFound,
                ["summary"] = null,
            }, masker, ("root_cause", r.RootCause), ("summary", r.Summary));
            /* #6004 (D17): the evidence is served as two lists by basis, with the items written before the basis existed apart as
               unlabelled. Both stores say so with EvidenceByBasis (#6011: Lite's submit now requires a basis); a read that leaves it
               null follows whether the store keeps feedback, as before. Feedback itself stays keyed on Feedback below. */
            if (read.EvidenceByBasis ?? read.Feedback is not null)
                AddEvidenceLists(node, storedEvidence, evidence);
            else
                node["evidence"] = evidence;
            /* #6004: family_of and queries_run are absent, never null, when the report gave none. queries_run is statement text. */
            if (!node.ContainsKey("withheld"))
            {
                if (r.FamilyOf is { } familyOf)
                    node["family_of"] = familyOf.ToString();
                if (r.QueriesRunJson is not null && JsonNode.Parse(r.QueriesRunJson) is JsonArray queries)
                    node["queries_run"] = new JsonArray(queries.Select(q => (JsonNode?)JsonValue.Create(masker.FreeText(q?.GetValue<string>()))).ToArray());
            }

            /* #5914: absent, never [], when the report carries no link. */
            if (r.Links is { Count: > 0 } links && !node.ContainsKey("withheld"))
                node["links"] = new JsonArray(links.Select(l => (JsonNode?)LinkNode(l, masker)).ToArray());
            node["labels"] = new JsonArray(InvestigationRules.EarlierReportLabels.Select(l => (JsonNode?)JsonValue.Create(l)).ToArray());
            /* #5913: what people said about this report. The free text is masked as the report's is; the author is a login. */
            var feedbackNodes = new JsonArray();
            foreach (var f in (read.Feedback ?? []).Where(f => f.RequestId == r.RequestId))
            {
                var fb = InvestigationFieldMasking.Mask(new JsonObject
                {
                    ["verdict"] = f.Verdict,
                    ["corrected_verdict"] = f.CorrectedVerdict,
                    ["missed_step"] = null,
                    ["comment"] = null,
                    ["recorded_at"] = JsonSerializer.SerializeToNode(f.CreatedAtUtc, McpHelpers.JsonOptions),
                }, masker, ("missed_step", f.MissedStep), ("comment", f.Comment));
                if (!fb.ContainsKey("withheld"))
                    fb["author"] = masker.Field("login_name", f.Author);
                feedbackNodes.Add(fb);
            }

            if (read.Feedback is not null)
                node["feedback"] = feedbackNodes;
            reports.Add(node);
        }

        var journal = new JsonArray();
        foreach (var j in read.Journal)
        {
            /* #5914: a link_recorded row's detail holds an agent's URL and key: free text, masked as the report's link is. */
            var link = j.Event == InvestigationReportLinks.JournalEvent ? InvestigationReportLinks.FromDetail(j.DetailJson) : null;
            var node = InvestigationFieldMasking.Mask(new JsonObject
            {
                ["event_time"] = JsonSerializer.SerializeToNode(j.EventTimeUtc, McpHelpers.JsonOptions),
                ["request_id"] = j.RequestId?.ToString(),
                ["server_id"] = j.ServerId,
                ["event"] = j.Event,
                ["actor"] = null,
                ["reason"] = null,
                ["detail"] = null,
            }, masker, ("reason", j.Reason));
            /* #5912: the detail through the one journal helper the web endpoint shares (its evidence as a report's, the
               free text of a Level 2 change's before and after images through FreeText). */
            if (!node.ContainsKey("withheld"))
                node["detail"] = InvestigationFieldMasking.JournalDetail(j.DetailJson, masker);
            if (!node.ContainsKey("withheld"))
                node["actor"] = InvestigationFieldMasking.Actor(j.Actor, masker);
            if (link is not null && !node.ContainsKey("withheld"))
                node["detail"] = LinkNode(link, masker);
            journal.Add(node);
        }

        var answer = new JsonObject
        {
            ["status"] = "ok",
            ["hours_back"] = hoursBack,
            ["requests_returned"] = read.Requests.Count,
            ["truncated"] = read.RequestsTruncated,
            ["journal_truncated"] = read.JournalTruncated,
            ["requests"] = JsonSerializer.SerializeToNode(read.Requests.Select(RequestPayload), McpHelpers.JsonOptions),
            ["reports"] = reports,
            ["journal"] = journal,
            ["brief"] = read.Brief is null ? null : InvestigationFieldMasking.StoredBrief(read.Brief, mode, masker),
        };
        /* #5913: only a store that keeps feedback (Darling's) sends the counts; Lite's answer keeps its shape. */
        if (read.FamilyCounts is not null)
            answer["family_counts"] = FamilyCountsPayload(read.FamilyCounts);
        return answer.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>The evidence as <c>evidence_verified</c> and <c>evidence_inferred</c> (always present, possibly empty), plus
    /// <c>evidence_unlabelled</c> only when the report holds items with no valid basis (#6004: written before the basis was
    /// required). <paramref name="masked"/> is index-aligned with <paramref name="stored"/>, the basis read from the stored item.</summary>
    private static void AddEvidenceLists(JsonObject node, JsonNode? stored, JsonArray masked)
    {
        var verified = new JsonArray();
        var inferred = new JsonArray();
        var unlabelled = new JsonArray();
        for (var i = 0; i < masked.Count; i++)
        {
            var item = masked[i]?.DeepClone();
            var basis = stored is JsonArray items && i < items.Count && items[i] is JsonObject o
                && o["basis"] is JsonValue v && v.TryGetValue(out string? word) ? word : null;
            var target = basis switch
            {
                InvestigationReportFields.BasisVerified => verified,
                InvestigationReportFields.BasisInferred => inferred,
                _ => unlabelled,
            };
            target.Add(item);
        }

        node["evidence_verified"] = verified;
        node["evidence_inferred"] = inferred;
        if (unlabelled.Count > 0)
            node["evidence_unlabelled"] = unlabelled;
    }

    /// <summary>
    /// The false-positive counts per alert family (#5913), one entry per family that had a report in the window:
    /// <c>reported</c>, <c>with_feedback</c>, <c>marked_wrong</c>, <c>agent_false_positives</c> (reports that say false_positive)
    /// and <c>false_positives</c> (the same after people's corrected verdicts; see <see cref="InvestigationFamilyCounts"/>).
    /// </summary>
    internal static JsonArray FamilyCountsPayload(IReadOnlyList<InvestigationFamilyCounts> counts)
    {
        var array = new JsonArray();
        foreach (var c in counts)
        {
            array.Add(new JsonObject
            {
                ["family"] = c.Family,
                ["reported"] = c.Reported,
                ["with_feedback"] = c.WithFeedback,
                ["marked_wrong"] = c.MarkedWrong,
                ["agent_false_positives"] = c.AgentFalsePositives,
                ["false_positives"] = c.FalsePositives,
            });
        }

        return array;
    }

    /// <summary>
    /// A report link as an answer serves it (#5914): <c>kind</c> is Darling's own word and kept; the <c>url</c> and the
    /// <c>key</c> an agent wrote are free text, masked through <see cref="McpDataDetailMasker.FreeText"/> as a report's
    /// <c>root_cause</c> is (literals replaced under <c>maskLiterals</c>, withheld under <c>metadataOnly</c>). <c>key</c> is
    /// absent when the link has none. Nothing here makes it safe to open: a renderer re-checks the URL.
    /// </summary>
    internal static JsonObject LinkNode(InvestigationReportLink link, McpDataDetailMasker masker)
    {
        var node = new JsonObject { ["kind"] = link.Kind, ["url"] = null };
        if (link.Key is null)
            return InvestigationFieldMasking.Mask(node, masker, ("url", link.Url));
        node["key"] = null;
        return InvestigationFieldMasking.Mask(node, masker, ("url", link.Url), ("key", link.Key));
    }

    internal static object RequestPayload(InvestigationRequestRow r)
    {
        var payload = JsonSerializer.SerializeToNode(RequestFields(r), McpHelpers.JsonOptions)!.AsObject();
        /* #5918: a practice request names the case it replays; every other row keeps its shape (absent, not null). */
        if (r.PracticeOf is { } practiceOf)
            payload["practice_of"] = practiceOf.ToString();
        /* #5999: the registry's names, read when served (a rename shows on the next read). Darling always has a display_name (its
           fallback floor is the name the request was opened with) and lacks only the instance_name once the server has left the
           registry. Lite fills both from its server list (ServerManager, #6011; its DuckDB servers table is never written), and
           leaves both keys absent only where a store is built with no list. The key is instance_name and NOT host_name on purpose: McpDataDetailKeys classifies host_name
           as a client machine (every other host_name in the product is one) and numbers it to host#1 at metadataOnly on every MCP
           answer, which would hide the very name this adds. */
        if (r.DisplayName is { } displayName)
            payload["display_name"] = displayName;
        if (r.InstanceName is { } instanceName)
            payload["instance_name"] = instanceName;
        /* #6551: a change-watch request has no alert row in get_alert_history, so its detail (what changed, on which database, which
           measure moved) rides the answer under the key an alert's brief row uses; the mcp.dataDetail filter masks detail_text by key. */
        if (r.DetailText is { } detailText)
            payload["detail_text"] = detailText;
        return payload;
    }

    private static object RequestFields(InvestigationRequestRow r) => new
    {
        request_id = r.RequestId,
        route_id = r.RouteId,
        server_id = r.ServerId,
        server_name = r.ServerName,
        family = r.Family,
        first_metric = r.FirstMetric,
        first_alert_at = r.FirstAlertAtUtc,
        last_alert_at = r.LastAlertAtUtc,
        alert_count = r.AlertCount,
        state = r.State,
        opened_at = r.OpenedAtUtc,
        expires_at = r.ExpiresAtUtc,
        claimed_by = r.ClaimedBy,
        lease_until = r.LeaseUntilUtc,
        claim_count = r.ClaimCount,
        reported_at = r.ReportedAtUtc,
    };

    internal static string ClaimStatus(InvestigationClaimOutcome outcome) => outcome switch
    {
        InvestigationClaimOutcome.Claimed => "claimed",
        InvestigationClaimOutcome.Reclaimed => "renewed",
        InvestigationClaimOutcome.AlreadyClaimed => "already_claimed",
        InvestigationClaimOutcome.NotOpen => "not_open",
        InvestigationClaimOutcome.NotFound => "not_found",
        InvestigationClaimOutcome.AgentIdInvalid => "agent_id_invalid",
        _ => "journal_capped",
    };

    internal static string ClaimMessage(InvestigationClaimResult claim) => claim.Outcome switch
    {
        InvestigationClaimOutcome.AlreadyClaimed => "Another claim holds a live lease on this request.",
        InvestigationClaimOutcome.NotOpen => claim.Reason switch
        {
            "claim_limit" => $"The request has used its {InvestigationRules.MaxClaims} claims.",
            { } reason => $"The request is not open to a claim: {reason}.",
            _ => "The request is not open to a claim.",
        },
        InvestigationClaimOutcome.NotFound => "No investigation request has that id.",
        InvestigationClaimOutcome.AgentIdInvalid => InvestigationRules.AgentIdRule,
        _ => $"The request already holds {InvestigationRules.MaxAgentJournalRows} agent journal rows; the claim was refused.",
    };

    internal static string Status(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);
}
