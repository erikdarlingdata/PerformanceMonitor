/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using PerformanceMonitor.Darling.Service.Hosting;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// PURE, comment-preserving TEXT SURGERY on darling.json for the <c>--configure-network</c> wizard
/// (issue #1561). This type has NO I/O, NO DPAPI, and NO Windows dependency by design: it is the
/// single correctness-critical piece, so it is fully unit-testable against the REAL
/// <c>darling.sample.json</c> as a fixture.
///
/// <para><b>Why text surgery and not a re-serialize.</b> darling.json / darling.sample.json are
/// comment-rich BY DESIGN — the comments ARE the documentation (see the sample header). Round-tripping
/// through <see cref="JsonSerializer"/> would silently delete every comment, so instead we splice the
/// three <c>network</c> blocks (<c>postgres.network</c>, <c>mcp.network</c>, <c>web.network</c>) in/out
/// while leaving every other byte — including the heavily-commented template blocks the sample ships
/// COMMENTED OUT — untouched.</para>
///
/// <para><b>Why a real scanner and not IndexOf.</b> The sample ships all three network blocks commented out
/// (the <c>// "network": { ... }</c> templates), so a naive <c>IndexOf("\"network\"")</c> would match
/// the TEMPLATE and corrupt the file. <see cref="Classify"/> walks the text once and labels every
/// character as code / string / line-comment / block-comment; every structural scan below
/// (brace matching, key finding) then counts ONLY characters in a code region, so a commented-out
/// <c>network</c> key is invisible to the surgery. That "scanner ignores the commented template"
/// property is the core correctness proof and is pinned by the fixture tests.</para>
///
/// <para>The parser is comment-tolerant (<c>ReadCommentHandling.Skip</c> + <c>AllowTrailingCommas</c>,
/// see <see cref="DarlingConfig"/>), so a spliced-in LIVE block that sits AFTER the surviving commented
/// template parses cleanly, and a removed block can leave a trailing comma behind without breaking the
/// parse. The wizard still re-parses (<see cref="DarlingConfig.Parse"/>) AND re-runs the resolvers on
/// the result before it ever writes, so an edit can never leave an unparseable or fail-closed file.</para>
/// </summary>
internal static class DarlingNetworkConfigEditor
{
    /// <summary>
    /// The direct children of the <c>postgres</c> / <c>mcp</c> objects are indented four spaces in the
    /// sample (a two-space step: root fields at two, their object children at four). The spliced-in
    /// <c>network</c> block mirrors that: the <c>"network":</c> line rides the four-space child indent
    /// the caller prepends, its fields ride six, and its closing brace rides four. Indentation is purely
    /// cosmetic to the parser — a reformatted darling.json still parses, it just would not match its own
    /// two-space style — so this is a faithful-to-the-sample default, not a correctness requirement.
    /// </summary>
    private const string ChildIndent = "    ";
    private const string FieldIndent = "      ";

    /// <summary>How <see cref="Classify"/> labels each character of the JSON text.</summary>
    internal enum RegionKind
    {
        /// <summary>Structural JSON (braces, commas, colons, whitespace, primitive tokens).</summary>
        Code,

        /// <summary>Inside a <c>"..."</c> string token, including its opening/closing quotes and escapes.</summary>
        StringLiteral,

        /// <summary>Inside a <c>// ...</c> line comment (the two slashes through the char before the newline).</summary>
        LineComment,

        /// <summary>Inside a <c>/* ... */</c> block comment, including the opening and closing delimiters.</summary>
        BlockComment,
    }

    /// <summary>
    /// Single-pass classifier: labels every character of <paramref name="json"/> as
    /// code / string / line-comment / block-comment. This is the foundation every structural scan
    /// stands on — a <c>{</c> inside a string or a comment is NOT a real brace, and a
    /// <c>"network"</c> inside a comment is NOT a real key. String escapes (<c>\"</c>, <c>\\</c>) are
    /// honored so an embedded quote never falsely closes a string. Pure.
    /// </summary>
    internal static RegionKind[] Classify(string json)
    {
        if (json is null)
        {
            throw new ArgumentNullException(nameof(json));
        }

        var regions = new RegionKind[json.Length];
        var state = RegionKind.Code;
        var i = 0;
        while (i < json.Length)
        {
            var c = json[i];
            switch (state)
            {
                case RegionKind.Code:
                    if (c == '"')
                    {
                        regions[i] = RegionKind.StringLiteral;
                        state = RegionKind.StringLiteral;
                        i++;
                    }
                    else if (c == '/' && i + 1 < json.Length && json[i + 1] == '/')
                    {
                        regions[i] = RegionKind.LineComment;
                        regions[i + 1] = RegionKind.LineComment;
                        state = RegionKind.LineComment;
                        i += 2;
                    }
                    else if (c == '/' && i + 1 < json.Length && json[i + 1] == '*')
                    {
                        regions[i] = RegionKind.BlockComment;
                        regions[i + 1] = RegionKind.BlockComment;
                        state = RegionKind.BlockComment;
                        i += 2;
                    }
                    else
                    {
                        regions[i] = RegionKind.Code;
                        i++;
                    }

                    break;

                case RegionKind.StringLiteral:
                    regions[i] = RegionKind.StringLiteral;
                    if (c == '\\' && i + 1 < json.Length)
                    {
                        /* An escaped char (\" or \\ etc.) — consume it as part of the string so a \" never
                           reads as the closing quote. */
                        regions[i + 1] = RegionKind.StringLiteral;
                        i += 2;
                    }
                    else if (c == '"')
                    {
                        state = RegionKind.Code;
                        i++;
                    }
                    else
                    {
                        i++;
                    }

                    break;

                case RegionKind.LineComment:
                    if (c == '\n')
                    {
                        /* The newline TERMINATES the line comment; label it Code so line-start scans and
                           "last code char" scans see a clean structural boundary. */
                        regions[i] = RegionKind.Code;
                        state = RegionKind.Code;
                    }
                    else
                    {
                        regions[i] = RegionKind.LineComment;
                    }

                    i++;
                    break;

                case RegionKind.BlockComment:
                    regions[i] = RegionKind.BlockComment;
                    if (c == '*' && i + 1 < json.Length && json[i + 1] == '/')
                    {
                        regions[i + 1] = RegionKind.BlockComment;
                        state = RegionKind.Code;
                        i += 2;
                    }
                    else
                    {
                        i++;
                    }

                    break;
            }
        }

        return regions;
    }

    /// <summary>Index of the first code <c>{</c> (the root object open), or -1 if there is none.</summary>
    internal static int FindRootObjectOpen(string json, RegionKind[] regions)
    {
        for (var i = 0; i < json.Length; i++)
        {
            if (regions[i] == RegionKind.Code && json[i] == '{')
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Index of the code brace/bracket that closes the one at <paramref name="openIndex"/>, or -1 if the
    /// text is unbalanced. Combined <c>{[</c>/<c>}]</c> depth counting is correct for well-formed JSON
    /// (which nests properly), and only code-region braces are counted, so braces inside strings/comments
    /// are ignored. Pure.
    /// </summary>
    internal static int FindMatchingClose(string json, RegionKind[] regions, int openIndex)
    {
        var depth = 0;
        for (var i = openIndex; i < json.Length; i++)
        {
            if (regions[i] != RegionKind.Code)
            {
                continue;
            }

            var c = json[i];
            if (c == '{' || c == '[')
            {
                depth++;
            }
            else if (c == '}' || c == ']')
            {
                depth--;
                if (depth == 0)
                {
                    return i;
                }
            }
        }

        return -1;
    }

    /// <summary>The span of a direct-child key inside an object.</summary>
    /// <param name="KeyStart">Index of the opening quote of the key's name.</param>
    /// <param name="ValueStart">Index of the first character of the value (after the colon).</param>
    /// <param name="ValueEnd">Index one past the last character of the value.</param>
    internal readonly record struct ObjectMemberSpan(int KeyStart, int ValueStart, int ValueEnd);

    /// <summary>
    /// Finds a DIRECT-CHILD member named <paramref name="key"/> inside the object whose braces are
    /// <paramref name="objectOpen"/>/<paramref name="objectClose"/>, returning the key's start and its
    /// value's extent, or null if there is no such LIVE (code-region) key. A <paramref name="key"/> that
    /// appears only inside a comment (the sample's commented template) or nested deeper is not matched.
    /// Pure.
    /// </summary>
    internal static ObjectMemberSpan? FindObjectValueSpan(
        string json, RegionKind[] regions, int objectOpen, int objectClose, string key)
    {
        var depth = 0;
        var i = objectOpen;
        while (i < objectClose)
        {
            if (regions[i] != RegionKind.Code)
            {
                /* A string/comment token — skip its whole span. For a string, that also skips any braces
                   or colons it contains so they cannot perturb the depth or be read as a key delimiter. */
                if (regions[i] == RegionKind.StringLiteral)
                {
                    var token = ReadStringToken(json, regions, i);
                    if (depth == 1 && string.Equals(token.Content, key, StringComparison.Ordinal)
                        && NextCodeChar(json, regions, token.End, objectClose) == ':')
                    {
                        var colon = NextCodeIndex(json, regions, token.End, objectClose);
                        var valueStart = NextValueIndex(json, regions, colon + 1, objectClose);
                        if (valueStart < 0)
                        {
                            return null;
                        }

                        var valueEnd = ValueEndFrom(json, regions, valueStart, objectClose);
                        return new ObjectMemberSpan(token.Start, valueStart, valueEnd);
                    }

                    i = token.End;
                    continue;
                }

                i++;
                continue;
            }

            var c = json[i];
            if (c == '{' || c == '[')
            {
                depth++;
            }
            else if (c == '}' || c == ']')
            {
                depth--;
            }

            i++;
        }

        return null;
    }

    /// <summary>
    /// Insert or replace <c>parentKey.network</c> with <paramref name="networkBlock"/> (the block text
    /// starting at <c>"network":</c>, with no leading indent on its first line — this method supplies the
    /// child indent). If a LIVE <c>network</c> key already exists it is replaced in place; otherwise the
    /// block is appended as the parent's last member (after any surviving commented template), with a
    /// separating comma synthesized only when the previous member lacks a trailing one. If the parent
    /// section itself is absent it is created at the root (the <c>mcp</c>-omitted case).
    ///
    /// <para><b>Replacing keeps what the wizard does not own (#4743).</b> The block the wizard builds holds
    /// only the keys it prompts for, so replacing the old block wholesale silently deleted every other
    /// member — <c>web.network.tls</c> and <c>web.network.oidc</c> among them, which turned the dashboard
    /// back into plain HTTP with no SSO after the restart. Every member of the old block whose name is not
    /// in <paramref name="ownedKeys"/> is copied into the new block verbatim (key through value, plus a
    /// comment on the same line), so keys added later survive too. Names are compared case-insensitively
    /// because the config parser is: a kept <c>"Listen"</c> beside the new <c>"listen"</c> would win at
    /// parse time and undo the operator's answer. The insert path has no old block and ignores
    /// <paramref name="ownedKeys"/>. Pure.</para>
    /// </summary>
    internal static string UpsertNetworkBlock(
        string json, string parentKey, string networkBlock, IReadOnlyCollection<string> ownedKeys) =>
        UpsertNetworkBlock(json, parentKey, networkBlock, ownedKeys, out _);

    /// <summary>
    /// <see cref="UpsertNetworkBlock(string, string, string, IReadOnlyCollection{string})"/> that also reports
    /// the names of the old members it kept (document order; empty when nothing was kept or nothing was
    /// replaced), so the wizard can tell the operator exactly what survived. Pure.
    /// </summary>
    internal static string UpsertNetworkBlock(
        string json, string parentKey, string networkBlock, IReadOnlyCollection<string> ownedKeys,
        out IReadOnlyList<string> carriedKeys)
    {
        carriedKeys = [];
        var regions = Classify(json);
        var rootOpen = FindRootObjectOpen(json, regions);
        if (rootOpen < 0)
        {
            throw new InvalidOperationException("darling.json has no root JSON object.");
        }

        var rootClose = FindMatchingClose(json, regions, rootOpen);
        if (rootClose < 0)
        {
            throw new InvalidOperationException("darling.json root object is not brace-balanced.");
        }

        var parent = FindObjectValueSpan(json, regions, rootOpen, rootClose, parentKey);
        if (parent is null)
        {
            /* No parent section at all (e.g. a minimal darling.json with no "mcp"): create it at root,
               wrapping the block one level deeper so its four-space child indent stays correct. */
            var section =
                $"\"{parentKey}\": {{\n" +
                ChildIndent + networkBlock + "\n" +
                "  }";
            return InsertAsLastMember(json, regions, rootOpen, rootClose, section, "  ");
        }

        if (json[parent.Value.ValueStart] != '{')
        {
            throw new InvalidOperationException($"darling.json '{parentKey}' is not a JSON object.");
        }

        var parentOpen = parent.Value.ValueStart;
        var parentClose = parent.Value.ValueEnd - 1;
        var existing = FindObjectValueSpan(json, regions, parentOpen, parentClose, "network");
        if (existing is not null)
        {
            /* Replace the existing LIVE block in place. KeyStart already sits after the four-space indent,
               so the block goes in without a leading indent. #4743: members the wizard does not own ride
               along into the replacement rather than vanishing with the old block. */
            var replacement = CarryUnownedMembers(json, regions, existing.Value, networkBlock, ownedKeys, out carriedKeys);
            return json[..existing.Value.KeyStart] + replacement + json[existing.Value.ValueEnd..];
        }

        return InsertAsLastMember(json, regions, parentOpen, parentClose, networkBlock, ChildIndent);
    }

    /// <summary>
    /// Remove a LIVE <c>parentKey.network</c> block, taking one adjacent separating comma with it so the
    /// result stays valid JSON (trailing comma preferred; else the leading comma; else the block is the
    /// sole member). A missing parent or a missing LIVE network key is a no-op. The surviving commented
    /// template is untouched. Pure.
    /// </summary>
    internal static string RemoveNetworkBlock(string json, string parentKey)
    {
        var regions = Classify(json);
        var rootOpen = FindRootObjectOpen(json, regions);
        if (rootOpen < 0)
        {
            return json;
        }

        var rootClose = FindMatchingClose(json, regions, rootOpen);
        if (rootClose < 0)
        {
            return json;
        }

        var parent = FindObjectValueSpan(json, regions, rootOpen, rootClose, parentKey);
        if (parent is null || json[parent.Value.ValueStart] != '{')
        {
            return json;
        }

        var parentOpen = parent.Value.ValueStart;
        var parentClose = parent.Value.ValueEnd - 1;
        var existing = FindObjectValueSpan(json, regions, parentOpen, parentClose, "network");
        if (existing is null)
        {
            return json;
        }

        var removeStart = existing.Value.KeyStart;
        var removeEnd = existing.Value.ValueEnd;

        /* Take a trailing comma if there is one (the block was not last), else a leading comma (the block
           was last). Comments between the block and the comma are whitespace to the parser, so removing
           the comma keeps the sibling separation correct either way. */
        var afterIdx = NextCodeIndex(json, regions, removeEnd, parentClose);
        if (afterIdx >= 0 && json[afterIdx] == ',')
        {
            removeEnd = afterIdx + 1;
        }
        else
        {
            var beforeIdx = PreviousCodeIndex(json, regions, removeStart, parentOpen);
            if (beforeIdx >= 0 && json[beforeIdx] == ',')
            {
                removeStart = beforeIdx;
            }
        }

        /* Also swallow the run of trailing horizontal whitespace + the block's line break so a removed
           block does not leave a widening blank gap behind (cosmetic; the parse is fine either way). */
        var trimmedTail = removeEnd;
        while (trimmedTail < json.Length && (json[trimmedTail] == ' ' || json[trimmedTail] == '\t' || json[trimmedTail] == '\r'))
        {
            trimmedTail++;
        }

        if (trimmedTail < json.Length && json[trimmedTail] == '\n')
        {
            removeEnd = trimmedTail + 1;
        }

        return json[..removeStart] + json[removeEnd..];
    }

    /// <summary>
    /// The keys <see cref="BuildStoreNetworkBlock"/> writes (#4743) — exactly the ones the wizard owns in
    /// <c>postgres.network</c>. Any other member of an old block is the operator's and is kept when the
    /// block is replaced. Keep in step with the builder; a test pins that every key it writes is listed.
    /// </summary>
    internal static readonly IReadOnlyList<string> StoreNetworkOwnedKeys = ["listen", "allowFrom", "role"];

    /// <summary>
    /// The active (uncommented) <c>postgres.network</c> block the wizard writes — a sample-styled block
    /// (per-field trailing comments, no fragile column alignment) whose VALUES are live so the store
    /// resolver picks it up. The values are pre-validated by the wizard through the real resolver; they
    /// are JSON-encoded here so any character is emitted safely. Pure.
    /// </summary>
    internal static string BuildStoreNetworkBlock(string listen, string allowFrom, string role) =>
        "\"network\": {\n" +
        FieldIndent + $"\"listen\": {JsonString(listen)},  // bind IP; 0.0.0.0 = all interfaces (connect by a cert SAN name).\n" +
        FieldIndent + $"\"allowFrom\": {JsonString(allowFrom)},  // pg_hba + firewall CIDR (address family must match listen).\n" +
        FieldIndent + $"\"role\": {JsonString(role)}  // remote pg_hba role(s): viewer (read-only, default), admin (remote writes), or both (\"admin,viewer\").\n" +
        ChildIndent + "}";

    /// <summary>
    /// #5288: the text the wizard writes as <c>mcp.network.allowFrom</c> / <c>web.network.allowFrom</c> (and
    /// builds the firewall hint from) for what the operator typed. ONE entry comes back exactly as typed, so a
    /// single-CIDR block is byte-for-byte what it was before the list existed. A LIST (two or more entries, or a
    /// comma list that de-duplicates to one) comes back as <c>CidrAllowList.ToString()</c> — each entry masked,
    /// duplicates dropped, joined by <c>,</c> with no spaces — so the file holds the text the hosts themselves
    /// compute, and the firewall hint carries the exact scope the service enforces.
    ///
    /// <para><b>Always ONE JSON string, never a JSON array</b>, whatever the count. The settings converter
    /// still READS an array (for hand edits), but a service older than #5288 cannot deserialize one: the whole
    /// service then fails to start, collection included. The same service reads a comma string as an invalid
    /// range and degrades only that listener to loopback-only. After a rollback the wizard's output must
    /// therefore stay the safe shape.</para>
    ///
    /// <para>Text <see cref="CidrAllowList.TryParse"/> refuses comes back as typed: the wizard validates through
    /// the bind resolvers before it builds a block and re-checks the final text, so that is only a direct
    /// caller's input. No second split, trim or de-dupe lives here. Pure.</para>
    /// </summary>
    internal static string AllowFromText(string allowFrom)
        => CidrAllowList.TryParse(allowFrom, out var list)
           && (list.Count > 1 || allowFrom.Contains(',', StringComparison.Ordinal))
            ? list.ToString()
            : allowFrom;

    /// <summary>
    /// The keys <see cref="BuildMcpNetworkBlock"/> writes (#4743). BOTH token keys are owned even though a
    /// block carries only one: switching a plaintext <c>token</c> to an <c>encryptedToken</c> must not
    /// leave both behind. A test pins that every key the builder writes is listed.
    /// </summary>
    internal static readonly IReadOnlyList<string> McpNetworkOwnedKeys = ["listen", "allowFrom", "token", "encryptedToken"];

    /// <summary>
    /// The active (uncommented) <c>mcp.network</c> block the wizard writes. Exactly one of
    /// <paramref name="encryptedToken"/> / <paramref name="plaintextToken"/> is non-null: the wizard
    /// prefers <c>encryptedToken</c> (a DPAPI blob) and only emits a plaintext <c>token</c> when
    /// preserving an existing plaintext value the operator chose to keep. #5288: <paramref name="allowFrom"/>
    /// may be one CIDR or a comma list, and is written as ONE JSON string either way, never an array
    /// (<see cref="AllowFromText"/>). Pure.
    /// </summary>
    internal static string BuildMcpNetworkBlock(
        string listen, string allowFrom, string? encryptedToken, string? plaintextToken)
    {
        var tokenLine = encryptedToken is not null
            ? FieldIndent + $"\"encryptedToken\": {JsonString(encryptedToken)}  // DPAPI bearer token (from --encrypt-password); required to expose.\n"
            : FieldIndent + $"\"token\": {JsonString(plaintextToken)}  // plaintext bearer token (dev only; prefer encryptedToken).\n";

        return
            "\"network\": {\n" +
            FieldIndent + $"\"listen\": {JsonString(listen)},  // bind IP; 0.0.0.0 = all interfaces.\n" +
            FieldIndent + $"\"allowFrom\": {JsonString(AllowFromText(allowFrom))},  // in-app RemoteIpAddress check + firewall CIDR (loopback always allowed).\n" +
            tokenLine +
            ChildIndent + "}";
    }

    /// <summary>
    /// The keys <see cref="BuildWebNetworkBlock"/> writes (#4743), with both token keys owned for the reason
    /// <see cref="McpNetworkOwnedKeys"/> gives. <c>tls</c>, <c>oidc</c> and any key added later are NOT
    /// listed on purpose: everything the wizard does not own is kept, so this list never has to learn a
    /// new setting before the wizard stops deleting it.
    /// </summary>
    internal static readonly IReadOnlyList<string> WebNetworkOwnedKeys = ["listen", "allowFrom", "token", "encryptedToken"];

    /// <summary>
    /// The active (uncommented) <c>web.network</c> block the wizard writes (#1617) — the web-dashboard twin
    /// of <see cref="BuildMcpNetworkBlock"/>, same exactly-one-of token contract: the wizard prefers
    /// <c>encryptedToken</c> (a DPAPI blob) and only emits a plaintext <c>token</c> when preserving an
    /// existing plaintext value the operator chose to keep. #5288: <paramref name="allowFrom"/> may be one CIDR
    /// or a comma list, and is written as ONE JSON string either way, never an array
    /// (<see cref="AllowFromText"/>). Pure.
    /// </summary>
    internal static string BuildWebNetworkBlock(
        string listen, string allowFrom, string? encryptedToken, string? plaintextToken)
    {
        var tokenLine = encryptedToken is not null
            ? FieldIndent + $"\"encryptedToken\": {JsonString(encryptedToken)}  // DPAPI access token (from --encrypt-password); the browser login secret.\n"
            : FieldIndent + $"\"token\": {JsonString(plaintextToken)}  // plaintext access token (dev only; prefer encryptedToken).\n";

        return
            "\"network\": {\n" +
            FieldIndent + $"\"listen\": {JsonString(listen)},  // bind IP; 0.0.0.0 = all interfaces.\n" +
            FieldIndent + $"\"allowFrom\": {JsonString(AllowFromText(allowFrom))},  // in-app RemoteIpAddress check + firewall CIDR (loopback always allowed).\n" +
            tokenLine +
            ChildIndent + "}";
    }

    /// <summary>
    /// The one line the wizard prints after writing when
    /// <see cref="UpsertNetworkBlock(string, string, string, IReadOnlyCollection{string}, out IReadOnlyList{string})"/>
    /// kept old members (#4743): <c>Kept web.network.tls and web.network.oidc from the existing darling.json.</c>
    /// Null when nothing was kept, so the caller prints nothing. A repeated path is named once. Pure.
    /// </summary>
    internal static string? FormatKeptLine(IReadOnlyList<string> keptPaths)
    {
        var distinct = new List<string>(keptPaths.Count);
        foreach (var path in keptPaths)
        {
            if (!distinct.Contains(path))
            {
                distinct.Add(path);
            }
        }

        if (distinct.Count == 0)
        {
            return null;
        }

        var names = distinct.Count == 1
            ? distinct[0]
            : string.Join(", ", distinct.GetRange(0, distinct.Count - 1)) + " and " + distinct[^1];
        return $"Kept {names} from the existing darling.json.";
    }

    /// <summary>
    /// A numbered menu of the local IPv4 addresses (the caller appends the two always-available choices
    /// outside this list). Pure — the caller does the (impure) NIC enumeration and passes the result in.
    /// </summary>
    internal static string FormatAdapterMenu(IReadOnlyList<(string Name, string Address)> adapters)
    {
        if (adapters.Count == 0)
        {
            return "  (no non-loopback IPv4 addresses were found on this machine)";
        }

        var lines = new List<string>(adapters.Count);
        for (var i = 0; i < adapters.Count; i++)
        {
            lines.Add($"  [{i + 1}] {adapters[i].Address}  ({adapters[i].Name})");
        }

        return string.Join("\n", lines);
    }

    /// <summary>
    /// One surface's exposure verdict, formatted for the wizard's status display. The caller derives
    /// these fields from the REAL resolvers (<c>ResolveNetworkExposure</c> / <c>ResolveMcpBind</c>) so
    /// this only formats; it never decides. <paramref name="role"/> is null for MCP (no remote role).
    /// Pure.
    /// </summary>
    internal static string FormatExposureState(
        string surfaceLabel, bool exposed, string? listen, string? cidr, string? role, string? degradeReason)
    {
        if (exposed)
        {
            var roleSuffix = string.IsNullOrEmpty(role) ? "" : $", role {role}";
            return $"  {surfaceLabel}: EXPOSED — listen {listen}, allowFrom {cidr}{roleSuffix}";
        }

        if (!string.IsNullOrEmpty(degradeReason))
        {
            return $"  {surfaceLabel}: loopback-only (DEGRADED — {degradeReason})";
        }

        return $"  {surfaceLabel}: loopback-only (secure default)";
    }

    /// <summary>
    /// The replacement for an old LIVE <c>network</c> object (#4743): <paramref name="newBlock"/> with every
    /// old member the wizard does not own appended before its closing brace. Each kept member is its text
    /// from the key through the value, verbatim, plus a comment on the same line; the commas are ours, so
    /// the last builder member gains one and the last kept member has none. An old value that is not an
    /// object (<c>"network": null</c>) has nothing to keep. Pure.
    /// </summary>
    private static string CarryUnownedMembers(
        string json, RegionKind[] regions, ObjectMemberSpan oldNetwork, string newBlock,
        IReadOnlyCollection<string> ownedKeys, out IReadOnlyList<string> carriedKeys)
    {
        carriedKeys = [];
        var open = oldNetwork.ValueStart;
        var close = oldNetwork.ValueEnd - 1;
        if (json[open] != '{' || close <= open || json[close] != '}')
        {
            return newBlock;
        }

        var names = new List<string>();
        var entries = new List<(string Core, string Comment)>();
        foreach (var member in EnumerateObjectMembers(json, regions, open, close))
        {
            if (IsOwned(ownedKeys, member.Name))
            {
                continue;
            }

            names.Add(member.Name);
            entries.Add((json[member.KeyStart..member.ValueEnd], SameLineComment(json, regions, member.ValueEnd, close)));
        }

        if (entries.Count == 0)
        {
            return newBlock;
        }

        var blockRegions = Classify(newBlock);
        var blockOpen = FindRootObjectOpen(newBlock, blockRegions);
        var blockClose = blockOpen < 0 ? -1 : FindMatchingClose(newBlock, blockRegions, blockOpen);
        if (blockClose < 0 || newBlock[LineStartOf(newBlock, blockClose)..blockClose].Trim().Length != 0)
        {
            throw new InvalidOperationException("The replacement network block must be an object whose closing brace starts its own line.");
        }

        var lines = new List<string>(entries.Count);
        for (var i = 0; i < entries.Count; i++)
        {
            lines.Add(entries[i].Core + (i < entries.Count - 1 ? "," : "") + entries[i].Comment);
        }

        carriedKeys = names;
        return InsertAsLastMember(newBlock, blockRegions, blockOpen, blockClose, string.Join("\n" + FieldIndent, lines), FieldIndent);
    }

    /// <summary>True when <paramref name="name"/> is one of <paramref name="ownedKeys"/>, ignoring case (the config parser does).</summary>
    private static bool IsOwned(IReadOnlyCollection<string> ownedKeys, string name)
    {
        foreach (var owned in ownedKeys)
        {
            if (string.Equals(owned, name, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Every DIRECT-CHILD member of the object whose braces are <paramref name="objectOpen"/>/<paramref name="objectClose"/>,
    /// in document order, with the key's unescaped name and its text extent (key start through value end). The
    /// sibling of <see cref="FindObjectValueSpan"/> that yields all members instead of matching one. Keys and
    /// braces inside comments and strings are invisible, like everywhere else in this type. Pure.
    /// </summary>
    private static List<(string Name, int KeyStart, int ValueEnd)> EnumerateObjectMembers(
        string json, RegionKind[] regions, int objectOpen, int objectClose)
    {
        var members = new List<(string Name, int KeyStart, int ValueEnd)>();
        var i = objectOpen + 1;
        while (i < objectClose)
        {
            if (regions[i] != RegionKind.StringLiteral)
            {
                i++;
                continue;
            }

            var token = ReadStringToken(json, regions, i);
            var colon = NextCodeIndex(json, regions, token.End, objectClose);
            if (colon < 0 || json[colon] != ':')
            {
                i = token.End;
                continue;
            }

            var valueStart = NextValueIndex(json, regions, colon + 1, objectClose);
            if (valueStart < 0)
            {
                break;
            }

            var valueEnd = ValueEndFrom(json, regions, valueStart, objectClose);
            members.Add((KeyName(json, token), token.Start, valueEnd));
            i = Math.Max(valueEnd, token.End);
        }

        return members;
    }

    /// <summary>The key's name with JSON escapes resolved (a <c>l</c> spelling of <c>listen</c> is still <c>listen</c> to the parser); the raw text if it does not decode.</summary>
    private static string KeyName(string json, StringToken token)
    {
        try
        {
            return JsonSerializer.Deserialize<string>(json.AsSpan(token.Start, token.End - token.Start)) ?? token.Content;
        }
        catch (JsonException)
        {
            return token.Content;
        }
    }

    /// <summary>
    /// The comment that trails a member's value on the SAME line, with the whitespace before it and WITHOUT
    /// the member's own separating comma (the caller places commas): for <c>"tls": {...},  // note</c> this
    /// returns <c>  // note</c>. Empty when no comment follows on that line. A block comment counts only when
    /// it ends the line, because otherwise it may belong to the next member. Pure.
    /// </summary>
    private static string SameLineComment(string json, RegionKind[] regions, int valueEnd, int limit)
    {
        var i = SkipHorizontalSpace(json, valueEnd, limit);
        var comma = -1;
        if (i < limit && regions[i] == RegionKind.Code && json[i] == ',')
        {
            comma = i;
            i = SkipHorizontalSpace(json, i + 1, limit);
        }

        if (i >= limit)
        {
            return "";
        }

        var end = i;
        if (regions[i] == RegionKind.LineComment)
        {
            while (end < limit && regions[end] == RegionKind.LineComment)
            {
                end++;
            }

            while (end > i && json[end - 1] == '\r')
            {
                end--;
            }
        }
        else if (regions[i] == RegionKind.BlockComment)
        {
            while (end < limit && regions[end] == RegionKind.BlockComment)
            {
                end++;
            }

            var after = SkipHorizontalSpace(json, end, limit);
            if (json.IndexOf('\n', i, end - i) >= 0 || (after < limit && json[after] != '\r' && json[after] != '\n'))
            {
                return "";
            }
        }
        else
        {
            return "";
        }

        return comma < 0 ? json[valueEnd..end] : json[valueEnd..comma] + json[(comma + 1)..end];
    }

    /// <summary>Index of the first character at or after <paramref name="from"/> (before <paramref name="limit"/>) that is not a space or tab.</summary>
    private static int SkipHorizontalSpace(string json, int from, int limit)
    {
        while (from < limit && (json[from] == ' ' || json[from] == '\t'))
        {
            from++;
        }

        return from;
    }

    /* ------------------------------------------------------------------------------------------------
       Internal scan helpers — all pure, all code-region-aware via the Classify labels.
       ------------------------------------------------------------------------------------------------ */

    private readonly record struct StringToken(int Start, int End, string Content);

    /// <summary>
    /// Reads the string token that starts at <paramref name="quoteIndex"/> (a code-adjacent opening
    /// quote), returning its extent (End is one past the closing quote) and its raw inner content. Our
    /// target keys have no escapes, so the raw slice is the name.
    /// </summary>
    private static StringToken ReadStringToken(string json, RegionKind[] regions, int quoteIndex)
    {
        var start = quoteIndex;
        var i = quoteIndex + 1;
        while (i < json.Length && regions[i] == RegionKind.StringLiteral)
        {
            i++;
        }

        /* i now points one past the closing quote (the first non-string index). */
        var innerStart = start + 1;
        var innerEnd = i - 1;
        var content = innerEnd > innerStart ? json[innerStart..innerEnd] : "";
        return new StringToken(start, i, content);
    }

    /// <summary>Index of the next code character in [<paramref name="from"/>, <paramref name="limit"/>), skipping whitespace + comments; -1 if none.</summary>
    private static int NextCodeIndex(string json, RegionKind[] regions, int from, int limit)
    {
        for (var i = from; i < limit && i < json.Length; i++)
        {
            if (regions[i] == RegionKind.Code && !char.IsWhiteSpace(json[i]))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Index where the value after a colon starts: like <see cref="NextCodeIndex"/> but a string value counts
    /// too. A string is entirely <see cref="RegionKind.StringLiteral"/> (quotes included), so the code-only
    /// walk skipped a string value and reported the comma after it — or, for a last member, found nothing (#4743).
    /// </summary>
    private static int NextValueIndex(string json, RegionKind[] regions, int from, int limit)
    {
        for (var i = from; i < limit && i < json.Length; i++)
        {
            if (regions[i] == RegionKind.StringLiteral
                || (regions[i] == RegionKind.Code && !char.IsWhiteSpace(json[i])))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>The next code character (skipping ws + comments), or '\0' if none.</summary>
    private static char NextCodeChar(string json, RegionKind[] regions, int from, int limit)
    {
        var idx = NextCodeIndex(json, regions, from, limit);
        return idx < 0 ? '\0' : json[idx];
    }

    /// <summary>Index of the previous code character before <paramref name="before"/> (down to <paramref name="floor"/>), skipping ws + comments; -1 if none.</summary>
    private static int PreviousCodeIndex(string json, RegionKind[] regions, int before, int floor)
    {
        for (var i = before - 1; i >= floor && i >= 0; i--)
        {
            if (regions[i] == RegionKind.Code && !char.IsWhiteSpace(json[i]))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Index of the end of the previous MEMBER before <paramref name="before"/> — the last character that
    /// is either code (skipping ws + comments, like <see cref="PreviousCodeIndex"/>) or part of a string
    /// literal; -1 if none. The distinction is the #2073 bug: a string VALUE is entirely
    /// <see cref="RegionKind.StringLiteral"/> (quotes included), so the code-only walk skips the whole
    /// value and lands on the colon BEFORE it — and a comma synthesized "after the previous member" then
    /// splices in after the colon (<c>"dataDirectory":, "…"</c>), invalid JSON. A string-literal hit here
    /// is always a real value's closing quote: quoted runs inside comments classify as Comment, and any
    /// trailing comma after the string is Code and would be found first.
    /// </summary>
    private static int PreviousMemberEndIndex(string json, RegionKind[] regions, int before, int floor)
    {
        for (var i = before - 1; i >= floor && i >= 0; i--)
        {
            if (regions[i] == RegionKind.StringLiteral
                || (regions[i] == RegionKind.Code && !char.IsWhiteSpace(json[i])))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// End (exclusive) of the value that starts at <paramref name="valueStart"/>: the matching brace for
    /// an object/array, the closing quote for a string, or the char after the last non-whitespace token
    /// for a primitive (up to the next code comma or closing brace/bracket).
    /// </summary>
    private static int ValueEndFrom(string json, RegionKind[] regions, int valueStart, int limit)
    {
        var c = json[valueStart];
        if (c == '{' || c == '[')
        {
            var close = FindMatchingClose(json, regions, valueStart);
            return close < 0 ? limit : close + 1;
        }

        if (c == '"')
        {
            return ReadStringToken(json, regions, valueStart).End;
        }

        /* Primitive (true/false/null/number): runs to the next code comma or closing brace/bracket. */
        var lastNonWs = valueStart;
        for (var i = valueStart; i < limit && i < json.Length; i++)
        {
            if (regions[i] != RegionKind.Code)
            {
                continue;
            }

            var ch = json[i];
            if (ch == ',' || ch == '}' || ch == ']')
            {
                break;
            }

            if (!char.IsWhiteSpace(ch))
            {
                lastNonWs = i;
            }
        }

        return lastNonWs + 1;
    }

    /// <summary>
    /// Insert <paramref name="member"/> as the last member of the object whose braces are
    /// <paramref name="objectOpen"/>/<paramref name="objectClose"/>, on its own line(s) just before the
    /// closing brace's line, prepending <paramref name="childIndent"/>. A separating comma is synthesized
    /// right after the previous member's value only when it lacks a trailing comma (comments in between
    /// are irrelevant — they are whitespace to the parser). Pure.
    /// </summary>
    private static string InsertAsLastMember(
        string json, RegionKind[] regions, int objectOpen, int objectClose, string member, string childIndent)
    {
        var lineStart = LineStartOf(json, objectClose);
        /* Member-end, not code-only (#2073): the previous member's value may be a string, whose every
           character (quotes included) is StringLiteral region — the code-only walk would skip it and put
           the synthesized comma after the member's COLON. */
        var lastCodeIdx = PreviousMemberEndIndex(json, regions, objectClose, objectOpen);

        /* Empty object (only the open brace precedes the close): drop the member in on its own line. */
        if (lastCodeIdx < 0 || lastCodeIdx == objectOpen)
        {
            return json[..lineStart] + childIndent + member + "\n" + json[lineStart..];
        }

        var lastCode = json[lastCodeIdx];
        var needComma = lastCode != ',';

        if (needComma)
        {
            return json[..(lastCodeIdx + 1)] + ","
                + json[(lastCodeIdx + 1)..lineStart]
                + childIndent + member + "\n"
                + json[lineStart..];
        }

        return json[..lineStart] + childIndent + member + "\n" + json[lineStart..];
    }

    /// <summary>Index of the start of the line containing <paramref name="index"/> (just after the previous newline, or 0).</summary>
    private static int LineStartOf(string json, int index)
    {
        for (var i = index - 1; i >= 0; i--)
        {
            if (json[i] == '\n')
            {
                return i + 1;
            }
        }

        return 0;
    }

    /// <summary>JSON-encodes a value as a quoted string (safe for any character). Pure.</summary>
    private static string JsonString(string? value) => JsonSerializer.Serialize(value ?? "");
}
