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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// The words a proposal is listed in (#7109), shared by the web page and the Viewer so both read the same: a short readable line for the
/// target of each of the seven Level 3 tools, and the name of whoever made a press. The full facts stay in the proposal's detail.
/// </summary>
internal static class ProposalDisplay
{
    /// <summary>The longest run of a plan handle the list line shows. The detail view has the whole handle.</summary>
    private const int HandleChars = 18;

    /// <summary>
    /// A one-line target for the list, built from the target AFTER <c>mcp.dataDetail</c> has been applied to it, so a value the masker withheld
    /// or replaced (a statement-class value such as a plan guide's <c>definition</c>) is never read from the unmasked target. Only names, ids and
    /// times are used, never a statement, a hint or a module body. A member the target does not carry is left out of the line.
    /// </summary>
    internal static string TargetLine(string tool, JsonNode? maskedTarget)
    {
        if (maskedTarget is not JsonObject target)
            return "";
        var database = Text(target, "database");
        var inDatabase = database is null ? "" : " in " + database;
        switch (tool)
        {
            case "force_query_store_plan":
            case "unforce_query_store_plan":
                {
                    /* The one place the line carries the verb: the web groups force and unforce under one kind, and the target is what tells them apart. */
                    var verb = tool == "force_query_store_plan" ? "Force" : "Unforce";
                    return verb + " plan " + (Text(target, "plan_id") ?? "?") + " of query " + (Text(target, "query_id") ?? "?") + inDatabase;
                }
            case "evict_cached_plan":
                return "Cached plan " + ShortHandle(Text(target, "plan_handle"));
            case "kill_session":
                {
                    if (Text(target, "pid") is { } pid)
                        return "PostgreSQL backend " + pid;
                    var login = Text(target, "login_time") is { } time && DateTimeOffset.TryParse(time, CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed)
                        ? ", logged in " + parsed.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC"
                        : "";
                    return "Session " + (Text(target, "session_id") ?? "?") + inDatabase + login;
                }
            case "recompile_module":
                return (Text(target, "module") ?? "A module") + inDatabase;
            case "create_plan_guide":
                return (target["definition"] is JsonObject definition && Text(definition, "name") is { } guide ? "New plan guide " + guide : "New plan guide") + inDatabase;
            case "drop_plan_guide":
                return "Plan guide " + (Text(target, "guide_name") ?? "?") + inDatabase;
            default:
                return "";
        }
    }

    /// <summary>The name for the key a press was stored under: <c>person:&lt;id&gt;</c> reads as the person's display name, and an administrator
    /// path reads as that, in words. Null when there was no press.</summary>
    internal static string? PressName(string? key, IReadOnlyDictionary<long, string> people)
    {
        if (string.IsNullOrEmpty(key))
            return null;
        if (key.StartsWith("person:", StringComparison.Ordinal))
        {
            return long.TryParse(key.AsSpan("person:".Length), NumberStyles.None, CultureInfo.InvariantCulture, out var id) && people.TryGetValue(id, out var name)
                ? name
                : "a person no longer on the People list";
        }

        return key switch
        {
            "admin:web" => "an administrator in the web (the shared web token, or a sign-in with no person mapped)",
            "admin:viewer" => "an administrator in the Viewer",
            _ => key.StartsWith("admin:", StringComparison.Ordinal) ? "an administrator (" + key["admin:".Length..] + ")" : key,
        };
    }

    /// <summary>The display names of the people the presses name, by person id. Reads the people list once; no person key means no read.</summary>
    internal static async Task<IReadOnlyDictionary<long, string>> PeopleAsync(NpgsqlDataSource source, IEnumerable<string?> keys, CancellationToken cancellationToken)
    {
        if (!keys.Any(k => k is not null && k.StartsWith("person:", StringComparison.Ordinal)))
            return new Dictionary<long, string>();
        var names = new Dictionary<long, string>();
        foreach (var person in await PersonDirectory.ListAsync(source, cancellationToken))
            names[person.PersonId] = person.DisplayName;
        return names;
    }

    private static string ShortHandle(string? handle) =>
        string.IsNullOrEmpty(handle) ? "?" : handle.Length <= HandleChars ? handle : handle[..HandleChars] + "…";

    private static string? Text(JsonObject o, string name) =>
        o[name] is JsonValue v ? (v.TryGetValue(out string? s) ? s : v.ToJsonString()) : null;
}
