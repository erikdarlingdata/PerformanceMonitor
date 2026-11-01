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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>The command types of the people directory (#7107), as the service executor names them.</summary>
public static class ViewerPersonCommands
{
    public const string AddPerson = "add_person";
    public const string RenamePerson = "rename_person";
    public const string SetPersonEnabled = "set_person_enabled";
    public const string AddIdentity = "add_person_identity";
    public const string RemoveIdentity = "remove_person_identity";
    public const string AddGrant = "add_person_grant";
    public const string RemoveGrant = "remove_person_grant";

    public static string AddPersonArgs(string displayName) => JsonSerializer.Serialize(new { display_name = displayName });

    public static string RenamePersonArgs(long personId, string displayName) => JsonSerializer.Serialize(new { person_id = personId, display_name = displayName });

    public static string SetPersonEnabledArgs(long personId, bool enabled) => JsonSerializer.Serialize(new { person_id = personId, enabled });

    public static string AddIdentityArgs(long personId, string kind, string externalId) =>
        JsonSerializer.Serialize(new { person_id = personId, kind, external_id = externalId });

    public static string RemoveIdentityArgs(long identityId) => JsonSerializer.Serialize(new { identity_id = identityId });

    public static string RemoveGrantArgs(long grantId) => JsonSerializer.Serialize(new { grant_id = grantId });

    /// <summary>The args of an add_person_grant: <c>kinds</c> only for a capability that takes kinds, and at most one of the two scope ids.</summary>
    public static string AddGrantArgs(long personId, string capability, IReadOnlyCollection<string>? kinds, int? scopeServerId, int? scopeTagId)
    {
        var node = new JsonObject { ["person_id"] = personId, ["capability"] = capability };
        if (PersonCapabilities.TakesKinds(capability) && kinds is { Count: > 0 })
        {
            var array = new JsonArray();
            foreach (var kind in ServerActionNames.All.Where(kinds.Contains))
                array.Add(kind);
            node["kinds"] = array;
        }

        if (scopeServerId is { } server)
            node["scope_server_id"] = server;
        else if (scopeTagId is { } tag)
            node["scope_tag_id"] = tag;
        return node.ToJsonString();
    }

    /// <summary>
    /// Sends one command through <paramref name="run"/> and reads the service's answer. A null result means the wait ended with the command still running
    /// in the service: the answer says so and tells the person to read the list again before trying twice. A refusal comes back as the answer, with the
    /// service's own sentence.
    /// </summary>
    public static async Task<PersonCommandAnswer> SendAsync(PersonCommandRun run, string commandType, string argsJson, string login, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(run);
        _ = AgentChangeUndo.PersonActor(login);
        var result = await run(commandType, argsJson, login, cancellationToken);
        if (result is null)
            return new PersonCommandAnswer(false, "timed_out", null, "The service has not answered yet. Read the list again before trying twice.");
        return PersonCommandAnswer.Read(result.ResultJson, result.ResultStatus);
    }
}

/// <summary>Enqueues one people command and waits for its terminal result (null: the service was still running it).</summary>
public delegate Task<CommandResult?> PersonCommandRun(string commandType, string argsJson, string login, CancellationToken cancellationToken);

/// <summary>What the service answered to one people command (#7107): <c>{status, message, code?, person_id?, identity_id?, grant_id?, journal_id?}</c>.</summary>
public sealed record PersonCommandAnswer(bool Succeeded, string Status, string? Code, string Message, long? PersonId = null, long? IdentityId = null, long? GrantId = null)
{
    public static PersonCommandAnswer Read(string? json, string? resultStatus)
    {
        try
        {
            if (json is not null && JsonNode.Parse(json) is JsonObject o)
            {
                var status = Text(o, "status");
                var message = Text(o, "message");
                if (status is not null && message is not null)
                    return new PersonCommandAnswer(status != "refused", status, Text(o, "code"), message, Number(o, "person_id"), Number(o, "identity_id"), Number(o, "grant_id"));
                if (Text(o, "error") is { } error && !string.IsNullOrWhiteSpace(error))
                    return new PersonCommandAnswer(false, resultStatus ?? "failed", null, error);
            }
        }
        catch (JsonException)
        {
            /* fall through to the plain sentence */
        }

        return new PersonCommandAnswer(false, resultStatus ?? "failed", null, "The service could not make that change.");
    }

    private static string? Text(JsonObject o, string name) => o[name] is JsonValue v && v.TryGetValue(out string? s) ? s : null;

    private static long? Number(JsonObject o, string name) => o[name] is JsonValue v && v.TryGetValue(out long n) ? n : null;
}

/// <summary>One row of the People grid.</summary>
public sealed class PersonViewRow
{
    public PersonViewRow(PersonRow person)
    {
        Person = person ?? throw new ArgumentNullException(nameof(person));
    }

    public PersonRow Person { get; }

    public long PersonId => Person.PersonId;

    public string DisplayName => Person.DisplayName;

    public bool Enabled => Person.Enabled;

    public string StateDisplay => Person.Enabled ? "Enabled" : "Disabled";

    public string IdentitiesDisplay => Person.Identities.Count == 0 ? "none" : Person.Identities.Count.ToString(CultureInfo.InvariantCulture);

    public string GrantsDisplay => Person.Grants.Count == 0 ? "none" : Person.Grants.Count.ToString(CultureInfo.InvariantCulture);

    public DateTime UpdatedAtUtc => Person.UpdatedAt;

    public string UpdatedDisplay => Person.UpdatedAt.ToLocalTime().ToString("g", CultureInfo.CurrentCulture);

    public string UpdatedBy => ServerActionViewRow.WithoutPersonPrefix(Person.UpdatedBy);

    public IReadOnlyList<PersonIdentityViewRow> Identities => Person.Identities.Select(i => new PersonIdentityViewRow(i)).ToList();

    public IReadOnlyList<PersonGrantViewRow> Grants => Person.Grants.Select(g => new PersonGrantViewRow(g)).ToList();

    public static IReadOnlyList<PersonViewRow> From(IEnumerable<PersonRow> people) =>
        people.OrderBy(p => p.DisplayName, StringComparer.CurrentCultureIgnoreCase).ThenBy(p => p.PersonId).Select(p => new PersonViewRow(p)).ToList();
}

/// <summary>One identity of a person, as the detail grid shows it.</summary>
public sealed class PersonIdentityViewRow
{
    public PersonIdentityViewRow(PersonIdentityRow identity)
    {
        Identity = identity ?? throw new ArgumentNullException(nameof(identity));
    }

    public PersonIdentityRow Identity { get; }

    public long IdentityId => Identity.IdentityId;

    public string KindDisplay => Identity.Kind == PersonIdentityKinds.Web ? "Web sign-in" : Identity.Kind == PersonIdentityKinds.Slack ? "Slack" : Identity.Kind;

    public string ExternalId => Identity.ExternalId;

    public string AddedDisplay => Identity.CreatedAt.ToLocalTime().ToString("g", CultureInfo.CurrentCulture) + " by " + ServerActionViewRow.WithoutPersonPrefix(Identity.CreatedBy);
}

/// <summary>One grant of a person, as the detail grid shows it.</summary>
public sealed class PersonGrantViewRow
{
    public PersonGrantViewRow(PersonGrantRow grant)
    {
        Grant = grant ?? throw new ArgumentNullException(nameof(grant));
    }

    public PersonGrantRow Grant { get; }

    public long GrantId => Grant.GrantId;

    public string CapabilityDisplay => PeopleText.CapabilityLabel(Grant.Capability);

    public string KindsDisplay => Grant.Kinds.Count == 0 ? "" : string.Join(", ", Grant.Kinds.Select(ServerActionNames.Label));

    /// <summary>The fleet, a server or a tag by name. A server or tag that has been removed still shows the id the grant names.</summary>
    public string ScopeDisplay =>
        Grant.ScopeServerId is { } server ? "Server: " + (Grant.ScopeServerName ?? "(removed, id " + server.ToString(CultureInfo.InvariantCulture) + ")")
        : Grant.ScopeTagId is { } tag ? "Tag: " + (Grant.ScopeTagName ?? "(deleted, id " + tag.ToString(CultureInfo.InvariantCulture) + ")")
        : "The whole fleet";

    public string AddedDisplay => Grant.CreatedAt.ToLocalTime().ToString("g", CultureInfo.CurrentCulture) + " by " + ServerActionViewRow.WithoutPersonPrefix(Grant.CreatedBy);
}

/// <summary>The words of the People screen and the checks its dialogs run before they send anything.</summary>
public static class PeopleText
{
    /// <summary>The lowest store version with the people tables (V237).</summary>
    public const int StoreSchemaVersion = ViewerDataService.PeopleStoreSchemaVersion;

    public const string NeedsUpgradeNote =
        "This store does not have the people tables yet. Upgrade the store to the newest version (start the newest service once, which upgrades it), then open this screen again.";

    public const string ReadOnlyNote =
        "Read-only connection: people can be viewed but not changed here. ";

    public const string NoPeopleNote =
        "No people yet. Add a person, then map the identities they sign in with and give them grants. Nothing reads a grant yet, so adding people changes nothing that anyone can do.";

    /// <summary>True when a store at <paramref name="storeVersion"/> has the people tables. A probe that could not answer (null) reads as yes, like every other fail-open rung check, and a failed read then says what went wrong.</summary>
    public static bool StoreHasPeople(int? storeVersion) => storeVersion is not int v || v >= StoreSchemaVersion;

    public static string CapabilityLabel(string capability) => capability switch
    {
        PersonCapabilities.Ask => "Ask the agent (Level 1)",
        PersonCapabilities.RequestLevel2 => "Request Level 2 changes",
        PersonCapabilities.RequestLevel3 => "Request Level 3 server actions",
        PersonCapabilities.Approve => "Approve server actions",
        PersonCapabilities.Undo => "Undo agent changes",
        PersonCapabilities.DecideNote => "Decide notes",
        PersonCapabilities.Feedback => "Give feedback",
        var other => other,
    };

    public static string WebKeyHelp => "The 43-character sign-in key. The person copies it from the People section of the web page after they sign in.";
}

/// <summary>The identity dialog's checks: builds the external id of a Slack or web identity, or says what is wrong.</summary>
public static class PersonIdentityEditor
{
    /// <summary>The external id for the kind: <c>TEAMID:USERID</c> for Slack (upper-cased), the pasted 43-character key for web. Null, with <paramref name="error"/>, when it is not valid.</summary>
    public static string? TryBuild(string kind, string? slackTeam, string? slackUser, string? webKey, out string? error)
    {
        error = null;
        string candidate;
        if (kind == PersonIdentityKinds.Slack)
        {
            var team = (slackTeam ?? string.Empty).Trim().ToUpperInvariant();
            var user = (slackUser ?? string.Empty).Trim().ToUpperInvariant();
            if (team.Length == 0 || user.Length == 0)
            {
                error = "Enter both the Slack team id (like T01ABC) and the Slack user id (like U02DEF).";
                return null;
            }

            candidate = team + ":" + user;
            if (!PersonDirectory.IsValidExternalId(kind, candidate))
            {
                error = "A Slack team id and user id are letters and digits only, up to 32 characters each.";
                return null;
            }

            return candidate;
        }

        if (kind == PersonIdentityKinds.Web)
        {
            candidate = (webKey ?? string.Empty).Trim();
            if (!PersonDirectory.IsValidExternalId(kind, candidate))
            {
                error = "A web sign-in key is exactly 43 characters: letters, digits, dash and underscore. " + PeopleText.WebKeyHelp;
                return null;
            }

            return candidate;
        }

        error = "Pick Slack or Web sign-in.";
        return null;
    }
}

/// <summary>The grant dialog's checks.</summary>
public static class PersonGrantEditor
{
    /// <summary>Whether the capability needs a list of server action kinds (and so shows the kind boxes).</summary>
    public static bool ShowsKinds(string? capability) => capability is not null && PersonCapabilities.TakesKinds(capability);

    /// <summary>What is wrong with the pick, or null. A capability that takes kinds needs at least one; the others take none.</summary>
    public static string? Error(string? capability, IReadOnlyCollection<string> kinds)
    {
        if (capability is null || !PersonCapabilities.All.Contains(capability))
            return "Pick a capability.";
        if (PersonCapabilities.TakesKinds(capability) && kinds.Count == 0)
            return PeopleText.CapabilityLabel(capability) + " needs at least one server action kind. Tick the kinds this person may cover.";
        return null;
    }
}
