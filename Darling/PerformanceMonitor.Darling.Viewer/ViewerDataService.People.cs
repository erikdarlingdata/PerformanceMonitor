/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>What the People screen reads: the people, or the fact that the store is below the rung that holds them.</summary>
public sealed record PeopleReadResult(bool StoreNeedsUpgrade, IReadOnlyList<PersonRow> People);

/// <summary>
/// The People screen's data and writes for the Viewer (#7107). The list is a SELECT on <c>config.person</c>, <c>config.person_identity</c> and
/// <c>config.person_grant</c>, which every store login can read. A change goes through the service as a command, because no login but the store
/// owner writes the three tables and the Viewer never connects as the owner.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The store rung (V237) that adds the people tables.</summary>
    public const int PeopleStoreSchemaVersion = 237;

    /// <summary>The wait for a people command's answer: the service polls its queue about every 5 seconds and a write waits at most 30 seconds in the store.</summary>
    public static readonly TimeSpan PersonCommandTimeout = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Reads the people with their identities and grants. A store below V237 has no tables, so the answer says the store needs an upgrade and
    /// nothing is queried. A probe that cannot tell reads the tables anyway, and a failed read surfaces its own message.
    /// </summary>
    public async Task<PeopleReadResult> ReadPeopleAsync(CancellationToken cancellationToken = default)
    {
        var version = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
        if (!PeopleText.StoreHasPeople(version))
            return new PeopleReadResult(true, Array.Empty<PersonRow>());
        return new PeopleReadResult(false, await PersonDirectory.ListAsync(_dataSource, cancellationToken));
    }

    /// <summary>Enqueues one people command, waits for its answer and removes the finished row (it holds no secret). A read-only role throws <see cref="ViewerReadOnlyException"/>.</summary>
    public Task<PersonCommandAnswer> SendPersonCommandAsync(string commandType, string argsJson, string login, CancellationToken cancellationToken = default) =>
        ViewerPersonCommands.SendAsync(RunPersonCommandAsync, commandType, argsJson, login, cancellationToken);

    private async Task<CommandResult?> RunPersonCommandAsync(string commandType, string argsJson, string login, CancellationToken cancellationToken)
    {
        var commandId = await EnqueueCommandAsync(commandType, targetServerId: null, argsJson, login, cancellationToken);
        var result = await PollCommandResultAsync(commandId, PersonCommandTimeout, cancellationToken);
        if (result is not null)
        {
            try
            {
                await DeleteCommandAsync(commandId, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* The service-side reaper is the backstop. */
            }
        }

        return result;
    }

    public Task<PersonCommandAnswer> AddPersonAsync(string displayName, string login, CancellationToken cancellationToken = default) =>
        SendPersonCommandAsync(ViewerPersonCommands.AddPerson, ViewerPersonCommands.AddPersonArgs(displayName), login, cancellationToken);

    public Task<PersonCommandAnswer> RenamePersonAsync(long personId, string displayName, string login, CancellationToken cancellationToken = default) =>
        SendPersonCommandAsync(ViewerPersonCommands.RenamePerson, ViewerPersonCommands.RenamePersonArgs(personId, displayName), login, cancellationToken);

    public Task<PersonCommandAnswer> SetPersonEnabledAsync(long personId, bool enabled, string login, CancellationToken cancellationToken = default) =>
        SendPersonCommandAsync(ViewerPersonCommands.SetPersonEnabled, ViewerPersonCommands.SetPersonEnabledArgs(personId, enabled), login, cancellationToken);

    public Task<PersonCommandAnswer> AddPersonIdentityAsync(long personId, string kind, string externalId, string login, CancellationToken cancellationToken = default) =>
        SendPersonCommandAsync(ViewerPersonCommands.AddIdentity, ViewerPersonCommands.AddIdentityArgs(personId, kind, externalId), login, cancellationToken);

    public Task<PersonCommandAnswer> RemovePersonIdentityAsync(long identityId, string login, CancellationToken cancellationToken = default) =>
        SendPersonCommandAsync(ViewerPersonCommands.RemoveIdentity, ViewerPersonCommands.RemoveIdentityArgs(identityId), login, cancellationToken);

    public Task<PersonCommandAnswer> AddPersonGrantAsync(long personId, string capability, IReadOnlyCollection<string>? kinds, int? scopeServerId, int? scopeTagId, string login,
        CancellationToken cancellationToken = default) =>
        SendPersonCommandAsync(ViewerPersonCommands.AddGrant, ViewerPersonCommands.AddGrantArgs(personId, capability, kinds, scopeServerId, scopeTagId), login, cancellationToken);

    public Task<PersonCommandAnswer> RemovePersonGrantAsync(long grantId, string login, CancellationToken cancellationToken = default) =>
        SendPersonCommandAsync(ViewerPersonCommands.RemoveGrant, ViewerPersonCommands.RemoveGrantArgs(grantId), login, cancellationToken);
}
