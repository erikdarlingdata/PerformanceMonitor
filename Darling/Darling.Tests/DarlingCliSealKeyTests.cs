/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>The key <c>--add-server</c> seals with (#5366): the key file when it opens and matches the published key, else
/// the published key when the service says it is healthy.</summary>
public sealed class DarlingCliSealKeyTests
{
    private static readonly PasswordPrivateKey FileKey = PasswordPrivateKey.Generate();

    private static PublishedPasswordKey Published(PasswordPrivateKey key) =>
        new(key.PublicKey.KeyId, key.PublicKey.Spki, PasswordSeal.Algorithm);

    private static PasswordKeyServiceState State(string state, string? note = null) =>
        new("host-example", FileKey.PublicKey.KeyId, state, note, DateTime.UtcNow);

    private static readonly PasswordBinding Binding = PasswordBinding.ForServer(
        ServerConnectionIdentity.FromStoredColumns("sql-example", 0, "sqlserver", null, false, "sql", "monitor", "Mandatory", false, false));

    [Fact]
    public void AKeyFileThatMatchesThePublishedKey_IsUsed_AndOpensWhatItSeals()
    {
        var decision = DarlingCliSealKey.Decide(Published(FileKey), State("ok"), FileKey.ExportPkcs8());

        Assert.True(decision.Ring.Status.CanSeal);
        Assert.Null(decision.Notice);
        Assert.Equal("p@ss-not-real", decision.Ring.Open(decision.Ring.Seal("p@ss-not-real", Binding), Binding));
    }

    [Fact]
    public void AKeyFileThatIsNotThePublishedKey_Refuses()
    {
        using var other = PasswordPrivateKey.Generate();

        var decision = DarlingCliSealKey.Decide(Published(FileKey), State("ok"), other.ExportPkcs8());

        Assert.False(decision.Ring.Status.CanSeal);
        Assert.Equal(DarlingCliSealKey.FileMismatchReason, decision.Ring.Status.Reason);
    }

    [Fact]
    public void WithNoKeyFile_ThePublishedKeyIsUsedWhenTheServiceIsOk_AndTheKeyIdIsPrinted()
    {
        var decision = DarlingCliSealKey.Decide(Published(FileKey), State("ok"), null);

        Assert.True(decision.Ring.Status.CanSeal);
        Assert.Equal(DarlingCliSealKey.NoticeFor(FileKey.PublicKey.KeyId), decision.Notice);
        Assert.Contains("Password key", decision.Notice, StringComparison.Ordinal);
        Assert.Equal("p@ss-not-real", FileKey.PublicKey.KeyId is { } ? PasswordSeal.Open(decision.Ring.Seal("p@ss-not-real", Binding), FileKey, Binding) : "");
    }

    [Fact]
    public void WithNoKeyFile_AServiceThatIsNotOk_NoServiceState_NoPublishedKey_OrABadKeyId_Refuses()
    {
        Assert.Equal(DarlingCliSealKey.StateReason(State("mismatch", "see the log")), DarlingCliSealKey.Decide(Published(FileKey), State("mismatch", "see the log"), null).Ring.Status.Reason);
        Assert.Equal(DarlingCliSealKey.NoStateReason, DarlingCliSealKey.Decide(Published(FileKey), null, null).Ring.Status.Reason);
        Assert.Equal(DarlingCliSealKey.NoKeyReason, DarlingCliSealKey.Decide(null, State("ok"), null).Ring.Status.Reason);
        Assert.Equal(
            DarlingCliSealKey.InvalidKeyReason,
            DarlingCliSealKey.Decide(new PublishedKeyWithWrongId(FileKey).Value, State("ok"), null).Ring.Status.Reason);
    }

    [Fact]
    public void WithNoKeyFile_AnOkStateRowForAnotherKey_OrWithNoKeyId_Refuses_AndOneForThePublishedKey_Seals()
    {
        var published = Published(FileKey);
        var otherKey = new PasswordServiceStateFor("0000000000000000", "ok");
        var noKeyId = new PasswordServiceStateFor(null, "ok");

        Assert.Equal(DarlingCliSealKey.NoStateReason, DarlingCliSealKey.Decide(published, otherKey.Value, null).Ring.Status.Reason);
        Assert.Equal(DarlingCliSealKey.NoStateReason, DarlingCliSealKey.Decide(published, noKeyId.Value, null).Ring.Status.Reason);
        Assert.Null(DarlingCliSealKey.Decide(published, State("ok"), null).Ring.Status.Reason);
    }

    private sealed class PasswordServiceStateFor
    {
        public PasswordServiceStateFor(string? keyId, string state) =>
            Value = new("host-example", keyId, state, null, DateTime.UtcNow);

        public PasswordKeyServiceState Value { get; }
    }

    private sealed class PublishedKeyWithWrongId
    {
        public PublishedKeyWithWrongId(PasswordPrivateKey key) => Value = new("0000000000000000", key.PublicKey.Spki, PasswordSeal.Algorithm);

        public PublishedPasswordKey Value { get; }
    }

    [Fact]
    public void ARefusedDecision_RefusesALiteralPassword_WithItsReason_ThroughTheAddRequest()
    {
        var decision = DarlingCliSealKey.Decide(Published(FileKey), null, null);

        var (entries, invalid, _) = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools.ParseRequest(
            "[{\"host\":\"sql-example\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"p@ss-not-real\"}]",
            decision.Ring.Status, allowSecretReferences: true);

        Assert.Empty(entries);
        Assert.Contains(DarlingCliSealKey.NoStateReason, Assert.Single(invalid).Detail, StringComparison.Ordinal);
    }
}
