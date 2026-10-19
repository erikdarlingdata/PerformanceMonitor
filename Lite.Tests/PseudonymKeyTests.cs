/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6066 in Lite: its key lives in Windows Credential Manager (here a fake vault), is made once and read back the same, is never
/// replaced when it cannot be read, and gives the same three pseudonyms as Darling for the shared test key.
/// </summary>
public sealed class PseudonymKeyTests
{
    private sealed class FakeVault : IPseudonymKeyVault
    {
        public CredentialReadOutcome Outcome = CredentialReadOutcome.NotFound;
        public string? Stored;
        public string? ReadFailure;
        public string? WriteFailure;
        public int Writes;

        public CredentialReadOutcome Read(out string? text, out string? failure)
        {
            text = Outcome == CredentialReadOutcome.Found ? Stored : null;
            failure = Outcome == CredentialReadOutcome.Unreadable ? ReadFailure ?? "Windows returned error 5" : null;
            return Outcome;
        }

        public string? Write(string value)
        {
            Writes++;
            if (WriteFailure is not null) return WriteFailure;
            Stored = value;
            Outcome = CredentialReadOutcome.Found;
            return null;
        }
    }

    private static PseudonymKey SharedKey()
    {
        var bytes = new byte[32];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)i;
        return new PseudonymKey(bytes);
    }

    /* The shared vector (rule 14): the same three lines as Darling's McpPseudonymTests, so both apps produce the same text. */
    [Theory]
    [InlineData("login", "CORP\\Alice", "login#5f484746")]
    [InlineData("login", "sa", "login#8f799a85")]
    [InlineData("host", "APP-01", "host#a06e9ea5")]
    public void TheSharedVector_GivesTheSameThreePseudonymsAsDarling(string kind, string name, string expected)
    {
        Assert.Equal(expected, SharedKey().Pseudonym(kind, name));
        string field = kind == "login" ? "login_name" : "host_name";
        Assert.Equal(expected, McpDataDetailMasker.MaskField(field, name, McpDataDetail.MetadataOnly, SharedKey()));
    }

    [Fact]
    public async Task TheKey_IsMadeOnce_ThenReadBackTheSame_ByAnotherSource()
    {
        var vault = new FakeVault();
        var first = new LitePseudonymKeySource(vault);
        PseudonymKey key = (await first.GetAsync())!;
        Assert.NotNull(key);
        Assert.Equal(1, vault.Writes);
        string made = McpDataDetailMasker.MaskField("login_name", "CORP\\alice", McpDataDetail.MetadataOnly, key)!;

        /* A restart: a new source over the same vault reads the entry and writes nothing. */
        var second = new LitePseudonymKeySource(vault);
        PseudonymKey again = (await second.GetAsync())!;
        Assert.Equal(1, vault.Writes);
        Assert.Equal(made, McpDataDetailMasker.MaskField("login_name", "CORP\\alice", McpDataDetail.MetadataOnly, again));
        Assert.Equal(44, vault.Stored!.Length);
    }

    [Fact]
    public async Task AnEntryItCannotRead_IsNeverReplaced_AndTheAnswerIsNumberedPerAnswer()
    {
        var warnings = new List<string>();
        var vault = new FakeVault { Outcome = CredentialReadOutcome.Unreadable, Stored = "keep-me" };
        var source = new LitePseudonymKeySource(vault, warnings.Add);

        Assert.Null(await source.GetAsync());
        Assert.Null(await source.GetAsync());
        Assert.Equal(0, vault.Writes);
        Assert.Equal("keep-me", vault.Stored);
        string warning = Assert.Single(warnings);
        Assert.Contains("never replaces", warning, StringComparison.Ordinal);
        Assert.DoesNotContain("keep-me", warning, StringComparison.Ordinal);

        var masker = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly, await source.GetAsync());
        Assert.Equal("login#1", masker.Field("login_name", "CORP\\alice"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("not base64 at all !!")]
    [InlineData("AAEC")]
    public async Task AnEntryThatIsNotA32ByteKey_IsLeftAsItIs(string entry)
    {
        var vault = new FakeVault { Outcome = CredentialReadOutcome.Found, Stored = entry };
        var source = new LitePseudonymKeySource(vault, _ => { });

        Assert.Null(await source.GetAsync());
        Assert.Equal(0, vault.Writes);
        Assert.Equal(entry, vault.Stored);
    }

    [Fact]
    public async Task ACredentialManagerThatCannotSave_GivesNoKey_AndTriesAgainAfterTheRetryWait()
    {
        var vault = new FakeVault { WriteFailure = "access denied" };
        var warnings = new List<string>();
        var source = new LitePseudonymKeySource(vault, warnings.Add);

        Assert.Null(await source.GetAsync());
        Assert.Equal(1, vault.Writes);
        Assert.Null(await source.GetAsync());
        Assert.Equal(1, vault.Writes);
        Assert.Single(warnings);
    }

    [Fact]
    public void TheEntry_IsNamedForLite_AndTheImportCopiesNoCredential()
    {
        Assert.Equal("PseudonymKey", CredentialManagerPseudonymKeyVault.EntryName);
        /* Import Settings copies named files only; the entry is in Credential Manager, so no file list can carry it. */
        Assert.DoesNotContain(SettingsImport.ConfigFiles, f => f.Contains("seudonym", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(SettingsImport.RootFiles, f => f.Contains("seudonym", StringComparison.OrdinalIgnoreCase));
    }
}
