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

/// <summary>
/// Pins what the service does with its password key at start (#5366): every row of the decision table, in the table's
/// order, and the key ring the rest of the service is handed (a ring that is not ready, a ring that refuses, a ring
/// that holds a key).
/// </summary>
public sealed class DarlingPasswordKeyStateTests
{
    private static readonly byte[] SpkiA = PasswordPrivateKey.Generate().PublicKey.Spki;
    private static readonly byte[] SpkiB = PasswordPrivateKey.Generate().PublicKey.Spki;

    private static PublishedKey Published(byte[] spki) => new(PasswordSeal.KeyIdFor(spki), (byte[])spki.Clone());

    private static PasswordKeyFacts Facts(
        bool filePresent = false, bool untrusted = false, string? refusal = null, byte[]? fileSpki = null,
        bool afterOpen = false, PublishedKey? current = null, bool retired = false, bool fromSetting = false) =>
        new(filePresent, untrusted, refusal, fileSpki, afterOpen, current, retired, fromSetting);

    private static string Display(byte[] spki) => PasswordSeal.DisplayKeyId(PasswordSeal.KeyIdFor(spki));

    private static PublishedKey Inconsistent(byte[] spki) => new("not-a-key-id with text", (byte[])spki.Clone());

    [Fact]
    public void An_untrusted_file_is_refused_and_never_replaced_whatever_else_is_true()
    {
        var d = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, untrusted: true, refusal: "The key file is not usable.", fileSpki: SpkiA,
            afterOpen: true, current: Published(SpkiA), retired: true));

        Assert.Equal(PasswordKeyAction.Refused, d.Action);
        Assert.False(d.RetireFileFirst);
        Assert.Null(d.RetireAs);
        Assert.Equal("The key file is not usable.", d.Reason);
    }

    [Fact]
    public void A_file_found_after_the_directory_was_open_that_matches_the_store_is_used_with_a_warning()
    {
        var d = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, fileSpki: SpkiA, afterOpen: true, current: Published(SpkiA)));

        Assert.Equal(PasswordKeyAction.Use, d.Action);
        Assert.False(d.RetireFileFirst);
        Assert.Equal(
            "The credentials directory was open to other users until this start. The password key still matches the store, "
            + "so saved passwords work, but another user may have read it. If that is possible, run --reset-password-key "
            + "and enter the saved passwords again.",
            d.Warning);
    }

    [Fact]
    public void A_file_found_after_the_directory_was_open_that_does_not_match_is_discarded_then_decided_as_absent()
    {
        var withoutStoreKey = DarlingPasswordKeyState.Decide(Facts(filePresent: true, fileSpki: SpkiA, afterOpen: true));
        var otherStoreKey = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, fileSpki: SpkiA, afterOpen: true, current: Published(SpkiB)));

        Assert.Equal(PasswordKeyAction.Generate, withoutStoreKey.Action);
        Assert.True(withoutStoreKey.RetireFileFirst);
        Assert.Equal("discarded", withoutStoreKey.RetireAs);
        Assert.Equal(PasswordKeyAction.Missing, otherStoreKey.Action);
        Assert.True(otherStoreKey.RetireFileFirst);
        Assert.Equal("discarded", otherStoreKey.RetireAs);
        Assert.Contains(PasswordSeal.DisplayKeyId(PasswordSeal.KeyIdFor(SpkiB)), otherStoreKey.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void No_file_and_no_published_key_generates()
    {
        var d = DarlingPasswordKeyState.Decide(Facts());

        Assert.Equal(PasswordKeyAction.Generate, d.Action);
        Assert.False(d.RetireFileFirst);
        Assert.Null(d.RetireAs);
    }

    [Fact]
    public void No_file_with_a_published_key_is_missing_and_names_the_key()
    {
        var d = DarlingPasswordKeyState.Decide(Facts(current: Published(SpkiA)));

        Assert.Equal(PasswordKeyAction.Missing, d.Action);
        Assert.False(d.RetireFileFirst);
        Assert.Contains(PasswordSeal.DisplayKeyId(PasswordSeal.KeyIdFor(SpkiA)), d.Reason, StringComparison.Ordinal);
        Assert.Contains("--reset-password-key", d.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void A_file_with_nothing_published_is_published_unless_the_store_has_retired_it()
    {
        var publish = DarlingPasswordKeyState.Decide(Facts(filePresent: true, fileSpki: SpkiA));
        var reset = DarlingPasswordKeyState.Decide(Facts(filePresent: true, fileSpki: SpkiA, retired: true));

        Assert.Equal(PasswordKeyAction.PublishFile, publish.Action);
        Assert.False(publish.RetireFileFirst);
        Assert.Equal(PasswordKeyAction.Generate, reset.Action);
        Assert.True(reset.RetireFileFirst);
        Assert.Equal("retired", reset.RetireAs);
    }

    [Fact]
    public void A_file_that_matches_the_published_key_is_used_without_a_warning()
    {
        var d = DarlingPasswordKeyState.Decide(Facts(filePresent: true, fileSpki: SpkiA, current: Published(SpkiA)));

        Assert.Equal(PasswordKeyAction.Use, d.Action);
        Assert.Null(d.Warning);
        Assert.Null(d.Reason);
    }

    [Fact]
    public void A_file_that_differs_from_the_published_key_is_a_mismatch_naming_both_keys()
    {
        var other = DarlingPasswordKeyState.Decide(Facts(filePresent: true, fileSpki: SpkiA, current: Published(SpkiB)));
        var sameIdOtherBytes = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, fileSpki: SpkiA, current: new PublishedKey(PasswordSeal.KeyIdFor(SpkiA), SpkiB)));

        Assert.Equal(PasswordKeyAction.Mismatch, other.Action);
        Assert.False(other.RetireFileFirst);
        Assert.Contains(PasswordSeal.DisplayKeyId(PasswordSeal.KeyIdFor(SpkiA)), other.Reason, StringComparison.Ordinal);
        Assert.Contains(PasswordSeal.DisplayKeyId(PasswordSeal.KeyIdFor(SpkiB)), other.Reason, StringComparison.Ordinal);
        Assert.Equal(PasswordKeyAction.Mismatch, sameIdOtherBytes.Action);
    }

    [Fact]
    public void A_published_key_whose_id_does_not_match_its_key_is_a_mismatch_with_a_file_and_refused_without_one()
    {
        var inconsistent = new PublishedKey("not-a-key-id with text", (byte[])SpkiB.Clone());

        var withFile = DarlingPasswordKeyState.Decide(Facts(filePresent: true, fileSpki: SpkiA, current: inconsistent));
        var withFileSameBytes = DarlingPasswordKeyState.Decide(Facts(filePresent: true, fileSpki: SpkiB, current: inconsistent));
        var withoutFile = DarlingPasswordKeyState.Decide(Facts(current: inconsistent));

        Assert.Equal(PasswordKeyAction.Mismatch, withFile.Action);
        Assert.Equal(PasswordKeyAction.Mismatch, withFileSameBytes.Action);
        Assert.Equal(PasswordKeyAction.Refused, withoutFile.Action);
        Assert.False(withoutFile.RetireFileFirst);
        Assert.StartsWith("The store's published password key is inconsistent: its id does not match the key.", withoutFile.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("not-a-key-id", withFile.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("not-a-key-id", withoutFile.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void The_reasons_print_the_key_id_worked_out_from_the_key_not_the_text_the_store_holds()
    {
        var text = "store-held-text-not-an-id";
        var withoutFile = DarlingPasswordKeyState.Decide(Facts(current: new PublishedKey(text, (byte[])SpkiB.Clone())));
        var mismatch = DarlingPasswordKeyState.Decide(Facts(filePresent: true, fileSpki: SpkiA, current: new PublishedKey(text, (byte[])SpkiB.Clone())));

        Assert.DoesNotContain(text, mismatch.Reason, StringComparison.Ordinal);
        Assert.Contains(PasswordSeal.DisplayKeyId(PasswordSeal.KeyIdFor(SpkiB)), mismatch.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain(text, withoutFile.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void A_present_file_with_no_readable_key_is_refused_rather_than_replaced()
    {
        var d = DarlingPasswordKeyState.Decide(Facts(filePresent: true, current: Published(SpkiA)));

        Assert.Equal(PasswordKeyAction.Refused, d.Action);
        Assert.False(d.RetireFileFirst);
    }

    [Fact]
    public void The_table_is_checked_in_order_so_a_retired_flag_does_not_beat_a_matching_published_key()
    {
        var d = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, fileSpki: SpkiA, current: Published(SpkiA), retired: true));

        Assert.Equal(PasswordKeyAction.Use, d.Action);
        Assert.False(d.RetireFileFirst);
    }

    [Fact]
    public void FromSetting_AnUnusableKey_IsRefusedWithItsReason()
    {
        var refused = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, untrusted: true, refusal: "passwordKey: x", current: Published(SpkiA), retired: true, fromSetting: true));
        var unreadable = DarlingPasswordKeyState.Decide(Facts(filePresent: true, current: Published(SpkiA), fromSetting: true));

        Assert.Equal(PasswordKeyAction.Refused, refused.Action);
        Assert.False(refused.RetireFileFirst);
        Assert.Null(refused.RetireAs);
        Assert.Equal("passwordKey: x", refused.Reason);
        Assert.Equal(PasswordKeyAction.Refused, unreadable.Action);
        Assert.Contains("passwordKey", unreadable.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void FromSetting_NoPublishedKey_PublishesIt()
    {
        var d = DarlingPasswordKeyState.Decide(Facts(filePresent: true, fileSpki: SpkiA, fromSetting: true));

        Assert.Equal(PasswordKeyAction.PublishFile, d.Action);
        Assert.False(d.RetireFileFirst);
        Assert.Null(d.RetireAs);
        Assert.Null(d.Reason);
        Assert.Null(d.Warning);
    }

    [Fact]
    public void FromSetting_TheSameKey_IsUsed()
    {
        var d = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, fileSpki: SpkiA, current: Published(SpkiA), retired: true, fromSetting: true));

        Assert.Equal(PasswordKeyAction.Use, d.Action);
        Assert.False(d.RetireFileFirst);
        Assert.Null(d.Reason);
        Assert.Null(d.Warning);
    }

    [Fact]
    public void FromSetting_AnotherKey_IsAMismatchNamingPasswordKey()
    {
        var d = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, fileSpki: SpkiA, current: Published(SpkiB), fromSetting: true));

        Assert.Equal(PasswordKeyAction.Mismatch, d.Action);
        Assert.False(d.RetireFileFirst);
        Assert.Equal(DarlingPasswordKeyState.SettingMismatchReason(SpkiA, Published(SpkiB)), d.Reason);
        Assert.Contains("passwordKey", d.Reason, StringComparison.Ordinal);
        Assert.Equal(
            $"The password key from passwordKey ({Display(SpkiA)}) is not the key the store publishes ({Display(SpkiB)}). "
            + "Put the published key back in the passwordKey source, or run --reset-password-key and enter the saved passwords again.",
            d.Reason);
    }

    [Fact]
    public void FromSetting_AResetKey_AwaitsANewKey_AndNeverGenerates()
    {
        var d = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, fileSpki: SpkiA, current: null, retired: true, fromSetting: true));

        Assert.Equal(PasswordKeyAction.AwaitNewKey, d.Action);
        Assert.False(d.RetireFileFirst);
        Assert.Null(d.RetireAs);
        Assert.Equal(DarlingPasswordKeyState.SettingResetReason(PasswordSeal.KeyIdFor(SpkiA)), d.Reason);
        Assert.Equal(
            $"The password key {Display(SpkiA)} from passwordKey was reset. Put a new key in the passwordKey source "
            + "(make one with --new-password-key), then restart the service. Saved passwords must be entered again.",
            d.Reason);
    }

    [Fact]
    public void FromSetting_AnInconsistentPublishedKey_IsAMismatch()
    {
        var other = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, fileSpki: SpkiA, current: Inconsistent(SpkiB), fromSetting: true));
        var sameBytes = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, fileSpki: SpkiB, current: Inconsistent(SpkiB), retired: true, fromSetting: true));

        Assert.Equal(PasswordKeyAction.Mismatch, other.Action);
        Assert.False(other.RetireFileFirst);
        Assert.Equal(DarlingPasswordKeyState.SettingMismatchReason(SpkiA, Inconsistent(SpkiB)), other.Reason);
        Assert.DoesNotContain("not-a-key-id", other.Reason, StringComparison.Ordinal);
        Assert.Equal(PasswordKeyAction.Mismatch, sameBytes.Action);
        Assert.Equal(DarlingPasswordKeyState.SettingMismatchReason(SpkiB, Inconsistent(SpkiB)), sameBytes.Reason);
    }

    [Fact]
    public void FromSetting_TheSweepSentences_NamePasswordKey_AndTheKeysWorkedOutFromTheirBytes()
    {
        Assert.Equal(
            "The password key was reset. Put a new key in the passwordKey source (make one with --new-password-key), "
            + "then restart the service. Saved passwords must be entered again.",
            DarlingPasswordKeyState.SettingSweepResetReason);
        Assert.Equal(
            $"The store now publishes the password key {Display(SpkiB)}, not the key this service holds from passwordKey ({Display(SpkiA)}). "
            + "Put the published key back in the passwordKey source and restart the service.",
            DarlingPasswordKeyState.SettingSweepMismatchReason(PasswordSeal.KeyIdFor(SpkiB), PasswordSeal.KeyIdFor(SpkiA)));
    }

    /// <summary>Every combination of an unusable key, the key held (none, A, B), the key published (none, A, B, or one
    /// whose id does not match its bytes) and the replaced flag.</summary>
    public static TheoryData<bool, int, int, bool> SettingCombinations()
    {
        var data = new TheoryData<bool, int, int, bool>();
        foreach (var untrusted in new[] { false, true })
        {
            for (var held = 0; held <= 2; held++)
            {
                for (var published = 0; published <= 3; published++)
                {
                    foreach (var retired in new[] { false, true })
                    {
                        data.Add(untrusted, held, published, retired);
                    }
                }
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(SettingCombinations))]
    public void FromSetting_NeverRetiresOrGenerates_WhateverTheFacts(bool untrusted, int held, int published, bool retired)
    {
        var heldSpki = held switch { 1 => SpkiA, 2 => SpkiB, _ => null };
        var current = published switch { 1 => Published(SpkiA), 2 => Published(SpkiB), 3 => Inconsistent(SpkiA), _ => null };

        var d = DarlingPasswordKeyState.Decide(Facts(
            filePresent: true, untrusted: untrusted, refusal: untrusted ? "passwordKey: x" : null, fileSpki: heldSpki,
            afterOpen: true, current: current, retired: retired, fromSetting: true));

        Assert.NotEqual(PasswordKeyAction.Generate, d.Action);
        Assert.NotEqual(PasswordKeyAction.Missing, d.Action);
        Assert.False(d.RetireFileFirst);
        Assert.Null(d.RetireAs);
        Assert.Null(d.Warning);
    }

    [Fact]
    public void WithoutTheSetting_EveryExistingRowIsUnchanged()
    {
        static string Mismatch(byte[] file, byte[] store) =>
            $"The password key in the credentials directory ({Display(file)}) is not the key the store publishes ({Display(store)}). "
            + "Restore the credentials volume that holds the key, or run --reset-password-key and enter the saved passwords again.";
        static string Missing(byte[] store) =>
            $"The password key {Display(store)} is missing from the credentials directory. "
            + "Restore the credentials volume, or run --reset-password-key and enter the saved passwords again.";

        var rows = new (string Row, PasswordKeyFacts Facts, PasswordKeyDecision Expected)[]
        {
            ("untrusted", Facts(filePresent: true, untrusted: true, refusal: "The key file is not usable.", fileSpki: SpkiA, afterOpen: true, current: Published(SpkiA), retired: true),
                new(PasswordKeyAction.Refused, false, null, "The key file is not usable.", null)),
            ("untrusted, no refusal", Facts(filePresent: true, untrusted: true),
                new(PasswordKeyAction.Refused, false, null, "The password key file cannot be used.", null)),
            ("inconsistent, file", Facts(filePresent: true, fileSpki: SpkiA, current: Inconsistent(SpkiB)),
                new(PasswordKeyAction.Mismatch, false, null, Mismatch(SpkiA, SpkiB), null)),
            ("inconsistent, file with the same bytes", Facts(filePresent: true, fileSpki: SpkiB, current: Inconsistent(SpkiB)),
                new(PasswordKeyAction.Mismatch, false, null, Mismatch(SpkiB, SpkiB), null)),
            ("inconsistent, no file", Facts(current: Inconsistent(SpkiB)),
                new(PasswordKeyAction.Refused, false, null,
                    "The store's published password key is inconsistent: its id does not match the key. "
                    + "Run --reset-password-key and enter the saved passwords again.", null)),
            ("open directory, same key", Facts(filePresent: true, fileSpki: SpkiA, afterOpen: true, current: Published(SpkiA)),
                new(PasswordKeyAction.Use, false, null, null, DarlingPasswordKeyState.OpenDirectoryWarning)),
            ("open directory, nothing published", Facts(filePresent: true, fileSpki: SpkiA, afterOpen: true),
                new(PasswordKeyAction.Generate, true, "discarded",
                    "The password key file was found while its directory was open to other users and does not match the store, so it was set aside.", null)),
            ("open directory, another key", Facts(filePresent: true, fileSpki: SpkiA, afterOpen: true, current: Published(SpkiB)),
                new(PasswordKeyAction.Missing, true, "discarded", Missing(SpkiB), null)),
            ("no file, nothing published", Facts(),
                new(PasswordKeyAction.Generate, false, null, null, null)),
            ("no file, a published key", Facts(current: Published(SpkiA)),
                new(PasswordKeyAction.Missing, false, null, Missing(SpkiA), null)),
            ("unreadable file", Facts(filePresent: true, current: Published(SpkiA)),
                new(PasswordKeyAction.Refused, false, null, "The password key file could not be read.", null)),
            ("file, nothing published", Facts(filePresent: true, fileSpki: SpkiA),
                new(PasswordKeyAction.PublishFile, false, null, null, null)),
            ("file, nothing published, retired", Facts(filePresent: true, fileSpki: SpkiA, retired: true),
                new(PasswordKeyAction.Generate, true, "retired", null, null)),
            ("file, same key", Facts(filePresent: true, fileSpki: SpkiA, current: Published(SpkiA)),
                new(PasswordKeyAction.Use, false, null, null, null)),
            ("file, same key, retired", Facts(filePresent: true, fileSpki: SpkiA, current: Published(SpkiA), retired: true),
                new(PasswordKeyAction.Use, false, null, null, null)),
            ("file, another key", Facts(filePresent: true, fileSpki: SpkiA, current: Published(SpkiB)),
                new(PasswordKeyAction.Mismatch, false, null, Mismatch(SpkiA, SpkiB), null)),
            ("file, same id other bytes", Facts(filePresent: true, fileSpki: SpkiA, current: new PublishedKey(PasswordSeal.KeyIdFor(SpkiA), SpkiB)),
                new(PasswordKeyAction.Mismatch, false, null, Mismatch(SpkiA, SpkiB), null)),
        };

        foreach (var (row, facts, expected) in rows)
        {
            Assert.False(facts.FromSetting);
            Assert.Equal((row, expected), (row, DarlingPasswordKeyState.Decide(facts)));
        }
    }

    [Fact]
    public void The_ring_starts_not_ready_and_refuses_to_seal_with_the_not_ready_sentence()
    {
        // A fresh ring built the way the process-wide one starts, so no other test's setting can change the answer.
        var ring = DarlingPasswordKey.Refusing(DarlingPasswordKey.NotReadyReason);
        var binding = PasswordBinding.ForSmtp("omega-01", 587, true, "example_mail_user");

        Assert.False(ring.Status.CanSeal);
        Assert.Equal(DarlingPasswordKey.NotReadyReason, ring.Status.Reason);
        Assert.Null(ring.Status.KeyId);
        Assert.Null(ring.Status.PublicKey);
        Assert.Equal(DarlingPasswordKey.NotReadyReason, Assert.Throws<InvalidOperationException>(() => ring.Seal("p@ss-not-real", binding)).Message);
    }

    [Fact]
    public void A_refusing_ring_seals_nothing_and_opens_nothing()
    {
        var ring = DarlingPasswordKey.Refusing("The password key file cannot be used.");
        var binding = PasswordBinding.ForSmtp("omega-01", 587, true, "example_mail_user");

        Assert.False(ring.Status.CanSeal);
        Assert.Equal("The password key file cannot be used.", ring.Status.Reason);
        Assert.Equal("The password key file cannot be used.", Assert.Throws<InvalidOperationException>(() => ring.Seal("p@ss-not-real", binding)).Message);
        var ex = Assert.Throws<PasswordSealException>(() => ring.Open("sealed:v1:0123456789abcdef:AAAA", binding));
        Assert.Equal(PasswordSealFailure.UnknownKey, ex.Kind);
        Assert.Null(ex.InnerException);
        Assert.Equal(PasswordSealFailure.UnknownKey, Assert.Throws<PasswordSealException>(() => ring.Open("not sealed", binding)).Kind);
    }

    [Fact]
    public void A_ring_over_a_private_key_seals_and_opens_and_reports_its_key()
    {
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var binding = PasswordBinding.ForSmtp("omega-01", 587, true, "example_mail_user");

        var text = ring.Seal("p@ss-not-real", binding);

        Assert.True(ring.Status.CanSeal);
        Assert.Null(ring.Status.Reason);
        Assert.Equal(key.PublicKey.KeyId, ring.Status.KeyId);
        Assert.Same(key.PublicKey, ring.Status.PublicKey);
        Assert.Equal("p@ss-not-real", ring.Open(text, binding));
        Assert.Equal(
            PasswordSealFailure.BindingOrTamper,
            Assert.Throws<PasswordSealException>(() => ring.Open(text, PasswordBinding.ForSmtp("omega-02", 587, true, "example_mail_user"))).Kind);
    }
}
