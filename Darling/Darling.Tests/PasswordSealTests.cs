/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the sealed password format (#5366): a value seals to a public key and opens only with the private key, for the
/// purpose and connection fields it was sealed for. Covers the round trip, every bound field, the four failure kinds
/// (each with a fixed message and no inner exception), the size classes, the key classes, and a committed known-answer
/// value so a change to the wire format cannot go unnoticed.
/// </summary>
public sealed class PasswordSealTests
{
    private const string FakePassword = "p@ss-not-real";

    private static readonly PasswordPrivateKey KeyA = PasswordPrivateKey.Generate();
    private static readonly PasswordPrivateKey KeyB = PasswordPrivateKey.Generate();

    private static ServerConnectionIdentity Server() =>
        new("example-sql-01", 1433, "sqlserver", "example_db", false, "sql", "example_login", "Mandatory", false, false);

    private static PasswordBinding ServerBinding() => PasswordBinding.ForServer(Server());

    private static byte[] Payload(string sealedText) =>
        Convert.FromBase64String(sealedText[(PasswordSeal.V1Prefix.Length + 17)..]);

    private static string WithPayload(string sealedText, byte[] payload) =>
        sealedText[..(PasswordSeal.V1Prefix.Length + 17)] + Convert.ToBase64String(payload);

    private static PasswordSealException Fails(Func<string> open)
    {
        var ex = Assert.Throws<PasswordSealException>(open);
        Assert.Null(ex.InnerException);
        return ex;
    }

    [Fact]
    public void A_sealed_password_opens_with_the_same_binding_and_each_seal_differs()
    {
        var first = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var second = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        Assert.StartsWith(PasswordSeal.V1Prefix + KeyA.PublicKey.KeyId + ":", first, StringComparison.Ordinal);
        Assert.NotEqual(first, second);
        Assert.DoesNotContain(FakePassword, first, StringComparison.Ordinal);
        Assert.Equal(FakePassword, PasswordSeal.Open(first, KeyA, ServerBinding()));
        Assert.Equal(FakePassword, PasswordSeal.Open(second, KeyA, ServerBinding()));
    }

    [Theory]
    [InlineData("")]
    [InlineData("x")]
    [InlineData("pässwörd-日本語-🙂")]
    public void Odd_passwords_round_trip(string password)
    {
        var text = PasswordSeal.Seal(password, KeyA.PublicKey, ServerBinding());

        Assert.Equal(password, PasswordSeal.Open(text, KeyA, ServerBinding()));
    }

    [Fact]
    public void The_longest_password_round_trips_and_one_more_byte_is_refused()
    {
        var longest = new string('a', PasswordSeal.MaxPlaintextBytes);
        var text = PasswordSeal.Seal(longest, KeyA.PublicKey, ServerBinding());

        Assert.Equal(longest, PasswordSeal.Open(text, KeyA, ServerBinding()));

        var tooLong = new string('a', PasswordSeal.MaxPlaintextBytes + 1);
        var ex = Assert.Throws<PasswordSealException>(() => PasswordSeal.Seal(tooLong, KeyA.PublicKey, ServerBinding()));
        Assert.Equal(PasswordSealFailure.Malformed, ex.Kind);
        Assert.Null(ex.InnerException);
    }

    public static IEnumerable<object[]> ServerFieldChanges()
    {
        var s = Server();
        yield return new object[] { "host", s with { Host = "example-sql-02" } };
        yield return new object[] { "port", s with { Port = 1434 } };
        yield return new object[] { "database", s with { Database = "other_db" } };
        yield return new object[] { "database to none", s with { Database = null } };
        yield return new object[] { "read_only_intent", s with { ReadOnlyIntent = true } };
        yield return new object[] { "auth", s with { Auth = "serviceprincipal" } };
        yield return new object[] { "username", s with { Username = "other_login" } };
        yield return new object[] { "encrypt_mode", s with { EncryptMode = "Optional" } };
        yield return new object[] { "trust", s with { TrustServerCertificate = true } };
        yield return new object[] { "multi_subnet", s with { MultiSubnetFailover = true } };
        yield return new object[] { "engine", s with { Engine = "postgres" } };
    }

    [Theory]
    [MemberData(nameof(ServerFieldChanges))]
    public void A_server_password_does_not_open_when_any_bound_field_changes(string field, ServerConnectionIdentity changed)
    {
        _ = field;
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        var ex = Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForServer(changed)));

        Assert.Equal(PasswordSealFailure.BindingOrTamper, ex.Kind);
    }

    public static IEnumerable<object[]> RemediationFieldChanges()
    {
        var s = Server();
        yield return new object[] { "host", s with { Host = "example-sql-02" }, "example_fix" };
        yield return new object[] { "database", s with { Database = "other_db" }, "example_fix" };
        yield return new object[] { "encrypt_mode", s with { EncryptMode = "Optional" }, "example_fix" };
        yield return new object[] { "trust", s with { TrustServerCertificate = true }, "example_fix" };
        yield return new object[] { "multi_subnet", s with { MultiSubnetFailover = true }, "example_fix" };
        yield return new object[] { "remediation_username", s, "other_fix" };
        yield return new object[] { "engine", s with { Engine = "postgres" }, "example_fix" };
    }

    [Theory]
    [MemberData(nameof(RemediationFieldChanges))]
    public void A_remediation_password_does_not_open_when_any_bound_field_changes(
        string field, ServerConnectionIdentity changed, string remediationUsername)
    {
        _ = field;
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, PasswordBinding.ForRemediation(Server(), "example_fix"));

        Assert.Equal(
            FakePassword,
            PasswordSeal.Open(text, KeyA, PasswordBinding.ForRemediation(Server(), "example_fix")));
        var ex = Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForRemediation(changed, remediationUsername)));

        Assert.Equal(PasswordSealFailure.BindingOrTamper, ex.Kind);
    }

    public static IEnumerable<object[]> SmtpFieldChanges()
    {
        yield return new object[] { "host", "omega-02", 587, true, "example_mail_user" };
        yield return new object[] { "port", "omega-01", 25, true, "example_mail_user" };
        yield return new object[] { "ssl", "omega-01", 587, false, "example_mail_user" };
        yield return new object[] { "username", "omega-01", 587, true, "other_mail_user" };
    }

    [Theory]
    [MemberData(nameof(SmtpFieldChanges))]
    public void A_mail_password_does_not_open_when_any_bound_field_changes(
        string field, string host, int port, bool ssl, string username)
    {
        _ = field;
        var text = PasswordSeal.Seal(
            FakePassword, KeyA.PublicKey, PasswordBinding.ForSmtp("omega-01", 587, true, "example_mail_user"));

        Assert.Equal(
            FakePassword,
            PasswordSeal.Open(text, KeyA, PasswordBinding.ForSmtp("omega-01", 587, true, "example_mail_user")));
        var ex = Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForSmtp(host, port, ssl, username)));

        Assert.Equal(PasswordSealFailure.BindingOrTamper, ex.Kind);
    }

    [Fact]
    public void A_password_sealed_for_one_purpose_does_not_open_for_another()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        Assert.Equal(
            PasswordSealFailure.BindingOrTamper,
            Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForRemediation(Server(), "example_login"))).Kind);
        Assert.Equal(
            PasswordSealFailure.BindingOrTamper,
            Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForSmtp("example-sql-01", 1433, false, "example_login"))).Kind);
    }

    [Fact]
    public void Case_only_changes_to_the_lowercased_fields_still_open_and_host_case_does_not()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var recased = Server() with { Auth = "SQL", EncryptMode = "MANDATORY", Engine = "SqlServer" };

        Assert.Equal(FakePassword, PasswordSeal.Open(text, KeyA, PasswordBinding.ForServer(recased)));
        Assert.Equal(
            PasswordSealFailure.BindingOrTamper,
            Fails(() => PasswordSeal.Open(text, KeyA, PasswordBinding.ForServer(Server() with { Host = "EXAMPLE-SQL-01" }))).Kind);
    }

    [Fact]
    public void A_swapped_key_id_is_an_unknown_key_and_so_is_another_key()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var swapped = text.Replace(KeyA.PublicKey.KeyId, KeyB.PublicKey.KeyId, StringComparison.Ordinal);

        var viaSwap = Fails(() => PasswordSeal.Open(swapped, KeyA, ServerBinding()));
        var viaOtherKey = Fails(() => PasswordSeal.Open(text, KeyB, ServerBinding()));

        Assert.Equal(PasswordSealFailure.UnknownKey, viaSwap.Kind);
        Assert.Equal(KeyB.PublicKey.KeyId, viaSwap.KeyId);
        Assert.Equal(PasswordSealFailure.UnknownKey, viaOtherKey.Kind);
        Assert.Equal(KeyA.PublicKey.KeyId, viaOtherKey.KeyId);
    }

    [Theory]
    [InlineData("sealed:v2:0123456789abcdef:AAAA")]
    [InlineData("sealed:v10:x")]
    [InlineData("sealed:")]
    [InlineData("sealed:v1")]
    public void A_version_other_than_one_is_a_newer_version(string text)
    {
        var ex = Fails(() => PasswordSeal.Open(text, KeyA, ServerBinding()));

        Assert.Equal(PasswordSealFailure.NewerVersion, ex.Kind);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not-sealed")]
    [InlineData("Sealed:v1:0123456789abcdef:AAAA")]
    [InlineData("sealed:v1:")]
    [InlineData("sealed:v1:0123456789abcdef")]
    [InlineData("sealed:v1:0123456789ABCDEF:AAAA")]
    [InlineData("sealed:v1:0123456789abcde:AAAA")]
    [InlineData("sealed:v1:0123456789abcdef:")]
    [InlineData("sealed:v1:0123456789abcdef:not base64 !!")]
    [InlineData("sealed:v1:0123456789abcdef:AAAA")]
    public void Text_that_is_not_a_sealed_value_is_malformed(string? text)
    {
        var ex = Fails(() => PasswordSeal.Open(text!, KeyA, ServerBinding()));

        Assert.Equal(PasswordSealFailure.Malformed, ex.Kind);
    }

    [Fact]
    public void A_payload_of_the_wrong_size_is_malformed()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var payload = Payload(text);
        var id = KeyA.PublicKey.KeyId;

        foreach (var bad in new[]
                 {
                     payload[..443],
                     payload[..(payload.Length - 1)],
                     payload.Concat(new byte[1]).ToArray(),
                     new byte[8700],
                 })
        {
            var ex = Fails(() => PasswordSeal.Open(WithPayload(text, bad), KeyA, ServerBinding()));
            Assert.Equal(PasswordSealFailure.Malformed, ex.Kind);
        }

        Assert.Equal(PasswordSealFailure.Malformed, Fails(() => PasswordSeal.Open(text + " ", KeyA, ServerBinding())).Kind);
        Assert.Equal(PasswordSealFailure.Malformed, Fails(() => PasswordSeal.Open(text.Replace(id + ":", id + ":\n", StringComparison.Ordinal), KeyA, ServerBinding())).Kind);
    }

    [Theory]
    [InlineData(10, "the wrapped key")]
    [InlineData(384 + 12 + 3, "the tag")]
    [InlineData(384 + 2, "the nonce")]
    [InlineData(444 - 1, "the ciphertext")]
    public void A_flipped_byte_is_a_binding_or_tamper_failure_with_a_fixed_message(int index, string part)
    {
        _ = part;
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var payload = Payload(text);
        payload[index] ^= 0x01;

        var ex = Fails(() => PasswordSeal.Open(WithPayload(text, payload), KeyA, ServerBinding()));

        Assert.Equal(PasswordSealFailure.BindingOrTamper, ex.Kind);
        Assert.Equal("The saved password does not open for this server's connection settings.", ex.Message);
        Assert.DoesNotContain(FakePassword, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_wrapped_key_that_does_not_decrypt_gives_the_same_failure_as_a_bad_tag()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());
        var flippedWrap = Payload(text);
        flippedWrap[0] ^= 0xFF;
        var flippedTag = Payload(text);
        flippedTag[400] ^= 0xFF;

        var a = Fails(() => PasswordSeal.Open(WithPayload(text, flippedWrap), KeyA, ServerBinding()));
        var b = Fails(() => PasswordSeal.Open(WithPayload(text, flippedTag), KeyA, ServerBinding()));

        Assert.Equal(a.Kind, b.Kind);
        Assert.Equal(a.Message, b.Message);
    }

    [Fact]
    public void Every_failure_kind_has_its_own_fixed_message_that_names_no_provider_text()
    {
        var kinds = Enum.GetValues<PasswordSealFailure>();
        var messages = kinds.Select(k => new PasswordSealException(k).Message).ToArray();

        Assert.Equal(kinds.Length, messages.Distinct().Count());
        Assert.All(messages, m => Assert.EndsWith(".", m, StringComparison.Ordinal));
        Assert.All(kinds, k => Assert.Null(new PasswordSealException(k, "0123456789abcdef").InnerException));
    }

    [Theory]
    [InlineData(1, 444)]
    [InlineData(28, 444)]
    [InlineData(29, 476)]
    [InlineData(31, 476)]
    [InlineData(32, 476)]
    [InlineData(33, 476)]
    [InlineData(60, 476)]
    [InlineData(61, 508)]
    public void Passwords_of_nearby_lengths_seal_to_the_same_size_class(int length, int expectedPayloadBytes)
    {
        var text = PasswordSeal.Seal(new string('x', length), KeyA.PublicKey, ServerBinding());

        Assert.Equal(expectedPayloadBytes, Payload(text).Length);
    }

    [Fact]
    public void The_committed_known_answer_value_opens()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(VectorPath()));
        var root = doc.RootElement;
        var b = root.GetProperty("binding");
        var binding = PasswordBinding.ForServer(new ServerConnectionIdentity(
            b.GetProperty("host").GetString()!, b.GetProperty("port").GetInt32(), b.GetProperty("engine").GetString()!,
            b.GetProperty("database").GetString(), b.GetProperty("readOnlyIntent").GetBoolean(),
            b.GetProperty("auth").GetString()!, b.GetProperty("username").GetString(),
            b.GetProperty("encryptMode").GetString()!, b.GetProperty("trustServerCertificate").GetBoolean(),
            b.GetProperty("multiSubnetFailover").GetBoolean()));
        using var key = PasswordPrivateKey.FromPkcs8(Convert.FromBase64String(root.GetProperty("privateKeyPkcs8").GetString()!));

        Assert.Equal(PasswordSeal.Algorithm, root.GetProperty("algorithm").GetString());
        Assert.StartsWith("test-only key", root.GetProperty("note").GetString(), StringComparison.Ordinal);
        Assert.Equal(root.GetProperty("keyId").GetString(), key.PublicKey.KeyId);
        Assert.Equal(
            Convert.FromBase64String(root.GetProperty("publicKeySpki").GetString()!), key.PublicKey.Spki);
        Assert.Equal(
            root.GetProperty("plaintext").GetString(),
            PasswordSeal.Open(root.GetProperty("sealed").GetString()!, key, binding));
    }

    [Fact]
    public void A_sealed_value_is_not_a_reference_and_not_base64()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        Assert.True(PasswordSeal.IsSealed(text));
        Assert.True(PasswordSeal.IsSealed("sealed:v2:anything"));
        Assert.False(PasswordSeal.IsSealed(null));
        Assert.False(PasswordSeal.IsSealed(""));
        Assert.False(PasswordSeal.IsSealed("Sealed:v1:x"));
        Assert.False(DarlingSecretSource.IsReference(text));
        Assert.False(DarlingSecretSource.IsReference("sealed:v2:anything"));
        Assert.False(PasswordSeal.IsSealed("env:SOME_NAME"));
        Assert.False(PasswordSeal.IsSealed("file:/some/path"));
        Assert.DoesNotContain(':', Convert.ToBase64String(new byte[] { 1, 2, 3, 250, 251, 252, 253 }));
        Assert.False(TryBase64(text));
        Assert.False(TryBase64(PasswordSeal.RoutingPrefix));
    }

    private static bool TryBase64(string text) => Convert.TryFromBase64String(text, new byte[text.Length], out _);

    [Fact]
    public void Key_ids_are_sixteen_lowercase_hex_characters_of_the_spki_hash()
    {
        var spki = KeyA.PublicKey.Spki;
        var expected = Convert.ToHexString(SHA256.HashData(spki))[..16].ToLowerInvariant();

        Assert.Equal(expected, PasswordSeal.KeyIdFor(spki));
        Assert.Equal(expected, KeyA.PublicKey.KeyId);
        Assert.Equal(SHA256.HashData(spki), KeyA.PublicKey.Fingerprint);
        Assert.NotEqual(KeyA.PublicKey.KeyId, KeyB.PublicKey.KeyId);
        Assert.Equal("ab12-cd34-ef56-7890".ToUpperInvariant(), PasswordSeal.DisplayKeyId("ab12cd34ef567890"));
        Assert.Equal("not a key id", PasswordSeal.DisplayKeyId("not a key id"));
    }

    [Fact]
    public void The_key_id_of_a_value_is_read_for_version_one_only()
    {
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        Assert.Equal(KeyA.PublicKey.KeyId, PasswordSeal.KeyIdOf(text));
        Assert.Null(PasswordSeal.KeyIdOf("sealed:v2:" + KeyA.PublicKey.KeyId + ":AAAA"));
        Assert.Null(PasswordSeal.KeyIdOf("sealed:v1:short:AAAA"));
        Assert.Null(PasswordSeal.KeyIdOf("env:SOME_NAME"));
    }

    [Fact]
    public void Keys_round_trip_through_their_encodings_and_only_three_thousand_seventy_two_bit_rsa_is_accepted()
    {
        var der = KeyA.ExportPkcs8();
        using var again = PasswordPrivateKey.FromPkcs8(der);
        CryptographicOperations.ZeroMemory(der);
        var text = PasswordSeal.Seal(FakePassword, KeyA.PublicKey, ServerBinding());

        Assert.Equal(KeyA.PublicKey.KeyId, again.PublicKey.KeyId);
        Assert.Equal(FakePassword, PasswordSeal.Open(text, again, ServerBinding()));
        Assert.Equal(PasswordSealFailure.Malformed, Assert.Throws<PasswordSealException>(() => PasswordPublicKey.FromSpki(new byte[] { 1, 2, 3 })).Kind);
        Assert.Equal(PasswordSealFailure.Malformed, Assert.Throws<PasswordSealException>(() => PasswordPrivateKey.FromPkcs8(new byte[] { 1, 2, 3 })).Kind);

        using var small = RSA.Create(2048);
        Assert.Equal(
            PasswordSealFailure.Malformed,
            Assert.Throws<PasswordSealException>(() => PasswordPublicKey.FromSpki(small.ExportSubjectPublicKeyInfo())).Kind);
        Assert.Equal(
            PasswordSealFailure.Malformed,
            Assert.Throws<PasswordSealException>(() => PasswordPrivateKey.FromPkcs8(small.ExportPkcs8PrivateKey())).Kind);
    }

    [Fact]
    public void A_public_key_hands_out_copies_of_its_bytes()
    {
        var copy = KeyA.PublicKey.Spki;
        copy[0] ^= 0xFF;

        Assert.NotEqual(copy[0], KeyA.PublicKey.Spki[0]);
        Assert.Equal(KeyA.PublicKey.KeyId, PasswordPublicKey.FromSpki(KeyA.PublicKey.Spki).KeyId);
    }

    [Fact]
    public void A_public_key_hands_out_a_copy_of_its_fingerprint()
    {
        var copy = KeyA.PublicKey.Fingerprint;
        var before = (byte[])copy.Clone();
        copy[0] ^= 0xFF;

        Assert.Equal(before, KeyA.PublicKey.Fingerprint);
        Assert.NotSame(KeyA.PublicKey.Fingerprint, KeyA.PublicKey.Fingerprint);
    }

    [Fact]
    public void A_disposed_private_key_gives_an_unknown_key_failure_with_no_inner_exception()
    {
        var key = PasswordPrivateKey.Generate();
        var text = PasswordSeal.Seal(FakePassword, key.PublicKey, ServerBinding());
        key.Dispose();
        key.Dispose();

        var ex = Fails(() => PasswordSeal.Open(text, key, ServerBinding()));

        Assert.Equal(PasswordSealFailure.UnknownKey, ex.Kind);
    }

    [Fact]
    public void A_password_that_is_not_valid_text_is_refused_and_two_bad_passwords_never_seal_the_same()
    {
        var ex = Assert.Throws<PasswordSealException>(() => PasswordSeal.Seal("ab\uD800", KeyA.PublicKey, ServerBinding()));

        Assert.Equal(PasswordSealFailure.Malformed, ex.Kind);
        Assert.Null(ex.InnerException);
        Assert.Equal("The password contains text that cannot be stored.", ex.Message);
        Assert.Equal(
            PasswordSealFailure.Malformed,
            Assert.Throws<PasswordSealException>(() => PasswordSeal.Seal("ab\uDFFF", KeyA.PublicKey, ServerBinding())).Kind);
    }

    [Fact]
    public void A_binding_field_that_is_not_valid_text_is_refused_without_echoing_it()
    {
        var bad = new ServerConnectionIdentity("example-sql-\uD800", 1433, "sqlserver", "example_db", false, "sql", "example_login", "Mandatory", false, false);
        var binding = PasswordBinding.ForServer(bad);

        var ex = Assert.Throws<ArgumentException>(() => binding.EncodeAad("0123456789abcdef"));
        var pin = Assert.Throws<ArgumentException>(() => binding.LegacyPinHash());
        var seal = Assert.Throws<ArgumentException>(() => PasswordSeal.Seal(FakePassword, KeyA.PublicKey, binding));

        foreach (var e in new[] { ex, pin, seal })
        {
            Assert.Null(e.InnerException);
            Assert.DoesNotContain("example-sql", e.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("D800", e.Message, StringComparison.OrdinalIgnoreCase);
        }
    }

    // A value with a good GCM tag but wrong content inside: built with the test key's public half, so only the inner
    // checks can reject it.
    private static string Crafted(byte[] aesKey, byte[] padded)
    {
        var binding = ServerBinding();
        var keyId = KeyA.PublicKey.KeyId;
        var payload = new byte[384 + 12 + 16 + padded.Length];
        RandomNumberGenerator.Fill(payload.AsSpan(384, 12));
        using (var rsa = RSA.Create())
        {
            rsa.ImportSubjectPublicKeyInfo(KeyA.PublicKey.Spki, out _);
            rsa.Encrypt(aesKey, RSAEncryptionPadding.OaepSHA256).CopyTo(payload, 0);
        }

        using (var aes = new AesGcm(aesKey, 16))
        {
            aes.Encrypt(payload.AsSpan(384, 12), padded, payload.AsSpan(412), payload.AsSpan(396, 16), binding.EncodeAad(keyId));
        }

        return PasswordSeal.V1Prefix + keyId + ":" + Convert.ToBase64String(payload);
    }

    private static byte[] Padded(int declaredLength, byte[] content)
    {
        var padded = new byte[32];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(padded, declaredLength);
        content.CopyTo(padded, 4);
        return padded;
    }

    private static byte[] NewAesKey(int bytes)
    {
        var key = new byte[bytes];
        RandomNumberGenerator.Fill(key);
        return key;
    }

    [Fact]
    public void The_crafting_helper_builds_a_value_that_opens_so_the_failures_below_come_from_the_inner_checks()
    {
        var text = Crafted(NewAesKey(32), Padded(3, "abc"u8.ToArray()));

        Assert.Equal("abc", PasswordSeal.Open(text, KeyA, ServerBinding()));
    }

    [Fact]
    public void A_value_with_a_good_tag_and_a_length_larger_than_the_content_is_a_binding_or_tamper_failure()
    {
        var text = Crafted(NewAesKey(32), Padded(40, "abc"u8.ToArray()));

        Assert.Equal(PasswordSealFailure.BindingOrTamper, Fails(() => PasswordSeal.Open(text, KeyA, ServerBinding())).Kind);
    }

    [Fact]
    public void A_value_with_a_good_tag_and_non_zero_padding_is_a_binding_or_tamper_failure()
    {
        var padded = Padded(3, "abc"u8.ToArray());
        padded[20] = 1;
        var text = Crafted(NewAesKey(32), padded);

        Assert.Equal(PasswordSealFailure.BindingOrTamper, Fails(() => PasswordSeal.Open(text, KeyA, ServerBinding())).Kind);
    }

    [Fact]
    public void A_value_with_a_good_tag_and_content_that_is_not_valid_utf8_is_a_binding_or_tamper_failure()
    {
        var text = Crafted(NewAesKey(32), Padded(2, new byte[] { 0xC3, 0x28 }));

        Assert.Equal(PasswordSealFailure.BindingOrTamper, Fails(() => PasswordSeal.Open(text, KeyA, ServerBinding())).Kind);
    }

    [Theory]
    [InlineData(16)]
    [InlineData(24)]
    public void A_value_with_a_good_tag_and_a_wrapped_key_that_is_not_thirty_two_bytes_is_a_binding_or_tamper_failure(int keyBytes)
    {
        var text = Crafted(NewAesKey(keyBytes), Padded(3, "abc"u8.ToArray()));

        Assert.Equal(PasswordSealFailure.BindingOrTamper, Fails(() => PasswordSeal.Open(text, KeyA, ServerBinding())).Kind);
    }

    [Fact]
    public void Keys_whose_public_exponent_is_not_65537_are_refused_on_both_imports()
    {
        var (publicOnly, full) = KeyWithExponentThree();

        Assert.Equal(PasswordSealFailure.Malformed, Assert.Throws<PasswordSealException>(() => PasswordPublicKey.FromSpki(publicOnly)).Kind);
        Assert.Equal(PasswordSealFailure.Malformed, Assert.Throws<PasswordSealException>(() => PasswordPrivateKey.FromPkcs8(full)).Kind);
    }

    // A 3072-bit key with e = 3: .NET will not make one, so two primes are found here (test data only).
    private static (byte[] Spki, byte[] Pkcs8) KeyWithExponentThree()
    {
        var e = new BigInteger(3);
        BigInteger p, q;
        do
        {
            p = TestPrime(1536);
        }
        while (BigInteger.GreatestCommonDivisor(p - 1, e) != 1);
        do
        {
            q = TestPrime(1536);
        }
        while (q == p || BigInteger.GreatestCommonDivisor(q - 1, e) != 1);

        var d = ModInverse(e, (p - 1) * (q - 1));
        var n = p * q;
        static byte[] Be(BigInteger v, int len)
        {
            var raw = v.ToByteArray(isUnsigned: true, isBigEndian: true);
            var padded = new byte[len];
            raw.CopyTo(padded, len - raw.Length);
            return padded;
        }

        var parameters = new RSAParameters
        {
            Modulus = Be(n, 384),
            Exponent = new byte[] { 3 },
            D = Be(d, 384),
            P = Be(p, 192),
            Q = Be(q, 192),
            DP = Be(d % (p - 1), 192),
            DQ = Be(d % (q - 1), 192),
            InverseQ = Be(ModInverse(q, p), 192),
        };
        using var rsa = RSA.Create();
        rsa.ImportParameters(parameters);
        return (rsa.ExportSubjectPublicKeyInfo(), rsa.ExportPkcs8PrivateKey());
    }

    private static BigInteger ModInverse(BigInteger a, BigInteger m)
    {
        BigInteger oldR = m, r = a % m, t0 = 0, t1 = 1;
        while (r != 0)
        {
            var quotient = oldR / r;
            (oldR, r) = (r, oldR - quotient * r);
            (t0, t1) = (t1, t0 - quotient * t1);
        }

        return ((t0 % m) + m) % m;
    }

    private static BigInteger TestPrime(int bits)
    {
        var bytes = new byte[bits / 8];
        var smallPrimes = new[] { 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97 };
        while (true)
        {
            RandomNumberGenerator.Fill(bytes);
            bytes[0] |= 0xC0;
            bytes[^1] |= 1;
            var candidate = new BigInteger(bytes, isUnsigned: true, isBigEndian: true);
            if (smallPrimes.Any(sp => candidate % sp == 0))
            {
                continue;
            }

            var probable = true;
            for (var round = 0; round < 12 && probable; round++)
            {
                var witnessBytes = new byte[bits / 8 - 1];
                RandomNumberGenerator.Fill(witnessBytes);
                probable = MillerRabin(candidate, new BigInteger(witnessBytes, isUnsigned: true) + 2);
            }

            if (probable)
            {
                return candidate;
            }
        }
    }

    private static bool MillerRabin(BigInteger n, BigInteger a)
    {
        var d = n - 1;
        var s = 0;
        while (d.IsEven)
        {
            d >>= 1;
            s++;
        }

        var x = BigInteger.ModPow(a, d, n);
        if (x == 1 || x == n - 1)
        {
            return true;
        }

        for (var i = 1; i < s; i++)
        {
            x = BigInteger.ModPow(x, 2, n);
            if (x == n - 1)
            {
                return true;
            }
        }

        return false;
    }

    private static string VectorPath([CallerFilePath] string thisFile = "") =>
        Path.Combine(Path.GetDirectoryName(thisFile)!, "TestData", "password-seal-v1-vector.json");
}
