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
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the <c>passwordKey</c> setting (#8006 — <see cref="DarlingPasswordKeySource"/>): it accepts only a
/// <c>file:</c> or <c>env:</c> reference, a literal key is a validation problem whose sentence never repeats the
/// value, the reference is kept as written at load (the key is read later, never into the config object), and a
/// <c>file:</c> reference joins the owned set so no configured path can name the key file.
/// </summary>
public sealed class DarlingPasswordKeySourceTests
{
    [Fact]
    public void ALiteralPasswordKey_IsAValidationProblem_ThatNeverRepeatsTheValue()
    {
        var config = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":\"MIIEvAIBADANBgkqhkiG9w0BAQEFAASC\"}");
        var problems = config.Validate();
        Assert.Contains(DarlingPasswordKeySource.LiteralRefusal, problems);
        Assert.DoesNotContain(problems, p => p.Contains("MIIEvAIB", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("file:/run/secrets/password_key")]
    [InlineData("env:DARLING_PASSWORD_KEY")]
    public void AReferencePasswordKey_IsNotAValidationProblem_AndIsNotResolvedAtLoad(string reference)
    {
        var config = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":\"" + reference + "\"}");
        Assert.DoesNotContain(DarlingPasswordKeySource.LiteralRefusal, config.Validate());
        Assert.Equal(reference, config.PasswordKey);   // never replaced by the key text
    }

    [Fact]
    public void ThePasswordKeyReference_IsInTheOwnedSet()
    {
        var dir = Directory.CreateTempSubdirectory().FullName;
        var config = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":\"file:" + Path.Combine(dir, "pk").Replace("\\", "\\\\") + "\"}");
        var set = DarlingOwnedSecrets.Compute(config, Path.Combine(dir, "darling.json"));
        Assert.Contains(Path.Combine(dir, "pk"), set.Paths);
    }

    [Fact]
    public void ABase64File_AndAPemFile_LoadTheSameKey()
    {
        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory().FullName;
        try
        {
            /* The files sit beside a credentials directory whose name is a prefix of theirs: beside is not inside. */
            var credentials = Path.Combine(dir, DarlingLogHashKeyFile.BringYourOwnDirectoryName);
            Directory.CreateDirectory(credentials);
            var base64Path = credentials + ".b64";
            var pemPath = credentials + ".pem";
            File.WriteAllText(base64Path, Convert.ToBase64String(pkcs8) + "\n");
            File.WriteAllText(pemPath, PemOf(pkcs8));

            foreach (var path in new[] { base64Path, pemPath })
            {
                var load = DarlingPasswordKeySource.LoadFromSetting("file:" + path, credentials, NullLogger.Instance);
                AssertLoaded(load, "file:" + path, keyId);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void AnEnvReference_LoadsAndIsTrimmed()
    {
        var (pkcs8, keyId) = NewKey();
        var name = UniqueVariable();
        try
        {
            Environment.SetEnvironmentVariable(name, "  " + Convert.ToBase64String(pkcs8) + "\n");

            AssertLoaded(DarlingPasswordKeySource.LoadFromSetting("env:" + name, null, NullLogger.Instance), "env:" + name, keyId);
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
            CryptographicOperations.ZeroMemory(pkcs8);
        }
    }

    [Fact]
    public void AKeyWithABomAndCrlf_Loads()
    {
        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory().FullName;
        try
        {
            /* What a Windows editor saves: a UTF-8 BOM, CRLF line endings and a trailing blank line. */
            var text = PemOf(pkcs8).Replace("\n", "\r\n", StringComparison.Ordinal) + "\r\n\r\n";
            var path = Path.Combine(dir, "password-key.pem");
            File.WriteAllText(path, text, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));
            Assert.Equal(0xEF, File.ReadAllBytes(path)[0]);

            AssertLoaded(DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, NullLogger.Instance), "file:" + path, keyId);

            /* The parser strips a BOM itself too: an env: value never goes through File.ReadAllText. */
            foreach (var withBom in new[] { Bom + text, Bom + Convert.ToBase64String(pkcs8) + "\r\n" })
            {
                var (parsed, problem) = DarlingPasswordKeySource.ParseKeyText(withBom);
                Assert.Null(problem);
                Assert.NotNull(parsed);
                Assert.Equal(keyId, KeyIdOf(parsed));
                CryptographicOperations.ZeroMemory(parsed);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void APemInAnEnvVariable_LoadsWithNewlines_AndIsRefusedWithEscapedNewlines()
    {
        var (pkcs8, keyId) = NewKey();
        var withNewlines = UniqueVariable();
        var escaped = UniqueVariable();
        try
        {
            var pem = PemOf(pkcs8);
            Environment.SetEnvironmentVariable(withNewlines, pem);
            /* What docker run -e or a YAML string can hand over: each line break is the two characters backslash, n. */
            Environment.SetEnvironmentVariable(escaped, pem.Replace("\n", "\\n", StringComparison.Ordinal));

            AssertLoaded(DarlingPasswordKeySource.LoadFromSetting("env:" + withNewlines, null, NullLogger.Instance), "env:" + withNewlines, keyId);

            var refused = DarlingPasswordKeySource.LoadFromSetting("env:" + escaped, null, NullLogger.Instance);
            AssertRefused(refused, "env:" + escaped, Convert.ToBase64String(pkcs8));
            Assert.StartsWith("passwordKey: ", refused.Refusal, StringComparison.Ordinal);
            Assert.Contains(escaped, refused.Refusal, StringComparison.Ordinal);
            Assert.Contains("base64", refused.Refusal, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable(withNewlines, null);
            Environment.SetEnvironmentVariable(escaped, null);
            CryptographicOperations.ZeroMemory(pkcs8);
        }
    }

    [Fact]
    public void APkcs1Pem_IsRefusedWithTheConversionHint()
    {
        var dir = Directory.CreateTempSubdirectory().FullName;
        try
        {
            using var rsa = RSA.Create(PasswordSeal.KeyBits);
            var path = Path.Combine(dir, "rsa-key.pem");
            File.WriteAllText(path, rsa.ExportRSAPrivateKeyPem());

            var load = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, NullLogger.Instance);

            AssertRefused(load, "file:" + path, Convert.ToBase64String(rsa.ExportRSAPrivateKey()));
            Assert.StartsWith("passwordKey: ", load.Refusal, StringComparison.Ordinal);
            Assert.Contains(path, load.Refusal, StringComparison.Ordinal);
            Assert.Contains("openssl pkcs8 -topk8 -nocrypt", load.Refusal, StringComparison.Ordinal);
            Assert.Contains("--new-password-key", load.Refusal, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData("encrypted", "encrypted")]
    [InlineData("2048", "it is not a 3072-bit RSA key")]
    [InlineData("two PEM blocks", "it holds more than one key")]
    [InlineData("two DER keys", "it holds more than one key")]
    [InlineData("garbage", "base64")]
    public void AnEncryptedPem_A2048BitKey_TwoKeys_AndGarbage_AreRefused(string input, string expected)
    {
        var (pkcs8, _) = NewKey();
        var (second, _) = NewKey();
        var dir = Directory.CreateTempSubdirectory().FullName;
        try
        {
            using var rsa = RSA.Create();
            rsa.ImportPkcs8PrivateKey(pkcs8, out _);
            using var small = RSA.Create(2048);
            var secret = Convert.ToBase64String(pkcs8);
            string text;
            switch (input)
            {
                case "encrypted":
                    text = rsa.ExportEncryptedPkcs8PrivateKeyPem(
                        "a passphrase", new PbeParameters(PbeEncryptionAlgorithm.Aes256Cbc, HashAlgorithmName.SHA256, 10_000));
                    break;
                case "2048":
                    secret = Convert.ToBase64String(small.ExportPkcs8PrivateKey());
                    text = small.ExportPkcs8PrivateKeyPem();
                    break;
                case "two PEM blocks":
                    text = PemOf(pkcs8) + "\n" + PemOf(second);
                    break;
                case "two DER keys":
                    var both = new byte[pkcs8.Length + second.Length];
                    pkcs8.CopyTo(both, 0);
                    second.CopyTo(both, pkcs8.Length);
                    text = Convert.ToBase64String(both);
                    CryptographicOperations.ZeroMemory(both);
                    break;
                default:
                    secret = "this is not a password key";
                    text = secret;
                    break;
            }

            var path = Path.Combine(dir, "password-key");
            File.WriteAllText(path, text);

            var load = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, NullLogger.Instance);

            AssertRefused(load, "file:" + path, secret);
            Assert.StartsWith("passwordKey: ", load.Refusal, StringComparison.Ordinal);
            Assert.Contains(path, load.Refusal, StringComparison.Ordinal);
            Assert.Contains(expected, load.Refusal, StringComparison.OrdinalIgnoreCase);
            AssertNoKeyText(load.Refusal!, Convert.ToBase64String(second));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            CryptographicOperations.ZeroMemory(second);
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ALiteral_IsRefusedByTheLoader()
    {
        var (pkcs8, _) = NewKey();
        try
        {
            var literal = Convert.ToBase64String(pkcs8);
            Assert.StartsWith("MII", literal, StringComparison.Ordinal);

            foreach (var value in new[] { literal, " " + literal + " " })
            {
                var load = DarlingPasswordKeySource.LoadFromSetting(value, null, NullLogger.Instance);
                /* The literal is the key, so the result's Path names the setting instead of repeating it. */
                AssertRefused(load, DarlingPasswordKeySource.SettingName, literal);
                Assert.Equal(DarlingPasswordKeySource.LiteralRefusal, load.Refusal);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
        }
    }

    [Fact]
    public void APathInsideTheCredentialsDirectory_IsRefused_HoweverItIsWritten()
    {
        var (pkcs8, _) = NewKey();
        var dir = Directory.CreateTempSubdirectory().FullName;
        try
        {
            var credentials = Path.Combine(dir, DarlingLogHashKeyFile.BringYourOwnDirectoryName);
            Directory.CreateDirectory(Path.Combine(credentials, "sub"));
            var keyPath = Path.Combine(credentials, "password-key");
            /* A usable key, so the refusal can only be the directory's. */
            File.WriteAllText(keyPath, Convert.ToBase64String(pkcs8));

            var paths = new List<string>
            {
                keyPath,
                Path.Combine(credentials, "sub", "..", "password-key"),
                credentials + Path.DirectorySeparatorChar,
                credentials,
            };
            if (OperatingSystem.IsWindows())
            {
                paths.Add(keyPath.ToUpperInvariant());
            }

            foreach (var credentialsArgument in new[] { credentials, credentials + Path.DirectorySeparatorChar })
            {
                foreach (var path in paths)
                {
                    var load = DarlingPasswordKeySource.LoadFromSetting("file:" + path, credentialsArgument, NullLogger.Instance);

                    AssertRefused(load, "file:" + path, Convert.ToBase64String(pkcs8));
                    Assert.StartsWith("passwordKey: ", load.Refusal, StringComparison.Ordinal);
                    Assert.Contains(path, load.Refusal, StringComparison.Ordinal);
                    Assert.Contains("secret mount outside the credentials directory", load.Refusal, StringComparison.Ordinal);
                }
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData("directory")]
    [InlineData("empty file")]
    [InlineData("missing file")]
    [InlineData("unset variable")]
    public void AnUnusableTarget_IsRefusedNamingTheSettingAndTheTarget(string target)
    {
        var dir = Directory.CreateTempSubdirectory().FullName;
        try
        {
            string reference;
            string named;
            switch (target)
            {
                case "directory":
                    named = Path.Combine(dir, "a-directory");
                    Directory.CreateDirectory(named);
                    reference = "file:" + named;
                    break;
                case "empty file":
                    named = Path.Combine(dir, "empty");
                    File.WriteAllText(named, "");
                    reference = "file:" + named;
                    break;
                case "missing file":
                    named = Path.Combine(dir, "missing");
                    reference = "file:" + named;
                    break;
                default:
                    named = UniqueVariable();
                    reference = "env:" + named;
                    break;
            }

            var load = DarlingPasswordKeySource.LoadFromSetting(reference, null, NullLogger.Instance);

            Assert.True(load.Present);
            Assert.True(load.Untrusted);
            Assert.Null(load.Pkcs8);
            Assert.False(load.FoundAfterOpenDirectory);
            Assert.Equal(reference, load.Path);
            Assert.StartsWith("passwordKey: ", load.Refusal, StringComparison.Ordinal);
            Assert.Contains(named, load.Refusal, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void LoadConfigured_WithoutTheSetting_IsExactlyTheKeyFileLoad()
    {
        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory().FullName;
        try
        {
            var configPath = Path.Combine(dir, "darling.json");
            var keys = DarlingLogHashKeyFile.DirectoryFor(DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"}}"), configPath);
            Assert.Null(DarlingPasswordKeyFile.Generate(keys, () => (byte[])pkcs8.Clone(), NullLogger.Instance).Refusal);
            var expected = DarlingPasswordKeyFile.Load(keys, NullLogger.Instance);
            Assert.NotNull(expected.Pkcs8);
            Assert.Equal(keyId, KeyIdOf(expected.Pkcs8));

            /* Blank counts as unset, the rule Validate uses. */
            foreach (var setting in new[] { "", ",\"passwordKey\":\"\"", ",\"passwordKey\":\"   \"" })
            {
                var config = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"}" + setting + "}");
                var load = DarlingPasswordKeySource.LoadConfigured(config, configPath, NullLogger.Instance);

                Assert.Equal(expected.Present, load.Present);
                Assert.Equal(expected.Untrusted, load.Untrusted);
                Assert.Equal(expected.Refusal, load.Refusal);
                Assert.Equal(expected.FoundAfterOpenDirectory, load.FoundAfterOpenDirectory);
                Assert.Equal(expected.Path, load.Path);
                Assert.Equal(expected.Pkcs8, load.Pkcs8);
                CryptographicOperations.ZeroMemory(load.Pkcs8!);

                var noConfigPath = DarlingPasswordKeySource.LoadConfigured(config, null, NullLogger.Instance);
                Assert.Equal(new PasswordKeyFileLoad(false, false, null, null, false, ""), noConfigPath);
            }

            CryptographicOperations.ZeroMemory(expected.Pkcs8);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void LoadConfigured_WithTheSetting_ReadsOnlyTheReference_AndRefusesTheCredentialsDirectory()
    {
        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory().FullName;
        try
        {
            var configPath = Path.Combine(dir, "darling.json");
            var keys = DarlingLogHashKeyFile.DirectoryFor(DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"}}"), configPath);
            Directory.CreateDirectory(keys);
            var inside = Path.Combine(keys, "password-key");
            var outside = Path.Combine(dir, "password-key");
            File.WriteAllText(inside, Convert.ToBase64String(pkcs8));
            File.WriteAllText(outside, Convert.ToBase64String(pkcs8));

            AssertLoaded(DarlingPasswordKeySource.LoadConfigured(ConfigWith("file:" + outside), configPath, NullLogger.Instance), "file:" + outside, keyId);

            var refused = DarlingPasswordKeySource.LoadConfigured(ConfigWith("file:" + inside), configPath, NullLogger.Instance);
            AssertRefused(refused, "file:" + inside, Convert.ToBase64String(pkcs8));
            Assert.Contains("secret mount outside the credentials directory", refused.Refusal, StringComparison.Ordinal);

            /* With no config path there is no credentials directory to compare against: the reference is still read. */
            AssertLoaded(DarlingPasswordKeySource.LoadConfigured(ConfigWith("file:" + outside), null, NullLogger.Instance), "file:" + outside, keyId);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    /* A UTF-8 BOM as it reads after decoding, written as a code so no editor can strip it unseen. */
    private static readonly string Bom = ((char)0xFEFF).ToString();

    private static DarlingConfig ConfigWith(string reference) =>
        DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":\"" + reference.Replace("\\", "\\\\", StringComparison.Ordinal) + "\"}");

    private static (byte[] Pkcs8, string KeyId) NewKey()
    {
        using var key = PasswordPrivateKey.Generate();
        return (key.ExportPkcs8(), key.PublicKey.KeyId);
    }

    private static string KeyIdOf(byte[] pkcs8)
    {
        using var rsa = RSA.Create();
        rsa.ImportPkcs8PrivateKey(pkcs8, out _);
        return PasswordSeal.KeyIdFor(rsa.ExportSubjectPublicKeyInfo());
    }

    private static string PemOf(byte[] pkcs8) => new(PemEncoding.Write("PRIVATE KEY", pkcs8));

    private static string UniqueVariable() => "DARLING_TEST_PASSWORD_KEY_" + Guid.NewGuid().ToString("N");

    private static void AssertLoaded(PasswordKeyFileLoad load, string reference, string keyId)
    {
        Assert.Null(load.Refusal);
        Assert.True(load.Present);
        Assert.False(load.Untrusted);
        Assert.False(load.FoundAfterOpenDirectory);
        Assert.Equal(reference, load.Path);
        Assert.NotNull(load.Pkcs8);
        Assert.Equal(keyId, KeyIdOf(load.Pkcs8));
        CryptographicOperations.ZeroMemory(load.Pkcs8);
    }

    private static void AssertRefused(PasswordKeyFileLoad load, string reference, string secret)
    {
        Assert.True(load.Present);
        Assert.True(load.Untrusted);
        Assert.Null(load.Pkcs8);
        Assert.False(load.FoundAfterOpenDirectory);
        Assert.Equal(reference, load.Path);
        Assert.NotNull(load.Refusal);
        Assert.Contains(DarlingPasswordKeySource.SettingName, load.Refusal, StringComparison.Ordinal);
        AssertNoKeyText(load.Refusal, secret);
    }

    /// <summary>The sentence holds neither the key text nor any 16-character slice of it.</summary>
    private static void AssertNoKeyText(string sentence, string secret)
    {
        Assert.DoesNotContain(secret, sentence, StringComparison.Ordinal);
        for (var i = 0; i + 16 <= secret.Length; i++)
        {
            Assert.False(sentence.AsSpan().Contains(secret.AsSpan(i, 16), StringComparison.Ordinal), $"the sentence holds key text at offset {i}");
        }
    }
}
