/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5366: the hidden <c>--self-check-password-key</c> verb. The Unix file checks (owner, link count, file identity) run only
/// on Linux, in the Linux build job, so what is pinned here is what can be checked on any platform: the verb is reachable
/// and stays out of the usage text, the sealed-value check opens the committed vector and fails closed on a changed
/// vector, and the workflow step runs the verb from the built image with the vector mounted read-only.
/// </summary>
public sealed class PasswordKeySelfCheckTests : IDisposable
{
    private static readonly string VectorPath =
        PathTo("Darling", "Darling.Tests", "Fixtures", "PasswordSeal", "linux-self-check-vector.json");

    private readonly string _scratch = Directory.CreateTempSubdirectory("password-key-self-check-test-").FullName;

    public void Dispose()
    {
        try
        {
            Directory.Delete(_scratch, recursive: true);
        }
        catch (IOException)
        {
            /* A leftover temporary directory changes nothing the tests found. */
        }
    }

    [Theory]
    [InlineData("--self-check-password-key", true)]
    [InlineData("--SELF-CHECK-PASSWORD-KEY", true)]
    [InlineData("--self-check-password", false)]
    [InlineData("--self-check", false)]
    [InlineData("--reset-password-key", false)]
    public void The_verb_is_recognized_case_insensitively(string arg, bool expected) =>
        Assert.Equal(expected, DarlingCliCommands.IsSelfCheckPasswordKeyVerb(arg));

    [Fact]
    public void The_startup_classifier_lets_the_verb_through_and_the_usage_text_does_not_list_it()
    {
        Assert.True(DarlingCliCommands.IsKnownVerb("--self-check-password-key"));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs(new[] { "--self-check-password-key", "x" }));
        Assert.DoesNotContain("self-check", DarlingCliCommands.UsageText(), StringComparison.Ordinal);
    }

    [Fact]
    public void Program_dispatches_the_verb_to_the_self_check()
    {
        var program = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Program.cs");
        Assert.Contains("IsSelfCheckPasswordKeyVerb(args[0])", program, StringComparison.Ordinal);
        Assert.Contains("DarlingPasswordKeySelfCheck.Run(args[1..], Console.Out, Console.Error)", program, StringComparison.Ordinal);
    }

    [Fact]
    public void The_committed_vector_opens_to_its_plaintext()
    {
        Assert.Null(DarlingPasswordKeySelfCheck.CheckVector(VectorPath));
    }

    [Fact]
    public void The_committed_vector_is_labelled_a_test_vector_and_holds_no_pem_text()
    {
        var text = File.ReadAllText(VectorPath);
        Assert.Contains("TEST VECTOR ONLY", text, StringComparison.Ordinal);
        Assert.DoesNotContain("-----BEGIN", text, StringComparison.Ordinal);
        Assert.Contains("testOnlyPrivateKeyPkcs8Base64", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("label")]
    [InlineData("purpose")]
    [InlineData("plaintext")]
    [InlineData("sealed")]
    [InlineData("testOnlyPrivateKeyPkcs8Base64")]
    [InlineData("binding")]
    public void A_vector_missing_a_field_fails(string field)
    {
        var vector = Vector();
        vector.Remove(field);
        Assert.NotNull(DarlingPasswordKeySelfCheck.CheckVector(Write(vector)));
    }

    [Fact]
    public void A_vector_whose_plaintext_differs_fails()
    {
        var vector = Vector();
        vector["plaintext"] = "another-fake-value";
        Assert.NotNull(DarlingPasswordKeySelfCheck.CheckVector(Write(vector)));
    }

    [Theory]
    [InlineData("host", "example-other-host")]
    [InlineData("username", "other_login")]
    [InlineData("database", "other_db")]
    public void A_vector_whose_binding_differs_fails(string field, string value)
    {
        var vector = Vector();
        vector["binding"]![field] = value;
        Assert.NotNull(DarlingPasswordKeySelfCheck.CheckVector(Write(vector)));
    }

    [Fact]
    public void A_vector_whose_key_is_another_key_fails()
    {
        var vector = Vector();
        using var other = PerformanceMonitor.Darling.Storage.PasswordPrivateKey.Generate();
        vector["testOnlyPrivateKeyPkcs8Base64"] = Convert.ToBase64String(other.ExportPkcs8());
        Assert.NotNull(DarlingPasswordKeySelfCheck.CheckVector(Write(vector)));
    }

    [Fact]
    public void A_vector_without_the_test_label_fails()
    {
        var vector = Vector();
        vector["label"] = "a sealed value";
        Assert.NotNull(DarlingPasswordKeySelfCheck.CheckVector(Write(vector)));
    }

    [Fact]
    public void A_missing_vector_file_fails()
    {
        Assert.NotNull(DarlingPasswordKeySelfCheck.CheckVector(Path.Combine(_scratch, "no-such-file.json")));
    }

    [Fact]
    public void Run_with_no_path_or_two_paths_exits_non_zero()
    {
        using var output = new StringWriter();
        using var error = new StringWriter();
        Assert.Equal(1, DarlingPasswordKeySelfCheck.Run(Array.Empty<string>(), output, error));
        Assert.Equal(1, DarlingPasswordKeySelfCheck.Run(new[] { VectorPath, "extra" }, output, error));
        Assert.Contains("Usage:", error.ToString(), StringComparison.Ordinal);
    }

    /// <summary>The Unix file checks cannot run on Windows, and the verb says so and exits non-zero rather than passing.</summary>
    [Fact]
    public void Run_on_Windows_exits_non_zero_without_claiming_a_pass()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        using var output = new StringWriter();
        using var error = new StringWriter();
        Assert.Equal(1, DarlingPasswordKeySelfCheck.Run(new[] { VectorPath }, output, error));
        Assert.DoesNotContain("passed", output.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void The_Linux_build_job_runs_the_verb_in_the_image_with_the_vector_mounted_read_only()
    {
        var workflow = ReadRepoFileLf(".github", "workflows", "build.yml");
        var start = workflow.IndexOf("\n  darling-linux:", StringComparison.Ordinal);
        Assert.True(start >= 0, "the darling-linux job was not found");
        var next = workflow.IndexOf("\n  darling-", start + 10, StringComparison.Ordinal);
        var job = next < 0 ? workflow[start..] : workflow[start..next];

        var step = job.IndexOf("--self-check-password-key", StringComparison.Ordinal);
        Assert.True(step >= 0, "the darling-linux job does not run --self-check-password-key");
        var window = job[Math.Max(0, step - 900)..Math.Min(job.Length, step + 300)];
        Assert.Contains("docker run", window, StringComparison.Ordinal);
        Assert.Contains("linux-self-check-vector.json", window, StringComparison.Ordinal);
        Assert.Contains(":ro", window, StringComparison.Ordinal);
        Assert.Contains("performancemonitor-darling:pr", window, StringComparison.Ordinal);
        Assert.DoesNotContain("continue-on-error", window, StringComparison.Ordinal);
        Assert.DoesNotContain("|| true", window, StringComparison.Ordinal);
    }

    private static JsonObject Vector() => JsonNode.Parse(File.ReadAllText(VectorPath))!.AsObject();

    private string Write(JsonObject vector)
    {
        var path = Path.Combine(_scratch, Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(path, vector.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
        return path;
    }
}
