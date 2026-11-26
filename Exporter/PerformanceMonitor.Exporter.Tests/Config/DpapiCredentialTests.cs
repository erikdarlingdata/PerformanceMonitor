using System.Net;
using System.Net.Sockets;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Exporter.Tests.Run;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Config;

/// <summary>
/// The `dpapi_file` password source on Windows: a file written with the calls the README's PowerShell snippet makes
/// (UTF-8 password, entropy 'PerformanceMonitor.Darling.v1', LocalMachine scope, base64 text) resolves, and a raw
/// blob, a blob made with other entropy, text that is not base64 and an empty file each stop `check-config` with
/// exit 2 and one line that names the likely causes. The entropy is written out here on purpose: it is the value the
/// README documents, so changing the code's value without the README fails this class. Off Windows the tests return,
/// as the other platform-bound tests do.
/// </summary>
public sealed class DpapiCredentialTests : IDisposable
{
    /// <summary>The entropy string the README documents. Deliberately not read from the code under test.</summary>
    private const string DocumentedEntropy = "PerformanceMonitor.Darling.v1";

    private readonly RunRoot _root = new();

    public void Dispose() => _root.Dispose();

    /// <summary>What the README snippet does: protect the UTF-8 password with the entropy at LocalMachine scope and keep the blob.</summary>
    [SupportedOSPlatform("windows")]
    private static byte[] Protect(string password, string entropy) =>
        ProtectedData.Protect(Encoding.UTF8.GetBytes(password), Encoding.UTF8.GetBytes(entropy), DataProtectionScope.LocalMachine);

    private string WriteConfig(string dpapiPath, int port)
    {
        // The default local destination must exist, or check-config stops at the destination check before the store.
        Directory.CreateDirectory(Path.Combine(_root.Root, "landing"));
        var config = Path.Combine(_root.Root, "c.json");
        File.WriteAllText(config, $$"""
            {
              "config_version": 1,
              "store": { "name": "dpapi_test", "host": "127.0.0.1", "port": {{port}}, "database": "darling", "user": "darling",
                         "password": { "source": "dpapi_file", "path": {{JsonSerializer.Serialize(dpapiPath)}} } },
              "paths": { "out": {{JsonSerializer.Serialize(_root.Out)}}, "state": {{JsonSerializer.Serialize(_root.State)}}, "logs": {{JsonSerializer.Serialize(_root.Logs)}} }
            }
            """);
        return config;
    }

    private static int ClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    [Fact]
    public async Task A_file_written_with_the_documented_calls_resolves_and_check_config_passes_it()
    {
        if (!OperatingSystem.IsWindows()) { return; }

        const string password = "made-up-Pa55 word";
        var file = Path.Combine(_root.Root, "store.dpapi");
        File.WriteAllText(file, Convert.ToBase64String(Protect(password, DocumentedEntropy)));

        Assert.Equal(password, CredentialResolver.Resolve(new CredentialSpec("dpapi_file", Path: file)));

        // An unreachable store is a warning in check-config, so exit 0 here means the credential resolved.
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await CliVerbs.CheckConfigAsync(["check-config", "--config", WriteConfig(file, ClosedPort())], output, error, CancellationToken.None);
        Assert.Equal(0, exit);
        Assert.StartsWith("warning: store collation not checked", error.ToString());
        Assert.DoesNotContain(password, output.ToString() + error);
    }

    [Theory]
    [InlineData("raw_blob")]
    [InlineData("wrong_entropy")]
    [InlineData("not_base64_text")]
    [InlineData("empty_file")]
    [InlineData("blank_file")]
    public async Task A_file_the_exporter_cannot_decrypt_stops_check_config_with_exit_2_and_one_line_naming_the_likely_causes(string kind)
    {
        if (!OperatingSystem.IsWindows()) { return; }

        const string password = "made-up-Pa55 word";
        var file = Path.Combine(_root.Root, "store.dpapi");
        switch (kind)
        {
            case "raw_blob": File.WriteAllBytes(file, Protect(password, DocumentedEntropy)); break;
            case "wrong_entropy": File.WriteAllText(file, Convert.ToBase64String(Protect(password, "some.other.entropy"))); break;
            case "not_base64_text": File.WriteAllText(file, password); break;
            case "empty_file": File.WriteAllText(file, ""); break;
            case "blank_file": File.WriteAllText(file, "  \r\n"); break;
            default: throw new ArgumentOutOfRangeException(nameof(kind));
        }

        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await CliVerbs.CheckConfigAsync(["check-config", "--config", WriteConfig(file, ClosedPort())], output, error, CancellationToken.None);

        Assert.Equal(2, exit);
        Assert.Equal("", output.ToString());
        var line = Assert.Single(error.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries)).TrimEnd();
        Assert.StartsWith($"Config error: store.password.path: DPAPI credential file '{file}' could not be decrypted (", line);
        Assert.Contains("Likely causes:", line);
        Assert.Contains("not the base64 text of a DPAPI blob", line);
        Assert.Contains($"other entropy than '{DocumentedEntropy}'", line);
        Assert.Contains("written on another machine", line);
        Assert.DoesNotContain(password, line);
    }
}
