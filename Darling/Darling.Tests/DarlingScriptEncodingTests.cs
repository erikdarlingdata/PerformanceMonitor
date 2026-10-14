using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5796: the three Darling PowerShell scripts are Authenticode-signed at release. Windows PowerShell 5.1 reads a
/// script that has no byte-order mark in the machine's ANSI code page, so a BOM-less script holding a non-ASCII
/// character reads as different text on a machine with another code page, and its signature fails to verify
/// there. Each script must be pure ASCII or start with the UTF-8 BOM (EF BB BF), the two forms that read the same
/// everywhere. The second pin keeps the script list here, the scripts the service project ships, and the release
/// guard's DARLING_SCRIPTS in verify_release_signatures.py the same three names.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class DarlingScriptEncodingTests
{
    private static readonly string[] ScriptNames = ["install-darling.ps1", "uninstall-darling.ps1", "upgrade-darling.ps1"];

    public static TheoryData<string> Scripts => new(ScriptNames);

    [Theory]
    [MemberData(nameof(Scripts))]
    public void Script_is_ascii_or_utf8_with_bom(string script)
    {
        var bytes = File.ReadAllBytes(PathTo("Darling", "tools", script));

        Assert.NotEmpty(bytes);
        var problem = EncodingProblem(bytes);
        Assert.True(problem is null, $"{script}: {problem}");
    }

    [Fact]
    public void Encoding_check_refuses_non_ascii_without_a_bom()
    {
        Assert.Null(EncodingProblem("Write-Output 'plain'\r\n"u8.ToArray()));
        Assert.Null(EncodingProblem([0xEF, 0xBB, 0xBF, .. "Write-Output 'caf\u00e9'\r\n"u8.ToArray()]));
        Assert.NotNull(EncodingProblem("Write-Output 'caf\u00e9'\r\n"u8.ToArray()));
        Assert.NotNull(EncodingProblem([0xFF, 0xFE, (byte)'W', 0x00]));
    }

    [Fact]
    public void Pin_covers_every_script_the_service_ships_and_the_release_guard_census()
    {
        var project = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "PerformanceMonitor.Darling.Service.csproj");
        var shipped = Regex.Matches(project, @"Include=""\.\.\\tools\\([^""\\]+\.ps1)""")
            .Select(m => m.Groups[1].Value)
            .Order(StringComparer.Ordinal)
            .ToArray();

        var guard = ReadRepoFile(".github", "scripts", "verify_release_signatures.py");
        var line = Regex.Match(guard, @"^DARLING_SCRIPTS = \(([^)]*)\)", RegexOptions.Multiline);
        Assert.True(line.Success, "verify_release_signatures.py no longer declares DARLING_SCRIPTS on one line");
        var census = Regex.Matches(line.Groups[1].Value, @"""([^""]+)""")
            .Select(m => m.Groups[1].Value)
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(ScriptNames, shipped);
        Assert.Equal(ScriptNames, census);
    }

    /// <summary>Why <paramref name="bytes"/> would not read the same under every code page, or null.</summary>
    private static string? EncodingProblem(byte[] bytes)
    {
        if (bytes is [0xEF, 0xBB, 0xBF, ..])
        {
            return null;
        }

        var offset = Array.FindIndex(bytes, b => b > 0x7F);
        if (offset < 0)
        {
            return null;
        }

        var line = bytes.Take(offset).Count(b => b == (byte)'\n') + 1;
        return $"byte 0x{bytes[offset]:X2} at offset {offset} (line {line}) is not ASCII and the file has no UTF-8 BOM. "
            + "Replace the character with ASCII, or save the file as UTF-8 with a BOM.";
    }
}
