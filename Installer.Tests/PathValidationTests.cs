using Installer.Core;

namespace Installer.Tests;

/// <summary>
/// Unit tests for PathValidation — the shape checks and normalization applied
/// to the --data-path / --log-path installer options (issue #768). These run
/// without a database.
/// </summary>
public class PathValidationTests
{
    [Theory]
    [InlineData(@"C:\SQLData")]
    [InlineData(@"D:\SQL Data\Monitor")]
    [InlineData(@"C:/SQLData")]
    [InlineData(@"\\fileserver\share\sql")]
    [InlineData("/var/opt/mssql/data")]
    [InlineData(@"C:\Bob's Data")]
    public void TryValidateDirectory_AcceptsAbsolutePaths(string path)
    {
        bool ok = PathValidation.TryValidateDirectory(path, out string normalized, out string error);

        Assert.True(ok, error);
        Assert.NotEmpty(normalized);
    }

    [Theory]
    [InlineData("SQLData")]
    [InlineData(@"relative\path")]
    [InlineData("data.mdf")]
    public void TryValidateDirectory_RejectsRelativePaths(string path)
    {
        bool ok = PathValidation.TryValidateDirectory(path, out _, out string error);

        Assert.False(ok);
        Assert.Contains("absolute", error, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void TryValidateDirectory_RejectsEmpty(string? path)
    {
        bool ok = PathValidation.TryValidateDirectory(path, out _, out string error);

        Assert.False(ok);
        Assert.NotEmpty(error);
    }

    [Theory]
    [InlineData("C:\\SQL\nData")]   // embedded newline (would break the T-SQL literal)
    [InlineData("C:\\SQL\rData")]   // embedded carriage return
    [InlineData("C:\\SQL\tData")]   // embedded tab
    public void TryValidateDirectory_RejectsControlCharacters(string path)
    {
        bool ok = PathValidation.TryValidateDirectory(path, out _, out string error);

        Assert.False(ok);
        Assert.Contains("control", error, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("C:\\SQL<Data")]
    [InlineData("C:\\SQL>Data")]
    [InlineData("C:\\SQL|Data")]
    [InlineData("C:\\SQL*Data")]
    [InlineData("C:\\SQL?Data")]
    [InlineData("C:\\SQL\"Data")]
    public void TryValidateDirectory_RejectsForbiddenCharacters(string path)
    {
        bool ok = PathValidation.TryValidateDirectory(path, out _, out string error);

        Assert.False(ok);
        Assert.Contains("invalid", error, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryValidateDirectory_RejectsOverlyLongPath()
    {
        string longPath = @"C:\" + new string('a', 500);

        bool ok = PathValidation.TryValidateDirectory(longPath, out _, out string error);

        Assert.False(ok);
        Assert.Contains("exceeds", error, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryValidateDirectory_NormalizesTrailingSeparator_Windows()
    {
        bool ok = PathValidation.TryValidateDirectory(@"C:\SQLData", out string normalized, out _);

        Assert.True(ok);
        Assert.Equal(@"C:\SQLData\", normalized);
    }

    [Fact]
    public void TryValidateDirectory_TrimsWhitespaceBeforeValidating()
    {
        bool ok = PathValidation.TryValidateDirectory("  C:\\SQLData  ", out string normalized, out _);

        Assert.True(ok);
        Assert.Equal(@"C:\SQLData\", normalized);
    }

    [Theory]
    [InlineData(@"C:\SQLData", @"C:\SQLData\")]
    [InlineData(@"C:\SQLData\", @"C:\SQLData\")]
    [InlineData("/var/opt/mssql", "/var/opt/mssql/")]
    [InlineData("/var/opt/mssql/", "/var/opt/mssql/")]
    [InlineData(@"\\srv\share", @"\\srv\share\")]
    public void EnsureTrailingSeparator_UsesPathStyle(string input, string expected)
    {
        Assert.Equal(expected, PathValidation.EnsureTrailingSeparator(input));
    }

    [Theory]
    [InlineData(@"C:\x", true)]
    [InlineData(@"\\server\share", true)]
    [InlineData("/etc/data", true)]
    [InlineData(@"relative\x", false)]
    [InlineData("plainword", false)]
    public void IsAbsolute_ClassifiesCorrectly(string path, bool expected)
    {
        Assert.Equal(expected, PathValidation.IsAbsolute(path));
    }
}
