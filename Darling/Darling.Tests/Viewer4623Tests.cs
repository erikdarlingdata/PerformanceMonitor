using System.IO;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>#4623: the empty Server Context card must not point at the repro script.</summary>
public class Viewer4623Tests
{
    [Fact]
    public void EmptyText_DescribesTheEmptyState_WithoutNamingTheReproScript()
    {
        Assert.Equal("Server context is shown for plans opened from a monitored server's data.", ServerContextCard.EmptyText);
        Assert.DoesNotContain("Repro", ServerContextCard.EmptyText);
    }

    [Fact]
    public void Xaml_HasNoLiteralText_AndCodeBehind_UsesTheConstant()
    {
        var root = Directory.GetCurrentDirectory();
        while (root != null && !Directory.Exists(Path.Combine(root, "PerformanceMonitor.Ui")))
            root = Path.GetDirectoryName(root);
        Assert.NotNull(root);
        var xaml = File.ReadAllText(Path.Combine(root!, "PerformanceMonitor.Ui", "PlanViewerControl.xaml"));
        var code = File.ReadAllText(Path.Combine(root!, "PerformanceMonitor.Ui", "PlanViewerControl.Properties.cs"));
        Assert.DoesNotContain("Repro Script to capture", xaml);
        Assert.Contains("ServerContextEmpty.Text = ServerContextCard.EmptyText;", code);
    }
}
