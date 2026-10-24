using System;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Alert Details window's link (#5847, #6390): what it says and what it does. Built in one place from the metric and the server's
/// engine (<c>AlertChainLink.TargetFor</c>), so the text names where <see cref="Open"/> goes.
/// </summary>
public sealed record AlertChainOpener(string Text, Action Open);
