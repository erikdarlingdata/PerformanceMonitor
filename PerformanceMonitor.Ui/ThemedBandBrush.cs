/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Media;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// #8221: the severity bands (healthy, warning, critical, unknown) are written as the Dark theme's pale tones, which are 1.4:1 to 2.5:1
    /// as text on a Light or Cool Breeze card ("Healthy" and "SYNCHRONIZED" read pale green on white). On those themes each band takes the
    /// theme's own status brush, which is 4.5:1 or better on the card (#3609). Both apps' fleet cards and Availability Group cards use this
    /// one table. A view that paints from it must be bound again after a theme change, since the brush is a frozen copy.
    /// </summary>
    public static class ThemedBandBrush
    {
        /// <summary>
        /// The stale and awaiting amber. It has no status brush of its own, and the Warning brush is an orange-brown on the light themes.
        /// Blocking and elevated (#FFB74D) take that Warning brush, so they stay orange as they are on Dark, and the amber starts from its own
        /// Dark color and is darkened until the text reads 4.5:1, which leaves it a yellow-brown olive. That keeps Dark's hue order
        /// (blocking toward orange, stale toward yellow) and the two card borders clearly apart.
        /// </summary>
        private const string AmberBand = "#FFD54F";

        private static readonly Dictionary<string, string> LightThemeKeys = new(StringComparer.OrdinalIgnoreCase)
        {
            ["#81C784"] = "SuccessBrush",
            ["#E57373"] = "ErrorBrush",
            ["#FFB74D"] = "WarningBrush",
            ["#888888"] = "ForegroundMutedBrush",
        };

        /// <summary>The fills a band's text can sit on: the page, an alternating grid row, and a hovered grid row (Cool Breeze's is #BDD0E1, darker than its page).</summary>
        private static readonly string[] SurfaceKeys = { "BackgroundDarkBrush", "AlternatingRowBrush", "BackgroundLighterBrush" };

        /// <summary>
        /// The current theme's brush for a Dark band color (the stale amber is its own Dark color, darkened), as a frozen copy, or null when the theme is Dark, the color is not a band
        /// color, or no application is running (a unit test): the caller then keeps the Dark color.
        /// </summary>
        public static SolidColorBrush? ForLightTheme(string darkHex)
        {
            if (!ThemeManager.HasLightBackground || Application.Current is null)
            {
                return null;
            }

            Color? start = null;
            if (LightThemeKeys.TryGetValue(darkHex, out var key) && Application.Current.TryFindResource(key) is SolidColorBrush themed)
            {
                start = themed.Color;
            }
            else if (string.Equals(darkHex, AmberBand, StringComparison.OrdinalIgnoreCase))
            {
                start = (Color)ColorConverter.ConvertFromString(darkHex);
            }

            if (start is { } color)
            {
                /* Grid cells and badges sit on the page's darker fill (Cool Breeze's is #CFDDE9, where the theme's green is 3.7:1), and a grid row
                   can show an alternating or a hover fill that is darker still (#BDD0E1), so the brush is darkened until the text reads 4.5:1
                   on the worst of them. The card itself is lighter, so it passes by more. */
                var surfaces = new List<Color>();
                foreach (var surfaceKey in SurfaceKeys)
                {
                    if (Application.Current.TryFindResource(surfaceKey) is SolidColorBrush surface)
                    {
                        surfaces.Add(surface.Color);
                    }
                }

                for (var step = 0; step < 12 && surfaces.Exists(surface => WcagContrast.Ratio(color, surface) < WcagContrast.TextMinimum); step++)
                {
                    color = Color.FromRgb((byte)(color.R * 0.94), (byte)(color.G * 0.94), (byte)(color.B * 0.94));
                }

                var copy = new SolidColorBrush(color);
                copy.Freeze();
                return copy;
            }

            return null;
        }
    }
}
