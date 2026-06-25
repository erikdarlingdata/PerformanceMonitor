/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Renders a list of <see cref="MetricBarItem"/> as per-entity horizontal bar cards
    /// (label | proportional entity-colored bar | value). Shared by both apps (lives in .Ui) so
    /// the per-DB overview can't drift. Ported from PerformanceStudio's per-DB bar cards.
    /// </summary>
    public partial class MetricBarCards : UserControl
    {
        public MetricBarCards()
        {
            InitializeComponent();
        }

        /// <summary>The bar rows to render (a list of <see cref="MetricBarItem"/>).</summary>
        public static readonly DependencyProperty ItemsProperty = DependencyProperty.Register(
            nameof(Items), typeof(IEnumerable), typeof(MetricBarCards), new PropertyMetadata(null));
        public IEnumerable? Items
        {
            get => (IEnumerable?)GetValue(ItemsProperty);
            set => SetValue(ItemsProperty, value);
        }
    }
}
