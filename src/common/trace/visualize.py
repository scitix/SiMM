#!/usr/bin/env python3
"""
SIMM Trace Visualization Tool
"""

import json
import argparse
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
from pathlib import Path

plt.rcParams['font.size'] = 10
plt.rcParams['figure.dpi'] = 150
plt.rcParams['savefig.dpi'] = 150
plt.rcParams['savefig.bbox'] = 'tight'

def load_trace_data(json_file):
    with open(json_file, 'r') as f:
        return json.load(f)

def plot_segment_timelines(data, output_dir=".", segments_per_plot=None):
    """Plot all segment timelines as line charts, split into multiple plots

    Args:
        segments_per_plot: Number of segments per plot, None means all segments in one plot
    """
    segment_data = defaultdict(list)

    # Find the first timestamp as the baseline
    first_timestamp_ns = None
    for trace in data['traces']:
        if trace['points']:
            first_point_timestamp = trace['points'][0]['timestamp_ns']
            if first_timestamp_ns is None or first_point_timestamp < first_timestamp_ns:
                first_timestamp_ns = first_point_timestamp
    
    if first_timestamp_ns is None:
        print("No timestamp data found")
        return

    # Collect all segment data: timestamp (seconds), latency, segment name
    for trace in data['traces']:
        for point in trace['points']:
            if 'segment_latency_us' in point:
                seg_name = point['segment_name']
                latency = point['segment_latency_us']
                timestamp_ns = point['timestamp_ns']
                # Convert to relative time (seconds), starting from the first timestamp
                timestamp_s = (timestamp_ns - first_timestamp_ns) / 1e9
                segment_data[seg_name].append((timestamp_s, latency))
    
    if not segment_data:
        print("No segment data found")
        return

    # Compute global maximum latency
    all_latencies = []
    for points in segment_data.values():
        all_latencies.extend([lat for _, lat in points])
    
    if not all_latencies:
        print("No latency data found")
        return

    # Compute global maximum latency
    max_latency = max(all_latencies)
    y_max = max_latency * 1.2
    
    print(f"\nMax latency: {max_latency:.2f} us")
    print(f"Y-axis range: 0 to {y_max:.2f} us")

    # Split segment list into multiple groups
    segment_list = list(segment_data.items())
    num_segments = len(segment_list)

    # If segments_per_plot is None, all segments in one plot
    if segments_per_plot is None or segments_per_plot <= 0:
        num_plots = 1
        segments_per_plot = num_segments
        print(f"\nTotal segments: {num_segments}, all in one plot")
    else:
        num_plots = (num_segments + segments_per_plot - 1) // segments_per_plot
        print(f"\nTotal segments: {num_segments}, splitting into {num_plots} plots ({segments_per_plot} segments per plot)")
    
    colors = plt.cm.tab20(np.linspace(0, 1, 20))
    saved_files = []
    
    for plot_idx in range(num_plots):
        start_idx = plot_idx * segments_per_plot
        end_idx = min(start_idx + segments_per_plot, num_segments)
        plot_segments = segment_list[start_idx:end_idx]
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(20, 10))
        
        for seg_idx, (seg_name, points) in enumerate(plot_segments):
            if not points:
                continue
            
            # Sort points by timestamp
            points_sorted = sorted(points, key=lambda x: x[0])
            timestamps, latencies = zip(*points_sorted)
            timestamps = np.array(timestamps)
            latencies = np.array(latencies)
            
            num_points = len(points)
            color = colors[seg_idx % len(colors)]
            
            # Plot line
            ax.plot(timestamps, latencies,
                   color=color,
                   linewidth=1.5,
                   alpha=0.7,
                   label=f'{seg_name} (n={num_points})')
        
        ax.set_xlabel('Time (seconds)', fontsize=14, fontweight='bold')
        ax.set_ylabel('Latency (us)', fontsize=14, fontweight='bold')
        ax.set_title(f'SIMM Trace Segments Timeline (Plot {plot_idx + 1}/{num_plots})\nSegments {start_idx + 1}-{end_idx} of {num_segments}', 
                     fontsize=14, fontweight='bold', pad=15)

        # Set y-axis range: from 0 to max latency * 1.2
        ax.set_ylim(0, y_max)
        
        ax.legend(fontsize=9, loc='upper left', framealpha=0.9, ncol=1)
        ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        
        plt.tight_layout()
        
        output_file = f"{output_dir}/segments_timeline_plot_{plot_idx + 1:02d}.png"
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        saved_files.append(output_file)
        print(f"Saved: {output_file}")
        plt.close()
    
    print(f"\nGenerated {len(saved_files)} timeline plots with {num_segments} segments total")


def main():
    parser = argparse.ArgumentParser(description='Visualize SIMM trace data')
    parser.add_argument('json_file', help='Input JSON file from simm_trace_analyzer')
    parser.add_argument('-o', '--output-dir', default='.', help='Output directory for plots')
    parser.add_argument('-n', '--segments-per-plot', type=int, default=None, 
                       help='Number of segments per plot. If not specified, all segments in one plot. If specified, split into multiple plots.')
    
    args = parser.parse_args()
    
    print(f"Loading trace data from: {args.json_file}")
    data = load_trace_data(args.json_file)
    print(f"Loaded {data['total_traces']} traces")
    
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"\nGenerating timeline plots (max {args.segments_per_plot} segments per plot)...")
    plot_segment_timelines(data, args.output_dir, args.segments_per_plot)
    
    print(f"\nPlots saved to: {args.output_dir}")

if __name__ == '__main__':
    main()
