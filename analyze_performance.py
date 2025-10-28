#!/usr/bin/env python3
"""
Performance Analysis Script for Reynard KV Service
Analyzes benchmark results and generates performance reports
"""

import json
import statistics
from datetime import datetime
from typing import Dict, List, Tuple


class PerformanceAnalyzer:
    def __init__(self):
        self.benchmark_data = {}
        self.analysis_results = {}

    def add_benchmark_data(self, operation: str, size: int, times: List[float]):
        """Add benchmark timing data"""
        if operation not in self.benchmark_data:
            self.benchmark_data[operation] = {}
        self.benchmark_data[operation][size] = times

    def analyze_performance(self):
        """Analyze performance characteristics"""
        for operation, sizes in self.benchmark_data.items():
            self.analysis_results[operation] = {}

            for size, times in sizes.items():
                mean_time = statistics.mean(times)
                median_time = statistics.median(times)
                std_dev = statistics.stdev(times) if len(times) > 1 else 0
                min_time = min(times)
                max_time = max(times)
                throughput = size / (mean_time / 1_000_000)  # ops/sec

                self.analysis_results[operation][size] = {
                    "mean_time_us": mean_time,
                    "median_time_us": median_time,
                    "std_dev_us": std_dev,
                    "min_time_us": min_time,
                    "max_time_us": max_time,
                    "throughput_ops_per_sec": throughput,
                    "per_operation_us": mean_time / size if size > 0 else 0,
                }

    def generate_report(self) -> str:
        """Generate comprehensive performance report"""
        report = []
        report.append("# Reynard KV Service Performance Analysis")
        report.append(f"Generated: {datetime.now().isoformat()}")
        report.append("")

        # Summary table
        report.append("## Performance Summary")
        report.append("")
        report.append(
            "| Operation | Data Size | Mean Time (µs) | Throughput (ops/sec) | Per-Op (µs) |"
        )
        report.append(
            "|-----------|-----------|----------------|---------------------|-------------|"
        )

        for operation, sizes in self.analysis_results.items():
            for size, stats in sizes.items():
                report.append(
                    f"| {operation} | {size} | {stats['mean_time_us']:.1f} | "
                    f"{stats['throughput_ops_per_sec']:,.0f} | {stats['per_operation_us']:.2f} |"
                )

        report.append("")

        # Detailed analysis
        report.append("## Detailed Analysis")
        report.append("")

        for operation, sizes in self.analysis_results.items():
            report.append(f"### {operation.replace('_', ' ').title()}")
            report.append("")

            # Performance trends
            sizes_list = sorted(sizes.keys())
            if len(sizes_list) > 1:
                first_size = sizes_list[0]
                last_size = sizes_list[-1]
                first_per_op = sizes[first_size]["per_operation_us"]
                last_per_op = sizes[last_size]["per_operation_us"]

                if last_per_op > first_per_op:
                    overhead_increase = (
                        (last_per_op - first_per_op) / first_per_op
                    ) * 100
                    report.append(
                        f"- **Scaling**: Per-operation overhead increases by {overhead_increase:.1f}% "
                        f"from {first_size} to {last_size} keys"
                    )
                else:
                    report.append(
                        f"- **Scaling**: Performance improves with larger datasets"
                    )

            # Best performance
            best_throughput = max(
                stats["throughput_ops_per_sec"] for stats in sizes.values()
            )
            best_size = max(
                sizes.keys(), key=lambda s: sizes[s]["throughput_ops_per_sec"]
            )
            report.append(
                f"- **Peak Performance**: {best_throughput:,.0f} ops/sec at {best_size} keys"
            )

            # Consistency
            all_std_devs = [stats["std_dev_us"] for stats in sizes.values()]
            avg_std_dev = statistics.mean(all_std_devs)
            report.append(
                f"- **Consistency**: Average std deviation {avg_std_dev:.1f}µs"
            )

            report.append("")

        # Recommendations
        report.append("## Recommendations")
        report.append("")

        # Find best performing operations
        best_ops = {}
        for operation, sizes in self.analysis_results.items():
            best_throughput = max(
                stats["throughput_ops_per_sec"] for stats in sizes.values()
            )
            best_ops[operation] = best_throughput

        best_operation = max(best_ops.keys(), key=lambda op: best_ops[op])
        report.append(
            f"1. **Best Operation**: {best_operation} achieves {best_ops[best_operation]:,.0f} ops/sec"
        )

        # Scaling recommendations
        report.append("2. **Scaling**: Use bulk operations for better throughput")
        report.append("3. **Memory**: In-memory storage provides optimal performance")
        report.append("4. **TTL**: Consider TTL cleanup frequency for large datasets")

        report.append("")

        # Comparison with Redis
        report.append("## Redis Comparison")
        report.append("")
        report.append("Based on typical Redis benchmarks:")
        report.append("")
        report.append("| Metric | Redis | KV Service | Ratio |")
        report.append("|--------|-------|------------|-------|")

        # Typical Redis performance (approximate)
        redis_performance = {
            "set_operations": {"1": 50, "1000": 2000},
            "get_operations": {"1": 20, "1000": 8000},
        }

        for operation in ["set_operations", "get_operations"]:
            if operation in self.analysis_results:
                for size in ["1", "1000"]:
                    if (
                        size in self.analysis_results[operation]
                        and size in redis_performance[operation]
                    ):
                        kv_time = self.analysis_results[operation][size]["mean_time_us"]
                        redis_time = redis_performance[operation][size]
                        ratio = redis_time / kv_time
                        report.append(
                            f"| {operation} ({size} keys) | {redis_time}µs | {kv_time:.1f}µs | {ratio:.2f}x |"
                        )

        return "\n".join(report)


def main():
    """Main analysis function"""
    analyzer = PerformanceAnalyzer()

    # Add benchmark data from our test results
    # Set operations
    analyzer.add_benchmark_data("set_operations", 1, [17.062])
    analyzer.add_benchmark_data("set_operations", 10, [48.114])
    analyzer.add_benchmark_data("set_operations", 100, [144.77])
    analyzer.add_benchmark_data("set_operations", 1000, [1511.0])

    # Get operations
    analyzer.add_benchmark_data("get_operations", 1, [140.73])
    analyzer.add_benchmark_data("get_operations", 10, [177.54])
    analyzer.add_benchmark_data("get_operations", 100, [345.28])
    analyzer.add_benchmark_data("get_operations", 1000, [4142.1])

    # Mixed operations
    analyzer.add_benchmark_data("mixed_operations", 100, [381.61])
    analyzer.add_benchmark_data("mixed_operations", 1000, [16408.0])

    # TTL operations
    analyzer.add_benchmark_data("ttl_operations", 100, [479.04])
    analyzer.add_benchmark_data("ttl_operations", 1000, [9595.8])

    # Analyze performance
    analyzer.analyze_performance()

    # Generate and save report
    report = analyzer.generate_report()

    with open("PERFORMANCE_ANALYSIS.md", "w") as f:
        f.write(report)

    print("Performance analysis complete!")
    print("Report saved to: PERFORMANCE_ANALYSIS.md")

    # Print summary
    print("\nQuick Summary:")
    for operation, sizes in analyzer.analysis_results.items():
        best_throughput = max(
            stats["throughput_ops_per_sec"] for stats in sizes.values()
        )
        print(f"- {operation}: {best_throughput:,.0f} ops/sec peak")


if __name__ == "__main__":
    main()
