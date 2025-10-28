#!/usr/bin/env python3
"""
KV vs Redis Benchmark Analysis Script

Parses Criterion benchmark results and generates comprehensive comparison reports
between the KV service and Redis performance.
"""

import argparse
import json
import statistics
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class BenchmarkAnalyzer:
    """Analyzes benchmark results and generates comparison reports."""

    def __init__(self, results_file: str):
        """Initialize with benchmark results file."""
        self.results_file = Path(results_file)
        self.data = self._load_results()
        self.comparisons = {}

    def _load_results(self) -> Dict:
        """Load benchmark results from JSON file."""
        try:
            with open(self.results_file, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: Results file {self.results_file} not found")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in {self.results_file}: {e}")
            sys.exit(1)

    def _extract_benchmark_data(self, benchmark_name: str) -> Dict:
        """Extract data for a specific benchmark."""
        if "benchmarks" not in self.data:
            return {}

        benchmark_data = {}
        for bench in self.data["benchmarks"]:
            if bench["name"].startswith(benchmark_name):
                # Extract size from benchmark name (e.g., "kv_set/1" -> size=1)
                parts = bench["name"].split("/")
                if len(parts) > 1:
                    try:
                        size = int(parts[1])
                        benchmark_data[size] = {
                            "mean": bench["mean"]["point_estimate"],
                            "median": bench["median"]["point_estimate"],
                            "p95": bench["p95"]["point_estimate"],
                            "p99": bench["p99"]["point_estimate"],
                            "min": bench["min"]["point_estimate"],
                            "max": bench["max"]["point_estimate"],
                        }
                    except ValueError:
                        continue

        return benchmark_data

    def _calculate_throughput(self, mean_time_ns: float, operations: int) -> float:
        """Calculate operations per second from mean time and operation count."""
        if mean_time_ns <= 0:
            return 0.0
        # Convert nanoseconds to seconds and calculate ops/sec
        time_seconds = mean_time_ns / 1_000_000_000
        return operations / time_seconds

    def analyze_basic_operations(self) -> Dict:
        """Analyze basic operations (SET, GET, DEL, EXISTS)."""
        operations = ["set", "get", "delete", "exists"]
        results = {}

        for op in operations:
            kv_data = self._extract_benchmark_data(f"kv_{op}")
            redis_data = self._extract_benchmark_data(f"redis_{op}")

            op_results = []
            for size in sorted(set(kv_data.keys()) | set(redis_data.keys())):
                if size in kv_data and size in redis_data:
                    kv_time = (
                        kv_data[size]["mean"] / 1_000_000
                    )  # Convert to microseconds
                    redis_time = redis_data[size]["mean"] / 1_000_000

                    ratio = kv_time / redis_time if redis_time > 0 else float("inf")
                    winner = "Redis" if ratio > 1 else "KV" if ratio < 1 else "Tie"

                    op_results.append(
                        {
                            "size": size,
                            "kv_time_us": kv_time,
                            "redis_time_us": redis_time,
                            "ratio": ratio,
                            "winner": winner,
                            "kv_throughput": self._calculate_throughput(
                                kv_data[size]["mean"], size
                            ),
                            "redis_throughput": self._calculate_throughput(
                                redis_data[size]["mean"], size
                            ),
                        }
                    )

            results[op] = op_results

        return results

    def analyze_ttl_operations(self) -> Dict:
        """Analyze TTL operations (SETEX, EXPIRE, TTL)."""
        operations = ["setex", "expire", "ttl"]
        results = {}

        for op in operations:
            kv_data = self._extract_benchmark_data(f"kv_{op}")
            redis_data = self._extract_benchmark_data(f"redis_{op}")

            op_results = []
            for size in sorted(set(kv_data.keys()) | set(redis_data.keys())):
                if size in kv_data and size in redis_data:
                    kv_time = (
                        kv_data[size]["mean"] / 1_000_000
                    )  # Convert to microseconds
                    redis_time = redis_data[size]["mean"] / 1_000_000

                    ratio = kv_time / redis_time if redis_time > 0 else float("inf")
                    winner = "Redis" if ratio > 1 else "KV" if ratio < 1 else "Tie"

                    op_results.append(
                        {
                            "size": size,
                            "kv_time_us": kv_time,
                            "redis_time_us": redis_time,
                            "ratio": ratio,
                            "winner": winner,
                        }
                    )

            results[op] = op_results

        return results

    def analyze_concurrent_operations(self) -> Dict:
        """Analyze concurrent operations."""
        operations = ["concurrent_set", "concurrent_get"]
        results = {}

        for op in operations:
            kv_data = self._extract_benchmark_data(f"kv_{op}")
            redis_data = self._extract_benchmark_data(f"redis_{op}")

            op_results = []
            for tasks in sorted(set(kv_data.keys()) | set(redis_data.keys())):
                if tasks in kv_data and tasks in redis_data:
                    kv_time = (
                        kv_data[tasks]["mean"] / 1_000_000
                    )  # Convert to microseconds
                    redis_time = redis_data[tasks]["mean"] / 1_000_000

                    ratio = kv_time / redis_time if redis_time > 0 else float("inf")
                    winner = "Redis" if ratio > 1 else "KV" if ratio < 1 else "Tie"

                    # Calculate total operations (tasks * 100 operations per task)
                    total_ops = tasks * 100
                    kv_throughput = self._calculate_throughput(
                        kv_data[tasks]["mean"], total_ops
                    )
                    redis_throughput = self._calculate_throughput(
                        redis_data[tasks]["mean"], total_ops
                    )

                    op_results.append(
                        {
                            "tasks": tasks,
                            "kv_time_us": kv_time,
                            "redis_time_us": redis_time,
                            "ratio": ratio,
                            "winner": winner,
                            "kv_throughput": kv_throughput,
                            "redis_throughput": redis_throughput,
                        }
                    )

            results[op] = op_results

        return results

    def analyze_mixed_workloads(self) -> Dict:
        """Analyze mixed workload operations."""
        workloads = ["read_heavy", "write_heavy", "balanced"]
        results = {}

        for workload in workloads:
            kv_data = self._extract_benchmark_data(f"kv_{workload}")
            redis_data = self._extract_benchmark_data(f"redis_{workload}")

            workload_results = []
            for ops in sorted(set(kv_data.keys()) | set(redis_data.keys())):
                if ops in kv_data and ops in redis_data:
                    kv_time = (
                        kv_data[ops]["mean"] / 1_000_000
                    )  # Convert to microseconds
                    redis_time = redis_data[ops]["mean"] / 1_000_000

                    ratio = kv_time / redis_time if redis_time > 0 else float("inf")
                    winner = "Redis" if ratio > 1 else "KV" if ratio < 1 else "Tie"

                    kv_throughput = self._calculate_throughput(
                        kv_data[ops]["mean"], ops
                    )
                    redis_throughput = self._calculate_throughput(
                        redis_data[ops]["mean"], ops
                    )

                    workload_results.append(
                        {
                            "operations": ops,
                            "kv_time_us": kv_time,
                            "redis_time_us": redis_time,
                            "ratio": ratio,
                            "winner": winner,
                            "kv_throughput": kv_throughput,
                            "redis_throughput": redis_throughput,
                        }
                    )

            results[workload] = workload_results

        return results

    def generate_summary_stats(self) -> Dict:
        """Generate summary statistics across all benchmarks."""
        all_ratios = []
        kv_wins = 0
        redis_wins = 0
        ties = 0

        # Collect ratios from all benchmark categories
        basic_ops = self.analyze_basic_operations()
        for op_data in basic_ops.values():
            for result in op_data:
                all_ratios.append(result["ratio"])
                if result["winner"] == "KV":
                    kv_wins += 1
                elif result["winner"] == "Redis":
                    redis_wins += 1
                else:
                    ties += 1

        ttl_ops = self.analyze_ttl_operations()
        for op_data in ttl_ops.values():
            for result in op_data:
                all_ratios.append(result["ratio"])
                if result["winner"] == "KV":
                    kv_wins += 1
                elif result["winner"] == "Redis":
                    redis_wins += 1
                else:
                    ties += 1

        concurrent_ops = self.analyze_concurrent_operations()
        for op_data in concurrent_ops.values():
            for result in op_data:
                all_ratios.append(result["ratio"])
                if result["winner"] == "KV":
                    kv_wins += 1
                elif result["winner"] == "Redis":
                    redis_wins += 1
                else:
                    ties += 1

        mixed_workloads = self.analyze_mixed_workloads()
        for workload_data in mixed_workloads.values():
            for result in workload_data:
                all_ratios.append(result["ratio"])
                if result["winner"] == "KV":
                    kv_wins += 1
                elif result["winner"] == "Redis":
                    redis_wins += 1
                else:
                    ties += 1

        if not all_ratios:
            return {}

        return {
            "total_benchmarks": len(all_ratios),
            "kv_wins": kv_wins,
            "redis_wins": redis_wins,
            "ties": ties,
            "kv_win_rate": kv_wins / len(all_ratios) * 100,
            "redis_win_rate": redis_wins / len(all_ratios) * 100,
            "avg_ratio": statistics.mean(all_ratios),
            "median_ratio": statistics.median(all_ratios),
            "min_ratio": min(all_ratios),
            "max_ratio": max(all_ratios),
        }

    def generate_markdown_report(self) -> str:
        """Generate a comprehensive markdown report."""
        report = []
        report.append("# KV vs Redis Benchmark Results")
        report.append("")
        report.append("## Summary")
        report.append("")

        summary = self.generate_summary_stats()
        if summary:
            report.append(f"- **Total Benchmarks**: {summary['total_benchmarks']}")
            report.append(
                f"- **KV Wins**: {summary['kv_wins']} ({summary['kv_win_rate']:.1f}%)"
            )
            report.append(
                f"- **Redis Wins**: {summary['redis_wins']} ({summary['redis_win_rate']:.1f}%)"
            )
            report.append(f"- **Ties**: {summary['ties']}")
            report.append(
                f"- **Average Performance Ratio**: {summary['avg_ratio']:.2f}x"
            )
            report.append(
                f"- **Median Performance Ratio**: {summary['median_ratio']:.2f}x"
            )
            report.append(
                f"- **Performance Range**: {summary['min_ratio']:.2f}x - {summary['max_ratio']:.2f}x"
            )
            report.append("")

            if summary["avg_ratio"] < 1.0:
                report.append("**Overall Winner**: KV Service (faster on average)")
            elif summary["avg_ratio"] > 1.0:
                report.append("**Overall Winner**: Redis (faster on average)")
            else:
                report.append("**Overall Winner**: Tie (comparable performance)")
            report.append("")

        # Basic Operations
        report.append("## Basic Operations Comparison")
        report.append("")
        report.append(
            "| Operation | Size | Redis (µs) | KV (µs) | Ratio | Winner | KV Ops/s | Redis Ops/s |"
        )
        report.append(
            "|-----------|------|------------|---------|-------|--------|----------|-------------|"
        )

        basic_ops = self.analyze_basic_operations()
        for op, results in basic_ops.items():
            for result in results:
                report.append(
                    f"| {op.upper()} | {result['size']} | {result['redis_time_us']:.1f} | {result['kv_time_us']:.1f} | {result['ratio']:.2f}x | {result['winner']} | {result['kv_throughput']:,.0f} | {result['redis_throughput']:,.0f} |"
                )

        report.append("")

        # TTL Operations
        report.append("## TTL Operations Comparison")
        report.append("")
        report.append("| Operation | Size | Redis (µs) | KV (µs) | Ratio | Winner |")
        report.append("|-----------|------|------------|---------|-------|--------|")

        ttl_ops = self.analyze_ttl_operations()
        for op, results in ttl_ops.items():
            for result in results:
                report.append(
                    f"| {op.upper()} | {result['size']} | {result['redis_time_us']:.1f} | {result['kv_time_us']:.1f} | {result['ratio']:.2f}x | {result['winner']} |"
                )

        report.append("")

        # Concurrent Operations
        report.append("## Concurrent Operations Comparison")
        report.append("")
        report.append(
            "| Operation | Tasks | Redis (µs) | KV (µs) | Ratio | Winner | KV Ops/s | Redis Ops/s |"
        )
        report.append(
            "|-----------|-------|------------|---------|-------|--------|----------|-------------|"
        )

        concurrent_ops = self.analyze_concurrent_operations()
        for op, results in concurrent_ops.items():
            for result in results:
                report.append(
                    f"| {op.replace('_', ' ').title()} | {result['tasks']} | {result['redis_time_us']:.1f} | {result['kv_time_us']:.1f} | {result['ratio']:.2f}x | {result['winner']} | {result['kv_throughput']:,.0f} | {result['redis_throughput']:,.0f} |"
                )

        report.append("")

        # Mixed Workloads
        report.append("## Mixed Workload Comparison")
        report.append("")
        report.append(
            "| Workload | Operations | Redis (µs) | KV (µs) | Ratio | Winner | KV Ops/s | Redis Ops/s |"
        )
        report.append(
            "|----------|------------|------------|---------|-------|--------|----------|-------------|"
        )

        mixed_workloads = self.analyze_mixed_workloads()
        for workload, results in mixed_workloads.items():
            for result in results:
                report.append(
                    f"| {workload.replace('_', ' ').title()} | {result['operations']} | {result['redis_time_us']:.1f} | {result['kv_time_us']:.1f} | {result['ratio']:.2f}x | {result['winner']} | {result['kv_throughput']:,.0f} | {result['redis_throughput']:,.0f} |"
                )

        report.append("")

        # Performance Analysis
        report.append("## Performance Analysis")
        report.append("")
        report.append("### Strengths")
        report.append("")

        # Find KV's best performing operations
        kv_best_ops = []
        for op, results in basic_ops.items():
            for result in results:
                if result["ratio"] < 0.8:  # KV is significantly faster
                    kv_best_ops.append(
                        f"- {op.upper()} with {result['size']} keys: {result['ratio']:.2f}x faster"
                    )

        if kv_best_ops:
            report.append("**KV Service performs best in:**")
            for op in kv_best_ops[:5]:  # Top 5
                report.append(op)
        else:
            report.append("- No significant performance advantages identified")

        report.append("")
        report.append("### Areas for Improvement")
        report.append("")

        # Find Redis's best performing operations
        redis_best_ops = []
        for op, results in basic_ops.items():
            for result in results:
                if result["ratio"] > 1.2:  # Redis is significantly faster
                    redis_best_ops.append(
                        f"- {op.upper()} with {result['size']} keys: {result['ratio']:.2f}x slower"
                    )

        if redis_best_ops:
            report.append("**KV Service needs improvement in:**")
            for op in redis_best_ops[:5]:  # Top 5
                report.append(op)
        else:
            report.append("- Performance is competitive across all operations")

        report.append("")
        report.append("### Recommendations")
        report.append("")

        if summary and summary["avg_ratio"] > 1.1:
            report.append(
                "1. **Focus on single-operation performance**: KV service shows higher latency for individual operations"
            )
            report.append(
                "2. **Optimize async overhead**: Consider reducing async runtime overhead"
            )
            report.append(
                "3. **Memory allocation optimization**: Review memory allocation patterns"
            )
        elif summary and summary["avg_ratio"] < 0.9:
            report.append(
                "1. **Excellent performance**: KV service is competitive with Redis"
            )
            report.append(
                "2. **Continue current optimizations**: Current implementation is well-optimized"
            )
        else:
            report.append(
                "1. **Balanced performance**: KV service shows competitive performance with Redis"
            )
            report.append(
                "2. **Focus on specific use cases**: Optimize for the most common operation patterns"
            )

        report.append("")
        report.append("---")
        report.append("")
        report.append("*Generated by KV vs Redis Benchmark Analysis*")

        return "\n".join(report)

    def save_report(self, output_file: str):
        """Save the markdown report to a file."""
        report = self.generate_markdown_report()
        with open(output_file, "w") as f:
            f.write(report)
        print(f"Report saved to {output_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Analyze KV vs Redis benchmark results"
    )
    parser.add_argument("results_file", help="Path to Criterion JSON results file")
    parser.add_argument(
        "-o",
        "--output",
        default="kv_vs_redis_report.md",
        help="Output markdown file (default: kv_vs_redis_report.md)",
    )
    parser.add_argument(
        "--json", action="store_true", help="Output raw analysis as JSON"
    )

    args = parser.parse_args()

    analyzer = BenchmarkAnalyzer(args.results_file)

    if args.json:
        # Output raw analysis data as JSON
        analysis = {
            "basic_operations": analyzer.analyze_basic_operations(),
            "ttl_operations": analyzer.analyze_ttl_operations(),
            "concurrent_operations": analyzer.analyze_concurrent_operations(),
            "mixed_workloads": analyzer.analyze_mixed_workloads(),
            "summary": analyzer.generate_summary_stats(),
        }
        print(json.dumps(analysis, indent=2))
    else:
        # Generate and save markdown report
        analyzer.save_report(args.output)


if __name__ == "__main__":
    main()
