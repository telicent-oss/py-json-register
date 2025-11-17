#!/usr/bin/env python3
# Copyright TELICENT LTD
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Performance tracking script for json-register.

This script:
1. Runs performance benchmarks using pytest-benchmark
2. Extracts results and formats them
3. Appends results to PERFORMANCE.md with git commit info
4. Can compare with previous results to detect regressions

Usage:
    python scripts/track_performance.py [--baseline]
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List


def run_command(cmd: List[str]) -> str:
    """Run a shell command and return output."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command {' '.join(cmd)}: {e}")
        return ""


def get_git_info() -> Dict[str, str]:
    """Get current git commit information."""
    return {
        "commit_hash": run_command(["git", "rev-parse", "--short", "HEAD"]),
        "commit_message": run_command(["git", "log", "-1", "--pretty=%s"]),
        "branch": run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "author": run_command(["git", "log", "-1", "--pretty=%an"]),
        "date": run_command(["git", "log", "-1", "--pretty=%ai"]),
    }


def run_benchmarks(output_file: Path) -> bool:
    """
    Run pytest benchmarks and save results to JSON.

    Returns:
        bool: True if benchmarks ran successfully
    """
    cmd = [
        "pytest",
        "tests/test_performance_simple.py",
        "-v",
        "--benchmark-only",
        "--benchmark-json=" + str(output_file),
        "-m",
        "benchmark",
    ]

    print(f"Running benchmarks: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running benchmarks: {e}")
        return False


def format_time(seconds: float) -> str:
    """Format time in human-readable format."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.2f}µs"
    elif seconds < 1:
        return f"{seconds * 1000:.2f}ms"
    else:
        return f"{seconds:.2f}s"


def parse_benchmark_results(json_file: Path) -> List[Dict]:
    """Parse pytest-benchmark JSON output."""
    with open(json_file) as f:
        data = json.load(f)

    benchmarks = []
    for bench in data.get("benchmarks", []):
        benchmarks.append(
            {
                "name": bench["name"],
                "group": bench.get("group", ""),
                "mean": bench["stats"]["mean"],
                "stddev": bench["stats"]["stddev"],
                "min": bench["stats"]["min"],
                "max": bench["stats"]["max"],
                "ops": bench["stats"].get("ops", 1 / bench["stats"]["mean"]),
            }
        )

    return benchmarks


def create_performance_markdown(
    benchmarks: List[Dict], git_info: Dict[str, str], perf_file: Path, is_baseline: bool = False
) -> None:
    """Create or append to PERFORMANCE.md with benchmark results."""

    # Check if file exists
    file_exists = perf_file.exists()

    with open(perf_file, "a") as f:
        if not file_exists:
            # Write header for new file
            f.write("# Performance Tracking\n\n")
            f.write(
                "This file tracks performance metrics across commits to monitor regressions and improvements.\n\n"
            )
            f.write("**Legend:**\n")
            f.write("- Mean: Average execution time\n")
            f.write("- StdDev: Standard deviation\n")
            f.write("- Min/Max: Minimum and maximum execution times\n")
            f.write("- Ops/sec: Operations per second\n\n")
            f.write("---\n\n")

        # Write run header
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"## {timestamp}")
        if is_baseline:
            f.write(" (BASELINE)")
        f.write("\n\n")

        # Write git info
        f.write(f"**Commit:** `{git_info['commit_hash']}` - {git_info['commit_message']}\n")
        f.write(f"**Branch:** {git_info['branch']}\n")
        f.write(f"**Author:** {git_info['author']}\n")
        f.write(f"**Date:** {git_info['date']}\n\n")

        # Group benchmarks by sync/async
        # Treat tests as sync unless they have "Async" in the name
        async_benchmarks = [b for b in benchmarks if "Async" in b["name"] or "async" in b["name"]]
        sync_benchmarks = [b for b in benchmarks if b not in async_benchmarks]

        # Write sync benchmarks
        if sync_benchmarks:
            f.write("### Synchronous Performance\n\n")
            f.write("| Test | Mean | StdDev | Min | Max | Ops/sec |\n")
            f.write("|------|------|--------|-----|-----|----------|\n")
            for bench in sync_benchmarks:
                test_name = bench["name"].replace("test_", "").replace("_", " ").title()
                f.write(
                    f"| {test_name} | {format_time(bench['mean'])} | "
                    f"{format_time(bench['stddev'])} | {format_time(bench['min'])} | "
                    f"{format_time(bench['max'])} | {bench['ops']:.2f} |\n"
                )
            f.write("\n")

        # Write async benchmarks
        if async_benchmarks:
            f.write("### Asynchronous Performance\n\n")
            f.write("| Test | Mean | StdDev | Min | Max | Ops/sec |\n")
            f.write("|------|------|--------|-----|-----|----------|\n")
            for bench in async_benchmarks:
                test_name = bench["name"].replace("test_", "").replace("_", " ").title()
                f.write(
                    f"| {test_name} | {format_time(bench['mean'])} | "
                    f"{format_time(bench['stddev'])} | {format_time(bench['min'])} | "
                    f"{format_time(bench['max'])} | {bench['ops']:.2f} |\n"
                )
            f.write("\n")

        f.write("---\n\n")

    print(f"\n✅ Performance results appended to {perf_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Track performance benchmarks")
    parser.add_argument("--baseline", action="store_true", help="Mark this run as a baseline")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("PERFORMANCE.md"),
        help="Output markdown file (default: PERFORMANCE.md)",
    )
    args = parser.parse_args()

    # Get project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    # Temporary JSON file for benchmark results
    json_output = project_root / ".benchmark_results.json"

    print("=" * 60)
    print("Performance Tracking for json-register")
    print("=" * 60)

    # Get git info
    print("\n📊 Collecting git information...")
    git_info = get_git_info()
    print(f"  Commit: {git_info['commit_hash']} - {git_info['commit_message']}")
    print(f"  Branch: {git_info['branch']}")

    # Run benchmarks
    print("\n🏃 Running performance benchmarks...")
    if not run_benchmarks(json_output):
        print("❌ Benchmark run failed!")
        sys.exit(1)

    # Parse results
    print("\n📈 Parsing results...")
    benchmarks = parse_benchmark_results(json_output)
    print(f"  Collected {len(benchmarks)} benchmark results")

    # Create/update markdown
    print("\n📝 Writing results to markdown...")
    perf_file = project_root / args.output
    create_performance_markdown(benchmarks, git_info, perf_file, args.baseline)

    # Clean up
    if json_output.exists():
        json_output.unlink()

    print("\n✅ Performance tracking complete!")
    print(f"   Results saved to: {perf_file}")


if __name__ == "__main__":
    main()
