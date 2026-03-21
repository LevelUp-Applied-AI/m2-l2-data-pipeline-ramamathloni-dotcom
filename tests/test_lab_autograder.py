"""
Lab 2 — CI Autograder Tests

DO NOT MODIFY THIS FILE. It is used by the GitHub Actions autograder.

These tests verify your pipeline meets the structural requirements.
They run alongside your own tests in tests/test_pipeline.py.
"""

import subprocess
import sys
from pathlib import Path


def test_pipeline_file_exists():
    """pipeline.py must exist at the repo root."""
    assert Path("pipeline.py").exists(), "pipeline.py not found"


def test_pipeline_runs_end_to_end():
    """python pipeline.py must run without errors."""
    result = subprocess.run(
        [sys.executable, "pipeline.py"],
        capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, f"pipeline.py failed:\n{result.stderr}"


def test_output_charts_created():
    """pipeline.py must create at least 3 PNG files in output/."""
    # Run the pipeline to generate output
    subprocess.run([sys.executable, "pipeline.py"], capture_output=True, timeout=30)
    output_dir = Path("output")
    assert output_dir.exists(), "output/ directory was not created"
    png_files = list(output_dir.glob("*.png"))
    assert len(png_files) >= 3, f"Expected >=3 PNG files in output/, found {len(png_files)}"


def test_summary_output_contains_revenue():
    """pipeline.py stdout must contain summary statistics."""
    result = subprocess.run(
        [sys.executable, "pipeline.py"],
        capture_output=True, text=True, timeout=30
    )
    output = result.stdout.lower()
    assert "revenue" in output or "total" in output, \
        "Pipeline stdout should contain summary statistics (expected 'revenue' or 'total')"


def test_learner_tests_pass():
    """tests/test_pipeline.py must pass."""
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/test_pipeline.py", "-v"],
        capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, \
        f"Learner tests failed:\n{result.stdout}\n{result.stderr}"


def test_pipeline_functions_importable():
    """Required pipeline functions must be importable and callable."""
    from src.pipeline import load_data, clean_data, add_features, generate_summary
    assert callable(load_data), "load_data must be a callable function"
    assert callable(clean_data), "clean_data must be a callable function"
    assert callable(add_features), "add_features must be a callable function"
    assert callable(generate_summary), "generate_summary must be a callable function"
