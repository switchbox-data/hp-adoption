#!/usr/bin/env python3

from pathlib import Path

import polars as pl

from data.ma.masssave_downloader import download_masssave_data


def test_masssave_downloader():
    """Test MassSave downloader with simplified filter sets."""

    # Simplified filter sets - only 2 dimensions
    filter_sets = {
        "Year": ["2019", "2020"],
        "End use": ["HVAC", "Hot Water"],
    }

    data = download_masssave_data(outfile=None, filter_sets=filter_sets).cast(pl.Utf8)

    # Create artifact for regression testing
    artifact_path = Path("tests/artifacts/test_masssave_downloader.csv")
    artifact_path.parent.mkdir(exist_ok=True)

    # If artifact doesn't exist, create it (first run)
    if not artifact_path.exists():
        print(f"Creating regression artifact: {artifact_path}")
        data.write_csv(artifact_path)
        print("First run - artifact created for future regression testing")
    else:
        # Load existing artifact and compare
        print(f"Loading existing artifact: {artifact_path}")
        expected_data = pl.read_csv(artifact_path).cast(pl.Utf8)

        # Compare DataFrames
        assert data.shape == expected_data.shape, f"Shape mismatch: got {data.shape}, expected {expected_data.shape}"

        # Compare column names
        assert list(data.columns) == list(expected_data.columns), (
            f"Column mismatch: got {list(data.columns)}, expected {list(expected_data.columns)}"
        )

        print("expected_data")
        print(expected_data)

        print("data")
        print(data)

        # Note that we do *not* sort here, as this test should break if the order changes
        assert expected_data.equals(data), "Data content mismatch"

        print("Regression test passed - data matches expected artifact")
