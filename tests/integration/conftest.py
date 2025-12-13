"""Shared fixtures for integration tests."""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def s3_client():
    """Create a mocked S3 client with a test bucket."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        yield client
