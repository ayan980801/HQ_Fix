import os
import pytest
import snowflake.connector


def test_connect_without_scheme():
    user = os.getenv('SF_USER')
    password = os.getenv('SF_PASSWORD')
    if not user or not password:
        pytest.skip('SF_USER and SF_PASSWORD environment variables not set')

    try:
        snowflake.connector.connect(
            user=user,
            password=password,
            account='hmkovlx-nu26765',
            host='hmkovlx-nu26765.snowflakecomputing.com',
            warehouse='COMPUTE_WH',
            database='DEV',
            schema='PUBLIC',
        )
    except snowflake.connector.errors.Error as e:
        pytest.fail(f'Authentication failed: {e}')
