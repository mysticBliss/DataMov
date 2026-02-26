from datamov.core.data_movements.DataMovements import EnvironmentConfig

def test_environment_config_repr_redaction():
    """
    Test that EnvironmentConfig.__repr__ redacts sensitive driver_details.
    """
    driver_details = {
        "user": "sensitive_user",
        "password": "sensitive_password",
        "jdbc_url": "jdbc:mysql://db.example.com:3306/prod"
    }
    env_config = EnvironmentConfig(
        environment="production",
        driver_details=driver_details,
        kudu_masters=["kudu1", "kudu2"]
    )

    repr_str = repr(env_config)

    # Assert that <REDACTED> is present instead of the actual details
    assert "driver_details=<REDACTED>" in repr_str

    # Assert that sensitive values are NOT present in the repr string
    assert "sensitive_password" not in repr_str
    assert "sensitive_user" not in repr_str
    assert "jdbc:mysql://db.example.com:3306/prod" not in repr_str

    # Assert that non-sensitive fields are still present
    assert "environment=production" in repr_str
    assert "kudu1" in repr_str
    assert "kudu2" in repr_str

if __name__ == "__main__":
    # If run directly, run the test function
    try:
        test_environment_config_repr_redaction()
        print("Test passed!")
    except AssertionError as e:
        print(f"Test failed: {e}")
        exit(1)
