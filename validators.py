"""
Input validation utilities for IRP Integration module.

Provides reusable validation functions that raise descriptive
IRPValidationError exceptions when validation fails.
"""

import os
from typing import Any, List
from .exceptions import IRPValidationError


def validate_non_empty_string(value: Any, param_name: str) -> None:
    """
    Validate that a value is a non-empty string.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a non-empty string
    """
    if not isinstance(value, str):
        raise IRPValidationError(
            f"{param_name} must be a string, got {type(value).__name__}"
        )
    if not value.strip():
        raise IRPValidationError(f"{param_name} cannot be empty")


def validate_positive_int(value: Any, param_name: str) -> None:
    """
    Validate that a value is a positive integer.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a positive integer
    """
    if not isinstance(value, int):
        raise IRPValidationError(
            f"{param_name} must be an integer, got {type(value).__name__}"
        )
    if value <= 0:
        raise IRPValidationError(
            f"{param_name} must be positive, got {value}"
        )
    

def validate_non_negative_int(value: Any, param_name: str) -> None:
    """
    Validate that a value is a non-negative integer.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a non-negative integer
    """
    if not isinstance(value, int):
        raise IRPValidationError(
            f"{param_name} must be an integer, got {type(value).__name__}"
        )
    if value < 0:
        raise IRPValidationError(
            f"{param_name} must be non-negative, got {value}"
        )


def validate_file_exists(file_path: str, param_name: str = "file_path") -> None:
    """
    Validate that a file exists at the given path.

    Args:
        file_path: Path to file
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If file does not exist
    """
    if not os.path.exists(file_path):
        raise IRPValidationError(
            f"{param_name} does not exist: {file_path}"
        )
    if not os.path.isfile(file_path):
        raise IRPValidationError(
            f"{param_name} is not a file: {file_path}"
        )


def validate_list_not_empty(value: Any, param_name: str) -> None:
    """
    Validate that a value is a non-empty list.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a non-empty list
    """
    if not isinstance(value, list):
        raise IRPValidationError(
            f"{param_name} must be a list, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise IRPValidationError(f"{param_name} cannot be empty")
    

def validate_positive_float(value: Any, param_name: str) -> None:
    """
    Validate that a value is a positive float.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a positive float
    """
    if not isinstance(value, (float, int)):
        raise IRPValidationError(
            f"{param_name} must be a float, got {type(value).__name__}"
        )
    if value <= 0:
        raise IRPValidationError(
            f"{param_name} must be positive, got {value}"
        )
    
def validate_non_negative_float(value: Any, param_name: str) -> None:
    """
    Validate that a value is a non-negative float.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a non-negative float
    """
    if not isinstance(value, (float, int)):
        raise IRPValidationError(
            f"{param_name} must be a float, got {type(value).__name__}"
        )
    if value < 0:
        raise IRPValidationError(
            f"{param_name} must be non-negative, got {value}"
        )