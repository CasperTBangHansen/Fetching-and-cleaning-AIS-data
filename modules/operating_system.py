#!/usr/bin/env python
"""
This module finds the operationg system and creates
correctly formated paths for that operationg system.
"""

# Imports
from sys import platform
import os
from typing import Union, TypeVar, List
from modules.errors import PathError

# Custom types for type hinting
PathLike = TypeVar("PathLike", str, None)

class OperatingSystem():
    """
    This module finds the operationg system and creates
    correctly formated paths for that operationg system.
    """
    def __init__(self):
        """Finds platform."""
        self.os = platform

    # pylint: disable = R0201
    def path(self, base_path : str, *paths : Union[str, os.PathLike]) -> str:
        """Returnes the correct formatted path from basepath and any number of *paths."""
        return os.path.join(base_path, *paths)

    # pylint: disable = R0201
    def check_file(self, file_path : str) -> bool:
        """Checks if a file exists."""
        return os.path.exists(file_path)

    def check_path(self, base_path : str, *paths : Union[str, os.PathLike]) -> str:
        """
        Returnes the correct formatted path, and checks if this file exists.
        If the file does not exists then an error is raised.
        """
        current_path = self.path(base_path, *paths)
        if not self.check_file(current_path):
            raise PathError(current_path)
        return current_path

    # pylint: disable = R0201
    def get_files(self, folder_path: str) -> List[str]:
        """ Finds every file at folder path."""
        return [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

if __name__ == "__main__":
    ops = OperatingSystem()
    output = ops.path('DataFolder','1','1','1')
    print(output)
