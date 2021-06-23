#!/usr/bin/env python
"""
Custom Errors.
"""

class Error(Exception):
    """Base class for other exceptions."""
    def __init__(self, message : str):
        self.message = message
        super().__init__(self.message)
    def __str__(self):
        return self.message

# Wrong path
class PathError(Error):
    """Raised when a file does not exists."""
    def __init__(self, path : str):
        self.path = path
        super().__init__("No file at: {0}".format(self.path))

# Wrong arguments was given
class WrongArguments(Error):
    """Raised if the input to a function is invalid."""
    def __init__(self, message : str):
        self.message = message
        super().__init__(self.message)

class MissingColumns(Error):
    """Raised if a column in the dataset is missing."""
    def __init__(self, columns : list):
        self.columns = columns
        self.message = (
            'The following columns are missing in the dataset: {0}\n'
            'Please add these columns to the dataset.'
            ).format(
                self.columns
            )
        super().__init__(self.message)

class WrongTimerCall(Error):
    """Tic was called after tic or toc was called after toc."""
    def __init__(self, function : str, should_be_function : str):
        self.function = function
        self.should_be_function = should_be_function
        self.message = "{0:s} was called after {0:s} but should have been {1:s}.".format(
            self.function,
            self.should_be_function
        )
        super().__init__(self.message)

class WrongFunctionCall(Error):
    """Wrong function was called first."""
    def __init__(self, function : str, should_be_function : str):
        self.function = function
        self.should_be_function = should_be_function
        self.message = "{0:s} was called but {1:s} should have been called first.".format(
            self.function,
            self.should_be_function
        )
        super().__init__(self.message)

class WrongArgument(Error):
    """Wrong argument."""
    def __init__(self, variable : str, fix : str):
        self.variable = variable
        self.fix = fix
        self.message = "{0:s} was called: {1:s}".format(
            self.variable,
            self.fix
        )
        super().__init__(self.message)
class WrongLength(Error):
    """List had wrong length"""
    def __init__(self, variable_name: str, length : int, should_be_length : int):
        self.variable_name = variable_name
        self.length = length
        self.should_be_length = should_be_length
        self.message = "{0:s} had the length of {1:d}, but it should have been {2:d}".format(
            self.variable_name,
            self.length,
            self.should_be_length
        )
        super().__init__(self.message)

class RateLimitExceededError(Error):
    """Limit has been exceeded"""
    def __init__(self, message: str = "Limit has been exceeded"):
        self.message = message
        super().__init__(self.message)

class ConnectionDBError(Error):
    """When a connection to a Postgres database fails"""
    def __init__(self, message: str = "Failed to connect with the Postgres database"):
        self.message = message
        super().__init__(self.message)

class NotdefinedError(Error):
    """When a variable is not defined or is None and should not be."""
    def __init__(self, variable : str):
        self.message = "{0} does not exist or is None and shouldnt be.".format(variable)
        super().__init__(self.message)
        