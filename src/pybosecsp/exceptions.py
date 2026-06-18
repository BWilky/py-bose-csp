"""Exceptions for the bose-csp-api library."""


class BoseCSPError(Exception):
    """Base exception for all Bose CSP errors."""


class BoseCSPConnectionError(BoseCSPError):
    """Raised when a connection to the device fails or is lost."""


class BoseCSPCommandError(BoseCSPError):
    """Raised when a command to the device fails."""
