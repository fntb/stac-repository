from enum import StrEnum

from typing import (
    NamedTuple,
)


class JobState(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    INPROGRESS = "INPROGRESS"


class JobReport(NamedTuple):
    context: str
    details: str | BaseException
    state: JobState


class JobReportBuilder():

    _context: str

    def __init__(
        self,
        context: str
    ):
        self._context = context

    def progress(
        self,
        message: str
    ):
        return JobReport(
            context=self._context,
            details=message,
            state=JobState.INPROGRESS
        )

    def fail(
        self,
        error: BaseException
    ):
        return JobReport(
            context=self._context,
            details=error,
            state=JobState.FAILURE
        )

    def complete(
        self,
        message: str
    ):
        return JobReport(
            context=self._context,
            details=message,
            state=JobState.SUCCESS
        )
