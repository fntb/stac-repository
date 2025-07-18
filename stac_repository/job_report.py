from enum import Enum

from typing import (
    NamedTuple,
    Union
)


class JobState(Enum):
    SUCCESS = 0
    FAILURE = 1
    INPROGRESS = -1


class JobReport(NamedTuple):
    context: str
    details: Union[str, BaseException]
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
