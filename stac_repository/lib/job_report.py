from enum import StrEnum
from typing import NamedTuple
from typing import Any, Self


class JobState(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    INPROGRESS = "INPROGRESS"


class JobReport(NamedTuple):
    context: Any
    details: str | BaseException | Any | None
    state: JobState = JobState.INPROGRESS

    @property
    def error(self) -> BaseException | None:
        if self.state == JobState.FAILURE and isinstance(self.details, BaseException):
            return self.details
        else:
            return None

    @property
    def result(self) -> Any:
        if self.state == JobState.SUCCESS:
            return self.details
        else:
            return None


class JobReporter():

    _report: JobReport

    def __init__(
        self,
        context: Any
    ):
        self._report = JobReport(
            context=context,
            details=None
        )

    @property
    def report(self):
        return self._report

    def progress(
        self,
        message: str | None = None
    ):
        self._report = JobReport(
            context=self._report.context,
            details=message
        )

        return self.report

    def fail(
        self,
        error: BaseException | None = None
    ):
        self._report = JobReport(
            state=JobState.FAILURE,
            context=self._report.context,
            details=error
        )

        return self.report

    def complete(
        self,
        result: Any = None
    ):
        self._report = JobReport(
            state=JobState.SUCCESS,
            context=self._report.context,
            details=result
        )

        return self.report
