from unittest.mock import Mock

import pytest
from hamcrest import assert_that, equal_to

from gstream.files.scripts import (
    DELAYS_SCRIPT_BODY,
    BaseRunnerScriptFile,
    DelaysRunnerScriptFile
)


class TestBaseRunnerScriptFile:

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        path = Mock()
        template_body = 'test-key'
        replace_arguments = {'test-key': 'test-value'}
        obj = BaseRunnerScriptFile(
            path=path,
            template_body=template_body,
            replace_arguments=replace_arguments
        )
        assert_that(
            actual_or_assertion=obj._BaseTxtFileWriter__path,
            matcher=equal_to(path)
        )
        assert_that(
            actual_or_assertion=obj._BaseTxtFileWriter__body,
            matcher=equal_to(replace_arguments['test-key'])
        )


class TestDelaysRunnerScriptFile:

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        path = Mock()
        task_id = 'test-id'
        obj = DelaysRunnerScriptFile(
            path=path,
            task_id=task_id
        )
        assert_that(
            actual_or_assertion=obj._BaseTxtFileWriter__path,
            matcher=equal_to(path)
        )
        assert_that(
            actual_or_assertion=obj._BaseTxtFileWriter__body,
            matcher=equal_to(DELAYS_SCRIPT_BODY.replace('[task-id]', task_id))
        )
