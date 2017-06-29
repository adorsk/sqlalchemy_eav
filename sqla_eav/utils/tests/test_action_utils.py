import collections
import unittest
from unittest.mock import call, MagicMock, patch

from .. import action_utils

class UtilsBaseTestCase(unittest.TestCase):
    def setup_module_mocks(self, attrs=None, module=action_utils):
        patchers = {attr: patch.object(module, attr)
                    for attr in attrs}
        mocks = {}
        for key, patcher in patchers.items():
            self.addCleanup(patcher.stop)
            mocks[key] = patcher.start()
        return mocks

class ActionProcessorBaseTestCase(UtilsBaseTestCase):
    def setUp(self):
        self.dao = MagicMock()
        self.action_processor = action_utils.DaoActionProcessor(dao=self.dao)
        self.action_files = [MagicMock() for i in range(3)]
        self.action_file = self.action_files[0]

    def setup_action_processor_mocks(self, attrs=None):
        for attr in attrs: setattr(self.action_processor, attr, MagicMock())

class ProcessActionFilesTestCase(ActionProcessorBaseTestCase):
    def setUp(self):
        super().setUp()
        self.setup_action_processor_mocks(attrs=['process_action_file'])
        self.result = self.action_processor.process_action_files(
            action_files=self.action_files)

    def test_dispatches_to_process_action_file(self):
        self.assertEqual(
            self.action_processor.process_action_file.call_args_list,
            [call(action_file=action_file) for action_file in self.action_files]
        )
        self.assertEqual(
            self.result,
            [self.action_processor.process_action_file.return_value
             for action_file in self.action_files]
        )

class ProcessActionFileTestCase(ActionProcessorBaseTestCase):
    def setUp(self):
        super().setUp()
        self.setup_action_processor_mocks(attrs=['parse_action_file',
                                                 'process_actions'])
        self.result = self.action_processor.process_action_file(
            action_file=self.action_file)

    def test_parses_action_file(self):
        self.assertEqual(self.action_processor.parse_action_file.call_args,
                         call(action_file=self.action_file))

    def test_processes_actions(self):
        expected_actions = self.action_processor.parse_action_file.return_value
        self.assertEqual(self.action_processor.process_actions.call_args,
                         call(actions=expected_actions))
        self.assertEqual(self.result,
                         self.action_processor.process_actions.return_value)

class ParseActionFileTestCase(ActionProcessorBaseTestCase):
    def setUp(self):
        super().setUp()
        self.mocks = self.setup_module_mocks(attrs=['json', 'open'])
        self.mock_lines = [MagicMock() for i in range(3)]
        expected_fh = self.mocks['open'].return_value.__enter__.return_value
        expected_fh.__iter__.return_value = self.mock_lines
        self.result = self.action_processor.parse_action_file(
            action_file=self.action_file)

    def test_returns_deserialized_action_lines(self):
        self.assertEqual(self.mocks['open'].call_args, call(self.action_file))
        self.assertEqual(self.mocks['json'].loads.call_args_list,
                         [call(line.strip()) for line in self.mock_lines])
        self.assertEqual(self.result, [self.mocks['json'].loads.return_value
                                       for line in self.mock_lines])

class ProcessActionsTestCase(ActionProcessorBaseTestCase):
    def setUp(self):
        super().setUp()
        self.setup_action_processor_mocks(attrs=['execute_action'])
        self.actions = [MagicMock() for i in range(3)]
        self.result = self.action_processor.process_actions(
            actions=self.actions)

    def test_executes_actions(self):
        self.assertEqual(self.action_processor.execute_action.call_args_list,
                         [call(action=action) for action in self.actions])
        self.assertEqual(
            self.result,
            [self.action_processor.execute_action.return_value
             for action in self.actions]
        )

class ExecuteActionTestCase(ActionProcessorBaseTestCase):
    def setUp(self):
        super().setUp()
        self.setup_action_processor_mocks(attrs=['get_action_handler'])
        self.action = MagicMock()
        self.result = self.action_processor.execute_action(action=self.action)

    def test_gets_handler(self):
        self.assertEqual(self.action_processor.get_action_handler.call_args,
                         call(action=self.action))

    def test_calls_handler_with_params(self):
        expected_handler = \
                self.action_processor.get_action_handler.return_value
        self.assertEqual(expected_handler.call_args,
                         call(**self.action['params']))
        self.assertEqual(self.result, expected_handler.return_value)

class GetActionHandlerTestCase(ActionProcessorBaseTestCase):
    def _get_action_handler(self, action_type=None):
        action = collections.defaultdict(MagicMock, **{'type': action_type})
        return self.action_processor.get_action_handler(action=action)

    def test_gets_handler_for_update_ent_action(self):
        result = self._get_action_handler(action_type='update_ent')
        self.assertEqual(result, self.action_processor.dao.update_ent)

    def test_gets_handler_for_upsert_ent_action(self):
        result = self._get_action_handler(action_type='upsert_ent')
        self.assertEqual(result, self.action_processor.dao.upsert_ent)
