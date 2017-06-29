import json


ACTION_TYPES = ['update_ent', 'upsert_ent']

class InvalidActionError(Exception):
    def __init__(self, *args, action=None, **kwargs):
        super().__init__(self, str(action), *args, **kwargs)

class DaoActionsWriter(object):
    def __init__(self, fh=None):
        self.fh = fh

    def write_actions(self, actions=None):
        for action in actions:
            self.validate_action(action=action)
            self.fh.write(json.dumps(action) + "\n")

    def validate_action(self, action=None):
        if action['type'] not in ACTION_TYPES:
            raise InvalidActionError(action=action)

def write_dao_actions(dao_actions=None, dest=None):
    with open(dest, 'w') as f:
        DaoActionsWriter(fh=f).write_actions(actions=dao_actions)

class DaoActionProcessor(object):
    def __init__(self, dao=None):
        self.dao = dao

    def process_action_files(self, action_files=None):
        return [self.process_action_file(action_file=action_file)
                for action_file in action_files]

    def process_action_file(self, action_file=None):
        actions = self.parse_action_file(action_file=action_file)
        return self.process_actions(actions=actions)

    def parse_action_file(self, action_file=None):
        with open(action_file) as f:
            return [json.loads(line.strip()) for line in f]

    def process_actions(self, actions=None):
        return [self.execute_action(action=action) for action in actions]

    def execute_action(self, action=None):
        handler = self.get_action_handler(action=action)
        return handler(**action.get('params', {}))

    def get_action_handler(self, action=None):
        return getattr(self.dao, action['type'])

def process_action_files(action_files=None, dao=None):
    return DaoActionProcessor(dao=dao).process_action_files(
        action_files=action_files)

def process_actions(actions=None, dao=None):
    return DaoActionProcessor(dao=dao).process_actions(actions=actions)
