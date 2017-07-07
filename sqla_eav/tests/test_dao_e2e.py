import textwrap
import time
import unittest

from .. import dao

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.db_uri = 'sqlite://'
        self.dao = dao.Dao(db_uri=self.db_uri)
        self.dao.create_tables()
        self.props = {'prop_%s' % i: 'value_%s' % i for i in range(3)}

class CreateEntTestCase(BaseTestCase):
    def test_create_ent(self):
        ent = self.dao.create_ent(props=self.props)
        fetched_ents = self.dao.query_ents(query={
            'ent_filters': [
                {'col': 'key', 'op': '=', 'arg': ent['key']}
            ]
        })
        self.assertEqual(fetched_ents[ent['key']], ent)

class PatchEntTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ent = self.dao.create_ent(props=self.props)
        self.patches = self.generate_patches()

    def generate_patches(self):
        now = time.time()
        return {
            key: '{key}:new_value:{now}'.format(key=key, now=now)
            for key in self.props
        }

    def test_update_ent(self):
        self.dao.update_ent(ent_key=self.ent['key'], patches=self.patches)
        self.assert_patches(patches=self.patches)

    def assert_patches(self, patches=None):
        ent_prop_dicts = self.dao.execute_sql(sql=textwrap.dedent(
            '''
            SELECT ents.modified as ent_modified, props.* FROM
            ents JOIN props ON ents.key=props.ent_key
            '''
        ))
        self.assertEqual(
            self.dao.ent_prop_dicts_to_ent_dicts(
                ent_prop_dicts)[self.ent['key']]['props'],
            patches
        )

    def test_validates_modified_if_given(self):
        self.dao.update_ent(ent_key=self.ent['key'], patches=self.patches)
        with self.assertRaises(self.dao.StaleEntError):
            self.dao.update_ent(ent_key=self.ent['key'], patches=self.patches,
                                 ent_modified=self.ent['modified'])

    def test_doesnt_validate_modified_if_not_given(self):
        patches_2 = self.generate_patches()
        self.dao.update_ent(ent_key=self.ent['key'], patches=self.patches)
        self.dao.update_ent(ent_key=self.ent['key'], patches=patches_2)
        self.assert_patches(patches=patches_2)

class QueryPropsTestCase(BaseTestCase):
    def test_prop_existence_query(self):
        self.dao.create_ent(props=self.props)
        sql = textwrap.dedent(
            '''
            SELECT props.* FROM ents JOIN props ON ents.key=props.ent_key
            WHERE props.prop=:prop
            '''
        )
        prop_dicts = self.dao.execute_sql(
            sql, params={'prop': list(self.props.keys())[0]})
        self.assertTrue(len(prop_dicts) > 0)

class CastPropsTestCase(BaseTestCase):
    def test_prop_casting(self):
        idxs = list(range(6))
        for idx in idxs: self.dao.create_ent(props={'idx': idx})
        sql = textwrap.dedent(
            '''
            SELECT prop, value
            FROM props
            WHERE
                prop=:prop
                AND (CAST(value AS INT) % 2) =:value
            '''
        )
        prop_dicts = self.dao.execute_sql(sql, params={'prop': 'idx',
                                                       'value': 0})
        self.assertEqual(prop_dicts, [{'prop': 'idx', 'value': str(idx)} 
                                      for idx in idxs if (idx % 2) == 0])

class QueryEntsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ent_specs = {
            'ent_1':  {
                'a': 'a.1',
                'b': 'b.1'
            },
            'ent_2': {
                'a': 'a.2',
                'c': 'c.1'
            },
            'ent_3': {
                'a': 'a.1',
                'b': 'b.2'
            }
        }
        self.ents = {
            ent_key: self.dao.create_ent(ent_key=ent_key, props=ent_props)
            for ent_key, ent_props in self.ent_specs.items()
        }

    def test_limit_props(self):
        props_to_select = ['a', 'b']
        actual = self.dao.query_ents(query={
            'props_to_select': props_to_select
        })
        expected = {
            ent_key: {
                'modified': self.ents[ent_key]['modified'],
                'key': ent_key,
                'props': {p: k for p, k in ent['props'].items()
                          if p in props_to_select}
            }
            for ent_key, ent in self.ents.items()
        }
        self.assertEqual(actual, expected)

    def test_prop_binary_filters(self):
        prop_filters = [
            {'prop': 'a', 'op': '=', 'arg': 'a.1'},
            {'prop': 'b', 'op': '=', 'arg': 'b.2'},
        ]
        actual = self.dao.query_ents(query={'prop_filters': prop_filters})
        expected = {}
        for ent_key, ent in self.ents.items():
            passed = True
            for filter_ in prop_filters:
                if ent['props'].get(filter_['prop']) != filter_['arg']:
                    passed = False
                    break
            if passed: expected[ent_key] = ent
        self.assertEqual(actual, expected)

    def test_prop_exists_filters(self):
        prop_filters = [
            {'prop': 'c', 'op': 'EXISTS'}
        ]
        actual = self.dao.query_ents(query={'prop_filters': prop_filters})
        expected = {
            ent_key: ent
            for ent_key, ent in self.ents.items()
            if 'c' in ent['props']
        }
        self.assertEqual(actual, expected)

    def test_prop_not_exists_filters(self):
        prop_filters = [
            {'prop': 'c', 'op': '! EXISTS'}
        ]
        actual = self.dao.query_ents(query={'prop_filters': prop_filters})
        expected = {
            ent_key: ent
            for ent_key, ent in self.ents.items()
            if 'c' not in ent['props']
        }
        self.assertEqual(actual, expected)

class UpsertEntTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ent_key = 'my_ent_key'

    def test_creates_if_not_exists(self):
        self.dao.upsert_ent(ent_key=self.ent_key, patches=self.props)
        fetched_ents = self.dao.query_ents()
        self.assertEqual(fetched_ents[self.ent_key]['props'], self.props)

    def test_patches_if_exists(self):
        self.dao.create_ent(ent_key=self.ent_key, props=self.props)
        prop_keys = list(self.props.keys())
        patches = {prop: '{prop}_new_value'.format(prop=prop)
                   for prop in prop_keys[:-1]}
        deletions = [prop_keys[-1]]
        self.dao.upsert_ent(ent_key=self.ent_key, patches=patches,
                            deletions=deletions)
        fetched_ents = self.dao.query_ents()
        self.assertEqual(fetched_ents[self.ent_key]['props'], patches)
