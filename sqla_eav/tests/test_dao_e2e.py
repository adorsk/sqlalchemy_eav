import textwrap
import time
import unittest

from .. import dao

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.db_uri = 'sqlite://'
        self.dao = dao.Dao(db_uri=self.db_uri)
        self.dao.create_tables()
        self.attrs = {'attr_%s' % i: 'value_%s' % i for i in range(3)}

class CreateEntTestCase(BaseTestCase):
    def test_create_ent(self):
        ent = self.dao.create_ent(attrs=self.attrs)
        fetched_ents = self.dao.query_ents(query={
            'ent_filters': [
                {'col': 'key', 'op': '=', 'arg': ent['key']}
            ]
        })
        self.assertEqual(fetched_ents[ent['key']], ent)

class PatchEntTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ent = self.dao.create_ent(attrs=self.attrs)
        self.patches = self.generate_patches()

    def generate_patches(self):
        now = time.time()
        return {
            key: '{key}:new_value:{now}'.format(key=key, now=now)
            for key in self.attrs
        }

    def test_update_ent(self):
        self.dao.update_ent(ent_key=self.ent['key'], patches=self.patches)
        self.assert_patches(patches=self.patches)

    def assert_patches(self, patches=None):
        ent_attr_dicts = self.dao.execute_sql(sql=textwrap.dedent(
            '''
            SELECT ents.modified as ent_modified, attrs.* FROM
            ents JOIN attrs ON ents.key=attrs.ent_key
            '''
        ))
        self.assertEqual(
            self.dao.ent_attr_dicts_to_ent_dicts(
                ent_attr_dicts)[self.ent['key']]['attrs'],
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
    def test_attr_existence_query(self):
        self.dao.create_ent(attrs=self.attrs)
        sql = textwrap.dedent(
            '''
            SELECT attrs.* FROM ents JOIN attrs ON ents.key=attrs.ent_key
            WHERE attrs.attr=:attr
            '''
        )
        attr_dicts = self.dao.execute_sql(
            sql, params={'attr': list(self.attrs.keys())[0]})
        self.assertTrue(len(attr_dicts) > 0)

class CastPropsTestCase(BaseTestCase):
    def test_attr_casting(self):
        idxs = list(range(6))
        for idx in idxs: self.dao.create_ent(attrs={'idx': idx})
        sql = textwrap.dedent(
            '''
            SELECT attr, value
            FROM attrs
            WHERE
                attr=:attr
                AND (CAST(value AS INT) % 2) =:value
            '''
        )
        attr_dicts = self.dao.execute_sql(sql, params={'attr': 'idx',
                                                       'value': 0})
        self.assertEqual(attr_dicts, [{'attr': 'idx', 'value': str(idx)} 
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
            ent_key: self.dao.create_ent(ent_key=ent_key, attrs=ent_attrs)
            for ent_key, ent_attrs in self.ent_specs.items()
        }

    def test_limit_attrs(self):
        attrs_to_select = ['a', 'b']
        actual = self.dao.query_ents(query={
            'attrs_to_select': attrs_to_select
        })
        expected = {
            ent_key: {
                'modified': self.ents[ent_key]['modified'],
                'key': ent_key,
                'attrs': {p: k for p, k in ent['attrs'].items()
                          if p in attrs_to_select}
            }
            for ent_key, ent in self.ents.items()
        }
        self.assertEqual(actual, expected)

    def test_attr_binary_filters(self):
        attr_filters = [
            {'attr': 'a', 'op': '=', 'arg': 'a.1'},
            {'attr': 'b', 'op': '=', 'arg': 'b.2'},
        ]
        actual = self.dao.query_ents(query={'attr_filters': attr_filters})
        expected = {}
        for ent_key, ent in self.ents.items():
            passed = True
            for filter_ in attr_filters:
                if ent['attrs'].get(filter_['attr']) != filter_['arg']:
                    passed = False
                    break
            if passed: expected[ent_key] = ent
        self.assertEqual(actual, expected)

    def test_attr_exists_filters(self):
        attr_filters = [
            {'attr': 'c', 'op': 'EXISTS'}
        ]
        actual = self.dao.query_ents(query={'attr_filters': attr_filters})
        expected = {
            ent_key: ent
            for ent_key, ent in self.ents.items()
            if 'c' in ent['attrs']
        }
        self.assertEqual(actual, expected)

    def test_attr_not_exists_filters(self):
        attr_filters = [
            {'attr': 'c', 'op': '! EXISTS'}
        ]
        actual = self.dao.query_ents(query={'attr_filters': attr_filters})
        expected = {
            ent_key: ent
            for ent_key, ent in self.ents.items()
            if 'c' not in ent['attrs']
        }
        self.assertEqual(actual, expected)

class UpsertEntTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ent_key = 'my_ent_key'

    def test_creates_if_not_exists(self):
        self.dao.upsert_ent(ent_key=self.ent_key, patches=self.attrs)
        fetched_ents = self.dao.query_ents()
        self.assertEqual(fetched_ents[self.ent_key]['attrs'], self.attrs)

    def test_patches_if_exists(self):
        self.dao.create_ent(ent_key=self.ent_key, attrs=self.attrs)
        attr_keys = list(self.attrs.keys())
        patches = {attr: '{attr}_new_value'.format(attr=attr)
                   for attr in attr_keys[:-1]}
        deletions = [attr_keys[-1]]
        self.dao.upsert_ent(ent_key=self.ent_key, patches=patches,
                            deletions=deletions)
        fetched_ents = self.dao.query_ents()
        self.assertEqual(fetched_ents[self.ent_key]['attrs'], patches)
