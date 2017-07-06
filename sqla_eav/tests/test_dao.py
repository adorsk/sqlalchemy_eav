import unittest
from unittest.mock import call, MagicMock, patch

from .. import dao

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.dao = dao.Dao(
            schema=MagicMock(),
            engine=MagicMock()
        )
        self.dao.create_tables()

class CreateEntTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.attrs = {'ent_key_%s' % i: 'value_%s' % i for i in range(3)}
        self.dao.execute = MagicMock()
        self.dao.generate_key = MagicMock()
        self.expected_ent_key = self.dao.generate_key.return_value
        self.connection = MagicMock()
        self.dao.create_attrs = MagicMock()
        self.dao.query_ents = MagicMock()
        self.result = self.dao.create_ent(attrs=self.attrs,
                                          connection=self.connection)

    def test_execute_ent_insert_statement(self):
        self.assertEqual(
            self.dao.execute.call_args,
            call(self.dao.schema['tables']['ents'].insert(),
                 [{'key': self.expected_ent_key}],
                 connection=self.connection)
        )

    def test_creates_attrs(self):
        self.assertEqual(
            self.dao.create_attrs.call_args,
            call(ent_key=self.expected_ent_key, attrs=self.attrs,
                 connection=self.connection)
        )

    def test_returns_ent(self):
        self.assertEqual(self.dao.query_ents.call_args,
            call(query={
                'ent_filters': [
                    {'col': 'key', 'op': '=', 'arg': self.expected_ent_key}
                ]
            }, connection=self.connection)
        )
        self.assertEqual(
            self.result,
            self.dao.query_ents.return_value[self.expected_ent_key]
        )

class CreatePropsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.attrs = {'key_%s' % i: 'value_%s' % i for i in range(3)}
        self.dao.execute = MagicMock()
        self.dao.serialize_value = MagicMock()
        self.ent_key = MagicMock()
        self.connection = MagicMock()
        self.dao.create_attrs(
            ent_key=self.ent_key, attrs=self.attrs, connection=self.connection)

    def test_executes_attrs_insert_statement(self):
        self.assertEqual(
            self.dao.execute.call_args,
            call(self.dao.schema['tables']['attrs'].insert(),
                 [{'ent_key': self.ent_key, 'attr': attr,
                   **self.dao.serialize_value(value=value)}
                   for attr, value in self.attrs.items()],
                 connection=self.connection)
        )

class PatchEntTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.patches = {'key_%s' % i: 'value_%s' % i for i in range(3)}
        self.deletions = ['key_to_delete_%s' % i for i in range(3)]
        self.dao.execute = MagicMock()
        self.ent_key = MagicMock()
        self.connection = MagicMock()
        self.ent_modified = MagicMock()
        self.dao.validate_and_update_ent_modified = MagicMock()
        self.dao.update_ent_modified = MagicMock()
        self.dao.delete_attrs = MagicMock()
        self.dao.create_attrs = MagicMock()

    def _update_ent(self, **kwargs):
        return self.dao.update_ent(**{
            'ent_key': self.ent_key, 'patches': self.patches,
            'deletions': self.deletions, 'connection': self.connection,
            **kwargs
        })

    def test_validates_and_updates_ent_modified_if_given_ent_modified(self):
        self._update_ent(ent_modified=self.ent_modified)
        self.assertEqual(
            self.dao.validate_and_update_ent_modified.call_args,
            call(ent_key=self.ent_key, modified=self.ent_modified,
                 connection=self.connection)
        )

    def test_doesnt_validate_ent_modified_if_not_given_ent_modified(self):
        self._update_ent(ent_modified=None)
        self.assertEqual(self.dao.validate_and_update_ent_modified.call_args,
                         None)

    def test_calls_delete_attrs(self):
        self._update_ent()
        expected_attrs_to_delete = list(self.patches.keys()) + self.deletions
        self.assertEqual(
            self.dao.delete_attrs.call_args,
            call(ent_key=self.ent_key, attrs_to_delete=expected_attrs_to_delete,
                 connection=self.connection)
        )

    def test_calls_create_attrs(self):
        self._update_ent()
        expected_patches_to_insert = {
            attr: value for attr, value in self.patches.items()
            if attr not in self.deletions
        }
        self.assertEqual(
            self.dao.create_attrs.call_args,
            call(ent_key=self.ent_key, attrs=expected_patches_to_insert,
                 connection=self.connection)
        )

    def test_updates_ent_modified(self):
        self._update_ent()
        self.assertEqual(self.dao.update_ent_modified.call_args,
                         call(ent_key=self.ent_key, connection=self.connection))

class ValidateAndUpdateEntModifiedTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao.update_ent_modified = MagicMock()
        self.ent_key = MagicMock()
        self.modified = MagicMock()
        self.connection = MagicMock()

    def _validate_and_updated_ent_modified(self, rowcount=1, **kwargs):
        self.dao.update_ent_modified.return_value.rowcount = rowcount
        return self.dao.validate_and_update_ent_modified(**{
            'ent_key': self.ent_key, 'modified': self.modified,
            'connection': self.connection, **kwargs
        })

    def test_calls_update(self):
        self._validate_and_updated_ent_modified()
        expected_ents = self.dao.schema['tables']['ents']
        self.assertEqual(
            self.dao.update_ent_modified.call_args,
            call(ent_key=self.ent_key,
                 wheres=[(expected_ents.c.modified == self.modified)],
                 connection=self.connection
                )
        )

    def test_raises_if_no_rows_returned(self):
        with self.assertRaises(self.dao.StaleEntError):
            self._validate_and_updated_ent_modified(rowcount=0)

class UpdateEntModifiedTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao.execute = MagicMock()
        self.ent_key = MagicMock()
        self.wheres = [MagicMock() for i in range(3)]
        self.connection = MagicMock()
        self.result = self.dao.update_ent_modified(ent_key=self.ent_key,
                                                   wheres=self.wheres,
                                                   connection=self.connection)

    def test_executes_expected_statement(self):
        expected_ents = self.dao.schema['tables']['ents']
        expected_statement = (
            expected_ents.update()
            .where(expected_ents.c.key == self.ent_key)
        )
        for where in self.wheres:
            expected_statement = expected_statement.where(where)
        self.assertEqual(self.dao.execute.call_args,
                         call(expected_statement, connection=self.connection))
        self.assertEqual(self.result, self.dao.execute.return_value)

class DeletePropsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao.execute = MagicMock()
        self.connection = MagicMock()
        self.ent_key = MagicMock()
        self.attrs_to_delete = MagicMock()
        self.dao.delete_attrs(ent_key=self.ent_key,
                              attrs_to_delete=self.attrs_to_delete,
                              connection=self.connection)

    def test_executes_delete_statement(self):
        expected_statement = self.dao.schema['tables']['attrs'].delete().where(
            (self.dao.schema['tables']['attrs'].c.ent_key == self.ent_key)
            & (self.dao.schema['tables']['attrs'].c.attr.in_(
                self.attrs_to_delete))
        )
        self.assertEqual(self.dao.execute.call_args,
                         call(expected_statement, connection=self.connection))

class ExecuteSqlTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao.execute = MagicMock()
        self.sql = 'some sql'
        self.params = MagicMock()
        self.connection = MagicMock()
        self.dao.result_proxy_to_dicts = MagicMock()

    def _execute_sql(self, **kwargs):
        return self.dao.execute_sql(
            **{'sql': self.sql, 'params': self.params,
               'connection': self.connection, **kwargs})

    @patch.object(dao._sqla, 'text')
    def test_executes_sql_statment(self, _text):
        self._execute_sql()
        self.assertEqual(
            self.dao.execute.call_args,
            call(_text(self.sql).bindparams(**self.params),
                 connection=self.connection)
        )

    def test_returns_dict_results(self):
        result = self._execute_sql()
        self.assertEqual(
            self.dao.result_proxy_to_dicts.call_args,
            call(result_proxy=self.dao.execute.return_value)
        )
        self.assertEqual(result, self.dao.result_proxy_to_dicts.return_value)

    def test_commits_transaction_for_w_mode(self):
        self._execute_sql(rw_mode='w')
        self.assertEqual(self.connection.begin().commit.call_args, call())

    def test_rollsback_transaction_for_non_w_mode(self):
        self._execute_sql()
        self.assertEqual(self.connection.begin().commit.call_args, None)
        self.assertEqual(self.connection.begin().rollback.call_args, call())

class QueryEntsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao.get_ents_query_statement = MagicMock()
        self.dao.execute = MagicMock()
        self.dao.ent_attr_dicts_to_ent_dicts = MagicMock()
        self.attrs_to_select = MagicMock()
        self.attr_filters = MagicMock()
        self.ent_filters = MagicMock()
        self.connection = MagicMock()
        self.query = {
            'attrs_to_select': self.attrs_to_select,
            'attr_filters': self.attr_filters,
            'ent_filters': self.ent_filters,
        }
        self.result = self.dao.query_ents(query=self.query,
                                          connection=self.connection)

    def test_gets_statement(self):
        self.assertEqual(self.dao.get_ents_query_statement.call_args,
                         call(query=self.query))

    def test_executes_statement(self):
        self.assertEqual(
            self.dao.execute.call_args,
            call(self.dao.get_ents_query_statement.return_value,
                 connection=self.connection)
        )

    def test_returns_ent_dicts(self):
        self.assertEqual(
            self.dao.ent_attr_dicts_to_ent_dicts.call_args,
            call(ent_attr_dicts=self.dao.execute.return_value.fetchall())
        )
        self.assertEqual(self.result,
                         self.dao.ent_attr_dicts_to_ent_dicts.return_value)

class GetEntsQueryStatement(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao.get_ents_query_components = MagicMock()
        self.dao.query_components_to_statement = MagicMock()
        self.attrs_to_select = MagicMock()
        self.attr_filters = MagicMock()
        self.query = {
            'attrs_to_select': self.attrs_to_select,
            'attr_filters': self.attr_filters
        }
        self.result = self.dao.get_ents_query_statement(query=self.query)

    def test_gets_query_components(self):
        self.assertEqual(self.dao.get_ents_query_components.call_args,
                         call(query=self.query))

    def test_returns_compiled_query_components(self):
        self.assertEqual(
            self.dao.query_components_to_statement.call_args,
            call(query_components=\
                 self.dao.get_ents_query_components.return_value)
        )
        self.assertEqual(
            self.result,
            self.dao.query_components_to_statement.return_value
        )

class GetEntsQueryComponents(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao._alter_ents_query_components_per_attr_filter = MagicMock()
        self.dao._alter_ents_query_components_per_ent_filter = MagicMock()
        self.dao._get_ents_base_query_components = MagicMock()
        self.expected_components = \
                self.dao._get_ents_base_query_components.return_value

    def test_gets_base_query_components(self):
        self.dao.get_ents_query_components()
        self.assertEqual(self.dao._get_ents_base_query_components.call_args,
                         call())

    def test_returns_base_query_components_if_no_params(self):
        actual = self.dao.get_ents_query_components()
        self.assertEqual(actual, self.expected_components)

    def test_limits_attrs_if_attrs_to_select_is_specified(self):
        attrs_to_select = ['attr_%s' % i for i in range(3)]
        actual = self.dao.get_ents_query_components(query={
            'attrs_to_select': attrs_to_select
        })
        outer_attrs = self.expected_components['tables']['outer_attrs']
        expected = {
            **self.expected_components,
            'wheres': (
                self.expected_components['wheres'] +
                [outer_attrs.c.attr.in_(attrs_to_select)]
            )
        }
        self.assertEqual(actual, expected)

    def test_alters_components_per_attr_filters(self):
        attr_filters = ['filter_%s' % i for i in range(3)]
        actual = self.dao.get_ents_query_components(query={
            'attr_filters': attr_filters
        })
        expected = (self.dao._alter_ents_query_components_per_attr_filter
                    .return_value)
        self.assertEqual(actual, expected)
        self.assertEqual(
            len(self.dao._alter_ents_query_components_per_attr_filter\
                .call_args_list),
            len(attr_filters)
        )

    def test_alters_components_per_ent_filters(self):
        ent_filters = ['filter_%s' % i for i in range(3)]
        actual = self.dao.get_ents_query_components(query={
            'ent_filters': ent_filters
        })
        expected = (self.dao._alter_ents_query_components_per_ent_filter
                    .return_value)
        self.assertEqual(actual, expected)
        self.assertEqual(
            len(self.dao._alter_ents_query_components_per_ent_filter\
                .call_args_list),
            len(ent_filters)
        )

class AlterEntsQueryComponentsPerPropFilterTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.attr = 'some_attr'
        self.arg = 'some_arg'
        self.op = 'some_op'
        self.query_components = MagicMock()
        self.filter = MagicMock()

    def _alter(self, query_components=None, filter_=None):
        return self.dao._alter_ents_query_components_per_attr_filter(
            query_components=(query_components or self.query_components),
            filter_=(filter_ or self.filter)
        )

    def test_dispatches_for_binary_filter(self):
        self.dao.get_filter_type = MagicMock(return_value='binary')
        self.dao._alter_ents_query_components_per_attr_binary_filter = MagicMock()
        self._alter()
        self.assertEqual(
            self.dao._alter_ents_query_components_per_attr_binary_filter\
            .call_args,
            call(query_components=self.query_components, filter_=self.filter)
        )

    def test_dispatches_for_existence_filter(self):
        self.dao.get_filter_type = MagicMock(return_value='existence')
        self.dao._alter_ents_query_components_per_attr_existence_filter = \
                MagicMock()
        self._alter()
        self.assertEqual(
            self.dao._alter_ents_query_components_per_attr_existence_filter\
            .call_args,
            call(query_components=self.query_components, filter_=self.filter)
        )

class GetFilterTypeTestCase(BaseTestCase):
    def test_handles_binary_filters(self):
        for op in self.dao.BINARY_OPS:
            expected = 'binary'
            actual = self.dao.get_filter_type(filter_={'op': op})
            self.assertEqual(actual, expected)

    def test_handles_existence_filters(self):
        expected = 'existence'
        actual = self.dao.get_filter_type(filter_={'op': self.dao.EXISTENCE_OP})
        self.assertEqual(actual, expected)

class AlterEntsQueryComponentsPerPropBinaryFilterTestCase(BaseTestCase):
    def test_adds_joined_subquery(self):
        self.dao.get_where_clause_for_binary_filter = MagicMock()
        filter_ = {'attr': 'some_attr', 'op': 'some_op', 'arg': 'some_arg'}
        query_components = MagicMock()
        attrs = query_components['tables']['attrs'].alias()
        subq = (
            attrs.select()
            .where(attrs.c.attr == filter_['attr'])
            .where(self.dao.get_where_clause_for_binary_filter(
                column=attrs.c.value, filter_=filter_))
        ).alias()
        expected = {
            **query_components,
            'from': query_components['from'].join(subq)
        }
        actual = self.dao._alter_ents_query_components_per_attr_binary_filter(
            query_components=query_components, filter_=filter_)
        self.assertEqual(query_components['from'].join.call_args,
                         call(subq))
        self.assertEqual(actual, expected)

class GetWhereClauseForBinaryFilterTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.column = MagicMock()
        self.arg = MagicMock()

    def _get_clause(self, filter_=None):
        return self.dao.get_where_clause_for_binary_filter(
            column=self.column, filter_=filter_)

    def _generate_filter(self, op=None): return {'op': op, 'arg': self.arg}

    def test_handles_non_negated_ops(self):
        for op in self.dao.BINARY_OPS:
            filter_ = self._generate_filter(op=op)
            expected = self.column.op(op)(filter_['arg'])
            actual = self._get_clause(filter_=filter_)
            self.assertEqual(actual, expected)

    def test_handles_negated_ops(self):
        negated_ops = ['! %s' % op for op in self.dao.BINARY_OPS]
        for op in negated_ops:
            filter_ = self._generate_filter(op=op)
            expected = ~self.column.op(op)(filter_['arg'])
            actual = self._get_clause(filter_=filter_)
            self.assertEqual(actual, expected)

@unittest.skip("test via e2e")
class AlterEntsQueryComponentsPerHasPropFilterTestCase(BaseTestCase):
    def test_something(self):
        self.fail()

class AlterEntsQueryComponentsPerEntFilterTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao.get_where_clause_for_binary_filter = MagicMock()
        self.query_components = MagicMock()

    def test_adds_wheres(self):
        filter_ = {'col': 'some_col', 'op': 'some_op', 'arg': 'some_arg'}
        query_components = MagicMock()
        expected_where_clause = self.dao.get_where_clause_for_binary_filter(
            column=query_components['tables']['outer_ents'].c[filter_['col']],
            filter_=filter_
        )
        expected = {
            **query_components,
            'wheres': (query_components.get('wheres', []) +
                       [expected_where_clause])
        }
        actual = self.dao._alter_ents_query_components_per_ent_filter(
            query_components=query_components, filter_=filter_)
        self.assertEqual(actual, expected)

class UpsertEntTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        for attr in ['create_ent', 'update_ent']:
            setattr(self.dao, attr, MagicMock())
        self.dao.get_where_clause_for_binary_filter = MagicMock()
        self.query_components = MagicMock()
        self.ent_key = MagicMock()
        self.patches = MagicMock()
        self.deletions = MagicMock()
        self.connection = MagicMock()

    def _upsert_ent(self):
        return self.dao.upsert_ent(
            ent_key=self.ent_key, patches=self.patches,
            deletions=self.deletions, connection=self.connection)

    def test_calls_create(self):
        result = self._upsert_ent()
        self.assertEqual(
            self.dao.create_ent.call_args,
            call(ent_key=self.ent_key, attrs=self.patches,
                 connection=self.connection)
        )
        self.assertEqual(result, self.dao.create_ent.return_value)

    def test_patches_if_create_raises_integrity_error(self):
        self.dao.create_ent.side_effect = \
                dao._sqla_exc.IntegrityError(*[MagicMock() for i in range(3)])
        result = self._upsert_ent()
        self.assertEqual(
            self.dao.update_ent.call_args,
            call(ent_key=self.ent_key, patches=self.patches,
                 deletions=self.deletions, connection=self.connection)
        )
        self.assertEqual(result, self.dao.update_ent.return_value)

class EntPropDictsToEntDictsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ent_attr_dicts = [MagicMock() for i in range(3)]
        self.dao.deserialize_value = MagicMock()
        self.result = self.dao.ent_attr_dicts_to_ent_dicts(
            ent_attr_dicts=self.ent_attr_dicts)

    def test_converts_ent_attr_dicts_to_ent_dicts(self):
        expected_ent_dicts = {}
        for ent_attr_dict in self.ent_attr_dicts:
            ent_key = ent_attr_dict['ent_key']
            if ent_key not in expected_ent_dicts:
                expected_ent_dicts[ent_key] = {
                    'key': ent_key,
                    'modified': ent_attr_dict['ent_modified'],
                    'attrs': {},
                }
            expected_ent_dicts[ent_key]['attrs'][ent_attr_dict['attr']] = \
                    self.dao.deserialize_value(raw_value=ent_attr_dict['value'],
                                               type_=ent_attr_dict['type'])
        self.assertEqual(self.result, expected_ent_dicts)
