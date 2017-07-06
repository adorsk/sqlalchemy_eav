import json
import logging
import re

import sqlalchemy as _sqla
import sqlalchemy.exc as _sqla_exc

from . import schema

class Dao(object):
    exc = _sqla_exc
    BINARY_OPS = ['=', '<', '>', '<=', '>=', 'LIKE']
    EXISTENCE_OP = 'EXISTS'

    class StaleEntError(Exception): pass

    def __init__(self, db_uri=None, schema=None, engine=None, logger=None):
        self.logger = logger or logging
        self.engine = engine or _sqla.create_engine(db_uri)
        self.schema = schema or self.get_default_schema()

    def get_default_schema(self): return schema.generate_schema()

    def ensure_tables(self): self.create_tables()

    def create_tables(self): self.schema['metadata'].create_all(self.engine)

    def drop_tables(self): self.schema['metadata'].drop_all(self.engine)

    def create_ent(self, ent_key=None, ent_patches=None, attrs=None,
                   connection=None):
        if ent_key is None: ent_key = self.generate_key()
        self.execute(self.schema['tables']['ents'].insert(),
                     [{'key': ent_key, **(ent_patches or {})}],
                     connection=connection)
        if attrs: self.create_attrs(ent_key=ent_key, attrs=attrs,
                                    connection=connection)
        return self.query_ents(query={
            'ent_filters': [{'col': 'key', 'op': '=', 'arg': ent_key}]
        }, connection=connection)[ent_key]

    def generate_key(self): return schema.generate_key()

    def execute(self, *args, connection=None, **kwargs):
        connection = connection or self.connection
        return connection.execute(*args, **kwargs)

    @property
    def connection(self): return self.engine.connect()

    def create_attrs(self, ent_key=None, attrs=None, connection=None):
        statement = self.schema['tables']['attrs'].insert()
        values = [
            {'ent_key': ent_key, 'attr': attr, **self.serialize_value(value)}
            for attr, value in attrs.items()
        ]
        return self.execute(statement, values, connection=connection)

    def serialize_value(self, value=None):
        type_ = type(value).__name__
        if type_ != 'str': value = json.dumps(value)
        return {'type': type_, 'value': value}

    def deserialize_value(self, raw_value=None, type_=None):
        if type_ and type_ != 'str': return json.loads(raw_value)
        return raw_value

    def update_ent(self, ent_key=None, ent_patches=None, attr_patches=None,
                   attr_deletions=None, ent_modified=None, connection=None):
        connection = connection or self.connection
        trans = connection.begin()
        try:
            if ent_modified:
                self.validate_and_update_ent_modified(
                    ent_key=ent_key, modified=ent_modified,
                    connection=connection)
            if attr_patches or attr_deletions:
                self.update_ent_attrs(ent_key=ent_key, patches=attr_patches,
                                      deletions=attr_deletions,
                                      connection=connection)
            self.patch_ent(ent_key=ent_key, patches=ent_patches, 
                           connection=connection)
            trans.commit()
        except:
            trans.rollback()
            raise

    def validate_and_update_ent_modified(self, ent_key=None, modified=None,
                                         connection=None):
        ents = self.schema['tables']['ents']
        result = self.patch_ent(
            ent_key=ent_key,
            wheres=[(ents.c.modified == modified)],
            connection=connection
        )
        if result.rowcount != 1: raise self.StaleEntError()

    def patch_ent(self, ent_key=None, patches=None, wheres=None,
                  connection=None):
        ents = self.schema['tables']['ents']
        statement = ents.update().where(ents.c.key == ent_key)
        if patches: statement = statement.values(patches)
        for where in (wheres or []): statement = statement.where(where)
        return self.execute(statement, connection=connection)

    def update_ent_attrs(self, ent_key=None, patches=None, deletions=None,
                         connection=None):
        deletions = deletions or []
        attrs_to_delete = list(patches.keys()) + deletions
        patches_to_insert = {attr: value for attr, value in patches.items()
                             if attr not in deletions}
        self.delete_attrs(ent_key=ent_key, attrs_to_delete=attrs_to_delete,
                          connection=connection)
        if patches_to_insert:
            self.create_attrs(ent_key=ent_key, attrs=patches_to_insert,
                              connection=connection)

    def delete_attrs(self, ent_key=None, attrs_to_delete=None, connection=None):
        if not attrs_to_delete: return
        attrs_table = self.schema['tables']['attrs']
        statement = attrs_table.delete().where(
            (attrs_table.c.ent_key == ent_key)
            & (attrs_table.c.attr.in_(attrs_to_delete))
        )
        self.execute(statement, connection=connection)

    def execute_sql(self, sql=None, params=None, rw_mode=None, connection=None):
        connection = connection or self.connection
        trans = connection.begin()
        try:
            statement = _sqla.text(sql).bindparams(**(params or {}))
            result_proxy = self.execute(statement, connection=connection)
            if rw_mode == 'w': trans.commit()
            else: trans.rollback()
            if result_proxy.returns_rows:
                results = self.result_proxy_to_dicts(result_proxy=result_proxy)
            else: results = None
            return results
        except:
            trans.rollback()
            raise

    def result_proxy_to_dicts(self, result_proxy=None):
        return [dict(row) for row in result_proxy.fetchall()]

    def ent_attr_dicts_to_ent_dicts(self, ent_attr_dicts=None):
        ent_dicts = {}
        for ent_attr_dict in ent_attr_dicts:
            ent_key = ent_attr_dict['ent_key']
            if ent_key not in ent_dicts:
                ent_dicts[ent_key] = {
                    'key': ent_key,
                    'modified': ent_attr_dict['ent_modified'],
                    'attrs': {},
                }
            try: type_ = ent_attr_dict['type']
            except: type_ = None
            ent_dicts[ent_key]['attrs'][ent_attr_dict['attr']] = \
                    self.deserialize_value(raw_value=ent_attr_dict['value'],
                                           type_=type_)
        return ent_dicts

    def query_ents(self, query=None, connection=None):
        statement = self.get_ents_query_statement(query=query)
        ent_attr_dicts = \
                self.execute(statement, connection=connection).fetchall()
        return self.ent_attr_dicts_to_ent_dicts(ent_attr_dicts=ent_attr_dicts)

    def get_ents_query_statement(self, query=None):
        query_components = self.get_ents_query_components(query=query)
        statement = self.query_components_to_statement(
            query_components=query_components)
        return statement

    def get_ents_query_components(self, query=None):
        query = query or {}
        query_components = self._get_ents_base_query_components()
        outer_attrs = query_components['tables']['outer_attrs']
        attrs_to_select = query.get('attrs_to_select')
        if attrs_to_select:
            query_components = {
                **query_components,
                'wheres': query_components['wheres'] + [
                    outer_attrs.c.attr.in_(attrs_to_select)]
            }
        for filter_ in (query.get('attr_filters') or []):
            query_components = \
                    self._alter_ents_query_components_per_attr_filter(
                        query_components=query_components, filter_=filter_)
        for filter_ in (query.get('ent_filters') or []):
            query_components = self._alter_ents_query_components_per_ent_filter(
                query_components=query_components, filter_=filter_)
        return query_components

    def _get_ents_base_query_components(self):
        query_components = {
            'tables': {
                'ents': self.schema['tables']['ents'],
                'attrs': self.schema['tables']['attrs'],
            },
        }
        for key, table in list(query_components['tables'].items()):
            query_components['tables']['outer_%s' % key] = table.alias(
                'outer_%s' % key)
        outer_ents = query_components['tables']['outer_ents']
        outer_attrs = query_components['tables']['outer_attrs']
        query_components.update({
            'columns': {
                'attr': outer_attrs.c.attr.label('attr'),
                'value': outer_attrs.c.value.label('value'),
                'type': outer_attrs.c.type.label('type'),
                'ent_key': outer_ents.c.key.label('ent_key'),
                'ent_modified': outer_ents.c.modified.label('ent_modified'),
            },
            'from': outer_ents.join(outer_attrs),
            'wheres': []
        })
        return query_components

    def _alter_ents_query_components_per_attr_filter(
        self, query_components=None, filter_=None):
        filter_type = self.get_filter_type(filter_=filter_)
        alter_fn = None
        if filter_type == 'binary':
            alter_fn = self._alter_ents_query_components_per_attr_binary_filter
        elif filter_type == 'existence':
            alter_fn = \
                    self._alter_ents_query_components_per_attr_existence_filter
        return alter_fn(query_components=query_components, filter_=filter_)

    def get_filter_type(self, filter_=None):
        filter_type = None
        op = filter_['op'].lstrip('! ')
        if op in self.BINARY_OPS: filter_type = 'binary'
        elif op == self.EXISTENCE_OP: filter_type = 'existence'
        else: raise Exception(
            "unknown filter type '{filter}'".format(filter=filter_))
        return filter_type

    def _alter_ents_query_components_per_attr_binary_filter(
        self, query_components=None, filter_=None):
        attrs = query_components['tables']['attrs'].alias()
        subq = (
            attrs.select()
            .where(attrs.c.attr == filter_['attr'])
            .where(self.get_where_clause_for_binary_filter(
                column=attrs.c.value, filter_=filter_))
        ).alias()
        altered_query_components = {
            **query_components,
            'from': query_components['from'].join(subq)
        }
        return altered_query_components

    def get_where_clause_for_binary_filter(self, column=None, filter_=None):
        parsed_op = self.parse_op(op=filter_['op'])
        clause = column.op(parsed_op['op'])(filter_['arg'])
        if parsed_op['negated']: clause = ~clause
        return clause

    def parse_op(self, op=None):
        groups = re.match(r'(! )?(.*)', op).groups()
        return {'negated': groups[0] is not None, 'op': groups[1]}

    def _alter_ents_query_components_per_attr_existence_filter(
        self, query_components=None, filter_=None):
        attrs = query_components['tables']['attrs'].alias()
        outer_ent_key = query_components['tables']['outer_ents'].c.key
        exists_clause = _sqla.exists(
            _sqla.select([attrs.c.ent_key])
            .where(attrs.c.attr == filter_['attr'])
            .where(attrs.c.ent_key == outer_ent_key)
        )
        parsed_op = self.parse_op(filter_['op'])
        if parsed_op['negated']: exists_clause = ~exists_clause
        altered_query_components = {
            **query_components,
            'wheres': query_components['wheres'] + [exists_clause]
        }
        return altered_query_components

    def _alter_ents_query_components_per_ent_filter(self, query_components=None,
                                                    filter_=None):
        where_clause = self.get_where_clause_for_binary_filter(
            column=query_components['tables']['outer_ents'].c[filter_['col']],
            filter_=filter_
        )
        altered_query_components = {
            **query_components,
            'wheres': query_components.get('wheres', []) + [where_clause]
        }
        return altered_query_components

    def query_components_to_statement(self, query_components=None):
        statement = (
            _sqla.select(list(query_components['columns'].values()))
            .select_from(query_components['from'])
            .where(_sqla.and_(*query_components['wheres']))
        )
        return statement

    def upsert_ent(self, ent_key=None, ent_patches=None, attr_patches=None,
                   attr_deletions=None, connection=None):
        connection = connection or self.connection
        with connection.begin():
            try:
                return self.create_ent(
                    ent_key=ent_key, ent_patches=ent_patches,
                    attrs=attr_patches, connection=connection
                )
            except _sqla_exc.IntegrityError:
                return self.update_ent(
                    ent_key=ent_key, ent_patches=ent_patches,
                    attr_patches=attr_patches, attr_deletions=attr_deletions,
                    connection=connection)
