import uuid
import time

import sqlalchemy as _sqla
import sqlalchemy.types as _sqla_types

def generate_schema():
    schema = {'metadata': _sqla.MetaData()}
    schema['tables'] = {}
    schema['tables']['ents'] = _sqla.Table(
        'ents', schema['metadata'],
        generate_key_column(),
        *generate_timestamp_columns()
    )
    schema['tables']['attrs'] = _sqla.Table(
        'attrs', schema['metadata'],
        generate_key_column(),
        _sqla.Column('ent_key', None, _sqla.ForeignKey('ents.key'),
                     index=True),
        _sqla.Column('attr', _sqla_types.String(length=1024),
                     index=True),
        _sqla.Column('value', _sqla_types.Text(),
                     nullable=True),
        _sqla.Column('type', _sqla_types.String(length=16), nullable=True),
        generate_modified_column()
    )
    return schema

def generate_key_column():
    return _sqla.Column('key', _sqla_types.String(length=255),
                        primary_key=True, default=generate_key)

def generate_key(): return str(uuid.uuid4())

def generate_timestamp_columns():
    return [generate_created_column(), generate_modified_column()]

def _int_time(): return int(time.time() * 1000)

def generate_created_column():
    return _sqla.Column('created', _sqla_types.Integer(), default=_int_time)

def generate_modified_column():
    return _sqla.Column('modified', _sqla_types.Integer(),
                        default=created_or_int_time, onupdate=_int_time)

def created_or_int_time(context):
    return context.current_parameters.get('created') or _int_time()
