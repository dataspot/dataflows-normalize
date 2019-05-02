import itertools
import copy

import dataflows as DF
from dataflows_normalize import normalize, normalize_to_db, NormGroup


def prepare(base, headers, value_field):
    base = list(base)
    assert len(base) == len(headers)
    data = itertools.product(*[
        range(x, x+3)
        for x in base
    ])
    data = (dict(zip(headers, x)) for x in data)
    data = (dict(**x, **{value_field: i}) for i, x in enumerate(data))
    return list(data)

headers = ['a1', 'a2', 'b1', 'b2', 'c1', 'c2']
base = list(range(10, 70, 10))
data = prepare(base, headers, 'value')
a_s = prepare(base[:2], headers[:2], 'id')
b_s = prepare(base[2:4], headers[2:4], 'id')
c_s = prepare(base[4:6], headers[4:6], 'id')

groups = [
    NormGroup(['a1', 'a2'], 'a_ref', 'id'),
    NormGroup(['b1', 'b2'], 'b_ref', 'id'),
    NormGroup(['c1', 'c2'], 'c_ref', 'id'),
]
primary_key = ['a1', 'b1', 'c1']


def test_simple():
    data = itertools.product(*[
        range(x, x+3)
        for x in range(10, 70, 10)
    ])
    headers = ['a1', 'a2', 'b1', 'b2', 'c1', 'c2']
    data = (dict(zip(headers, x)) for x in data)
    data = (dict(**x, value=i) for i, x in enumerate(data))
    res, *_ = DF.Flow(
        data,
        normalize(groups),
    ).results()
    assert res[1] == a_s
    assert res[2] == b_s
    assert res[3] == c_s


def test_multiple_resources():
    res, *_ = DF.Flow(
        [{'x': 1}],
        data,
        normalize(groups, resource=-1),
    ).results()
    assert res[2] == a_s
    assert res[3] == b_s
    assert res[4] == c_s


def test_dump_to_sql():
    res, *_ = DF.Flow(
        data,
        DF.update_resource(-1, name='dfn_test'),
        normalize(groups, resource=-1),
        DF.dump_to_sql(dict(
            (x, {'resource-name': x})
            for x in ['dfn_test', 'dfn_test_a_ref', 'dfn_test_b_ref', 'dfn_test_c_ref']
        ))
    ).results()
    assert res[1] == a_s
    assert res[2] == b_s
    assert res[3] == c_s


def test_with_existing_rows():
    existing_as = [copy.deepcopy(x) for x in a_s[3:6]]
    for x in existing_as:
        x['id'] += 100
    groups = [
        NormGroup(['a1', 'a2'], 'a_ref', 'id', existing_rows=existing_as),
        NormGroup(['b1', 'b2'], 'b_ref', 'id'),
        NormGroup(['c1', 'c2'], 'c_ref', 'id'),
    ]
    res, *_ = DF.Flow(
        data,
        DF.update_resource(-1, name='dfn_test'),
        normalize(groups, resource=-1),
    ).results()
    assert res[1] == [
        {'a1': 10, 'a2': 20, 'id': 106},
        {'a1': 10, 'a2': 21, 'id': 107},
        {'a1': 10, 'a2': 22, 'id': 108},
        {'a1': 11, 'a2': 20, 'id': 103},
        {'a1': 11, 'a2': 21, 'id': 104},
        {'a1': 11, 'a2': 22, 'id': 105},
        {'a1': 12, 'a2': 20, 'id': 109},
        {'a1': 12, 'a2': 21, 'id': 110},
        {'a1': 12, 'a2': 22, 'id': 111}
    ]
    assert res[2] == b_s
    assert res[3] == c_s


def test_with_existing_rows_in_db():
    DF.Flow(
        data[:100],
        DF.update_resource(-1, name='dfn2_test'),
        normalize(groups, resource=-1),
        DF.dump_to_sql(dict(
            (x, {'resource-name': x})
            for x in ['dfn2_test', 'dfn2_test_a_ref', 'dfn2_test_b_ref', 'dfn2_test_c_ref']
        ))
    ).results()

    new_groups = [
        NormGroup(['{}{}'.format(x, i) for i in range(1, 3)],
                  '{}_ref'.format(x),
                  'id',
                  existing_rows=DF.Flow(DF.load('env://DATAFLOWS_DB_ENGINE', table='dfn2_test_{}_ref'.format(x))).results()[0][0]
                  )
        for x in ['a', 'b', 'c']
    ]

    res, *_ = DF.Flow(
        data[100:],
        DF.update_resource(-1, name='dfn2_test'),
        normalize(new_groups, resource=-1),
        DF.dump_to_sql(dict(
            (x, {'resource-name': x, 'mode': 'update'})
            for x in ['dfn2_test', 'dfn2_test_a_ref', 'dfn2_test_b_ref', 'dfn2_test_c_ref']
        ))
    ).results()

    assert res[1] == a_s
    assert res[2] == b_s
    assert res[3] == c_s


def test_normalize_db():
    res, *_ = DF.Flow(
        data[:100],
        DF.update_resource(-1, name='dfn3_test'),
        normalize_to_db(groups, 'dfn3_test', 'dfn3_test'),
    ).results()

    res, *_ = DF.Flow(
        data[100:],
        DF.update_resource(-1, name='dfn3_test'),
        normalize_to_db(groups, 'dfn3_test', 'dfn3_test'),
    ).results()

    assert res[1] == a_s
    assert res[2] == b_s
    assert res[3] == c_s
