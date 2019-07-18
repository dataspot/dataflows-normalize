from dataflows import Flow, load, dump_to_sql
from dataflows.helpers import ResourceMatcher
from dataflows.processors.join import KeyCalc
from kvfile import KVFile


class NormGroup():
    def __init__(self, fields, ref_field_name, index_field_name,
                 db_table=None, existing_rows=None):
        self.fields = fields
        self.ref_field_name = ref_field_name
        self.index_field_name = index_field_name
        self.existing_rows = existing_rows
        self.db_table = db_table

    def __str__(self):
        return '{} -> {}.{} ({})'.format(
            self.ref_field_name,
            self.db_table,
            self.index_field_name,
            ', '.join(self.fields)
        )


class Indexer():

    def __init__(self, resources, group: NormGroup):
        self.key_calc = KeyCalc(group.fields)
        self.keys = set()
        self.kv = KVFile()
        self.max = 0
        if group.existing_rows:
            for row in group.existing_rows:
                key = self.key_calc(row)
                self.kv.set(key, row)
                self.keys.add(key)
                self.max = max(self.max, row[group.index_field_name] + 1)

        self.resources = resources
        self.group = group

        self.resource_name = None
        self.saved_schema = None
        self.saved_pk = None

    def index(self):

        def process(resource):
            for row in resource:
                index = None
                key = self.key_calc(row)
                if key not in self.keys:
                    subrow = dict(
                        (k, row.get(k))
                        for k in self.group.fields
                    )
                    subrow[self.group.index_field_name] = self.max
                    self.kv.set(key, subrow)
                    index = self.max
                    self.max += 1
                    self.keys.add(key)
                else:
                    index = self.kv.get(key)[self.group.index_field_name]
                row = dict(
                    (k, v)
                    for k, v in row.items()
                    if k not in self.group.fields
                )
                row[self.group.ref_field_name] = index
                yield row

        def func(package):
            descriptor = package.pkg.descriptor
            matcher = ResourceMatcher(self.resources, package.pkg)
            resource = list(
                filter(lambda r: matcher.match(r['name']),
                    descriptor['resources'])
            )
            assert len(resource) == 1, 'Failed to find resource single %s in package' % self.resources
            resource = resource[0]
            self.resource_name = resource['name']
            self.saved_schema = [
                f for f in resource['schema']['fields']
                if f['name'] in self.group.fields
            ]
            self.saved_pk = [self.group.index_field_name] + [
                f for f in resource['schema'].get('primaryKey', [])
                if f in self.group.fields
            ]
            resource['schema']['fields'] = [
                f for f in resource['schema']['fields']
                if f['name'] not in self.group.fields
            ] + [
                dict(
                    name=self.group.ref_field_name,
                    type='integer'
                )
            ]
            resource['schema']['primaryKey'] = [
                f for f in resource['schema'].get('primaryKey', [])
                if f not in self.group.fields
            ] + [self.group.ref_field_name]
            yield package.pkg
            for resource in package:
                if resource.res.name == self.resource_name:
                    yield process(resource)
                else:
                    yield resource

        return func

    def emit(self):
        def func(package):
            resource_name = '{}_{}'.format(self.resource_name, self.group.ref_field_name)
            package.pkg.descriptor['resources'].append(
                dict(
                    name=resource_name,
                    path='%s.csv' % resource_name,
                    schema=dict(
                        fields=[
                            dict(
                                name=self.group.index_field_name,
                                type='integer'
                            )
                        ] + self.saved_schema,
                        primaryKey=self.saved_pk
                    )
                )
            )
            yield package.pkg
            yield from package
            yield (row for _, row in self.kv.items())

        return func


def normalize(groups, resource=None):

    indexers = [
        Indexer(resource, group)
        for group in groups
    ]

    return Flow(
        *(
            i.index()
            for i in indexers
        ),
        *(
            i.emit()
            for i in indexers
        )
    )


def normalize_to_db(groups, db_table,
                    resource_name,
                    db_connection_str='env://DATAFLOWS_DB_ENGINE',
                    fact_table_mode='update'):

    for group in groups:
        group: NormGroup
        if group.db_table is None:
            group.db_table = '{}_{}'.format(db_table, group.ref_field_name)
        try:
            existing_rows = Flow(
                load(db_connection_str, table=group.db_table)
            ).results()[0][0]
        except Exception:
            existing_rows = []
        group.existing_rows = existing_rows

    return Flow(
        normalize(groups, resource=resource_name),
        dump_to_sql(dict(
            [(db_table,
                {'resource-name': resource_name,
                 'mode': fact_table_mode})] +
            [(group.db_table,
                {'resource-name': '{}_{}'.format(resource_name, group.ref_field_name),
                 'mode': 'update'})
             for group in groups]
        ), engine=db_connection_str)
    )
