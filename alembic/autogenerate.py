"""Provide the 'autogenerate' feature which can produce migration operations
automatically."""

from alembic import util
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.util import OrderedSet
from sqlalchemy import schema as sa_schema, types as sqltypes
import re
from coloredlog import ColoredStreamHandler
import logging
log = logging.getLogger(__name__)

###################################################
# public
def compare_metadata(context, metadata):
    """Compare a database schema to that given in a
    :class:`~sqlalchemy.schema.MetaData` instance.

    The database connection is presented in the context
    of a :class:`.MigrationContext` object, which
    provides database connectivity as well as optional
    comparison functions to use for datatypes and
    server defaults - see the "autogenerate" arguments
    at :meth:`.EnvironmentContext.configure`
    for details on these.

    The return format is a list of "diff" directives,
    each representing individual differences::

        from alembic.migration import MigrationContext
        from alembic.autogenerate import compare_metadata
        from sqlalchemy.schema import SchemaItem
        from sqlalchemy.types import TypeEngine
        from sqlalchemy import (create_engine, MetaData, Column,
                Integer, String, Table)
        import pprint

        engine = create_engine("sqlite://")

        engine.execute('''
            create table foo (
                id integer not null primary key,
                old_data varchar,
                x integer
            )''')

        engine.execute('''
            create table bar (
                data varchar
            )''')

        metadata = MetaData()
        Table('foo', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', Integer),
            Column('x', Integer, nullable=False)
        )
        Table('bat', metadata,
            Column('info', String)
        )

        mc = MigrationContext.configure(engine.connect())

        diff = compare_metadata(mc, metadata)
        pprint.pprint(diff, indent=2, width=20)

    Output::

        [ ( 'add_table',
            Table('bat', MetaData(bind=None),
                Column('info', String(), table=<bat>), schema=None)),
          ( 'remove_table',
            Table(u'bar', MetaData(bind=None),
                Column(u'data', VARCHAR(), table=<bar>), schema=None)),
          ( 'add_column',
            None,
            'foo',
            Column('data', Integer(), table=<foo>)),
          ( 'remove_column',
            None,
            'foo',
            Column(u'old_data', VARCHAR(), table=None)),
          [ ( 'modify_nullable',
              None,
              'foo',
              u'x',
              { 'existing_server_default': None,
                'existing_type': INTEGER()},
              True,
              False)]]


    :param context: a :class:`.MigrationContext`
     instance.
    :param metadata: a :class:`~sqlalchemy.schema.MetaData`
     instance.

    """
    autogen_context, connection = _autogen_context(context, None)
    diffs = []
    _produce_net_changes(connection, metadata, diffs, autogen_context)
    return diffs

###################################################
# top level

def _produce_migration_diffs(context, template_args,
                                imports, include_symbol=None,
                                include_schemas=False):
    opts = context.opts
    metadata = opts['target_metadata']
    include_symbol = opts.get('include_symbol', include_symbol)
    include_schemas = opts.get('include_schemas', include_schemas)

    if metadata is None:
        raise util.CommandError(
                "Can't proceed with --autogenerate option; environment "
                "script %s does not provide "
                "a MetaData object to the context." % (
                    context.script.env_py_location
                ))
    autogen_context, connection = _autogen_context(context, imports)

    diffs = []
    remove_tables = template_args['config'].get_main_option("remove_tables")
    #if removate_tables is '1', then will generate drop table statement
    _produce_net_changes(connection, metadata, diffs,
                                autogen_context, include_symbol,
                                include_schemas, remove_tables=='1')
    template_args[opts['upgrade_token']] = \
            _indent(_produce_upgrade_commands(diffs, autogen_context))
    template_args[opts['downgrade_token']] = \
            _indent(_produce_downgrade_commands(diffs, autogen_context))
    template_args['imports'] = "\n".join(sorted(imports))

def _autogen_context(context, imports):
    opts = context.opts
    connection = context.bind
    return {
        'imports': imports,
        'connection': connection,
        'dialect': connection.dialect,
        'context': context,
        'opts': opts
    }, connection

def _indent(text):
    text = "### commands auto generated by Alembic - "\
                    "please adjust! ###\n" + text
    text += "\n### end Alembic commands ###"
    text = re.compile(r'^', re.M).sub("    ", text).strip()
    return text

###################################################
# walk structures

def _produce_net_changes(connection, metadata, diffs, autogen_context,
                            include_symbol=None,
                            include_schemas=False,
                            remove_tables=False):
    inspector = Inspector.from_engine(connection)
    # TODO: not hardcode alembic_version here ?
    conn_table_names = set()
    if include_schemas:
        schemas = set(inspector.get_schema_names())
        # replace default schema name with None
        schemas.discard("information_schema")
        # replace the "default" schema with None
        schemas.add(None)
        schemas.discard(connection.dialect.default_schema_name)
    else:
        schemas = [None]

    for s in schemas:
        tables = set(inspector.get_table_names(schema=s)).\
                difference(['alembic_version'])
        conn_table_names.update(zip([s] * len(tables), tables))

    metadata_table_names = OrderedSet([(table.schema, table.name)
                                for table in metadata.sorted_tables])

    if include_symbol:
        conn_table_names = set((s, name)
                                for s, name in conn_table_names
                                if include_symbol(name, s))
        metadata_table_names = OrderedSet((s, name)
                                for s, name in metadata_table_names
                                if include_symbol(name, s))

    _compare_tables(conn_table_names, metadata_table_names,
                    inspector, metadata, diffs, autogen_context, remove_tables)

def _compare_tables(conn_table_names, metadata_table_names,
                    inspector, metadata, diffs, autogen_context, remove_tables):

    for s, tname in metadata_table_names.difference(conn_table_names):
        name = '%s.%s' % (s, tname) if s else tname
        if metadata.tables[tname].__mapping_only__:
            log.info("{{white|red:Skipped}} added table %r", name)
        else:
            diffs.append(("add_table", metadata.tables[name]))
            log.info("{{white|green:Detected}} added table %r", name)

    removal_metadata = sa_schema.MetaData()
    for s, tname in conn_table_names.difference(metadata_table_names):
        name = '%s.%s' % (s, tname) if s else tname
        exists = name in removal_metadata.tables
        t = sa_schema.Table(tname, removal_metadata, schema=s)
        if not exists:
            inspector.reflecttable(t, None)
        if remove_tables:
            diffs.append(("remove_table", t))
            log.info("{{white|green:Detected}} removed table %r", name)
#        else:
#            log.info("{{white|red:Skipped}} removed table %r", name)

    existing_tables = conn_table_names.intersection(metadata_table_names)

    conn_column_info = dict(
        ((s, tname),
            dict(
                (rec["name"], rec)
                for rec in inspector.get_columns(tname, schema=s)
            )
        )
        for s, tname in existing_tables
    )

    for s, tname in sorted(existing_tables):
        name = '%s.%s' % (s, tname) if s else tname
        _compare_columns(s, tname,
                conn_column_info[(s, tname)],
                metadata.tables[name],
                diffs, autogen_context, inspector)

    # TODO:
    # index add/drop
    # table constraints
    # sequences

###################################################
# element comparison

def _compare_columns(schema, tname, conn_table, metadata_table,
                                diffs, autogen_context, inspector):
    name = '%s.%s' % (schema, tname) if schema else tname
    metadata_cols_by_name = dict((c.name, c) for c in metadata_table.c)
    conn_col_names = set(conn_table)
    metadata_col_names = set(metadata_cols_by_name)

    for cname in metadata_col_names.difference(conn_col_names):
        if not metadata_table.__mapping_only__:
            diffs.append(
                ("add_column", schema, tname, metadata_cols_by_name[cname])
            )
            log.info("{{white|green:Detected}} added column '%s.%s'", name, cname)
        else:
            log.info("{{white|red:Skipped}} added column '%s.%s'", name, cname)

    for cname in conn_col_names.difference(metadata_col_names):
        if not metadata_table.__mapping_only__:
            diffs.append(
                ("remove_column", schema, tname, sa_schema.Column(
                    cname,
                    conn_table[cname]['type'],
                    nullable=conn_table[cname]['nullable'],
                    server_default=conn_table[cname]['default']
                ))
            )
            log.info("{{white|green:Detected}} removed column '%s.%s'", name, cname)
        else:
            log.info("{{white|red:Skipped}} removed column '%s.%s'", name, cname)
        
    for colname in metadata_col_names.intersection(conn_col_names):
        metadata_col = metadata_cols_by_name[colname]
        conn_col = conn_table[colname]
        col_diff = []
        _compare_type(schema, tname, colname,
            conn_col,
            metadata_col,
            col_diff, autogen_context
        )
        _compare_nullable(schema, tname, colname,
            conn_col,
            metadata_col,
            col_diff, autogen_context
        )
        _compare_server_default(schema, tname, colname,
            conn_col,
            metadata_col,
            col_diff, autogen_context
        )
        if col_diff:
            diffs.append(col_diff)
            
    #compare index
    conn_indexes = inspector.get_indexes(tname)
    
    m_indexes = {}
    m_keys = set()
    c_indexes = {}
    c_keys = set()
    
    for i in metadata_table.indexes:
        m_indexes[i.name] = {'name':i.name, 'table':tname, 'unique':i.unique, 'column_names': [y.name for y in i.columns]}
        m_keys.add(i.name)
        
    for i in conn_indexes:
        c_indexes[i['name']] = {'name':i['name'], 'table':tname, 'unique':i['unique'], 'column_names': i['column_names']}
        c_keys.add(i['name'])
    
    diff_add = m_keys - c_keys
    if diff_add:
        for x in diff_add:
            if not metadata_table.__mapping_only__:
                diffs.append(("add_index", m_indexes[x]))
                log.info("{{white|green:Detected}} add index '%s on %s(%s)'" % (x, tname, ','.join(["%r" % y for y in m_indexes[x]['column_names']])))
            else:
                log.info("{{white|red:Skipped}} add index '%s on %s(%s)'" % (x, tname, ','.join(["%r" % y for y in m_indexes[x]['column_names']])))
            
    diff_del = c_keys - m_keys
    if diff_del:
        for x in diff_del:
            if not metadata_table.__mapping_only__:
                diffs.append(("remove_index", c_indexes[x]))
                log.info("{{white|green:Detected}} remove index '%s on %s'" % (x, tname))
            else:
                log.info("{{white|red:Skipped}} remove index '%s on %s'" % (x, tname))
            
    diff_change = m_keys & c_keys
    if diff_change:
        for x in diff_change:
            a = m_indexes[x]
            b = c_indexes[x]
            
            if a != b:
                if not metadata_table.__mapping_only__:
                    diffs.append(("remove_index", b))
                    diffs.append(("add_index", a))
                
                d = ''
                if a['unique'] != b['unique']:
                    d += (' unique=%r' % a['unique']) + ' to ' + ('unique=%r' % b['unique'])
                if a['column_names'] != b['column_names']:
                    d += ' columns %r to %r' % (a['column_names'], b['column_names'])
                if not metadata_table.__mapping_only__:
                    log.info("{{white|green:Detected}} change index '%s on %s changes as: %s'" % (x, tname, d))
                else:
                    log.info("{{white|red:Skipped}} change index '%s on %s changes as: %s'" % (x, tname, d))
                    
def _compare_nullable(schema, tname, cname, conn_col,
                            metadata_col, diffs,
                            autogen_context):
    conn_col_nullable = conn_col['nullable']
    if conn_col_nullable is not metadata_col.nullable:
        if not metadata_col.table.__mapping_only__:
            diffs.append(
                ("modify_nullable", schema, tname, cname,
                    {
                        "existing_type": conn_col['type'],
                        "existing_server_default": conn_col['default'],
                    },
                    conn_col_nullable,
                    metadata_col.nullable),
            )
            log.info("{{white|green:Detected}} %s on column '%s.%s'",
                "NULL" if metadata_col.nullable else "NOT NULL",
                tname,
                cname
            )
        else:
            log.info("{{white|red:Skipped}} %s on column '%s.%s'",
                "NULL" if metadata_col.nullable else "NOT NULL",
                tname,
                cname
            )
            

def _get_type(t):
    
    name = t.__class__.__name__
    r = repr(t)
    if name.upper() == 'VARCHAR':
        r = '%s(length=%d)' % (name, t.length)
    elif name.upper() == 'CHAR':
        r = '%s(length=%d)' % (name, t.length)
    elif name.upper() == 'DECIMAL':
        r = '%s(precision=%d, scale=%d)' % ('Numeric', t.precision, t.scale)
    elif name.upper() == 'PICKLETYPE':
        r = '%s()' % 'BLOB'
    elif name.upper() == 'INTEGER':
        r = '%s()' % 'INTEGER'
    return r
    
def _compare(c1, c2):
    r1 = _get_type(c1)
    r2 = _get_type(c2)
    if r1.upper() == 'BOOLEAN()' or r2.upper() == 'BOOLEAN()':
        return False
    if r1.upper() == 'MEDIUMTEXT()':
        return False
    else:
        return r1.upper() != r2.upper()

def _compare_type(schema, tname, cname, conn_col,
                            metadata_col, diffs,
                            autogen_context):

    conn_type = conn_col['type']
    metadata_type = metadata_col.type
    if conn_type._type_affinity is sqltypes.NullType:
        log.info("Couldn't determine database type "
                    "for column '%s.%s'" % (tname, cname))
        return
    if metadata_type._type_affinity is sqltypes.NullType:
        log.info("Column '%s.%s' has no type within "
                        "the model; can't compare" % (tname, cname))
        return

    #isdiff = autogen_context['context']._compare_type(conn_col, metadata_col)
    isdiff = _compare(conn_col['type'], metadata_col.type)

    if isdiff:
        if not metadata_col.table.__mapping_only__:
            diffs.append(
                ("modify_type", schema, tname, cname,
                        {
                            "existing_nullable": conn_col['nullable'],
                            "existing_server_default": conn_col['default'],
                        },
                        conn_type,
                        metadata_type),
            )
            log.info("{{white|green:Detected}} type change from %r to %r on '%s.%s'",
                conn_type, metadata_type, tname, cname
            )
        else:
            log.info("{{white|red:Skipped}} type change from %r to %r on '%s.%s'",
                conn_type, metadata_type, tname, cname
            )
            
def _compare_server_default(schema, tname, cname, conn_col, metadata_col,
                                diffs, autogen_context):

    metadata_default = metadata_col.server_default
    conn_col_default = conn_col['default']
    if conn_col_default is None and metadata_default is None:
        return False
    rendered_metadata_default = _render_server_default(
                            metadata_default, autogen_context)
    isdiff = autogen_context['context']._compare_server_default(
                        conn_col, metadata_col,
                        rendered_metadata_default
                    )
    if isdiff:
        if not metadata_col.table.__mapping_only__:
            conn_col_default = conn_col['default']
            diffs.append(
                ("modify_default", schema, tname, cname,
                    {
                        "existing_nullable": conn_col['nullable'],
                        "existing_type": conn_col['type'],
                    },
                    conn_col_default,
                    metadata_default),
            )
            log.info("{{white|green:Detected}} server default %s changed on column '%s.%s'",
                rendered_metadata_default,
                tname,
                cname
            )
        else:
            log.info("{{white|red:Skipped}} server default %s changed on column '%s.%s'",
                rendered_metadata_default,
                tname,
                cname
            )
            


###################################################
# produce command structure

def _produce_upgrade_commands(diffs, autogen_context):
    buf = []
    for diff in diffs:
        buf.append(_invoke_command("upgrade", diff, autogen_context))
    if not buf:
        buf = ["pass"]

    return "\n".join(buf)

def _produce_downgrade_commands(diffs, autogen_context):
    buf = []
    for diff in reversed(diffs):
        buf.append(_invoke_command("downgrade", diff, autogen_context))
    if not buf:
        buf = ["pass"]
    return "\n".join(buf)

def _invoke_command(updown, args, autogen_context):
    if isinstance(args, tuple):
        return _invoke_adddrop_command(updown, args, autogen_context)
    else:
        return _invoke_modify_command(updown, args, autogen_context)

def _invoke_adddrop_command(updown, args, autogen_context):
    cmd_type = args[0]
    adddrop, cmd_type = cmd_type.split("_")

    cmd_args = args[1:] + (autogen_context,)

    _commands = {
        "table": (_drop_table, _add_table),
        "column": (_drop_column, _add_column),
        "index": (_drop_index, _add_index),
    }

    cmd_callables = _commands[cmd_type]

    if (
        updown == "upgrade" and adddrop == "add"
    ) or (
        updown == "downgrade" and adddrop == "remove"
    ):
        return cmd_callables[1](*cmd_args)
    else:
        return cmd_callables[0](*cmd_args)

def _invoke_modify_command(updown, args, autogen_context):
    sname, tname, cname = args[0][1:4]
    kw = {}

    _arg_struct = {
        "modify_type": ("existing_type", "type_"),
        "modify_nullable": ("existing_nullable", "nullable"),
        "modify_default": ("existing_server_default", "server_default"),
    }
    for diff in args:
        diff_kw = diff[4]
        for arg in ("existing_type", \
                "existing_nullable", \
                "existing_server_default"):
            if arg in diff_kw:
                kw.setdefault(arg, diff_kw[arg])
        old_kw, new_kw = _arg_struct[diff[0]]
        if updown == "upgrade":
            kw[new_kw] = diff[-1]
            kw[old_kw] = diff[-2]
        else:
            kw[new_kw] = diff[-2]
            kw[old_kw] = diff[-1]

    if "nullable" in kw:
        kw.pop("existing_nullable", None)
    if "server_default" in kw:
        kw.pop("existing_server_default", None)
    return _modify_col(tname, cname, autogen_context, schema=sname, **kw)

###################################################
# render python

def _add_table(table, autogen_context):
    text = "%(prefix)screate_table(%(tablename)r,\n%(args)s" % {
        'tablename': table.name,
        'prefix': _alembic_autogenerate_prefix(autogen_context),
        'args': ',\n'.join(
            [col for col in
                [_render_column(col, autogen_context) for col in table.c]
            if col] +
            sorted([rcons for rcons in
                [_render_constraint(cons, autogen_context) for cons in
                    table.constraints]
                if rcons is not None
            ])
        )
    }
    
    if table.schema:
        text += ",\nschema=%r" % table.schema
    for k in sorted(table.kwargs):
        text += ",\n%s=%r" % (k.replace(" ", "_"), table.kwargs[k])
    text += "\n)"
    
#    print text
#    raise Exception
    return text

def _drop_table(table, autogen_context):
    text = "%(prefix)sdrop_table(%(tname)r" % {
            "prefix": _alembic_autogenerate_prefix(autogen_context),
            "tname": table.name
        }
    if table.schema:
        text += ", schema=%r" % table.schema
    text += ")"
    return text

def _add_index(index, autogen_context):
    #process indexes by limodou 2013/05/09
    text = "op.create_index('%(name)s', '%(table)s', %(columns)s, unique=%(unique)r)" % {
        'name':index['name'],
        'table':index['table'],
        'columns':[str(x) for x in index['column_names']],
        'unique': index['unique']
    }
    return text
    
def _drop_index(index, autogen_context):
    text = "op.drop_index('%s', '%s')" % (index['name'], index['table'])
    return text
    
def _add_column(schema, tname, column, autogen_context):
    text = "%(prefix)sadd_column(%(tname)r, %(column)s" % {
            "prefix": _alembic_autogenerate_prefix(autogen_context),
            "tname": tname,
            "column": _render_column(column, autogen_context)
            }
    if schema:
        text += ", schema=%r" % schema
    text += ")"
    return text

def _drop_column(schema, tname, column, autogen_context):
    text = "%(prefix)sdrop_column(%(tname)r, %(cname)r" % {
            "prefix": _alembic_autogenerate_prefix(autogen_context),
            "tname": tname,
            "cname": column.name
            }
    if schema:
        text += ", schema=%r" % schema
    text += ")"
    return text

def _modify_col(tname, cname,
                autogen_context,
                server_default=False,
                type_=None,
                nullable=None,
                existing_type=None,
                existing_nullable=None,
                existing_server_default=False,
                schema=None):
    sqla_prefix = _sqlalchemy_autogenerate_prefix(autogen_context)
    indent = " " * 11
    text = "%(prefix)salter_column(%(tname)r, %(cname)r" % {
                            'prefix': _alembic_autogenerate_prefix(
                                                autogen_context),
                            'tname': tname,
                            'cname': cname}
    text += ",\n%sexisting_type=%s" % (indent,
                    _repr_type(sqla_prefix, existing_type, autogen_context))
    if server_default is not False:
        rendered = _render_server_default(
                                server_default, autogen_context)
        text += ",\n%sserver_default=%s" % (indent, rendered)

    if type_ is not None:
        text += ",\n%stype_=%s" % (indent,
                        _repr_type(sqla_prefix, type_, autogen_context))
    if nullable is not None:
        text += ",\n%snullable=%r" % (
                        indent, nullable,)
    if existing_nullable is not None:
        text += ",\n%sexisting_nullable=%r" % (
                        indent, existing_nullable)
    if existing_server_default:
        rendered = _render_server_default(
                            existing_server_default,
                            autogen_context)
        text += ",\n%sexisting_server_default=%s" % (
                        indent, rendered)
    if schema:
        text += ",\n%sschema=%r" % (indent, schema)
    text += ")"
    return text

def _sqlalchemy_autogenerate_prefix(autogen_context):
    return autogen_context['opts']['sqlalchemy_module_prefix'] or ''

def _alembic_autogenerate_prefix(autogen_context):
    return autogen_context['opts']['alembic_module_prefix'] or ''


def _user_defined_render(type_, object_, autogen_context):
    if 'opts' in autogen_context and \
            'render_item' in autogen_context['opts']:
        render = autogen_context['opts']['render_item']
        if render:
            rendered = render(type_, object_, autogen_context)
            if rendered is not False:
                return rendered
    return False

def _render_column(column, autogen_context):
    rendered = _user_defined_render("column", column, autogen_context)
    if rendered is not False:
        return rendered

    opts = []
    if column.server_default:
        rendered = _render_server_default(
                            column.server_default, autogen_context
                    )
        if rendered:
            opts.append(("server_default", rendered))

    if not column.autoincrement:
        opts.append(("autoincrement", column.autoincrement))

    if column.nullable is not None:
        opts.append(("nullable", column.nullable))

    # TODO: for non-ascii colname, assign a "key"
    return "%(prefix)sColumn(%(name)r, %(type)s, %(kw)s)" % {
        'prefix': _sqlalchemy_autogenerate_prefix(autogen_context),
        'name': column.name,
        'type': _repr_type(_sqlalchemy_autogenerate_prefix(autogen_context),
                                column.type, autogen_context),
        'kw': ", ".join(["%s=%s" % (kwname, val) for kwname, val in opts])
    }

def _render_server_default(default, autogen_context):
    rendered = _user_defined_render("server_default", default, autogen_context)
    if rendered is not False:
        return rendered

    if isinstance(default, sa_schema.DefaultClause):
        if isinstance(default.arg, basestring):
            default = default.arg
        else:
            default = str(default.arg.compile(
                            dialect=autogen_context['dialect']))
    if isinstance(default, basestring):
        # TODO: this is just a hack to get
        # tests to pass until we figure out
        # WTF sqlite is doing
        default = re.sub(r"^'|'$", "", default)
        return repr(default)
    else:
        return None

def _repr_type(prefix, type_, autogen_context):
    from sqlalchemy.types import PickleType

    def _repr(t):
        if isinstance(t, PickleType):
            return 'PickleType()'
        else:
            return repr(t)

    mod = type(type_).__module__
    imports = autogen_context.get('imports', None)
    if mod.startswith("sqlalchemy.dialects"):
        dname = re.match(r"sqlalchemy\.dialects\.(\w+)", mod).group(1)
        if imports is not None:
            imports.add("from sqlalchemy.dialects import %s" % dname)
        return "%s.%s" % (dname, _repr(type_))
    else:
        return "%s%s" % (prefix, _repr(type_))

def _render_constraint(constraint, autogen_context):
    renderer = _constraint_renderers.get(type(constraint), None)
    if renderer:
        return renderer(constraint, autogen_context)
    else:
        return None

def _render_primary_key(constraint, autogen_context):
    rendered = _user_defined_render("primary_key", constraint, autogen_context)
    if rendered is not False:
        return rendered

    opts = []
    if constraint.name:
        opts.append(("name", repr(constraint.name)))
    return "%(prefix)sPrimaryKeyConstraint(%(args)s)" % {
        "prefix": _sqlalchemy_autogenerate_prefix(autogen_context),
        "args": ", ".join(
            [repr(c.key) for c in constraint.columns] +
            ["%s=%s" % (kwname, val) for kwname, val in opts]
        ),
    }

def _fk_colspec(fk, metadata_schema):
    """Implement a 'safe' version of ForeignKey._get_colspec() that
    never tries to resolve the remote table.

    """
    if metadata_schema is None:
        return fk._get_colspec()
    else:
        # need to render schema breaking up tokens by hand, since the
        # ForeignKeyConstraint here may not actually have a remote
        # Table present
        tokens = fk._colspec.split(".")
        # no schema in the colspec, render it
        if len(tokens) == 2:
            return "%s.%s" % (metadata_schema, fk._colspec)
        else:
            return fk._colspec

def _render_foreign_key(constraint, autogen_context):
    rendered = _user_defined_render("foreign_key", constraint, autogen_context)
    if rendered is not False:
        return rendered

    opts = []
    if constraint.name:
        opts.append(("name", repr(constraint.name)))
    if constraint.onupdate:
        opts.append(("onupdate", repr(constraint.onupdate)))
    if constraint.ondelete:
        opts.append(("ondelete", repr(constraint.ondelete)))
    if constraint.initially:
        opts.append(("initially", repr(constraint.initially)))
    if constraint.deferrable:
        opts.append(("deferrable", repr(constraint.deferrable)))

    apply_metadata_schema = constraint.parent.metadata.schema
    return "%(prefix)sForeignKeyConstraint([%(cols)s], "\
            "[%(refcols)s], %(args)s)" % {
        "prefix": _sqlalchemy_autogenerate_prefix(autogen_context),
        "cols": ", ".join("'%s'" % f.parent.key for f in constraint.elements),
        "refcols": ", ".join(repr(_fk_colspec(f, apply_metadata_schema))
                            for f in constraint.elements),
        "args": ", ".join(
            ["%s=%s" % (kwname, val) for kwname, val in opts]
        ),
    }

def _render_check_constraint(constraint, autogen_context):
    rendered = _user_defined_render("check", constraint, autogen_context)
    if rendered is not False:
        return rendered

    # detect the constraint being part of
    # a parent type which is probably in the Table already.
    # ideally SQLAlchemy would give us more of a first class
    # way to detect this.
    if constraint._create_rule and \
        hasattr(constraint._create_rule, 'target') and \
        isinstance(constraint._create_rule.target,
                sqltypes.TypeEngine):
        return None
    opts = []
    if constraint.name:
        opts.append(("name", repr(constraint.name)))
    return "%(prefix)sCheckConstraint(%(sqltext)r)" % {
            "prefix": _sqlalchemy_autogenerate_prefix(autogen_context),
            "sqltext": str(
                constraint.sqltext.compile(
                    dialect=autogen_context['dialect']
                )
            )
        }

def _render_unique_constraint(constraint, autogen_context):
    rendered = _user_defined_render("unique", constraint, autogen_context)
    if rendered is not False:
        return rendered

    opts = []
    if constraint.name:
        opts.append(("name", "'%s'" % constraint.name))
    return "%(prefix)sUniqueConstraint(%(cols)s%(opts)s)" % {
        'opts': ", " + (", ".join("%s=%s" % (k, v)
                            for k, v in opts)) if opts else "",
        'cols': ",".join(["'%s'" % c.name for c in constraint.columns]),
        "prefix": _sqlalchemy_autogenerate_prefix(autogen_context)
        }
_constraint_renderers = {
    sa_schema.PrimaryKeyConstraint: _render_primary_key,
    sa_schema.ForeignKeyConstraint: _render_foreign_key,
    sa_schema.UniqueConstraint: _render_unique_constraint,
    sa_schema.CheckConstraint: _render_check_constraint
}
