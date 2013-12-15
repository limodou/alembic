from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import schema as sa_schema, types as sqltypes
import logging
from .render import _render_server_default
from sqlalchemy.util import OrderedSet


log = logging.getLogger(__name__)

def _run_filters(object_, name, type_, reflected, compare_to, object_filters):
    for fn in object_filters:
        if not fn(object_, name, type_, reflected, compare_to):
            return False
    else:
        return True

def _compare_tables(conn_table_names, metadata_table_names,
                    object_filters,
                    inspector, metadata, diffs, autogen_context, remove_tables):

    for s, tname in metadata_table_names.difference(conn_table_names):
        name = '%s.%s' % (s, tname) if s else tname
        metadata_table = metadata.tables[sa_schema._get_table_key(tname, s)]
        if _run_filters(metadata_table, tname, "table", False, None, object_filters):
            diffs.append(("add_table", metadata.tables[name]))
            log.info("{{white|green:Detected}} added table %r", name)

    removal_metadata = sa_schema.MetaData()
    for s, tname in conn_table_names.difference(metadata_table_names):
        name = sa_schema._get_table_key(tname, s)
        exists = name in removal_metadata.tables
        t = sa_schema.Table(tname, removal_metadata, schema=s)
        if not exists:
            inspector.reflecttable(t, None)
        if remove_tables:
            if _run_filters(t, tname, "table", True, None, object_filters):
                diffs.append(("remove_table", t))
                log.info("{{white|green:Detected}} removed table %r", name)
    
    existing_tables = conn_table_names.intersection(metadata_table_names)

    existing_metadata = sa_schema.MetaData()
    conn_column_info = {}
    for s, tname in existing_tables:
        name = sa_schema._get_table_key(tname, s)
        exists = name in existing_metadata.tables
        t = sa_schema.Table(tname, existing_metadata, schema=s)
        if not exists:
            inspector.reflecttable(t, None)
        conn_column_info[(s, tname)] = t

    for s, tname in sorted(existing_tables):
        name = '%s.%s' % (s, tname) if s else tname
        metadata_table = metadata.tables[name]
        conn_table = existing_metadata.tables[name]
        if _run_filters(metadata_table, tname, "table", False, conn_table, object_filters):
            _compare_columns(s, tname, object_filters,
                    conn_table,
                    metadata_table,
                    diffs, autogen_context, inspector)
            c_uniques = _compare_uniques(s, tname,
                    object_filters, conn_table, metadata_table,
                    diffs, autogen_context, inspector)
            _compare_indexes(s, tname, object_filters,
                    conn_table,
                    metadata_table,
                    diffs, autogen_context, inspector,
                    c_uniques)

    # TODO:
    # table constraints
    # sequences

def _make_index(params, conn_table):
    return sa_schema.Index(
            params['name'],
            *[conn_table.c[cname] for cname in params['column_names']],
            unique=params['unique']
    )

def _make_unique_constraint(params, conn_table):
    return sa_schema.UniqueConstraint(
            *[conn_table.c[cname] for cname in params['column_names']],
            name=params['name']
    )

def _compare_columns(schema, tname, object_filters, conn_table, metadata_table,
                                diffs, autogen_context, inspector):
    name = '%s.%s' % (schema, tname) if schema else tname
    metadata_cols_by_name = dict((c.name, c) for c in metadata_table.c)
    conn_col_names = dict((c.name, c) for c in conn_table.c)
    metadata_col_names = OrderedSet(sorted(metadata_cols_by_name))

    for cname in metadata_col_names.difference(conn_col_names):
        if _run_filters(metadata_cols_by_name[cname], cname,
                                "column", False, None, object_filters):
            if not metadata_table.__mapping_only__:
                diffs.append(
                    ("add_column", schema, tname, metadata_cols_by_name[cname])
                )
                log.info("{{white|green:Detected}} added column '%s.%s'", name, cname)
            else:
                log.info("{{white|red:Skipped}} added column '%s.%s'", name, cname)

    for cname in set(conn_col_names).difference(metadata_col_names):
        rem_col = sa_schema.Column(
                    cname,
                    conn_table.c[cname].type,
                    nullable=conn_table.c[cname].nullable,
                    server_default=conn_table.c[cname].server_default
                )
        if _run_filters(rem_col, cname,
                                "column", True, None, object_filters):
            if not metadata_table.__mapping_only__:
                diffs.append(
                    ("remove_column", schema, tname, rem_col)
                )
                log.info("{{white|green:Detected}} removed column '%s.%s'", name, cname)
            else:
                log.info("{{white|red:Skipped}} removed column '%s.%s'", name, cname)
 
    for colname in metadata_col_names.intersection(conn_col_names):
        metadata_col = metadata_cols_by_name[colname]
        conn_col = conn_table.c[colname]
        if not _run_filters(
                    metadata_col, colname, "column", False, conn_col, object_filters):
            continue
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


def _compare_uniques(schema, tname, object_filters, conn_table,
            metadata_table, diffs, autogen_context, inspector):

    m_objs = dict(
        (i.name, i) for i in metadata_table.constraints
        if isinstance(i, sa_schema.UniqueConstraint)
        and i.name is not None
    )
    m_keys = set(m_objs.keys())

    if hasattr(inspector, "get_unique_constraints"):
        try:
            conn_uniques = inspector.get_unique_constraints(tname)
        except NotImplementedError:
            return None
        except NoSuchTableError:
            conn_uniques = []
    else:
        return None

    c_objs = dict(
        (i['name'], _make_unique_constraint(i, conn_table))
        for i in conn_uniques
        if i['name'] is not None
    )
    c_keys = set(c_objs)

    for key in m_keys.difference(c_keys):
        meta_constraint = m_objs[key]
        diffs.append(("add_constraint", meta_constraint))
        log.info("Detected added unique constraint '%s' on %s",
            key, ', '.join([
                "'%s'" % y.name for y in meta_constraint.columns
                ])
        )

    for key in c_keys.difference(m_keys):
        diffs.append(("remove_constraint", c_objs[key]))
        log.info("Detected removed unique constraint '%s' on '%s'",
            key, tname
        )

    for key in m_keys.intersection(c_keys):
        meta_constraint = m_objs[key]
        conn_constraint = c_objs[key]
        conn_cols = [col.name for col in conn_constraint.columns]
        meta_cols = [col.name for col in meta_constraint.columns]

        if meta_cols != conn_cols:
            diffs.append(("remove_constraint", conn_constraint))
            diffs.append(("add_constraint", meta_constraint))
            log.info("Detected changed unique constraint '%s' on '%s':%s",
                key, tname, ' columns %r to %r' % (conn_cols, meta_cols)
            )

    # inspector.get_indexes() can conflate indexes and unique
    # constraints when unique constraints are implemented by the database
    # as an index. so we pass uniques to _compare_indexes() for
    # deduplication
    return c_keys

def _compare_indexes(schema, tname, object_filters, conn_table,
            metadata_table, diffs, autogen_context, inspector,
            c_uniques_keys):

    try:
        reflected_indexes = inspector.get_indexes(tname)
    except NoSuchTableError:
        c_objs = {}
    else:
        c_objs = dict(
            (i['name'], _make_index(i, conn_table))
            for i in reflected_indexes
        )

    m_objs = dict((i.name, i) for i in metadata_table.indexes)

    # deduplicate between conn uniques and indexes, because either:
    #   1. a backend reports uniques as indexes, because uniques
    #      are implemented as a type of index.
    #   2. our backend and/or SQLA version does not reflect uniques
    # in either case, we need to avoid comparing a connection index
    # for what we can tell from the metadata is meant as a unique constraint
    if c_uniques_keys is None:
        c_uniques_keys = set(
            i.name for i in metadata_table.constraints \
            if isinstance(i, sa_schema.UniqueConstraint) and i.name is not None
        )

    c_keys = set(c_objs).difference(c_uniques_keys)
    m_keys = set(m_objs).difference(c_uniques_keys)

    for key in m_keys.difference(c_keys):
        meta = m_objs[key]
        if not metadata_table.__mapping_only__:
            diffs.append(("add_index", meta))
            log.info("{{white|green:Detected}} add index '%s' on '%s(%s)'" % (key, tname, ','.join([exp.name for exp in m_objs[key].columns])))
        else:
            log.info("{{white|red:Skipped}} add index '%s' on '%s(%s)'" % (key, tname, ','.join([exp.name for exp in m_objs[key].columns])))

    for key in c_keys.difference(m_keys):
        if not metadata_table.__mapping_only__:
            diffs.append(("remove_index", c_objs[key]))
            log.info("{{white|green:Detected}} remove index '%s' on '%s'" % (key, tname))
        else:
            log.info("{{white|red:Skipped}} remove index '%s' on '%s'" % (key, tname))

    for key in m_keys.intersection(c_keys):
        meta_index = m_objs[key]
        conn_index = c_objs[key]
        # TODO: why don't we just render the DDL here
        # so we can compare the string output fully
#        conn_exps = [exp.name for exp in conn_index.expressions]
#        meta_exps = [exp.name for exp in meta_index.expressions]
        conn_exps = [exp.name for exp in conn_index.columns]
        meta_exps = [exp.name for exp in meta_index.columns]

        if bool(meta_index.unique) != bool(conn_index.unique) \
                or meta_exps != conn_exps:
            if not metadata_table.__mapping_only__:
                diffs.append(("remove_index", conn_index))
                diffs.append(("add_index", meta_index))

            msg = []
            if bool(meta_index.unique) != bool(conn_index.unique):
                msg.append(' unique=%r to unique=%r' % (
                    conn_index.unique, meta_index.unique
                ))
            if meta_exps != conn_exps:
                msg.append(' columns %r to %r' % (
                    conn_exps, meta_exps
                ))
            if not metadata_table.__mapping_only__:
                log.info("{{white|green:Detected}} change index '%s' on '%s' changes as: '%s'" % (key, tname, ', '.join(msg)))
            else:
                log.info("{{white|red:Skipped}} change index '%s' on '%s' changes as: '%s'" % (key, tname, ', '.join(msg)))


def _compare_nullable(schema, tname, cname, conn_col,
                            metadata_col, diffs,
                            autogen_context):
    conn_col_nullable = conn_col.nullable
    if not metadata_col.table.__mapping_only__:
        if conn_col_nullable is not metadata_col.nullable:
            diffs.append(
                ("modify_nullable", schema, tname, cname,
                    {
                        "existing_type": conn_col.type,
                        "existing_server_default": conn_col.server_default,
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

    conn_type = conn_col.type
    metadata_type = metadata_col.type
    if conn_type._type_affinity is sqltypes.NullType:
        log.info("Couldn't determine database type "
                    "for column '%s.%s'", tname, cname)
        return
    if metadata_type._type_affinity is sqltypes.NullType:
        log.info("Column '%s.%s' has no type within "
                        "the model; can't compare", tname, cname)
        return

    #isdiff = autogen_context['context']._compare_type(conn_col, metadata_col)
    isdiff = _compare(conn_type, metadata_type)

    if isdiff:
        if not metadata_col.table.__mapping_only__:
            diffs.append(
                ("modify_type", schema, tname, cname,
                        {
                            "existing_nullable": conn_col.nullable,
                            "existing_server_default": conn_col.server_default,
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
    conn_col_default = conn_col.server_default
    if conn_col_default is None and metadata_default is None:
        return False
    rendered_metadata_default = _render_server_default(
                            metadata_default, autogen_context)
#    rendered_conn_default = conn_col.server_default.arg.text \
#                            if conn_col.server_default else None
#    import pdb; pdb.set_trace()
    rendered_conn_default = _render_server_default(
                            conn_col_default, autogen_context)
    isdiff = autogen_context['context']._compare_server_default(
                        conn_col, metadata_col,
                        rendered_metadata_default,
                        rendered_conn_default
                    )
    if isdiff:
        if not metadata_col.table.__mapping_only__:
            conn_col_default = rendered_conn_default
            diffs.append(
                ("modify_default", schema, tname, cname,
                    {
                        "existing_nullable": conn_col.nullable,
                        "existing_type": conn_col.type,
                    },
                    conn_col_default,
                    metadata_default),
            )
            log.info("{{white|green:Detected}} server default changed from %s to %s on column '%s.%s'",
                rendered_conn_default,
                rendered_metadata_default,
                tname,
                cname
            )
        else:
            log.info("{{white|red:Skipped}} server default %s changed on column '%s.%s'",
                rendered_conn_default,
                rendered_metadata_default,
                tname,
                cname
            )



