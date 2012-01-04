from sqlalchemy import MetaData, Column, Table, Integer, String, Text, \
    Numeric, CHAR, ForeignKey, DATETIME, TypeDecorator
from sqlalchemy.types import NULLTYPE
from sqlalchemy.engine.reflection import Inspector
from alembic import autogenerate, context
from unittest import TestCase
from tests import staging_env, sqlite_db, clear_staging_env, eq_, \
        eq_ignore_whitespace, requires_07
import re
import sys
py3k = sys.version_info >= (3, )

def _model_one():
    m = MetaData()

    Table('user', m,
        Column('id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('a1', Text),
        Column("pw", String(50))
    )

    Table('address', m,
        Column('id', Integer, primary_key=True),
        Column('email_address', String(100), nullable=False),
    )

    Table('order', m,
        Column('order_id', Integer, primary_key=True),
        Column("amount", Numeric(8, 2), nullable=False, 
                server_default="0"),
    )

    Table('extra', m,
        Column("x", CHAR),
        Column('uid', Integer, ForeignKey('user.id'))
    )

    return m

def _model_two():
    m = MetaData()

    Table('user', m,
        Column('id', Integer, primary_key=True),
        Column('name', String(50), nullable=False),
        Column('a1', Text, server_default="x"),
    )

    Table('address', m,
        Column('id', Integer, primary_key=True),
        Column('email_address', String(100), nullable=False),
        Column('street', String(50))
    )

    Table('order', m,
        Column('order_id', Integer, primary_key=True),
        Column("amount", Numeric(10, 2), nullable=True, 
                    server_default="0"),
        Column('user_id', Integer, ForeignKey('user.id')),
    )

    Table('item', m, 
        Column('id', Integer, primary_key=True),
        Column('description', String(100)),
        Column('order_id', Integer, ForeignKey('order.order_id')),
    )
    return m

class AutogenerateDiffTest(TestCase):
    @classmethod
    @requires_07
    def setup_class(cls):
        staging_env()
        cls.bind = sqlite_db()
        cls.m1 = _model_one()
        cls.m1.create_all(cls.bind)
        cls.m2 = _model_two()
        context.configure(
            connection = cls.bind.connect(),
            compare_type = True,
            compare_server_default = True,
            target_metadata=cls.m2
        )
        connection = context.get_bind()
        cls.autogen_context = {
            'imports':set(),
            'connection':connection,
            'dialect':connection.dialect,
            'context':context.get_context()
            }

    @classmethod
    def teardown_class(cls):
        clear_staging_env()

    def test_diffs(self):
        """test generation of diff rules"""

        metadata = self.m2
        connection = context.get_bind()
        diffs = []
        autogenerate._produce_net_changes(connection, metadata, diffs, 
                                        self.autogen_context)

        eq_(
            diffs[0],
            ('add_table', metadata.tables['item'])
        )

        eq_(diffs[1][0], 'remove_table')
        eq_(diffs[1][1].name, "extra")

        eq_(diffs[2][0], "add_column")
        eq_(diffs[2][1], "address")
        eq_(diffs[2][2], metadata.tables['address'].c.street)

        eq_(diffs[3][0], "add_column")
        eq_(diffs[3][1], "order")
        eq_(diffs[3][2], metadata.tables['order'].c.user_id)

        eq_(diffs[4][0][0], "modify_type")
        eq_(diffs[4][0][1], "order")
        eq_(diffs[4][0][2], "amount")
        eq_(repr(diffs[4][0][4]), "NUMERIC(precision=8, scale=2)")
        eq_(repr(diffs[4][0][5]), "Numeric(precision=10, scale=2)")


        eq_(diffs[5][0], 'remove_column')
        eq_(diffs[5][2].name, 'pw')

        eq_(diffs[6][0][0], "modify_default")
        eq_(diffs[6][0][1], "user")
        eq_(diffs[6][0][2], "a1")
        eq_(diffs[6][0][5].arg, "x")

        eq_(diffs[7][0][0], 'modify_nullable')
        eq_(diffs[7][0][4], True)
        eq_(diffs[7][0][5], False)


    def test_render_nothing(self):
        context.configure(
            connection = self.bind.connect(),
            compare_type = True,
            compare_server_default = True,
            target_metadata=self.m1
        )
        template_args = {}
        autogenerate.produce_migration_diffs(template_args, self.autogen_context)
        eq_(re.sub(r"u'", "'", template_args['upgrades']),
"""### commands auto generated by Alembic - please adjust! ###
    pass
    ### end Alembic commands ###""")
        eq_(re.sub(r"u'", "'", template_args['downgrades']),
"""### commands auto generated by Alembic - please adjust! ###
    pass
    ### end Alembic commands ###""")

    def test_render_diffs(self):
        """test a full render including indentation"""

        metadata = self.m2
        template_args = {}
        autogenerate.produce_migration_diffs(template_args, self.autogen_context)
        eq_(re.sub(r"u'", "'", template_args['upgrades']),
"""### commands auto generated by Alembic - please adjust! ###
    op.create_table('item',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('description', sa.String(length=100), nullable=True),
    sa.Column('order_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['order_id'], ['order.order_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.drop_table('extra')
    op.add_column('address', sa.Column('street', sa.String(length=50), nullable=True))
    op.add_column('order', sa.Column('user_id', sa.Integer(), nullable=True))
    op.alter_column('order', 'amount', 
               existing_type=sa.NUMERIC(precision=8, scale=2), 
               type_=sa.Numeric(precision=10, scale=2), 
               nullable=True, 
               existing_server_default='0')
    op.drop_column('user', 'pw')
    op.alter_column('user', 'a1', 
               existing_type=sa.TEXT(), 
               server_default='x', 
               existing_nullable=True)
    op.alter_column('user', 'name', 
               existing_type=sa.VARCHAR(length=50), 
               nullable=False)
    ### end Alembic commands ###""")
        eq_(re.sub(r"u'", "'", template_args['downgrades']),
"""### commands auto generated by Alembic - please adjust! ###
    op.drop_table('item')
    op.create_table('extra',
    sa.Column('x', sa.CHAR(), nullable=True),
    sa.Column('uid', sa.INTEGER(), nullable=True),
    sa.ForeignKeyConstraint(['uid'], ['user.id'], ),
    sa.PrimaryKeyConstraint()
    )
    op.drop_column('address', 'street')
    op.drop_column('order', 'user_id')
    op.alter_column('order', 'amount', 
               existing_type=sa.Numeric(precision=10, scale=2), 
               type_=sa.NUMERIC(precision=8, scale=2), 
               nullable=False, 
               existing_server_default='0')
    op.add_column('user', sa.Column('pw', sa.VARCHAR(length=50), nullable=True))
    op.alter_column('user', 'a1', 
               existing_type=sa.TEXT(), 
               server_default=None, 
               existing_nullable=True)
    op.alter_column('user', 'name', 
               existing_type=sa.VARCHAR(length=50), 
               nullable=True)
    ### end Alembic commands ###""")

    def test_skip_null_type_comparison_reflected(self):
        diff = []
        autogenerate._compare_type("sometable", "somecol",
            {"name":"somecol", "type":NULLTYPE, 
            "nullable":True, "default":None},
            Column("somecol", Integer()),
            diff, self.autogen_context
        )
        assert not diff

    def test_skip_null_type_comparison_local(self):
        diff = []
        autogenerate._compare_type("sometable", "somecol",
            {"name":"somecol", "type":Integer(), 
            "nullable":True, "default":None},
            Column("somecol", NULLTYPE),
            diff, self.autogen_context
        )
        assert not diff

    def test_affinity_typedec(self):
        class MyType(TypeDecorator):
            impl = CHAR

            def load_dialect_impl(self, dialect):
                if dialect.name == 'sqlite':
                    return dialect.type_descriptor(Integer())
                else:
                    return dialect.type_descriptor(CHAR(32))

        diff = []
        autogenerate._compare_type("sometable", "somecol",
            {"name":"somecol", "type":Integer(), 
            "nullable":True, "default":None},
            Column("somecol", MyType()),
            diff, self.autogen_context
        )
        assert not diff

    def test_dont_barf_on_already_reflected(self):
        diffs = []
        from sqlalchemy.util import OrderedSet
        inspector = Inspector.from_engine(self.bind)
        autogenerate._compare_tables(
            OrderedSet(['extra', 'user']), OrderedSet(), inspector, 
                MetaData(), diffs, self.autogen_context
        )
        eq_(
            [(rec[0], rec[1].name) for rec in diffs],
            [('remove_table', 'extra'), ('remove_table', u'user')]
        )

class AutogenRenderTest(TestCase):
    """test individual directives"""

    @classmethod
    @requires_07
    def setup_class(cls):
        context._context_opts['sqlalchemy_module_prefix'] = 'sa.'
        context._context_opts['alembic_module_prefix'] = 'op.'

    def test_render_table_upgrade(self):
        m = MetaData()
        t = Table('test', m,
            Column('id', Integer, primary_key=True),
            Column("address_id", Integer, ForeignKey("address.id")),
            Column("timestamp", DATETIME, server_default="NOW()"),
            Column("amount", Numeric(5, 2)),
        )
        eq_ignore_whitespace(
            autogenerate._add_table(t, {}),
            "op.create_table('test',"
            "sa.Column('id', sa.Integer(), nullable=False),"
            "sa.Column('address_id', sa.Integer(), nullable=True),"
            "sa.Column('timestamp', sa.DATETIME(), "
                "server_default='NOW()', "
                "nullable=True),"
            "sa.Column('amount', sa.Numeric(precision=5, scale=2), nullable=True),"
            "sa.ForeignKeyConstraint(['address_id'], ['address.id'], ),"
            "sa.PrimaryKeyConstraint('id')"
            ")"
        )

    def test_render_drop_table(self):
        eq_(
            autogenerate._drop_table(Table("sometable", MetaData()), {}),
            "op.drop_table('sometable')"
        )

    def test_render_add_column(self):
        eq_(
            autogenerate._add_column(
                    "foo", Column("x", Integer, server_default="5"), {}),
            "op.add_column('foo', sa.Column('x', sa.Integer(), "
                "server_default='5', nullable=True))"
        )

    def test_render_drop_column(self):
        eq_(
            autogenerate._drop_column(
                    "foo", Column("x", Integer, server_default="5"), {}),

            "op.drop_column('foo', 'x')"
        )

    def test_render_modify_type(self):
        eq_ignore_whitespace(
            autogenerate._modify_col(
                        "sometable", "somecolumn", 
                        {},
                        type_=CHAR(10), existing_type=CHAR(20)),
            "op.alter_column('sometable', 'somecolumn', "
                "existing_type=sa.CHAR(length=20), type_=sa.CHAR(length=10))"
        )

    def test_render_modify_nullable(self):
        eq_ignore_whitespace(
            autogenerate._modify_col(
                        "sometable", "somecolumn", 
                        {},
                        existing_type=Integer(),
                        nullable=True),
            "op.alter_column('sometable', 'somecolumn', "
            "existing_type=sa.Integer(), nullable=True)"
        )

    def test_render_modify_nullable_w_default(self):
        eq_ignore_whitespace(
            autogenerate._modify_col(
                        "sometable", "somecolumn", 
                        {},
                        existing_type=Integer(),
                        existing_server_default="5",
                        nullable=True),
            "op.alter_column('sometable', 'somecolumn', "
            "existing_type=sa.Integer(), nullable=True, "
            "existing_server_default='5')"
        )

# TODO: tests for dialect-specific type rendering + imports
