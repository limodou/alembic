from sqlalchemy import MetaData, Column, Table, Integer, String, Text, \
    Numeric, CHAR, ForeignKey, DATETIME, \
    TypeDecorator, CheckConstraint, Unicode, Enum,\
    UniqueConstraint, Boolean
from sqlalchemy.types import NULLTYPE, TIMESTAMP
from sqlalchemy.dialects import mysql
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import and_, column, literal_column
from alembic import autogenerate
from alembic.migration import MigrationContext
from unittest import TestCase
from tests import staging_env, sqlite_db, clear_staging_env, eq_, \
        eq_ignore_whitespace, requires_07, db_for_dialect
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
        CheckConstraint('len(description) > 5')
    )
    return m

def _model_three():
    m = MetaData()
    return m

def _model_four():
    m = MetaData()

    Table('parent', m,
        Column('id', Integer, primary_key=True)
    )

    Table('child', m,
        Column('parent_id', Integer, ForeignKey('parent.id')),
    )

    return m



class AutogenTest(object):
    @classmethod
    def _get_bind(cls):
        return sqlite_db()

    @classmethod
    @requires_07
    def setup_class(cls):
        staging_env()
        cls.bind = cls._get_bind()
        cls.m1 = cls._get_db_schema()
        cls.m1.create_all(cls.bind)
        cls.m2 = cls._get_model_schema()

        conn = cls.bind.connect()
        cls.context = context = MigrationContext.configure(
            connection=conn,
            opts={
                'compare_type': True,
                'compare_server_default':True,
                'target_metadata':cls.m2,
                'upgrade_token':"upgrades",
                'downgrade_token':"downgrades",
                'alembic_module_prefix':'op.',
                'sqlalchemy_module_prefix':'sa.',
            }
        )

        connection = context.bind
        cls.autogen_context = {
            'imports':set(),
            'connection':connection,
            'dialect':connection.dialect,
            'context':context
            }

    @classmethod
    def teardown_class(cls):
        cls.m1.drop_all(cls.bind)
        clear_staging_env()


class ImplicitConstraintNoGenTest(AutogenTest, TestCase):

    @classmethod
    def _get_bind(cls):
        return db_for_dialect('mysql') #sqlite_db()

    @classmethod
    def _get_db_schema(cls):
        m = MetaData()

        Table('someothertable', m,
            Column('id', Integer, primary_key=True),
            Column('value', Boolean()),
        )
        return m

    @classmethod
    def _get_model_schema(cls):
        m = MetaData()

        Table('sometable', m,
            Column('id', Integer, primary_key=True),
            Column('value', Boolean()),
        )
        return m


    def test_boolean_gen_upgrade(self):
        template_args = {}
        autogenerate._produce_migration_diffs(self.context,
            template_args, set(),
            include_symbol=lambda name: name == 'sometable')
        eq_(
            re.sub(r"u'", "'", template_args['upgrades']),
            "### commands auto generated by Alembic - please adjust! ###\n"
            "    op.create_table('sometable',\n"
            "    sa.Column('id', sa.Integer(), nullable=False),\n"
            "    sa.Column('value', sa.Boolean(), nullable=True),\n"
            "    sa.PrimaryKeyConstraint('id')\n    )\n"
            "    ### end Alembic commands ###"
        )

    def test_boolean_gen_downgrade(self):
        # on the downgrade side, we are OK for now, as SQLAlchemy
        # does not reflect check constraints yet.

        template_args = {}
        autogenerate._produce_migration_diffs(self.context,
            template_args, set(),
            )
        eq_(
            re.sub(r"u'", "'", template_args['downgrades']),
            "### commands auto generated by Alembic - please adjust! ###\n"
            "    op.create_table('someothertable',\n"
            "    sa.Column('id', mysql.INTEGER(display_width=11), "
                "nullable=False),\n"
            "    sa.Column('value', mysql.TINYINT(display_width=1), "
            "nullable=True),\n"
            "    sa.PrimaryKeyConstraint('id')\n    )\n"
            "    op.drop_table('sometable')\n"
            "    ### end Alembic commands ###"
        )



class AutogenerateDiffTest(AutogenTest, TestCase):
    @classmethod
    def _get_db_schema(cls):
        return _model_one()

    @classmethod
    def _get_model_schema(cls):
        return _model_two()

    def test_diffs(self):
        """test generation of diff rules"""

        metadata = self.m2
        connection = self.context.bind
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
        context = MigrationContext.configure(
            connection = self.bind.connect(),
            opts = {
                'compare_type' : True,
                'compare_server_default' : True,
                'target_metadata' : self.m1,
                'upgrade_token':"upgrades",
                'downgrade_token':"downgrades",
            }
        )
        template_args = {}
        autogenerate._produce_migration_diffs(context, template_args, set())
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
        autogenerate._produce_migration_diffs(self.context, template_args, set())
        eq_(re.sub(r"u'", "'", template_args['upgrades']),
"""### commands auto generated by Alembic - please adjust! ###
    op.create_table('item',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('description', sa.String(length=100), nullable=True),
    sa.Column('order_id', sa.Integer(), nullable=True),
    sa.CheckConstraint('len(description) > 5'),
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
    op.alter_column('user', 'name',
               existing_type=sa.VARCHAR(length=50),
               nullable=True)
    op.alter_column('user', 'a1',
               existing_type=sa.TEXT(),
               server_default=None,
               existing_nullable=True)
    op.add_column('user', sa.Column('pw', sa.VARCHAR(length=50), nullable=True))
    op.alter_column('order', 'amount',
               existing_type=sa.Numeric(precision=10, scale=2),
               type_=sa.NUMERIC(precision=8, scale=2),
               nullable=False,
               existing_server_default='0')
    op.drop_column('order', 'user_id')
    op.drop_column('address', 'street')
    op.create_table('extra',
    sa.Column('x', sa.CHAR(), nullable=True),
    sa.Column('uid', sa.INTEGER(), nullable=True),
    sa.ForeignKeyConstraint(['uid'], ['user.id'], ),
    sa.PrimaryKeyConstraint()
    )
    op.drop_table('item')
    ### end Alembic commands ###""")

    def test_include_symbol(self):
        context = MigrationContext.configure(
            connection=self.bind.connect(),
            opts={
                'compare_type': True,
                'compare_server_default': True,
                'target_metadata': self.m2,
                'include_symbol': lambda name, schema=None:
                                    name in ('address', 'order'),
                'upgrade_token': "upgrades",
                'downgrade_token': "downgrades",
                'alembic_module_prefix': 'op.',
                'sqlalchemy_module_prefix': 'sa.',
            }
        )
        template_args = {}
        autogenerate._produce_migration_diffs(context, template_args, set())
        assert "alter_column('user'" not in template_args['upgrades']
        assert "alter_column('user'" not in template_args['downgrades']
        assert "alter_column('order'" in template_args['upgrades']
        assert "alter_column('order'" in template_args['downgrades']

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

class AutogenerateDiffOrderTest(TestCase):
    @classmethod
    @requires_07
    def setup_class(cls):
        staging_env()
        cls.bind = sqlite_db()
        cls.m3 = _model_three()
        cls.m3.create_all(cls.bind)
        cls.m4 = _model_four()

        cls.empty_context = empty_context = MigrationContext.configure(
            connection = cls.bind.connect(),
            opts = {
                'compare_type':True,
                'compare_server_default':True,
                'target_metadata':cls.m3,
                'upgrade_token':"upgrades",
                'downgrade_token':"downgrades",
                'alembic_module_prefix':'op.',
                'sqlalchemy_module_prefix':'sa.'
            }
        )

        connection = empty_context.bind
        cls.autogen_empty_context = {
            'imports':set(),
            'connection':connection,
            'dialect':connection.dialect,
            'context':empty_context
            }

    @classmethod
    def teardown_class(cls):
        clear_staging_env()

    def test_diffs_order(self):
        """
        Added in order to test that child tables(tables with FKs) are generated
        before their parent tables
        """

        metadata = self.m4
        connection = self.empty_context.bind
        diffs = []

        autogenerate._produce_net_changes(connection, metadata, diffs,
                                          self.autogen_empty_context)

        eq_(diffs[0][0], 'add_table')
        eq_(diffs[0][1].name, "parent")
        eq_(diffs[1][0], 'add_table')
        eq_(diffs[1][1].name, "child")

class AutogenRenderTest(TestCase):
    """test individual directives"""

    @classmethod
    @requires_07
    def setup_class(cls):
        cls.autogen_context = {
            'opts':{
                'sqlalchemy_module_prefix' : 'sa.',
                'alembic_module_prefix' : 'op.',
            },
            'dialect':mysql.dialect()
        }

    def test_render_table_upgrade(self):
        m = MetaData()
        t = Table('test', m,
            Column('id', Integer, primary_key=True),
            Column('name', Unicode(255)),
            Column("address_id", Integer, ForeignKey("address.id")),
            Column("timestamp", DATETIME, server_default="NOW()"),
            Column("amount", Numeric(5, 2)),
            UniqueConstraint("name", name="uq_name"),
            UniqueConstraint("timestamp"),
        )
        eq_ignore_whitespace(
            autogenerate._add_table(t, self.autogen_context),
            "op.create_table('test',"
            "sa.Column('id', sa.Integer(), nullable=False),"
            "sa.Column('name', sa.Unicode(length=255), nullable=True),"
            "sa.Column('address_id', sa.Integer(), nullable=True),"
            "sa.Column('timestamp', sa.DATETIME(), "
                "server_default='NOW()', "
                "nullable=True),"
            "sa.Column('amount', sa.Numeric(precision=5, scale=2), nullable=True),"
            "sa.ForeignKeyConstraint(['address_id'], ['address.id'], ),"
            "sa.PrimaryKeyConstraint('id'),"
            "sa.UniqueConstraint('name', name='uq_name'),"
            "sa.UniqueConstraint('timestamp')"
            ")"
        )

    def test_render_drop_table(self):
        eq_(
            autogenerate._drop_table(Table("sometable", MetaData()),
                        self.autogen_context),
            "op.drop_table('sometable')"
        )

    def test_render_add_column(self):
        eq_(
            autogenerate._add_column(
                    "foo", Column("x", Integer, server_default="5"),
                        self.autogen_context),
            "op.add_column('foo', sa.Column('x', sa.Integer(), "
                "server_default='5', nullable=True))"
        )

    def test_render_drop_column(self):
        eq_(
            autogenerate._drop_column(
                    "foo", Column("x", Integer, server_default="5"),
                        self.autogen_context),

            "op.drop_column('foo', 'x')"
        )

    def test_render_quoted_server_default(self):
        eq_(
            autogenerate._render_server_default(
                "nextval('group_to_perm_group_to_perm_id_seq'::regclass)",
                    self.autogen_context),
            '"nextval(\'group_to_perm_group_to_perm_id_seq\'::regclass)"'
        )

    def test_render_col_with_server_default(self):
        c = Column('updated_at', TIMESTAMP(),
                server_default='TIMEZONE("utc", CURRENT_TIMESTAMP)',
                nullable=False)
        result = autogenerate._render_column(
                    c, self.autogen_context
                )
        eq_(
            result,
            'sa.Column(\'updated_at\', sa.TIMESTAMP(), '
                'server_default=\'TIMEZONE("utc", CURRENT_TIMESTAMP)\', '
                'nullable=False)'
        )

    def test_render_modify_type(self):
        eq_ignore_whitespace(
            autogenerate._modify_col(
                        "sometable", "somecolumn",
                        self.autogen_context,
                        type_=CHAR(10), existing_type=CHAR(20)),
            "op.alter_column('sometable', 'somecolumn', "
                "existing_type=sa.CHAR(length=20), type_=sa.CHAR(length=10))"
        )

    def test_render_modify_nullable(self):
        eq_ignore_whitespace(
            autogenerate._modify_col(
                        "sometable", "somecolumn",
                        self.autogen_context,
                        existing_type=Integer(),
                        nullable=True),
            "op.alter_column('sometable', 'somecolumn', "
            "existing_type=sa.Integer(), nullable=True)"
        )

    def test_render_check_constraint_literal(self):
        eq_ignore_whitespace(
            autogenerate._render_check_constraint(
                CheckConstraint("im a constraint"),
                self.autogen_context
            ),
            "sa.CheckConstraint('im a constraint')"
        )

    def test_render_check_constraint_sqlexpr(self):
        c = column('c')
        five = literal_column('5')
        ten = literal_column('10')
        eq_ignore_whitespace(
            autogenerate._render_check_constraint(
                CheckConstraint(and_(c > five, c < ten)),
                self.autogen_context
            ),
            "sa.CheckConstraint('c > 5 AND c < 10')"
        )

    def test_render_modify_nullable_w_default(self):
        eq_ignore_whitespace(
            autogenerate._modify_col(
                        "sometable", "somecolumn",
                        self.autogen_context,
                        existing_type=Integer(),
                        existing_server_default="5",
                        nullable=True),
            "op.alter_column('sometable', 'somecolumn', "
            "existing_type=sa.Integer(), nullable=True, "
            "existing_server_default='5')"
        )

    def test_render_enum(self):
        eq_ignore_whitespace(
            autogenerate._repr_type(
                        "sa.",
                        Enum("one", "two", "three", name="myenum"),
                        self.autogen_context),
            "sa.Enum('one', 'two', 'three', name='myenum')"
        )
        eq_ignore_whitespace(
            autogenerate._repr_type(
                        "sa.",
                        Enum("one", "two", "three"),
                        self.autogen_context),
            "sa.Enum('one', 'two', 'three')"
        )

# TODO: tests for dialect-specific type rendering + imports
