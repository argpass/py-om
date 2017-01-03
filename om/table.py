#!coding: utf-8
from collections import OrderedDict
import copy
from threading import RLock
from utils import make_meta_fn as meta_fn
from tracking import convert_to_tracking_class, ZERO_VALUE, get_holder
from exceptions import ImproperlyConfig
from db import Database, Rows

__all__ = ["TableMapper", "Meta", "Column"]

_TABLE_META_CLASS_NAME = "_table_meta_class"
FIELD_TYPE_SET = {int, str}


class EntityInfo(object):
    def __init__(self, entity, field_dict):
        """
        Args:
            entity:
            field_dict(dict): raw field map `{property_name=>raw value}`
        """
        self._entity = entity
        self._field_dict = field_dict

    def new_instance(self):
        """New an instance of the entity
        """
        try:
            instance = self._entity()
        except TypeError:
            # can't all __init__()
            raise ImproperlyConfig(u"entity %s hasn't method __init__() "
                                   u"so i'm not aware how to new "
                                   u"an instance for it",
                                   self._entity)
        return instance

    def field_names(self):
        """
        Returns:
            list
        """
        return self._field_dict.keys()


class EntityMapper(object):
    def __init__(self):
        self._lock = RLock()
        # entity_class => info
        self._entity_map = dict()

    def get_entity_info(self, entity_or_obj):
        """
        Args:
            entity_or_obj(object):
        Returns:
            EntityInfo
        """
        entity = entity_or_obj
        if not hasattr(entity_or_obj, "mro"):
            # may be entity is instance object
            # so change to its class as entity
            entity = entity_or_obj.__class__
        info = self._get_or_create(entity)
        return info

    def _get_or_create(self, entity):
        self._lock.acquire()
        try:
            if entity not in self._entity_map:
                merged_dict = dict()
                for p in reversed(entity.mro()):
                    if p is object:
                        continue
                    # merge fields
                    merged_dict.update({k: v for k, v in p.__dict__.items()
                                        if v in FIELD_TYPE_SET})
                if not merged_dict:
                    raise ImproperlyConfig(u"entity never be empty class, "
                                           u"cls:%s", entity)
                # replace fields with field descriptor
                self._entity_map[entity] = EntityInfo(entity, merged_dict)
            return self._entity_map[entity]
        finally:
            self._lock.release()


entity_mapper = EntityMapper()


class Expr(object):
    def __init__(self, column, op, value):
        """
        Args:
            column(Column):
            op(str):
            value:
        """
        self.column = column
        self.op = op
        self.value = value
        self.next_expr = []

    def __or__(self, other):
        """
        a | b | c
        Args:
            other(Expr):

        Returns:
            Expr
        """
        self.next_expr.append(("OR", other))
        return self

    def __and__(self, other):
        """
        a & b & c
        Args:
            other(Expr):

        Returns:
            Expr
        """
        self.next_expr.append(("AND", other))
        return self

    def __gt__(self, v):
        """
        Args:
            v:

        Returns:
            Expr
        """
        return self and self.column > v

    def __ge__(self, v):
        """
        Args:
            v:

        Returns:
            Expr
        """
        return self and self.column >= v

    def __le__(self, v):
        """
        Args:
            v:

        Returns:
            Expr
        """
        return self and self.column <= v

    def __lt__(self, v):
        """
        Args:
            v:

        Returns:
            Expr
        """
        return self and self.column < v

    def __eq__(self, v):
        """
        Args:
            v:

        Returns:
            Expr
        """
        return self and self.column == v

    def building(self, col_name_fn, args):
        """
        Args:
            col_name_fn(function):
            args(list):

        Returns:
        """
        self_sql = self._generating_sql(col_name_fn, args)
        sql_s = [self_sql]
        for tp, expr in self.next_expr:
            s = expr.building(col_name_fn, args)
            sql_s.append("%s (%s)" % (tp, s))
        return " ".join(sql_s)

    def _generating_sql(self, col_name_fn, args):
        """
        Args:
            args(list):
            col_name_fn(function)
        Returns:
            str
        """
        col_name = col_name_fn(self.column)
        args.append(self.value)
        return "%s %s %%s" % (col_name, self.op)


class BetweenExpr(Expr):
    def _generating_sql(self, col_name_fn, args):
        args.append(self.value[0])
        args.append(self.value[1])
        return " ".join([col_name_fn(self.column),
                         "BETWEEN", "%s", "AND", "%s"])


class NullExpr(Expr):
    def _generating_sql(self, col_name_fn, args):
        col_name = col_name_fn(self.column)
        return " ".join([col_name, "IS NULL"])


class NotNullExpr(Expr):
    def _generating_sql(self, col_name_fn, args):
        col_name = col_name_fn(self.column)
        return " ".join([col_name, "IS NOT NULL"])


def eq_expr(column, value):
    return Expr(column, "=", value)


def gt_expr(column, value):
    return Expr(column, ">", value)


def ge_expr(column, value):
    return Expr(column, ">=", value)


def lt_expr(column, value):
    return Expr(column, "<", value)


def le_expr(column, value):
    return Expr(column, "<=", value)


def ne_expr(column, value):
    return Expr(column, "<>", value)


def like_expr(column, value):
    return Expr(column, "LIKE", value)


def in_expr(column, value):
    return Expr(column, "IN", value)


def between_expr(column, value_a, value_b):
    return BetweenExpr(column, "", (value_a, value_b))


def is_null_expr(column):
    return NullExpr(column, op="", value=None)


def is_not_null_expr(column):
    return NotNullExpr(column, op="", value=None)


class Column(object):
    def __init__(self, db_column=None):
        self.db_column = db_column
        self.table_mapper = None

    def clone_for(self, mapper):
        """
        Args:
            mapper(TableMapper):
        Returns:
            Column
        """
        new = copy.copy(self)
        new.table_mapper = mapper
        return new

    def resolve_meta(self, f_name, table_mapper):
        if self.db_column is None:
            self.db_column = f_name
        self.table_mapper = table_mapper

    def asc(self):
        return self, "ASC"

    def desc(self):
        return self, "DESC"

    def __gt__(self, other):
        """
        Args:
            other: value or other columns
        Returns:
            Expr
        """
        return gt_expr(self, other)

    def __ge__(self, other):
        """
        Args:
            other: value or other columns
        Returns:
            Expr
        """
        return ge_expr(self, other)

    def __eq__(self, other):
        """Equal test
        Notes: col == None means `col is null`
        Args:
            other: value or other columns
        Returns:
            Expr
        """
        if other is None:
            return is_null_expr(self)
        return eq_expr(self, other)

    def __ne__(self, other):
        """Not equal test
        Notes: col != None means `col is not null`
        Args:
            other: value or other columns
        Returns:
            Expr
        """
        if other is None:
            return is_not_null_expr(self)
        return ne_expr(self, other)

    def __le__(self, other):
        """
        Args:
            other: value or other columns
        Returns:
            Expr
        """
        return le_expr(self, other)

    def __lt__(self, other):
        """
        Args:
            other: value or other columns
        Returns:
            Expr
        """
        return lt_expr(self, other)

    def __mul__(self, other):
        """
        Examples:
            col << (1,2,3) => `col in (1,2,3)`
        Args:
            other: value or other columns
        Returns:
            Expr
        """
        return in_expr(self, other)

    def __lshift__(self, other):
        """
        Examples:
            col << (1,2,3) => `col in (1,2,3)`
        Args:
            other: value or other columns
        Returns:
            Expr
        """
        return in_expr(self, other)

    def __rshift__(self, other):
        """
        Examples:
            col >> (1,2) => `col between 1 and 2`
        Returns:
            Expr
        """
        if not isinstance(other, (tuple, list)) or len(other) != 2:
            raise ValueError(u"between expr expects a tuple of length 2")
        return between_expr(self, other[0], other[1])

    def __neg__(self):
        """
        Examples:
            -col  => `col is null`
        Returns:
            Expr
        """
        return is_null_expr(self)

    def __pos__(self):
        """
        Examples:
            +col => `col is not null`
        Returns:
            Expr
        """
        return is_not_null_expr(self)


class Meta(object):
    """Base class for all Metas
    """
    # :builds by `TableMapperType`
    # prop name => Column object
    __cols__ = None
    # all managed entities (include parents') collected here
    __managed_set__ = None
    # all identifiers (include parents') collected here
    # Note: no identifier is allowed (i can use where condition to execute sql)
    __identifier_set__ = None

    # identifiers declares that what fields can be used as an identifier
    identifiers = ()
    # if is_abstract tag is True, the meta class wouldn't be checked
    is_abstract = True
    # db pool object (of course a database spec)
    database = None
    # table name i'm mapping with
    db_table = None
    # all entity classes i manage
    managed = ()

    @classmethod
    def get_column(cls, name):
        """
        Args:
            name(str):
        Returns:
            Column
        """
        return cls.__cols__.get(name)

    @classmethod
    def get_db_column(cls, field_name):
        return cls.get_column(field_name).db_column

    @classmethod
    def get_managed_set(cls):
        return cls.__managed_set__ or set()

    @classmethod
    def get_identifier_set(cls):
        """
        Returns:
            set
        """
        return cls.__identifier_set__ or set()


def check_meta(meta):
    """
    Args:
        meta(Meta):
    """
    required = ("database", "db_table", "managed")
    for k in required:
        if not getattr(meta, k, None):
            raise ImproperlyConfig(u"%s is need to be configured", k)
    return True


class JoinSpec(object):
    LEFT_JOIN = "LEFT JOIN"
    RIGHT_JOIN = "RIGHT JOIN"
    INNER_JOIN = "INNER JOIN"
    JOIN = "JOIN"

    def __init__(self, tb, join_tp, on=None):
        """
        Args:
            tb(TableMapper):
            join_tp(str):
            on(tuple):
        """
        self.tb = tb
        self.join_tp = join_tp
        self.on = on


class TableMapperType(type):
    __slots__ = []

    def __new__(mcs, name, bases, options):
        """
        Args:
            name(str):
            bases(tuple):
            options(dict):
        """
        self_new = super(TableMapperType, mcs).__new__
        if name == _TABLE_META_CLASS_NAME \
                or bases[0].__name__ == _TABLE_META_CLASS_NAME:
            return self_new(mcs, name, bases, options)

        # build Meta
        meta = options.pop("Meta", None)
        parent_meta = getattr(bases[0], "Meta", None)
        all_metas = tuple(m for m in (meta, parent_meta, Meta) if m is not None)
        # build meta class named `{name}_meta`
        meta = type.__new__(type, "%s_meta" % (name,), all_metas, {})
        if not getattr(meta, "is_abstract", False):
            # validate the meta class if not abstract
            check_meta(meta)
        options["Meta"] = meta

        cls = self_new(mcs, name, bases, options)
        cols = dict()
        for f_name, prop in options.items():
            if isinstance(prop, Column):
                prop.resolve_meta(f_name, cls)
                cols[f_name] = prop

        # :build props which need merged with parents'
        meta.__managed_set__ = set(getattr(meta, "managed", None) or ())
        # hack entities, make it suitable for the `TableMapper`
        for entity in meta.__managed_set__:
            mcs.hack_entity(entity)
        # hack managed entities
        meta.__identifier_set__ = set(getattr(meta, "identifiers", None) or ())
        for name in meta.__identifier_set__:
            if not hasattr(cls, name):
                raise ImproperlyConfig(u"the identifier '%s' isn't property "
                                       u"of the table mapper class")
        meta.__cols__ = cols
        # merge parents'
        if parent_meta is not None:
            meta.__managed_set__ |= parent_meta.__managed_set__
            meta.__identifier_set__ |= parent_meta.__identifier_set__
            # clone cols for the cls from parents
            meta.__cols__.update(
                {name: col.clone_for(cls)
                 for name, col in parent_meta.__cols__.items()})
            for name, col in meta.__cols__.items():
                setattr(cls, name, col)

        return cls

    @classmethod
    def hack_entity(mcs, entity):
        # make entity as a dirty tracking class
        # 1.save raw field map of the entity ({property_name=>raw value})
        # 2.replace raw field map to `{property_name=>tracking.Field()}`
        info = entity_mapper.get_entity_info(entity)
        convert_to_tracking_class(entity, info.field_names())


class TableMapper(meta_fn(_TABLE_META_CLASS_NAME)(TableMapperType)):
    """TableMapper is the mapping configuration to a table
    """

    @classmethod
    def _new_context(cls):
        db_spec = cls.get_meta().database
        if db_spec is None:
            raise ImproperlyConfig(u"no database spec configured on %s", cls)
        return ExecContext(cls, db_spec)

    @classmethod
    def clone(cls):
        """Clone an aliased mapper class, used in `join same table` context
        Returns:
            TableMapper
        """
        return TableMapperType.__new__(
            TableMapperType, "%s__alias" % cls.__name__,
            (cls,), {"__is_alias__": True})

    @classmethod
    def left_join(cls, other, on=None):
        """Left join
        Args:
            other(TableMapper):
            on(Expr): join on condition expr
        Returns:
            Context
        """
        return cls._new_context().left_join(other, on)

    @classmethod
    def right_join(cls, other, on=None):
        """Right join
        Args:
            other(TableMapper):
            on(Expr): join on condition expr
        Returns:
            Context
        """
        return cls._new_context().right_join(other, on)

    @classmethod
    def join(cls, other, on=None):
        """inner join
        Args:
            other(TableMapper):
            on(Expr): join on condition expr
        Returns:
            Context
        """
        return cls._new_context().join(other, on)

    @classmethod
    def where(cls, expr):
        """
        Args:
            expr(Expr):
        Returns:
            Context
        """
        return cls._new_context().where(expr)

    @classmethod
    def select(cls, entity_config, *entities_config):
        """
        Args:
            entity_config:
            *entities_config:

        Returns:
            SelectPlan
        """
        return cls._new_context().select(entity_config, *entities_config)

    @classmethod
    def save(cls, obj_or_tuple):
        """
        Args:
            obj_or_tuple:

        Returns:
            UpdatePlan
        """
        return cls._new_context().save(obj_or_tuple)

    @classmethod
    def delete(cls, *instances):
        """
        Args:
            *instances:

        Returns:
            DeletePlan
        """
        return cls._new_context().delete(*instances)

    @classmethod
    def insert(cls, instance, *instances):
        """
        Args:
            instance:
            *instances:

        Returns:
            InsertPlan
        """
        return cls._new_context().insert(instance, *instances)

    @classmethod
    def get_meta(cls):
        """
        Returns:
            Meta
        """
        return cls.Meta


class Context(object):
    def left_join(self, other_table, on=None):
        """
        Args:
            other_table:
            on(Expr):

        Returns:
            ExecContext
        """
        raise NotImplementedError(u"Implement the method by subclass")

    def join(self, other_table, on=None):
        """
        Args:
            other_table:
            on(Expr):

        Returns:
            Context
        """
        raise NotImplementedError(u"Implement the method by subclass")

    def right_join(self, other_table, on=None):
        """
        Args:
            other_table:
            on(Expr):

        Returns:
            Context
        """
        raise NotImplementedError(u"Implement the method by subclass")

    def where(self, expr):
        """
        Args:
            expr(Expr):
        Returns:
            Context
        """
        raise NotImplementedError(u"Implement the method by subclass")

    def select(self, entity, *entities):
        """
        Returns:
            SelectPlan
        """
        raise NotImplementedError(u"Implement the method by subclass")

    def save(self, obj_or_tuple, only_dirty=True):
        """
        Returns:
            UpdatePlan
        """
        raise NotImplementedError(u"Implement the method by subclass")

    def delete(self, *instances):
        """
        Args:
            *instances(object): if empty, delete with where expr
        Returns:
            DeletePlan
        """
        raise NotImplementedError(u"Implement the method by subclass")

    def insert(self, instance, *instances):
        """
        Args:
            instance:
            *instances:

        Returns:
            InsertPlan
        """
        raise NotImplementedError(u"Implement the method by subclass")


class ExecContext(Context):
    """Implement of the `Context`
    """

    def __init__(self, table_mapper, db_spec):
        """
        Args:
            table_mapper(TableMapper):
            db_spec(Database):
        """
        self.db_spec = db_spec
        self.table_mapper = table_mapper
        self.joins = []
        self._lock = RLock()
        # `TableMapper` => alias
        self.mappers = dict()
        # alias => `TableMapper`
        self.alias_map = dict()
        self.where_expr = None

        # register the first one
        self._add_table_mapper(table_mapper)

    def left_join(self, other_table, on=None):
        """
        Args:
            other_table:
            on(Expr):

        Returns:
            ExecContext
        """
        on = (on.column, on.value)
        return self._join_ctx(other_table, on, j_type=JoinSpec.LEFT_JOIN)

    def join(self, other_table, on=None):
        """
        Args:
            other_table:
            on(Expr):

        Returns:
            ExecContext
        """
        on = (on.column, on.value)
        return self._join_ctx(other_table, on, j_type=JoinSpec.INNER_JOIN)

    def right_join(self, other_table, on=None):
        """
        Args:
            other_table:
            on(Expr):

        Returns:
            ExecContext
        """
        on = (on.column, on.value)
        return self._join_ctx(other_table, on, j_type=JoinSpec.RIGHT_JOIN)

    def _join_ctx(self, other_table, on=None, j_type=JoinSpec.INNER_JOIN):
        """
        Args:
            other_table(TableMapper):
            on(tuple):

        Returns:
            ExecContext
        """
        self.joins.append(JoinSpec(other_table,
                                   join_tp=j_type, on=on))
        self._add_table_mapper(other_table)
        return self

    def _add_table_mapper(self, mapper):
        self._lock.acquire()
        try:
            if mapper not in self.mappers:
                n = len(self.mappers) + 1
                alias = "t%s" % n
                self.mappers[mapper] = alias
                self.alias_map[alias] = mapper
        finally:
            self._lock.release()

    def where(self, expr):
        """
        Args:
            expr(Expr):
        Returns:
            ExecContext
        """
        self.where_expr = expr
        return self

    def where_sql(self):
        """
        Returns:
            xx > %s and xx <%s and xx=%s
        """
        if self.where_expr is None:
            return None, []
        col_name_fn = self.get_col_name
        args = []
        sql = self.where_expr.building(col_name_fn, args)
        return sql, args

    def get_col_name(self, column):
        """
        Args:
            column(Column):
        Returns:
            str
        Examples:
            "`t1`.`col_a`"
        """
        alias = self.mappers[column.table_mapper]
        return self.db_spec.format_column(alias, column.db_column)

    def resolve_mapper(self, entity_or_instance):
        """
        Returns:
            TableMapper
        """
        entity = entity_or_instance
        if not hasattr(entity, "mro"):
            # entity_or_instance is an instance, resolve class
            entity = entity_or_instance.__class__
        for mapper, alias in self.mappers.items():
            if entity in mapper.Meta.__managed_set__:
                return mapper
        return None

    def get_joins_sql(self):
        """
        Returns:
            str:

        Examples:
            "t_table as t1 left join t_b as t2 on ...."
        """
        table_name = self.db_spec \
            .format_table_name(self.table_mapper.Meta.db_table)
        tb_alias = self.mappers[self.table_mapper]
        join_s = ["%(table_name)s AS %(tb_alias)s" % dict(
            table_name=table_name,
            tb_alias=self.db_spec.format_table_name(tb_alias))]
        for joinSpec in self.joins:
            tp = joinSpec.join_tp
            tb = joinSpec.tb
            tb_alias = self.mappers[tb]
            table_name = self.db_spec.format_table_name(tb.Meta.db_table)
            col_a, col_b = joinSpec.on
            alias_b = self.mappers[col_b.table_mapper]
            alias_a = self.mappers[col_a.table_mapper]
            col_name_b = self.db_spec.format_column(alias_b,
                                                    col_b.db_column)
            col_name_a = self.db_spec.format_column(alias_a,
                                                    col_a.db_column)
            join_s.append(
                "%(tp)s %(table_name)s AS %(tb_alias)s "
                "ON %(col_name_a)s = %(col_name_b)s" % dict(
                    tp=tp, table_name=table_name, tb_alias=tb_alias,
                    col_name_a=col_name_a, col_name_b=col_name_b))
        return " ".join(join_s)

    def select(self, entity_config, *entities_config):
        """
        Args:
            entity_config:
            *entities_config:

        Returns:
            SelectPlan
        """
        entities_config = (entity_config,) + entities_config
        # ensure entity_map ordered same with entity_config order
        # thus the iter result would be always as expected
        entity_map = OrderedDict()
        for config in entities_config:
            if isinstance(config, (tuple, list)):
                table_mapper, entity = config[0], config[1]
            else:
                entity = config
                table_mapper = self.resolve_mapper(entity)
                if table_mapper is None:
                    raise ImproperlyConfig(u"there is no table "
                                           u"mapper to manage %s", entity)
            entity_map[entity] = table_mapper
        return SelectPlan(self, entity_map)

    def save(self, obj_or_tuple, only_dirty=True):
        """
        Returns:
            UpdatePlan
        """
        if not isinstance(obj_or_tuple, (tuple, list)):
            obj_or_tuple = (obj_or_tuple,)

        return UpdatePlan(self, obj_or_tuple, only_dirty)

    def delete(self, *instances):
        """
        Args:
            *instances: if empty, delete with where expr
        Returns:
            DeletePlan
        """
        return DeletePlan(self, *instances)

    def insert(self, instance, *instances):
        """
        Args:
            instance:
            *instances:

        Returns:
            InsertPlan
        """
        return InsertPlan(self, instance, *instances)


class SelectPlan(object):
    def __init__(self, context, entity_map):
        """
        Args:
            context(ExecContext):
            entity_map(dict)
        """
        self._ctx = context
        self._orders = []
        self._t_limit = None
        self._result_field_pos_idx = None
        self._sql = None
        self._args = None
        # entity class => `TableMapper`
        self._entity_map = entity_map

    def limit(self, begin, end):
        """
        Args:
            begin(int):
            end(int):

        Returns:
            SelectPlan

        """
        self._t_limit = begin, end
        return self

    def order_by(self, *order_pairs):
        """
        Args:
            *order_pairs(tuple): ((Column, "ASC"), (Column, "DESC"),)
        Returns:
            SelectPlan

        """
        for pair in order_pairs:
            self._orders.append(pair)
        return self

    def _order_sql(self):
        if not self._orders:
            return None
        # Column, str("ASC", "DESC")
        sql_s = []
        for column, tp in self._orders:
            col_name = self._ctx.get_col_name(column)
            sql_s.append("%s %s" % (col_name, tp))
        return "ORDER BY %s" % (",".join(sql_s),)

    def _limit_sql(self):
        if not self._t_limit:
            return None
        begin, end = self._t_limit
        return "LIMIT %s,%s" % (begin, end)

    def _get_sql_args(self, rebuild=False):
        if self._sql is not None and not rebuild:
            return self._sql, self._args or []
        field_s = []
        self._result_field_pos_idx = []
        cur_pos = 0
        for entity, table_mapper in self._entity_map.items():
            info = entity_mapper.get_entity_info(entity)
            tb_alias = self._ctx.mappers[table_mapper]
            for f_name in info.field_names():
                col = table_mapper.get_meta().get_column(f_name)
                if col is None:
                    raise ImproperlyConfig(u"%s not defined in "
                                           u"table mapper (%s)",
                                           f_name, table_mapper)
                col_name = self._ctx.db_spec.format_column(tb_alias,
                                                           col.db_column)
                field_s.append(col_name)
                cur_pos += 1
            self._result_field_pos_idx.append((cur_pos, entity))

        select_s = ["SELECT", ",".join(field_s), "FROM",
                    self._ctx.get_joins_sql()]
        where_sql, args = self._ctx.where_sql()
        if where_sql:
            select_s.append("WHERE %s" % (where_sql,))
        order_by = self._order_sql()
        limit = self._limit_sql()
        [select_s.append(s) for s in (order_by, limit) if s is not None]
        self._sql = " ".join(select_s)
        self._args = args
        return self._sql, self._args

    def iter(self):
        """Get a generator to iter the results
        Returns:
            QueryIterator
        """
        sql, args = self._get_sql_args()
        rows = self._ctx.db_spec.iter(sql, *args)
        return QueryIterator(self._result_field_pos_idx, rows)


class QueryIterator(object):
    """QueryIterator is an iterator to scan query results
    """

    def __init__(self, result_field_pos_idx, rows):
        """
        Args:
            result_field_pos_idx(tuple):
            rows(Rows):
        """
        self._result_field_pos_idx = result_field_pos_idx
        self._rows = rows
        self._gen = None

    @property
    def entities_tuple(self):
        """entity class with order in current iterator,
        the order is same with iter result

        Returns:
            tuple:(entity_a, entity_b, ...)
        """
        return tuple([entity for _, entity in self._result_field_pos_idx])

    def __str__(self):
        holders_str = "(%s)" % (
            ",".join(
                [str(entity) for _, entity in self._result_field_pos_idx]),)
        return "<%s(%s,...) at %s>" % (self.__class__.__name__,
                                       holders_str, id(self))

    def __repr__(self):
        return self.__str__()

    def _iter(self):
        for row in self._rows:
            i = 0
            col_names = row.get_col_names()
            data = row.get_data_tuple()
            # [instance_of_entity_a, instance_of_entity_b, ...]
            result = []
            # build a result for current row
            for cur_pos, entity in self._result_field_pos_idx:
                entity_col_names = col_names[i:cur_pos]
                entity_row = data[i:cur_pos]
                it = zip(entity_col_names, entity_row)
                result.append(self._wrap_instance(entity, it))
            if len(result) > 1:
                yield tuple(result)
            elif len(result) == 1:
                yield result[0]

    @classmethod
    def _wrap_instance(cls, entity_class, it):
        info = entity_mapper.get_entity_info(entity_class)
        instance = info.new_instance()
        for name, v in it:
            setattr(instance, name, v)
        # mark as no dirty
        get_holder(instance).reset()
        return instance

    def __iter__(self):
        return self

    def next(self):
        if self._gen is None:
            self._gen = self._iter()
        return next(self._gen)

    def __del__(self):
        return self.close()

    def close(self):
        return self._rows.close()


class UpdatePlan(object):
    def __init__(self, context, instances, only_dirty=True):
        """
        Args:
            context(ExecContext):
            instances(tuple)
        """
        self._ctx = context
        self._instances = instances
        self._affected_cnt = 0
        self._only_dirty = only_dirty

        self._execute()

    def _execute(self):
        if not self._instances:
            raise ImproperlyConfig(u"nothing to be saved")
        args = []
        # build fields sql block
        field_s = []
        where_s = []
        holders = []
        for instance in self._instances:
            holder = get_holder(instance)
            holders.append(holder)
            dirty = holder.dirty_fields_map()
            if not dirty and self._only_dirty:
                continue
            table_mapper = self._ctx.resolve_mapper(instance)
            if table_mapper is None:
                raise ImproperlyConfig(u"can't find table mapper for "
                                       u"instance:%s", instance)
            info = entity_mapper.get_entity_info(instance.__class__)
            id_names = table_mapper.get_meta().get_identifier_set()
            field_names = dirty.keys() if self._only_dirty \
                else info.field_names()
            for f_name in field_names:
                column = table_mapper.get_meta().get_column(f_name)
                col_name = self._ctx.get_col_name(column)
                field_s.append("%s=%%s" % (col_name,))
                if self._only_dirty:
                    v = dirty[f_name]
                else:
                    v = getattr(instance, f_name)
                if v is ZERO_VALUE:
                    raise ValueError(u"got zero value")
                args.append(v)
            # chose an identifier
            # the block must follow the fields block,
            # the order is important, otherwise, args's order would be invalid
            id_can_use = id_names & set(info.field_names())
            if id_can_use:
                id_chosen = id_can_use.pop()
                v = getattr(instance, id_chosen)
                where_s.append("%s=%%s" % (id_chosen,))
                args.append(v)
        # no data to save
        if not field_s:
            return
        # build where sql block
        where_expr = self._ctx.where_expr
        if where_expr:
            # if where expr set, merge it in where sql block
            where_more_s = where_expr.building(self._ctx.get_col_name, args)
            where_s.append(where_more_s)

        if not where_s:
            raise ImproperlyConfig(u"no where expr and "
                                   u"all instances are with on identifiers, "
                                   u"the save action is aborted")

        sql = "UPDATE %s SET %s WHERE %s" % (
            self._ctx.get_joins_sql(), ",".join(field_s), " AND ".join(where_s))
        self._affected_cnt = self._ctx.db_spec.execute_rowcount(sql, *args)

        # change dirty state of instances
        for holder in holders:
            holder.reset()

    @property
    def affected_cnt(self):
        """Get affected rows count
        Returns:
            int
        """
        return self._affected_cnt


class DeletePlan(object):
    def __init__(self, context, *instances):
        """
        Args:
            context(ExecContext):
            *instances:
                instances's class must be the same,
                instances may be empty (delete some rows with where expr)
        """
        self._ctx = context
        self._instances = instances
        self._affected_cnt = None
        self._mapper = context.table_mapper

        self._execute()

    def _execute(self):
        if not self._instances and not self._ctx.where_expr:
            raise ImproperlyConfig(u"delete action need either where expr "
                                   u"or instances issued, action aborted")
        table_mapper = self._mapper
        meta = table_mapper.get_meta()
        in_lst = []
        where_s = []
        if self._instances:
            entity_class = self._instances[0].__class__
            if any([c.__class__ is not entity_class for c in self._instances]):
                raise ImproperlyConfig(u"instances's class must be the same")
            if entity_class not in table_mapper.get_meta().__managed_set__:
                raise ImproperlyConfig(u"%s isn't managed by mapper %s",
                                       entity_class,
                                       table_mapper)
            info = entity_mapper.get_entity_info(entity_class)
            id_set = set(info.field_names()) & meta.get_identifier_set()
            id_name_chosen = None
            if id_set:
                id_name_chosen = id_set.pop()
            if id_name_chosen is None:
                raise ImproperlyConfig(u"%s has no identifier,"
                                       u" i can't delete its instances",
                                       entity_class)
            column = meta.get_column(id_name_chosen)
            col_name = column.db_column
            where_s.append("%s IN %%s" % (self._ctx.db_spec.quote(col_name),))
            for instance in self._instances:
                v = getattr(instance, id_name_chosen)
                in_lst.append(v)

        # build where sql block
        args = []
        where_expr = self._ctx.where_expr
        if in_lst:
            args.append(in_lst)
        if where_expr:
            # if where expr set, merge it in where sql block
            # never use table alias on db_column
            where_more_s = where_expr\
                .building(lambda x: self._ctx.db_spec.quote(x.db_column), args)
            where_s.append(where_more_s)

        if not where_s:
            raise ImproperlyConfig(u"no where expr and "
                                   u"all instances are with no identifier, "
                                   u"the delete action is aborted")

        tb_name = self._ctx.db_spec \
            .format_table_name(meta.db_table)
        sql = "DELETE FROM %s WHERE %s" % (tb_name,
                                           " AND ".join(where_s))
        self._affected_cnt = self._ctx.db_spec.execute_rowcount(sql, *args)

    @property
    def affected_cnt(self):
        """Get affected rows count
        Returns:
            int
        """
        return self._affected_cnt


class InsertPlan(object):
    def __init__(self, context, instance, *instances):
        """
        Args:
            context(ExecContext):
            *instances(tuple)
            instance:
        """
        instances = (instance,) + instances
        self._ctx = context
        self._instances = instances
        self._last_id = None
        self._cnt = len(instances)

        self._execute()

    def _execute(self):
        if not self._instances:
            raise ImproperlyConfig(u"nothing to be inserted")
        entity_class = self._instances[0].__class__
        info = entity_mapper.get_entity_info(entity_class)
        f_names = entity_mapper.get_entity_info(entity_class).field_names()
        mapper = self._ctx.resolve_mapper(entity_class)
        meta = mapper.get_meta()
        tb_name = self._ctx.db_spec \
            .format_table_name(meta.db_table)
        sql_base = "INSERT INTO %s (%s)" % (
            tb_name, ",".join(["%s" % (meta.get_db_column(f_name),)
                               for f_name in f_names]))
        args = []
        field_s = ["VALUES(%s)" % (
            ",".join(["%s" for _ in range(len(f_names))]))]
        for instance in self._instances:
            # check values
            values = dict()
            for f_name in f_names:
                v = getattr(instance, f_name)
                if v is ZERO_VALUE:
                    raise ValueError(u"got zero value")
                values[f_name] = v
            args.append([(values[f_name]) for f_name in f_names])
        sql = "%s %s" % (sql_base, ",".join(field_s))
        self._last_id = self._ctx.db_spec.insertmany(sql, args)

    @property
    def last_id(self):
        """Get the last row id
        Returns:
            int
        """
        return self._last_id

    @property
    def rows_cnt(self):
        """Get rows count inserted
        Returns:
            int
        """
        return self._cnt
