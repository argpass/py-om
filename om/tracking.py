#!coding: utf-8
from utils import make_meta_fn

__all__ = ["DirtyTracking", "Field", "convert_to_tracking_class",
           "TrackingHolder", "TrackingManager"]


class TrackingManager(object):
    """TrackingHolder holds all fields values and dirty states
    """
    def __init__(self, fields):
        self._fields = fields
        # instance's id => holder
        self._holders = dict()

    def __getitem__(self, instance):
        key = id(instance)
        if key not in self._holders:
            self._holders[key] = TrackingHolder(self)
        return self._holders[key]

    @property
    def fields(self):
        return self._fields

    def merge_fields(self, fields):
        """
        Args:
            fields(dict): field_name => field descriptor instance
        """
        self._fields.update(fields)


class TrackingHolder(object):
    def __init__(self, manager):
        """
        Args:
            manager(TrackingManager):
        """
        self._manager = manager
        self._dirty = set()
        self._values_map = dict()

    def reset(self, data_dict=None):
        """Reset holder state
        Args:
            data_dict(dict): field_name => value
        """
        self._dirty.clear()
        if data_dict:
            self._values_map.update(data_dict)

    def update(self, name, value):
        """Update field name, mark it dirty
        Args:
            name(str):
            value:
        """
        if name not in self._manager.fields:
            raise ValueError(u"un managed field %s", name)
        if not self._manager.fields[name].unwatch:
            self._dirty.add(name)
        self._values_map[name] = value

    def dirty_fields_map(self):
        """All dirty fields dict
        Returns:
            dict: field_name => value
        """
        return {k: self._values_map[k] for k in self._dirty}

    def fields_map(self):
        """All fields dict
        Returns:
            dict: field_name => value
        """
        return {k: self._values_map.get(k, ZERO_VALUE)
                for k in self._manager.fields}

    def get(self, f_name):
        """Get field value of f_name
        Args:
            f_name(str):
        """
        return self._values_map.get(f_name, ZERO_VALUE)


def get_holder(instance):
    """
    Returns:
        TrackingHolder
    """
    manager = getattr(instance, "__tracking__", None)
    if manager:
        return manager[instance]


_META_CLASS_ = "m_tracking"
ZERO_VALUE = type("zero", (), {})

with_meta_class = make_meta_fn(_META_CLASS_)


class TrackingBase(type):
    """Metaclass of all tracked objects
    """
    def __new__(mcs, name, bases, options):
        if name == _META_CLASS_ or bases[0].__name__ == _META_CLASS_:
            # temp class
            return super(TrackingBase, mcs).__new__(mcs, name, bases, options)
        fields = dict()
        new_options = dict()
        for f_name, field in options.items():
            new_options[f_name] = field
            if isinstance(field, Field):
                # replace field descriptor as zero value
                field.setup(f_name)
                fields[f_name] = field
        # register fields to the holder
        holder = TrackingManager(fields)
        # merge parents' fields
        for c in bases:
            other_holder = getattr(c, "__tracking__", None)
            if other_holder:
                holder.merge_fields(other_holder.fields)

        new_options["__tracking__"] = holder
        cls = super(TrackingBase, mcs).__new__(mcs, name, bases, new_options)
        return cls


class DirtyTracking(with_meta_class(TrackingBase)):
    """Tracked class base
    """


class Field(object):
    """Field is to define a tracked property of an object
    """
    def __init__(self, alias=None, unwatch=False):
        self.name = None
        self.alias = alias
        self.unwatch = unwatch

    def setup(self, name):
        """Setup the field
        Args:
            name(str):
        """
        self.name = name

    def __set__(self, model, value):
        model.__tracking__[model].update(self.name, value)

    def __get__(self, model, owner):
        if model:
            return model.__tracking__[model].get(self.name)
        else:
            return self


def convert_to_tracking_class(normal_cls, tracking_fields_names):
    """
    Args:
        normal_cls:
        tracking_fields_names(list):
    """
    fields = dict()
    for name in tracking_fields_names:
        # replace field as an `Field` (tracking dirty state)
        f = Field()
        f.setup(name)
        setattr(normal_cls, name, f)
        fields[name] = f
    setattr(normal_cls, "__tracking__", TrackingManager(fields))
    return normal_cls
