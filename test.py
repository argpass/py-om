#!coding: utf-8
from unittest import TestCase
from om.db.backends import mysql
from om.table import TableMapper, Column, Meta
from om.tracking import get_holder

spec = mysql.MySQLSpec("vagrant", "test", "root", "akun@123")
pool = mysql.ConnectionPool(spec, driver=mysql.MySQLDriver())
db = mysql.MySQLDatabase(3600 * 72, pool)


class Base(TableMapper):
    id = Column(db_column="id")

    class Meta(Meta):
        is_abstract = True


class Company(object):
    id = int
    name = str


class Companies(Base):
    name = Column(db_column="name")

    class Meta(Meta):
        identifiers = ("id",)
        db_table = "author"
        database = db
        managed = (Company,)


class AuthorBook(object):
    author_id = int
    book_id = int


class AuthorBooks(TableMapper):
    author_id = Column(db_column="aid")
    book_id = Column(db_column="bid")

    class Meta(Meta):
        identifiers = ("author_id", "book_id")
        managed = (AuthorBook,)
        db_table = "t_author_book"
        database = db


class Entity(object):
    id = int


class Author(Entity):
    name = str


class Book(Entity):
    name = str


class Authors(Base):
    name = Column(db_column="name")

    class Meta(Meta):
        identifiers = ("id",)
        db_table = "author"
        database = db
        managed = (Author,)


class Books(Base):
    name = Column(db_column="name")

    class Meta(Meta):
        identifiers = ("id",)
        db_table = "t_book"
        database = db
        managed = (Book,)


class TestOm(TestCase):
    def setUp(self):
        Books.where(Books.id > 0).delete()
        AuthorBooks.where(AuthorBooks.author_id > 0).delete()
        Authors.where(Authors.id > 0).delete()
        Companies.where(Companies.id > 0).delete()

    def test_insert(self):
        book = Book()
        try:
            Books.insert(book)
        except ValueError:
            pass
        else:
            self.fail(u"expect zero value error")
        book.id = 9
        book.name = "python"
        book2 = Book()
        book2.id = 2
        book2.name = "golang"
        self.assertTrue(Books.insert(book).last_id == 9)
        self.assertTrue(Books.insert(book2).last_id == 2)

    def test_update(self):
        book = Book()
        book.id = 3
        book.name = "akun"
        self.assertTrue(
            Books.where(Books.name == "no exists")\
            .save(book).affected_cnt == 0)
        holder = get_holder(book)
        self.assertTrue(not holder.dirty_fields_map(),
                        u"fail to clear dirty state")

    def test_query(self):
        book = Book()
        book.id = 1
        book.name = "Python"
        Books.insert(book)
        books = list(Books.where(Books.id == 1).select(Book).iter())
        self.assertTrue(books[0].id == book.id, u"fail to query the book")
        book = books[0]
        holder = get_holder(book)
        self.assertTrue(not holder.dirty_fields_map(), u"invalid dirty state")
        # update name
        book.name = "Golang"
        Books.save(book)

        # query result will be updated
        books2 = list(Books.where(Books.id == 1).select(Book).iter())
        book2 = books2[0]
        self.assertTrue(book2.name == "Golang", u"update fail, "
                                                u"name:%s" % book2.name)
        self.assertTrue(not get_holder(book2).dirty_fields_map(),
                        u"invalid dirty state")

    def test_join_query(self):
        book = Book()
        book.name = "perl"
        book.id = 1
        author = Author()
        author.id = 1
        author.name = "akun"
        author_book = AuthorBook()
        author_book.author_id = author.id
        author_book.book_id = book.id
        AuthorBooks.insert(author_book)
        Authors.insert(author)
        Books.insert(book)

        it = Books.left_join(AuthorBooks, (Books.id == AuthorBooks.book_id))\
            .left_join(Authors, (Authors.id == AuthorBooks.author_id))\
            .where(Books.id == 999)\
            .select(Book, Author).limit(0, 2)\
            .order_by(Authors.name.desc()).iter()
        self.assertTrue(not list(it), u"expect empty list")
        it = Books.left_join(AuthorBooks, (Books.id == AuthorBooks.book_id)) \
            .left_join(Authors, (Authors.id == AuthorBooks.author_id)) \
            .select(Book, Author).limit(0, 2) \
            .order_by(Authors.name.desc(), Books.id.asc()).iter()
        data = list(it)
        self.assertTrue(len(data) == 1, u"expect only one row")
        a_book, a_author = data[0]
        self.assertTrue(a_book.id == 1 and a_author.name == "akun",
                        u"query invalid data")


class TestColumn(TestCase):

    def test_bool_expr(self):
        # and expr
        and_expr = (Books.id > 0) & (Books.id < 8)
        args = []
        sql = and_expr.building(lambda x: "ok", args)
        self.assertTrue(args == [0, 8], u"args invalid")
        self.assertTrue("ok > %s AND (ok < %s)" == sql, u"sql invalid")

        # and or expr
        or_expr = ((Books.id > 0) | (Books.id < 9)) & (Books.name < 8)
        args = []
        sql = or_expr.building(lambda x: "ok", args)
        self.assertTrue(args == [0, 9, 8], u"args invalid")
        self.assertTrue("ok > %s OR (ok < %s) AND (ok < %s)" == sql,
                        u"sql invalid")

    def test_in_expr(self):
        in_expr = Books.id << (1, 2, 3)
        args = []
        sql = in_expr.building(lambda x: x.db_column, args)
        self.assertTrue(args == [(1, 2, 3)] and sql == "id IN %s",
                        u"in expr invalid")

    def test_between_null_expr(self):
        between_expr = Books.id >> (1, 2)
        args = []
        sql = between_expr.building(lambda x: x.db_column, args)
        self.assertTrue(args == [1, 2] and sql == "id BETWEEN %s AND %s",
                        u"in expr invalid")
        null_expr = -Books.id
        args = []
        sql = null_expr.building(lambda x: x.db_column, args)
        self.assertTrue(args == [] and sql == "id IS NULL",
                        u"is null expr test fail")
        null_expr = +Books.id
        args = []
        sql = null_expr.building(lambda x: x.db_column, args)
        self.assertTrue(args == [] and sql == "id IS NOT NULL",
                        u"is not null expr test fail")


class TestBasic(TestCase):
    def test_basic_magic(self):
        self.assertTrue(Books.id is not Base.id, u"fail to clone parent's cols")
        self.assertTrue(Book.id is not int, u"fail to replace entity fields")
        self.assertTrue(Book.name is not str, u"fail to replace entity fields")

    def test_dirty_tracking(self):
        book = Book()
        holder = get_holder(book)

        self.assertTrue(not holder.dirty_fields_map(),
                        u"initial state is wrong")
        book.id = 99
        book.name = "akun"
        self.assertTrue(book.id == 99 and book.name == "akun",
                        u"can't set entity field")
        id_dirty = holder.dirty_fields_map().get("id") == 99
        name_dirty = holder.dirty_fields_map().get("name") == "akun"
        self.assertTrue(id_dirty and name_dirty, u"expect name, id dirty")
        # reset
        holder.reset(dict(id=1, name="abc"))
        self.assertTrue(book.id == 1 and book.name == "abc",
                        u"fail to reset instance")
        self.assertTrue(not holder.dirty_fields_map(),
                        u"fail to reset dirty state")
