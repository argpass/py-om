# py-om
`om` isn't a `ORM` framework but object mapping library.

## Domain Models
Domain models(See `Domain Driven Development`) are normal classes without
complex magic fields.

```python
class Company(object):
    id = int
    name = str


class AuthorBook(object):
    author_id = int
    book_id = int


class Entity(object):
    id = int


class Author(Entity):
    name = str


class Book(Entity):
    name = str

```

## Repository
Repositories are mapping with db tables.

```python
class Base(TableMapper):
    id = Column(db_column="id")

    class Meta(Meta):
        is_abstract = True



class Companies(Base):
    name = Column(db_column="name")

    class Meta(Meta):
        identifiers = ("id",)
        db_table = "author"
        database = db
        managed = (Company,)


class AuthorBooks(TableMapper):
    author_id = Column(db_column="aid")
    book_id = Column(db_column="bid")

    class Meta(Meta):
        identifiers = ("author_id", "book_id")
        managed = (AuthorBook,)
        db_table = "t_author_book"
        database = db


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
```

## Insert 
```python
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
```

## Delete

```python
Books.where(Books.id > 0).delete()
AuthorBooks.where(AuthorBooks.author_id > 0).delete()
Authors.where(Authors.id > 0).delete()
Companies.where(Companies.id > 0).delete()
```

## Update
```python
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
```

## Query

```python
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
```
