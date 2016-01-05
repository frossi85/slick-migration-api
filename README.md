[![Build Status](https://travis-ci.org/nafg/slick-migration-api.svg?branch=master)](https://travis-ci.org/nafg/slick-migration-api)
[![Coverage Status](https://img.shields.io/coveralls/nafg/slick-migration-api.svg)](https://coveralls.io/r/nafg/slick-migration-api?branch=master)


A library for defining database migrations, for use with Slick,
including a DSL to define type safe and typo safe table migrations
that are defined in terms of Slick table definitions.

Example:

````scala
implicit val dialect = new H2Dialect

val migrate =
  TableMigration(myTable)
    .create
    .addColumns(_.col1, _.col2)
    .addIndexes(_.index1)
    .renameColumn(_.col03, "col3") &
  SqlMigration("insert into myTable (col1, col2) values (10, 20)")

withSession { implicit session: Session =>
  migrate()
}
````

Note: Some test in MySql could fail, please see: https://www.farbeyondcode.com/Solution-for-MariaDB-Field--xxx--doesn-t-have-a-default-value-5-2720.html
