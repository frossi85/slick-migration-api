package scala.slick
package migration.api

import java.sql.Types
import com.typesafe.slick.testkit.util.JdbcTestDB
import org.scalatest.{BeforeAndAfterAll, FunSuite, Inside, Matchers}
import slick.driver.JdbcDriver
import slick.jdbc.JdbcBackend
import slick.jdbc.meta.{MIndexInfo, MPrimaryKey, MQName, MTable}
import slick.migration.api.{Dialect, TableMigration}
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

abstract class DbTest[D <: JdbcDriver](val tdb: JdbcTestDB { val driver: D })(implicit protected val dialect: Dialect[D])
  extends FunSuite
  with Matchers
  with Inside
  with BeforeAndAfterAll {

  implicit lazy val db = tdb.createDB

  implicit lazy val session = db.createSession

  lazy val driver: D = tdb.driver

  import driver.api._

  override def beforeAll() = tdb.cleanUpBefore()
  override def afterAll() = {
    session.close()
    tdb.cleanUpAfter()
  }

  val catalog, schema = Option("")

  def noActionReturns: ForeignKeyAction = ForeignKeyAction.NoAction

  def getTables(implicit session: JdbcBackend#Session): List[MTable] = Await.result(db.run(MTable.getTables(catalog, schema, None, None).map(x => x.toList)), Duration.Inf)
  def getTable(name: String)(implicit session: JdbcBackend#Session) =
    getTables.find(_.name.name == name)

  def longJdbcType = Types.BIGINT

  /**
   * How JDBC metadata returns a column's default string value
   */
  def columnDefaultFormat(s: String) = s"'$s'"

  test("create, drop") {
    class Table1(tag: Tag) extends Table[(Long, String)](tag, "table1") {
      def id = column[Long]("id", O.AutoInc, O.PrimaryKey)
      def col1 = column[String]("col1", O.Default("abc"))
      def * = (id, col1)
    }
    val table1 = TableQuery[Table1]

    val before = getTables

    val tm = TableMigration(table1)
    val createTable = tm.create.addColumns(_.id, _.col1)

    createTable()

    try {
      val after = getTables
      val tableName = table1.baseTableRow.tableName
      inside(after filterNot before.contains) {
        case (table @ MTable(MQName(_, _, `tableName`), "TABLE", _, _, _, _)) :: Nil =>
          val cols = Await.result(db.run(table.getColumns.map(x => x.toList)), Duration.Inf)
          cols.map(col => (col.name, col.sqlType, col.nullable)) should equal (List(
            ("id", longJdbcType, Some(false)),
            ("col1", Types.VARCHAR, Some(false))
          ))
          val autoinc: Map[String, Boolean] = cols.flatMap(col => col.isAutoInc.map(col.name -> _)).toMap
          autoinc.get("id") foreach (_ should equal (true))
          autoinc.get("col1") foreach (_ should equal (false))
          cols.find(_.name == "col1").flatMap(_.columnDef).foreach(_ should equal (columnDefaultFormat("abc")))
      }

      tm.create.reverse should equal (tm.drop)

    } finally
      tm.drop.apply()

    getTables should equal (before)
  }

  test("addColumns") {
    class Table5(tag: Tag) extends Table[(Long, String, Option[Int])](tag, "table5") {
      def col1 = column[Long]("col1")
      def col2 = column[String]("col2", O.Default(""))
      def col3 = column[Option[Int]]("col3")
      def * = (col1, col2, col3)
    }
    val table5 = TableQuery[Table5]

    val tm = TableMigration(table5)
    tm.create.addColumns(_.col1)()




    def columnsCount = getTable("table5").map(x => Await.result(db.run(x.getColumns.map(x => x.toList)), Duration.Inf).length)
    //def columnsCount = getTable("table5").map(_.getColumns.list.length)

    try {
      columnsCount should equal (Some(1))

      val addColumn = tm.addColumns(_.col2, _.col3)
      addColumn.reverse should equal (tm.dropColumns(_.col3, _.col2))

      addColumn()

      columnsCount should equal (Some(3))
    } finally
      tm.drop.apply()
  }

  test("addIndexes, dropIndexes") {
    class Table4(tag: Tag) extends Table[(Long, Int, Int, Int)](tag, "table4") {
      def id = column[Long]("id")
      def col1 = column[Int]("col1")
      def col2 = column[Int]("col2")
      def col3 = column[Int]("col3")
      def * = (id, col1, col2, col3)
      val index1 = index("index1", col1)
      val index2 = index("index2", (col2, col3), true)
    }
    val table4 = TableQuery[Table4]

    def indexList = getTable("table4").map(x => Await.result(db.run(x.getIndexInfo().map(x => x.toList)), Duration.Inf)) getOrElse Nil
    //def indexList = getTable("table4").map(x => x.getIndexInfo().list) getOrElse Nil

    def indexes = indexList
      .groupBy(i => (i.indexName, !i.nonUnique))
      .mapValues {
        _ collect {
          case MIndexInfo(MQName(_, _, "table4"), _, _, _, _, seq, col, _, _, _, _) =>
            (seq, col)
        }
      }

    val tm = TableMigration(table4)
    tm.create.addColumns(_.id, _.col1, _.col2, _.col3)()

    try {
      val createIndexes = tm.addIndexes(_.index1, _.index2)
      createIndexes()

      indexes(Some("index1") -> false) should equal (List((1, Some("col1"))))
      indexes(Some("index2") -> true) should equal (List((1, Some("col2")), (2, Some("col3"))))

      createIndexes.reverse should equal (tm.dropIndexes(_.index2, _.index1))

      createIndexes.reverse.apply()

      indexes.keys.flatMap(_._1).exists(Set("index1", "index2") contains _) should equal (false)
    } finally
      tm.drop.apply()
  }

  test("rename, renameIndex") {
    class Table7Base(tag: Tag, name: String) extends Table[Long](tag, name) {
      def col1 = column[Long]("col1")
      def * = col1
      val index1 = index("oldIndexName", col1)
    }
    class OldName(tag: Tag) extends Table7Base(tag, "oldname")
    class Table7(tag: Tag) extends Table7Base(tag, "table7")
    val oldname = TableQuery[OldName]
    val table7 = TableQuery[Table7]

    val tm = TableMigration(oldname)
    tm.create.addColumns(_.col1).addIndexes(_.index1)()

    def tables = getTables.map(_.name.name).filterNot(_ == "oldIndexName")
    def indexes = getTables.flatMap(x => Await.result(db.run(x.getIndexInfo().map(x => x.toList)), Duration.Inf).flatMap(_.indexName))

    tables should equal (List("oldname"))
    indexes should equal (List("oldIndexName"))

    tm.rename("table7")()
    tables should equal (List("table7"))

    val tm7 = TableMigration(table7)
    try {
      tm7.renameIndex(_.index1, "index1")()
      indexes should equal (List("index1"))
    } finally
      tm7.drop.apply()

    tm.rename("table7").reverse should equal (tm7.rename("oldname"))
  }
}

trait CompleteDbTest { this: DbTest[_ <: JdbcDriver] =>
  import driver.api._

  test("addPrimaryKeys, dropPrimaryKeys") {
    def pkList = getTable("table8").map(x => Await.result(db.run(x.getPrimaryKeys.map(x => x.toList)), Duration.Inf)) getOrElse Nil
    def pks = pkList
      .groupBy(_.pkName)
      .mapValues {
        _ collect { case MPrimaryKey(MQName(_, _, "table8"), col, seq, _) => (seq, col) }
      }

    class Table8(tag: Tag) extends Table[(Long, String)](tag, "table8") {
      def id = column[Long]("id")
      def stringId = column[String]("stringId")
      def * = (id, stringId)
      def pk = primaryKey("PRIMARY", (id, stringId))  // note mysql will always use the name "PRIMARY" anyway
    }
    val table8 = TableQuery[Table8]

    val tm = TableMigration(table8)
    tm.create.addColumns(_.id, _.stringId)()

    val before = pks

    before.get(Some("PRIMARY")) should equal (None)

    try {
      tm.addPrimaryKeys(_.pk)()

      pks(Some("PRIMARY")) should equal (List(1 -> "id", 2 -> "stringId"))

      tm.addPrimaryKeys(_.pk).reverse should equal (tm.dropPrimaryKeys(_.pk))

      tm.dropPrimaryKeys(_.pk)()

      pks should equal (before)
    } finally tm.drop.apply()
  }

  test("addForeignKeys, dropForeignKeys") {
    class Table3(tag: Tag) extends Table[Long](tag, "table3") {
      def id = column[Long]("id", O.PrimaryKey)
      def * = id
    }
    val table3 = TableQuery[Table3]
    class Table2(tag: Tag) extends Table[(Long, Long)](tag, "table2") {
      def id = column[Long]("id", O.PrimaryKey)
      def other = column[Long]("other")

      def * = (id, other)
      // not a def, so that equality works
      lazy val fk = foreignKey("fk_other", other, table3)(_.id, ForeignKeyAction.NoAction, ForeignKeyAction.Cascade)
    }
    val table2 = TableQuery[Table2]

    val tm2 = TableMigration(table2)
    val tm3 = TableMigration(table3)

    try {
      tm2.create.addColumns(_.id, _.other)()
      tm3.create.addColumns(_.id)()

      def fks = getTables.to[Set] map { t =>
        (
          t.name.name,
          Await.result(db.run(t.getExportedKeys.map(x => x.toList)), Duration.Inf) map { fk =>
            (fk.pkTable.name, fk.pkColumn, fk.fkTable.name, fk.fkColumn, fk.updateRule, fk.deleteRule, fk.fkName)
          }
        )
      }

      val before = fks

      before should equal (Set(
        ("table2", Nil),
        ("table3", Nil)
      ))

      val createForeignKey = tm2.addForeignKeys(_.fk)
      createForeignKey()

      fks should equal (Set(
        ("table2", Nil),
        ("table3", ("table3", "id", "table2", "other", noActionReturns, ForeignKeyAction.Cascade, Some("fk_other")) :: Nil)
      ))

      val dropForeignKey = createForeignKey.reverse

      dropForeignKey.data.foreignKeysDrop.toList should equal (table2.baseTableRow.fk.fks.toList)
      dropForeignKey should equal (tm2.dropForeignKeys(_.fk))

      dropForeignKey()

      fks should equal (before)
    } finally {
      tm2.drop.apply()
      tm3.drop.apply()
    }
  }

  test("dropColumns") {
    class Table11(tag: Tag) extends Table[(Long, String)](tag, "table11") {
      def col1 = column[Long]("col1")
      def col2 = column[String]("col2", O.Default(""))
      def * = (col1, col2)
    }
    val table11 = TableQuery[Table11]

    val tm = TableMigration(table11)
    tm.create.addColumns(_.col1, _.col2)()

    def columnsCount = getTable("table11").map(x => Await.result(db.run(x.getColumns.map(x => x.toList)), Duration.Inf).length)

    try {
      columnsCount should equal (Some(2))

      tm.dropColumns(_.col2)()

      columnsCount should equal (Some(1))
    } finally
      tm.drop.apply()

    tm.addColumns(_.col1, _.col2).reverse should equal (tm.dropColumns(_.col2, _.col1))
  }

  test("alterColumnTypes") {
    class Table6(tag: Tag) extends Table[String](tag, "table6") {
      def tmpOldId = column[java.sql.Date]("id")
      def id = column[String]("id", O.Default(""))
      def * = id
    }
    val table6 = TableQuery[Table6]

    val tm = TableMigration(table6)
    tm.create.addColumns(_.tmpOldId)()

    def columnTypes = getTable("table6").map(x => Await.result(db.run(x.getColumns.map(x => x.toList)), Duration.Inf).map(_.sqlType)) getOrElse Nil

    try {
      columnTypes.toList should equal (List(Types.DATE))
      tm.alterColumnTypes(_.id)()
      columnTypes.toList should equal (List(Types.VARCHAR))
    } finally
      tm.drop.apply()
  }

  test("alterColumnDefaults") {
    class Table9(tag: Tag) extends Table[String](tag, "table9") {
      def tmpOldId = column[String]("id")
      def id = column[String]("id", O.Default("abc"))
      def * = id
    }
    val table9 = TableQuery[Table9]

    val tm = TableMigration(table9)
    tm.create.addColumns(_.tmpOldId)()

    def columns = getTable("table9").map(x => Await.result(db.run(x.getColumns.map(x => x.toList)), Duration.Inf).map(_.columnDef)) getOrElse Nil

    try {
      columns.toList should equal (List((None)))
      tm.alterColumnDefaults(_.id)()
      columns.toList should equal (List((Some(columnDefaultFormat("abc")))))
    } finally
      tm.drop.apply()
  }

  /*
  test("alterColumnNulls") {
    class Table10(tag: Tag) extends Table[String](tag, "table10") {
      def tmpOldId = column[Option[String]]("id")
      def id = column[String]("id")
      def * = id
    }
    val table10 = TableQuery[Table10]

    val tm = TableMigration(table10)
    tm.create.addColumns(_.tmpOldId)()

    try {
      table10.map(_.tmpOldId) += (null: String)
      table10.delete

      tm.alterColumnNulls(_.id)()

      intercept[SQLException] {
        table10.map(_.id).insert(null: String)
      }
    } finally
      tm.drop.apply()
  }*/

  test("renameColumn") {
    class Table12(tag: Tag) extends Table[Long](tag, "table12") {
      def col1 = column[Long]("oldname")
      def * = col1
    }
    val table12 = TableQuery[Table12]

    def columns = getTable("table12").toList.flatMap(x => Await.result(db.run(x.getColumns.map(x => x.toList)), Duration.Inf).map(_.name))

    val tm = TableMigration(table12)
    tm.create.addColumns(_.col1)()

    try {
      columns should equal (List("oldname"))

      tm.renameColumn(_.col1, "col1")()

      columns should equal (List("col1"))
    } finally
      tm.drop.apply()

    tm.renameColumn(_.col1, "col1").reverse should equal (tm.renameColumn(_.column[Long]("col1"), "oldname"))
  }
}
