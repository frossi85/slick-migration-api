package scala.slick
package migration.api

import org.scalatest.{FunSuite, Matchers}
import slick.driver.H2Driver
import slick.migration.api.{Migration, MigrationSeq, ReversibleMigration, ReversibleMigrationSeq}

class MigrationSeqTest extends FunSuite with Matchers {
  test("& returns the right type and doesn't keep nesting") {
    val m = new Migration {
      def apply()(implicit s: H2Driver.api.Session) = ()
    }
    m & m & m should equal (MigrationSeq(m, m, m))

    val rm = new ReversibleMigration {
      def apply()(implicit s: H2Driver.api.Session) = ()
      def reverse = this
    }

    val rms = rm & rm & rm
    implicitly[rms.type <:< ReversibleMigrationSeq]
    rms should equal (new ReversibleMigrationSeq(rm, rm, rm))
  }
}
