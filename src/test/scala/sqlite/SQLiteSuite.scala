package sqlite

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SQLiteSuite extends FunSuite {
  test("void is always true") {
    def sqlite = new SQLite()
    assert(sqlite.void)
  }
}
