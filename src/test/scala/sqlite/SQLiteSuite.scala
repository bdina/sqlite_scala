package sqlite

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.file.{Files, Paths}
import java.util

import org.scalatest.{FlatSpec, Matchers}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SQLiteSuite extends FlatSpec with Matchers {

  def run_script ( commands : util.List[String] ) : util.List[String] = {
    val process = new ProcessBuilder("./db").start()

    val stdin  = new BufferedWriter( new OutputStreamWriter( process.getOutputStream ))
    val stdout = new BufferedReader( new InputStreamReader ( process.getInputStream  ))

    val out = new util.ArrayList[String]()

    commands.forEach(c => {
      try {
        stdin.write(c)
        stdin.newLine()
        stdin.flush()
        out.add(stdout.readLine())
      } catch {
        case e : Exception => println(e) ; stdin.close() ; stdout.close()
      }
    })

    try {
      stdin.write(f".exit%n")
      stdin.newLine()
      stdin.flush()
    } catch {
      case e : Exception => println(e)
    } finally {
      stdin.close()
      stdout.close()
    }

    out
  }

  it should "insert and retrieve a row" in {
    Files.deleteIfExists(Paths.get("sqlite.db"))

    val result = run_script(util.List.of("insert 1 user1 person1@example.com" , "select *")).iterator()
    result.next should be ( "db > Executed." )
    result.next should be ( "db > (1, user1, person1@example.com)" )
  }

  it should "print error message when table is full" in {
    Files.deleteIfExists(Paths.get("sqlite.db"))

    val commands = new util.ArrayList[String]()
    for ( i <- 0 until 4801 ) {
      commands.add(s"insert $i user$i person$i@example.com")
    }
    val result = run_script(commands).iterator()
    for ( _ <- 1 until 4801 ) { result.next should be ( "db > Executed." ) }

    result.next should be ( "db > Table full!" )
  }

  it should "allow inserting strings that are the maximum length" in {
    Files.deleteIfExists(Paths.get("sqlite.db"))

    val user  = "a"*40
    val email = "a"*40
    val commands = util.List.of(s"insert 1 $user $email" , "select *")
    val result = run_script(commands).iterator()
    result.next should be ( "db > Executed." )
  }

  it should "print error message if strings are too long" in {
    Files.deleteIfExists(Paths.get("sqlite.db"))

    val user  = "a"*41
    val email = "a"*41
    val commands = util.Arrays.asList(s"insert 1 $user $email")
    val result = run_script(commands).iterator()
    result.next should be ( "db > String is too long." )
  }

  it should "print error message is id is negative" in {
    Files.deleteIfExists(Paths.get("sqlite.db"))

    val result = run_script(java.util.Arrays.asList("insert -1 user1 person1@example.com")).iterator()
    result.next should be ( "db > ID must be positive." )
  }

  it should "keep data after closing connection" in {
    Files.deleteIfExists(Paths.get("sqlite.db"))

    val result1 = run_script(util.List.of("insert 1 user1 person1@example.com" , "select *")).iterator()
    result1.next should be ( "db > Executed." )
    result1.next should be ( "db > (1, user1, person1@example.com)" )

    val result2 = run_script(util.Arrays.asList("select *")).iterator()
    result2.next should be ( "db > (1, user1, person1@example.com)" )
  }

  it should "allow printing out the structure of a one-node btree" in {
    Files.deleteIfExists(Paths.get("sqlite.db"))

    val result = run_script(util.List.of("insert 3 user3 person3@example.com"
                                       , "insert 1 user1 person1@example.com"
                                       , "insert 2 user2 person2@example.com"
                                       , ".exit")).iterator

    result.next should be ( "db > Executed." )
    result.next should be ( "db > Executed." )
    result.next should be ( "db > Executed." )
    result.next should be ( "db > Tree:"     )
    result.next should be ( "leaf (size 3)"  )
    result.next should be ( "  - 0 : 3"      )
    result.next should be ( "  - 1 : 1"      )
    result.next should be ( "  - 2 : 2"      )
    result.next should be ( "db > "          )
  }

  it should "print constants" in {
    Files.deleteIfExists(Paths.get("sqlite.db"))

    val result = run_script(util.List.of(".constants" , ".exit")).iterator
    result.next should be ( "db > Constants:"                 )
    result.next should be ( "ROW_BYTES: 293"                  )
    result.next should be ( "COMMON_NODE_HEADER_BYTES: 6"     )
    result.next should be ( "LEAF_NODE_HEADER_BYTES: 10"      )
    result.next should be ( "LEAF_NODE_CELL_BYTES: 297"       )
    result.next should be ( "LEAF_NODE_SPACE_FOR_CELLS: 4086" )
    result.next should be ( "LEAF_NODE_MAX_CELLS: 13"         )
    result.next should be ( "db > "                           )
  }
}