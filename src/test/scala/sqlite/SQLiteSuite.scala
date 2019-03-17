package sqlite

import java.io.{InputStreamReader, OutputStreamWriter}
import java.nio.CharBuffer
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.TimeUnit

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SQLiteSuite extends FlatSpec with Matchers with BeforeAndAfter {

  before {
    Files.deleteIfExists(Paths.get("sqlite.db"))
  }

  def run_script ( commands : util.List[String] ) : util.List[String] = {
    val process = new ProcessBuilder("./db").start()

    val os     = process.getOutputStream
    val stdin  = new OutputStreamWriter ( os )
    val is     = process.getInputStream
    val stdout = new InputStreamReader  ( is )

    val out  = new util.ArrayList[String]()

    val sbuf = new StringBuffer()

    commands.forEach(c => {
      try {
        val cmd_line = s"$c\r\n"
        stdin.write(cmd_line)
        stdin.flush()

        os.flush()

        TimeUnit.MILLISECONDS.sleep(3000L)

        val cbuf = CharBuffer.allocate(8192)

        var chars = is.available()
        var total = 0
        while ( chars > 0 ) {
          total += stdout.read(cbuf)
          chars  = is.available()
        }

        sbuf.append(cbuf.array() , 0 , total)
      } catch {
        case e : Exception => println(e) ; stdin.close() ; stdout.close()
      }
    })

    try {
      stdin.write(s".exit\r\n")
      stdin.flush()

      os.flush()

      TimeUnit.MILLISECONDS.sleep(3000L)

      val cbuf = CharBuffer.allocate(8192)

      var chars = is.available()
      var total = 0
      while ( chars > 0 ) {
        total += stdout.read(cbuf)
        chars = is.available()
      }

      sbuf.append(cbuf.array(), 0, total)
    } catch {
      case e : Exception => println(e)
    } finally {
      stdin.close()
      stdout.close()
    }

    sbuf.toString.split("\n").foreach(l => out.add(l))

    out
  }

  it should "insert and retrieve a row" in {
    val result = run_script(util.List.of("insert 1 user1 person1@example.com" , "select *")).iterator()
    result.next should be ( "db > Executed." )
    result.next should be ( "db > (1, user1, person1@example.com)" )
    result.next should be ( "Executed."                            )
    result.next should be ( "db > "                                )
  }

  it should "print error message when table is full" in {
    val commands = new util.ArrayList[String]()
    for ( i <- 0 to 14 ) {
      commands.add(s"insert $i user$i person$i@example.com")
    }
    val result = run_script(commands).iterator()
    for ( _ <- 0 until 14 ) { result.next should be ( "db > Executed." ) }

    result.next should be ( "db > Executed." )
  }

  it should "allow inserting strings that are the maximum length" in {
    val user  = "a"*145
    val email = "a"*145
    val commands = util.List.of(s"insert 1 $user $email" , "select *")
    val result = run_script(commands).iterator()
    result.next should be ( "db > Executed." )
  }

  it should "print error message if strings are too long" in {
    val user  = "a"*146
    val email = "a"*146
    val commands = util.Arrays.asList(s"insert 1 $user $email")
    val result = run_script(commands).iterator()
    result.next should be ( "db > String is too long." )
  }

  it should "print error message is id is negative" in {
    val result = run_script(java.util.Arrays.asList("insert -1 user1 person1@example.com")).iterator()
    result.next should be ( "db > ID must be positive." )
  }

  it should "keep data after closing connection" in {
    val result1 = run_script(util.List.of("insert 1 user1 person1@example.com" , "select *")).iterator()
    result1.next should be ( "db > Executed." )
    result1.next should be ( "db > (1, user1, person1@example.com)" )

    val result2 = run_script(util.Arrays.asList("select *")).iterator()
    result2.next should be ( "db > (1, user1, person1@example.com)" )
  }

  it should "allow printing out the structure of a one-node btree" in {
    val result = run_script(util.List.of("insert 3 user3 person3@example.com"
                                       , "insert 1 user1 person1@example.com"
                                       , "insert 2 user2 person2@example.com"
                                       , ".btree")).iterator

    result.next should be ( "db > Executed."   )
    result.next should be ( "db > Executed."   )
    result.next should be ( "db > Executed."   )
    result.next should be ( "db > Tree:"       )
    result.next should be ( "- leaf (size 3)"  )
    result.next should be ( "  - 1"            )
    result.next should be ( "  - 2"            )
    result.next should be ( "  - 3"            )
    result.next should be ( "db > "            )
  }

  it should "print constants" in {
    val list = new util.ArrayList[String]()
    list.add ( ".constants" )

    val result = run_script(list).iterator
    result.next should be ( "db > Constants:"                 )
    result.next should be ( "ROW_BYTES: 294"                  )
    result.next should be ( "COMMON_NODE_HEADER_BYTES: 6"     )
    result.next should be ( "LEAF_NODE_HEADER_BYTES: 10"      )
    result.next should be ( "LEAF_NODE_CELL_BYTES: 298"       )
    result.next should be ( "LEAF_NODE_SPACE_FOR_CELLS: 4090" )
    result.next should be ( "LEAF_NODE_MAX_CELLS: 13"         )
    result.next should be ( "db > "                           )
  }

  it should "prints an error message if there is a duplicate id" in {
    val result = run_script(util.List.of(
        "insert 1 user1 person1@example.com"
      , "insert 1 user1 person1@example.com"
      , "select *")).iterator

    result.next should be ( "db > Executed."                       )
    result.next should be ( "db > Error: Duplicate key."           )
    result.next should be ( "db > (1, user1, person1@example.com)" )
    result.next should be ( "Executed."                            )
    result.next should be ( "db > "                                )
  }

  it should "allow printing out the structure of a 3-leaf-node btree" in {
    val commands = new util.ArrayList[String]()
    for ( i <- 1 to 14 ) {
      commands.add(s"insert $i user$i person$i@example.com")
    }
    commands.add(".btree")

    val result = run_script(commands).iterator()

    for ( _ <- 0 until 14 ) { result.next should be ( "db > Executed." ) }

    result.next should be ("db > Tree:" )
    result.next should be ("- internal (size 1)" )
    result.next should be ("  - leaf (size 7)" )
    result.next should be ("    - 1" )
    result.next should be ("    - 2" )
    result.next should be ("    - 3" )
    result.next should be ("    - 4" )
    result.next should be ("    - 5" )
    result.next should be ("    - 6" )
    result.next should be ("    - 7" )
    result.next should be ("  - key 7" )
    result.next should be ("  - leaf (size 7)" )
    result.next should be ("    - 8" )
    result.next should be ("    - 9" )
    result.next should be ("    - 10" )
    result.next should be ("    - 11" )
    result.next should be ("    - 12" )
    result.next should be ("    - 13" )
    result.next should be ("    - 14" )
  }
}
