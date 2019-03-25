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

  def run_script ( commands : util.List[String] , delay : Long = 3000l ) : util.List[String] = {
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

        TimeUnit.MILLISECONDS.sleep(delay)

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

      TimeUnit.MILLISECONDS.sleep(delay)

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
/*
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
    result.next should be ( "LEAF_NODE_HEADER_BYTES: 14"      )
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

    result.next should be ( "db > Tree:" )
    result.next should be ( "- internal (size 1)" )
    result.next should be ( "  - leaf (size 7)" )
    result.next should be ( "    - 1" )
    result.next should be ( "    - 2" )
    result.next should be ( "    - 3" )
    result.next should be ( "    - 4" )
    result.next should be ( "    - 5" )
    result.next should be ( "    - 6" )
    result.next should be ( "    - 7" )
    result.next should be ( "  - key 7" )
    result.next should be ( "  - leaf (size 7)" )
    result.next should be ( "    - 8" )
    result.next should be ( "    - 9" )
    result.next should be ( "    - 10" )
    result.next should be ( "    - 11" )
    result.next should be ( "    - 12" )
    result.next should be ( "    - 13" )
    result.next should be ( "    - 14" )
  }

  it should "print all rows in a multi-level tree" in {
    val commands = new util.ArrayList[String]()
    for ( i <- 1 to 15 ) {
      commands.add(s"insert $i user$i person$i@example.com")
    }

    run_script(commands,3000l)

    val result = run_script(util.Arrays.asList("select *")).iterator()

    result.next should be ( "db > (1, user1, person1@example.com)" )
    result.next should be ( "(2, user2, person2@example.com)" )
    result.next should be ( "(3, user3, person3@example.com)" )
    result.next should be ( "(4, user4, person4@example.com)" )
    result.next should be ( "(5, user5, person5@example.com)" )
    result.next should be ( "(6, user6, person6@example.com)" )
    result.next should be ( "(7, user7, person7@example.com)" )
    result.next should be ( "(8, user8, person8@example.com)" )
    result.next should be ( "(9, user9, person9@example.com)" )
    result.next should be ( "(10, user10, person10@example.com)" )
    result.next should be ( "(11, user11, person11@example.com)" )
    result.next should be ( "(12, user12, person12@example.com)" )
    result.next should be ( "(13, user13, person13@example.com)" )
    result.next should be ( "(14, user14, person14@example.com)" )
    result.next should be ( "(15, user15, person15@example.com)" )
    result.next should be ( "Executed." )
  }
*/

  it should "allow printing out the structure of a 4-leaf-node btree" in {
    val commands = util.List.of(
        "insert 18 user18 person18@example.com"
      , "insert 7 user7 person7@example.com"
      , "insert 10 user10 person10@example.com"
      , "insert 29 user29 person29@example.com"
      , "insert 23 user23 person23@example.com"
      , "insert 4 user4 person4@example.com"
      , "insert 14 user14 person14@example.com"
      , "insert 30 user30 person30@example.com"
      , "insert 15 user15 person15@example.com"
      , "insert 26 user26 person26@example.com"
      , "insert 22 user22 person22@example.com"
      , "insert 19 user19 person19@example.com"
      , "insert 2 user2 person2@example.com"
      , "insert 1 user1 person1@example.com"
      , "insert 21 user21 person21@example.com"
      , "insert 11 user11 person11@example.com"
      , "insert 6 user6 person6@example.com"
      , "insert 20 user20 person20@example.com"
      , "insert 5 user5 person5@example.com"
      , "insert 8 user8 person8@example.com"
      , "insert 9 user9 person9@example.com"
      , "insert 3 user3 person3@example.com"
      , "insert 12 user12 person12@example.com"
      , "insert 27 user27 person27@example.com"
      , "insert 17 user17 person17@example.com"
      , "insert 16 user16 person16@example.com"
      , "insert 13 user13 person13@example.com"
      , "insert 24 user24 person24@example.com"
      , "insert 25 user25 person25@example.com"
      , "insert 28 user28 person28@example.com"
      , ".btree"
    )
    val result = run_script(commands).iterator

    result.next should be ( "db > (1, user1, person1@example.com)" )
  }
}
