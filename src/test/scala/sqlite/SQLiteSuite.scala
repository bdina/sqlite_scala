package sqlite

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
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
    val result = run_script(util.List.of("insert 1 user1 person1@example.com" , "select *")).iterator()
    result.next should be ( "db > Executed." )
    result.next should be ( "db > (1, user1, person1@example.com)" )
  }

  it should "print error message when table is full" in {
    val commands = new util.ArrayList[String]()
    for ( i <- 0 until 4801 ) {
      commands.add(s"insert $i user$i person$i@example.com")
    }
    val result = run_script(commands).iterator()
    for ( _ <- 1 until 4801 ) { result.next should be ( "db > Executed." ) }

    result.next should be ( "db > Table full!" )
  }

  it should "allow inserting strings that are the maximum length" in {
    val user  = "a"*40
    val email = "a"*40
    val commands = util.List.of(s"insert 1 $user $email" , "select *")
    val result = run_script(commands).iterator()
    result.next should be ( "db > Executed." )
  }

  it should "print error message if strings are too long" in {
    val user  = "a"*41
    val email = "a"*41
    val commands = util.List.of(s"insert 1 $user $email" , "select *")
    val result = run_script(commands).iterator()
    result.next should be ( "db > String is too long." )
    result.next should be ( "db > Executed." )
  }

  it should "print error message is id is negative" in {
    val result = run_script(java.util.List.of("insert -1 user1 person1@example.com" , "select *")).iterator()
    result.next should be ( "db > ID must be positive." )
    result.next should be ( "db > Executed." )
  }
}