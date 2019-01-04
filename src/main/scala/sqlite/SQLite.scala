package sqlite

class SQLite {
  def void () : Boolean = true
}

object SQLite {

  import java.util.Scanner

  def print_prompt () = printf("db > ")

  def read_input ( ) : Scanner = {
    val scanner = new Scanner ( System.in )

    if ( ! scanner.hasNext() ) {
      printf("Error reading input\n")
      System.exit(1)
    }

    scanner
  }

  def main ( args : Array[String] ) : Unit = {
    while ( true ) {
      print_prompt()

      val scanner = read_input()

      val command = scanner.next

      if ( ".exit".equals(command) ) {
        System.exit(0)
      } else {
        printf("Unrecognized command '%s'.\n", command)
      }
    }
  }
}
