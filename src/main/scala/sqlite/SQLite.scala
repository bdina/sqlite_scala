package sqlite

object StatementType {
  sealed trait StatementType
  case object INSERT extends StatementType
  case object SELECT extends StatementType
}

import StatementType._

case class Statement ( t : StatementType )

class SQLite {
  def void () : Boolean = true
}

object SQLite {

  def do_meta_command ( input_buffer : String ) : Unit = {
    if ( ".exit".equals(input_buffer) ) {
      System.exit ( 0 )
    } else {
      printf("Unrecognized command '%s'\n", input_buffer)
    }
  }

  def prepare_statement ( input_buffer : String ) : Option[Statement] = {
           if ( input_buffer.startsWith("insert") ) {
      return Some(Statement(StatementType.INSERT))
    } else if ( input_buffer.startsWith("select") ) {
      return Some(Statement(StatementType.SELECT))
    } else {
      return None
    }
  }

  def execute_statement ( statement : Statement ) : Unit = {
    statement.t match {
      case ( StatementType.INSERT ) => printf("This is where we would do an insert.\n")
      case ( StatementType.SELECT ) => printf("This is where we would do a select.\n")
    }
  }

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

      if ( command.startsWith(".") ) {
        do_meta_command ( command )
      }

      val statement = prepare_statement ( command )
       statement match {
        case Some(_) => {
          execute_statement ( statement.get )
          printf("Executed.\n");
        }
        case None => printf("Unrecognized keyword at start of '%s'.\n", command)
      }
    }
  }
}
