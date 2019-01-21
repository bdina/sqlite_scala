package sqlite

import scala.collection.mutable.ArrayBuffer

class Page ( data : ArrayBuffer[Byte] = new ArrayBuffer[Byte](Table.PAGE_BYTES) ) {
  def getData () : ArrayBuffer[Byte] = data
}

case object Table {
  val PAGE_BYTES      = 4096
  val TABLE_MAX_PAGES = 100
  val ROWS_PER_PAGE   = PAGE_BYTES / UserRow.Column.ROW_BYTES
  val TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
}

case class Table ( ) {
  var row_num = 0
  var pages   = ArrayBuffer[sqlite.Page](new Page ())

  def row_slot ( row_num : Int ) : ( Page , Int ) = {
    val page_num = row_num / Table.ROWS_PER_PAGE
    var page = this.pages(page_num)
    if ( page == null ) {
      // Allocate memory only when we try to access page
      this.pages(page_num) = new Page ()
      page = this.pages(page_num)
    }
    val row_offset  = row_num % Table.ROWS_PER_PAGE
    val byte_offset = row_offset * UserRow.Column.ROW_BYTES
    return ( page , byte_offset )
  }
}

object StatementType {
  sealed trait StatementType
  case object INSERT extends StatementType
  case object SELECT extends StatementType
}

case object UserRow {
  case object Column {
    val ID_BYTES    = 4
    val USER_BYTES  = 40
    val EMAIL_BYTES = 40

    val ID_OFFSET    = 0
    val USER_OFFSET  = ID_OFFSET + ID_BYTES
    val EMAIL_OFFSET = USER_OFFSET + USER_BYTES

    val ROW_BYTES = ID_BYTES + USER_BYTES + EMAIL_BYTES
  }

  def serialize ( row : UserRow ) : Array[Byte] = {
    val id_arr    = Array( row.id.byteValue )

    val user_arr  = row.user.getBytes
    val email_arr = row.email.getBytes

    val padded_id    = id_arr    ++ Array.fill[Byte]( UserRow.Column.ID_BYTES    - id_arr.length   ) { 0 }
    val padded_user  = user_arr  ++ Array.fill[Byte]( UserRow.Column.USER_BYTES  - user_arr.length ) { 0 }
    val padded_email = email_arr ++ Array.fill[Byte]( UserRow.Column.EMAIL_BYTES - user_arr.length ) { 0 }

    padded_id ++ padded_user ++ padded_email
  }

  def deserialze ( row : Array[Byte] ) : UserRow = {
    import java.nio.ByteBuffer
    val id    = row.slice( UserRow.Column.ID_OFFSET    , UserRow.Column.ID_BYTES    )
    val user  = row.slice( UserRow.Column.USER_OFFSET  , UserRow.Column.USER_BYTES  )
    val email = row.slice( UserRow.Column.EMAIL_OFFSET , UserRow.Column.EMAIL_BYTES )
    UserRow( ByteBuffer.wrap(id).getInt , new String(user) , new String(email) )
  }
}

sealed trait Row
case class UserRow ( id : Int = 0
                   , user  : String = ""
                   , email : String = "" ) extends Row {

  override def toString () : String = s"($id, $user, $email)"
  def print_row () : Unit = println ( toString () )
}

import StatementType._

case class Statement ( statement_type : StatementType , row : UserRow )

object PrepareStatement {
  sealed trait Result
  case object SUCCESS                extends Result
  case object SYNTAX_ERROR           extends Result
  case object UNRECOGNIZED_STATEMENT extends Result
}

object ExecuteStatement {
  sealed trait Result
  case object SUCCESS    extends Result
  case object TABLE_FULL extends Result
}

class SQLite {
  def void () : Boolean = true
}

object SQLite {

  import Table._

  def do_meta_command ( command : String ) : Unit = {
    if ( ".exit".equals(command) ) {
      System.exit ( 0 )
    } else {
      println(s"Unrecognized command '$command'")
    }
  }

  def prepare_statement ( statement : String ) : ( PrepareStatement.Result , Option[Statement] ) = {
    val insert_matcher = "insert(.*)into(.*)".r
    val select_matcher = "select(.*)from(.*)".r

    if ( statement.startsWith("insert") ) {
      val row = insert_matcher.findFirstMatchIn(statement) match {
        case Some(v) => UserRow(0,v.group(1),v.group(2))
        case _ => return ( PrepareStatement.UNRECOGNIZED_STATEMENT , None )
      }
      return ( PrepareStatement.SUCCESS , Some(Statement(StatementType.INSERT, row)) )
    } else if ( statement.startsWith("select") ) {
      val row = select_matcher.findFirstMatchIn(statement) match {
        case Some(v) => UserRow(0,v.group(1),v.group(2))
        case _ => return ( PrepareStatement.UNRECOGNIZED_STATEMENT , None )
      }
      return ( PrepareStatement.SUCCESS , Some(Statement(StatementType.SELECT , row)) )
    } else {
      return ( PrepareStatement.SYNTAX_ERROR , None )
    }
  }

  def execute_insert ( statement : Statement , table : Table ) : ExecuteStatement.Result = {
    if ( table.row_num >= TABLE_MAX_ROWS ) {
      return ExecuteStatement.TABLE_FULL
    }

    val row_to_insert = statement.row
    val ( page , slot ) = table.row_slot(table.row_num)
    page.getData().insertAll(slot,UserRow.serialize(row_to_insert))
    table.row_num += 1

    return ExecuteStatement.SUCCESS
  }

  def execute_select ( statement : Statement , table : Table ) : ExecuteStatement.Result = {
    for ( i <- 0 until table.row_num ) {
      val ( page , slot ) = table.row_slot(i)
      val row = UserRow.deserialze(page.getData().slice(slot,UserRow.Column.ROW_BYTES).toArray)
      println(row)
    }
    return ExecuteStatement.SUCCESS
  }

  def execute_statement ( statement : Statement , table : Table ) : ExecuteStatement.Result = {
    statement.statement_type match {
      case ( StatementType.INSERT ) => execute_insert(statement , table )
      case ( StatementType.SELECT ) => execute_select(statement , table )
    }
  }

  import java.util.Scanner

  def print_prompt () = printf("db > ")

  def read_input ( ) : Scanner = {
    val scanner = new Scanner ( System.in )

    if ( ! scanner.hasNext() ) {
      println("Error reading input")
      System.exit(1)
    }

    scanner
  }

  def main ( args : Array[String] ) : Unit = {
    val table = Table()

    while ( true ) {
      print_prompt()

      val scanner = read_input()

      val command = scanner.next

      if ( command.startsWith(".") ) {
        do_meta_command ( command )
      }

      val statement = command + scanner.nextLine

      prepare_statement ( statement ) match {
        case ( PrepareStatement.SUCCESS , Some(statement) ) => {
          execute_statement ( statement , table ) match {
            case ExecuteStatement.SUCCESS    => println("executed.")
            case ExecuteStatement.TABLE_FULL => println("table full!")
          }
        }
        case ( PrepareStatement.SYNTAX_ERROR , None ) => println(s"Unrecognized keyword at start of '$statement'.")
        case ( PrepareStatement.UNRECOGNIZED_STATEMENT , None ) => println(s"Unrecognized statement '$statement'.")
        case  _ => println(s"Unknown error for '$statement'.")
      }
    }
  }
}
