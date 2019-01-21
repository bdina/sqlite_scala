package sqlite

import scala.collection.mutable.ArrayBuffer

class Page ( val data : ArrayBuffer[Byte] = new ArrayBuffer[Byte](Table.PAGE_BYTES) )

case object Table {
  val PAGE_BYTES      : Int = 4096
  val TABLE_MAX_PAGES : Int = 100
  val ROWS_PER_PAGE   : Int = PAGE_BYTES / UserRow.Column.ROW_BYTES
  val TABLE_MAX_ROWS  : Int = ROWS_PER_PAGE * TABLE_MAX_PAGES
}

case class Table ( ) {
  var row_num : Int = 0
  var pages   : ArrayBuffer[sqlite.Page] = ArrayBuffer[sqlite.Page](new Page ())

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
    ( page , byte_offset )
  }
}

object StatementType {
  sealed trait StatementType
  case object INSERT extends StatementType
  case object SELECT extends StatementType
}

case object UserRow {
  case object Column {
    val ID_BYTES    : Int = 4
    val USER_BYTES  : Int = 40
    val EMAIL_BYTES : Int = 40

    val ID_OFFSET    : Int = 0
    val USER_OFFSET  : Int = ID_OFFSET + ID_BYTES
    val EMAIL_OFFSET : Int = USER_OFFSET + USER_BYTES

    val ROW_BYTES : Int = ID_BYTES + USER_BYTES + EMAIL_BYTES
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

  override def toString : String = s"($id, $user, $email)"
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

  val INSERT_MATCHER : scala.util.matching.Regex = "insert(.*)into(.*)".r
  val SELECT_MATCHER : scala.util.matching.Regex = "select(.*)from(.*)".r

  def prepare_statement ( statement : String ) : ( PrepareStatement.Result , Option[Statement] ) = {
    if ( statement.startsWith("insert") ) {
      val row = INSERT_MATCHER.findFirstMatchIn(statement) match {
        case Some(v) => UserRow(0,v.group(1),v.group(2))
        case _ => return ( PrepareStatement.UNRECOGNIZED_STATEMENT , None )
      }
      ( PrepareStatement.SUCCESS , Some(Statement(StatementType.INSERT, row)) )
    } else if ( statement.startsWith("select") ) {
      val row = SELECT_MATCHER.findFirstMatchIn(statement) match {
        case Some(v) => UserRow(0,v.group(1),v.group(2))
        case _ => return ( PrepareStatement.UNRECOGNIZED_STATEMENT , None )
      }
      ( PrepareStatement.SUCCESS , Some(Statement(StatementType.SELECT , row)) )
    } else {
      ( PrepareStatement.SYNTAX_ERROR , None )
    }
  }

  def execute_insert ( statement : Statement , table : Table ) : ExecuteStatement.Result = {
    if ( table.row_num >= TABLE_MAX_ROWS ) {
      return ExecuteStatement.TABLE_FULL
    }

    val row_to_insert = statement.row
    val ( page , slot ) = table.row_slot(table.row_num)
    page.data.insertAll(slot,UserRow.serialize(row_to_insert))
    table.row_num += 1

    ExecuteStatement.SUCCESS
  }

  def execute_select ( statement : Statement , table : Table ) : ExecuteStatement.Result = {
    for ( i <- 0 until table.row_num ) {
      val ( page , slot ) = table.row_slot(i)
      val row = UserRow.deserialze(page.data.slice(slot,UserRow.Column.ROW_BYTES).toArray)
      println(row)
    }
    ExecuteStatement.SUCCESS
  }

  def execute_statement ( statement : Statement , table : Table ) : ExecuteStatement.Result = {
    statement.statement_type match {
      case StatementType.INSERT => execute_insert(statement , table )
      case StatementType.SELECT => execute_select(statement , table )
    }
  }

  import java.util.Scanner

  def print_prompt () : Unit = printf("db > ")

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

      val token = scanner.next

      if ( token.startsWith(".") ) {
        do_meta_command ( token )
      }

      val line = token + scanner.nextLine

      prepare_statement ( line ) match {
        case ( PrepareStatement.SUCCESS , Some(statement) ) =>
          execute_statement ( statement , table ) match {
            case ExecuteStatement.SUCCESS    => println("executed.")
            case ExecuteStatement.TABLE_FULL => println("table full!")
          }
        case ( PrepareStatement.SYNTAX_ERROR , None ) => println(s"Unrecognized keyword at start of '$line'.")
        case ( PrepareStatement.UNRECOGNIZED_STATEMENT , None ) => println(s"Unrecognized statement '$line'.")
        case  _ => println(s"Unknown error for '$line'.")
      }
    }
  }
}
