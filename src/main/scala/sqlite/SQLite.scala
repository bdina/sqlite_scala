package sqlite

import java.io.{File, RandomAccessFile}
import java.nio.file.{Files, Path, Paths}

import scala.collection.mutable.ArrayBuffer

case class Page ( data : ArrayBuffer[Byte] = new ArrayBuffer[Byte](Pager.PAGE_BYTES) )

case class Pager ( file : File ) {

  val file_descriptor : RandomAccessFile = new RandomAccessFile(file, "rw")

  var pages : ArrayBuffer[sqlite.Page] = ArrayBuffer[sqlite.Page](Page ())

  def get_page ( page_num : Int ) : Page = {
    if ( page_num >= this.pages.size ) {
      // Allocate memory only when we try to access new page
      this.pages += Page ()
    }

    if ( this.pages(page_num).data.isEmpty ) {
      // Cache miss. Allocate memory and load from file.
      var num_pages = file_descriptor.length() / Pager.PAGE_BYTES

      // We might save a partial page at the end of the file
      if (file_descriptor.length() % Pager.PAGE_BYTES > 0) {
        num_pages += 1
      }

      if ( page_num <= num_pages ) {
        file_descriptor.seek(page_num * Pager.PAGE_BYTES)
        val page_data = new Array[Byte](Pager.PAGE_BYTES)
        file_descriptor.read(page_data)
        this.pages(page_num) = Page(collection.mutable.ArrayBuffer(page_data: _*))
      }
    }

    this.pages(page_num)
  }

  def pager_flush ( page_num : Int , size : Int ) : Unit = {
    if ( this.pages(page_num) == null ) {
      println("Tried to flush null page")
      System.exit(-1)
    }

    file_descriptor.seek(page_num * Pager.PAGE_BYTES)
    file_descriptor.write(pages(page_num).data.toArray, 0 , size)
  }
}

case object Pager {
  val PAGE_BYTES    : Int = 4096
  val ROWS_PER_PAGE : Int = Pager.PAGE_BYTES / UserRow.Column.ROW_BYTES

  def pager_open ( path : Path ) : Pager = {
    val fd = path.toFile
    if ( ! Files.exists(path) ) { Files.createFile(path) }
    Pager(fd)
  }
}

case object Table {
  val TABLE_MAX_PAGES : Int = 100
  val TABLE_MAX_ROWS  : Int = Pager.ROWS_PER_PAGE * TABLE_MAX_PAGES
}

class Cursor ( val table : Table
             , var row_num : Int
             , var end_of_table : Boolean /* Indicates a position one past the last element */ )
{

  def cursor_value () : ( Page , Int ) = {
    val page_num    = this.row_num / Pager.ROWS_PER_PAGE
    val page        = table.pager.get_page(page_num)
    val row_offset  = row_num % Pager.ROWS_PER_PAGE
    val byte_offset = row_offset * UserRow.Column.ROW_BYTES
    ( page , byte_offset )
  }

  def cursor_advance () : Unit = {
    this.row_num += 1
    if ( this.row_num >= this.table.row_num ) {
      this.end_of_table = true
    }
  }
}

case class Table ( ) {

  val pager : Pager = Pager.pager_open(Paths.get("sqlite.db") )

  var row_num : Int = ( pager.file_descriptor.length() / UserRow.Column.ROW_BYTES ).toInt

  def row_slot ( row_num : Int ) : ( Page , Int ) = {
    val page_num = row_num / Pager.ROWS_PER_PAGE
    val page = pager.get_page(page_num)
    val row_offset = row_num % Pager.ROWS_PER_PAGE
    val byte_offset = row_offset * UserRow.Column.ROW_BYTES
    ( page , byte_offset )
  }

  def db_close () : Unit = {
    val num_full_pages = row_num / Pager.ROWS_PER_PAGE

    for ( i <- 0 until num_full_pages ) {
      if ( pager.pages(i) != null ) {
        pager.pager_flush(i, Pager.PAGE_BYTES)
        pager.pages(i) = null
      }
    }

    // There may be a partial page to write to the end of the file
    // This should not be needed after we switch to a B-tree
    val num_additional_rows = row_num % Pager.ROWS_PER_PAGE
    if ( num_additional_rows > 0 ) {
      val page_num = num_full_pages
      if ( pager.pages(page_num) != null ) {
        pager.pager_flush(page_num, num_additional_rows * UserRow.Column.ROW_BYTES)
        pager.pages(page_num) = null
      }
    }
  }

  def table_start () = new Cursor ( this , 0            , false )
  def table_end   () = new Cursor ( this , this.row_num , true  )
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
    import java.nio.ByteBuffer
    val id_arr    = ByteBuffer.allocate(4).putInt(row.id).array

    val user_arr  = ByteBuffer.allocate( UserRow.Column.USER_BYTES  ).put( row.user.getBytes  ).array
    val email_arr = ByteBuffer.allocate( UserRow.Column.EMAIL_BYTES ).put( row.email.getBytes ).array

    id_arr ++ user_arr ++ email_arr
  }

  def deserialze ( row : Array[Byte] ) : UserRow = {
    import java.nio.ByteBuffer
    val id    = row.slice( UserRow.Column.ID_OFFSET    , UserRow.Column.USER_OFFSET  )
    val user  = row.slice( UserRow.Column.USER_OFFSET  , UserRow.Column.EMAIL_OFFSET )
    val email = row.slice( UserRow.Column.EMAIL_OFFSET , row.length )
    UserRow( ByteBuffer.wrap(id).getInt , new String(user).trim , new String(email).trim )
  }
}

sealed trait Row
case class UserRow ( id    : Int    = 0
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
  case object NEGATIVE_ID            extends Result
  case object STRING_TOO_LONG        extends Result
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

  def do_meta_command ( command : String , table : Table ) : Unit = {
    if ( ".exit".equals(command) ) {
      table.db_close()
      System.exit ( 0 )
    } else {
      println(s"Unrecognized command '$command'")
    }
  }

  val INSERT_MATCHER : scala.util.matching.Regex = "insert (.*) (.*) (.*)".r
  val SELECT_MATCHER : scala.util.matching.Regex = "select (.*)".r

  def prepare_statement ( statement : String ) : ( PrepareStatement.Result , Option[Statement] ) = {
    if ( statement.startsWith("insert") ) {
      val row = INSERT_MATCHER.findFirstMatchIn(statement) match {
        case Some(v) =>
          val id    = Integer.parseInt(v.group(1))
          val user  = v.group(2)
          val email = v.group(3)

          if ( id < 0 ) { return ( PrepareStatement.NEGATIVE_ID , None ) }

          if ( ( user.length  > UserRow.Column.USER_BYTES ) || ( email.length > UserRow.Column.EMAIL_BYTES ) ) {
            return ( PrepareStatement.STRING_TOO_LONG , None )
          }

          UserRow(id,user,email)
        case _ => return ( PrepareStatement.UNRECOGNIZED_STATEMENT , None )
      }
      ( PrepareStatement.SUCCESS , Some(Statement(StatementType.INSERT, row)) )
    } else if ( statement.startsWith("select") ) {
      val row = SELECT_MATCHER.findFirstMatchIn(statement) match {
        case Some(_) => UserRow()
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
    val cursor = table.table_end()
    val ( page , slot ) = cursor.cursor_value()
    val bytes = UserRow.serialize(row_to_insert)
    page.data.insertAll(slot,bytes)
    table.row_num += 1

    ExecuteStatement.SUCCESS
  }

  def execute_select ( statement : Statement , table : Table ) : ExecuteStatement.Result = {
    val cursor = table.table_start()
    while ( ! cursor.end_of_table ) {
      val ( page , slot ) = cursor.cursor_value()
      val eob = slot + UserRow.Column.ROW_BYTES
      val row = UserRow.deserialze(page.data.slice(slot,eob).toArray)
      println(row)
      cursor.cursor_advance()
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

      try {
        val scanner = read_input()

        val token = scanner.next

        if ( token.startsWith(".") ) {
          do_meta_command(token , table)
        }

        val line = token + scanner.nextLine

        prepare_statement(line) match {
          case ( PrepareStatement.SUCCESS , Some(statement) ) =>
            execute_statement(statement, table) match {
              case ExecuteStatement.SUCCESS    => println( "Executed."   )
              case ExecuteStatement.TABLE_FULL => println( "Table full!" )
            }
          case ( PrepareStatement.NEGATIVE_ID            , None ) => println(s"ID must be positive.")
          case ( PrepareStatement.STRING_TOO_LONG        , None ) => println(s"String is too long.")
          case ( PrepareStatement.SYNTAX_ERROR           , None ) => println(s"Unrecognized keyword at start of '$line'.")
          case ( PrepareStatement.UNRECOGNIZED_STATEMENT , None ) => println(s"Unrecognized statement '$line'.")
          case _ => println(s"Error: '$line'.")
        }
      } catch {
        case e : Exception => println(s"Error: '${e.getMessage}'.")
      }
    }
  }
}
