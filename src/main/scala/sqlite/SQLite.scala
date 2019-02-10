package sqlite

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}

import sqlite.NodeType.NodeType

import scala.collection.mutable.ArrayBuffer

case object NodeType {
  sealed trait NodeType
  case object INTERNAL extends NodeType
  case object LEAF     extends NodeType
}

/* Common Node Header Layout */
case object NodeHeaderLayout {
  val NODE_TYPE_BYTES       : Int = 1
  val NODE_TYPE_OFFSET      : Int = 0
  val IS_ROOT_BYTES         : Int = 1
  val IS_ROOT_OFFSET        : Int = NODE_TYPE_BYTES
  val PARENT_POINTER_BYTES  : Int = 4
  val PARENT_POINTER_OFFSET : Int = IS_ROOT_OFFSET + IS_ROOT_BYTES
  val HEADER_BYTES          : Int = NODE_TYPE_BYTES + IS_ROOT_BYTES + PARENT_POINTER_BYTES
}

/* Leaf Node Header Layout */
case object LeafNodeHeaderLayout {
  val NUM_CELLS_BYTES  : Int = 4
  val NUM_CELLS_OFFSET : Int = NodeHeaderLayout.HEADER_BYTES
  val HEADER_BYTES     : Int = NodeHeaderLayout.HEADER_BYTES + NUM_CELLS_BYTES
}

/* Leaf Node Body Layout */
case object LeafNodeBodyLayout {
  val KEY_BYTES       : Int = 4
  val KEY_OFFSET      : Int = 0
  val VALUE_BYTES     : Int = UserRow.Column.ROW_BYTES
  val VALUE_OFFSET    : Int = KEY_OFFSET + KEY_BYTES
  val CELL_BYTES      : Int = KEY_BYTES + VALUE_BYTES
  val SPACE_FOR_CELLS : Int = Pager.PAGE_BYTES - NodeHeaderLayout.HEADER_BYTES
  val MAX_CELLS       : Int = SPACE_FOR_CELLS / CELL_BYTES
}

case class Node ( node_type : NodeType ) {

  private val bytes : Int = LeafNodeHeaderLayout.HEADER_BYTES + LeafNodeBodyLayout.SPACE_FOR_CELLS

  val data : ArrayBuffer[Byte] = ArrayBuffer[Byte]().padTo(bytes, 0.asInstanceOf[Byte])

  def num_cells () : Int = {
    val start = LeafNodeHeaderLayout.NUM_CELLS_OFFSET
    val end   = start + LeafNodeHeaderLayout.NUM_CELLS_BYTES
    val bytes = data.slice(start,end).toArray
    ByteBuffer.wrap(bytes).getInt
  }

  def incr_num_cells () : Int = {
    val start      = LeafNodeHeaderLayout.NUM_CELLS_OFFSET
    val end        = start + LeafNodeHeaderLayout.NUM_CELLS_BYTES
    val bytes      = data.slice(start, end).toArray
    val next       = ByteBuffer.wrap(bytes).getInt + 1
    val next_bytes = ByteBuffer.allocate(4).putInt(next).array()

    for {
      ( index , i ) <- ( start until end ) zip ( next_bytes.indices )
    } yield {
      data.update(index, next_bytes(i))
    }
    next
  }

  def cell ( cell_num : Int ) : Int = LeafNodeHeaderLayout.HEADER_BYTES + ( cell_num * LeafNodeBodyLayout.CELL_BYTES )

  def key ( cell_num : Int ) : Int = {
    val start = cell(cell_num)
    val end   = start + LeafNodeBodyLayout.KEY_BYTES
    val bytes = data.slice(start,end).toArray
    ByteBuffer.wrap(bytes).getInt
  }

  def value ( cell_num : Int ) : Array[Byte] = {
    val start = cell(cell_num) + LeafNodeBodyLayout.KEY_BYTES
    val end   = start + LeafNodeBodyLayout.VALUE_BYTES
    data.slice(start,end).toArray
  }

  def value ( cell_num : Int , key : Int , value : Array[Byte] ) : Unit = {
    val key_offset = cell ( cell_num )
    val val_offset = key_offset + LeafNodeBodyLayout.KEY_BYTES

    val key_bytes = ByteBuffer.allocate(LeafNodeBodyLayout.KEY_BYTES).putInt(key).array()
    for {
      ( index , i ) <- ( key_offset until ( key_offset + LeafNodeBodyLayout.KEY_BYTES ) ) zip ( 0 until key_bytes.size )
    } yield {
      data.update(index, key_bytes(i))
    }

    for {
      ( index , i ) <- ( val_offset until ( val_offset + value.length ) ) zip ( value.indices )
    } yield {
      data.update(index, value(i))
    }
  }

  override def toString : String = {
    val sb = new StringBuffer()
    sb.append(f"leaf (size ${num_cells()})")
    for ( i <- 0 until num_cells ) {
      sb.append(f" - $i : ${key(i)}")
    }
    sb.toString
  }
}

case object Node {

  def initialize_leaf_node () : Node = Node ( NodeType.LEAF )

  def print_constants () : Unit = {
    println(f"ROW_BYTES: ${UserRow.Column.ROW_BYTES}")
    println(f"COMMON_NODE_HEADER_BYTES: ${NodeHeaderLayout.HEADER_BYTES}")
    println(f"LEAF_NODE_HEADER_BYTES: ${LeafNodeHeaderLayout.HEADER_BYTES}")
    println(f"LEAF_NODE_CELL_BYTES: ${LeafNodeBodyLayout.CELL_BYTES}")
    println(f"LEAF_NODE_SPACE_FOR_CELLS: ${LeafNodeBodyLayout.SPACE_FOR_CELLS}")
    println(f"LEAF_NODE_MAX_CELLS: ${LeafNodeBodyLayout.MAX_CELLS}")
  }

  def print_leaf_node ( node : Node ) : Unit = {
    val num_cells = node.num_cells()
    println(s"leaf (size $num_cells)")
    for ( i <- 0 until num_cells ) {
      val key = node.key(i)
      println(s"  - $i : $key")
    }
  }
}

case class Page ( node : Node = Node.initialize_leaf_node() )

case class Pager ( file : File ) {

  val file_descriptor : RandomAccessFile = new RandomAccessFile(file, "rw")

  var pages : ArrayBuffer[sqlite.Page] = ArrayBuffer[sqlite.Page](Page ())

  def get_page ( page_num : Int ) : Page = {
    if ( page_num >= this.pages.size ) {
      // Allocate memory only when we try to access new page
      this.pages += Page ()
    }

    if ( this.pages(page_num).node.num_cells() == 0 ) {
      // Cache miss. Allocate memory and load from file.
      var num_pages = file_descriptor.length() / Pager.PAGE_BYTES

      // We might save a partial page at the end of the file
      if ( file_descriptor.length() % Pager.PAGE_BYTES > 0 ) {
        num_pages += 1
      }

      if ( page_num <= num_pages ) {
        file_descriptor.seek(page_num * Pager.PAGE_BYTES)
        val page_data = new Array[Byte](Pager.PAGE_BYTES)
        file_descriptor.read(page_data)
        this.pages(page_num) = Page ()
      }
    }

    this.pages(page_num)
  }

  def pager_flush ( page_num : Int ) : Unit = {
    if ( this.pages(page_num) == null ) {
      println("Tried to flush null page")
      System.exit(-1)
    }

    file_descriptor.seek(page_num * Pager.PAGE_BYTES)
    file_descriptor.write(pages(page_num).node.data.toArray, 0 , Pager.PAGE_BYTES)
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
             , var page_num : Int
             , var cell_num : Int
             , var end_of_table : Boolean /* Indicates a position one past the last element */ )
{

  def cursor_value () : ( Page , Int ) = {
    val page = table.pager.get_page(page_num)
    ( page , this.cell_num )
  }

  def cursor_advance () : Unit = {
    val page = table.pager.get_page(page_num)
    this.cell_num += 1
    if ( cell_num >= page.node.num_cells() ) {
      this.end_of_table = true
    }
  }
}

case class Table ( node : Node = Node.initialize_leaf_node() ) {

  val pager : Pager = Pager.pager_open(Paths.get("sqlite.db") )

  var root_page_num : Int = 0

  def db_close () : Unit = {
    for ( i <- pager.pages.indices ) {
      if ( pager.pages(i) != null ) {
        pager.pager_flush(i)
        pager.pages(i) = null
      }
    }
  }

  def table_start () : Cursor = {
    val page_num = this.root_page_num
    val cell_num = 0

    val page = pager.get_page(this.root_page_num)
    val num_cells = page.node.num_cells()
    val end_of_table = num_cells == 0

    new Cursor ( this , page_num , cell_num  , end_of_table )
  }

  def table_end   () : Cursor = {
    val page_num = this.root_page_num

    val page = pager.get_page(this.root_page_num)
    val num_cells = page.node.num_cells()
    val cell_num = num_cells

    new Cursor ( this , page_num , cell_num , true  )
  }
}

object StatementType {
  sealed trait StatementType
  case object INSERT extends StatementType
  case object SELECT extends StatementType
}

case object UserRow {
  case object Column {
    val ID_BYTES     : Int = 4
    val USER_BYTES   : Int = 145
    val EMAIL_BYTES  : Int = 145

    val ID_OFFSET    : Int = 0
    val USER_OFFSET  : Int = ID_OFFSET + ID_BYTES
    val EMAIL_OFFSET : Int = USER_OFFSET + USER_BYTES

    val ROW_BYTES    : Int = ID_BYTES + USER_BYTES + EMAIL_BYTES
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

  def do_meta_command ( command : String , table : Table ) : Unit = {
    if ( ".exit".equals(command) ) {
      table.db_close()
      System.exit ( 0 )
    } else if ( ".btree".equals(command) ) {
      println("Tree:")
      Node.print_leaf_node(table.pager.get_page(0).node)
    } else if ( ".constants".equals(command) ) {
      println("Constants:")
      Node.print_constants()
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

  def execute_insert ( statement : Statement , cursor : Cursor ) : ExecuteStatement.Result = {
    val node = cursor.table.pager.get_page(cursor.table.root_page_num).node

    val num_cells = node.num_cells()

    if ( num_cells > LeafNodeBodyLayout.MAX_CELLS ) { ExecuteStatement.TABLE_FULL }

    node.incr_num_cells()

    val key = statement.row.id
    val row = UserRow.serialize(statement.row)
    node.value( cursor.cell_num , key , row )

    ExecuteStatement.SUCCESS
  }

  def execute_select ( statement : Statement , table : Table ) : ExecuteStatement.Result = {
    val cursor = table.table_start()
    while ( ! cursor.end_of_table ) {
      val ( page , slot ) = cursor.cursor_value()
      val row = UserRow.deserialze(page.node.value(cursor.cell_num))
      println(row)
      cursor.cursor_advance()
    }
    ExecuteStatement.SUCCESS
  }

  def execute_statement ( statement : Statement , cursor : Cursor ) : ExecuteStatement.Result = {
    statement.statement_type match {
      case StatementType.INSERT => execute_insert(statement , cursor )
      case StatementType.SELECT => execute_select(statement , cursor.table )
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
    val cursor = Table().table_start()

    while ( true ) {
      print_prompt()

      try {
        val scanner = read_input()

        val token = scanner.next

        if ( token.startsWith(".") ) {
          do_meta_command(token , cursor.table)
        } else {
          val line = token + scanner.nextLine

          prepare_statement(line) match {
            case (PrepareStatement.SUCCESS, Some(statement)) =>
              execute_statement(statement, cursor) match {
                case ExecuteStatement.SUCCESS => println("Executed.")
                case ExecuteStatement.TABLE_FULL => println("Table full!")
              }
            case (PrepareStatement.NEGATIVE_ID, None) => println(s"ID must be positive.")
            case (PrepareStatement.STRING_TOO_LONG, None) => println(s"String is too long.")
            case (PrepareStatement.SYNTAX_ERROR, None) => println(s"Unrecognized keyword at start of '$line'.")
            case (PrepareStatement.UNRECOGNIZED_STATEMENT, None) => println(s"Unrecognized statement '$line'.")
            case _ => println(s"Error: '$line'.")
          }
        }
      } catch {
        case e : Exception => println(s"Error: '${e.getMessage}'.")
      }
    }
  }
}
