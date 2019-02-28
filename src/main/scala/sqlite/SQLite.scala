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
  val KEY_BYTES         : Int = 4
  val KEY_OFFSET        : Int = 0
  val VALUE_BYTES       : Int = UserRow.Column.ROW_BYTES
  val VALUE_OFFSET      : Int = KEY_OFFSET + KEY_BYTES
  val CELL_BYTES        : Int = KEY_BYTES + VALUE_BYTES
  val SPACE_FOR_CELLS   : Int = Pager.PAGE_BYTES - NodeHeaderLayout.HEADER_BYTES
  val MAX_CELLS         : Int = SPACE_FOR_CELLS / CELL_BYTES
  val RIGHT_SPLIT_COUNT : Int = ( MAX_CELLS + 1 ) / 2
  val LEFT_SPLIT_COUNT  : Int = ( MAX_CELLS + 1 ) - RIGHT_SPLIT_COUNT
}

case object InternalNodeHeaderLayout {
  val NUM_KEYS_SIZE      : Int = 4
  val NUM_KEYS_OFFSET    : Int = NodeHeaderLayout.HEADER_BYTES
  val RIGHT_CHILD_SIZE   : Int = 4
  val RIGHT_CHILD_OFFSET : Int = NUM_KEYS_OFFSET + NUM_KEYS_SIZE
  val HEADER_SIZE        : Int = NodeHeaderLayout.HEADER_BYTES + NUM_KEYS_SIZE + RIGHT_CHILD_SIZE
}

case object InternalNodeBodyLayout {
  val KEY_SIZE   : Int = 4
  val CHILD_SIZE : Int = 4
  val CELL_SIZE  : Int = CHILD_SIZE + KEY_SIZE
}

case class Node ( node_type : NodeType
                , data : ArrayBuffer[Byte] = ArrayBuffer[Byte]().padTo(Node.bytes, 0.asInstanceOf[Byte]) ) {

  def num_cells () : Int = {
    val start = LeafNodeHeaderLayout.NUM_CELLS_OFFSET
    val end   = start + LeafNodeHeaderLayout.NUM_CELLS_BYTES
    val bytes = data.slice(start,end).toArray
    ByteBuffer.wrap(bytes).getInt
  }

  def num_cells ( num : Int ) : Unit = {
    val start      = LeafNodeHeaderLayout.NUM_CELLS_OFFSET
    val end        = start + LeafNodeHeaderLayout.NUM_CELLS_BYTES
    val next_bytes = ByteBuffer.allocate(4).putInt(num).array()

    for {
      ( index , i ) <- ( start until end ) zip next_bytes.indices
    } yield {
      data.update(index, next_bytes(i))
    }
  }

  def incr_num_cells () : Int = {
    val start      = LeafNodeHeaderLayout.NUM_CELLS_OFFSET
    val end        = start + LeafNodeHeaderLayout.NUM_CELLS_BYTES
    val bytes      = data.slice(start, end).toArray
    val next       = ByteBuffer.wrap(bytes).getInt + 1
    val next_bytes = ByteBuffer.allocate(4).putInt(next).array()

    for {
      ( index , i ) <- ( start until end ) zip next_bytes.indices
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

  def key ( cell_num : Int , key : Int ) : Unit = {
    val key_offset = cell ( cell_num )

    val key_bytes = ByteBuffer.allocate(LeafNodeBodyLayout.KEY_BYTES).putInt(key).array()
    for {
      ( index , i ) <- ( key_offset until ( key_offset + LeafNodeBodyLayout.KEY_BYTES ) ) zip ( 0 until key_bytes.size )
    } yield {
      data.update(index, key_bytes(i))
    }
  }

  def value ( cell_num : Int ) : Array[Byte] = {
    val start = cell(cell_num) + LeafNodeBodyLayout.KEY_BYTES
    val end   = start + LeafNodeBodyLayout.VALUE_BYTES
    data.slice(start,end).toArray
  }

  def value ( cell_num : Int , value : Array[Byte] ) : Unit = {
    val key_offset = cell ( cell_num )
    val val_offset = key_offset + LeafNodeBodyLayout.KEY_BYTES

    for {
      ( index , i ) <- ( val_offset until ( val_offset + value.length ) ) zip value.indices
    } yield {
      data.update(index, value(i))
    }
  }

  def key_and_value ( cell_num : Int , key : Int , value : Array[Byte] ) : Unit = {
    this.key   ( cell_num , key   )
    this.value ( cell_num , value )
  }

  def set_node_root ( is_root : Boolean ) : Unit = {
    val byte_val = if ( is_root ) 1.asInstanceOf[Byte] else 0.asInstanceOf[Byte]
    this.data.update(NodeHeaderLayout.IS_ROOT_OFFSET, byte_val)
  }

  def num_keys ( ) : Int = {
    val from  = InternalNodeHeaderLayout.NUM_KEYS_OFFSET
    val until = from + InternalNodeHeaderLayout.NUM_KEYS_SIZE
    val bytes = this.data.slice(from,until).toArray
    ByteBuffer.wrap(bytes).getInt
  }

  def num_keys ( keys : Int ) : Int = {
    val size   = InternalNodeHeaderLayout.NUM_KEYS_SIZE
    val bytes  = ByteBuffer.allocate(size).putInt(keys).array()
    val offset = InternalNodeHeaderLayout.NUM_KEYS_OFFSET
    bytes.foldLeft ( offset ) { ( index , byte ) => this.data.update ( index , byte ) ; index + 1 }
  }

  def right_child () : Int = {
    val from  = InternalNodeHeaderLayout.RIGHT_CHILD_OFFSET
    val until = from + InternalNodeHeaderLayout.RIGHT_CHILD_SIZE
    val bytes = this.data.slice(from,until).toArray
    ByteBuffer.wrap(bytes).getInt
  }

  def right_child ( value : Int ) : Unit = {
    val size   = InternalNodeHeaderLayout.RIGHT_CHILD_SIZE
    val bytes  = ByteBuffer.allocate(size).putInt(value).array()
    val offset = InternalNodeHeaderLayout.RIGHT_CHILD_OFFSET
    bytes.foldLeft ( offset ) { ( index , byte ) => this.data.update ( index , byte ) ; index + 1 }
  }

  def child ( child_num : Int ) : Int = {
    val num_keys = this.num_keys()
    if ( child_num > num_keys ) {
      throw new RuntimeException(s"Tried to access child_num $child_num > num_keys $num_keys")
    } else if (child_num == num_keys) {
      right_child()
    } else {
      cell(child_num)
    }
  }

  def max_key ( ) : Int = {
    this.node_type match {
      case NodeType.INTERNAL => this.key ( this.num_keys  () - 1 )
      case NodeType.LEAF     => this.key ( this.num_cells () - 1 )
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

  val bytes : Int = LeafNodeHeaderLayout.HEADER_BYTES + LeafNodeBodyLayout.SPACE_FOR_CELLS

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

  var pages : ArrayBuffer[sqlite.Page] = ArrayBuffer[sqlite.Page]()

  def get_unused_page_num() : Int = pages.length

  def create_new_root ( table : Table , right_child_page_num : Int ) : Unit = {
    /*
     * Handle splitting the root.
     * Old root copied to new page, becomes left child.
     * Address of right child passed in.
     * Re-initialize root page to contain the new root node.
     * New root node points to two children.
     */
    val root_node           = table.pager.get_page(table.root_page_num).node
    val right_child_node    = table.pager.get_page(right_child_page_num).node
    val left_child_page_num = table.pager.get_unused_page_num()
    val left_child_node     = table.pager.get_page(left_child_page_num).node

    /* Left child has data copied from old root */
    val root_bytes = root_node.data.slice(0, Pager.PAGE_BYTES)
    root_bytes.foldLeft ( 0 ) { (index,byte) => left_child_node.data.update ( index , byte ) ; index + 1 }
    left_child_node.set_node_root(false)

    /* Root node is a new internal node with one key and two children */
    root_node.set_node_root(true)
    root_node.num_keys(1)
    root_node.child(0)
    val left_child_max_key = left_child_node.max_key()
    root_node.key(0, left_child_max_key)
    root_node.right_child(right_child_page_num)
  }

  def get_page ( page_num : Int ) : Page = {
    if ( this.pages.isEmpty || ( this.pages.length >= page_num && this.pages(page_num) == null ) ) {
      // Cache miss. Allocate memory and load from file.
      var num_pages = file_descriptor.length() / Pager.PAGE_BYTES

      // We might save a partial page at the end of the file
      if ( file_descriptor.length() % Pager.PAGE_BYTES > 0 ) {
        num_pages += 1
      }

      var page_data = ArrayBuffer[Byte]()
      if ( page_num <= num_pages ) {
        file_descriptor.seek(page_num * Pager.PAGE_BYTES)
        val data = new Array[Byte](Pager.PAGE_BYTES)
        file_descriptor.read(data)
        page_data = ArrayBuffer[Byte](data:_*)
      }

      if ( this.pages.length <= page_num ) {
        this.pages += Page ( Node ( NodeType.LEAF , page_data ) )
      } else {
        this.pages(page_num) = Page ( Node ( NodeType.LEAF , page_data ) )
      }

      if ( page_num >= num_pages ) {
        num_pages = page_num + 1
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
             , var end_of_table : Boolean /* Indicates a position one past the last element */ ) {

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

case class Table () {

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

  /**
    * Return the position of the given key. If the key is not present, return the position where it should be inserted
    */
  def table_find ( key : Int ) : Cursor = {
    val root_page_num = this.root_page_num
    val root_node = pager.get_page(root_page_num).node

    if ( root_node.node_type == NodeType.LEAF ) {
      leaf_node_find(root_page_num, key)
    } else {
      println("Need to implement searching an internal node")
      null /* code replaced system.exit -1 */
    }
  }

  def leaf_node_find ( page_num : Int , key : Int ) : Cursor = {
    val page = pager.get_page ( page_num )
    val num_cells = page.node.num_cells()

    val cursor = new Cursor ( this , page_num , num_cells , false )

    // Binary search
    var min_index = 0
    var one_past_max_index = num_cells
    while ( one_past_max_index != min_index ) {
      val index = ( min_index + one_past_max_index ) / 2
      val key_at_index = page.node.key(index)
      if ( key == key_at_index ) {
        cursor.cell_num = index
        return cursor
      }
      if ( key < key_at_index ) {
        one_past_max_index = index
      } else {
        min_index = index + 1
      }
    }

    cursor.cell_num = min_index
    cursor
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
  case object SUCCESS       extends Result
  case object DUPLICATE_KEY extends Result
  case object TABLE_FULL    extends Result
}

class SQLite {
  def void () : Boolean = true
}

object SQLite {

  def do_meta_command(command: String, table: Table): Unit = {
    if (".exit".equals(command)) {
      table.db_close()
      System.exit(0)
    } else if (".btree".equals(command)) {
      println("Tree:")
      Node.print_leaf_node(table.pager.get_page(0).node)
    } else if (".constants".equals(command)) {
      println("Constants:")
      Node.print_constants()
    } else {
      println(s"Unrecognized command '$command'")
    }
  }

  val INSERT_MATCHER: scala.util.matching.Regex = "insert (.*) (.*) (.*)".r
  val SELECT_MATCHER: scala.util.matching.Regex = "select (.*)".r

  def prepare_statement(statement: String): (PrepareStatement.Result, Option[Statement]) = {
    if (statement.startsWith("insert")) {
      val row = INSERT_MATCHER.findFirstMatchIn(statement) match {
        case Some(v) =>
          val id = Integer.parseInt(v.group(1))
          val user = v.group(2)
          val email = v.group(3)

          if (id < 0) {
            return (PrepareStatement.NEGATIVE_ID, None)
          }

          if ((user.length > UserRow.Column.USER_BYTES) || (email.length > UserRow.Column.EMAIL_BYTES)) {
            return (PrepareStatement.STRING_TOO_LONG, None)
          }

          UserRow(id, user, email)
        case _ => return (PrepareStatement.UNRECOGNIZED_STATEMENT, None)
      }
      (PrepareStatement.SUCCESS, Some(Statement(StatementType.INSERT, row)))
    } else if (statement.startsWith("select")) {
      val row = SELECT_MATCHER.findFirstMatchIn(statement) match {
        case Some(_) => UserRow()
        case _ => return (PrepareStatement.UNRECOGNIZED_STATEMENT, None)
      }
      (PrepareStatement.SUCCESS, Some(Statement(StatementType.SELECT, row)))
    } else {
      (PrepareStatement.SYNTAX_ERROR, None)
    }
  }

  def leaf_node_cell ( node : Node , cell_num : Int ) : Int =
    node.cell(LeafNodeHeaderLayout.HEADER_BYTES + cell_num * LeafNodeBodyLayout.CELL_BYTES)

  def is_node_root ( node : Node ) : Boolean = node.data(NodeHeaderLayout.IS_ROOT_OFFSET) == 0

  private def leaf_node_split_and_insert ( cursor : Cursor, key : Int, row : UserRow ) : Unit = {
    /*
     * Create a new node and move half the cells over.
     * Insert the new value in one of the two nodes.
     * Update parent or create a new parent.
     */
    val old_node     = cursor.table.pager.get_page(cursor.page_num).node
    val new_page_num = cursor.table.pager.get_unused_page_num()
    val new_node     = cursor.table.pager.get_page(new_page_num).node

    /*
     * All existing keys plus new key should be divided
     * evenly between old (left) and new (right) nodes.
     * Starting from the right, move each key to correct position.
     */
    for ( i <- LeafNodeBodyLayout.MAX_CELLS until 0 by -1 ) {
      var destination_node : Node = null
      if ( i >= LeafNodeBodyLayout.LEFT_SPLIT_COUNT ) {
        destination_node = new_node
      } else {
        destination_node = old_node
      }

      val index_within_node = i % LeafNodeBodyLayout.LEFT_SPLIT_COUNT
      val destination = leaf_node_cell(destination_node, index_within_node)

      if ( i == cursor.cell_num ) {
        val serialized_row = UserRow.serialize(row)
      } else if ( i > cursor.cell_num ) {
        val src_cell_from  = leaf_node_cell(old_node, i - 1)
        val src_cell_until = src_cell_from + LeafNodeBodyLayout.CELL_BYTES
        val src_bytes      = old_node.data.slice(src_cell_from, src_cell_until)

        src_bytes.foldLeft ( destination ) { (index,byte) => old_node.data.update ( index , byte ) ; index + 1 }
//        memcpy(destination, leaf_node_cell(old_node, i - 1), LeafNodeBodyLayout.CELL_BYTES)
      } else {
        val src_cell_from  = leaf_node_cell(old_node, i)
        val src_cell_until = src_cell_from + LeafNodeBodyLayout.CELL_BYTES
        val src_bytes      = old_node.data.slice(src_cell_from, src_cell_until)

        src_bytes.foldLeft ( destination ) { (index,byte) => old_node.data.update ( index , byte ) ; index + 1 }
//        memcpy(destination, leaf_node_cell(old_node, i), LeafNodeBodyLayout.CELL_BYTES)
      }
    }

    /* Update cell count on both leaf nodes */
    old_node.num_cells( LeafNodeBodyLayout.LEFT_SPLIT_COUNT  )
    new_node.num_cells( LeafNodeBodyLayout.RIGHT_SPLIT_COUNT )

    if ( is_node_root(old_node) ) {
      cursor.table.pager.create_new_root(cursor.table, new_page_num)
    } else {
      println("Need to implement updating parent after split")
      System.exit(-1)
    }
  }

  private def leaf_node_insert ( cursor : Cursor , key : Int , row : UserRow ) : Unit = {
    val node = cursor.table.pager.get_page(cursor.page_num).node

    val num_cells = node.num_cells()
    if ( num_cells >= LeafNodeBodyLayout.MAX_CELLS ) {
      leaf_node_split_and_insert(cursor, key, row)
    }

    if ( cursor.cell_num < num_cells ) {
      // Make room for new cell
      for ( i <- num_cells until cursor.cell_num by -1 ) {
        val dest = node.cell(i)
        val src  = node.cell(i - 1)

        val src_bytes = node.data.slice(src, src + LeafNodeBodyLayout.CELL_BYTES)

        src_bytes.foldLeft ( dest ) { (index,byte) => node.data.update ( index , byte ) ; index + 1 }
      }
    }

    node.incr_num_cells()

    node.key_and_value( cursor.cell_num , key , UserRow.serialize(row) )
  }

  def execute_insert ( statement : Statement , table : Table ) : ExecuteStatement.Result = {
    val node      = table.pager.get_page(table.root_page_num).node
    val num_cells = node.num_cells()

    val row_to_insert = statement.row
    val key_to_insert = row_to_insert.id

    val cursor = table.table_find(key_to_insert)

    if ( cursor.cell_num < num_cells ) {
      val key_at_index = node.key(cursor.cell_num)
      if ( key_at_index == key_to_insert ) {
        return ExecuteStatement.DUPLICATE_KEY
      }
    }

    leaf_node_insert(cursor, key_to_insert, row_to_insert)

    ExecuteStatement.SUCCESS
  }

  def execute_select ( statement : Statement , table : Table ) : ExecuteStatement.Result = {
    val cursor = table.table_start()
    while ( ! cursor.end_of_table ) {
      val ( page , _ ) = cursor.cursor_value()
      val row = UserRow.deserialze(page.node.value(cursor.cell_num))
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
        } else {
          val line = token + scanner.nextLine

          prepare_statement(line) match {
            case (PrepareStatement.SUCCESS, Some(statement)) =>
              execute_statement(statement, table) match {
                case ExecuteStatement.SUCCESS       => println( "Executed."             )
                case ExecuteStatement.DUPLICATE_KEY => println( "Error: Duplicate key." )
                case ExecuteStatement.TABLE_FULL    => println( "Table full!"           )
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