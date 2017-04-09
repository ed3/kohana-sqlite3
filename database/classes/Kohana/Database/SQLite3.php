<?php
/**
 * SQLite3 database connection.
 *
 * @package    Kohana/Database
 * @category   Drivers
 * @author     Ed [https://github.com/ed3/kohana-sqlite3]
 * @copyright  (c) Kohana Team
 * @license    https://koseven.ga/LICENSE.md
 */
class Kohana_Database_SQLite3 extends Database {

	// Database in use by each connection
	protected static $_current_databases = array();

	// Use SET NAMES to set the character set
	protected static $_set_names;

	// Identifier for this connection within the PHP driver
	protected $_connection_id;

	// MySQL uses a backtick for identifiers
	protected $_identifier = '"';

	public function connect()
	{
		$database = Arr::get($this->_config, 'connection', NULL);

		if (empty($database['database']))
		{
			throw new Database_Exception('Database path not available in Kohana Database configuration');
		}

		// Load new SQLite3 DB
		$this->_connection = new SQLite3($database['database']);

		if (Database_SQLite3::$_set_names === NULL)
		{
			Database_SQLite3::$_set_names = ! function_exists('mysqli_set_charset');
		}

		// Extract the connection parameters, adding required variabels
		extract($this->_config['connection'] + array(
			'database' => '',
			'persistent' => FALSE,
		));

		// Prevent this information from showing up in traces
		unset($this->_config['connection']['database']);

		$this->_connection_id = sha1($database);

		if ( ! empty($this->_config['charset']))
		{
			// Set the character set
			$this->set_charset($this->_config['charset']);
		}

		if ( ! empty($this->_config['connection']['variables']))
		{
			// Set session variables
			$variables = array();

			foreach ($this->_config['connection']['variables'] as $var => $val)
			{
				$variables[] = 'SESSION '.$var.' = '.$val;
			}

			$this->_connection->query('SET '.implode(', ', $variables));
		}
	}

	public function disconnect()
	{
		try
		{
			// Database is assumed disconnected
			$status = TRUE;

			if (is_resource($this->_connection))
			{
				if ($status = $this->_connection->close())
				{
					// Clear the connection
					$this->_connection = NULL;

					// Clear the instance
					parent::disconnect();
				}
			}
		}
		catch (Exception $e)
		{
			// Database is probably not disconnected
			$status = ! is_resource($this->_connection);
		}

		return $status;
	}

	public function set_charset($charset)
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();

		if (Database_SQLite3::$_set_names == NULL)
		{
			$status = (bool) $this->_connection->query('PRAGMA encoding="'.$charset.'"');
		}
		else
		{
			$status = $this->_connection->set_charset('PRAGMA encoding="UTF-8"');
		}

		if ($status === FALSE)
		{
			throw new Database_Exception(':error', array(':error' => $this->_connection->lastErrorMsg()), $this->_connection->lastErrorCode());
		}
	}

	public function query($type, $sql, $as_object = FALSE, array $params = NULL)
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();

		if (Kohana::$profiling)
		{
			// Benchmark this query for the current instance
			$benchmark = Profiler::start("Database ({$this->_instance})", $sql);
		}

		// Execute the query
		if (($result = $this->_connection->query($sql)) === FALSE)
		{
			if (isset($benchmark))
			{
				// This benchmark is worthless
				Profiler::delete($benchmark);
			}

			throw new Database_Exception(':error [ :query ]', array(
				':error' => $this->_connection->lastErrorMsg(),
				':query' => $sql
			), $this->_connection->lastErrorCode());
		}

		if (isset($benchmark))
		{
			Profiler::stop($benchmark);
		}

		// Set the last query
		$this->last_query = $sql;

		if ($type === Database::SELECT)
		{
			// Return an iterator of results
			return new Database_SQLite3_Result($result, $sql, $as_object, $params);
		}
		elseif ($type === Database::INSERT)
		{
			// Return a list of insert id and rows created
			return array(
				$this->_connection->lastInsertRowID(),
				$this->_connection->changes(),
			);
		}
		else
		{
			// Return the number of rows affected
			return $this->_connection->changes();
		}
	}

	public function datatype($type)
	{
		static $types = array
		(
			'blob'                      => array('type' => 'string', 'binary' => TRUE, 'character_maximum_length' => '65535'),
			'bool'                      => array('type' => 'bool'),
			'bigint'   			        => array('type' => 'int', 'min' => '0', 'max' => '18446744073709551615'),
			'datetime'                  => array('type' => 'string'),
			'decimal'			        => array('type' => 'float', 'exact' => TRUE, 'min' => '0'),
			'double'                    => array('type' => 'float'),
			'int'			            => array('type' => 'int', 'min' => '0', 'max' => '4294967295'),
			'integer'    			    => array('type' => 'int', 'min' => '0', 'max' => '4294967295'),
			'longblob'                  => array('type' => 'string', 'binary' => TRUE, 'character_maximum_length' => '4294967295'),
			'longtext'                  => array('type' => 'string', 'character_maximum_length' => '4294967295'),
			'mediumblob'                => array('type' => 'string', 'binary' => TRUE, 'character_maximum_length' => '16777215'),
			'mediumint'                 => array('type' => 'int', 'min' => '-8388608', 'max' => '8388607'),
			'mediumtext'                => array('type' => 'string', 'character_maximum_length' => '16777215'),
			'numeric'		            => array('type' => 'float', 'exact' => TRUE, 'min' => '0'),
			'varchar'                   => array('type' => 'string'),
			'point'                     => array('type' => 'string', 'binary' => TRUE),
			'real unsigned'             => array('type' => 'float', 'min' => '0'),
			'set'                       => array('type' => 'string'),
			'text'                      => array('type' => 'string', 'character_maximum_length' => '65535'),
			'tinyblob'                  => array('type' => 'string', 'binary' => TRUE, 'character_maximum_length' => '255'),
			'tinyint'                   => array('type' => 'int', 'min' => '-128', 'max' => '127'),
			'tinyint unsigned'          => array('type' => 'int', 'min' => '0', 'max' => '255'),
			'tinytext'                  => array('type' => 'string', 'character_maximum_length' => '255'),
		);

		$type = str_replace(' zerofill', '', $type);

		if (isset($types[$type]))
			return $types[$type];

		return parent::datatype($type);
	}

	/**
	 * Start a SQL transaction
	 *
	 * @param string $mode  Isolation level
	 * @return boolean
	 */
	public function begin($mode = NULL)
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();

		return (bool) $this->_connection->query('BEGIN TRANSACTION');
	}

	/**
	 * Commit a SQL transaction
	 *
	 * @return boolean
	 */
	public function commit()
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();

		return (bool) $this->_connection->query('COMMIT');
	}

	/**
	 * Rollback a SQL transaction
	 *
	 * @return boolean
	 */
	public function rollback()
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();

		return (bool) $this->_connection->query('ROLLBACK');
	}

	public function list_tables($like = NULL)
	{
		if (is_string($like))
		{
			// Search for table names
			$result = $this->query(Database::SELECT, 'SELECT name FROM sqlite_master WHERE type="table" AND name LIKE "'.$like.'"', FALSE);
		}
		else
		{
			// Find all table names
			$result = $this->query(Database::SELECT, 'SELECT name FROM SQLITE_MASTER WHERE type="table"', FALSE);
		}

		$tables = array();
		foreach ($result as $row)
		{
			//$tables[] = reset($row);
			$tables[] = $row;
		}

		return $tables;
	}

	public function list_columns($table, $like = NULL, $add_prefix = TRUE)
	{
		if (is_string($like)) {
			throw new Kohana_Exception('Database method :method is not supported by :class', array(':method' => __FUNCTION__, ':class' => __CLASS__));
		}
		$result = $this->query(Database::SELECT, 'PRAGMA table_info('.$table.')', FALSE);
		$columns = array();
		foreach ($result as $row) {
			$columns[$row['name']] = $row['name'];
		}
		return $columns;
	}

	public function escape($value)
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();

		if (($value = $this->_connection->escapeString( (string) $value)) === FALSE)
		{
			throw new Database_Exception(':error', array(
				':error' => $this->_connection->lastErrorMsg(),
			), $this->_connection->lastErrorCode());
		}

		// SQL standard is to use single-quotes for all values
		return "'$value'";
	}

} // End Database_SQLite3
