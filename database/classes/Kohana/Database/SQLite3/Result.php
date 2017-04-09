<?php
/**
 * SQLite database result.   See [Results](/database/results) for usage and examples.
 *
 * @package    Kohana/Database
 * @category   Query/Result
 * @author     Ed [https://github.com/ed3/kohana-sqlite3]
 * @license    https://koseven.ga/LICENSE.md
 */
class Kohana_Database_SQLite3_Result extends Database_Result {

	protected $_internal_row = 0;

	public function __construct($result, $sql, $as_object = FALSE, array $params = NULL)
	{
		parent::__construct($result, $sql, $as_object, $params);

		// Find the number of rows in the result
		while($row = $result->fetchArray(SQLITE3_NUM)) {
			$res[] = $row;
		}
		$this->_total_rows = count($res);
	}

	public function __destruct()
	{
		if (is_resource($this->_result))
		{
			$this->_result->finalize();
			//$this->_result->reset();
		}
	}

	public function seek($offset)
	{
		if ($this->offsetExists($offset) AND $this->_result->data_seek($offset))
		{
			// Set the current row to the offset
			$this->_current_row = $this->_internal_row = $offset;

			return TRUE;
		}
		else
		{
			return FALSE;
		}
	}

	public function current()
	{
		if ($this->_current_row !== $this->_internal_row AND ! $this->seek($this->_current_row))
			return NULL;

		// Increment internal row for optimization assuming rows are fetched in order
		$this->_internal_row++;

		if ($this->_as_object === TRUE)
		{
			// Return an stdClass
			return $this->_result->fetchArray(SQLITE3_ASSOC);
		}
		else
		{
			// Return an array of the row
			return $this->_result->fetchArray();
		}
	}

} // End Database_SQLite3_Result_Select
