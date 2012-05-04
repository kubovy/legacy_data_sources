<?php

/**
 * Bolos Content Manager (http://bcm.poterion.com)
 * Copyright (c) 2005-2012, Poterion (http://www.poterion.com)
 * 
 * @category   Database
 * @package    com.poterion.BCM.Plugin.LegacyDataSources
 * @subpackage com.poterion.BCM.Plugin.LegacyDataSources.Model.Datasource.Database
 * @author     Jan Kubovy <jan@kubovy.eu>
 * @copyright  2005-2012 Copyright (c) Poterion (http://www.poterion.com)
 * @license    MIT License (http://www.opensource.org/licenses/mit-license.php)
 * @link       http://bcm.poterion.com Bolos CMS
 * @since      1.0.0
 */
App::uses('LegacyMysqlBase', 'LegacyDataSources.Model/Datasource/Database');

/**
 * MySQLi DBO driver object
 *
 * Provides connection and SQL generation for MySQL RDMS using PHP's MySQLi 
 * Interface
 *
 * @category   Database
 * @package    com.poterion.BCM.Plugin.LegacyDataSources
 * @subpackage com.poterion.BCM.Plugin.LegacyDataSources.Model.Datasource.Database
 * @author     Jan Kubovy <jan@kubovy.eu>
 * @copyright  2005-2012 Copyright (c) Poterion (http://www.poterion.com)
 * @license    MIT License (http://www.opensource.org/licenses/mit-license.php)
 * @link       http://bcm.poterion.com Bolos CMS
 * @since      1.0.0
 */
class LegacyMysqli extends LegacyMysqlBase {

	/**
	 * Datasource Description
	 *
	 * @var String
	 */
	public $description = "Legacy Mysqli DBO Driver";

	/**
	 * Base configuration settings for Mysqli driver
	 *
	 * @var array
	 */
	public $_baseConfig = array(
		'persistent' => true,
		'host' => 'localhost',
		'login' => 'root',
		'password' => '',
		'database' => 'cake',
		'port' => '3306',
		'socket' => null
	);

	/**
	 * Connects to the database using options in the given configuration array.
	 *
	 * @return boolean True if the database could be connected, else false
	 */
	public function connect() {
		$config          = $this->config;
		$this->connected = false;

		$this->connection = mysqli_connect(
			$config['host'],
			$config['login'], 
			$config['password'],
			$config['database'],
			$config['port'],
			$config['socket']
		);

		if ($this->connection !== false) {
			$this->connected = true;
		} else {
			return false;
		}

		$this->_useAlias = (bool) version_compare(
				mysqli_get_server_info($this->connection), "4.1", ">="
		);

		if (!empty($config['encoding'])) {
			$this->setEncoding($config['encoding']);
		}
		return $this->connected;
	}

	/**
	 * Check that MySQLi is installed/enabled
	 *
	 * @return boolean
	 */
	public function enabled() {
		return extension_loaded('mysqli');
	}

	/**
	 * Disconnects from database.
	 *
	 * @return boolean True if the database could be disconnected, else false
	 */
	public function disconnect() {
		if (isset($this->results) && is_resource($this->results)) {
			mysqli_free_result($this->results);
		}
		$this->connected = !@mysqli_close($this->connection);
		return !$this->connected;
	}

	/**
	 * Executes given SQL statement.
	 *
	 * @param string $sql SQL statement
	 * 
	 * @return resource Result resource identifier
	 */
	protected function _execute($sql) {
		if (preg_match('/^\s*call/i', $sql)) {
			return $this->_executeProcedure($sql);
		}
		return mysqli_query($this->connection, $sql);
	}

	/**
	 * Executes given SQL statement (procedure call).
	 *
	 * @param string $sql SQL statement (procedure call)
	 * 
	 * @return resource Result resource identifier for first recordset
	 */
	protected function _executeProcedure($sql) {
		$answer = mysqli_multi_query($this->connection, $sql);

		$firstResult = mysqli_store_result($this->connection);

		if (mysqli_more_results($this->connection)) {
			while ($lastResult = mysqli_next_result($this->connection));
		}
		return $firstResult;
	}

	/**
	 * Returns an array of sources (tables) in the database.
	 *
	 * @return array Array of tablenames in the database
	 */
	public function listSources() {
		$cache = parent::listSources();
		if ($cache !== null) {
			return $cache;
		}
		$result = $this->_execute(
			'SHOW TABLES FROM ' . $this->name($this->config['database']) . ';'
		);

		if (!$result) {
			return array();
		}

		$tables = array();

		while ($line = mysqli_fetch_row($result)) {
			$tables[] = $line[0];
		}
		parent::listSources($tables);
		return $tables;
	}

	/**
	 * Returns a quoted and escaped string of $data for use in an SQL statement.
	 *
	 * @param string  $data   String to be prepared for use in an SQL statement
	 * @param string  $column The column into which this data will be inserted
	 * @param boolean $safe   Whether or not numeric data should be handled 
	 *                        automagically if no column data is provided
	 * 
	 * @return string Quoted and escaped data
	 */
	public function value($data, $column = null, $safe = false) {
		$parent = parent::value($data, $column, $safe);

		if ($parent != null) {
			return $parent;
		}
		if ($data === null || (is_array($data) && empty($data))) {
			return 'NULL';
		}
		if ($data === ''
			&& $column !== 'integer'
			&& $column !== 'float'
			&& $column !== 'boolean'
		) {
			return "''";
		}
		if (empty($column)) {
			$column = $this->introspectType($data);
		}

		switch ($column) {
			case 'boolean':
				return $this->boolean((bool) $data);
				break;
			case 'integer' :
			case 'float' :
			case null :
				if ($data === '') {
					return 'NULL';
				}
				if (is_float($data)) {
					return str_replace(',', '.', strval($data));
				}
				if ((is_int($data) || is_float($data) || $data === '0') || (
					is_numeric($data) && strpos($data, ',') === false &&
					$data[0] != '0' && strpos($data, 'e') === false)) {
					return $data;
				}
			default:
				$data = "'"
					. mysqli_real_escape_string($this->connection, $data)
					. "'";
				break;
		}

		return $data;
	}

	/**
	 * Returns a formatted error message from previous database operation.
	 *
	 * @return string Error message with error number
	 */
	public function lastError() {
		if (mysqli_errno($this->connection)) {
			return mysqli_errno($this->connection) . ': '
				. mysqli_error($this->connection);
		}
		return null;
	}

	/**
	 * Returns number of affected rows in previous database operation. If no 
	 * previous operation exists, this returns false.
	 *
	 * @return integer Number of affected rows
	 */
	public function lastAffected() {
		if ($this->_result) {
			return mysqli_affected_rows($this->connection);
		}
		return null;
	}

	/**
	 * Returns number of rows in previous resultset. If no previous resultset 
	 * exists, this returns false.
	 *
	 * @return integer Number of rows in resultset
	 */
	public function lastNumRows() {
		if ($this->hasResult()) {
			return mysqli_num_rows($this->_result);
		}
		return null;
	}

	/**
	 * Returns the ID generated from the previous INSERT operation.
	 *
	 * @param unknown_type $source Source.
	 * 
	 * @return Integer Last inserted ID or null.
	 */
	public function lastInsertId($source = null) {
		$id = $this->fetchRow('SELECT LAST_INSERT_ID() AS insertID', false);
		if ($id !== false
			&& !empty($id)
			&& !empty($id[0])
			&& isset($id[0]['insertID'])
		) {
			return $id[0]['insertID'];
		}
		return null;
	}

	/**
	 * Enter description here...
	 *
	 * @param unknown_type &$results Restuls.
	 * 
	 * @return void
	 */
	public function resultSet(&$results) {
		if (isset($this->results)
			&& is_resource($this->results)
			&& $this->results != $results
		) {
			mysqli_free_result($this->results);
		}
		$this->results = & $results;
		$this->map     = array();
		$numFields     = mysqli_num_fields($results);
		$index         = 0;
		$j             = 0;
		
		while ($j < $numFields) {
			$column = mysqli_fetch_field_direct($results, $j);
			if (!empty($column->table)
				&& strpos($column->name, $this->virtualFieldSeparator) === false
			) {
				$this->map[$index++] = array($column->table, $column->name);
			} else {
				$this->map[$index++] = array(0, $column->name);
			}
			$j++;
		}
	}

	/**
	 * Fetches the next row from the current result set
	 *
	 * @return unknown
	 */
	public function fetchResult() {
		if ($row = mysqli_fetch_row($this->results)) {
			$resultRow = array();
			foreach ($row as $index => $field) {
				$table = $column = null;
				if (count($this->map[$index]) === 2) {
					list($table, $column) = $this->map[$index];
				}
				$resultRow[$table][$column] = $row[$index];
			}
			return $resultRow;
		}
		return false;
	}

	/**
	 * Gets the database encoding
	 *
	 * @return string The database encoding
	 */
	public function getEncoding() {
		return mysqli_client_encoding($this->connection);
	}

	/**
	 * Query charset by collation
	 *
	 * @param string $name Collation name
	 * 
	 * @return string Character set name
	 */
	public function getCharsetName($name) {
		if ((bool) version_compare(
			mysqli_get_server_info($this->connection), "5", ">=")
		) {
			// @codingStandardsIgnoreStart
			$cols = $this->query('SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLLATIONS WHERE COLLATION_NAME= ' . $this->value($name) . ';');
			if (isset($cols[0]['COLLATIONS']['CHARACTER_SET_NAME'])) {
				return $cols[0]['COLLATIONS']['CHARACTER_SET_NAME'];
			}
			// @codingStandardsIgnoreEnd
		}
		return false;
	}

	/**
	 * Checks if the result is valid
	 *
	 * @return boolean True if the result is valid, else false
	 */
	public function hasResult() {
		return is_object($this->_result);
	}

	/**
	 * Returns the name of the schema.
	 * 
	 * @return String Name of the schema.
	 */
	public public function getSchemaName() {
		return $this->config['database'];
	}

}
