<?php
/**
 * MySQL layer for DBO
 *
 * PHP versions 4 and 5
 *
 * CakePHP(tm) : Rapid Development Framework (http://cakephp.org)
 * Copyright 2005-2011, Cake Software Foundation, Inc. (http://cakefoundation.org)
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @copyright     Copyright 2005-2011, Cake Software Foundation, Inc. (http://cakefoundation.org)
 * @link          http://cakephp.org CakePHP(tm) Project
 * @package       cake
 * @subpackage    cake.cake.libs.model.datasources.dbo
 * @since         CakePHP(tm) v 0.10.5.1790
 * @license       MIT License (http://www.opensource.org/licenses/mit-license.php)
 */

App::uses('LegacyMysqlBase', 'LegacyDataSources.Model/Datasource/Database');

/**
 * MySQL DBO driver object
 *
 * Provides connection and SQL generation for MySQL RDMS
 *
 * @package       cake
 * @subpackage    cake.cake.libs.model.datasources.dbo
 */
class LegacyMysql extends LegacyMysqlBase {

/**
 * Datasource description
 *
 * @var string
 */
	var $description = "Legacy MySQL DBO Driver";

/**
 * Base configuration settings for MySQL driver
 *
 * @var array
 */
	var $_baseConfig = array(
		'persistent' => true,
		'host' => 'localhost',
		'login' => 'root',
		'password' => '',
		'database' => 'cake',
		'port' => '3306'
	);

/**
 * Connects to the database using options in the given configuration array.
 *
 * @return boolean True if the database could be connected, else false
 */
	function connect() {
		$config = $this->config;
		$this->connected = false;

		if (!$config['persistent']) {
			$this->connection = mysql_connect($config['host'] . ':' . $config['port'], $config['login'], $config['password'], true);
			$config['connect'] = 'mysql_connect';
		} else {
			$this->connection = mysql_pconnect($config['host'] . ':' . $config['port'], $config['login'], $config['password']);
		}

		if (!$this->connection) {
			return false;
		}

		if (mysql_select_db($config['database'], $this->connection)) {
			$this->connected = true;
		}

		if (!empty($config['encoding'])) {
			$this->setEncoding($config['encoding']);
		}

		$this->_useAlias = (bool)version_compare(mysql_get_server_info($this->connection), "4.1", ">=");

		return $this->connected;
	}

/**
 * Check whether the MySQL extension is installed/loaded
 *
 * @return boolean
 */
	function enabled() {
		return extension_loaded('mysql');
	}
/**
 * Disconnects from database.
 *
 * @return boolean True if the database could be disconnected, else false
 */
	function disconnect() {
		if (isset($this->results) && is_resource($this->results)) {
			mysql_free_result($this->results);
		}
		$this->connected = !@mysql_close($this->connection);
		return !$this->connected;
	}

/**
 * Executes given SQL statement.
 *
 * @param string $sql SQL statement
 * @return resource Result resource identifier
 * @access protected
 */
	function _execute($sql) {
		return mysql_query($sql, $this->connection);
	}

/**
 * Returns an array of sources (tables) in the database.
 *
 * @return array Array of tablenames in the database
 */
	function listSources() {
		$cache = parent::listSources();
		if ($cache != null) {
			return $cache;
		}
		$result = $this->_execute('SHOW TABLES FROM ' . $this->name($this->config['database']) . ';');

		if (!$result) {
			return array();
		} else {
			$tables = array();

			while ($line = mysql_fetch_row($result)) {
				$tables[] = $line[0];
			}
			parent::listSources($tables);
			return $tables;
		}
	}

/**
 * Returns a quoted and escaped string of $data for use in an SQL statement.
 *
 * @param string $data String to be prepared for use in an SQL statement
 * @param string $column The column into which this data will be inserted
 * @param boolean $safe Whether or not numeric data should be handled automagically if no column data is provided
 * @return string Quoted and escaped data
 */
	function value($data, $column = null, $safe = false) {
		$parent = parent::value($data, $column, $safe);

		if ($parent != null) {
			return $parent;
		}
		if ($data === null || (is_array($data) && empty($data))) {
			return 'NULL';
		}
		if ($data === '' && $column !== 'integer' && $column !== 'float' && $column !== 'boolean') {
			return  "''";
		}
		if (empty($column)) {
			$column = $this->introspectType($data);
		}

		switch ($column) {
			case 'boolean':
				return $this->boolean((bool)$data);
			break;
			case 'integer':
			case 'float':
				if ($data === '') {
					return 'NULL';
				}
				if (is_float($data)) {
					return str_replace(',', '.', strval($data));
				}
				if ((is_int($data) || $data === '0') || (
					is_numeric($data) && strpos($data, ',') === false &&
					$data[0] != '0' && strpos($data, 'e') === false)
				) {
					return $data;
				}
			default:
				return "'" . mysql_real_escape_string($data, $this->connection) . "'";
			break;
		}
	}

/**
 * Returns a formatted error message from previous database operation.
 *
 * @return string Error message with error number
 */
	function lastError() {
		if (mysql_errno($this->connection)) {
			return mysql_errno($this->connection).': '.mysql_error($this->connection);
		}
		return null;
	}

/**
 * Returns number of affected rows in previous database operation. If no previous operation exists,
 * this returns false.
 *
 * @return integer Number of affected rows
 */
	function lastAffected() {
		if ($this->_result) {
			return mysql_affected_rows($this->connection);
		}
		return null;
	}

/**
 * Returns number of rows in previous resultset. If no previous resultset exists,
 * this returns false.
 *
 * @return integer Number of rows in resultset
 */
	function lastNumRows() {
		if ($this->hasResult()) {
			return mysql_num_rows($this->_result);
		}
		return null;
	}

/**
 * Returns the ID generated from the previous INSERT operation.
 *
 * @param unknown_type $source
 * @return in
 */
	function lastInsertId($source = null) {
		$id = $this->fetchRow('SELECT LAST_INSERT_ID() AS insertID', false);
		if ($id !== false && !empty($id) && !empty($id[0]) && isset($id[0]['insertID'])) {
			return $id[0]['insertID'];
		}

		return null;
	}

/**
 * Enter description here...
 *
 * @param unknown_type $results
 */
	function resultSet(&$results) {
		if (isset($this->results) && is_resource($this->results) && $this->results != $results) {
			mysql_free_result($this->results);
		}
		$this->results =& $results;
		$this->map = array();
		$numFields = mysql_num_fields($results);
		$index = 0;
		$j = 0;

		while ($j < $numFields) {
			$column = mysql_fetch_field($results, $j);
			if (!empty($column->table) && strpos($column->name, $this->virtualFieldSeparator) === false) {
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
	function fetchResult() {
		if ($row = mysql_fetch_row($this->results)) {
			$resultRow = array();
			$i = 0;
			foreach ($row as $index => $field) {
				list($table, $column) = $this->map[$index];
				$resultRow[$table][$column] = $row[$index];
				$i++;
			}
			return $resultRow;
		} else {
			return false;
		}
	}

/**
 * Gets the database encoding
 *
 * @return string The database encoding
 */
	function getEncoding() {
		return mysql_client_encoding($this->connection);
	}

/**
 * Query charset by collation
 *
 * @param string $name Collation name
 * @return string Character set name
 */
	function getCharsetName($name) {
		if ((bool)version_compare(mysql_get_server_info($this->connection), "5", ">=")) {
			$cols = $this->query('SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLLATIONS WHERE COLLATION_NAME= ' . $this->value($name) . ';');
			if (isset($cols[0]['COLLATIONS']['CHARACTER_SET_NAME'])) {
				return $cols[0]['COLLATIONS']['CHARACTER_SET_NAME'];
			}
		}
		return false;
	}

	public function getSchemaName() {
		return $this->config['database'];
	}
}
