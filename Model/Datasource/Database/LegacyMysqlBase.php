<?php
/**
 * MySQL Base layer for DBO
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

App::uses('LegacyDboSource', 'LegacyDataSources.Model/Datasource');

/**
 * Provides common base for MySQL & MySQLi connections
 *
 * @package       cake
 * @subpackage    cake.cake.libs.model.datasources.dbo
 */
class LegacyMysqlBase extends LegacyDboSource {

/**
 * Description property.
 *
 * @var string
 */
	var $description = "Legacy MySQL DBO Base Driver";

/**
 * Start quote
 *
 * @var string
 */
	var $startQuote = "`";

/**
 * End quote
 *
 * @var string
 */
	var $endQuote = "`";

/**
 * use alias for update and delete. Set to true if version >= 4.1
 *
 * @var boolean
 * @access protected
 */
	var $_useAlias = true;

/**
 * Index of basic SQL commands
 *
 * @var array
 * @access protected
 */
	var $_commands = array(
		'begin'    => 'START TRANSACTION',
		'commit'   => 'COMMIT',
		'rollback' => 'ROLLBACK'
	);

/**
 * List of engine specific additional field parameters used on table creating
 *
 * @var array
 * @access public
 */
	var $fieldParameters = array(
		'charset' => array('value' => 'CHARACTER SET', 'quote' => false, 'join' => ' ', 'column' => false, 'position' => 'beforeDefault'),
		'collate' => array('value' => 'COLLATE', 'quote' => false, 'join' => ' ', 'column' => 'Collation', 'position' => 'beforeDefault'),
		'comment' => array('value' => 'COMMENT', 'quote' => true, 'join' => ' ', 'column' => 'Comment', 'position' => 'afterDefault')
	);

/**
 * List of table engine specific parameters used on table creating
 *
 * @var array
 * @access public
 */
	var $tableParameters = array(
		'charset' => array('value' => 'DEFAULT CHARSET', 'quote' => false, 'join' => '=', 'column' => 'charset'),
		'collate' => array('value' => 'COLLATE', 'quote' => false, 'join' => '=', 'column' => 'Collation'),
		'engine' => array('value' => 'ENGINE', 'quote' => false, 'join' => '=', 'column' => 'Engine')
	);

/**
 * MySQL column definition
 *
 * @var array
 */
	var $columns = array(
		'primary_key' => array('name' => 'NOT NULL AUTO_INCREMENT'),
		'string' => array('name' => 'varchar', 'limit' => '255'),
		'text' => array('name' => 'text'),
		'integer' => array('name' => 'int', 'limit' => '11', 'formatter' => 'intval'),
		'float' => array('name' => 'float', 'formatter' => 'floatval'),
		'datetime' => array('name' => 'datetime', 'format' => 'Y-m-d H:i:s', 'formatter' => 'date'),
		'timestamp' => array('name' => 'timestamp', 'format' => 'Y-m-d H:i:s', 'formatter' => 'date'),
		'time' => array('name' => 'time', 'format' => 'H:i:s', 'formatter' => 'date'),
		'date' => array('name' => 'date', 'format' => 'Y-m-d', 'formatter' => 'date'),
		'binary' => array('name' => 'blob'),
		'boolean' => array('name' => 'tinyint', 'limit' => '1')
	);

/**
 * Returns an array of the fields in given table name.
 *
 * @param string $tableName Name of database table to inspect
 * @return array Fields in table. Keys are name and type
 */
	function describe(&$model) {
		$cache = parent::describe($model);
		if ($cache != null) {
			return $cache;
		}
		$fields = false;
		$cols = $this->query('SHOW FULL COLUMNS FROM ' . $this->fullTableName($model));

		foreach ($cols as $column) {
			$colKey = array_keys($column);
			if (isset($column[$colKey[0]]) && !isset($column[0])) {
				$column[0] = $column[$colKey[0]];
			}
			if (isset($column[0])) {
				$fields[$column[0]['Field']] = array(
					'type' => $this->column($column[0]['Type']),
					'null' => ($column[0]['Null'] == 'YES' ? true : false),
					'default' => $column[0]['Default'],
					'length' => $this->length($column[0]['Type']),
				);
				if (!empty($column[0]['Key']) && isset($this->index[$column[0]['Key']])) {
					$fields[$column[0]['Field']]['key'] = $this->index[$column[0]['Key']];
				}
				foreach ($this->fieldParameters as $name => $value) {
					if (!empty($column[0][$value['column']])) {
						$fields[$column[0]['Field']][$name] = $column[0][$value['column']];
					}
				}
				if (isset($fields[$column[0]['Field']]['collate'])) {
					$charset = $this->getCharsetName($fields[$column[0]['Field']]['collate']);
					if ($charset) {
						$fields[$column[0]['Field']]['charset'] = $charset;
					}
				}
			}
		}
		$this->__cacheDescription($this->fullTableName($model, false), $fields);
		return $fields;
	}

/**
 * Generates and executes an SQL UPDATE statement for given model, fields, and values.
 *
 * @param Model $model
 * @param array $fields
 * @param array $values
 * @param mixed $conditions
 * @return array
 */
	function update(&$model, $fields = array(), $values = null, $conditions = null) {
		if (!$this->_useAlias) {
			return parent::update($model, $fields, $values, $conditions);
		}

		if ($values == null) {
			$combined = $fields;
		} else {
			$combined = array_combine($fields, $values);
		}

		$alias = $joins = false;
		$fields = $this->_prepareUpdateFields($model, $combined, empty($conditions), !empty($conditions));
		$fields = implode(', ', $fields);
		$table = $this->fullTableName($model);

		if (!empty($conditions)) {
			$alias = $this->name($model->alias);
			if ($model->name == $model->alias) {
				$joins = implode(' ', $this->_getJoins($model));
			}
		}
		$conditions = $this->conditions($this->defaultConditions($model, $conditions, $alias), true, true, $model);

		if ($conditions === false) {
			return false;
		}

		if (!$this->execute($this->renderStatement('update', compact('table', 'alias', 'joins', 'fields', 'conditions')))) {
			$model->onError();
			return false;
		}
		return true;
	}

/**
 * Generates and executes an SQL DELETE statement for given id/conditions on given model.
 *
 * @param Model $model
 * @param mixed $conditions
 * @return boolean Success
 */
	function delete(&$model, $conditions = null) {
		if (!$this->_useAlias) {
			return parent::delete($model, $conditions);
		}
		$alias = $this->name($model->alias);
		$table = $this->fullTableName($model);
		$joins = implode(' ', $this->_getJoins($model));

		if (empty($conditions)) {
			$alias = $joins = false;
		}
		$complexConditions = false;
		foreach ((array)$conditions as $key => $value) {
			if (strpos($key, $model->alias) === false) {
				$complexConditions = true;
				break;
			}
		}
		if (!$complexConditions) {
			$joins = false;
		}

		$conditions = $this->conditions($this->defaultConditions($model, $conditions, $alias), true, true, $model);
		if ($conditions === false) {
			return false;
		}
		if ($this->execute($this->renderStatement('delete', compact('alias', 'table', 'joins', 'conditions'))) === false) {
			$model->onError();
			return false;
		}
		return true;
	}

/**
 * Sets the database encoding
 *
 * @param string $enc Database encoding
 */
	function setEncoding($enc) {
		return $this->_execute('SET NAMES ' . $enc) != false;
	}

/**
 * Returns an array of the indexes in given datasource name.
 *
 * @param string $model Name of model to inspect
 * @return array Fields in table. Keys are column and unique
 */
	function index($model) {
		$index = array();
		$table = $this->fullTableName($model);
		if ($table) {
			$indexes = $this->query('SHOW INDEX FROM ' . $table);
			if (isset($indexes[0]['STATISTICS'])) {
				$keys = Set::extract($indexes, '{n}.STATISTICS');
			} else {
				$keys = Set::extract($indexes, '{n}.0');
			}
			foreach ($keys as $i => $key) {
				if (!isset($index[$key['Key_name']])) {
					$col = array();
					$index[$key['Key_name']]['column'] = $key['Column_name'];
					$index[$key['Key_name']]['unique'] = intval($key['Non_unique'] == 0);
				} else {
					if (!is_array($index[$key['Key_name']]['column'])) {
						$col[] = $index[$key['Key_name']]['column'];
					}
					$col[] = $key['Column_name'];
					$index[$key['Key_name']]['column'] = $col;
				}
			}
		}
		return $index;
	}

/**
 * Generate a MySQL Alter Table syntax for the given Schema comparison
 *
 * @param array $compare Result of a CakeSchema::compare()
 * @return array Array of alter statements to make.
 */
	function alterSchema($compare, $table = null) {
		if (!is_array($compare)) {
			return false;
		}
		$out = '';
		$colList = array();
		foreach ($compare as $curTable => $types) {
			$indexes = $tableParameters = $colList = array();
			if (!$table || $table == $curTable) {
				$out .= 'ALTER TABLE ' . $this->fullTableName($curTable) . " \n";
				foreach ($types as $type => $column) {
					if (isset($column['indexes'])) {
						$indexes[$type] = $column['indexes'];
						unset($column['indexes']);
					}
					if (isset($column['tableParameters'])) {
						$tableParameters[$type] = $column['tableParameters'];
						unset($column['tableParameters']);
					}
					switch ($type) {
						case 'add':
							foreach ($column as $field => $col) {
								$col['name'] = $field;
								$alter = 'ADD ' . $this->buildColumn($col);
								if (isset($col['after'])) {
									$alter .= ' AFTER ' . $this->name($col['after']);
								}
								$colList[] = $alter;
							}
						break;
						case 'drop':
							foreach ($column as $field => $col) {
								$col['name'] = $field;
								$colList[] = 'DROP ' . $this->name($field);
							}
						break;
						case 'change':
							foreach ($column as $field => $col) {
								if (!isset($col['name'])) {
									$col['name'] = $field;
								}
								$colList[] = 'CHANGE ' . $this->name($field) . ' ' . $this->buildColumn($col);
							}
						break;
					}
				}
				$colList = array_merge($colList, $this->_alterIndexes($curTable, $indexes));
				$colList = array_merge($colList, $this->_alterTableParameters($curTable, $tableParameters));
				$out .= "\t" . join(",\n\t", $colList) . ";\n\n";
			}
		}
		return $out;
	}

/**
 * Generate a MySQL "drop table" statement for the given Schema object
 *
 * @param object $schema An instance of a subclass of CakeSchema
 * @param string $table Optional.  If specified only the table name given will be generated.
 *                      Otherwise, all tables defined in the schema are generated.
 * @return string
 */
	function dropSchema($schema, $table = null) {
		if (!is_a($schema, 'CakeSchema')) {
			trigger_error(__('Invalid schema object', true), E_USER_WARNING);
			return null;
		}
		$out = '';
		foreach ($schema->tables as $curTable => $columns) {
			if (!$table || $table == $curTable) {
				$out .= 'DROP TABLE IF EXISTS ' . $this->fullTableName($curTable) . ";\n";
			}
		}
		return $out;
	}

/**
 * Generate MySQL table parameter alteration statementes for a table.
 *
 * @param string $table Table to alter parameters for.
 * @param array $parameters Parameters to add & drop.
 * @return array Array of table property alteration statementes.
 * @todo Implement this method.
 */
	function _alterTableParameters($table, $parameters) {
		if (isset($parameters['change'])) {
			return $this->buildTableParameters($parameters['change']);
		}
		return array();
	}

/**
 * Generate MySQL index alteration statements for a table.
 *
 * @param string $table Table to alter indexes for
 * @param array $new Indexes to add and drop
 * @return array Index alteration statements
 */
	function _alterIndexes($table, $indexes) {
		$alter = array();
		if (isset($indexes['drop'])) {
			foreach($indexes['drop'] as $name => $value) {
				$out = 'DROP ';
				if ($name == 'PRIMARY') {
					$out .= 'PRIMARY KEY';
				} else {
					$out .= 'KEY ' . $name;
				}
				$alter[] = $out;
			}
		}
		if (isset($indexes['add'])) {
			foreach ($indexes['add'] as $name => $value) {
				$out = 'ADD ';
				if ($name == 'PRIMARY') {
					$out .= 'PRIMARY ';
					$name = null;
				} else {
					if (!empty($value['unique'])) {
						$out .= 'UNIQUE ';
					}
				}
				if (is_array($value['column'])) {
					$out .= 'KEY '. $name .' (' . implode(', ', array_map(array(&$this, 'name'), $value['column'])) . ')';
				} else {
					$out .= 'KEY '. $name .' (' . $this->name($value['column']) . ')';
				}
				$alter[] = $out;
			}
		}
		return $alter;
	}

/**
 * Inserts multiple values into a table
 *
 * @param string $table
 * @param string $fields
 * @param array $values
 */
	function insertMulti($table, $fields, $values) {
		$table = $this->fullTableName($table);
		if (is_array($fields)) {
			$fields = implode(', ', array_map(array(&$this, 'name'), $fields));
		}
		$values = implode(', ', $values);
		$this->query("INSERT INTO {$table} ({$fields}) VALUES {$values}");
	}
/**
 * Returns an detailed array of sources (tables) in the database.
 *
 * @param string $name Table name to get parameters 
 * @return array Array of tablenames in the database
 */
	function listDetailedSources($name = null) {
		$condition = '';
		if (is_string($name)) {
			$condition = ' LIKE ' . $this->value($name);
		}
		$result = $this->query('SHOW TABLE STATUS FROM ' . $this->name($this->config['database']) . $condition . ';');
		if (!$result) {
			return array();
		} else {
			$tables = array();
			foreach ($result as $row) {
				$tables[$row['TABLES']['Name']] = $row['TABLES'];
				if (!empty($row['TABLES']['Collation'])) {
					$charset = $this->getCharsetName($row['TABLES']['Collation']);
					if ($charset) {
						$tables[$row['TABLES']['Name']]['charset'] = $charset;
					}
				}
			}
			if (is_string($name)) {
				return $tables[$name];
			}
			return $tables;
		}
	}

/**
 * Converts database-layer column types to basic types
 *
 * @param string $real Real database-layer column type (i.e. "varchar(255)")
 * @return string Abstract column type (i.e. "string")
 */
	function column($real) {
		if (is_array($real)) {
			$col = $real['name'];
			if (isset($real['limit'])) {
				$col .= '('.$real['limit'].')';
			}
			return $col;
		}

		$col = str_replace(')', '', $real);
		$limit = $this->length($real);
		if (strpos($col, '(') !== false) {
			list($col, $vals) = explode('(', $col);
		}

		if (in_array($col, array('date', 'time', 'datetime', 'timestamp'))) {
			return $col;
		}
		if (($col == 'tinyint' && $limit == 1) || $col == 'boolean') {
			return 'boolean';
		}
		if (strpos($col, 'int') !== false) {
			return 'integer';
		}
		if (strpos($col, 'char') !== false || $col == 'tinytext') {
			return 'string';
		}
		if (strpos($col, 'text') !== false) {
			return 'text';
		}
		if (strpos($col, 'blob') !== false || $col == 'binary') {
			return 'binary';
		}
		if (strpos($col, 'float') !== false || strpos($col, 'double') !== false || strpos($col, 'decimal') !== false) {
			return 'float';
		}
		if (strpos($col, 'enum') !== false) {
			return "enum($vals)";
		}
		return 'text';
	}
}
