<?php
/**
 * @author: Ondrej Hlavacek <ondrej.hlavacek@keboola.com>
 * @created: 10.12.12
 */

namespace Keboola\HierarchyBundle;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDOMySql\Driver;
use Doctrine\DBAL\Logging\DebugStack;
use Syrup\ComponentBundle\Component\Component,
	Guzzle\Http\Client
;

class Hierarchy extends Component
{
	protected $_name = 'hierarchy';
	protected $_prefix = 'rt';
	protected $_uniqid = '';

	/**
	 * @return string
	 */
	protected function _getTmpDir() {
		$dirName = "/tmp/{$this->_prefix}-{$this->_name}/" . $this->_getUniqId();
		if (!file_exists($dirName)) {
			mkdir($dirName, 0777, true);
		}
		return $dirName . "/";
	}

	/**
	 *
	 * Creates a filename
	 *
	 * @param string $stage
	 * @return string
	 */
	protected function _getFileName($fileName) {
		return $this->_getTmpDir() . $fileName . ".csv";
	}

	/**
	 * Clears temp dir
	 */
	protected function _removeTmpDir() {
		// cleanup
		exec("rm {$this->_getTmpDir()} -rf");
	}

	/**
	 * Returns uniqid
	 * @return string
	 */
	protected function _getUniqId() {
		if ($this->_uniqid == '') {
			$this->_uniqid = uniqid();
		}
		return $this->_uniqid;
	}

	/**
	 * @param $token
	 * @return Connection
	 */
	protected function _getConnection($token)
	{
		$provisioning = new \Keboola\Provisioning\Client('mysql', $token, $this->_storageApi->getRunId());
		$credentials = $provisioning->getCredentials();
		$dbal = new Driver();
		$db =  new Connection(array(
			'driver'    => 'pdo_mysql',
			'host'      => $credentials['credentials']['hostname'],
			'dbname'    => $credentials['credentials']['db'],
			'user'      => $credentials['credentials']['user'],
			'password'  => $credentials['credentials']['password'],
			'charset' => 'utf8',
		), $dbal);
		return $db;
	}

	/**
	 * @param $config
	 * @param $params
	 * @return bool|void
	 */
	protected function _process($config, $params)
	{

		$db = $this->_getConnection($this->_storageApi->getTokenString());

		// TODO runid
		$this->_log->info("HierarchyDB transformation START");

		$tableInfo = $this->_storageApi->getTable($params["source"]);

		$columns = array(
			$params["columns"]["id"],
			$params["columns"]["parent"],
			$params["columns"]["sort"]
		);

		$calculateDeltas = 0;
		if (isset($params["timeDelta"]) && $params["timeDelta"] == 1) {
			$calculateDeltas = 1;
		}

		$defOptions = array(
			"datatypes" => array(
				$params["columns"]["id"] => "VARCHAR(255)",
				$params["columns"]["parent"] => "VARCHAR(255)",
				$params["columns"]["sort"] => "VARCHAR(255)"
			),
			"destination" => "source",
			"columns" => $columns
		);

		$tableDefinition = $this->getTableDefinition($tableInfo, $defOptions);
		$db->query("DROP TABLE IF EXISTS `source`");
		$db->query("DROP VIEW IF EXISTS `out.source`");
		$db->query("DROP TABLE IF EXISTS `out.source`");
		$db->query("DROP TABLE IF EXISTS `tmp.Fill`");
		$db->query($tableDefinition);

		$inFile = $this->_getFileName("in");
		$options = array(
			"columns" => $columns
		);
		$this->_storageApi->exportTable($params["source"], $inFile, $options);

		$loadQuery = "
				LOAD DATA LOCAL INFILE '{$inFile}'
				INTO TABLE `source`
				FIELDS TERMINATED BY ','
				OPTIONALLY ENCLOSED BY '\"'
				ESCAPED BY ''
				IGNORE 1 LINES
				";
		$db->query($loadQuery);

		$cId = $params["columns"]["id"];
		$cParent = $params["columns"]["parent"];
		$cSort = $params["columns"]["sort"];

		// Alter columns
		$query = "ALTER TABLE `source`
			CHANGE `{$cId}` `id` VARCHAR(255),
			CHANGE `{$cParent}` `parent` VARCHAR(255),
			CHANGE `{$cSort}` `sort` VARCHAR(255),
			ADD COLUMN `__root` VARCHAR(255),
			ADD COLUMN `__depth` INT(11) NOT NULL DEFAULT 0,
			ADD COLUMN `__tmpRoot` VARCHAR(255) NULL,
			ADD COLUMN `__position` VARCHAR(2000) NULL,
			ADD COLUMN `__position_relative` INT(11) NULL,
			ADD COLUMN `__position_depth` INT(11) NULL
		";
		$db->query($query);


		if ($calculateDeltas) {
			$query = "
				ALTER TABLE `source`
				ADD COLUMN `__timestamp` INT(11) NOT NULL DEFAULT 0;
			";
			$db->query($query);
			$query = "
				UPDATE `source`
				SET `__timestamp` = UNIX_TIMESTAMP(`sort`);
			";
			$db->query($query);
		}

		// Create indexes
		$query = "ALTER TABLE `source`
			ADD KEY(`id`),
			ADD KEY(`parent`),
			ADD KEY(`id`, `parent`),
			ADD KEY(`sort`),
			ADD KEY(`__depth`),
			ADD KEY(`__position_depth`),
			ADD KEY(`__tmpRoot`)
		";
		$db->query($query);

		// Detect Orphans (items with missing parents) and set them to null.
		$orphans = $db->executeUpdate("
			UPDATE `source` t1
			LEFT JOIN `source` t2 ON t1.`parent` = t2.`id`
			SET t1.`parent` = NULL
			WHERE t1.`parent` IS NOT NULL AND t2.`id` IS NULL;
		");

		// Clean self referencing items
		$db->query("
			UPDATE `source`
			SET `parent` = NULL
			WHERE `parent` = `id`;
		");

		// Set roots items where no parent is available
		$roots = $db->executeUpdate("
			UPDATE `source`
			SET `__root` = `id`
			WHERE `parent` IS NULL;
		");

		// Set temporary root for all items (their direct parent) - tmpRoot will bubble up to real root.
		$db->query("
			UPDATE `source`
			SET `__tmpRoot` = `parent`
			WHERE `parent` IS NOT NULL;
		");

		// Recursion - while there are any __tmpRoot items increase depth and set tmpRoot a level up
		$depth = 0;
		while ($db->executeUpdate("
			UPDATE `source`
			SET `__root` = `__tmpRoot`
			WHERE `__tmpRoot` IS NOT NULL;
		") > 0) {
			$depth++;
			$db->query("
				UPDATE `source` t1
				JOIN `source` t2 ON t1.`__tmpRoot` = t2.`id`
				SET
					t1.`__tmpRoot` = t2.`parent`,
					t1.`__depth` = t1.`__depth` + 1
				;
			");
		}

		// Table for creating position

		$db->query("CREATE TABLE `tmp.Fill` (`id` VARCHAR(255), `__position` VARCHAR(2000), `__position_depth` INT(11), INDEX(`id`));");
		// Create positions
		// For each level of depth there will be an increasing number like 00001 - easy to sort alphanumerically
		// Eg Record with depth level = 0 will get a 01234, a child item of this will be appended to it's parents position number, eg 01234 + 00001 => 0123400001
		// All lower depths are padded with zeros at the end, so the parent will look like 0123400000.
		// Then this is sorted and inserted in a new table with and will get simple +1 increments
		for ($i=0; $i<=$depth; $i++) {
			// How long is the number of items in source table, used for numeric padding
			$db->query("SELECT @base := LENGTH(COUNT(*)) FROM `source`;");
			$db->query("SELECT @increment := 0;");
			$db->query("SELECT @depth := {$i};");
			$db->query("TRUNCATE `tmp.Fill`;");
			$db->query("
				INSERT INTO `tmp.Fill`
				SELECT
					t.`id`,
					CONCAT(IFNULL(parent.`__position`, ''), LPAD(@increment := @increment+1, @base, 0)) AS `__position`,
					@increment AS `__position_depth`
				FROM `source` t
				LEFT JOIN `source` parent ON t.`parent` = parent.`id`
				WHERE t.`__depth` = @depth
				ORDER BY t.`sort` ASC;
			");

			$db->query("
				UPDATE `source` t
				JOIN `tmp.Fill` f USING(`id`)
				SET t.`__position` = f.`__position`,
					t.`__position_depth` = f.`__position_depth`
				;
			");
		}

		$db->query("
			SELECT @increment := 0;
		");

		$db->query("
			TRUNCATE `tmp.Fill`;
		");

		// Flatten the position numbers
		$db->query("
			INSERT INTO `tmp.Fill`
			SELECT
				t.`id`,
				@increment := @increment+1 AS `__position`,
				0 AS `__position_depth`
			FROM `source` t
			ORDER BY t.`__position` ASC;
		");

		// And then back to source table
		$allItems = $db->executeUpdate("
			UPDATE `source` t
			JOIN `tmp.Fill` f USING(`id`)
			SET t.`__position` = f.`__position`
			;
		");

		// Relative position
		$db->query("
			SELECT @increment := 0;
		");

		$db->query("
			TRUNCATE `tmp.Fill`;
		");

		$db->query("
			INSERT INTO `tmp.Fill`
			SELECT s.`id`, CAST(s.__position - r.__position AS SIGNED) AS `__position`, 0 AS `__position_depth`
			FROM `source` s
			LEFT JOIN `source` r ON s.`__root` = r.`id`;
		");

		$db->query("
			UPDATE `source` t
			JOIN `tmp.Fill` f USING(`id`)
			SET t.`__position_relative` = f.`__position`;
		");

		if ($calculateDeltas) {
			$db->query("
				ALTER TABLE source
					ADD INDEX `root_depth` (`__root`, `__depth`),
					ADD INDEX `root_relpos` (`__root`, `__position_relative`),
					ADD INDEX `root_depth_pos` (`__root`, `__depth`, `__position_depth`)
					;
			");
			$db->query("
				CREATE VIEW `out.source` AS
				SELECT
					source.`id` AS {$cId},
					source.`__root` AS `root`,
					source.`__position` AS `position`,
					source.`__position_relative` AS `position_relative`,
					source.`__depth` AS `depth`,
					IF(source.`__timestamp` - root.`__timestamp` < 0, 0, source.`__timestamp` - root.`__timestamp`) AS `time_delta_runsum`,
					COALESCE(
						(source.`__timestamp` - previous.`__timestamp`),
						(source.`__timestamp` - previous_2.`__timestamp`),
						(source.`__timestamp` - previous_3.`__timestamp`),
						0
					) AS `time_delta`
				FROM
					`source` source
					LEFT JOIN `source` root USE INDEX(`root_depth`) ON source.`__root` = root.`__root` AND root.`__depth` = 0
					# Same level, direct previous
					LEFT JOIN `source` previous USE INDEX(`root_relpos`) ON source.`__root`= previous.`__root` AND
						previous.`__depth` = source.`__depth` AND previous.`__position_relative` = source.`__position_relative` - 1
					# One level up, direct previous
					LEFT JOIN `source` previous_2 USE INDEX(`root_relpos`) ON source.`__root`= previous_2.`__root` AND
						previous_2.`__depth` = source.`__depth` - 1 AND previous_2.`__position_relative` = source.`__position_relative` - 1
					# No direct previous, finding closest previous on the same level
					LEFT JOIN `source` previous_3 USE INDEX(`root_depth_pos`) ON source.`__root`= previous_3.`__root` AND
						previous_3.`__depth` = source.`__depth` AND previous_3.`__position_depth` = source.`__position_depth` - 1

			");
		} else {
			$db->query("
				CREATE VIEW `out.source` AS
				SELECT
					`id` AS {$cId},
					`__root` AS `root`,
					`__position` AS `position`,
					`__position_relative` AS `position_relative`,
					`__depth` AS `depth`
				FROM
					`source`;
			");
		}

		// Export Data
		$outFile = $this->_getFileName("out") . ".gz";
		$errFile = $this->_getFileName("err");
		$destinationTable = $params["destination"];

		$command = 'mysql -u ' . $db->getUsername()
			. ' -p' . $db->getPassword()
			. ' -h ' . $db->getHost()
			. ' '. $db->getDatabase(). ' --default-character-set=UTF8 --batch --execute ' . escapeshellarg((string) "SELECT * FROM `out.source`;")
			. ' --quick 2> ' . $errFile;

		$command .= ' | gzip --fast > ' . $outFile;
		$result = exec($command);
		if ($result != "" || file_exists($errFile) && filesize($errFile) > 0) {
			$error = $result;
			if ($error == '') {
				$error = trim(file_get_contents($errFile));
			}
			throw new Exception("MySQL export error: " . $error);
		}

		if (!$this->_storageApi->tableExists($destinationTable)) {
			$tableInfo = explode(".", $destinationTable);
			if (count($tableInfo) != 3) {
				throw new Exception("Wrong table identifier '{$destinationTable}' in the output mapping.");
			}
			$bucketId = $tableInfo[0] . "." . $tableInfo[1];
			if (!$this->_storageApi->bucketExists($bucketId)) {
				$this->_storageApi->createBucket(str_replace("c-", "", $tableInfo[1]), $tableInfo[0], "Bucket created by Transformation API");
			}
			$this->_storageApi->createTableAsync($bucketId, $tableInfo[2], new \Keboola\Csv\CsvFile($outFile, "\t", "", "\\"));
		} else {
			$this->_storageApi->writeTableAsync($destinationTable, new \Keboola\Csv\CsvFile($outFile, "\t", "", "\\"));
		}

		// Cleanup
		$db->query("DROP TABLE IF EXISTS `source`");
		$db->query("DROP VIEW IF EXISTS `out.source`");
		$db->query("DROP TABLE IF EXISTS `out.source`");
		$db->query("DROP TABLE IF EXISTS `tmp.Fill`");

		$this->_removeTmpDir();

		$this->_log->info("HierarchyDB transformation END", array(
				"items" => $allItems,
				"roots" => $roots,
				"orphans" => $orphans
		));

		return;
	}


	/**
	 *
	 * Generates a MySQL table definition
	 *
	 * @param $table array Storage API table
	 * @param $options - export options ("destination", "columns", "datatypes", "indexes")
	 * @return string
	 */
	public function getTableDefinition($table, $options=array())
	{
		if (!isset($options["destination"])) {
			$options["destination"] =  $table["id"];
		}
		$definition = "CREATE TABLE `{$options["destination"]}`\n(";

		// Column definition
		$columns = array();
		foreach($table["columns"] as $column) {
			$index = null;
			if (isset($options["columns"]) && count($options["columns"])) {
				if (!in_array($column, $options["columns"])) {
					continue;
				}
				$index = array_search($column, $options["columns"]);
			}

			// Column datatypes
			if (isset($options["datatypes"]) && in_array($column, array_keys($options["datatypes"]))) {
				$columns[$index] = "`{$column}` {$options["datatypes"][$column]}";
			} elseif (isset($table["primaryKey"]) && in_array($column, $table["primaryKey"])) {
				// Default for primary key
				$columns[$index] = "`{$column}` VARCHAR(255) NOT NULL DEFAULT ''";
			} else {
				$columns[$index] = "`{$column}` TEXT NOT NULL";
			}
		}
		ksort($columns);
		$definition .= join(",\n", $columns);

		// Primary key indexes
		if ($table["primaryKey"] && count($table["primaryKey"])) {
			$includePK = true;
			// Do not create PK if not all parts of the PK are imported
			if (isset($options["columns"]) && count($options["columns"])) {
				foreach($table["primaryKey"] as $pk) {
					if (!in_array($pk, $options["columns"])) {
						$includePK = false;
					}
				}
			}
			if ($includePK) {
				$definition .= ",\nPRIMARY KEY (`" . join("`, `", $table["primaryKey"]) . "`)";
			}
		}

		$definition .= ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
		return $definition;
	}
}

