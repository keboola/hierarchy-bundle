<?php
/**
 * @author: Ondrej Hlavacek <ondrej.hlavacek@keboola.com>
 * @created: 10.12.12
 */

namespace Keboola\HierarchyBundle;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use Keboola\Csv\CsvFile;
use Symfony\Component\Process\Process;

class Hierarchy
{
    /**
     * Configuration options.
     * @var Connection
     */
    private $db;


    /**
     * Constructor.
     * @param Connection $db
     */
    public function __construct(Connection $db)
    {
        $this->db = $db;
    }


    /**
     * @param string $inputFile Input CSV file with table data.
     * @param bool $calculateDeltas Set to true to add timeDelta column
     * @param string $idColumn Name of column with primary key.
     * @param string $sortColumn Name of column by which data are sorted.
     * @param string $parentColumn Name of column with parent Id
     * @param string $outDirectory Directory in which the output file will be stored.
     * @throws DBALException
     */
    public function process($inputFile, $calculateDeltas, $idColumn, $sortColumn, $parentColumn, $outDirectory)
    {
        $dataTypes = [
            $idColumn => 'VARCHAR(255) NOT NULL DEFAULT \'\'',
            $parentColumn => 'VARCHAR(255)',
            $sortColumn => 'VARCHAR(255)'
        ];
        $csv = new CsvFile($inputFile);
        $header = $csv->getHeader();

        $tableDefinition = $this->getTableDefinition('source', $dataTypes, $idColumn, $header);
        $this->db->query('DROP TABLE IF EXISTS `source`');
        $this->db->query('DROP VIEW IF EXISTS `out.source`');
        $this->db->query('DROP TABLE IF EXISTS `out.source`');
        $this->db->query('DROP TABLE IF EXISTS `tmp.Fill`');
        $this->db->query($tableDefinition);

        $loadQuery = '
            LOAD DATA LOCAL INFILE \'' . str_replace('\\', '/', $inputFile) .'\'
            INTO TABLE `source`
            FIELDS TERMINATED BY \',\'
            OPTIONALLY ENCLOSED BY \'"\'
            ESCAPED BY \'\'
            IGNORE 1 LINES;';
        $this->db->query($loadQuery);

        // Alter columns
        $query = 'ALTER TABLE `source`
            CHANGE `' . $idColumn .'` `id` VARCHAR(255),
            CHANGE `' . $parentColumn . '` `parent` VARCHAR(255),
            CHANGE `' . $sortColumn . '` `sort` VARCHAR(255),
            ADD COLUMN `__root` VARCHAR(255),
            ADD COLUMN `__depth` INT(11) NOT NULL DEFAULT 0,
            ADD COLUMN `__tmpRoot` VARCHAR(255) NULL,
            ADD COLUMN `__position` VARCHAR(2000) NULL,
            ADD COLUMN `__position_relative` INT(11) NULL,
            ADD COLUMN `__position_depth` INT(11) NULL;';
        $this->db->query($query);


        if ($calculateDeltas) {
            $query = '
                ALTER TABLE `source`
                ADD COLUMN `__timestamp` INT(11) NOT NULL DEFAULT 0;';
            $this->db->query($query);
            $query = '
                UPDATE `source`
                SET `__timestamp` = UNIX_TIMESTAMP(`sort`);';
            $this->db->query($query);
        }

        // Create indexes
        $query = 'ALTER TABLE `source`
            ADD KEY(`id`),
            ADD KEY(`parent`),
            ADD KEY(`id`, `parent`),
            ADD KEY(`sort`),
            ADD KEY(`__depth`),
            ADD KEY(`__position_depth`),
            ADD KEY(`__tmpRoot`)';
        $this->db->query($query);

        // Detect Orphans (items with missing parents) and set them to null.
        $this->db->executeUpdate(
            'UPDATE `source` t1
            LEFT JOIN `source` t2 ON t1.`parent` = t2.`id`
            SET t1.`parent` = NULL
            WHERE t1.`parent` IS NOT NULL AND t2.`id` IS NULL;'
        );

        // Clean self referencing items
        $this->db->query(
            'UPDATE `source`
            SET `parent` = NULL
            WHERE `parent` = `id`;'
        );

        // Set roots items where no parent is available
        $this->db->executeUpdate(
            'UPDATE `source`
            SET `__root` = `id`
            WHERE `parent` IS NULL;'
        );

        // Set temporary root for all items (their direct parent) - tmpRoot will bubble up to real root.
        $this->db->query(
            'UPDATE `source`
            SET `__tmpRoot` = `parent`
            WHERE `parent` IS NOT NULL;'
        );

        // Recursion - while there are any __tmpRoot items increase depth and set tmpRoot a level up
        $depth = 0;
        while ($this->db->executeUpdate(
            'UPDATE `source`
            SET `__root` = `__tmpRoot`
            WHERE `__tmpRoot` IS NOT NULL;'
        ) > 0) {
            $depth++;
            $this->db->query(
                'UPDATE `source` t1
                JOIN `source` t2 ON t1.`__tmpRoot` = t2.`id`
                SET
                    t1.`__tmpRoot` = t2.`parent`,
                    t1.`__depth` = t1.`__depth` + 1
                ;'
            );
        }

        // Table for creating position

        $this->db->query(
            'CREATE TABLE `tmp.Fill` (
                `id` VARCHAR(255),
                `__position` VARCHAR(2000),
                `__position_depth` INT(11),
                INDEX(`id`)
             );'
        );
        /* Create positions:
            For each level of depth there will be an increasing number like 00001 - easy to sort alphanumerically
            E.g. Record with depth level = 0 will get a 01234, a child item of this will be appended to it's parents
            position number, eg 01234 + 00001 => 0123400001
            All lower depths are padded with zeros at the end, so the parent will look like 0123400000.
            Then this is sorted and inserted in a new table with and will get simple +1 increments.
        */
        for ($i = 0; $i <= $depth; $i++) {
            // How long is the number of items in source table, used for numeric padding
            $this->db->query('SELECT @base := LENGTH(COUNT(*)) FROM `source`;');
            $this->db->query('SELECT @increment := 0;');
            $this->db->query('SELECT @depth := ' . $i . ';');
            $this->db->query('TRUNCATE `tmp.Fill`;');
            $this->db->query(
                'INSERT INTO `tmp.Fill`
                SELECT
                    t.`id`,
                    CONCAT(
                        IFNULL(parent.`__position`, \'\'),
                        LPAD(@increment := @increment+1, @base, 0)
                    ) AS `__position`,
                    @increment AS `__position_depth`
                FROM `source` t
                LEFT JOIN `source` parent ON t.`parent` = parent.`id`
                WHERE t.`__depth` = @depth
                ORDER BY t.`sort` ASC;'
            );

            $this->db->query(
                'UPDATE `source` t
                JOIN `tmp.Fill` f USING(`id`)
                SET t.`__position` = f.`__position`,
                    t.`__position_depth` = f.`__position_depth`
                ;'
            );
        }

        $this->db->query('SELECT @increment := 0;');

        $this->db->query('TRUNCATE `tmp.Fill`;');

        // Flatten the position numbers
        $this->db->query(
            'INSERT INTO `tmp.Fill`
            SELECT
                t.`id`,
                @increment := @increment+1 AS `__position`,
                0 AS `__position_depth`
            FROM `source` t
            ORDER BY t.`__position` ASC;'
        );

        // And then back to source table
        $this->db->executeUpdate(
            'UPDATE `source` t
            JOIN `tmp.Fill` f USING(`id`)
            SET t.`__position` = f.`__position`
            ;'
        );

        // Relative position
        $this->db->query('SELECT @increment := 0;');

        $this->db->query('TRUNCATE `tmp.Fill`;');

        $this->db->query(
            'INSERT INTO `tmp.Fill`
            SELECT s.`id`, CAST(s.__position - r.__position AS SIGNED) AS `__position`, 0 AS `__position_depth`
            FROM `source` s
            LEFT JOIN `source` r ON s.`__root` = r.`id`;'
        );

        $this->db->query(
            'UPDATE `source` t
            JOIN `tmp.Fill` f USING(`id`)
            SET t.`__position_relative` = f.`__position`;'
        );

        if ($calculateDeltas) {
            $this->db->query(
                'ALTER TABLE source
                    ADD INDEX `root_depth` (`__root`, `__depth`),
                    ADD INDEX `root_relpos` (`__root`, `__position_relative`),
                    ADD INDEX `root_depth_pos` (`__root`, `__depth`, `__position_depth`)
                    ;'
            );
            $this->db->query(
                'CREATE VIEW `out.source` AS
                SELECT
                    source.`id` AS `' . $idColumn . '`,
                    source.`__root` AS `root`,
                    source.`__position` AS `position`,
                    source.`__position_relative` AS `position_relative`,
                    source.`__depth` AS `depth`,
                    IF(source.`__timestamp` - root.`__timestamp` < 0, 0, source.`__timestamp` - root.`__timestamp`)
                        AS `time_delta_runsum`,
                    COALESCE(
                        (source.`__timestamp` - previous.`__timestamp`),
                        (source.`__timestamp` - previous_2.`__timestamp`),
                        (source.`__timestamp` - previous_3.`__timestamp`),
                        0
                    ) AS `time_delta`
                FROM
                    `source` source
                    LEFT JOIN `source` root USE INDEX(`root_depth`)
                        ON source.`__root` = root.`__root` AND root.`__depth` = 0
                    # Same level, direct previous
                    LEFT JOIN `source` previous USE INDEX(`root_relpos`)
                        ON source.`__root`= previous.`__root` AND
                            previous.`__depth` = source.`__depth` AND
                            previous.`__position_relative` = source.`__position_relative` - 1
                    # One level up, direct previous
                    LEFT JOIN `source` previous_2 USE INDEX(`root_relpos`)
                        ON source.`__root`= previous_2.`__root` AND
                            previous_2.`__depth` = source.`__depth` - 1 AND
                            previous_2.`__position_relative` = source.`__position_relative` - 1
                    # No direct previous, finding closest previous on the same level
                    LEFT JOIN `source` previous_3 USE INDEX(`root_depth_pos`)
                        ON source.`__root`= previous_3.`__root` AND
                            previous_3.`__depth` = source.`__depth` AND
                            previous_3.`__position_depth` = source.`__position_depth` - 1
                ;'
            );
        } else {
            $this->db->query(
                'CREATE VIEW `out.source` AS
                SELECT
                    `id` AS `' . $idColumn . '`,
                    `__root` AS `root`,
                    `__position` AS `position`,
                    `__position_relative` AS `position_relative`,
                    `__depth` AS `depth`
                FROM
                    `source`;'
            );
        }

        // Export Data
        $outFile = $outDirectory . DIRECTORY_SEPARATOR . 'destination.csv';
        $command = 'mysql -u ' . $this->db->getUsername()
            . ' -p' . $this->db->getPassword()
            . ' -h ' . $this->db->getHost()
            . ' '. $this->db->getDatabase()
            . ' --default-character-set=UTF8 --batch --execute ' . escapeshellarg('SELECT * FROM `out.source`;')
            . ' --quick > ' . $outFile;
        $process = new Process($command);
        $process->run();
        if ($process->getExitCode() != 0) {
            $error = $process->getErrorOutput();
            if (!$error) {
                $error = $process->getOutput();
            }
            throw new DBALException('MySQL export error: ' . $error);
        }
    }


    /**
     * Generates a MySQL table definition
     *
     * @param string $destination Name of table.
     * @param array $dataTypes Array with column types (index is column name)
     * @param string $pkColumn Name of column with primary key.
     * @param array $order Array with ordering of columns (key is index, value is column name).
     * @return string Create statement
     */
    public function getTableDefinition($destination, array $dataTypes, $pkColumn, $order)
    {
        $definition = "CREATE TABLE `$destination`\n(";

        // Column definition
        $columns = [];
        foreach ($order as $index => $column) {
            $columns[$index] = "`$column` $dataTypes[$column]";
        }
        ksort($columns);
        $definition .= implode(",\n", $columns);

        // Primary key indexes
        $definition .= ",\nPRIMARY KEY (`$pkColumn`)";
        $definition .= ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
        return $definition;
    }
}
