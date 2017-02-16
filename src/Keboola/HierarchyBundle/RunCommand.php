<?php

namespace Keboola\HierarchyBundle;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDOMySql\Driver;
use Doctrine\DBAL\Exception\ConnectionException;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Class RunCommand
 * @package Keboola\HierarchyBundle
 */
class RunCommand extends Command
{
    /**
     * @inheritdoc
     */
    protected function configure()
    {
        $this
            ->setName('hierarchy:run')
            ->addOption('data', 'd', InputOption::VALUE_REQUIRED, 'Path to data directory')
        ;
    }

    /**
     * @inheritdoc
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $dataDir = $input->getOption('data');
        if (!$dataDir || !file_exists($dataDir)) {
            throw new \InvalidArgumentException("Input directory not specified or does not exist.");
        }
        $output->writeln("Initializing");

        $configFile = file_get_contents($dataDir . DIRECTORY_SEPARATOR . 'config.json');
        $config = json_decode($configFile, true);

        if (!empty($config['storage']['input']['tables'][0]['destination'])) {
            $inputTable = $config['storage']['input']['tables'][0]['destination'];
        } else {
            throw new \InvalidArgumentException("No source table found in config.");
        }
        if (!empty($config['parameters']['columns']['id'])) {
            $idColumn = $config['parameters']['columns']['id'];
        } else {
            throw new \InvalidArgumentException("Id column not found in parameters.");
        }
        if (!empty($config['parameters']['columns']['sort'])) {
            $sortColumn = $config['parameters']['columns']['sort'];
        } else {
            throw new \InvalidArgumentException("Sort column not found in parameters.");
        }
        if (!empty($config['parameters']['columns']['parent'])) {
            $parentColumn = $config['parameters']['columns']['parent'];
        } else {
            throw new \InvalidArgumentException("Parent column not found in parameters.");
        }
        $delta = !empty($config['parameters']['timeDelta']);

        $output->writeln("Parameters validated");

        $driver = new Driver();
        $retries = 0;
        do {
            try {
                $db = new Connection([
                    'driver' => 'pdo_mysql',
                    'host' => 'localhost',
                    'user' => 'root',
                    'password' => 'root',
                    'charset' => 'utf8',
                ], $driver);
                $db->query('DROP DATABASE IF EXISTS `hierarchy`');
                $db->query('CREATE SCHEMA `hierarchy`;');
                $db->query('USE `hierarchy`;');
                $output->writeln("Database connection successful");
                break;
            } catch (\Exception $e) {
                $retries++;
                if ($retries > 10) {
                    throw $e;
                }
                $output->writeln("Waiting for connection: $retries");
                sleep(10);
            }
        } while (true);

        $hierarchy = new Hierarchy($db);
        $inputFile = $dataDir . DIRECTORY_SEPARATOR . 'in' . DIRECTORY_SEPARATOR .
            'tables' . DIRECTORY_SEPARATOR . $inputTable;
        $outDir = $dataDir . DIRECTORY_SEPARATOR . 'out' . DIRECTORY_SEPARATOR . 'tables';
        $hierarchy->process($inputFile, $delta, $idColumn, $sortColumn, $parentColumn, $outDir);

        $output->writeln("Using directory $dataDir");
    }
}
