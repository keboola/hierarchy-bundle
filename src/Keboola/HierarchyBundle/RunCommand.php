<?php

namespace Keboola\HierarchyBundle;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class RunCommand extends Command
{
    protected function configure()
    {
        $this
            ->setName('hierarchy:run')
            ->addOption('data', 'd', InputOption::VALUE_REQUIRED, 'Path to data directory')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $dataDir = $input->getOption('data');
        if (!$dataDir || !file_exists($dataDir)) {
            throw new \InvalidArgumentException("Input directory not specified or does not exist.");
        }
        $output->writeln("Using directory $dataDir");
    }
}
