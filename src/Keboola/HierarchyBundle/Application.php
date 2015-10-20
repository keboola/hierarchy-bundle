<?php

namespace Keboola\HierarchyBundle;

use Symfony\Component\Console\Application as BaseApplication;

class Application extends BaseApplication
{
    const NAME = 'HierarchyBundle Application';
    const VERSION = '1.0';

    public function __construct()
    {
        parent::__construct(static::NAME, static::VERSION);
        $runCommand = new RunCommand();
        $this->add($runCommand);
    }
}
