<?php

namespace Keboola\HierarchyBundle\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;

class DefaultController extends Controller
{
    public function indexAction($name)
    {
        return $this->render('KeboolaHierarchyBundle:Default:index.html.twig', array('name' => $name));
    }
}
