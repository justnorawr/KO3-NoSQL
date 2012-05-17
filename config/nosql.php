<?php defined('SYSPATH') or die('No direct access allowed.');

return array
(
	'dynamo' => array
	(
		'type'		=>	'dynamo',
		'debug'		=>	40,
		'profiling'    	=>	TRUE,
	),
	'simpledb' => array
	(
		'type'		=>	'simpledb',
		'debug'		=>	40,
		'profiling'	=>	TRUE,
	),
	'mongo' => array
	(
		'type'		=>	'mongo',
		'server'		=>	'mongodb://YOURHOST:27017',
		'database'	=>	'example_db',
		'debug'		=>	40,
		'profiling'    	=>	TRUE,
	),
	'redis' => array
	(
		'type'		=>	'redis',
		'server'		=>	'mongodb://YOURHOST:27017',
		'database'	=>	'example_db',
		'debug'		=>	40,
		'profiling'    	=>	TRUE,
	),
);
