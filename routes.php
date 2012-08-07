<?php defined('SYSPATH') or die('No direct script access.');

Route::set('nosql_example_mongo_all', 'example_mongo(/<action>(/<collection>(/<item_name>)))')
	->defaults(array(
		'controller'	=>	'nosql_example_mongo',
		'collection'	=>	'example_collection',
		'item_name'	=>	'4fab863bf01d9dfb12000000'
	));

Route::set('nosql_example_dynamo_table', 'example_dynamo/<action>(/<table>(/<read>(/<write>)))', array(
		'action'		=>	'(create_table|update_table)'
           ))
	->defaults(array(
		'controller'	=>	'nosql_example_dynamo',
		'table'		=>	'example_table',
		'read'		=>	10,
		'write'		=>	10
	));

Route::set('nosql_example_dynamo_all', 'example_dynamo(/<action>(/<table>(/<hash>(/<range>))))')
	->defaults(array(
		'controller'	=>	'nosql_example_dynamo',
		'table'		=>	'example_table',
		'hash'		=>	1,
		'range'		=>	1
	));

Route::set('nosql_example_simpledb_all', 'example_simpledb(/<action>(/<domain>(/<item_name>)))')
	->defaults(array(
		'controller'	=>	'nosql_example_simpledb',
		'domain'	=>	'example_domain',
		'item_name'	=>	1
	));