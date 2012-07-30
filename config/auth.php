<?php defined('SYSPATH') or die('No direct access allowed.');

return array(

	'driver'			=>	'nosql_mongo',
	'salt'			=>	'SOME-KEY-GOES-HERE',
	'lifetime'		=>	1209600,
	'session_type'		=>	'native',
	'session_key' 		=>	'yc-md-auth',

	// database config group to use for Auth NoSQL driver
	'database'		=>	'default', // nosql db instance name
	'table_name'		=>	'users' // data store name
);
