<?php defined('SYSPATH') or die('No direct script access.');
/**
 *
 *
 * @package		NoSQL
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 */
abstract class Kohana_NoSQL_AWS extends Kohana_NoSQL
{
	/**
	 * Loads AWS PHP SDK
	 *
	 * [!!] This method cannot be accessed directly, you must use [NoSQL::instance].
	 *
	 * @return  void
	 */
	protected function __construct($name, array $config)
	{
		require_once Kohana::find_file('vendor/Amazon', 'sdk.class');

		parent::__construct($name, $config);
	}
}
