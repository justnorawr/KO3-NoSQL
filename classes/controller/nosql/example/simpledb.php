<?php defined('SYSPATH') or die('No direct script access.');
/**
 *
 *
 * @package		NoSQL
 * @category		Example
 * @category		SimpleDB
 * @category		AmazonWebServices
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 */

class Controller_NoSQL_Example_SimpleDB extends Controller_NoSQL_Example
{
	public function before()
	{
		parent::before();

		$this->SimpleDB = NoSQL::instance('simpledb');

		if (array_key_exists('debug', $_GET))
			$this->SimpleDB->debug(Kohana::DEVELOPMENT);
	}

	public function after()
	{
		unset($this->SimpleDB);

		parent::after();
	}

	public function action_create_domain()
	{
		$domain = $this->request->param('domain');

		echo 'Create Domain ' . $domain . PHP_EOL;

		try
		{
			$result = $this->SimpleDB->create_store($domain);

			echo 'Result: ';
			var_dump($result);

			if ($result === TRUE) {
				echo 'Domain Has Been Created'. PHP_EOL;
			} else {
				echo 'Failed To Create Domain'. PHP_EOL;
			}
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_delete_domain()
	{
		$domain = $this->request->param('domain');

		echo 'Deleted Domain ' . $domain . PHP_EOL;

		try
		{
			$result = $this->SimpleDB->delete_store($domain);

			echo 'Result: ';
			var_dump($result);

			if ($result === TRUE) {
				echo 'Domain Has Been Deleted'. PHP_EOL;
			} else {
				echo 'Failed To Delete Domain'. PHP_EOL;
			}
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_describe_domain()
	{
		$domain = $this->request->param('domain');

		echo 'Describe Domain ' . $domain . PHP_EOL;

		try
		{
			$response = $this->SimpleDB->describe($domain);

			print_r($response);
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_count_items()
	{
		$domain = $this->request->param('domain');

		echo 'Count Items in Domain ' . $domain . PHP_EOL;

		try
		{
			$count = $this->SimpleDB->count($domain);

			echo 'Record Count: ' . $count . PHP_EOL;
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_list()
	{
		echo 'List Domains ' . PHP_EOL;

		try
		{
			$tables = $this->SimpleDB->list_all();

			print_r($tables);
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_get_item()
	{
		$domain = $this->request->param('domain');
		$item_name = $this->request->param('item_name');

		echo 'Get Item ' . $item_name . ' from domain ' . $domain . PHP_EOL;

		$attributes = array('key1', 'key2', 'key6');

		try
		{
			$item = $this->SimpleDB->get($domain, $item_name, $attributes);

			print_r($item);
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_get_items()
	{
		$domain = $this->request->param('domain');

		// @todo batch get items - not implemented yet
	}

	public function action_put_item()
	{
		$domain = $this->request->param('domain');
		$item_name = $this->request->param('item_name');

		$item = array(
			'item_name'	=>	$item_name,
			'replace'	=>	false,
			'key1'		=>	'value1',
			'key2'		=>	'value2'
		);

		echo 'Putting Item To domain ' . $domain . PHP_EOL;
		var_dump($item);

		try
		{
			$result = $this->SimpleDB->put($domain, $item);

			if ($result === TRUE) {
				echo 'Item Has Been Sent'. PHP_EOL;
			} else {
				echo 'Failed To Send Item'. PHP_EOL;
			}
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_update_item()
	{
		$domain = $this->request->param('domain');
		$item_name = $this->request->param('item_name');

		$item = array(
			'item_name'	=>	$item_name,
			'key1'		=>	'value1',
			'key2'		=>	'value2',
			'key6'		=>	'value6'
		);

		echo 'Putting Item To domain ' . $domain . PHP_EOL;
		print_r($item);

		try
		{
			$result = $this->SimpleDB->update($domain, $item);

			if ($result === TRUE) {
				echo 'Item Has Been Sent'. PHP_EOL;
			} else {
				echo 'Failed To Send Item'. PHP_EOL;
			}
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_query()
	{
		$domain =$this->request->param('domain');

		$time = 1334772600;

		echo 'Current Time: ' . date('m-d-Y H:i:s', $time) . PHP_EOL;

		$results = $this->SimpleDB->query($domain);

		print_r($results);
	}

	public function action_delete_item()
	{
		$domain = $this->request->param('domain');
		$item_name = $this->request->param('item_name');

		echo 'Deleting Item from domain ' . $domain . PHP_EOL;

		try
		{
			$result = $this->SimpleDB->delete($domain, $item_name);

			if ($result === TRUE) {
				echo 'Item Has Been Deleted'. PHP_EOL;
			} else {
				echo 'Failed To Delete Item'. PHP_EOL;
			}
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}
}
