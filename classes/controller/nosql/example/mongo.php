<?php defined('SYSPATH') or die('No direct script access.');
/**
 *
 *
 * @package		NoSQL
 * @category		Example
 * @category		MongoDB
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 */

class Controller_NoSQL_Example_Mongo extends Controller_NoSQL_Example
{
	public function before ()
	{
		parent::before();

		$this->Mongo = NoSQL::instance('mongo');

		if (array_key_exists('debug', $_GET))
			$this->Mongo->debug(Kohana::DEVELOPMENT);
	}

	public function action_create_collection()
	{
		$collection = $this->request->param('collection');

		echo 'Create Collection ' . $collection . PHP_EOL;

		try
		{
			$result = $this->Mongo->create_store($collection);

			echo 'Result: ';
			var_dump($result);

			if ($result === TRUE) {
				echo 'Collection Has Been Created'. PHP_EOL;
			}
			else {
				echo 'Failed To Create Collection'. PHP_EOL;
			}
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_delete_collection()
	{
		$collection = $this->request->param('collection');

		echo 'Deleting Collection ' . $collection . PHP_EOL;

		try
		{
			$result = $this->Mongo->delete_store($collection);

			echo 'Result: ';
			var_dump($result);

			if ($result === TRUE) {
				echo 'Collection Has Been Deleted'. PHP_EOL;
			}
			else {
				echo 'Failed To Delete Collection'. PHP_EOL;
			}
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_count_items ()
	{
		$collection = $this->request->param('collection');

		echo 'Count items in collection ' . $collection . PHP_EOL;

		try
		{
			$count = $this->Mongo->count($collection);

			echo 'Record Count: ' . $count . PHP_EOL;
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_list()
	{
		echo 'List Collections ' . PHP_EOL;

		try
		{
			$collections = $this->Mongo->list_all();

			print_r($collections);
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_get_item()
	{
		$collection = $this->request->param('collection');
		$item_name = $this->request->param('item_name');

		echo 'Get Item ' . $item_name . ' from collection ' . $collection . PHP_EOL;

		$attributes = array('key1', 'key2', 'key6');

		try
		{
			$item = $this->Mongo->get($collection, $item_name, $attributes);

			print_r($item);
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_get_items()
	{
		$collection = $this->request->param('collection');

		echo 'Get Items from collection ' . $collection . PHP_EOL;

		$query = array('key2' => 'value3');
		$attributes = array('key1', 'key2');

		try
		{
			$items = $this->Mongo->get($collection, $query, $attributes);

			if (count($items) > 0)
			{
				foreach ($items AS $item)
				{
					var_dump($item);
				}
			}
			else
			{
				echo 'No items found'.PHP_EOL;
			}
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_put_item()
	{
		$collection = $this->request->param('collection');

		$item = array(
			'key1'		=>	'value1',
			'key2'		=>	'value2'
		);

		echo 'Putting item to collection ' . $collection . PHP_EOL;
		var_dump($item);

		try
		{
			$options['item'] = $item;

			$result = $this->Mongo->put($collection, $options);

			if ($result === TRUE) {
				echo 'Item Has Been Sent'. PHP_EOL;
			}
			else {
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
		$collection = $this->request->param('collection');

		$query = array(
			'key1'		=>	'value1',
		);

		$updates = array(
			'key2'		=>	'value3'
		);

		echo 'Updating Items in collection ' . $collection . PHP_EOL;
		echo 'Matching: ' . PHP_EOL;
		print_r($query);
		echo 'New Values: ' . PHP_EOL;
		print_r($updates);

		try
		{
			$result = $this->Mongo->update($collection, $query, $updates);

			if ($result === TRUE) {
				echo 'Item Has Been Sent'. PHP_EOL;
			}
			else {
				echo 'Failed To Send Item'. PHP_EOL;
			}
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_delete_item()
	{
		$collection = $this->request->param('collection');

		$query = array(
			'key1'		=>	'value1',
		);

		$options = array(
			'justOne'	=>	true
		);

		echo 'Deleting Item from collection ' . $collection . PHP_EOL;

		try
		{
			$result = $this->Mongo->delete($collection, $query, $options);

			if ($result === TRUE) {
				echo 'Item Has Been Deleted'. PHP_EOL;
			}
			else {
				echo 'Failed To Delete Item'. PHP_EOL;
			}
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function after ()
	{
		unset($this->Mongo);

		parent::after();
	}
}
