<?php defined('SYSPATH') or die('No direct script access.');
/**
 *
 *
 * @package		NoSQL
 * @category		Example
 * @category		MongoDB
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 */

Route::set('nosql_admin_mongo', 'nosql_admin_mongo(/<action>(/<collection>(/<item_name>)))', array(
		'collection' => '[^/]+',
		'item_name' => '[^/]+',
	))
	->defaults(array(
		'controller'	=>	'admin_mongo'
	));

class Controller_Nosql_Admin_Mongo extends Controller_Nosql_Admin
{
	public function before ()
	{
		parent::before();

		$this->Mongo = NoSQL::instance('mongo');
	}

	public function action_index()
	{
		try
		{
			$collections = $this->Mongo->list_all();
			$this->template->set('collections', $collections);
		}
		catch (Exception $e)
		{
			die($e);
		}
	}

	public function action_query()
	{
		try
		{
			$errors = array();
			$item_id = (array_key_exists('item_id', $_POST)) ? $_POST['item_id'] : '' ;
			$collection = (array_key_exists('collection', $_POST)) ? $_POST['collection'] : '' ;
			$query = (array_key_exists('query', $_POST)) ? $_POST['query'] : '' ;
			$attributes = (array_key_exists('attributes', $_POST)) ? $_POST['attributes'] : '' ;

			// make sure collection name is passed and not empty
			if (empty($collection)) $errors['collection'] = 'select a collection to query';

			if (count($errors) <= 0)
			{
				$query = (array) json_decode($query);
				$attributes = (array) json_decode($attributes);

				// if item_id is set then query for specific item, otherwise searcg all items using $query
				if (isset($item_id) AND ! empty($item_id))
				{
					$items = $this->Mongo->get($collection, $item_id, $attributes);
				}
				else
				{
					$result = $this->Mongo->get_items($collection, $query, $attributes);

					$items = array();

					foreach ($result AS $item)
					{
						$items[] = (array) $item;
					}
				}

				if (count($items) > 0) {
					if (isset($item_id) AND ! empty($item_id)) {
						// single item retrieved by item_id
						$response = array('status' => 'success', 'name' => $collection, 'result' => $items);
					}
					else {
						// list of items retrieved by query
						$response = array('status' => 'success', 'name' => $collection, 'results' => $items);
					}
				} else {
					$response = array('status' => 'failure', 'response' => 'No results found');
				}
			}
			else
			{
				$response = array('status' => 'error', 'errors' => $errors);
			}
		}
		catch (Exception $e)
		{
			$response = array('status' => 'failure', 'response' => $e->getMessage());
		}

		header('Content-Type: application/json');
		die(json_encode($response));
		return;
	}

	public function action_insert()
	{
		$collection = $this->request->param('collection');

		try
		{
			$errors = array();
			$collection = (array_key_exists('collection', $_POST)) ? $_POST['collection'] : '' ;
			$object = (array_key_exists('object', $_POST)) ? $_POST['object'] : '' ;

			// make sure collection name is passed and not empty
			if (empty($collection)) $errors['collection'] = 'select a collection to insert into';

			if (count($errors) <= 0)
			{
				$item = (array) json_decode($object);

				$options = array();
				$options['item'] = $item;

				$result = $this->Mongo->put($collection, $options);

				if ($result === TRUE) {
					$response = array('status' => 'success', 'name' => $collection);
				} else {
					$response = array('status' => 'failure', 'response' => 'An unexpected error has occurred');
				}
			}
			else
			{
				$response = array('status' => 'error', 'errors' => $errors);
			}
		}
		catch (Exception $e)
		{
			$response = array('status' => 'failure', 'response' => $e->getMessage());
		}

		header('Content-Type: application/json');
		die(json_encode($response));
		return;
	}

	public function action_delete()
	{
		$collection = $this->request->param('collection');
		$item_name = $this->request->param('item_name');

		try
		{
			$errors = array();

			// make sure collection name is passed and not empty
			if (empty($collection)) $errors['collection'] = 'select a collection to insert into';

			// make sure item id is passed also
			if (empty($item_name)) $errors['item_name'] = 'please enter an item _id to delete';

			if (count($errors) <= 0)
			{
				// get item since we need to do a delete that matches the entire object
				// including the item name or _id property
				$item = $this->Mongo->get($collection, new MongoId($item_name));

				// only going to delete one record
				$options = array(
					'justOne'	=>	true
				);

				// now pass item we got to delete method to delete it
				// since we are passing the entire object with _id this should
				// only match one item
				$result = $this->Mongo->delete($collection, $item, $options);

				if ($result === TRUE) {
					$response = array('status' => 'success', 'name' => $collection, 'item_name' => $item_name);
				} else {
					$response = array('status' => 'failure', 'response' => 'An unexpected error has occurred');
				}
			}
			else
			{
				$response = array('status' => 'error', 'errors' => $errors);
			}
		}
		catch (Exception $e)
		{
			$response = array('status' => 'failure', 'response' => $e->getMessage());
		}

		header('Content-Type: application/json');
		die(json_encode($response));
		return;
	}

	public function action_create_collection()
	{
		$collection = $this->request->param('collection');

		try
		{
			$result = $this->Mongo->create_store($collection);

			if ($result === TRUE) {
				$response = array('status' => 'success', 'name' => $collection);
			} else {
				$response = array('status' => 'failure');
			}
		}
		catch (Exception $e)
		{
			$response = array('status' => 'failure', 'response' => $e->getMessage());
		}

		header('Content-Type: application/json');
		die(json_encode($response));
		return;
	}

	public function action_delete_collection()
	{
		$collection = $this->request->param('collection');

		try
		{
			$result = $this->Mongo->delete_store($collection, array(), array());

			if ($result === TRUE) {
				$response = array('status' => 'success', 'name' => $collection);
			} else {
				$response = array('status' => 'failure');
			}
		}
		catch (Exception $e)
		{
			$response = array('status' => 'failure', 'response' => $e);
		}

		header('Content-Type: application/json');
		die(json_encode($response));
		return;
	}
}
