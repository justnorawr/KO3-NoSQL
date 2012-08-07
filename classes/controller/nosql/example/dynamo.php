<?php defined('SYSPATH') or die('No direct script access.');
/**
 *
 *
 * @package		NoSQL
 * @category		Example
 * @category		DynamoDB
 * @category		AmazonWebServices
 * @author		Nicholas Curtis		<nich.curtis@gmail.com>
 */

class Controller_NoSQL_Example_Dynamo extends Controller_NoSQL_Example
{
	public function before ()
	{
		parent::before();

		$this->Dynamo = NoSQL::instance('dynamo');

		if (array_key_exists('debug', $_GET))
			$this->Dynamo->debug(Kohana::DEVELOPMENT);
	}

	public function after ()
	{
		unset($this->Dynamo);

		parent::after();
	}

	public function action_create_table()
	{
		$table =$this->request->param('table');
		$read =(int) $this->request->param('read');
		$write =(int) $this->request->param('write');

		echo 'Create Table ' . $table . PHP_EOL;

		$options = array(
			'KeySchema' => array(
				'HashKeyElement' => array(
					'AttributeName' => 'ID',
					'AttributeType' => AmazonDynamoDB::TYPE_NUMBER
				),
				 'RangeKeyElement' => array(
				 	'AttributeName' => 'Date',
				 	'AttributeType' => AmazonDynamoDB::TYPE_NUMBER
				)
			),
			'ProvisionedThroughput' => array(
				'ReadCapacityUnits' => $read,
				'WriteCapacityUnits' => $write
			)
		);

		echo 'Creating Table With Options' . PHP_EOL;
		print_r($options);

		try
		{
			$result = $this->Dynamo->create_store($table, $options);

			echo 'Result: '; var_dump($result);

			if ($result === TRUE) {
				echo 'Table Has Been Created'. PHP_EOL;
			} else {
				echo 'Failed To Create Table'. PHP_EOL;
			}
		}
		catch (Kohana_NoSQL_Limit_Exception $e)
		{
			echo 'Table created but not active within poll limit'. PHP_EOL;
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_update_table()
	{
		$table =$this->request->param('table');
		$read =(int) $this->request->param('read');
		$write =(int) $this->request->param('write');

		echo 'Update Table ' . $table . PHP_EOL;

		$options = array(
			'ProvisionedThroughput' => array(
				'ReadCapacityUnits' => $read,
				'WriteCapacityUnits' => $write
			)
		);

		echo 'Updating Table With Options' . PHP_EOL;
		print_r($options);

		try
		{
			$result = $this->Dynamo->update_store($table, $options);

			if ($result === TRUE) {
				echo 'Table Has Been Updated'. PHP_EOL;
			} else {
				echo 'Failed To Update Table'. PHP_EOL;
			}
		}
		catch (Kohana_NoSQL_Limit_Exception $e)
		{
			echo 'Table updated but not active within poll limit'. PHP_EOL;
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_delete_table()
	{
		$table =$this->request->param('table');

		echo 'Delete Table ' . $table . PHP_EOL;

		try
		{
			$result = $this->Dynamo->delete_store($table);

			if ($result === TRUE) {
				echo 'Table Has Been Deleted'. PHP_EOL;
			} else {
				echo 'Failed To Delete Table'. PHP_EOL;
			}
		}
		catch (Kohana_NoSQL_Limit_Exception $e)
		{
			echo 'Table deleted but not confirmed within poll limit'. PHP_EOL;
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_describe_table()
	{
		$table =$this->request->param('table');

		echo 'Describe Table ' . $table . PHP_EOL;

		try
		{
			$response = $this->Dynamo->describe($table);

			print_r($response);
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_count_items()
	{
		$table =$this->request->param('table');

		echo 'Count Items in Table ' . $table . PHP_EOL;

		try
		{
			$count = $this->Dynamo->count($table);

			echo 'Record Count: ' . $count . PHP_EOL;
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_list()
	{
		echo 'List Tables ' . PHP_EOL;

		try
		{
			$tables = $this->Dynamo->list_all();

			print_r($tables);
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_get_item()
	{
		$table =$this->request->param('table');
		$hash =(int) $this->request->param('hash');
		$range =(int) $this->request->param('range');

		echo 'Get Item ' . $hash .  '-'.$range.' from table ' . $table . PHP_EOL;

		$options = array(
			'AttributesToGet' => array('key1', 'key2'),
			'ConsistentRead' => false
		);

		try
		{
			$item = $this->Dynamo->get($table, array($hash, $range), $options);

			var_dump($item);
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_get_items()
	{
		$table =$this->request->param('table');

		echo 'Batch Get Items 1, 2, 3, 4 ' . PHP_EOL;

		$options = array(
			'AttributesToGet' => array('key1', 'key2'),
			'ConsistentRead' => true
		);

		try
		{
			$item = $this->Dynamo->get_items($table, array(1, 2, 3, 4));

			print_r($item);
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_put_item()
	{
		$table =$this->request->param('table');
		$hash =(int) $this->request->param('hash');
		$range =(int) $this->request->param('range');

		$item = array(
			 'ID'		=>	$hash,
			 'Date'		=>	$range,
			'key1'		=>	'value1',
			'key2'		=>	'value2'
		);

		echo 'Putting Item To table ' . $table . PHP_EOL;
		print_r($item);

		try
		{
			$result = $this->Dynamo->put($table, $item);

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
		$table =$this->request->param('table');
		$hash =(int) $this->request->param('hash');
		$range =$this->request->param('range');

		echo 'Update Item ' . $hash . '-' . $range . PHP_EOL;

		$options = array(
			'AttributeUpdates' => array(
				'key1' => array(
					'Action' => AmazonDynamoDB::ACTION_PUT,
					'Value' => array(AmazonDynamoDB::TYPE_STRING => 'updated-value1')
				),
				'key2' => array(
					'Action' => AmazonDynamoDB::ACTION_DELETE
				),
				'key3' => array(
					'Action' => AmazonDynamoDB::ACTION_ADD,
					'Value' => array(AmazonDynamoDB::TYPE_ARRAY_OF_STRINGS => array('sub-value3'))
				),
			),
			'Expected' => array(
				'key1' => array(
					'Value' => array( AmazonDynamoDB::TYPE_STRING => 'value2' )
				)
			)
		);

		echo 'Updating Item in table ' . $table . ' with options' . PHP_EOL;
		print_r($options);

		try
		{
			$result = $this->Dynamo->update($table, array($hash, $range), $options);

			if ($result === TRUE) {
				echo 'Item Has Been Updated'. PHP_EOL;
			} else {
				echo 'Failed To Update Item'. PHP_EOL;
			}
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_query()
	{
		try
		{
			$table =$this->request->param('table');
			$hash =(int) $this->request->param('hash');

			$time = 1334772600;

			echo 'Current Time: ' . date('m-d-Y H:i:s', $time) . PHP_EOL;

			$options = array(
				'ConsistentRead'	=>	TRUE,
				'AttributesToGet'	=>	array('key1', 'key2', 'key6'),
				'HashKeyValue'		=>	$hash
			);

			echo 'Query by hash on table ' . $table . ' with options' . PHP_EOL;
			print_r($options);

			$results = $this->Dynamo->query($table, $options);

			print_r($results);
		}
		catch (Exception $e)
		{
			echo $e->getMessage();
		}
	}

	public function action_delete_item()
	{
		$table =$this->request->param('table');
		$hash =(int) $this->request->param('hash');
		$range =$this->request->param('range');

		echo 'Deleting Item from table ' . $table . PHP_EOL;

		try
		{
			$result = $this->Dynamo->delete($table, array($hash, $range));

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
