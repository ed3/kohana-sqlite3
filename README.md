# SQLite3 driver for Kohana v.3.x (koseven)

## Settings
- copy files in modules folder
- in bootstrap activate database module
- in config database:
```php
'type'=> 'SQLite3',
	'connection'=> array(
		'database'=> APPPATH.'my.db3',
	),
```
That's all!
