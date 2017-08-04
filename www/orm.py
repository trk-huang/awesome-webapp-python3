import logging;logging.basicConfig(level=logging.INFO)
import asyncio,aiomysql

@asyncio.coroutine
def create_pool(loop,**kw):
	logging.info("create database connection pool...")
	global __pool
	__pool = yield from aiomysql.create_pool(
		host = kw.get('host','localhost'),
		port = kw.get('port',3306),
		user = kw['user'],
		password = kw['password'],
		db = kw['db'],
		charset = kw.get('charset','utf-8'),
		autocommit = kw.get('autocommit',True),
		maxsize = kw.get('maxsize',10),
		minsize = kw.get('minsize',1),
		loop = loop
		)


@asyncio.coroutine
def select(sql,args,size= None):
	log(sql,args)
	global __pool
	with(yield from __pool) as conn:
			cur = yield from conn.cursor(aiomysql.DictCursor)
			yield from cur.execute(sql.replace('?','%s'), args or ())
			if size :
				rs = yield from cur.fetchmany(size)
			else 
				rs = yield from cur.fetchall()
			yield from cur.close()
			logging.info('rows returns %s' % len(rs))
			return rs
		

@asyncio.coroutine
def execute(sql, args):
	log(sql)
	with (yield from __pool) as conn:
		try:
			cur = yield from conn.cursor()
			yield from cur.execute(sql.replace('?','%s'),args)
			affected = cur.rowcount
		except Exception as e:
			raise
		return affected


class Model(dict,metaclass=ModelMetaclass):
	def __init__(self,**kw):
		super(Model,self).__init__(**kw)
	def __getattr__(self,key):
		try:
			return self[key]
		except Exception as e:
			raise AttributeError(r"'Model' object has no attribute '%s'" % key)

	def __setattr__(self, key, value):
		self[key] = value
	
	def getValue(self, key):
		return getattr(self,key,None)

	def getValueOrDefault(self, key):
		value = getattr(self, key, None)
		if  value is None:
				field = self.__mappings__[key]
				if field.default is not None:
					value = field.default() if callable(filed.default) else filed.default
					logging.debug('using default value for %s: %s' % (key, str(value)))
					setattr(self,key, value)
		return value

	@classmethod
	@asyncio.coroutine
	def find(cls,pk):
		'find object by primary key.'
		rs = yield from select('%s where `%s`=?' % (cls.__select__,cls.__primary_key__),[pk],1)
		if len(rs) == 0:
			return None
		return cls(**rs[0])


	@asyncio.coroutine
	def save(self):
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rs = yield from execute(self.__insert__, args)
		if rs != 1:
			logging.warn('failed to insert record: affected rows: %s' % rs)

class Field(object):
	def __init__(self, name, column_type, primary_key, default):
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default


	def __str__(self):
		return '<%s, %s:%s>' % (self.__class__.__name__,self.column_type,self.name)


class ModelMetaclass(type):
	def __new__(cls, name, bases, attrs):
		if name == 'Model':
			return type.__new__(cls,name,bases,attrs)

		tableName = attrs.get('__table__',None) or name
		logging.info('found model: %s (table: %s)' % (name, tableName))	
		mapping = dict()
		fields = []
		primarykey = None
		for k,v in  attrs.items():
			if isinstance(v,Field):
				logging.info(' found mapping: %s ==> %s' % (k,v))
				mapping[k] = v
				if v.primary_key:
					if primarykey:
						raise RuntimeError('Duplicate primary_key for field: %s' % k)
					primarykey = k
				else:
					fields.append(k)	
		if not primarykey:
			raise RuntimeError('primary key not found.')
		for k in mapping.keys():
				attrs.pop(k)
		escaped_fileds = list(map(lambda f:'`%s`' % f),fields)
		attrs['__mappings__'] = mappings
		attrs['__table__'] = tableName
		attrs['__primary_key__'] = primarykey
		attrs['__fields__'] = fields
		attrs['__select__'] = 'select `%s`, %s from `%s`' %(primarykey,', '.join(escaped_fileds),tableName)	
		attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName,','.join(escaped_fileds),primarykey,create_args_string(len(escaped_fileds) + 1))
		attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda
			f: '`%s`=?' % (mapping.get(f).name or f),fields)),primarykey)
		attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName,primarykey)
		return type.__new__(cls,name,bases,attrs)


