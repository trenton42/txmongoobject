import txmongo
from bson.objectid import ObjectId, InvalidId
from twisted.internet import defer
from datetime import datetime


class metaMongoObj(type):
    def __new__(meta, classname, bases, classDict):
        classDict['_id'] = mongoidProperty()
        classDict['cdate'] = dateProperty()

        for k, v in classDict.iteritems():
            if not issubclass(v.__class__, mongoProperty):
                continue
            v._name = k
        return type.__new__(meta, classname, bases, classDict)


class mongoProperty(object):

    value = None
    _name = None

    def __init__(self, allowNone=True, default=None):
        self.allowNone = allowNone
        self.default = default
        self.values = {}

    def set(self, value):
        if value is None and self.default is not None:
            value = self.default
        return value

    def get(self, value):
        return value

    def __set__(self, instance, value):
        instance._prop_data[self._name] = self.set(value)
        # self.values[id(instance)] = self.set(value)

    def __get__(self, instance, owner):
        if self._name not in instance._prop_data:
            return self.default
        return self.get(instance._prop_data[self._name])


class boolProperty(mongoProperty):

    def set(self, value):
        return bool(value)


class stringProperty(mongoProperty):

    def __init__(self, maxLength=None, allowNone=True, default=None):
        self.default = default
        if maxLength is not None:
            if maxLength.__class__ is not int or maxLength <= 0:
                maxLength = None

        self.maxLength = maxLength
        self.allowNone = allowNone
        super(stringProperty, self).__init__(allowNone=allowNone, default=default)

    def set(self, value):
        if value is None and self.default is not None:
            value = self.default

        if value.__class__ is unicode:
            value = value.encode('utf-8')
        elif value is None:
            value = None if self.allowNone else ''
        elif value.__class__ is not str:
            value = str(value)
        return value

    def get(self, value):
        if value is not None:
            value = unicode(value, 'utf-8')
        return value


class dateProperty(mongoProperty):
    def set(self, value):
        if value.__class__ is not datetime:
            value = None

        return value


class intProperty(mongoProperty):

    def __init__(self, unsigned=False, allowNone=True, default=None):
        self.unsigned = unsigned
        self.allowNone = allowNone
        self.default = default
        super(intProperty, self).__init__(allowNone=allowNone, default=default)

    def set(self, value):
        if value.__class__ is unicode:
            value = value.encode('utf-8')

        if value.__class__ is int:
            pass
        elif value.__class__ is float:
            value = int(round(value))
        elif value.__class__ is str:
            try:
                value = int(value)
            except ValueError:
                # Some invalid stuff passed in the string.
                value = None
        else:
            # Handle everything else that was passed.
            value = None

        if value is None and self.default is not None:
            value = self.default

        if not self.allowNone and value is None:
            value = 0

        if self.unsigned and value is not None and value < 0:
            value = abs(value)

        return value


class floatProperty(mongoProperty):

    def __init__(self, unsigned=False, allowNone=True, default=None):
        self.default = default
        self.unsigned = unsigned
        self.allowNone = allowNone
        super(floatProperty, self).__init__(allowNone=allowNone, default=default)

    def set(self, value):
        if value.__class__ is unicode:
            value = value.encode('utf-8')

        if value.__class__ is int:
            value = float(value)
        elif value.__class__ is float:
            pass
        elif value.__class__ is str:
            try:
                value = float(value)
            except ValueError:
                # Some invalid stuff passed in the string.
                value = None
        else:
            # Handle everything else that was passed.
            value = None

        if value is None and self.default is not None:
            value = self.default

        if not self.allowNone and value is None:
            value = 0.0

        if self.unsigned and value is not None and value < 0:
            value = abs(value)

        return value


class mongoidProperty(mongoProperty):

    def set(self, value):
        if value is None:
            return None
        try:
            value = ObjectId(value)
        except (TypeError, InvalidId):
            value = None
        return value

    def get(self, value):
        return None if value is None else ObjectId(value)


class referenceProperty(mongoProperty):
    """Creates a reference to another mongo object by storing the _id"""
    def __init__(self, cls, key=None, allowNone=True, multi=False):
        super(referenceProperty, self).__init__(allowNone=allowNone)

        if not issubclass(cls, MongoObj):
            raise ValueError('cls must be subclass of MongoObj')
        self._refCls = cls
        self._key = key

    def set(self, value):

        if value is None or isinstance(value, ObjectId):
            return value

        if isinstance(value, self._refCls):
            return value

        try:
            value = ObjectId(value)
        except InvalidId:
            raise ValueError('%s must be subclass of %s' % (str(value), str(self._refCls.__name__)))

        return value


class dictProperty(mongoProperty):

    def set(self, value):
        if value.__class__ is not dict:
            value = None

        if not self.allowNone and value is None:
            value = {}
        return value


class listProperty(mongoProperty):

    _defaultWrapper = None

    def __init__(self, *args, **kwargs):
        wrapper = kwargs.get('wrapper', None)
        if isinstance(wrapper, type):
            wrapper = wrapper()
        if wrapper is not None and isinstance(wrapper, mongoProperty):
            self._defaultWrapper = wrapper
        if 'wrapper' in kwargs:
            del kwargs['wrapper']
        super(listProperty, self).__init__(*args, **kwargs)

    def set(self, value):
        if value.__class__ is not list:
            value = None
        elif self._defaultWrapper is not None:
            value = map(lambda key: self._defaultWrapper.set(key), value)

        if not self.allowNone and value is None:
            value = {}
        return value

    def _getIds(self, values):
        ''' Get all ObjectIds of members (for serialization) if the base wrapper is referenceProperty '''
        if not isinstance(self._defaultWrapper, referenceProperty) or values is None:
            return values
        out = []
        for i in values:
            if i is None:
                continue
            if isinstance(i, ObjectId):
                out.append(i)
            else:
                out.append(i._id)
        return out


class coordinateProperty(dictProperty):

    def __init__(self, allowNone=False, default={'x': 0, 'y': 0}):
        self.allowNone = allowNone
        self.default = default
        super(coordinateProperty, self).__init__(allowNone=allowNone, default=default)


class MongoSubObj(object):

    _prop_data = {}

    def getValues(self):
        ''' Serialize all of the values into a mongoable dict '''
        out = {}

        for k, v in self.schema.iteritems():
            if isinstance(v, referenceProperty):
                tmp = getattr(self, k)
                if tmp is None or isinstance(tmp, ObjectId):
                    out[k] = tmp
                else:
                    out[k] = tmp._id
            elif isinstance(v, listProperty):
                tmp = getattr(self, k)
                out[k] = v._getIds(tmp)
            elif issubclass(v.__class__, objectProperty):
                out[k] = getattr(self, k).getValues()
            else:
                out[k] = getattr(self, k)

        return out

    def setValues(self, data):
        ''' Set the values of the object recursively '''

        schema = self.schema
        for k, v in data.iteritems():
            if k not in schema:
                continue
            setattr(self, k, v)

    def __iter__(self):
        def iterKeys():
            for i in self.getKeys():
                yield i, getattr(self, i)
        return iterKeys()

    def __len__(self):
        return len(self.getKeys())

    def getKeys(self):
        out = []
        for k, v in self.__class__.__dict__.iteritems():
            if not issubclass(v.__class__, mongoProperty):
                continue
            out.append(k)
        return out

    @defer.inlineCallbacks
    def loadRefs(self):
        ''' Load references that are defined in this object '''

        for k, v in self.schema.iteritems():
            if isinstance(v, listProperty) and isinstance(v._defaultWrapper, referenceProperty):
                val = getattr(self, k)
                if val is None:
                    val = []
                tmp = []
                for i in val:
                    if i is None or isinstance(i, v._defaultWrapper._refCls):
                        continue
                    row = yield v._defaultWrapper._refCls().load(i)
                    tmp.append(row)
                setattr(self, k, tmp)
                continue
            if not isinstance(v, referenceProperty):
                continue
            val = getattr(self, k)
            if val is None or isinstance(val, v._refCls):
                continue
            try:
                tmp = yield v._refCls().load(val)
            except KeyError:
                tmp = None
            setattr(self, k, tmp)

    @property
    def schema(self):
        out = {}
        for i in self.__class__.__mro__:
            if not issubclass(i, MongoSubObj):
                continue
            for k, v in i.__dict__.iteritems():
                if not issubclass(v.__class__, mongoProperty):
                    continue
                out[k] = v
        return out

    def create(self, data):
        ''' Called when the object is first created. All data will be
        passed through this function, and should be returned '''
        return data


class objectProperty(mongoProperty):

    def __init__(self, refClass=None, allowNone=False):
        if not issubclass(refClass, MongoSubObj):
            raise ValueError('refClass must be a subclass of MongoSubObj')
        self.value = refClass()

    def __set__(self, instance, value):
        if self.value:
            self.value.setValues(value)


class MongoObj(MongoSubObj):

    loaded = False
    dbname = 'brndydb'
    __metaclass__ = metaMongoObj
    mongo = txmongo.MongoConnectionPool('127.0.0.1', 27017)

    def __init__(self):
        self._id = None
        self._prop_data = {}
        super(MongoObj, self).__init__()

    @classmethod
    def getCollection(cls):
        # mongo = txmongo.MongoConnectionPool('127.0.0.1', 27017)
        db = getattr(cls.mongo, cls.dbname)
        collection = getattr(db, cls.__name__)
        return collection

    @defer.inlineCallbacks
    def load(self, docid=None):
        # mongo = yield txmongo.MongoConnectionPool('127.0.0.1', 27017)

        if docid is not None and not isinstance(docid, ObjectId):
            # Raises exception if docid is not ObjectId-able
            docid = ObjectId(docid)
        if docid is None:
            defer.returnValue(self)

        db = getattr(self.mongo, self.dbname)
        collection = getattr(db, self.__class__.__name__)

        docs = yield collection.find({'_id': docid}, limit=1)
        if not len(docs):
            raise KeyError('Object id: %s not found' % docid)

        self.setValues(docs[0])

        self.loaded = True

        defer.returnValue(self)

    @classmethod
    def find(cls, search, **kwargs):
        ''' Get a list of all objects in this collection that match _search_'''
        return MongoSet(search, cls, **kwargs)._runQuery()

    @defer.inlineCallbacks
    def save(self):
        # mongo = yield txmongo.MongoConnectionPool('127.0.0.1', 27017)

        db = getattr(self.mongo, self.dbname)
        collection = getattr(db, self.__class__.__name__)

        data = self.getValues()
        if '_id' in data and data['_id'] is None:
            del data['_id']

        if not '_id' in data:
            data = self.create(data)
            data['cdate'] = datetime.today()
            self.cdate = data['cdate']

        result = yield collection.save(data, safe=True)
        if result.__class__ is ObjectId:
            self._id = result
        defer.returnValue(result)

    @classmethod
    def aggregate(cls, spec, **kwargs):
        db = getattr(cls.mongo, cls.dbname)
        collection = getattr(db, cls.__name__)

        return collection.aggregate(spec, **kwargs)


class MongoSet(object):

    _limit = 0
    _skip = 0
    _sort = None
    _data = None
    _queryRun = False
    _result = []

    def __init__(self, search, cls, limit=0, skip=0, sort=None, loadRefs=False):
        self._search = search
        self._class = cls
        self._limit = limit
        self._skip = skip
        self._sort = sort
        self._loadRefs = loadRefs

    def limit(self, num):
        self._limit = num

    def skip(self, num):
        self._skip = num

    def sort(self, obj):
        self._sort = obj

    def __iter__(self):
        for i in self._result:
            # yield self._applyItem(i)
            yield i

    def __len__(self):
        return len(self._result)

    def _afterQuery(self, objs):
        self._result = objs
        return self

    @defer.inlineCallbacks
    def _runQuery(self):
        # mongo = yield txmongo.MongoConnectionPool('127.0.0.1', 27017)
        mongo = MongoObj.mongo
        db = getattr(mongo, self._class.dbname)
        collection = getattr(db, self._class.__name__)
        if self._sort is not None:
            ftr = txmongo.filter.sort(self._sort)
        else:
            ftr = None
        docs = yield collection.find(spec=self._search, limit=self._limit, skip=self._skip, filter=ftr)

        out = []
        for i in docs:
            o = self._applyItem(i)
            if self._loadRefs:
                yield o.loadRefs()
            out.append(o)
        self._result = out
        defer.returnValue(self)

        # load refs (if there are any)
        # if self._loadRefs:
        #     for i in docs:
        #         yield i.loadRefs()
            # schema = self._class().schema
            # for k, v in schema.iteritems():
            #     if not isinstance(v, referenceProperty):
            #         continue
            #     ids = set()
            #     for i in docs:
            #         val = i.get(k, None)
            #         if val is None:
            #             continue
            #         ids.add(val)
            #     if not len(ids):
            #         continue
            #     for j in chunks(list(ids), 100):
            #         tmp = yield v._refCls.find({'_id': {'$in': j}})
            #         _tmp = {}
            #         for i in tmp:
            #             _tmp[i._id] = i
            #         del tmp
            #         for i in docs:
            #             key = i.get(k, None)
            #             if key in _tmp:
            #                 i[k] = _tmp[key]
            #             else:
            #                 i[k] = None

        # self._result = docs
        # defer.returnValue(self)

    def __getitem__(self, index):
        if index.__class__ is not int:
            raise TypeError
        # if index < 0 or index >= len(self._result):
        #     raise IndexError
        # out = self._applyItem(self._result[index])
        # return out
        return self._result[index]

    def _applyItem(self, obj):
        out = self._class()
        out.setValues(obj)
        out.loaded = True
        return out


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    From StackOverflow: http://stackoverflow.com/a/312464/999844
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]
