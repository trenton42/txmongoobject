from txmongoobject import model
from twisted.trial import unittest
from twisted.internet import defer
from datetime import datetime
from bson.objectid import ObjectId


model.MongoObj.dbname = 'test_database'

class Fragment(model.MongoObj):
	testValue = model.stringProperty()


class CollectionObject(model.MongoObj):
	testString = model.stringProperty()
	testInt = model.intProperty()
	testFloat = model.floatProperty()
	testBool = model.boolProperty()
	testDate = model.dateProperty()
	testRef = model.referenceProperty(Fragment)
	testRefList = model.listProperty(wrapper=model.referenceProperty(Fragment))


class TestCollection(unittest.TestCase):

	timeout = 15

	@defer.inlineCallbacks
	def setUp(self):
		model.MongoObj.mongo = yield model.txmongo.MongoConnection('127.0.0.1', 27017)


	@defer.inlineCallbacks
	def tearDown(self):
		yield model.MongoObj.mongo.disconnect()

	def test_default(self):
		col = CollectionObject()
		self.assertIdentical(col.testBool, None)
		self.assertIdentical(col.testString, None)
		self.assertIdentical(col.testInt, None)
		self.assertIdentical(col.testFloat, None)
		self.assertIdentical(col.testDate, None)

	@defer.inlineCallbacks
	def test_reference(self):
		col = CollectionObject()
		frag = Fragment()
		frag.testValue = 'This is a test'

		yield frag.save()

		col.testRef = frag

		yield col.save()

		self.assertEqual(col.testRef, frag)

		newobj = CollectionObject()
		yield newobj.load(col._id)

		self.assertEqual(newobj.testRef, frag)

	@defer.inlineCallbacks
	def test_load(self):
		col = CollectionObject()
		yield col.save()

		self.assertTrue(col.loaded)

		frag = Fragment()
		yield frag.save()

		newcol = yield CollectionObject.findOne(col._id)

		self.assertTrue(newcol.loaded)

		self.assertEqual(col, newcol)


	@defer.inlineCallbacks
	def test_comparison(self):
		col = CollectionObject()
		yield col.save()

		dup = CollectionObject()
		yield dup.load(col._id)
		
		other = CollectionObject()
		yield other.save()

		self.assertNotEqual(col, other)
		self.assertEqual(col, dup)
		self.assertEqual(col, dup._id)

	@defer.inlineCallbacks
	def test_remove(self):
		obj = CollectionObject()
		obj.testString = 'this is a test'
		yield obj.save()

		self.assertTrue(obj.loaded)

		_id = obj._id
		yield obj.remove()
		
		self.assertFalse(obj.loaded, 'Object should not be loaded after being deleted')

		tmp = yield CollectionObject.find({'_id': _id})

		self.assertEqual(len(tmp), 0)


	@defer.inlineCallbacks
	def test_dirty(self):
		col = CollectionObject()
		col.testBool = False
		col.testString = 'Some string'
		col.testFloat = 1.1
		col.testInt = 5
		col.testDate = datetime.today()
		self.assertFalse(col.loaded, 'Ensure this object has not been loaded')
		self.assertIdentical(col._id, None, 'A new object must not have an _id set')
		yield col.save()

		self.assertIsInstance(col._id, ObjectId, 'Once an object is saved, _id must be an ObjectId')

		col.testFloat = 4.0

		self.assertIn('testFloat', col._prop_dirty)

		yield col.save()

		# Reload object from database

		newobj = CollectionObject()
		yield newobj.load(col._id)

		self.assertTrue(newobj.loaded)
		self.assertEqual(newobj._id, col._id)

		newobj.testBool = True

		for i in ('testString', 'testDate', 'testFloat', 'testRef', 'testInt', 'testRefList'):
			# Set all attributes to the same value. Should not set the dirty flag
			setattr(newobj, i, getattr(newobj, i))
		
		self.assertIn('testBool', newobj._prop_dirty)
		self.assertNotIn('testString', newobj._prop_dirty)
		self.assertNotIn('testDate', newobj._prop_dirty)
		self.assertNotIn('testFloat', newobj._prop_dirty)
		self.assertNotIn('testInt', newobj._prop_dirty)
		self.assertNotIn('testRef', newobj._prop_dirty)
		self.assertNotIn('testRefList', newobj._prop_dirty)

		yield newobj.save()

		newobj.testString = 'well, that is some new string'

		self.assertIn('testString', newobj._prop_dirty)

		yield newobj.save()

		self.assertEqual(len(newobj._prop_dirty), 0)

		
