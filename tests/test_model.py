from txmongoobject import model
from twisted.trial import unittest
from twisted.internet import defer
from datetime import datetime
from bson.objectid import ObjectId


model.MongoObj.dbname = 'test_database'

class CollectionObject(model.MongoObj):
	testString = model.stringProperty()
	testInt = model.intProperty()
	testFloat = model.floatProperty()
	testBool = model.boolProperty()
	testDate = model.dateProperty()


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

		# Reload object from database

		newobj = CollectionObject()
		yield newobj.load(col._id)

		self.assertTrue(newobj.loaded)
		self.assertEqual(newobj._id, col._id)

		newobj.testBool = True
		
		self.assertIn('testBool', newobj._prop_dirty)
		self.assertNotIn('testString', newobj._prop_dirty)
		self.assertNotIn('testDate', newobj._prop_dirty)
		self.assertNotIn('testFloat', newobj._prop_dirty)
		self.assertNotIn('testInt', newobj._prop_dirty)

		yield newobj.save()

		self.assertEqual(len(newobj._prop_dirty), 0)

		
