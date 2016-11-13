import platform
from itertools import chain
from threading import Thread
from unittest.case import skipIf

from requests.packages.urllib3.packages import six

from eventsourcing.domain.services.notification_log import append_item_to_notification_log
from eventsourcing.infrastructure.event_sourced_repos.log_repo import LogRepo
from eventsourcing.infrastructure.event_sourced_repos.notificationlog_repo import NotificationLogRepo
from eventsourcing.infrastructure.event_sourced_repos.sequence import SequenceRepo
from eventsourcing.interface.notification_feed import NotificationFeed, AtomNotificationFeed, NotificationFeedReader, \
    AtomNotificationFeedReader
from eventsourcing.tests.unit_test_cases import AppishTestCase
from eventsourcing.tests.unit_test_cases_cassandra import CassandraRepoTestCase
from eventsourcing.tests.unit_test_cases_python_objects import PythonObjectsRepoTestCase
from eventsourcing.tests.unit_test_cases_sqlalchemy import SQLAlchemyRepoTestCase


class NotificationFeedTestCase(AppishTestCase):
    def _test_get_items(self):
        # Build a log.
        notification_log_repo = NotificationLogRepo(self.event_store)
        log_repo = LogRepo(self.event_store)
        sequence_repo = SequenceRepo(event_store=self.event_store)
        notification_log = notification_log_repo.get_or_create(
            log_name='log1',
            sequence_size=10,
        )
        for i in range(13):
            item = 'item{}'.format(i + 1)
            append_item_to_notification_log(notification_log, item, sequence_repo, log_repo, self.event_store)

        # Get pages.
        feed = NotificationFeed(notification_log, sequence_repo, log_repo, self.event_store, doc_size=5)

        items = feed.get_items('current')
        self.assertEqual(len(items), 3, items)

        items = feed.get_items('1,5')
        self.assertEqual(len(items), 5, items)

        items = feed.get_items('6,10')
        self.assertEqual(len(items), 5, items)

        items = feed.get_items('11,15')
        self.assertEqual(len(items), 3, items)

        items = feed.get_items('16,20')
        self.assertEqual(len(items), 0, items)

        # Add some more items.
        for i in range(13, 24):
            item = 'item{}'.format(i + 1)
            append_item_to_notification_log(notification_log, item, sequence_repo, log_repo, self.event_store)

        items = feed.get_items('current')
        self.assertEqual(len(items), 4, items)

        items = feed.get_items('11,15')
        self.assertEqual(len(items), 5, items)

        items = feed.get_items('16,20')
        self.assertEqual(len(items), 5, items)

        items = feed.get_items('21,25')
        self.assertEqual(len(items), 4, items)

        items = feed.get_items('26,30')
        self.assertEqual(len(items), 0, items)

        # Check sequence size must be divisible by doc size.
        with self.assertRaises(ValueError):
            NotificationFeed(notification_log, sequence_repo, log_repo, self.event_store, doc_size=6)

        # Check the doc ID must match the doc size.
        feed = NotificationFeed(notification_log, sequence_repo, log_repo, self.event_store, doc_size=5)
        with self.assertRaises(ValueError):
            feed.get_items('1,2')

        # Check the doc ID must be aligned to the doc size.
        feed = NotificationFeed(notification_log, sequence_repo, log_repo, self.event_store, doc_size=5)
        with self.assertRaises(ValueError):
            feed.get_items('2,6')

    def _test_get_doc(self):
        # Check can update from current, back to first, and forward to last.

        # Build a notification log.
        notification_log_repo = NotificationLogRepo(self.event_store)
        log_repo = LogRepo(self.event_store)
        sequence_repo = SequenceRepo(event_store=self.event_store)
        notification_log = notification_log_repo.get_or_create(
            log_name='log1',
            sequence_size=10,
        )
        # Add some items to the log.
        for i in range(13):
            item = 'item{}'.format(i + 1)
            append_item_to_notification_log(notification_log, item, sequence_repo, log_repo, self.event_store)

        # Create the feed object.
        doc_size = 5
        doc_id = 'current'
        feed = NotificationFeed(
            notification_log=notification_log,
            sequence_repo=sequence_repo,
            log_repo=log_repo,
            event_store=self.event_store,
            doc_size=doc_size,
        )
        doc = feed.get_doc(doc_id)
        all_docs = []
        while 'previous' in doc:
            # Get the previous document.
            doc_id = doc['previous']

            # Create the feed object again.
            doc = feed.get_doc(doc_id)

        # Assume we got the first document.
        all_docs.append(doc)

        # Get all the subsequent documents.
        while 'next' in doc:
            doc_id = doc['next']
            doc = feed.get_doc(doc_id)
            all_docs.append(doc)

        # Check there are three docs.
        self.assertEqual(len(all_docs), 3)

        # Check the docs have all the items.
        all_items = list(chain(*[doc.get('items') for doc in all_docs]))
        self.assertEqual(len(all_items), 13, all_items)
        for i in range(13):
            self.assertEqual(all_items[i], 'item{}'.format(i + 1))

    def _test_notification_feed_reader(self):
        # Build a notification log.
        notification_log_repo = NotificationLogRepo(self.event_store)
        log_repo = LogRepo(self.event_store)
        sequence_repo = SequenceRepo(event_store=self.event_store)
        log_name = 'log1'
        notification_log = notification_log_repo.get_or_create(
            log_name=log_name,
            sequence_size=10,
        )
        # Add some items to the log.
        for i in range(13):
            item = 'item{}'.format(i + 1)
            append_item_to_notification_log(notification_log, item, sequence_repo, log_repo, self.event_store)

        # Construct a feed object.
        feed = NotificationFeed(
            notification_log=notification_log,
            sequence_repo=sequence_repo,
            log_repo=log_repo,
            event_store=self.event_store,
            doc_size=5,
        )

        # Use a feed reader to read the feed.
        feed_reader = NotificationFeedReader(feed)
        self.assertEqual(len(list(feed_reader.get_items())), 13)

        # Add some more items to the log.
        for i in range(13, 21):
            item = 'item{}'.format(i + 1)
            append_item_to_notification_log(notification_log, item, sequence_repo, log_repo, self.event_store)

        # Use a feed reader to read the feed.
        feed_reader = NotificationFeedReader(feed)
        self.assertEqual(len(list(feed_reader.get_items())), 21)

        # Read items after last item number.
        self.assertEqual(len(list(feed_reader.get_items(last_item_num=1))), 20)
        self.assertEqual(len(list(feed_reader.get_items(last_item_num=2))), 19)
        self.assertEqual(len(list(feed_reader.get_items(last_item_num=3))), 18)
        self.assertEqual(len(list(feed_reader.get_items(last_item_num=19))), 2)
        self.assertEqual(len(list(feed_reader.get_items(last_item_num=20))), 1)
        self.assertEqual(len(list(feed_reader.get_items(last_item_num=21))), 0)
        self.assertEqual(len(list(feed_reader.get_items(last_item_num=22))), 0)
        self.assertEqual(len(list(feed_reader.get_items(last_item_num=23))), 0)

        # Check last item numbers less than 1 cause a value errors.
        with self.assertRaises(ValueError):
            list(feed_reader.get_items(last_item_num=-1))

        with self.assertRaises(ValueError):
            list(feed_reader.get_items(last_item_num=0))

        # Use a feed reader to read the feed with a larger doc size.
        feed_reader = NotificationFeedReader(feed)
        self.assertEqual(len(list(feed_reader.get_items())), 21)

    # Todo: Rework the atom stuff to use the atom package from the gdata distribution.
    @skipIf(platform.python_implementation() == 'PyPy', "The FeedGenerator uses lxml which doesn't work with PyPI.")
    @skipIf(six.PY3, "The HTTP libs don't work with Python3.")
    def test_atom_client_with_server(self):
        # Build a notification log.
        notification_log_repo = NotificationLogRepo(self.event_store)
        log_repo = LogRepo(self.event_store)
        sequence_repo = SequenceRepo(event_store=self.event_store)
        log_name = 'log1'
        notification_log = notification_log_repo.get_or_create(
            log_name=log_name,
            sequence_size=30,
        )
        # Add some items to the log.
        for i in range(13):
            item = 'item{}'.format(i + 1)
            append_item_to_notification_log(notification_log, item, sequence_repo, log_repo, self.event_store)

        # Start a simple server.
        from wsgiref.util import setup_testing_defaults
        from wsgiref.simple_server import make_server
        base_url = 'http://127.0.0.1:8000/notifications/'

        def simple_app(environ, start_response):
            setup_testing_defaults(environ)
            status = '200 OK'
            headers = [('Content-type', 'text/plain; charset=utf-8')]
            start_response(status, headers)

            # Extract log name and doc ID from path info.
            log_name, doc_id = environ['PATH_INFO'].strip('/').split('/')[-2:]
            notification_log = notification_log_repo[log_name]

            # Return the atom feed notification doc.
            atom_feed = AtomNotificationFeed(
                base_url=base_url,
                notification_log=notification_log,
                sequence_repo=sequence_repo,
                log_repo=log_repo,
                event_store=self.event_store,
                doc_size=3,
            )
            return atom_feed.get_doc(doc_id=doc_id)

        httpd = make_server('', 8000, simple_app)
        print("Serving on port 8000...")
        thread = Thread(target=httpd.serve_forever)
        thread.setDaemon(True)

        try:
            thread.start()
            # Use atom feed reader to read all items in the feed.
            feed_reader = AtomNotificationFeedReader(base_url, log_name)
            items = list(feed_reader.get_items(last_item_num=5))

            # Check we got all the items after item 5.
            self.assertEqual(len(items), 8)
            self.assertEqual(items[0], 'item6')

        finally:
            httpd.shutdown()
            thread.join()


class TestNotificationFeedWithPythonObjects(PythonObjectsRepoTestCase, NotificationFeedTestCase):
    pass


class TestNotificationFeedWithCassandra(CassandraRepoTestCase, NotificationFeedTestCase):
    pass


class TestNotificationFeedWithSQLAlchemy(SQLAlchemyRepoTestCase, NotificationFeedTestCase):
    pass
