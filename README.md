Generic feed processing
=======================

This library implements persistent and consistent processing of feed data without making assumptions of a feed's structure or source. In this context a feed can represent any chain of data that can be implemented as the `FeedEndpoint` interface of the *feed-api* module. For convenience, this library already implements this interface for different standard feed formats:

- Regular Atom feed (Atom feeds with fixed page ids)
- Offset-based Atom feeds (Atom feeds with dynamic page ids based on a monotonically increasing numbers for entries)
- Name-based Atom feeds (Atom feeds with dynamic page ids based on entry names)
- Atom Hopper (http://atomhopper.org)
- SQL tables using a column as sorting property
- Kafka topics
- Skatteetaten's Skattekiste format
- Skatteetaten's Saksmappe format
- Skatteetaten's AA-Register format
- Skatteetaten's Partsregister v3 format
- An in-memory feed implementation (for testing purposes)

Using this library, feeds of any source can be iterated in either forwards (oldest to newest) or backwards (newest to oldest) direction. The library takes responsibility for reading a feed in the specified direction and to assure that entries are only read a single time while new data is added over time. If a feed is read forwards, new entries will simply be read in their incoming order. In backwards direction, new entries will be read in reverse until the last known entry is discovered. If an application fails during reading a feed, transactional support is offered for updating the feed reader's state and to resume reading a feed from the last known position. It is also possible to reprocess a feed if processing fails during reverse reading where any previously read entry is explicitly marked.

A feed can be consumed by implementing the `FeedConsumer` interface that notifies the implementor of any new entries that are discovered in the order of their discovery. The callback interface also allows for transactional updates of the feed's state.

All standard feed endpoints interact with the `FeedHttpClient` interface to avoid strong coupling to a specific HTTP client implementation.

Feed concept
------------

Considering a feed with three pages:

> |a,b| -> |c,d| -> |e|

a forward reading feed of this library would notify the consumer interface of discovering each of the three page with their respective entries. If new entries are later added to the last page as in

> |a,b| -> |c,d| -> |e,f| -> |g|

the feed processor would notify the consumer of having discovered the new entry *f* on the previously visited page and *g* on a new page.

If the feed was processed backwards, the same would occur only that the feed pages would be processed in the opposite direction. This can be helpful if a consumer is only interested in the newest data that is found on a feed where old data is discarded.

Simple example
--------------

A feed is processed by creating an instance of FeedManager of the *feed-reader* module. It takes a *FeedEndpoint* as one of its arguments. 

As an example, the following FeedEndpoint implements reading of an Atom feed where any entry is represented as the feed entry's unique id: 

```java
AtomFeedEndpoint<String> endpoint = new AtomFeedEndpoint<>(
  new URL("http://some.feed"), // feed endpoint
  HttpClients.createDefault(), // Apache HttpClient
  true, // do not treat 404 HTTP status as empty feed
  (page, entry) -> entry.getUri() // Instances of the ROME Atom library
);
```

Additionally, feed positions are persisted into a repository. Typically, the state is written to an SQL database which is implemented by the *feed-state-persistent* module. For testing, an `InMemoryFeedRepository` is included in the *feed-memory* module:

```java
InMemoryFeedRepository<AtomFeedLocation, String> repository = new InMemoryFeedRepository<>();
```

As arguments for it's type variables, the repository takes the type of the feed's location implementation and the name of the identifier of a feed that is read, typically a name as string.

Finally, a trivial implementation of a `FeedConsumer` can be implemented as a lambda expression:

```java
FeedConsumer<AtomFeedLocation, AtomFeedEntry, Void> consumer = 
  (commitment, location, direction, completed, entries) -> {
   System.out.println("Reading feed at " + location // The current AtomFeedLocation of the feed
     + " in " + direction // an enum representing the current reading direction
     + " where " + entries.size() + " entries" // any new AtomFeedEntry found at this location
     + " which is the last page? " + completed); // true if there currently are no additional pages
    commitment.accept(null); // commits the current page as successfully processed
}
```

The consumer can choose to commit the current feed processing state where the commitment's argument is determined by the `FeedRepository` being used. In the above case, no argument is required for the `InMemoryFeedRepository` that will be used whereas the `JdbcFeedRepository` would require a Java SQL `Connection` instance to represent the current transaction. The `FeedConsumer` interface also offers other life-cycle callback methods that can be implemented optionally.

After setting up, feed endpoint, feed consumer and a feed repository, a feed can be processed by assembling the previous parts using an instance of `FeedManager`:

```java
ExecutorService service = Executors.newSingleThreadExecutor(); // the executor to run the feed.
FeedProcessor<String> feed = new FeedManager<>(
  "my-feed-name", // The name to identify feeds in the supplied repository, if persistent
  repository,
  service,
  endpoint,
  FeedReader.BACKWARDS, // read the feed in backwards direction
  FeedContinuation.RESUMING, // if an error occurs during backwards reading, reading will be resumed at the last page
  false, // do not abort feed reading upon an error
  TimeUnit.SECONDS.toMillis(10) // pause time before polling for new entries at feed end or after error
);
```

With this instance of `FeedProcessor`, feed reading can be started and aborted using the interface's methods. The following operations are supported:

- *start*: begins processing the feed, returns false if the feed was already started. If the feed could not be started within the given time interval, an exception is thrown.
- *stop*: stops processing the feed, returns false if the feed is not currently processed. If the feed did not stop within the given time interval, an exception is thrown.
- *reset*: resets the feed as if it was never run. If the feed is currently active, it is stopped before doing so. If the feed cannot be stopped and reset within the specified timeout, an exception is thrown.
- *complete*: forwards the feed to its newest page. If the feed is currently active, it is stopped before doing so. If the feed cannot be stopped and forwarded within the specified timeout, an exception is thrown.
  
All operations are interruptable and thread-safe. However, a feed repository must not be shared for the same pointer in two or more instances of a `FeedManager`. A `FeedConsumer` is not required to be thread-safe unless it is provided to several `FeedManager`s.

Feed reading can be applied either forwards or backwards:
- `FORWARD`: The feed is read from the oldest to the newest entry. If no more entries are found, the feed pauses and continues reading the feed from the latest known position. If a page needs to be reread, all entries that were already supplied to the feed consumer are dropped from the list of entries that is handed to the consumer.
- `BACKWARD`: The feed is read from the newest to the oldest unknown entry. On the first run, the feed reads from the last entry to the beginning of the feed. On the second run, it reads from the last additional entry to the previous last entry. If pages are revisited in this process, any previous entries are dropped before supplying them to the consumer.
- `BACKWARD_INITIAL_ONLY`: The feed is read backwards once from the last entry to the beginning of the feed. After that, the feed is read forwards beginning after the previous last entry.
- `BACKWARD_ONCE_ONLY`: The feed is read backwards from the last entry to the first entry and never reads an entry after this. This implies that the feed will no longer issue results unless the feed pointer is manually deleted.

A feed continuation determines the manager's behavior in case of a feed reading being aborted during backwards reading:

- `REPEATING`: The feed is reread starting from the same location the last backwards reading started from. All entries that already were discovered are supplied once again to the `FeedConsumer` but with a `FeedDirection` `RECOVERY`. Once the last committed location is discovered, all entries after it will be supplied with the `BACKWARD` feed direction. Doing so, it becomes possible to handle duplicates in a specific way if such reprocessing is required. 
- `RESUMING`: The backwards reading resumes is read from the last committed location.
- `FAILING`: A failed backwards reading stops all processing until an explicit measure (resetting or completing) is applied manually.
    
Note that it is possible to change the feed direction when creating a new instance of `FeedManager`. If a backwards reading is not completed, the continuation will complete this backwards reading before resuming with forward reading. If a forward reading is changed into a backwards reading, the last commited location will simply be treated as the lower end of the next backwards reading.  
  
Feed repository
---------------

A feed repository stores up to three locations for any given feed. A location can be one of several `Category`s:

- `CURRENT`: The last committed feed location. When reading forward, this is the latest known location where feed reading will resume. When reading backwards, this is the last page that was processed and committed.
- `UPPER`: When reading backwards, this is the location of the newest entry on the feed. In case of `REPEATING` feed continuation, feed reading for `RECOVERY`, if enabled, will begin at this location.
- `LOWER`: When reading backwards, this is the newest entry that was read in the latest backwards reading is marked as the end of the backwards reading by this category. If an upper location but no lower location is set, the backwards reading will end at the beginning of the feed.

The bundled `JdbcFeedRepository` allows for storage of generic location's by using a serializer and a deserializer for any given type. It is the serializer's responsibility to translate the generic type to a key-value map of strings and back.

Custom feed endpoints
---------------------

To implement a custom `FeedEnpoint`, one must first decide on a way to represent a feed location. A feed location can be any object that uniquely identifies a specific feed page and entry. To correctly process a feed with a `FeedManager`, the location object only needs to provide correct semantics for its `equals` contract. To ease development, adequat implementations for `hashCode` and `toString` are also recommended.

For a trivial example, consider a feed where any page contains a single entry and where the page is identified within a continuous sequence of numbers. In this scenario, Java's `Long` type can serve as a feed location. The `FeedEndpoint` then needs to implement the methods `getPage(Long)` to read a given page, the method `getFirstPage()` to read the first page of the feed and the `getLastPage()` method to read its last page. If one of the two latter operations is impossible to provide, a runtime exception can be thrown from them. A feed can then only be read backwards or forwards respectively.

All mentioned methods need to return an instance of `FeedPage` which provides a list of entries in the order of oldest entry to newest entry and the page's location, in the example a singular entry and the running page number as instance of `Long`. The page's entries must be instances of type `FeedEntry`  that only need to supply their location. Normally, the entries also expose a form of payload since they are passed to the `FeedConsumer`. The `FeedPage` interface additionally requires to expose their previous and next location. For the example, a feed page at location `7` would have a previous location of `6` and a next location `8`. Both values are optional to indicate that the lower or upper end of the feed was reached. Additionally, the `FeedPage` interface implements various default methods that can be overridden to improve the performance of operations by the `FeedManager`.

Custom feed processing
----------------------

Besides the `FeedManager`, the `FeedProcessor` interface can be implemented directly, to share a common abstraction with feed-similar processing models. The *feed-stream* module offers one such implementation by `StreamProcessor` where a Java `Reader` can be processed line-by-line with the capability to skip previous lines in a reprocessing if an error occurs. This can for example be used to process large files with intermediate commits where lines should not be reprocessed in case of an error. 

Also, the *feed-persistent-jdbc* module offers a wrapper for `FeedProcessor` by `JdbcFeedProcessor` for different databases where the current activation of a feed is persisted to a database. This allows for example to resume feed processing in case of an application restart without necessarily starting all feeds available in an application.
