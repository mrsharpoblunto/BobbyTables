BobbyTables
===========

BobbyTables is a .Net ORM library for the Dropbox datastore API. It handles serializing and deserializing objects to and from the remote Dropbox datastore as well as handling pushing/pulling updates. BobbyTables supports .Net versions 2+, Silverlight 4+, Windows Phone 7.5+, and Windows store applications.

### API & Usage

#### DatastoreManager
---------------------
DatastoreManager Is used to list/add/remove datastore objects. To create a DatastoreManager, you will need a Dropbox OAuth 2.0 bearer token (You can get this by completing an OAuth 2.0 handshake - see https://www.dropbox.com/developers/core/docs#oa2-authorize for more details)

```c#
var manager = new DatastoreManager("oauth_token");

// Getting or creating a datastore
var datastore = await manager.GetOrCreateAsync("test");

// Getting an existing datastore
var existing_datastore = await manager.GetAsync("default");

// Listing available datastores
var datastore_list = await manager.ListAsync();
foreach (var ds in datastore_list) {
  // ...
}

// Deleting a datastore
await manager.DeleteAsync(datastore);

// Wait for up to a minute or for remote changes to occur, whichever comes first
List<Datastore> changed = new List<Datastore>();
if (await manager.AwaitDatastoreChangesAsync(changed)) {
  // Any datastores that have remote changes applied are now in the changed list
}

// Wait for up to a minute or for a change to the list of datastores, whichever comes first
if (await manager.AwaitListChangesAsync()) {
  // calling manager.ListAsync will retrieve the changed list of datastores
}
```

__NOTE__: All API methods that make remote requests to dropbox have a synchronous implementation, an async implementation that uses .net 4.5 async/await, and an async implementation that uses callback functions for when async/await is not supported.

#### Datastore
--------------
A Datastore object Is analagous to a Database object in a typical ORM library. A Datastore contains Tables that can contain objects which can be added/updated/removed

#### Inserting data into a datastore table
```c#
class Appointment {
  public string Id;
  public DateTime Time;
  public List<String> People = new List<string>();
}

...

// pull in any remote changes and make sure we are up to date before
// trying to apply our own local changes
await datastore.PullAsync();

// get a reference to the appointments table
var table = datastore.GetTable<Appointment>("appointments");

var new_appointment = new Appointment{ Id = "1", Time = DateTime.Now() };
new_appointment.People.Add("Jules");

// insert the object into the table. An Id can also be specified for this method
// though because this object has a public string field called Id, this is worked
// out automatically. Also note this method is not awaited as it is only recording
// the change as pending locally. No changes have been pushed to Dropbox yet.
table.Insert(new_appointment);

// now lets commit the pending insert and push it out to Dropbox
if (await datastore.PushAsync()) {
  // Yay! the changes were accepted
} else {
  // Oh no! a conflict occurred due to another user submitting a change concurrently.
  // In this case we should revert our local changes, Pull in the latest changes and
  // try again. NOTE: The Transaction feature can help make handling conflicts easier.
}
```

#### Using transactions to handle conflicts
```c#
  // every operation inside the transaction will try to be pushed to Dropbox in a single
  // commit. If anything fails, all changes are reverted, the latest changes are pulled
  // from Dropbox, and the changes will be re-applied until the commit succeeds, or the 
  // max number of retries is exceeded
  var success = await datastore.Transaction(()=> {
    var table = datastore.GetTable<Appointment>("appointments");
    
    var new_appointment = new Appointment{ Id = "1", Time = DateTime.Now() };
    new_appointment.People.Add("Jules");
    
    table.Insert(new_appointment);
  }).PushAsync(); // can specify the number of retries as a parameter (default 1)
  
```

#### Retrieving and updating existing data
```c#
var table = datastore.GetTable<Appointment>("appointments");

// can search for objects using LINQ queries
var appointment = (from appt in table where t.Id == "1" select appt).SingleOrDefault();

// or you can search using the Id directly
appointment = table.Get("1");

appointment.People.Add("Vincent");
appointment.People.Add("Marcellus");

// now lets commit the pending update and push it out to Dropbox
await datastore.Transaction(()=> {
  table.Update(appointment);
}).PushAsync();
```

#### Saving and loading local datastore snapshots
```c#
// the local state of a datastore can be saved to a streamWriter. In this case we
// are choosing to save the local snapshot to a file on disk
using (var stream = new FileStream("C:\\db.json",FileMode.Create,FileAccess.Write)) {
  using (StreamWriter writer = new StreamWriter(stream))
  {
  	datastore.Save(writer);
  }
}

...

// We can then reload the old state by loading this file
using (var stream = new FileStream("C:\\db.json",FileMode.Open,FileAccess.Read)) {
  using (StreamReader reader = new StreamReader(stream))
  {
  	datastore = manager.Load(reader);
  }
}
```

#### Detecting when remote changes occur
```c#
while (true) {
  // Waits for up to a minute or for a change to occur, whichever happens first
  if (await datastore.AwaitPullAsync()) {
    // some remote changes occurred within the last minute and have been pulled in
    // to the local snapshot
  } else {
    // no changes occurred
  }
}
```

#### The fiddly details of serializing objects

Only public fields, and public readable/writable properties can be serialized. If you want a field to be ignored from serialization tag it with the <code>[BobbyTables.Ignore]</code> attribute.

The dropbox datastore API only has support for the following datatypes so any objects that have fields with an unsupported datatype will not be able to be serialized or deserialized correctly (Lists or Arrays of any of the below data types are also supported)

| Dropbox datatype | .NET datatype         |
|------------------|:----------------------|
| str              | string                |
| number           | float,Single,double   |
| int              | int/uint 16,32,64     |
| timestamp        | DateTime              |
| blob             | List\<byte\>, byte[]  |
