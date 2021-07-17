## godbengine
godbengine is a library help for working with data more convenience.

### Redis connectstring 
- single server: password@address:port/dbNum[numClient]
- multiple server: password1@address1:port1/dbNum1[numClient1],password2@address2:port2/dbNum2[numClient2]...

### Memdb shading
- Hash function : hashValue = IteratorSum([]byte(key))
- Select server : serverID = hashValue % NumServer

### Memdb FindKey 
- Redis scan on all server