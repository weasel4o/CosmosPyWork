import azure.cosmos.cosmos_client as cosmos_client
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import azure.cosmos.errors as errors
import json
import azure.cosmos.http_constants as http_constants

url = 'https://localhost:8081'
key = 'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=='
client = cosmos_client.CosmosClient(url, {'masterKey': key})

database_name = 'testDatabase'
container_name = 'mycontainer'
try:
    database = client.CreateDatabase({'id': database_name})
except errors.HTTPFailure:
    database = client.ReadDatabase("dbs/" + database_name)

    import azure.cosmos.documents as documents

    container_definition = {'id': 'products',
                            'partitionKey':
                                {
                                    'paths': ['/productName'],
                                    'kind': documents.PartitionKind.Hash
                                }
                            }
    try:
        container = client.CreateContainer("dbs/" + database['id'], container_definition, {'offerThroughput': 400})
    except errors.HTTPFailure as e:
        if e.status_code == http_constants.StatusCodes.CONFLICT:
            container = client.ReadContainer("dbs/" + database['id'] + "/colls/" + container_definition['id'])
        else:
            raise e

        # Get the offer for the container
        offers = list(client.QueryOffers("Select * from root r where r.offerResourceId='" + container['_rid'] + "'"))
        offer = offers[0]
        print("current throughput for " + container['id'] + ": " + str(offer['content']['offerThroughput']))

        # Replace the offer with a new throughput
        offer['content']['offerThroughput'] = 1000
        client.ReplaceOffer(offer['_self'], offer)
        print("new throughput for " + container['id'] + ": " + str(offer['content']['offerThroughput']))

        database_id = 'testDatabase'
        container_id = 'products'
        container = client.ReadContainer("dbs/" + database_id + "/colls/" + container_id)

        for i in range(1, 10):
            client.UpsertItem("dbs/" + database_id + "/colls/" + container_id, {
                'id': 'item{0}'.format(i),
                'productName': 'Widget',
                'productModel': 'Model {0}'.format(i)
            }
                              )

            for item in client.QueryItems("dbs/" + database_id + "/colls/" + container_id,
                                          'SELECT * FROM products p WHERE p.productModel = "DISCONTINUED"',
                                          {'enableCrossPartitionQuery': True}):
                client.DeleteItem("dbs/" + database_id + "/colls/" + container_id + "/docs/" + item['id'],
                                  {'partitionKey': 'Pager'})

                # Enumerate the returned items
        for item in client.QueryItems("dbs/" + database_id + "/colls/" + container_id,
                                      'SELECT * FROM ' + container_id + ' r WHERE r.id="item3"',
                                      {'enableCrossPartitionQuery': True}):
            print(json.dumps(item, indent=True))
