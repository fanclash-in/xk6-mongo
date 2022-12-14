package xk6_mongo

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	k6modules "go.k6.io/k6/js/modules"
)

// Register the extension on module initialization, available to
// import from JS as "k6/x/mongo".
func init() {
	k6modules.Register("k6/x/mongo", new(Mongo))
}

// Mongo is the k6 extension for a Mongo client.
type Mongo struct{}

// Client is the Mongo client wrapper.
type Client struct {
	client *mongo.Client
}

// NewClient represents the Client constructor (i.e. `new mongo.Client()`) and
// returns a new Mongo client object.
// connURI -> mongodb://username:password@address:port/db?connect=direct
func (*Mongo) NewClient(connURI string) interface{} {

	clientOptions := options.Client().ApplyURI(connURI)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return err
	}
	return &Client{client: client}

}

func (c *Client) Insert(database string, collection string, doc map[string]string) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.InsertOne(context.TODO(), doc)
	if err != nil {
		return err
	}
	return nil
}

func toBsonD(v interface{}) (doc *bson.D, err error) {
	data, err := bson.Marshal(v)
	if err != nil {
		panic(err)
		// log.Fatal("Not able to marshal to bson.D")
	}

	err = bson.Unmarshal(data, &doc)
	return
}

func (*Mongo) ToObjectId(id string) (primitive.ObjectID, error) {
	objectId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return primitive.ObjectID{}, err
	}

	return objectId, nil
}

func (*Mongo) ToString(objectId primitive.ObjectID) string {
	id := objectId.Hex()
	return id
}

// type FindOptions struct {
// 	limit      int64
// 	skip       int64
// 	sort       interface{}
// 	projection interface{}
// 	hint       interface{}
// }

func (c *Client) Find(database string, collection string, filter interface{}, findOptions interface{}) []bson.M {
	db := c.client.Database(database)
	col := db.Collection(collection)

	// log.Print("findOptions", findOptions)
	findOptionsV2 := findOptions.(map[string]interface{})
	options := options.FindOptions{}

	// sortValue := &primitive.D{}
	// projectionValue := &primitive.D{}
	// var limitValue int64 = 0
	// var skipValue int64 = 0
	// var hintValue string = ""

	if findOptionsV2["sort"] != nil {
		doc, err := toBsonD(findOptionsV2["sort"])
		if err != nil {
			panic(err)
		}
		options.Sort = doc
	}
	if findOptionsV2["projection"] != nil {
		doc, err := toBsonD(findOptionsV2["projection"])
		if err != nil {
			panic(err)
		}
		options.Projection = doc
	}
	if findOptionsV2["limit"] != nil {
		limitValue := findOptionsV2["limit"].(int64)
		options.Limit = &limitValue
	}
	if findOptionsV2["skip"] != nil {
		skipValue := findOptionsV2["skip"].(int64)
		options.Skip = &skipValue
	}
	if findOptionsV2["hint"] != nil {
		options.Hint = findOptionsV2["hint"].(string)
	}

	// log.Print("options", options)

	cur, err := col.Find(context.TODO(), filter, &options)
	if err != nil {
		panic(err)
		// log.Fatal("Error in fetching documents.")
	}

	var results []bson.M
	if err = cur.All(context.TODO(), &results); err != nil {
		panic(err)
		// log.Fatal("Error in fetching documents.")
	}

	//Converting ObjectId to string
	// for i := 0; i < 1; i++ {
	// 	// log.Print(results[i]["_id"].(primitive.ObjectID).Hex())
	// 	results[i]["_id"] = results[i]["_id"].(primitive.ObjectID).Hex()
	// }

	return results
}

func (c *Client) FindOne(database string, collection string, filter map[string]string) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	var result bson.M
	opts := options.FindOne().SetSort(bson.D{{"_id", 1}})
	log.Print("filter is ", filter)
	err := col.FindOne(context.TODO(), filter, opts).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("found document %v", result)
	return nil
}
